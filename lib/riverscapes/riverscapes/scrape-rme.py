"""
Demo script to download files from Data Exchange
"""
import csv
import shutil
import glob
import re
import os
import sqlite3
import logging
import subprocess
import argparse
from osgeo import ogr
from rsxml import dotenv, Logger, safe_makedirs
from riverscapes import RiverscapesAPI, RiverscapesSearchParams

# RegEx for finding the RME output GeoPackages
RME_OUTPUT_GPKG_REGEX = r'.*riverscapes_metrics\.gpkg'
# RCAT_OUTPUT_GPKG_REGEX = r'.*rcat\.gpkg'


def scrape_rme(rs_api: RiverscapesAPI, search_params: RiverscapesSearchParams, download_dir: str, output_gpkg: str, delete_downloads: bool) -> None:
    """
    Download RME output GeoPackages from Data Exchange and scrape the metrics into a single GeoPackage
    """

    log = Logger('Scrape RME')

    # Loop over all projects yielded by the search
    for project, _stats, _searchtotal in rs_api.search(search_params, progress_bar=True, page_size=100):
        try:

            # Attempt to retrieve the huc10 from the project metadata if it exists
            huc10 = None
            for key in ['HUC10', 'huc10', 'HUC', 'huc']:
                if key in project.project_meta:
                    value = project.project_meta[key]
                    huc10 = value if len(value) == 10 else None
                    break

            if continue_with_huc(huc10, output_gpkg) is not True:
                continue

            log.info(f'Scraping RME metrics for HUC {huc10}')
            huc_dir = os.path.join(download_dir, huc10)
            safe_makedirs(huc_dir)

            huc_dir = os.path.join(download_dir, huc10)
            rme_gpkg = download_file(rs_api, project.id, os.path.join(huc_dir, 'rme'), RME_OUTPUT_GPKG_REGEX)

            scrape_huc(huc10, rme_gpkg, project.id, output_gpkg)

        except Exception as e:
            log.error(f'Error scraping HUC {huc10}: {e}')

        if delete_downloads is True and os.path.isdir(huc_dir):
            try:
                log.info(f'Deleting download directory {huc_dir}')
                shutil.rmtree(huc_dir)
            except Exception as e:
                log.error(f'Error deleting download directory {huc_dir}: {e}')


def download_file(rs_api: RiverscapesAPI, project_id: str, download_dir: str, regex: str) -> str:
    '''
    Download files from a project on Data Exchange
    '''

    gpkg_path = get_matching_file(download_dir, regex)
    if gpkg_path is not None and os.path.isfile(gpkg_path):
        return gpkg_path

    rs_api.download_files(project_id, download_dir, [regex])

    gpkg_path = get_matching_file(download_dir, regex)

    if gpkg_path is None or not os.path.isfile(gpkg_path):
        raise FileNotFoundError(f'Could not find output GeoPackage in {download_dir}')

    return gpkg_path


def get_matching_file(parent_dir: str, regex: str) -> str:
    '''
    Get the path to a file that matches the regex
    '''

    regex = re.compile(regex)
    for root, __dirs, files in os.walk(parent_dir):
        for file_name in files:
            # Check if the file name matches the regex
            if regex.match(file_name):
                return os.path.join(root, file_name)

    return None


def continue_with_huc(huc10: str, output_gpkg: str) -> bool:
    '''
    Check if the HUC already exists in the output GeoPackage
    '''

    if not os.path.isfile(output_gpkg):
        return True

    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()

        # The hucs table only exists if at least one HUC has been scraped
        curs.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'hucs'")
        if curs.fetchone() is None:
            return True

        curs.execute('SELECT huc FROM hucs WHERE huc = ? LIMIT 1', [huc10])
        return curs.fetchone() is None

    return False


def scrape_huc(huc10: str, rme_gpkg: str, rme_guid: str, output_gpkg: str) -> None:

    log = Logger('Scrape HUC')

    create_side_tables = not os.path.isfile(output_gpkg)

    # Perform OG2OGR to append the IGO features to the output GeoPackage
    # First time through, this will create the output GeoPackage
    cmd = f'ogr2ogr -f GPKG -makevalid -append "{output_gpkg}" "{rme_gpkg}" igos'
    log.debug(f'EXECUTING: {cmd}')
    subprocess.call([cmd], shell=True, cwd=os.path.dirname(output_gpkg))

    driver = ogr.GetDriverByName('GPKG')
    data_source = driver.Open(output_gpkg, update=1)  # update=0 for read-only mode
    layer = data_source.GetLayerByName('igos')

    if create_side_tables is True:
        # Add the HUC column to the IGOs feature class
        field_name = 'huc'
        field_definition = ogr.FieldDefn(field_name, ogr.OFTString)
        if layer.CreateField(field_definition) == 0:
            log.info(f"Field '{field_name}' added successfully to the feature class igos.")
        else:
            log.error(f"Failed to add field '{field_name}'.")
            raise Exception(f"Failed to add field '{field_name}'.")

        # Create the non-spatial tables now the output GeoPackage exists
        create_output_tables(output_gpkg)

    # The HUC code is not on the RME igos feature class. We need to update it.
    # Store the HUC code with the IGOs that were just inserted.
    # Its a feature class so we need to use OGR to do this!
    data_source.ExecuteSQL(f'UPDATE igos SET huc = {huc10} WHERE huc IS NULL')
    layer = None
    data_source = None

    # now copy the remaining data from the source database to the output database
    with sqlite3.connect(rme_gpkg) as in_conn:
        in_cursor = in_conn.cursor()

        with sqlite3.connect(output_gpkg) as out_conn:
            out_cursor = out_conn.cursor()

            # DGOs are done manually because we don't need the geometry
            process_rme_dgos(in_cursor, out_cursor, huc10)
            # process_rcat_dgos(rcat_pgkg, out_cursor, huc)

            for prefix in ['dgo']:
                process_rme_metric_values(in_cursor, out_cursor, prefix, huc10)

            out_cursor.execute('INSERT INTO hucs (huc, rme_project_id) VALUES (?, ?)', [huc10, rme_guid])
            out_conn.commit()


def process_rme_dgos(in_cursor, out_cursor, huc: str) -> None:
    """
    Use SQL to copy the DGOs from the RME to the output GeoPackage with their geometries
    """

    in_cursor.execute('''
        SELECT
            ? huc,
            level_path,
            seg_distance,
            FCode,
            low_lying_floodplain_area,
            low_lying_floodplain_prop,
            active_channel_area,
            active_channel_prop,
            elevated_floodplain_area,
            elevated_floodplain_prop,
            floodplain_area,
            floodplain_prop,
            centerline_length,
            segment_area,
            integrated_width
        FROM dgos
        WHERE level_path IS NOT NULL
            AND seg_distance IS NOT NULL
        ''', [huc])

    out_cursor.executemany('''
        INSERT INTO dgos (
            huc,
            level_path,
            seg_distance,
            FCode,
            low_lying_floodplain_area,
            low_lying_floodplain_prop,
            active_channel_area,
            active_channel_prop,
            elevated_floodplain_area,
            elevated_floodplain_prop,
            floodplain_area,
            floodplain_prop,
            centerline_length,
            segment_area,
            integrated_width)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', in_cursor.fetchall())


# def process_rcat_dgos(rcat_gpkg: str, out_cursor, huc: str) -> None:

#     with sqlite3.connect(rcat_gpkg) as conn:
#         in_cursor = conn.cursor()
#         in_cursor.execute('''
#             SELECT
#                 ? huc,
#                 level_path,
#                 seg_distance,
#                 FCode,
#                 centerline_length,
#                 segment_area,
#                 LUI,
#                 FloodplainAccess,
#                 FromConifer,
#                 FromDevegetated,
#                 FromGrassShrubland,
#                 NoChange,
#                 GrassShrubland,
#                 Devegetation,
#                 Conifer,
#                 Invasive,
#                 Development,
#                 Agriculture,
#                 NonRiparian,
#                 ExistingRiparianMean,
#                 HistoricRiparianMean,
#                 RiparianDeparture,
#                 Condition
#             FROM DGOAttributes
#             WHERE level_path IS NOT NULL
#                 AND seg_distance IS NOT NULL
#             ''', [huc])

#         out_cursor.executemany('''
#             INSERT INTO rcat_dgos (
#                 huc,
#                 level_path,
#                 seg_distance,
#                 FCode,
#                 centerline_length,
#                 segment_area,
#                 LUI,
#                 FloodplainAccess,
#                 FromConifer,
#                 FromDevegetated,
#                 FromGrassShrubland,
#                 NoChange,
#                 GrassShrubland,
#                 Devegetation,
#                 Conifer,
#                 Invasive,
#                 Development,
#                 Agriculture,
#                 NonRiparian,
#                 ExistingRiparianMean,
#                 HistoricRiparianMean,
#                 RiparianDeparture,
#                 Condition)
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#     ''', in_cursor.fetchall())


def process_rme_metric_values(in_cursor, out_cursor, prefix: str, huc: str) -> None:
    """
    Use SQL to copy the metric values from the RME to the output GeoPackage
    """

    in_cursor.execute(f'''
        SELECT ? huc, level_path, seg_distance, metric_id, metric_value
        FROM {prefix}_metric_values v INNER JOIN {prefix}s i on v.{prefix}_id = i.fid
    ''', [huc])

    out_cursor.executemany(f'''
        INSERT INTO {prefix}_metric_values (huc, level_path, seg_distance, metric_id, metric_value)
        VALUES (?, ?, ?, ?, ?)
    ''', in_cursor.fetchall())


def get_table_columns(cursor, table_name):
    """Retrieve column names for a given table."""

    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    return [col[1] for col in columns]  # Column names are in the second field of each row


def create_output_tables(outputs_gpkg: str) -> None:
    '''
    Create the schema of the output RME scrape GeoPackage and load lookup data.
    Assumes that the GeoPackage has already been created.
    '''

    schema_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'packages', 'rme', 'rme', 'database')
    if not os.path.isdir(schema_dir):
        raise FileNotFoundError(f'Could not find database directory {schema_dir}')

    # GeoPackage should already be created when creating feature classes with OGR
    if not os.path.isfile(outputs_gpkg):
        raise FileNotFoundError(f'Could not find output GeoPackage {outputs_gpkg}')

    # Create non-spatial tables
    with sqlite3.connect(outputs_gpkg) as conn:
        cursor = conn.cursor()
        with open(os.path.join(schema_dir, 'rme_scrape_schema.sql'), encoding='utf-8') as sqlfile:
            sql_commands = sqlfile.read()
            cursor.executescript(sql_commands)
            conn.commit()

    # Load Measurements and Metrics data table records
    load_lookup_data(outputs_gpkg, os.path.join(schema_dir, 'data_metrics'))


def load_lookup_data(db_path, csv_dir):
    """Load the database lookup data from CSV files.
    This gets called both during database creation during BRAT build,
    but also during refresh of lookup data at the start of BRAT Run so that
    the database has the latest hydrologic equations and other BRAT parameters

    Args:
        db_path (str): Full path to SQLite database
        csv_dir (str): Full path to the root folder containing CSV lookup files
    """

    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    curs = conn.cursor()

    log = Logger('Database')

    if not os.path.isdir(csv_dir):
        raise Exception(f'csv_dir path was not a valid directory: {csv_dir}')

    # Load lookup table data into the database
    dir_search = os.path.join(csv_dir, '**', '*.csv')
    for file_name in glob.glob(dir_search, recursive=True):
        table_name = os.path.splitext(os.path.basename(file_name))[0]
        with open(file_name, mode='r', encoding='utf8') as csvfile:
            d = csv.DictReader(csvfile)
            sql = 'INSERT OR REPLACE INTO {0} ({1}) VALUES ({2})'.format(table_name, ','.join(d.fieldnames), ','.join('?' * len(d.fieldnames)))

            to_db = [[i[col] for col in d.fieldnames] for i in d]
            curs.executemany(sql, to_db)
            log.info('{:,} records loaded into {} lookup data table'.format(curs.rowcount, table_name))

    conn.commit()


def dict_factory(cursor, row):
    """Convert the database row into a dictionary."""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def main():
    """
    Scrape RME projects. Combine IGOs with their geometries. Include DGO metrics only.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('tags', help='Data Exchange tags to search for projects', type=str)
    parser.add_argument('--delete', help='Whether or not to delete downloaded GeoPackages',  action='store_true', default=False)
    parser.add_argument('--huc_filter', help='HUC filter begins with (e.g. 14)', type=str, default='')
    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    working_folder = args.working_folder
    download_folder = os.path.join(working_folder, 'downloads')
    output_gpkg = os.path.join(working_folder, 'rme_scrape.gpkg')

    safe_makedirs(working_folder)
    log = Logger('Setup')
    log.setup(log_path=os.path.join(working_folder, 'rme-scrape.log'), log_level=logging.DEBUG)

    # Data Exchange Search Params
    search_params = RiverscapesSearchParams({
        'tags': args.tags.split(','),
        'projectTypeId': 'rs_metric_engine',
    })

    if args.huc_filter != '' and args.huc_filter != '.':
        search_params.meta = {
            "HUC": args.huc_filter
        }

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_rme(api, search_params, download_folder, output_gpkg, args.delete)

    log.info('Process complete')


if __name__ == '__main__':
    main()
