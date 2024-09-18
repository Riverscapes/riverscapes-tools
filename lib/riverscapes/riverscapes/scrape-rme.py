"""
Demo script to download files from Data Exchange
"""
from typing import Dict
import shutil
import sys
import re
import os
import sqlite3
import logging
import subprocess
import argparse
from osgeo import ogr
from rsxml import dotenv, Logger, safe_makedirs
from rscommons.database import load_lookup_data
from riverscapes import RiverscapesAPI

# RegEx for finding the RME output GeoPackages
RME_OUTPUT_GPKG_REGEX = r'.*riverscapes_metrics\.gpkg'
RCAT_OUTPUT_GPKG_REGEX = r'.*rcat\.gpkg'


def scrape_rme(rs_api: RiverscapesAPI,  projects: Dict[str, str], download_dir: str, output_gpkg: str, delete_downloads: bool) -> None:

    log = Logger('Scrape RME')

    for huc, project_ids in projects.items():
        try:

            if continue_with_huc(huc, output_gpkg) is not True:
                continue

            log.info(f'Scraping RME metrics for HUC {huc}')
            huc_dir = os.path.join(download_dir, huc)
            safe_makedirs(huc_dir)

            rme_guid = project_ids['rme']
            rme_gpkg = download_file(rs_api, rme_guid, os.path.join(huc_dir, 'rme'), RME_OUTPUT_GPKG_REGEX)

            rcat_guid = project_ids['rcat']
            rcat_gpkg = download_file(rs_api, rcat_guid, os.path.join(huc_dir, 'rcat'), RCAT_OUTPUT_GPKG_REGEX)

            scrape_huc(huc, rme_guid, rme_gpkg, rcat_guid, rcat_gpkg, output_gpkg)

            if delete_downloads is True:
                try:
                    log.info(f'Deleting download directory {huc_dir}')
                    shutil.rmtree(huc_dir)
                except Exception as e:
                    log.error(f'Error deleting download directory {huc_dir}: {e}')
        except Exception as e:
            log.error(f'Error scraping HUC {huc}: {e}')
            continue


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

    regex = re.compile(regex)
    for root, __dirs, files in os.walk(parent_dir):
        for file_name in files:
            # Check if the file name matches the regex
            if regex.match(file_name):
                return os.path.join(root, file_name)

    return None


def continue_with_huc(huc: str, output_gpkg: str) -> bool:
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

        curs.execute('SELECT huc FROM hucs WHERE huc = ? LIMIT 1', [huc])
        return curs.fetchone() is None

    return False


def scrape_huc(huc: str, rme_guid: str, rme_gpkg: str, rcat_guid: str, rcat_pgkg: str, output_gpkg: str) -> None:

    log = Logger('Scrape HUC')

    # Track whether the output GeoPackage already exists so we know whether to create tables
    create_tables = not os.path.isfile(output_gpkg)

    # Perform OG2OGR to append the IGOs to the output GeoPackage
    cmd = f'ogr2ogr -f GPKG -makevalid -append "{output_gpkg}" "{rme_gpkg}" igos'
    log.debug(f'EXECUTING: {cmd}')
    subprocess.call([cmd], shell=True, cwd=os.path.dirname(output_gpkg))

    driver = ogr.GetDriverByName('GPKG')
    data_source = driver.Open(output_gpkg, update=1)  # update=0 for read-only mode

    if create_tables is True:
        # First time through. Add the HUC column to the IGOs feature class
        layer = data_source.GetLayerByName('igos')
        field_name = 'huc'
        field_definition = ogr.FieldDefn(field_name, ogr.OFTString)
        # Add the new field to the feature class
        if layer.CreateField(field_definition) == 0:
            print(f"Field '{field_name}' added successfully to the feature class igos.")
        else:
            print(f"Failed to add field '{field_name}'.")

    # Store the HUC code with the IGOs that were just inserted.
    data_source.ExecuteSQL(f'UPDATE igos SET huc = {huc} WHERE huc IS NULL')
    layer = None
    data_source = None

    # Now the output database exists we can create the non-spatial tables if necessary
    if create_tables is True:
        create_output_tables(output_gpkg)

    # now copy the remaining data from the source database to the output database
    with sqlite3.connect(rme_gpkg) as in_conn:
        in_cursor = in_conn.cursor()

        with sqlite3.connect(output_gpkg) as out_conn:
            out_cursor = out_conn.cursor()

            # DGOs are done manually because we don't need the geometry
            process_rme_dgos(in_cursor, out_cursor, huc)
            process_rcat_dgos(rcat_pgkg, out_cursor, huc)

            for prefix in ['dgo', 'igo']:
                process_rme_metric_values(in_cursor, out_cursor, prefix, huc)

            out_cursor.execute('INSERT INTO hucs (huc, rme_project_id, rcat_project_id) VALUES (?, ?, ?)', [huc, rme_guid, rcat_guid])
            out_conn.commit()


def process_rme_dgos(in_cursor, out_cursor, huc: str) -> None:

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


def process_rcat_dgos(rcat_gpkg: str, out_cursor, huc: str) -> None:

    with sqlite3.connect(rcat_gpkg) as conn:
        in_cursor = conn.cursor()
        in_cursor.execute('''
            SELECT
                ? huc,
                level_path,
                seg_distance,
                FCode,
                centerline_length,
                segment_area,
                LUI,
                FloodplainAccess,
                FromConifer,
                FromDevegetated,
                FromGrassShrubland,
                NoChange,
                GrassShrubland,
                Devegetation,
                Conifer,
                Invasive,
                Development,
                Agriculture,
                NonRiparian,
                ExistingRiparianMean,
                HistoricRiparianMean,
                RiparianDeparture,
                Condition
            FROM DGOAttributes
            WHERE level_path IS NOT NULL
                AND seg_distance IS NOT NULL
            ''', [huc])

        out_cursor.executemany('''
            INSERT INTO rcat_dgos (
                huc,
                level_path,
                seg_distance,
                FCode,
                centerline_length,
                segment_area,
                LUI,
                FloodplainAccess,
                FromConifer,
                FromDevegetated,
                FromGrassShrubland,
                NoChange,
                GrassShrubland,
                Devegetation,
                Conifer,
                Invasive,
                Development,
                Agriculture,
                NonRiparian,
                ExistingRiparianMean,
                HistoricRiparianMean,
                RiparianDeparture,
                Condition)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', in_cursor.fetchall())


def process_rme_metric_values(in_cursor, out_cursor, prefix: str, huc: str) -> None:

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


def main():
    """
    Scrape RME projects
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('db_path', help='Path to the warehouse dump database', type=str)
    parser.add_argument('huc2', help='2 digit HUC over which to operate', type=str)
    args = dotenv.parse_args_env(parser)

    if not os.path.isfile(args.db_path):
        print(f'Data Exchange project dump database file not found: {args.db_path}')
        sys.exit(1)

    if args.huc2 < 1 or args.huc2 > 18:
        print(f'HUC2 {args.huc2} must be between 1 and 18')

    # Set up some reasonable folders to store things
    working_folder = args.working_folder  # os.path.join(args.working_folder, output_name)
    download_folder = os.path.join(working_folder, 'downloads')
    scraped_folder = working_folder  # os.path.join(working_folder, 'scraped')

    safe_makedirs(scraped_folder)
    log = Logger('Setup')
    log.setup(log_path=os.path.join(scraped_folder, 'rme-scrape.log'), log_level=logging.DEBUG)

    # Determine all the RME projects from the dumped warehouse
    with sqlite3.connect(args.db_path) as conn:
        curs = conn.cursor()
        curs.execute('''
            select huc10, min(rme_project_id), min(rcat_project_id)
            from
            (
                select huc10,
                    case when project_type_id = 'rs_metric_engine' then project_id else null end rme_project_id,
                    case when project_type_id = 'rcat' then project_id else null end             rcat_project_id
                from vw_conus_projects
                where project_type_id in ('rs_metric_engine', 'rcat')
                    and tags = '2024CONUS'
            )
            group by huc10
            having min(rme_project_id) is not null
                and min(rcat_project_id) is not null
                and huc10 like ?''', [f'{args.huc2}%'])
        projects = {row[0]: {
            'rme': row[1],
            'rcat': row[2]
        } for row in curs.fetchall()}

    if len(projects) == 0:
        log.info('No RME projects found in Data Exchange dump')
        sys.exit(0)

    log.info(f'Found {len(projects)} RME projects in Data Exchange dump')

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_rme(api, projects, download_folder, os.path.join(scraped_folder, f'rme_scrape_huc{args.huc2}.gpkg'), False)

    log.info('Process complete')


if __name__ == '__main__':
    main()
