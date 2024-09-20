"""
Scrapes RME and RCAT outout GeoPackages from Data Exchange and extracts statistics for each HUC.
Produced for the BLM 2024 September analysis of 2024 CONUS RME projects.
Philip Bailey
"""
from typing import Dict
import shutil
import sys
import re
import os
import copy
import sqlite3
import logging
import argparse
import uuid
from rsxml import dotenv, Logger, safe_makedirs
from riverscapes import RiverscapesAPI

# RegEx for finding RME and RCAT output GeoPackages
RME_OUTPUT_GPKG_REGEX = r'.*riverscapes_metrics\.gpkg'
RCAT_OUTPUT_GPKG_REGEX = r'.*rcat\.gpkg'

# Conversion factors
METRES_TO_MILES = 0.000621371
SQMETRES_TO_ACRES = 0.000247105

# Output template for the data to be scraped.
# Keys must match the schema of the output database 'metrics' table
DATA_TEMPLATE = {
    'state_id': None,
    'owner_id': None,
    'flow_id': None,
    'huc10': None,
    'dgo_count': None,
    'dgo_area_acres': None,
    'dgo_length_miles': None,
    'active_area': None,
    'floodplain_access_area': None,
    'lui_zero_area': None,
    'hist_riparian_area': None,
}


def scrape_hucs(rs_api: RiverscapesAPI,  projects: Dict[str, str], download_dir: str, output_db: str, delete_downloads: bool) -> None:
    """
    Loop over all the projects, download the RME and RCAT output GeoPackages, and scrape the statistics
    """

    log = Logger('Scrape HUCs')

    # Load the foreign key look up tables for owners and flows
    owners = load_filters(output_db, 'owners')
    flows = load_filters(output_db, 'flows')
    states = load_filters(output_db, 'us_states')

    for index, (huc, project_ids) in enumerate(projects.items(), start=1):
        try:
            # HUCs that appears in 'hucs' db table are skipped
            if continue_with_huc(huc, output_db) is not True:
                continue

            log.info(f'Scraping RME metrics for HUC {huc} ({index} of {len(projects)})')
            huc_dir = os.path.join(download_dir, huc)

            rme_guid = project_ids['rme']
            rme_gpkg = download_file(rs_api, rme_guid, os.path.join(huc_dir, 'rme'), RME_OUTPUT_GPKG_REGEX)

            rcat_guid = project_ids['rcat']
            rcat_gpkg = download_file(rs_api, rcat_guid, os.path.join(huc_dir, 'rcat'), RCAT_OUTPUT_GPKG_REGEX)

            # Copy RCAT db so we copy some RME data into it without mutating the original
            rcat_gpkg_copy = copy_file_with_unique_name(rcat_gpkg)

            huc_metrics = []
            with sqlite3.connect(rme_gpkg) as rme_conn:
                rme_curs = rme_conn.cursor()

                with sqlite3.connect(rcat_gpkg_copy) as rcat_conn:
                    rcat_curs = rcat_conn.cursor()

                    copy_table_between_cursors(rme_curs, rcat_curs, 'dgo_metric_values')
                    copy_table_between_cursors(rme_curs, rcat_curs, 'dgos')
                    rcat_conn.commit()  # so we can test queries in DataGrip

                    for __state_name, state_data in states.items():

                        for __flow_name, flow_data in flows.items():

                            # Without an owner filter we get statistics for all owners for a certain FCode
                            data = copy.deepcopy(DATA_TEMPLATE)
                            data['state_id'] = state_data['id']
                            data['flow_id'] = flow_data['id']
                            data['huc10'] = huc
                            scrape_rme_statistics(rme_curs, state_data, flow_data, None, data)
                            scrape_rcat_statistics(rcat_curs, state_data, flow_data, None, data)

                            if data['dgo_count'] > 0:
                                huc_metrics.append(data)

                            for __owner_name, owner_data in owners.items():

                                data = copy.deepcopy(DATA_TEMPLATE)
                                data['state_id'] = state_data['id']
                                data['owner_id'] = owner_data['id']
                                data['flow_id'] = flow_data['id']
                                data['huc10'] = huc

                                # Statistics with both owner and flow filters
                                scrape_rme_statistics(rme_curs, state_data, flow_data, owner_data, data)
                                scrape_rcat_statistics(rcat_curs, state_data, flow_data, owner_data, data)

                                if data['dgo_count'] > 0:
                                    huc_metrics.append(data)

            # Store the output HUC metrics
            keys = huc_metrics[0].keys()
            with sqlite3.connect(output_db) as conn:
                curs = conn.cursor()
                curs.execute('INSERT INTO hucs (huc10, rme_project_guid, rcat_project_guid) VALUES (?, ?, ?)', [huc, rme_guid, rcat_guid])
                curs.executemany(f'INSERT INTO metrics ({", ".join(keys)}) VALUES ({", ".join(["?" for _ in keys])})', [tuple(m[k] for k in keys) for m in huc_metrics])
                conn.commit()

        except Exception as e:
            log.error(f'Error scraping HUC {huc}: {e}')

        if delete_downloads is True and os.path.isdir(huc_dir):
            try:
                log.info(f'Deleting download directory {huc_dir}')
                shutil.rmtree(huc_dir)
            except Exception as e:
                log.error(f'Error deleting download directory {huc_dir}: {e}')


def copy_file_with_unique_name(file_path):
    """
    Deduce a new, unique file name from the original file name and copy the file to the new file name.
    """

    folder = os.path.dirname(file_path)
    original_filename = os.path.basename(file_path)
    name, ext = os.path.splitext(original_filename)

    # Generate a unique filename using uuid
    unique_filename = f"{name}_{uuid.uuid4().hex}{ext}"
    new_file_path = os.path.join(folder, unique_filename)

    # Copy the file to the new file path
    shutil.copy2(file_path, new_file_path)

    # print(f"File copied to: {new_file_path}")
    return new_file_path


def copy_table_between_cursors(src_cursor, dest_cursor, table_name):
    """
    Copy a table structure and data from the source cursor to destination cursor
    """

    # Get table schema from the source database
    src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    create_table_sql = src_cursor.fetchone()[0]
    dest_cursor.execute(create_table_sql)

    # Get all data from the source table
    src_cursor.execute(f"SELECT * FROM {table_name}")
    rows = src_cursor.fetchall()

    # Get the column names from the source table
    src_cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in src_cursor.fetchall()]  # info[1] gives the column names
    columns_str = ', '.join(columns)

    # Insert data into the destination table
    placeholders = ', '.join(['?' for _ in columns])  # Create placeholders for SQL insert
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    dest_cursor.executemany(insert_sql, rows)


def scrape_rme_statistics(curs: sqlite3.Cursor, state: Dict[str, str], flow: Dict[str, str], owner: Dict[str, str], output: Dict[str, float]) -> None:
    """
    Scrape statistics from the RME output. The owner and flow filters are optional.
    The output of this function is to insert several RME statistics into the "data" dictionary.
    """

    base_sql = '''
        SELECT count(*), coalesce(sum(d.centerline_length),0) length, coalesce(sum(d.segment_area), 0) area
        FROM dgos d
        LEFT JOIN dgo_metric_values dms ON d.fid = dms.dgo_id
        '''

    if owner is not None:
        base_sql += ' LEFT JOIN dgo_metric_values dmo ON d.fid = dmo.dgo_id'

    final_sql = add_where_clauses(base_sql, state, flow, owner)
    curs.execute(final_sql)
    dgo_count, dgo_length, dgo_area = curs.fetchone()

    output['dgo_count'] = dgo_count
    output['dgo_length_miles'] = dgo_length * METRES_TO_MILES
    output['dgo_area_acres'] = dgo_area * SQMETRES_TO_ACRES


def scrape_rcat_statistics(curs: sqlite3.Cursor, state: Dict[str, str], flow: Dict[str, str], owner: Dict[str, str], output: Dict) -> None:
    """
    Scrape statistics from the RCAT output. Note that by this point RCAT db should include several RME tables.
    The owner and flow filters are optional. The output of this function is to insert several RME statistics into the "data" dictionary.
    """

    base_sql = '''
        SELECT coalesce(sum(d.HistoricRiparianMean * d.segment_area), 0)         historic_riparian_area,
            coalesce(sum(d.FloodplainAccess * d.segment_area), 0)             floodplain_access_area,
            coalesce(
                sum(
                    max(
                        min(
                            dgos.low_lying_floodplain_prop + dgos.active_channel_prop,
                            FloodplainAccess,
                            min(
                                1,
                                RiparianDeparture
                            )
                        ),
                        active_channel_prop
                    )
                       ) * d.segment_area
            , 0)            active_area,
            coalesce(sum(CASE WHEN lui = 0 THEN d.segment_area ELSE 0 END), 0)             lui_zero_count
        FROM DGOAttributes d
            INNER JOIN dgos on dgos.level_path = d.level_path AND dgos.seg_distance = d.seg_distance
            INNER JOIN dgo_metric_values dms ON dgos.fid = dms.dgo_id
        '''

    if owner is not None:
        base_sql += ' INNER JOIN dgo_metric_values dmo ON dgos.fid = dmo.dgo_id'

    final_sql = add_where_clauses(base_sql, state, flow, owner)
    curs.execute(final_sql)
    hist_riparian_area, floodplain_access_area, active_area, lui_zero_area = curs.fetchone()

    output['hist_riparian_area'] = hist_riparian_area * SQMETRES_TO_ACRES
    output['floodplain_access_area'] = floodplain_access_area * SQMETRES_TO_ACRES
    output['active_area'] = active_area * SQMETRES_TO_ACRES
    output['lui_zero_area'] = lui_zero_area * SQMETRES_TO_ACRES


def add_where_clauses(base_sql: str, state: Dict[str, str], flow: Dict[str, str], owner: Dict[str, str]) -> str:
    """
    Add WHERE clauses to the SQL query based on the state, owner and flow.
    Note that owner is the only filter than can be None!
    """

    final_sql = base_sql + ' WHERE '

    s_clause = ','.join([f"'{s}'" for s in state['where_clause'].split(",")])
    final_sql += f'( dms.metric_id = 2 AND dms.metric_value IN ({s_clause}))'

    f_clause = ','.join([f"'{f}'" for f in flow['where_clause'].split(",")])
    final_sql += f' AND (d.FCode IN ({f_clause}))'

    if owner is not None:
        o_clause = ','.join([f"'{o}'" for o in owner['where_clause'].split(",")])
        final_sql += f' AND (dmo.metric_id = 1 AND dmo.metric_value IN ({o_clause}))'

    return final_sql


def download_file(rs_api: RiverscapesAPI, project_id: str, download_dir: str, regex: str) -> str:
    """
    Download files from a project on Data Exchange that match the regex string
    Return the path to the downloaded file
    """

    gpkg_path = get_matching_file(download_dir, regex)
    if gpkg_path is not None and os.path.isfile(gpkg_path):
        return gpkg_path

    rs_api.download_files(project_id, download_dir, [regex])

    gpkg_path = get_matching_file(download_dir, regex)

    # Cannot proceed with this HUC if the output GeoPackage is missing
    if gpkg_path is None or not os.path.isfile(gpkg_path):
        raise FileNotFoundError(f'Could not find output GeoPackage in {download_dir}')

    return gpkg_path


def get_matching_file(parent_dir: str, regex: str) -> str:
    """
    Get the path to the first file in the parent directory that matches the regex.
    Returns None if no file is found.
    This is used to check if the output GeoPackage has already been downloaded and
    to avoid downloading it again.
    """

    regex = re.compile(regex)
    for root, __dirs, files in os.walk(parent_dir):
        for file_name in files:
            # Check if the file name matches the regex
            if regex.match(file_name):
                return os.path.join(root, file_name)

    return None


def load_filters(output_db: str, table_name: str) -> Dict[str, Dict[str, str]]:
    '''
    Load the filters from the output database for a particular table.
    This is used for both ownerships and flows lookups
    '''

    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        curs.execute(f'SELECT name, id, where_clause FROM {table_name}')
        return {f[0]: {'id': f[1], 'where_clause': f[2]} for f in curs.fetchall()}


def continue_with_huc(huc: str, output_db: str) -> bool:
    '''
    Check if the HUC already exists in the output GeoPackage. 
    This is used to determine if the HUC has already been scraped and whether it
    can be skipped.
    '''

    if not os.path.isfile(output_db):
        return True

    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        curs.execute('SELECT huc10 FROM hucs WHERE huc10 = ? LIMIT 1', [huc])
        return curs.fetchone() is None

    return False


def create_output_db(output_db: str) -> None:
    """ 
    Build the output SQLite database by running the schema file.
    """

    # As a precaution, do not overwrite or delete the output database.
    # Force the user to delete it manually if they want to rebuild it.
    if os.path.isfile(output_db):
        return

    schema_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'packages', 'rme', 'rme', 'database')
    if not os.path.isdir(schema_dir):
        raise FileNotFoundError(f'Could not find database schema directory {schema_dir}')

    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        with open(os.path.join(schema_dir, 'rme_scrape_huc_statistics.sql'), encoding='utf-8') as sqlfile:
            sql_commands = sqlfile.read()
            curs.executescript(sql_commands)
            conn.commit()


def main():
    """
    Scrape RME projects
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('db_path', help='Path to the warehouse dump database', type=str)
    parser.add_argument('--delete', help='Whether or not to delete downloaded GeoPackages', type=bool, default=False)
    parser.add_argument('--huc_filter', help='HUC filter SQL prefix ("17%")', type=str, default='')
    args = dotenv.parse_args_env(parser)

    if not os.path.isfile(args.db_path):
        print(f'Data Exchange project dump database file not found: {args.db_path}')
        sys.exit(1)

    # Set up some reasonable folders to store things
    working_folder = args.working_folder  # os.path.join(args.working_folder, output_name)
    download_folder = os.path.join(working_folder, 'downloads')
    scraped_folder = working_folder  # os.path.join(working_folder, 'scraped')

    safe_makedirs(scraped_folder)
    log = Logger('Setup')
    log.setup(log_path=os.path.join(scraped_folder, 'rme-scrape.log'), log_level=logging.DEBUG)

    huc_filter = f" AND (huc10 LIKE ('{args.huc_filter}')) " if args.huc_filter and args.huc_filter != '.' else ''

    # Determine projects in the dumped warehouse database that have both RCAT and RME available
    with sqlite3.connect(args.db_path) as conn:
        curs = conn.cursor()
        curs.execute(f'''
            SELECT huc10, min(rme_project_id), min(rcat_project_id)
            FROM
            (
                SELECT huc10,
                    CASE WHEN project_type_id = 'rs_metric_engine' THEN project_id ELSE NULL END rme_project_id,
                    CASE WHEN project_type_id = 'rcat' then project_id ELSE NULL END             rcat_project_id
                FROM vw_conus_projects
                WHERE project_type_id IN ('rs_metric_engine', 'rcat')
                    AND tags = '2024CONUS'
            )
            GROUP BY huc10
            HAVING min(rme_project_id) IS NOT NULL
                AND min(rcat_project_id) IS NOT NULL
                {huc_filter}
            ''')
        projects = {row[0]: {
            'rme': row[1],
            'rcat': row[2]
        } for row in curs.fetchall()}

    if len(projects) == 0:
        log.info('No projects found in Data Exchange dump with both RCAT and RME')
        sys.exit(0)

    # Filters for debugging
    # projects = {'1701010111': projects['1701010111']}
    # projects = {key: val for key, val in projects.items() if key.startswith('17')}

    log.info(f'Found {len(projects)} RME projects in Data Exchange dump with both RME and RCAT')

    output_db = os.path.join(scraped_folder, 'rme_scrape_output.sqlite')
    create_output_db(output_db)

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_hucs(api, projects, download_folder, output_db, args.delete)

    log.info('Process complete')


if __name__ == '__main__':
    main()
