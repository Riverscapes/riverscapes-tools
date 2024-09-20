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
import argparse
import uuid
from rsxml import dotenv, Logger, safe_makedirs
from riverscapes import RiverscapesAPI

# RegEx for finding the RME output GeoPackages
RME_OUTPUT_GPKG_REGEX = r'.*riverscapes_metrics\.gpkg'
RCAT_OUTPUT_GPKG_REGEX = r'.*rcat\.gpkg'

METRES_TO_MILES = 0.000621371
SQMETRES_TO_ACRES = 0.000247105


def scrape_rme(rs_api: RiverscapesAPI,  projects: Dict[str, str], download_dir: str, output_db: str, delete_downloads: bool) -> None:

    log = Logger('Scrape RME')

    owners = load_filters(output_db, 'owners')
    flows = load_filters(output_db, 'flows')

    for huc, project_ids in projects.items():
        try:
            if continue_with_huc(huc, output_db) is not True:
                continue

            log.info(f'Scraping RME metrics for HUC {huc}')
            huc_dir = os.path.join(download_dir, huc)
            safe_makedirs(huc_dir)

            rme_guid = project_ids['rme']
            rme_gpkg = download_file(rs_api, rme_guid, os.path.join(huc_dir, 'rme'), RME_OUTPUT_GPKG_REGEX)

            rcat_guid = project_ids['rcat']
            rcat_gpkg = download_file(rs_api, rcat_guid, os.path.join(huc_dir, 'rcat'), RCAT_OUTPUT_GPKG_REGEX)
            rcat_gpkg_copy = copy_file_with_unique_name(rcat_gpkg)

            huc_metrics = []

            with sqlite3.connect(rme_gpkg) as rme_conn:
                rme_curs = rme_conn.cursor()

                with sqlite3.connect(rcat_gpkg_copy) as rcat_conn:
                    rcat_curs = rcat_conn.cursor()

                    copy_table_between_cursors(rme_curs, rcat_curs, 'dgo_metric_values')
                    copy_table_between_cursors(rme_curs, rcat_curs, 'dgos')
                    rcat_conn.commit()  # so we can test queries in DataGrip

                    for __owner_name, owner_data in owners.items():
                        for __flow_name, flow_data in flows.items():

                            data = {
                                'owner_id': owner_data['id'],
                                'flow_id': flow_data['id'],
                                'huc10': huc,
                                'dgo_count': None,
                                'dgo_area_acres': None,
                                'dgo_length_miles': None,
                                'active_area': None,
                                'floodplain_access_area': None,
                                'lui_zero_count': None,
                                'hist_riparian_area': None,
                            }

                            scrape_rme_statistics(rme_curs, owner_data, flow_data, data)
                            scrape_rcat_statistics(rcat_curs, owner_data, flow_data, data)
                            huc_metrics.append(data)

            # Store the output HUC metrics
            keys = huc_metrics[0].keys()
            with sqlite3.connect(output_db) as conn:
                curs = conn.cursor()
                curs.execute('INSERT INTO hucs (huc10) VALUES (?)', [huc])
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
    # Get the folder and original file name
    folder = os.path.dirname(file_path)
    original_filename = os.path.basename(file_path)

    # Split the filename into name and extension
    name, ext = os.path.splitext(original_filename)

    # Generate a unique filename using uuid
    unique_filename = f"{name}_{uuid.uuid4().hex}{ext}"
    new_file_path = os.path.join(folder, unique_filename)

    # Copy the file to the new file path
    shutil.copy2(file_path, new_file_path)

    print(f"File copied to: {new_file_path}")
    return new_file_path


def copy_table_between_cursors(src_cursor, dest_cursor, table_name):
    # Get table schema from the source database
    src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    create_table_sql = src_cursor.fetchone()[0]

    # Create the table in the destination database
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


def scrape_rme_statistics(curs: sqlite3.Cursor, owner: Dict[str, str], flow: Dict[str, str], data: Dict) -> None:

    base_sql = '''
      select count(*), coalesce(sum(d.centerline_length),0) length, coalesce(sum(d.segment_area), 0) area
        from dgos d
        left join dgo_metric_values dmv on d.fid = dmv.dgo_id
        '''

    final_sql = add_where_clauses(base_sql, owner, flow)
    curs.execute(final_sql)
    dgo_count, dgo_length, dgo_area = curs.fetchone()

    data['dgo_count'] = dgo_count
    data['dgo_length_miles'] = dgo_length * METRES_TO_MILES
    data['dgo_area_acres'] = dgo_area * SQMETRES_TO_ACRES


def scrape_rcat_statistics(curs: sqlite3.Cursor, owner: Dict[str, str], flow: Dict[str, str], data: Dict) -> None:

    base_sql = '''
select coalesce(sum(d.HistoricRiparianMean * d.segment_area), 0)         historic_riparian_area,
       coalesce(sum(d.FloodplainAccess * d.segment_area), 0)             floodplain_access_area,
       coalesce(sum(min(dgos.low_lying_floodplain_prop, dgos.active_channel_prop, FloodplainAccess,
                        min(1, RiparianDeparture)) * d.segment_area), 0) active_area,
       coalesce(sum(CASE WHEN lui = 0 THEN 1 ELSE 0 END), 0)             lui_zero_count
from DGOAttributes d
         inner join dgos
                    on dgos.level_path = d.level_path and dgos.seg_distance = d.seg_distance
         inner join dgo_metric_values dmv on dgos.fid = dmv.dgo_id
    '''

    final_sql = add_where_clauses(base_sql, owner, flow)
    curs.execute(final_sql)
    hist_riparian_area, floodplain_access_area, active_area, lui_zero_count = curs.fetchone()

    data['hist_riparian_area'] = hist_riparian_area * SQMETRES_TO_ACRES
    data['floodplain_access_area'] = floodplain_access_area * SQMETRES_TO_ACRES
    data['active_area'] = active_area * SQMETRES_TO_ACRES
    data['lui_zero_count'] = lui_zero_count


def add_where_clauses(base_sql: str, owner: Dict[str, str], flow: Dict[str, str]) -> str:

    final_sql = base_sql
    if owner is not None or flow is not None:
        final_sql += ' WHERE'

        if owner is not None:
            # make a comma separated list wrapping each item in single quotes
            o_clause = ','.join([f"'{o}'" for o in owner['where_clause'].split(",")])
            final_sql += f' dmv.metric_id = 1 AND dmv.metric_value IN ({o_clause})'

        if flow is not None:
            if owner is not None:
                final_sql += ' AND'

            # make a comma separated list wrapping each item in single quotes
            f_clause = ','.join([f"'{f}'" for f in flow['where_clause'].split(",")])
            final_sql += f' d.FCode IN ({f_clause})'

    return final_sql


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


def load_filters(output_db: str, table_name: str) -> Dict[str, Dict[str, str]]:
    '''
    Load the filters from the output database for a particular table
    '''

    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        curs.execute(f'SELECT name, id, where_clause FROM {table_name}')
        return {f[0]: {'id': f[1], 'where_clause': f[2]} for f in curs.fetchall()}


def continue_with_huc(huc: str, output_db: str) -> bool:
    '''
    Check if the HUC already exists in the output GeoPackage
    '''

    if not os.path.isfile(output_db):
        return True

    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        curs.execute('SELECT huc10 FROM hucs WHERE huc10 = ? LIMIT 1', [huc])
        return curs.fetchone() is None

    return False


def create_output_db(output_db: str) -> None:

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
            ''')
        projects = {row[0]: {
            'rme': row[1],
            'rcat': row[2]
        } for row in curs.fetchall()}

    if len(projects) == 0:
        log.info('No RME projects found in Data Exchange dump')
        sys.exit(0)

    projects = {'1701010111': projects['1701010111']}

    log.info(f'Found {len(projects)} RME projects in Data Exchange dump')

    output_db = os.path.join(scraped_folder, 'rme_scrape_output.db')
    create_output_db(output_db)

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_rme(api, projects, download_folder, output_db, False)

    log.info('Process complete')


if __name__ == '__main__':
    main()
