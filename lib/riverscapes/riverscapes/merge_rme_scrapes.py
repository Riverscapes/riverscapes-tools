"""
Scrapes RME and RCAT output GeoPackages from Data Exchange and extracts statistics for each HUC.
Produced for the BLM 2024 September analysis of 2024 CONUS RME projects.

This script assumes that the `scrape_huc_statistics.py` script has been run on each RME project.
The scrape_huc_statistics.py script extracts statistics from the RME and RCAT output GeoPackages
and generates a new 'rme_scrape.sqlite' file in the project. This is then uploaded into the 
project on the Data Exchange. 
"""
import shutil
import re
import os
import sqlite3
import logging
import argparse
from rsxml import dotenv, Logger, safe_makedirs
from riverscapes import RiverscapesAPI, RiverscapesSearchParams

# RegEx for finding RME and RCAT output GeoPackages
RME_SCRAPE_GPKG_REGEX = r'.*rme_scrape\.sqlite'


def merge_rme_scrapes(rs_api: RiverscapesAPI, search_params: RiverscapesSearchParams, download_dir: str, output_curs: sqlite3.Cursor, delete_downloads: bool) -> None:
    """
    Loop over all the projects, download the RME and RCAT output GeoPackages, and scrape the statistics
    """

    log = Logger('Merge RME Scrapes')

    # Create a timedelta object with a difference of 1 day
    for project, _stats, _searchtotal in rs_api.search(search_params, progress_bar=True, page_size=100):

        # Attempt to retrieve the huc10 from the project metadata if it exists
        huc10 = None
        try:
            for key in ['HUC10', 'huc10', 'HUC', 'huc']:
                if key in project.project_meta:
                    value = project.project_meta[key]
                    huc10 = value if len(value) == 10 else None
                    break

            output_curs.execute('SELECT huc10 FROM hucs WHERE huc10 = ? LIMIT 1', [huc10])
            if output_curs.fetchone() is not None:
                continue

            huc_dir = os.path.join(download_dir, huc10)
            rme_gpkg = download_file(rs_api, project.id, os.path.join(huc_dir, 'rme'), RME_SCRAPE_GPKG_REGEX)

            with sqlite3.connect(rme_gpkg) as rme_conn:
                rme_conn.execute('PRAGMA foreign_keys = ON')
                rme_curs = rme_conn.cursor()

                # Keep track of the HUCs that have been scraped
                output_curs.execute('INSERT INTO hucs (huc10) VALUES (?)', [huc10])

                # Copy the metrics table from the RME GeoPackage to the output database
                copy_table_between_cursors(rme_curs, output_curs, 'metrics', False)

        except Exception as e:
            log.error(f'Error scraping HUC {huc10}: {e}')

        if delete_downloads is True and os.path.isdir(huc_dir):
            try:
                log.info(f'Deleting download directory {huc_dir}')
                shutil.rmtree(huc_dir)
            except Exception as e:
                log.error(f'Error deleting download directory {huc_dir}: {e}')


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


def copy_table_between_cursors(src_cursor, dest_cursor, table_name, create_table: bool):
    """
    Copy a table structure and data from the source cursor to destination cursor
    """

    if create_table is True:
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


def create_output_db(output_db: str, delete: bool) -> None:
    """ 
    Build the output SQLite database by running the schema file.
    """
    log = Logger('Create Output DB')

    # As a precaution, do not overwrite or delete the output database.
    # Force the user to delete it manually if they want to rebuild it.
    if os.path.isfile(output_db):
        if delete is True:
            log.info(f'Deleting existing output database {output_db}')
            os.remove(output_db)
        else:
            log.info('Output database already exists. Skipping creation.')
            return

    schema_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'packages', 'rme', 'rme', 'database')
    if not os.path.isdir(schema_dir):
        raise FileNotFoundError(f'Could not find database schema directory {schema_dir}')

    safe_makedirs(os.path.dirname(output_db))

    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        log.info('Creating output database schema')
        with open(os.path.join(schema_dir, 'rme_scrape_huc_statistics.sql'), encoding='utf-8') as sqlfile:
            sql_commands = sqlfile.read()
            curs.executescript(sql_commands)
            conn.commit()

    log.info(f'Output database at {output_db}')


def main():
    """
    Search the Data Exchange for RME projects that have the RME scrape and then
    merge the contents into a single output database.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('tags', help='Data Exchange tags to search for projects', type=str)
    parser.add_argument('--delete', help='Whether or not to delete downloaded GeoPackages',  action='store_true', default=False)
    parser.add_argument('--huc_filter', help='HUC filter SQL prefix ("17%")', type=str, default='')
    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    working_folder = args.working_folder
    download_folder = os.path.join(working_folder, 'downloads')
    output_db = os.path.join(working_folder, 'rme_scrape_merge.sqlite')
    create_output_db(output_db, False)

    safe_makedirs(working_folder)
    log = Logger('Setup')
    log.setup(log_path=os.path.join(working_folder, 'rme-scrape-merge.log'), log_level=logging.DEBUG)

    # Data Exchange Search Params
    search_params = RiverscapesSearchParams({
        'tags': args.tags.split(','),
        'projectTypeId': 'rs_metric_engine',
    })

    if args.huc_filter != '':
        search_params.meta = {
            "HUC": args.huc_filter
        }

    with sqlite3.connect(output_db) as output_conn:
        output_conn.execute('PRAGMA foreign_keys = ON')
        output_curs = output_conn.cursor()
        with RiverscapesAPI(stage=args.stage) as api:
            merge_rme_scrapes(api, search_params, download_folder, output_curs, args.delete)

    log.info('Process complete')


if __name__ == '__main__':
    main()
