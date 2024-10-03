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
import sqlite3
import logging
import argparse
from rsxml import dotenv, Logger, safe_makedirs
from riverscapes import RiverscapesAPI, scrape_huc_statistics
# from riverscapes.scrape_huc_statistics import scrape_huc_statistics, create_output_db

# RegEx for finding RME and RCAT output GeoPackages
RME_OUTPUT_GPKG_REGEX = r'.*riverscapes_metrics\.gpkg'
RCAT_OUTPUT_GPKG_REGEX = r'.*rcat\.gpkg'


def scrape_hucs_batch(rs_api: RiverscapesAPI,  projects: Dict[str, str], download_dir: str, output_db: str, delete_downloads: bool) -> None:
    """
    Loop over all the projects, download the RME and RCAT output GeoPackages, and scrape the statistics
    """

    log = Logger('Scrape HUC Batch')

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

            scrape_huc_statistics(huc, rme_gpkg, rcat_gpkg, output_db)

        except Exception as e:
            log.error(f'Error scraping HUC {huc}: {e}')

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


def main():
    """
    Scrape RME projects for multiple HUCs specified by a HUC filter.
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

    log.info(f'Found {len(projects)} RME projects in Data Exchange dump with both RME and RCAT')

    output_db = os.path.join(scraped_folder, 'rme_scrape_output.sqlite')
    create_output_db(output_db)

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_hucs_batch(api, projects, download_folder, output_db, args.delete)

    log.info('Process complete')


if __name__ == '__main__':
    main()
