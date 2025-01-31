"""
RME scrape.

This script unpivots the DGO metrics from the RME output GeoPackages and stores them in a single output
feature class using the IGO points as geometries.

1) Searches Data Exchange for RME projects with the specified tags (and optional HUC filter)
2) Downloads the RME output GeoPackages
3) Scrapes the metrics from the RME output GeoPackages into a single output GeoPackage
4) Optionally deletes the downloaded GeoPackages
"""
from typing import Dict, Tuple, List
import shutil
import re
import os
import sys
import subprocess
import sqlite3
import logging
import argparse
import json
from osgeo import ogr, osr
from datetime import datetime
import xml.etree.ElementTree as ET
from osgeo import gdal, ogr
from rsxml import dotenv, Logger, safe_makedirs
from riverscapes import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject

# RegEx for finding the RME output GeoPackages
RME_OUTPUT_GPKG_REGEX = r'.*riverscapes_metrics\.gpkg'


def scrape_rme(rs_api: RiverscapesAPI, search_params: RiverscapesSearchParams, download_dir: str, output_gpkg: str, delete_downloads: bool) -> None:
    """
    Download RME output GeoPackages from Data Exchange and scrape the metrics into a single GeoPackage
    """

    log = Logger('Scrape RME')
    gpkg_driver = ogr.GetDriverByName("GPKG")

    file_regex_list = [RME_OUTPUT_GPKG_REGEX]
    file_regex_list.append(r'project\.rs\.xml')
    file_regex_list.append(r'project_bounds\.geojson')
    file_regex_list.append(r'.*\.log')

    for project, _stats, search_total, _prg in rs_api.search(search_params, progress_bar=True):
        if search_total < 1:
            raise ValueError(f'No projects found for search params: {search_params}')

        # Attempt to retrieve the huc10 from the project metadata if it exists
        huc10 = None
        for key in ['HUC10', 'huc10', 'HUC', 'huc']:
            if key in project.project_meta:
                value = project.project_meta[key]
                huc10 = value if len(value) == 10 else None
                break

        if continue_with_huc(huc10, output_gpkg) is not True:
            first_project_xml = False
            continue

        download_path = os.path.join(download_dir, project.id)
        rs_api.download_files(project.id, download_path, file_regex_list)

        input_gpkg = os.path.join(download_path, project.id, 'outputs', 'riverscapes_metrics.gpkg')
        if not os.path.isfile(input_gpkg):
            raise FileNotFoundError(f'No RME output GeoPackage found for project at {input_gpkg}')

        # Copy IGOs from the input RME GeoPackages to the output GeoPackage
        # This will also create the GeoPackage if it doesn't exist
        cmd = f'ogr2ogr -f GPKG -makevalid -append  -nln rme_igos "{output_gpkg}" "{input_gpkg}" rme_igos'
        log.debug(f'EXECUTING: {cmd}')
        subprocess.call([cmd], shell=True, cwd=os.path.dirname(output_gpkg))
        first_project_xml = False

        copy_dgo_values(input_gpkg, output_gpkg, huc10, gpkg_driver)

        # Record that the HUC is processed, so that the script can continue where it left off
        track_huc(output_gpkg, huc10)


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
        if curs.fetchone() is None:
            return True
        else:
            log = Logger('Scrape RME')
            log.info(f'HUC {huc10} already scraped. Skipping...')

    return False


def track_huc(output_gpkg: str, huc: str) -> None:
    '''
    Tract the progress of scraping a HUC
    '''

    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()

        curs.execute('''
            CREATE TABLE IF NOT EXISTS hucs (
                huc TEXT PRIMARY KEY NOT NULL,
                rme_project_id TEXT,
                scraped_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        curs.execute('INSERT INTO hucs (huc) VALUES (?)', [huc])
        conn.commit()


def copy_dgo_values(input_gpkg: str, output_gpkg: str, huc10: str, gpkg_driver: ogr.Driver) -> None:
    '''
    Copy the DGO values from the input GeoPackage to the output GeoPackage
    '''

    # Open the input GeoPackage
    input_ds = gpkg_driver.Open(input_gpkg, 0)  # 0 means read-only mode
    if input_ds is None:
        raise FileNotFoundError(f'Unable to open input GeoPackage: {input_gpkg}')

    # Open the output GeoPackage
    target_ds = gpkg_driver.Open(output_gpkg, 1)  # 1 means read/write mode
    if target_ds is None:
        raise FileNotFoundError(f'Unable to open output GeoPackage: {output_gpkg}')

    # Get the input layer
    input_layer = input_ds.GetLayerByName('rme_dgos')
    if input_layer is None:
        raise ValueError('Input GeoPackage does not contain the "rme_dgos" layer')

    # Get the output layer
    target_layer = target_ds.GetLayerByName('rme_igos')
    if target_layer is None:
        raise ValueError('Output GeoPackage does not contain the "rme_igos" layer')

    # Get the list of columns and their data types for the 'rme_dgos' layer
    input_layer_defn = input_layer.GetLayerDefn()
    dgo_cols = {input_layer_defn.GetFieldDefn(i).GetName(): input_layer_defn.GetFieldDefn(i).GetType() for i in range(input_layer_defn.GetFieldCount())}

    # Get the list of columns and their data types for the 'rme_igos' layer
    target_layer_defn = target_layer.GetLayerDefn()
    igo_cols = {target_layer_defn.GetFieldDefn(i).GetName(): target_layer_defn.GetFieldDefn(i).GetType() for i in range(target_layer_defn.GetFieldCount())}

    # Find the columns in rme_dgos that are not in rme_igos
    required_dgo_cols = {k: v for k, v in dgo_cols.items() if k not in igo_cols}

    # Add the required columns to the output layer
    for field_name, field_type in required_dgo_cols.items():
        target_ds.ExecuteSQL(f"ALTER TABLE rme_igos ADD COLUMN {field_name} {field_type};")

    # Copy the DGO values to the output layer
    for input_feature in input_layer:
        level_path = input_feature.GetField('level_path')
        seg_distance = input_feature.GetField('seg_distance')

        if level_path is None or seg_distance is None:
            continue

        for field_name in required_dgo_cols.keys():
            input_value = input_feature.GetField(field_name)
            target_ds.ExecuteSQL(f"UPDATE rme_igos SET {field_name} = {input_value} WHERE id = {input_feature.GetField('id')}")


def configure_gpkg(gpkg_driver, output_gpkg: str) -> Dict[str, str]:
    '''
    Assumes the output GeoPackage has already been created.
    1. Adds the 'hucs' table to keep track of progress
    2. Adds the DGO columns to the 'rme_igos' layer
    '''

    # Create the hucs table to keep track of progress
    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()

        # Get the list of columns and their data types for the 'rme_igos' layer
        curs.execute('PRAGMA table_info(rme_igos)')
        igo_cols = {row['name']: row['type'] for row in curs.fetchall()}

        # Get the list of columns for the rme_dgos layer
        curs.execute('PRAGMA table_info(rme_dgos)')
        dgo_cols = {row['name']: row['type'] for row in curs.fetchall()}

        # Find the columns in rme_dgos that are not in rme_igos
        required_dgo_cols = {k: v for k, v in dgo_cols.items() if k not in igo_cols}

        curs.execute('''
            CREATE TABLE hucs (
                huc TEXT PRIMARY KEY NOT NULL,
                rme_project_id TEXT,
                scraped_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ''')

    target_ds = gpkg_driver.Open(output_gpkg, 1)  # 1 means read/write mode
    for field_name, field_type in required_dgo_cols.items():
        target_ds.ExecuteSQL(f"ALTER TABLE rme_igos ADD COLUMN {field_name} {field_type};")
    target_ds = None

    print(f'GeoPackage created with the "igos" point layer: {output_gpkg}')

    return required_dgo_cols


def main():
    '''
    Scrape RME projects. Combine IGOs with their geometries. Include DGO metrics only.
    '''

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

    # Optional HUC filter
    if args.huc_filter != '' and args.huc_filter != '.':
        search_params.meta = {
            "HUC": args.huc_filter
        }

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_rme(api, search_params, download_folder, output_gpkg, args.delete)

    log.info('Process complete')


if __name__ == '__main__':
    main()
