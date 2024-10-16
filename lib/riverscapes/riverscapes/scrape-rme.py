"""
RME scrape.

This script unpivots the DGO metrics from the RME output GeoPackages and stores them in a single output
feature class using the IGO points as geometries.

1) Searches Data Exchange for RME projects with the specified tags (and optional HUC filter)
2) Downloads the RME output GeoPackages
3) Scrapes the metrics from the RME output GeoPackages into a single output GeoPackage
4) Optionally deletes the downloaded GeoPackages
"""
from typing import Dict
import shutil
import re
import os
import sqlite3
import logging
import argparse
from osgeo import ogr, osr
from rsxml import dotenv, Logger, safe_makedirs
from riverscapes import RiverscapesAPI, RiverscapesSearchParams

# RegEx for finding the RME output GeoPackages
RME_OUTPUT_GPKG_REGEX = r'.*riverscapes_metrics\.gpkg'


def scrape_rme(rs_api: RiverscapesAPI, search_params: RiverscapesSearchParams, download_dir: str, output_gpkg: str, delete_downloads: bool) -> None:
    """
    Download RME output GeoPackages from Data Exchange and scrape the metrics into a single GeoPackage
    """

    log = Logger('Scrape RME')
    gpkg_driver = ogr.GetDriverByName("GPKG")
    target_ds = None
    target_layer = None
    metric_col_types = {}
    metric_ids = {}

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

            if not os.path.isfile(output_gpkg) or metric_ids == {}:

                # First, there are some fields from the DGO feature class that we want as well
                metric_col_types['fcode'] = 'INTEGER'
                metric_col_types['segment_area'] = 'REAL'
                metric_col_types['centerline_length'] = 'REAL'

                # Now get the metric columns from the RME GeoPackage
                with sqlite3.connect(rme_gpkg) as rme_conn:
                    rme_curs = rme_conn.cursor()
                    rme_curs.execute('''
                        SELECT metric_id, field_name, data_type
                        FROM metrics
                        WHERE (is_active <> 0)
                            AND (field_name is not null)
                            AND (data_type is not null)
                    ''')
                    for row in rme_curs.fetchall():
                        metric_col_types[row[1]] = row[2]
                        metric_ids[row[0]] = row[1]

                # Create the output GeoPackage with the IGOs feature class
                if not os.path.isfile(output_gpkg):
                    create_gpkg(output_gpkg, metric_col_types)

            if target_ds is None:
                target_ds = gpkg_driver.Open(output_gpkg, 1)  # 1 means read/write mode
                target_layer = target_ds.GetLayer('igos')

            scrape_huc(huc10, rme_gpkg, metric_ids, gpkg_driver, target_layer)

            target_ds.ExecuteSQL(f"INSERT INTO hucs (huc, rme_project_id) VALUES ('{huc10}', '{project.id}')")

        except Exception as e:
            log.error(f'Error scraping HUC {huc10}: {e}')

        if delete_downloads is True and os.path.isdir(huc_dir):
            try:
                log.info(f'Deleting download directory {huc_dir}')
                shutil.rmtree(huc_dir)
            except Exception as e:
                log.error(f'Error deleting download directory {huc_dir}: {e}')

    target_layer = None
    target_ds = None


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
        if curs.fetchone() is None:
            return True
        else:
            log = Logger('Scrape RME')
            log.info(f'HUC {huc10} already scraped. Skipping...')

    return False


def create_gpkg(output_gpkg: str, metric_cols: Dict[str, str]) -> None:
    '''
    Creates the output GeoPackage with the IGOs feature class
    uses DGO metrics as fields
    '''

    driver = ogr.GetDriverByName("GPKG")
    data_source = driver.CreateDataSource(output_gpkg)
    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(4326)
    layer = data_source.CreateLayer("igos", spatial_ref, ogr.wkbPoint)

    field_huc = ogr.FieldDefn("huc", ogr.OFTString)
    field_huc.SetWidth(10)
    layer.CreateField(field_huc)

    field_level_path = ogr.FieldDefn("level_path", ogr.OFTString)
    field_level_path.SetWidth(50)
    layer.CreateField(field_level_path)

    field_seg_distance = ogr.FieldDefn("seg_distance", ogr.OFTInteger)
    layer.CreateField(field_seg_distance)

    for field_name, field_type in metric_cols.items():
        oft_type = ogr.OFTString
        if field_type.lower() == 'integer':
            oft_type = ogr.OFTInteger
        elif field_type.lower() == 'real':
            oft_type = ogr.OFTReal

        field = ogr.FieldDefn(field_name, oft_type)
        layer.CreateField(field)

    layer = None
    data_source = None

    # Create the hucs table to keep track of progress
    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()
        curs.execute('''
            CREATE TABLE hucs (
                huc TEXT PRIMARY KEY NOT NULL,
                rme_project_id TEXT,
                scraped_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ''')

    print(f'GeoPackage created with the "igos" point layer: {output_gpkg}')


def scrape_huc(huc10: str, rme_gpkg: str, metric_ids: Dict[int, str], driver: ogr.Driver, target_layer: ogr.Layer) -> None:
    '''
    Perform the actual scrape on a single HUC
    Creates new IGO features with DGO metrics as fields
    '''

    with sqlite3.connect(rme_gpkg) as rme_conn:
        rme_curs = rme_conn.cursor()

        source_ds = driver.Open(rme_gpkg, 0)
        source_layer = source_ds.GetLayer('igos')

        # Get the feature definition from the target layer
        target_layer_defn = target_layer.GetLayerDefn()

        # Loop over features in the source layer
        for source_feature in source_layer:

            level_path = source_feature.GetField('level_path')
            seg_distance = source_feature.GetField('seg_distance')

            # Create a new feature for the target layer using the target layer's definition
            target_feature = ogr.Feature(target_layer_defn)

            # Copy the geometry from the source feature to the target feature
            geom = source_feature.GetGeometryRef()
            target_feature.SetGeometry(geom.Clone())

            target_feature.SetField('huc', huc10)
            target_feature.SetField('level_path', level_path)
            target_feature.SetField('seg_distance', seg_distance)

            # Retrieve the DGO metric values and store them on the IGO feature
            rme_curs.execute('''
                SELECT dmv.metric_id, dmv.metric_value
                FROM dgos d INNER JOIN dgo_metric_values dmv ON d.fid = dmv.dgo_id
                WHERE (dmv.metric_value IS NOT NULL)
                    AND (d.level_path = ?)
                    AND (d.seg_distance = ?)''', [level_path, seg_distance])

            for row in rme_curs.fetchall():
                target_feature.SetField(metric_ids[row[0]], row[1])

            # and now the DGO fields
            rme_curs.execute('''
                SELECT fcode, segment_area, centerline_length
                FROM dgos
                WHERE (level_path = ?)
                    AND (seg_distance = ?)
                LIMIT 1''', [level_path, seg_distance])
            dgo_row = rme_curs.fetchone()
            if dgo_row is not None:
                target_feature.SetField('fcode', dgo_row[0])
                target_feature.SetField('segment_area', dgo_row[1])
                target_feature.SetField('centerline_length', dgo_row[2])

            # Add the feature to the target layer
            target_layer.CreateFeature(target_feature)
            target_feature = None


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
