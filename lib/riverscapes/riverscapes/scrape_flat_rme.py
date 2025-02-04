"""
RME scrape.

This script unpivots the DGO metrics from the RME output GeoPackages and stores them in a single output
feature class using the IGO points as geometries.

1) Searches Data Exchange for RME projects with the specified tags (and optional HUC filter)
2) Downloads the RME output GeoPackages
3) Scrapes the metrics from the RME output GeoPackages into a single output GeoPackage
4) Optionally deletes the downloaded GeoPackages
"""
from typing import Dict, List
import shutil
import os
import subprocess
import sqlite3
import logging
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime
import semver
from osgeo import ogr
from rsxml import dotenv, Logger, safe_makedirs
from rsxml.project_xml import Project, ProjectBounds, Coords, BoundingBox, Realization, MetaData, Meta, Geopackage, GeopackageLayer, GeoPackageDatasetTypes
from riverscapes import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject
from riverscapes.merge_projects import union_polygons

# RegEx for finding the RME output GeoPackages
RME_OUTPUT_GPKG_REGEX = r'.*riverscapes_metrics\.gpkg'
MINIMUM_RME_VERSION = '2.0.0'
SCRAPE_VERSION = '1.0.0'


def scrape_rme(rs_api: RiverscapesAPI, rs_stage: str, search_params: RiverscapesSearchParams, project_name: str, download_dir: str, output_gpkg: str, delete_downloads: bool, min_rme_version: str) -> None:
    """
    Download RME output GeoPackages from Data Exchange and scrape the metrics into a single GeoPackage
    """

    log = Logger('Scrape RME')
    gpkg_driver = ogr.GetDriverByName("GPKG")
    bounds_geojson_files = []

    # This is a temporary location to store the bounds GeoJSON files
    # so that we can delete the bulky project download folder but still have the bounds
    # for unioning them at the end of the process.
    bounds_geojson_dir = os.path.join(download_dir, 'bounds_geojson_files')
    safe_makedirs(bounds_geojson_dir)

    file_regex_list = [RME_OUTPUT_GPKG_REGEX]
    file_regex_list.append(r'project\.rs\.xml')
    file_regex_list.append(r'project_bounds\.geojson')
    # file_regex_list.append(r'.*\.log')

    for project, _stats, search_total, _prg in rs_api.search(search_params, progress_bar=True):
        project: RiverscapesProject = project
        if search_total < 1:
            raise ValueError(f'No projects found for search params: {search_params}')

        # Attempt to retrieve the huc10 from the project metadata if it exists
        huc10 = get_metadata_value(project.project_meta, ['HUC10', 'huc10', 'HUC', 'huc'], 10)
        rme_version = get_metadata_value(project.project_meta, ['Model Version', 'ModelVersion', 'model_version', 'modelversion'])

        # Create a semver and ensure the model version is greater or equal to 2.1.1
        if rme_version is not None:
            if semver.compare(rme_version, min_rme_version) < 0:
                log.warning(f'RME version {rme_version} is less than the minimum version {min_rme_version}')
                continue
        else:
            log.warning('No Model Version found in project metadata')
            continue

        if continue_with_huc(huc10, output_gpkg) is not True:
            continue

        download_path = os.path.join(download_dir, project.id)
        rs_api.download_files(project.id, download_path, file_regex_list)

        input_gpkg = os.path.join(download_path, 'outputs', 'riverscapes_metrics.gpkg')
        if not os.path.isfile(input_gpkg):
            log.warning(f'No RME output GeoPackage found for project at {input_gpkg}')
            continue

        safe_makedirs(os.path.dirname(output_gpkg))

        input_igo_layer_name = get_layer_name(input_gpkg, ['rme_igos', 'igos'])

        # Get the project bounds GeoJSON file
        project_xml_path = os.path.join(download_path, 'project.rs.xml')
        if not os.path.isfile(project_xml_path):
            log.warning(f'No project.rs.xml file found for project at {project_xml_path}')
            continue

        # Get the project bounds file and copy it to a temporary location for unioning.
        tree = ET.parse(project_xml_path)
        nodBounds = tree.find('.//ProjectBounds/Path')
        if not nodBounds is None:
            project_bounds_path = os.path.join(os.path.dirname(project_xml_path), nodBounds.text)
            if os.path.isfile(project_bounds_path):
                temp_bounds_path = os.path.join(bounds_geojson_dir, f'{project.id}_bounds.geojson')
                if os.path.isfile(temp_bounds_path):
                    os.remove(temp_bounds_path)
                shutil.copyfile(project_bounds_path, temp_bounds_path)
                bounds_geojson_files.append(temp_bounds_path)

        # Copy IGOs from the input RME GeoPackages to the output GeoPackage
        # This will also create the GeoPackage if it doesn't exist
        cmd = f'ogr2ogr -f GPKG -makevalid -append  -nln igos "{output_gpkg}" "{input_gpkg}" {input_igo_layer_name}'
        log.debug(f'EXECUTING: {cmd}')
        subprocess.call([cmd], shell=True, cwd=os.path.dirname(output_gpkg))

        copy_dgo_values(input_gpkg, output_gpkg, gpkg_driver, huc10)

        # Record that the HUC is processed, so that the script can continue where it left off
        track_huc(output_gpkg, project.id, huc10)

        # Cleanup the download folder
        if delete_downloads:
            shutil.rmtree(download_path)

    # build union of project bounds
    output_bounds_path = os.path.join(os.path.dirname(output_gpkg), 'project_bounds.geojson')
    centroid, bounding_rect = union_polygons(bounds_geojson_files, output_bounds_path)

    project = Project(
        name=project_name,
        proj_path='project.rs.xml',
        project_type='igos',
        description='''This project was generated by scraping RME projects together,
            using the scrape_flat_rme.py script. Only the IGO feature class is retained, and it is 
            enriched with any columns from the rme_dgos feature class that don't already exist on the
            rme_igos feature class.
            
            The project bounds are the union of the bounds of the individual projects.''',
        bounds=ProjectBounds(
            centroid=Coords(centroid[0], centroid[1]),
            bounding_box=BoundingBox(bounding_rect[0], bounding_rect[2], bounding_rect[1], bounding_rect[3]),
            filepath=os.path.basename(output_bounds_path),
        ),
        meta_data=MetaData([
            Meta('Created On', str(datetime.now().isoformat()), type='isodate'),
            Meta('ModelVersion', SCRAPE_VERSION)
        ]),
        realizations=[
            Realization(
                xml_id='igo_scrape_01',
                name='Realization',
                product_version='1.0.0',
                date_created=datetime.now(), datasets=[
                    Geopackage(
                        xml_id='igo_geopackage',
                        name='IGOs GeoPackage',
                        path=os.path.basename(output_gpkg),
                        description='''The one and only GeoPackage produced by the RME scrape. There is one feature class in this GeoPackage, the IGOs, which contains the IGOs from all the scraped projects.
                        There is one non-spatial table called hucs, which tracks the progress of the scrape by recording the HUCs that have been processed.''',
                        layers=[
                            GeopackageLayer(lyr_name='igos',
                                            name='IGO Points',
                                            ds_type=GeoPackageDatasetTypes.VECTOR,
                                            description='IGO points with all IGO and DGO metrics.')
                        ]
                    )
                ]
            )
        ]
    )

    scrape_project_xml = os.path.join(os.path.dirname(output_bounds_path), 'project.rs.xml')
    project.write(scrape_project_xml)
    log.info(f'Scrape project.rs.xml file written to {scrape_project_xml}')


def get_metadata_value(metadata: Dict[str, str], keys: List[str], required_length: int = None) -> str:

    for key in keys:
        if key in metadata:
            value = metadata[key]
            if value is not None:
                if required_length is not None:
                    if len(value) == required_length:
                        return value
                    else:
                        raise ValueError(f'Value for key {key} is not the required length of {required_length}')
                return value

    raise ValueError(f'No Metadata found with one of the following keys: {keys}')


def get_layer_name(gpkg_path: str, layer_names: List[str]) -> str:
    '''
    Get the name of the first layer in the GeoPackage that matches one of the layer names
    '''

    gpkg_driver = ogr.GetDriverByName("GPKG")
    ds = gpkg_driver.Open(gpkg_path, 0)  # 0 means read-only mode
    if ds is None:
        raise FileNotFoundError(f'Unable to open GeoPackage: {gpkg_path}')

    for layer_name in layer_names:
        layer = ds.GetLayerByName(layer_name)
        if layer is not None:
            return layer_name

    ds = None
    raise ValueError(f'No layer found in GeoPackage: {gpkg_path} with names: {layer_names}')


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


def track_huc(output_gpkg: str, rme_project_id: str, huc: str) -> None:
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

        curs.execute('INSERT INTO hucs (huc, rme_project_id) VALUES (?, ?)', [huc, rme_project_id])
        conn.commit()


def copy_dgo_values(input_gpkg: str, output_gpkg: str, gpkg_driver: ogr.Driver, huc: str) -> None:
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
    input_layer_name = get_layer_name(input_gpkg, ['rme_dgos', 'dgos'])
    input_layer = input_ds.GetLayerByName(input_layer_name)

    # Get the output layer
    output_layer_name = get_layer_name(output_gpkg, ['rme_igos', 'igos'])
    target_layer = target_ds.GetLayerByName(output_layer_name)

    # Get the list of columns and their data types for the 'rme_dgos' layer
    input_layer_defn = input_layer.GetLayerDefn()
    dgo_cols = {input_layer_defn.GetFieldDefn(i).GetName(): input_layer_defn.GetFieldDefn(i).GetType() for i in range(input_layer_defn.GetFieldCount())}
    # del dgo_cols['fid']
    # del dgo_cols['geom']

    # Get the list of columns and their data types for the 'rme_igos' layer
    target_layer_defn = target_layer.GetLayerDefn()
    igo_cols = {target_layer_defn.GetFieldDefn(i).GetName(): target_layer_defn.GetFieldDefn(i).GetType() for i in range(target_layer_defn.GetFieldCount())}

    # Find the columns in rme_dgos that are not in rme_igos
    required_dgo_cols = {k: v for k, v in dgo_cols.items() if k not in igo_cols}

    if 'huc' not in igo_cols:
        target_layer.CreateField(ogr.FieldDefn('huc', ogr.OFTString))

    # Add the required columns to the output layer
    for field_name, field_type in required_dgo_cols.items():
        target_layer.CreateField(ogr.FieldDefn(field_name, field_type))

    # Copy the DGO values to the output layer
    for input_feature in input_layer:
        level_path = input_feature.GetField('level_path')
        seg_distance = input_feature.GetField('seg_distance')

        if level_path is None or seg_distance is None:
            continue

        for field_name in required_dgo_cols.keys():
            input_value = input_feature.GetField(field_name)
            target_ds.ExecuteSQL(f"UPDATE igos SET {field_name} = {input_value} WHERE level_path = '{level_path}' AND seg_distance = {seg_distance} AND huc is NULL")

    target_ds.ExecuteSQL(f"UPDATE igos SET huc = '{huc}' WHERE huc is NULL")

    # Ensure that the necessary fields are indexed
    target_ds.ExecuteSQL('CREATE INDEX IF NOT EXISTS idx_level_path ON igos (level_path)')
    target_ds.ExecuteSQL('CREATE INDEX IF NOT EXISTS idx_seg_distance ON igos (seg_distance)')
    target_ds.ExecuteSQL('CREATE INDEX IF NOT EXISTS idx_huc ON igos (huc)')
    target_ds.ExecuteSQL('CREATE INDEX IF NOT EXISTS idx_fcode ON igos (FCode)')
    target_ds.ExecuteSQL('CREATE INDEX IF NOT EXISTS idx_ownership ON igos (rme_dgo_ownership)')
    target_ds.ExecuteSQL('CREATE INDEX IF NOT EXISTS idx_state ON igos (rme_dgo_state)')

    target_ds.CommitTransaction()
    target_ds = None


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
    target_layer = target_ds.GetLayerByName('igos')
    target_layer.CreateField(ogr.FieldDefn('huc', ogr.OFTString))

    print(f'GeoPackage created with the "igos" point layer: {output_gpkg}')

    return required_dgo_cols


def main():
    '''
    Scrape RME projects. Combine IGOs with their geometries. Include DGO metrics only.
    '''

    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('project_name', help='Name for the new RME scrape project', type=str)
    parser.add_argument('tags', help='Data Exchange tags to search for projects', type=str)
    parser.add_argument('min_rme_version', help='Minimum RME version to scrape', type=str, default=MINIMUM_RME_VERSION)
    parser.add_argument('--delete', help='Whether or not to delete downloaded GeoPackages',  action='store_true', default=False)
    parser.add_argument('--huc_filter', help='HUC filter begins with (e.g. 14)', type=str, default='')
    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    working_folder = args.working_folder
    download_folder = os.path.join(working_folder, 'downloads')
    output_gpkg = os.path.join(working_folder, 'rme-scrape-project', 'rme-igos.gpkg')

    safe_makedirs(working_folder)
    log = Logger('Setup')
    log.setup(log_path=os.path.join(os.path.dirname(output_gpkg), 'rme-scrape.log'), log_level=logging.DEBUG)
    log.info(f'Data Exchange Tags: {args.tags}')
    log.info(f'HUC Filter: {args.huc_filter if args.huc_filter != "" else "None"}')
    log.info(f'Project Name: {args.project_name}')
    log.info(f'Deleting downloads: {args.delete}')
    log.info(f'Minimum RME version: {args.min_rme_version}')

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
        scrape_rme(api, args.stage, search_params, args.project_name, download_folder, output_gpkg, args.delete, args.min_rme_version)

    log.info('Process complete')


if __name__ == '__main__':
    main()
