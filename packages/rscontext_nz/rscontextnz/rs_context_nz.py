"""
Name:       Riverscapes Context New Zealand

Purpose:    Build a Riverscapes Context project for a single New Zealand watershed

Setup:      1. Use a GitHub CodeSpace within the Riverscapes Tools repo to run this script.
            2. Ensure you are using the RSContext for New Zealand Workspace.
            2. Use rscli to download the New Zealand National Project to /Workspace/data.
            3. Use the Debug command and pick "RS Context - NEW ZEALAND" to run this script.
            4. When prompted, provide the Watershed ID for the watershed (HUC) you want to process.
                This must correspond to a feature in the national hydrography watersheds feature class.
            5. The script will process the hydrography and topography data for the specified watershed.
            6. The output will be saved to the /Workspace/output folder.
            7. If you want to keep the output, use rscli to upload the output project to the Riverscapes Data Exchange.

Author:     Philip Bailey

Date:       9 Nov 2024
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
from typing import Tuple, Dict
import argparse
import sqlite3
import json
import os
import sys
import traceback
from osgeo import ogr

from rscommons import (Logger, ModelConfig, dotenv, initGDALOGRErrors)
from rscommons.classes.rs_project import RSLayer, RSProject, RSMeta, RSMetaTypes
from rscommons.geographic_raster import gdal_dem_geographic
from rscommons.project_bounds import generate_project_extents_from_layer
from rscommons.raster_warp import raster_warp
from rscommons.util import (parse_metadata, safe_makedirs, safe_remove_dir)
from rscommons.vector_ops import copy_feature_class
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions, raster_resolution_meta
from rscommons.shapefile import copy_feature_class
from rscommons.shapefile import get_geometry_union

from rscontextnz.__version__ import __version__

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)


def rs_context_nz(watershed_id: str, natl_hydro_gpkg: str, dem_north: str, dem_south: str, output_folder: str, download_folder: str, scratch_dir: str, meta: Dict[str, str]):
    """
    Run the Riverscapes Context Tool for New Zealand for a single watershed.

    This function processes geographic and hydrologic data for a specified watershed in New Zealand.
    It performs various operations such as downloading necessary data, clipping vector layers, 
    and generating reports.

    Parameters:
    watershed_id (str): Watershed ID for the watershed to be processed.
    natl_hydro_gpkg (str): Path to the national hydrography GeoPackage.
    topo_very (str): Path to the national topography VRT file.
    output_folder (str): Directory where the output files will be saved.
    download_folder (str): Directory where downloaded files will be stored.
    scratch_dir (str): Directory for temporary files.
    meta (Dict[str, str]): Metadata dictionary containing additional information that will be saved to the output project.

    Returns:
    None
    """

    log = Logger('RS Context')

    safe_makedirs(output_folder)
    safe_makedirs(download_folder)

    hydro_gpkg, boundary, is_north = process_hydrography(natl_hydro_gpkg, watershed_id, output_folder)
    dem, slope, hillshade = process_topography(dem_north if is_north is True else dem_south, output_folder, boundary)
    
    bounds_file = os.path.join(output_folder, 'project_bounds.geojson')
    bounds_info = generate_project_extents_from_layer(boundary, bounds_file)
    
    metrics_file = os.path.join(output_folder, 'metrics.json')
    calculate_metrics(hydro_gpkg, dem, slope, metrics_file)

    # TODO: Optional... add other contextual data (land cover, soils, transportation, vegetation etc.)
    # TODO: Optional... build HTML report for the watershed, summarizing the data and processing steps
    # TODO: Optional... write a JSON file with metrics for the watershed (km of perennial, intermittent, ephemeral streams, etc.)


    # Write a the project bounds as a GeoJSON file and return the centroid and bounding box
    

    # TODO: Build the Riverscapes Project metadata XML file project.rs.xml
    # bounds_info['CENTROID'], bounds_info['BBOX']

    log.info('Riverscapes Context processing complete')

def process_hydrography(hydro_gpkg: str, watershed_id: str, output_folder: str) -> Tuple[str, str, geom, bool]:
    """
    Process the hydrography data for the specified watershed.

    This function processes the hydrography data for a given watershed by clipping the national hydrography 
    feature classes to the watershed boundary and saving the results to an output GeoPackage.

    Parameters:
    hydro_gpkg (str): Path to the GeoPackage containing national hydrography feature classes.
    watershed_id (str): Identifier for the watershed.
    output_folder (str): Directory where the output files will be saved.

    Returns:
    Tuple[str, str, geom, bool]: A tuple containing the path to the output GeoPackage, the name of the watershed,
    the watershed boundary geometry and whether the watershed is on the North Island.
    """

    log = Logger('Hydrography')
    log.info(f'Processing Hydrography for watershed {watershed_id}')

    input_watersheds = os.path.join(hydro_gpkg, 'watersheds')
    input_rivers = os.path.join(hydro_gpkg, 'riverlines')
    input_catchments = os.path.join(hydro_gpkg, 'rec2ws')
    input_junctions = os.path.join(hydro_gpkg, 'hydro_net_junctions')

    # Load the watershed boundary polygon
    watershed_boundary = get_geometry_union(input_watersheds, cfg.OUTPUT_EPSG, f'watershed_id={watershed_id}')

    # Retrieve the watershed name and whether this watershed is on the North or South Island
    with sqlite3.connect(hydro_gpkg) as conn:
        cur = conn.cursor()
        cur.execute('SELECT name, island_flag FROM watersheds WHERE watershed_id = ?', [watershed_id])
        row = cur.fetchone()
        watershed_name = row[0]
        is_north = row[1] == 'N'

    # TODO: If more processing of hydrography is needed, then perform these operations here.
    # Optionally, copy the hydrography feature classes into an intermediates GeoPackage
    # and do the processing there. Avoid modifying the original hydrography data.
    # Finally, copy the resultant feature classes into the output GeoPackage.

    # Clip the national hydrography feature classes into the output GeoPackage
    output_gpkg = os.path.join(output_folder, 'hydrography.gpkg')
    copy_feature_class(input_watersheds, cfg.OUTPUT_EPSG, output_gpkg, attribute_filter=f'"watershed_id"=\'{watershed_id}\'')
    copy_feature_class(input_rivers, cfg.OUTPUT_EPSG, output_gpkg, clip_shape=watershed_boundary)
    copy_feature_class(input_catchments, cfg.OUTPUT_EPSG, output_gpkg, clip_shape=watershed_boundary)
    copy_feature_class(input_junctions, cfg.OUTPUT_EPSG, output_gpkg, clip_shape=watershed_boundary)

    # TODO: Add and calculate fields needed by other Riverscapes tools
    # 1. level_path (string)
    # 2. FCode (ReachCode) https://github.com/Riverscapes/riverscapes-tools/blob/master/packages/brat/database/data/ReachCodes.csv
    # 3. DRNAREA (double) upstream drainage area


    log.info(f'Hydrography processed and saved to {output_gpkg}')

    return  output_gpkg, watershed_name, watershed_boundary, is_north

def calculate_metrics(output_gpkg: str, dem_path: str, slope_path: str, output_file: str) -> dict:

    log = Logger('Metrics')
    log.info('Calculating metrics for the watershed')

    metrics = {}
    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()

        curs.execute('SELECT watershed_name, island_flag, areasqkm FROM watersheds')
        row = curs.fetchone()
        metrics['watershed_name'] = row[0]
        metrics['island_flag'] = 'north' if row[1] == 'N' else 'south'
        metrics['watershed_area'] = row[2]

        # River lines
        curs.execute('SELECT COUNT(*), Sum(length_km), Min(TotDASqKm), Max(TotDASqKm) FROM riverlines')
        row = curs.fetchone()
        metrics['river_count'] = row[0]
        metrics['river_length'] = row[1]
        metrics['river_min_drainage_area'] = row[2]
        metrics['river_max_drainage_area'] = row[3]

    # TODO: Include raster statistics in metrics

    with open(output_file, 'w', encoding='utf8') as f:
        json.dump(metrics, f, indent=2)

def process_topography(input_dem: str, output_folder: str, processing_boundary) -> Tuple[str,str,str]:
    """
    Process the topography data for the specified watershed.
    """

    log = Logger('Topography')
    log.info(f'Processing topography using DEM: {input_dem}')

    output_dem = os.path.join(output_folder, 'dem.tif')
    output_slope = os.path.join(output_folder, 'slope.tif')
    output_hillshade = os.path.join(output_folder, 'hillshade.tif')

    raster_warp(input_dem, output_dem, epsg, processing_boundary, {"cutlineBlend": 1})
    gdal_dem_geographic(output_dem, output_slope, 'slope')
    gdal_dem_geographic(output_dem, output_slope, 'hillshade')

    log.info(f'DEM produced at {output_dem}')
    log.info(f'Slope produced at {output_slope}')
    log.info(f'Hillshade produced at {output_hillshade}')
    log.info('Topography processing complete')

    return output_dem, output_slope, output_hillshade


def main():
    """
    Main entry point for New Zealand RS Context
    """
    parser = argparse.ArgumentParser(description='Riverscapes Context Tool')
    parser.add_argument('watershed_id', help='Watershed/HUC identifier', type=str)
    parser.add_argument('hydro_gpkg', help='Path to GeoPackage containing national hydrography feature classes', type=str)
    parser.add_argument('dem_north', help='Path to North Island DEM raster.', type=str)
    parser.add_argument('dem_south', help='Path to South Island DEM raster.', type=str)
    parser.add_argument('output', help='Path to the output folder', type=str)
    parser.add_argument('--temp_folder', help='(optional) cache folder for downloading files ', type=str)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about things like memory usage. There is a performance cost', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("RS Context")
    log.setup(logPath=os.path.join(args.output, "rs_context_nz.log"), verbose=args.verbose)
    log.title(f'Riverscapes Context NZ For Watershed: {args.watershed_id}')

    log.info(f'HUC: {args.huc}')
    log.info(f'Model Version: {__version__}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')
    log.info(f'Output folder: {args.output}')

    # This is a general place for unzipping downloaded files and other temporary work.
    # We use GUIDS to make it specific to a particular run of the tool to avoid unzip collisions
    scratch_dir = args.temp_folder if args.temp_folder else os.path.join('scratch', 'rs_context')
    safe_makedirs(scratch_dir)

    meta = parse_metadata(args.meta)

    try:
        rs_context_nz(args.watershed_id, args.hydro_gpkg, args.dem_north, args.dem_south, args.output, args.download, scratch_dir, meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        # Cleaning up the scratch folder is essential
        safe_remove_dir(scratch_dir)
        sys.exit(1)

    # Cleaning up the scratch folder is essential
    safe_remove_dir(scratch_dir)
    sys.exit(0)


if __name__ == '__main__':
    main()
