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

from rscommons import Logger, ModelConfig, dotenv, initGDALOGRErrors
from rscommons.classes.rs_project import RSLayer, RSProject, RSMeta, RSMetaTypes
from rscommons.geographic_raster import gdal_dem_geographic
from rscommons.project_bounds import generate_project_extents_from_geom
from rscommons.raster_warp import raster_warp
from rscommons.util import safe_makedirs, parse_metadata
from rscommons.vector_ops import copy_feature_class
from rscommons.classes.vector_classes import GeopackageLayer
from rscommons.shapefile import get_transform_from_epsg
from rscontextnz.__version__ import __version__
from .calc_level_path import calc_level_path, get_triggers

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

initGDALOGRErrors()

LayerTypes = {
    # key: (name, id, tag, relpath)
    'DEM': RSLayer('8m DEM', 'DEM', 'Raster', 'topography/dem.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'topography/dem_hillshade.tif'),
    'SLOPE': RSLayer('Slope', 'SLOPE', 'Raster', 'topography/slope.tif'),
    'HYDRO': RSLayer('Hydrography', 'HYDROGRAPHY', 'Geopackage', 'hydrography/hydrography.gpkg', {
        'FlowLines': RSLayer('Flow Lines', 'riverlines', 'Vector', 'riverlines'),
        'Watersheds': RSLayer('Watersheds', 'watersheds', 'Vector', 'watersheds'),
        'Catchments': RSLayer('Catchments', 'catchments', 'Vector', 'catchments'),
        'Junctions': RSLayer('Hydro Junctions', 'junctions', 'Vector', 'junctions'),
        'Lakes': RSLayer('Lakes', 'lakes', 'Vector', 'lakes'),
    }),
}


def rs_context_nz(watershed_id: str, natl_hydro_gpkg: str, dem_north: str, dem_south: str, output_folder: str, meta: Dict[str, str]) -> None:
    """
    Run the Riverscapes Context Tool for New Zealand for a single watershed.
    This function processes hydrographic and topographic data for a specified watershed in New Zealand.

    Parameters:
    watershed_id (str): Watershed ID for the watershed to be processed.
    natl_hydro_gpkg (str): Path to the national hydrography GeoPackage.
    dem_north (str): Path to the North Island DEM raster.
    dem_south (str): Path to the South Island DEM raster.
    output_folder (str): Directory where the output files will be saved.
    """

    log = Logger('RS Context')

    safe_makedirs(output_folder)

    hydro_gpkg, ws_name, is_north, trans_geom, ws_boundary_path = process_hydrography(natl_hydro_gpkg, watershed_id, output_folder)
    dem, slope, hillshade = process_topography(dem_north if is_north is True else dem_south, output_folder, ws_boundary_path)

    # Write a the project bounds as a GeoJSON file and return the centroid and bounding box
    bounds_file = os.path.join(output_folder, 'project_bounds.geojson')
    bounds_info = generate_project_extents_from_geom(trans_geom, bounds_file)

    metrics_file = os.path.join(output_folder, 'rscontext_metrics.json')
    calculate_metrics(hydro_gpkg, dem, slope, metrics_file)

    # Build the Riverscapes Context project
    project_name = f'Riverscapes Context for {ws_name}'
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'RSContextNZ', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/rscontextnz', RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(watershed_id), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(watershed_id), locked=True),
        RSMeta('Watershed Name', ws_name, RSMetaTypes.HIDDEN, locked=True),
    ])

    project.add_project_extent(bounds_file, bounds_info['CENTROID'], bounds_info['BBOX'])
    project.add_metadata([RSMeta(key, val, RSMetaTypes.HIDDEN, locked=True) for key, val in meta.items()])

    realization = project.add_realization(project_name, 'REALIZATION1', cfg.version)
    datasets = project.XMLBuilder.add_sub_element(realization, 'Datasets')

    project.add_project_geopackage(datasets, LayerTypes['HYDRO'])
    project.add_dataset(datasets, metrics_file, RSLayer('Metrics', 'Metrics', 'File', os.path.basename(metrics_file)), 'File')
    project.add_dataset(datasets, dem, LayerTypes['DEM'], 'DEM')
    project.add_dataset(datasets, slope, LayerTypes['SLOPE'], 'Raster')
    project.add_dataset(datasets, hillshade, LayerTypes['HILLSHADE'], 'Raster')

    log.info('Riverscapes Context processing complete')


def process_hydrography(national_hydro_gpkg: str, watershed_id: str, output_folder: str) -> Tuple[str, str, bool, ogr.Geometry]:
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

    input_watersheds = os.path.join(national_hydro_gpkg, 'NZ_Large_River_Catchments')
    input_rivers = os.path.join(national_hydro_gpkg, 'riverlines')
    input_catchments = os.path.join(national_hydro_gpkg, 'rec2ws')
    input_junctions = os.path.join(national_hydro_gpkg, 'hydro_net_junctions')
    input_lakes = os.path.join(national_hydro_gpkg, 'nz_lake_polygons_topo_150k')

    # Load the watershed boundary polygon (in original projection)
    orig_ws_boundary, trans_geom = get_geometry(national_hydro_gpkg,  'NZ_Large_River_Catchments', f'HydroID={watershed_id}', cfg.OUTPUT_EPSG)

    # Retrieve the watershed name and whether this watershed is on the North or South Island
    with sqlite3.connect(national_hydro_gpkg) as conn:
        curs = conn.cursor()
        curs.execute('SELECT RivName, island FROM NZ_Large_River_Catchments WHERE HydroID = ?', [watershed_id])
        row = curs.fetchone()
        watershed_name = row[0]
        is_north = row[1].lower() == 'n'

    # Clip the national hydrography feature classes into the output GeoPackage
    output_gpkg = os.path.join(output_folder, 'hydrography', 'hydrography.gpkg')
    output_ws = os.path.join(output_gpkg, 'watersheds')
    copy_feature_class(input_watersheds, output_ws, 2193, attribute_filter=f'"HydroID"=\'{watershed_id}\'', make_valid=True)
    copy_feature_class(input_rivers, os.path.join(output_gpkg, 'riverlines'), cfg.OUTPUT_EPSG, clip_shape=orig_ws_boundary, make_valid=True)
    copy_feature_class(input_catchments, os.path.join(output_gpkg, 'catchments'), cfg.OUTPUT_EPSG, clip_shape=orig_ws_boundary, make_valid=True)
    copy_feature_class(input_junctions, os.path.join(output_gpkg, 'junctions'), cfg.OUTPUT_EPSG, clip_shape=orig_ws_boundary, make_valid=True)
    copy_feature_class(input_lakes, os.path.join(output_gpkg, 'lakes'), cfg.OUTPUT_EPSG, clip_shape=orig_ws_boundary, make_valid=True)

    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()

        # Drop triggers so that we can update the feature class
        log.info('Dropping triggers to allow for updating level path')
        triggers = get_triggers(curs, 'riverlines')
        for trigger in triggers:
            curs.execute(f"DROP TRIGGER {trigger[1]}")

        # Add the riverscape fields to the riverlines feature class
        for field, data_type in [('level_path', 'REAL'), ('FCode', 'INTEGER'), ('TotDASqKM', 'REAL')]:
            curs.execute(f'ALTER TABLE riverlines ADD COLUMN {field} {data_type}')

        for field in ['level_path', 'HydroID', 'FCode', 'TO_NODE', 'FROM_NODE']:
            curs.execute(f'CREATE INDEX idx_{field} ON riverlines({field})')

        # Assign level paths to all reaches in the GeoPackage
        calc_level_path(curs, watershed_id, True)

        # Assign FCode. Apply the NHD artifical path code to any features that have an LID (presumed to be Lake ID)
        curs.execute('UPDATE riverlines SET FCode = ? WHERE LID <> 0', [55800])
        curs.execute('UPDATE riverlines SET FCode = ? WHERE LID = 0', [46006])

        # Assign Drainage Area to the riverlines
        curs.execute('UPDATE riverlines SET TotDASqKM = CUM_AREA / 1000000.0')

        log.info('Recreating triggers')
        for trigger in triggers:
            curs.execute(trigger[4])

        conn.commit()

    log.info(f'Hydrography processed and saved to {output_gpkg}')

    return output_gpkg, watershed_name, is_north, trans_geom, output_ws


def get_geometry(gpkg: str, layer_name: str, where_clause: str, output_epsg: int) -> Tuple[ogr.Geometry]:
    """
    Get the geometry for a feature in a GeoPackage layer.
    """

    with GeopackageLayer(gpkg, layer_name) as in_layer:

        output_srs, _transform = get_transform_from_epsg(in_layer.spatial_ref, output_epsg)

        for rme_feature, *_ in in_layer.iterate_features(attribute_filter=where_clause):
            rme_feature: ogr.Feature
            original_geom: ogr.Geometry = rme_feature.GetGeometryRef().Clone()
            transform_geom = original_geom.Clone()
            transform_geom.TransformTo(output_srs)
            return original_geom, transform_geom


def calculate_metrics(output_gpkg: str, dem_path: str, slope_path: str, output_file: str) -> dict:

    log = Logger('Metrics')
    log.info('Calculating metrics for the watershed')

    metrics = {}
    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()

        curs.execute('SELECT RivName, island, CUM_AREA FROM watersheds')
        row = curs.fetchone()
        metrics['watershedName'] = row[0]
        metrics['island'] = 'north' if row[1] == 'N' else 'south'
        metrics['watershedArea'] = row[2]

        # River lines
        curs.execute('SELECT COUNT(*), Sum(Shape_Length), Min(TotDASqKm), Max(TotDASqKm) FROM riverlines')
        row = curs.fetchone()
        metrics['riverCount'] = row[0]
        metrics['riverLength'] = row[1]
        metrics['riverMinDrainageArea'] = row[2]
        metrics['riverMaxDrainageArea'] = row[3]

    # TODO: Include raster statistics in metrics

    with open(output_file, 'w', encoding='utf8') as f:
        json.dump(metrics, f, indent=2)


def process_topography(input_dem: str, output_folder: str, processing_boundary) -> Tuple[str, str, str]:
    """
    Process the topography data for the specified watershed.
    """

    log = Logger('Topography')
    log.info(f'Processing topography using DEM: {input_dem}')

    topo_folder = os.path.join(output_folder, 'topography')
    output_dem = os.path.join(topo_folder, 'dem.tif')
    output_slope = os.path.join(topo_folder, 'slope.tif')
    output_hillshade = os.path.join(topo_folder, 'dem_hillshade.tif')

    raster_warp(input_dem, output_dem, 2193, processing_boundary, {"cutlineBlend": 1})
    gdal_dem_geographic(output_dem, output_slope, 'slope')
    gdal_dem_geographic(output_dem, output_hillshade, 'hillshade')

    log.info(f'DEM produced at {output_dem}')
    log.info(f'Slope produced at {output_slope}')
    log.info(f'Hillshade produced at {output_hillshade}')
    log.info('Topography processing complete')

    return output_dem, output_slope, output_hillshade


def main():
    """ Main entry point for New Zealand RS Context"""
    parser = argparse.ArgumentParser(description='Riverscapes Context Tool for New Zealand')
    parser.add_argument('watershed_id', help='Watershed/HUC identifier', type=int)
    parser.add_argument('hydro_gpkg', help='Path to GeoPackage containing national hydrography feature classes', type=str)
    parser.add_argument('dem_north', help='Path to North Island DEM raster.', type=str)
    parser.add_argument('dem_south', help='Path to South Island DEM raster.', type=str)
    parser.add_argument('output', help='Path to the output folder', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about things like memory usage. There is a performance cost', action='store_true', default=False)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)

    args = dotenv.parse_args_env(parser)

    log = Logger('RS Context')
    log.setup(logPath=os.path.join(args.output, 'RSContextNZ.log'), verbose=args.verbose)
    log.title(f'Riverscapes Context NZ For Watershed: {args.watershed_id}')

    log.info(f'Watershed ID: {args.watershed_id}')
    log.info(f'Model Version: {__version__}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')
    log.info(f'Output folder: {args.output}')

    meta = parse_metadata(args.meta)

    try:
        rs_context_nz(args.watershed_id, args.hydro_gpkg, args.dem_north, args.dem_south, args.output, meta)
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
