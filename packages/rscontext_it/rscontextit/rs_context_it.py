"""
Name:       Riverscapes Context Italy

Purpose:    Build a Riverscapes Context project for a single Italy watershed

Setup:      1. Use a GitHub CodeSpace within the Riverscapes Tools repo to run this script.
            2. Ensure you are using the RSContextIT Workspace.
            2. Use rscli to download the Italy National Project (project id = '8455c796-9ac1-4eb4-993c-40ff400c6693') to /Workspace/data.
            3. Use the Debug command and pick "RS Context - ITALY" to run this script.
            4. When prompted, provide the Watershed ID for the watershed (HUC) you want to process.
                This must correspond to a feature in the national hydrography watersheds feature class.
            5. The script will process the hydrography and topography data for the specified watershed.
            6. The output will be saved to the /Workspace/output folder.
            7. If you want to keep the output, use rscli to upload the output project to the Riverscapes Data Exchange.

Author:     Lorin Gaertner - based on Riverscapes Context NZ by Philip Bailey

Date:       2025-03-31
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
from rscommons.classes.raster import Raster
from osgeo import osr
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
from rscommons.project_bounds import generate_project_extents_from_layer
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
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'topography/dem.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'topography/dem_hillshade.tif'),
    'SLOPE': RSLayer('Slope', 'SLOPE', 'Raster', 'topography/slope.tif'),
    'HYDRO': RSLayer('Hydrography', 'HYDROGRAPHY', 'Geopackage', 'hydrography/hydrography.gpkg', {
        'FlowLines': RSLayer('Flow Lines', 'riverlines', 'Vector', 'riverlines'),
        'Watersheds': RSLayer('Watersheds', 'watersheds', 'Vector', 'watersheds'),
    }),
}


def rs_context_it(watershed_id: str, natl_hydro_gpkg: str, watershed_gpkg: str, dem: str, output_folder: str, meta: Dict[str, str]) -> None:
    """
    Run the Riverscapes Context Tool for Italy for a single watershed.
    This function processes hydrographic and topographic data for a specified watershed.

    Parameters:
    watershed_id (str): Watershed ID for the watershed to be processed.
    natl_hydro_gpkg (str): Path to the national hydrography GeoPackage.
    dem (str): Path to the DEM raster.
    output_folder (str): Directory where the output files will be saved.
    """

    log = Logger('RS Context')

    safe_makedirs(output_folder)

    hydro_gpkg, ws_name, ws_layer_name = process_hydrography(natl_hydro_gpkg, watershed_gpkg, watershed_id, output_folder, cfg.OUTPUT_EPSG)
    dem, slope, hillshade = process_topography(dem, output_folder, ws_layer_name, cfg.OUTPUT_EPSG)

    # Write the project bounds as a GeoJSON file and return the centroid and bounding box
    bounds_file = os.path.join(output_folder, 'project_bounds.geojson')
    bounds_info = generate_project_extents_from_layer(ws_layer_name, bounds_file)

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


GPKG_DATA_COLUMNS_DEFINITION = """
CREATE TABLE IF NOT EXISTS gpkg_data_columns (
  table_name TEXT NOT NULL,
  column_name TEXT NOT NULL,
  name TEXT,
  title TEXT,
  description TEXT,
  mime_type TEXT,
  constraint_name TEXT,
  CONSTRAINT pk_gdc PRIMARY KEY (table_name, column_name),
  CONSTRAINT gdc_tn UNIQUE (table_name, name)
);
"""


def copy_feature_class_attribute_descriptions(from_layer: str,
                                              to_layer: str) -> None:
    """ Copy the description (also known as the comment) for each attribute in the input layer
    to the same-named attribute in the output layer. 

    This is useful for copying metadata after creating a layer from another, for example using the RSCommons.copy_feature_class method. 

    Geopackages have two different metadata implementations: 
    * using `gpkg_data_columns` table
    * using `gpkg_metadata` and `gpkg_metadata_reference` tables

    This uses only the first, which is the one implemented by QGIS.

    Future enhancement ideas: 
    * copy both forms of metadata
    * copy constraints
    * add parameter to allow user to specify which attributes to copy
    * add parameter to allow usre to specify attributes to exclude
    * also copy layer-level metadata (probably with a separate function) 

    Args:
        input_gpkg_layer (str): geopackage layer (e.g. "path/to/hydrography.gpkg/hydro_net_l")
        output_gpkg_layer (str): geopackage layer (e.g. "path/to/hydrography.gpkg/riverlines")
    """
    # we don't actually check if these are truly geopackages or the layers exist within them
    # from rscommons import get_shp_or_gpkg, VectorBase
    log = Logger('Copy Feature Class Attribute Descriptions')

    from_pkg = os.path.dirname(from_layer)
    from_lyr = os.path.basename(from_layer)
    to_pkg = os.path.dirname(to_layer)
    to_lyr = os.path.basename(to_layer)

    # connect to the input geopackage and fetch metadata
    with sqlite3.connect(from_pkg) as conn:
        curs = conn.cursor()
        curs.execute('SELECT table_name, column_name, name, title, description, mime_type, constraint_name FROM gpkg_data_columns WHERE table_name = ?', (from_lyr,))
        from_metadata = curs.fetchall()

    log.info(f'Fetched {len(from_metadata)} records from {from_pkg}: gpkg_data_columns for {from_lyr}')

    # connect to the output geopackage
    with sqlite3.connect(to_pkg) as conn:
        curs = conn.cursor()
        # create table if not exists
        curs.execute(GPKG_DATA_COLUMNS_DEFINITION)
        # delete any existing metadata for the output layer
        curs.execute('DELETE FROM gpkg_data_columns WHERE table_name = ?', (to_lyr,))
        if curs.rowcount > 0:
            log.warning(f'Deleted {curs.rowcount} existing rows from {to_pkg}: gpkg_data_columns for {to_lyr}')
        # insert records from from_metadata but use to_lyr as table_name
        for record in from_metadata:
            new_record = (to_lyr, record[1], record[2], record[3], record[4], record[5], record[6])
            curs.execute('INSERT INTO gpkg_data_columns (table_name, column_name, name, title, description, mime_type, constraint_name) VALUES (?, ?, ?, ?, ?, ?, ?)', new_record)
        conn.commit()

    log.info(f'Inserted {len(from_metadata)} records into {to_pkg}: gpkg_data_columns for {to_lyr}')


def process_hydrography(national_hydro_gpkg: str, watershed_gpkg: str, watershed_id: str, output_folder: str, output_epsg: int) -> Tuple[str, str, str]:
    """
    Process the hydrography data for the specified watershed.

    This function processes the hydrography data for a given watershed by clipping the national hydrography 
    feature classes to the watershed boundary and saving the results to an output GeoPackage.

    Parameters:
    hydro_gpkg (str): Path to the GeoPackage containing national hydrography feature classes.
    watershed_gpkg (str): Path to the GeoPackage containing watershed feature classes.
    watershed_id (str): Identifier for the watershed.
    output_folder (str): Directory where the output files will be saved.

    Returns:
    Tuple[str, str, str]: A tuple containing the path to the output GeoPackage, the name of the watershed,
    the watershed boundary layer
    """

    log = Logger('Hydrography')
    log.info(f'Processing Hydrography for watershed {watershed_id}')

    # name of the watersheds layer hardcoded here
    watershed_layer = 'watersheds'
    input_watersheds = os.path.join(watershed_gpkg, watershed_layer)
    input_rivers = os.path.join(national_hydro_gpkg, 'hydro_net_l')

    # Load the watershed boundary polygon (in original projection)
    orig_ws_boundary, _trans_geom = get_geometry(watershed_gpkg,  watershed_layer, f'CatchID={watershed_id}', cfg.OUTPUT_EPSG)

    # Retrieve the watershed name
    # currently the watershed layer doesn't have name so we'll just call it the id
    watershed_name = 'spartiacque ' + str(watershed_id)
    # with sqlite3.connect(national_hydro_gpkg) as conn:
    #     curs = conn.cursor()
    #     # SQLite does not support parameterizing table or column names. These must be hardcoded or dynamically constructed in the query string.
    #     query = f'SELECT RivName, island FROM {watershed_layer} WHERE HydroID = ?'
    #     curs.execute(query, [watershed_id])
    #     row = curs.fetchone()
    #     watershed_name = row[0]

    # Define the output geopackage and layer paths
    output_gpkg = os.path.join(output_folder, 'hydrography', 'hydrography.gpkg')
    output_ws = os.path.join(output_gpkg, 'watersheds')

    # Check if the output GeoPackage already exists and if so, remove it
    if os.path.exists(output_gpkg):
        log.warning(f'Removing existing GeoPackage: {output_gpkg}')
        os.remove(output_gpkg)

    # Clip the national hydrography feature classes into the output GeoPackage
    copy_feature_class(input_watersheds, output_ws, output_epsg, attribute_filter=f'"CatchID"=\'{watershed_id}\'', make_valid=True)
    copy_feature_class(input_rivers, os.path.join(output_gpkg, 'riverlines'), output_epsg, clip_shape=orig_ws_boundary, make_valid=True)
    copy_feature_class_attribute_descriptions(input_rivers, os.path.join(output_gpkg, 'riverlines'))

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

        for field in ['level_path', 'FCode', 'CatchID', 'TNODE', 'FNODE']:
            curs.execute(f'CREATE INDEX idx_{field} ON riverlines({field})')

        # Assign level paths to all reaches in the GeoPackage
        calc_level_path(curs, watershed_id, True)

        # Assign FCode.

        curs.execute("UPDATE riverlines SET FCode = 33600 WHERE (DFDD = 'BH020' OR DFDD = 'BH030')")
        curs.execute("UPDATE riverlines SET FCode = 46006 WHERE (DFDD='BH140' AND HYP=1)")
        curs.execute("UPDATE riverlines SET FCode = 46000 WHERE (DFDD='BH140' AND (HYP IS NULL OR HYP<>1))")

        # Assign Drainage Area to the riverlines
        # TODO: Make this work
        # curs.execute('UPDATE riverlines SET TotDASqKM = CUM_AREA / 1000000.0')

        log.info('Recreating triggers')
        for trigger in triggers:
            curs.execute(trigger[4])

        conn.commit()

    log.info(f'Hydrography processed and saved to {output_gpkg}')

    return output_gpkg, watershed_name, output_ws


def get_geometry(gpkg: str, layer_name: str, where_clause: str, output_epsg: int) -> Tuple[ogr.Geometry]:
    """
    Retrieves the geometry for a feature in a specified GeoPackage layer, 
    optionally transforming it to a target (output) spatial reference system (SRS).

    Args:
        gpkg (str): The file path to the GeoPackage.
        layer_name (str): The name of the layer within the GeoPackage to query.
        where_clause (str): An SQL-like WHERE clause to filter the features.
        output_epsg (int): The EPSG code of the target spatial reference system 
            to which the geometry should be transformed.

    Returns:
        Tuple[ogr.Geometry]: A tuple containing 
        the original geometry and 
        the transformed geometry in the target SRS.

    Raises:
        ValueError: If no features or multiple features match the `where_clause`.
    """
    with GeopackageLayer(gpkg, layer_name) as in_layer:
        output_srs, _transform = get_transform_from_epsg(in_layer.spatial_ref, output_epsg)

        feature_count = 0
        original_geom = None
        transform_geom = None

        for rme_feature, *_ in in_layer.iterate_features(attribute_filter=where_clause):
            feature_count += 1
            if feature_count > 1:
                raise ValueError(f"Multiple features ({feature_count}) found for where_clause: '{where_clause}' in layer '{layer_name}'. Expected exactly one feature.")

            rme_feature: ogr.Feature
            original_geom: ogr.Geometry = rme_feature.GetGeometryRef().Clone()
            transform_geom = original_geom.Clone()
            transform_geom.TransformTo(output_srs)

        if feature_count == 0:
            raise ValueError(f"No features found for where_clause: '{where_clause}' in layer '{layer_name}'.")

        return original_geom, transform_geom


def calculate_metrics(output_gpkg: str, dem_path: str, slope_path: str, output_file: str) -> dict:
    """
    Calculate some basic metrics
    """
    log = Logger('Metrics')
    log.info('Calculating metrics for the watershed')

    metrics = {}
    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()

        curs.execute('SELECT CatchID FROM watersheds')
        row = curs.fetchone()
        metrics['watershed id'] = row[0]

        # River lines
        curs.execute('SELECT COUNT(*), Sum(LENGTH), Min(TotDASqKm), Max(TotDASqKm) FROM riverlines')
        row = curs.fetchone()
        metrics['riverCount'] = row[0]
        metrics['riverLength'] = row[1]
        metrics['riverMinDrainageArea'] = row[2]
        metrics['riverMaxDrainageArea'] = row[3]

    # TODO: Include raster statistics in metrics

    with open(output_file, 'w', encoding='utf8') as f:
        json.dump(metrics, f, indent=2)


def get_raster_epsg(raster_path: str) -> int:
    """
    Get the EPSG code of a raster file using the Raster class from rscommons.

    Args:
        raster_path (str): Path to the raster file.

    Returns:
        int: EPSG code of the raster's CRS, or None if not found.
    """
    # Create a Raster object
    raster = Raster(raster_path)

    # Get the projection in WKT format
    wkt_projection = raster.proj

    # Parse the WKT projection to get the EPSG code
    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromWkt(wkt_projection)

    # Extract the EPSG code
    if spatial_ref.IsProjected() or spatial_ref.IsGeographic():
        epsg = spatial_ref.GetAttrValue("AUTHORITY", 1)
        return int(epsg) if epsg else None
    else:
        return None


def process_topography(input_dem: str, output_folder: str, processing_boundary, output_epsg: int) -> Tuple[str, str, str]:
    """
    Process the topography data for the specified watershed.
    Return: tuple with paths of each of output dem, slope and hillshade rasters.
    """

    log = Logger('Topography')
    log.info(f'Processing topography using DEM: {input_dem}')

    topo_folder = os.path.join(output_folder, 'topography')
    output_dem = os.path.join(topo_folder, 'dem.tif')
    output_slope = os.path.join(topo_folder, 'slope.tif')
    output_hillshade = os.path.join(topo_folder, 'dem_hillshade.tif')

    # check if we are trying to transform the raster and WARNING if so
    if get_raster_epsg(input_dem) != output_epsg:
        log.warning(f'Input DEM EPSG {get_raster_epsg(input_dem)} does not match output EPSG {output_epsg}. Usually  inadvisable to transform, but we will.')
    # clip, and possibly transform, the input DEM
    raster_warp(input_dem, output_dem, output_epsg, processing_boundary, {"cutlineBlend": 1})
    # generate slope and hillshade rasters
    gdal_dem_geographic(output_dem, output_slope, 'slope')
    gdal_dem_geographic(output_dem, output_hillshade, 'hillshade')

    log.info(f'DEM produced at {output_dem}')
    log.info(f'Slope produced at {output_slope}')
    log.info(f'Hillshade produced at {output_hillshade}')
    log.info('Topography processing complete')

    return output_dem, output_slope, output_hillshade


def main():
    """ Main entry point for Italy RS Context"""
    parser = argparse.ArgumentParser(description='Riverscapes Context Tool for Italy')
    parser.add_argument('watershed_id', help='Watershed/HUC identifier', type=int)
    parser.add_argument('hydro_gpkg', help='Path to GeoPackage containing national hydrography feature classes', type=str)
    parser.add_argument('watershed_gpkg', help='Path to GeoPackage containing watershed classes', type=str)
    parser.add_argument('dem', help='Path to DEM raster.', type=str)
    parser.add_argument('output', help='Path to the output folder', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about things like memory usage. There is a performance cost', action='store_true', default=False)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)

    args = dotenv.parse_args_env(parser)

    log = Logger('RS Context')
    log.setup(logPath=os.path.join(args.output, 'RSContextIT.log'), verbose=args.verbose)
    log.title(f'Riverscapes Context IT For Watershed: {args.watershed_id}')

    log.info(f'Watershed ID: {args.watershed_id}')
    log.info(f'Model Version: {__version__}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')
    log.info(f'Output folder: {args.output}')

    meta = parse_metadata(args.meta)

    try:
        rs_context_it(args.watershed_id, args.hydro_gpkg, args.watershed_gpkg, args.dem, args.output, meta)
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
