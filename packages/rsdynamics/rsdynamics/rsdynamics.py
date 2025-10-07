"""
Name:       Riverscapes Dynamics

Purpose:    Build a Riverscapes Dynamics project for a single New Zealand watershed

Setup:

Author:     Philip Bailey

Date:       1 Oct 2025
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
import re
import copy
from typing import Tuple, Dict, List
from datetime import datetime
import argparse
import sqlite3
import json
import os
import sys
import traceback
from xml.etree.ElementTree import Element
from osgeo import ogr
import geopandas as gpd
import rasterio
import rasterio.mask
import numpy as np


from rscommons import Logger, ModelConfig, dotenv, initGDALOGRErrors
from rscommons.classes.rs_project import RSLayer, RSProject, RSMeta, RSMetaTypes
from rscommons.geographic_raster import gdal_dem_geographic
from rscommons.project_bounds import generate_project_extents_from_geom
from rscommons.raster_warp import raster_warp
from rscommons.util import safe_makedirs, parse_metadata
from rscommons.vector_ops import copy_feature_class
from rscommons.classes.vector_classes import GeopackageLayer
from rscommons.shapefile import get_transform_from_epsg
from rsdynamics.__version__ import __version__
from rscommons.classes.vector_classes import VectorBase

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)
cfg.OUTPUT_EPSG = 2193  # NZTM

initGDALOGRErrors()

# regex for epoch raster names
epoch_raster_pattern = r'mosaic_(\w*)_frequency_([0-9]{1,2})_.*_([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{4})-([0-9]{2})-([0-9]{2})\.tif'
classified_raster_pattern = r'.*_([0-9]{8})_(\w*)\.tif'


LayerTypes = {
    # key: (name, id, tag, relpath)
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'topography/dem_hillshade.tif'),
}


def rsdynamics(watershed_id: str, vbet_project_xml: str, raster_folder: str, output_dir: str):
    """ Main function for running Riverscapes Dynamics"""

    log = Logger('RS Dynamics')

    rsd_gpkg = os.path.join(output_dir, 'dgos', 'rsdynamics.gpkg')
    vbet_full = copy_vbet_layer(vbet_project_xml, 'VBET_OUTPUTS', 'vbet_full', rsd_gpkg)
    vbet_dgos = copy_vbet_layer(vbet_project_xml, 'Intermediates', 'vbet_dgos', rsd_gpkg)

    input_rasters = process_classified_rasters(raster_folder, output_dir, vbet_full)
    epoch_rasters = process_epoch_rasters(raster_folder, output_dir, vbet_full)
    __hillshade_raster = copy_hillshade(vbet_project_xml, output_dir)

    for raster_path, __raster_name, __raster_id, att_prefix in epoch_rasters:
        try:
            calc_raster_stats(raster_path, vbet_dgos, att_prefix)
        except Exception as e:
            log.warning(f'Error calculating stats for {raster_path}: {e}')
            continue

    # Create the output Riverscapes project.rs.xml based on the rscontext XML
    rsd_project = build_dynamics_project(vbet_project_xml, output_dir)

    nod_realization = rsd_project.add_realization('Riverscapes Dynamics', 'REALIZATION1', cfg.version)
    nod_inputs = rsd_project.XMLBuilder.add_sub_element(nod_realization, 'Inputs')
    nod_outputs = rsd_project.XMLBuilder.add_sub_element(nod_realization, 'Outputs')

    rsd_project.add_project_raster(nod_inputs, LayerTypes['HILLSHADE'])

    for i, classified_info in enumerate(input_rasters):
        raster_path, raster_name, raster_id = classified_info
        rel_layer_path = rsd_project.get_relative_path(raster_path)
        layer_rs = RSLayer(raster_name, raster_id, 'Raster', rel_layer_path)
        rsd_project.add_project_raster(nod_inputs, layer_rs)

    for i, epoch_info in enumerate(epoch_rasters):
        raster_path, raster_name, raster_id, __att_prefix = epoch_info
        rel_layer_path = rsd_project.get_relative_path(raster_path)
        layer_rs = RSLayer(raster_name, raster_id, 'Raster', rel_layer_path)
        rsd_project.add_project_raster(nod_outputs, layer_rs)

    rsd_project.XMLBuilder.write()

    return rsd_project


def copy_vbet_layer(vbet_project_xml: str, gpkg_id: str, layer_name: str, output_gpkg: str) -> str:
    """
    Copy a specific VBET layer to the output folder.
    """

    vbet_project = RSProject(None, vbet_project_xml)
    nod_gpkg = vbet_project.XMLBuilder.find_by_id(gpkg_id)
    if nod_gpkg is None:
        raise Exception(f'No {gpkg_id} layer found in the input VBET project')
    rel_gpkg_path = nod_gpkg.find('Path').text
    vbet_gpkg = os.path.join(os.path.dirname(vbet_project_xml), rel_gpkg_path)

    vbet_layer_path = os.path.join(vbet_gpkg, layer_name)
    rsd_layer_path = os.path.join(output_gpkg, layer_name)

    copy_feature_class(vbet_layer_path, rsd_layer_path)
    return rsd_layer_path


def build_dynamics_project(vbet_project_xml: str, output_dir: str) -> RSProject:
    """
    Create a new Riverscapes project for Riverscapes Dynamics based on the input
    Riverscapes Context project.
    """

    # Create the output Riverscapes project
    vbet_project = RSProject(None, vbet_project_xml)
    vbet_project_meta = vbet_project.get_metadata_dict()
    vbet_project_name = vbet_project.XMLBuilder.find('Name')

    rsd_project_meta = copy.deepcopy(vbet_project_meta)
    rsd_project_meta['Model Documentation'] = 'https://tools.riverscapes.net/rsdynamics'
    rsd_project_meta['Model Version'] = __version__
    rsd_project_meta['Date Created'] = datetime.now().isoformat()
    rsd_project_meta.pop('Flowline Type', None)
    rsd_project_meta.pop('Low Lying Valley Threshold', None)
    rsd_project_meta.pop('Elevated Valley Threshold', None)
    rsd_project_meta.pop('Runner', None)
    rsd_project_meta.pop('ProcTimeS', None)
    rsd_project_meta.pop('Processing Time', None)

    rs_meta = [RSMeta(k, v) for k, v in rsd_project_meta.items()]

    # Build the Riverscapes Context project
    project_name = vbet_project_name.text.replace('VBET', 'Riverscapes Dynamics')
    rsd_project = RSProject(cfg, output_dir)
    rsd_project.create(project_name, 'rsdynamics', rs_meta)

    # Reuse the project extents from the input Riverscapes Context project
    rsd_project.rs_copy_project_extents(vbet_project_xml)

    return rsd_project


def copy_hillshade(rscontext_xml: str, output_dir: str) -> str:
    """
    Copy the DEM hillshade from the input Riverscapes Context project to the output folder.
    """

    log = Logger('DEM Hillshade')

    rsc_project = RSProject(None, rscontext_xml)
    nod_hillshade = rsc_project.XMLBuilder.find_by_id('HILLSHADE')
    if nod_hillshade is None:
        log.warning('No DEM Hillshade found in the input Riverscapes Context project')
        return

    input_hillshade = os.path.join(os.path.dirname(rscontext_xml), nod_hillshade.find('Path').text)
    output_hillshade = os.path.join(output_dir, 'topography', 'dem_hillshade.tif')
    safe_makedirs(os.path.dirname(output_hillshade))
    raster_warp(input_hillshade, output_hillshade, cfg.OUTPUT_EPSG)
    return output_hillshade


def process_classified_rasters(raster_folder: str, output_dir: str, dgos: str) -> List[Tuple[str, str, str]]:
    """
    Copy the input classified rasters for alluvial, vegetated, wetted to the output folder
    and clip them to the DGO polygons.

    Returns tuple of (raster_path, raster plain english display name, project raster ID)
    """

    log = Logger('Frequency Rasters')

    raster_paths = []
    for freq_type in ['00_WETTED', '01_ALLUVIAL', '02_VEGETATED']:
        sub_dir = os.path.join('frequency_binaries', freq_type)
        freq_dir = os.path.join(raster_folder, sub_dir)
        if not os.path.exists(freq_dir):
            log.warning(f'Frequency folder not found: {freq_dir}')
            continue

        for raster_file in os.listdir(freq_dir):
            if not raster_file.endswith('.tif'):
                continue

            match = re.match(classified_raster_pattern, raster_file)
            if not match:
                log.warning(f'Raster file does not match pattern: {raster_file}')
                continue

            raster_date_str = match.group(1)
            raster_class = match.group(2)

            # parse the date as yyyymmdd
            try:
                raster_date = datetime.strptime(raster_date_str, '%Y%m%d').date()
            except ValueError:
                log.warning(f'Invalid raster date format: {raster_date_str} in file {raster_file}')
                continue

            raster_name = f'Classified Binary - {raster_class.capitalize()} - {raster_date.strftime("%d %b %Y")}'
            raster_id = f'CLASS_{raster_class.upper()}_{raster_date.strftime("%Y%m%d")}'

            out_raster = os.path.join(output_dir, sub_dir, os.path.basename(raster_file))
            safe_makedirs(os.path.dirname(out_raster))
            raster_warp(os.path.join(freq_dir, raster_file), out_raster, cfg.OUTPUT_EPSG, dgos)
            raster_paths.append((out_raster, raster_name, raster_id))

    return raster_paths


def process_epoch_rasters(raster_folder: str, output_dir: str, vbet_dgos: str) -> List[Tuple[str, str, str, str]]:
    """
    Copy the epoch rasters (5yr, 30yr etc) to the output folder and clip them to the
    DGO ppolygons.

    Returns list of raster info tuple (
        raster_path,
        raster plain english display name,
        project raster ID,
        attribute prefix for DGO feature class
    )
    """

    log = Logger('Epoch Rasters')

    raster_paths = []
    epoch_parent_dir = os.path.join(raster_folder, 'frequency_outputs')
    if not os.path.exists(epoch_parent_dir):
        log.warning(f'Frequency outputs folder not found: {epoch_parent_dir}')
    else:
        # Get the subfolders that refer to each epoch
        for epoch in os.listdir(epoch_parent_dir):
            epoch_dir = os.path.join(epoch_parent_dir, epoch)
            if not os.path.isdir(epoch_dir):
                continue

            for raster_file in os.listdir(epoch_dir):
                if not raster_file.endswith('.tif'):
                    continue

                # Match the epoch pattern to get the epoch name
                match = re.match(epoch_raster_pattern, os.path.basename(raster_file))
                if match:
                    epoch_type = match.group(1)
                    epoch_length = match.group(2)
                    epoch_start_year = match.group(3)
                    epoch_end_year = match.group(6)
                    epoch_prefix = 'veg' if epoch_type == 'vegetation' else epoch_type

                    att_prefix = f'{epoch_length}yr-{epoch_prefix}-{epoch_start_year}-{epoch_end_year}'
                else:
                    log.warning(f'Could not parse epoch from raster name: {epoch}')
                    continue

                out_raster = os.path.join(output_dir, 'frequency_outputs', epoch, os.path.basename(raster_file))
                safe_makedirs(os.path.dirname(out_raster))
                raster_warp(os.path.join(epoch_dir, raster_file), out_raster, cfg.OUTPUT_EPSG, vbet_dgos)

                raster_name = f'Epoch Raster - {epoch_type.capitalize()} - {epoch_length} yr{"s" if int(epoch_length) > 1 else ""} - {epoch_start_year}-{epoch_end_year}'
                raster_id = f'EPOCH_{epoch_type.upper()}_{epoch_length}yr_{epoch_start_year}_{epoch_end_year}'
                raster_paths.append((out_raster, raster_name, raster_id, att_prefix))

    return raster_paths


def calc_raster_stats(raster_path: str, polygon_path: str, prefix: str) -> None:
    """
    Calculate zonal statistics for a raster based on polygons in a vector file.
    The stats are added as new columns to the input vector file and saved to a new
    output file (GeoPackage or Shapefile).
    """

    log = Logger('Raster Stats')
    log.info(f'Calculating raster stats for {prefix} at {raster_path}')

    # Load polygons from the feature class that is already in the RS Dynamics project
    gpkg, layer = VectorBase.path_sorter(polygon_path)
    gdf = gpd.read_file(gpkg, layer=layer)

    # add columns for the stats
    stats = ["mean", "min", "max", "std"]
    for stat in stats:
        gdf[f'{prefix}-{stat}'] = np.nan

    # Open the raster and calculate stats for each polygon
    means, mins, maxs, stds = [], [], [], []

    with rasterio.open(raster_path) as src:
        for _, row in gdf.iterrows():
            out_image, __out_transform = rasterio.mask.mask(src, [row.geometry], crop=True)
            data = out_image[0]
            data = data[data != src.nodata]
            data = data[~np.isnan(data)]
            if data.size > 0:
                means.append(float(data.mean()))
                mins.append(float(data.min()))
                maxs.append(float(data.max()))
                stds.append(float(data.std()))
            else:
                means.append(np.nan)
                mins.append(np.nan)
                maxs.append(np.nan)
                stds.append(np.nan)

    gdf[f'{prefix}-mean'] = means
    gdf[f'{prefix}-min'] = mins
    gdf[f'{prefix}-max'] = maxs
    gdf[f'{prefix}-std'] = stds

    # Write back to GeoPackage
    gdf.to_file(gpkg, layer=layer, driver="GPKG")


def main():
    """ Main entry point for New Zealand RS Context"""
    parser = argparse.ArgumentParser(description='Riverscapes Context Tool for New Zealand')
    parser.add_argument('watershed_id', help='Watershed/HUC identifier', type=int)
    parser.add_argument('vbet_project_xml', help='Path to VBET project XML file.', type=str)
    parser.add_argument('raster_folder', help='Path to the folder containing the river dynamics raster files', type=str)
    parser.add_argument('output_dir', help='Path to the output folder', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    log = Logger('RS Dynamics')
    log.setup(logPath=os.path.join(args.output_dir, 'rsdynamics.log'), verbose=args.verbose)
    log.title(f'Riverscapes Dynamics For Watershed: {args.watershed_id}')

    log.info(f'Watershed ID: {args.watershed_id}')
    log.info(f'Model Version: {__version__}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')
    log.info(f'Output folder: {args.output_dir}')

    try:
        rsdynamics(args.watershed_id, args.vbet_project_xml, args.raster_folder, args.output_dir)
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
