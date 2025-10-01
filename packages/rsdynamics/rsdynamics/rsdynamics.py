"""
Name:       Riverscapes Dynamics

Purpose:    Build a Riverscapes Dynamics project for a single New Zealand watershed

Setup:

Author:     Philip Bailey

Date:       1 Oct 2025
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
import copy
from typing import Tuple, Dict, List
import argparse
import sqlite3
import json
import os
import sys
import traceback
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


cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)
cfg.OUTPUT_EPSG = 2193  # NZTM

initGDALOGRErrors()


LayerTypes = {
    # key: (name, id, tag, relpath)
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'topography/dem_hillshade.tif'),
}


def rsdynamics(watershed_id: str, rscontext_xml: str, dgos: str, raster_folder: str, output_dir: str):
    """ Main function for running Riverscapes Dynamics"""

    input_rasters = project_input_rasters(raster_folder, output_dir, dgos)
    epoch_rasters = project_epoch_rasters(raster_folder, output_dir, dgos)
    hillshade_raster = copy_hillshade(rscontext_xml, output_dir)

    dgos_layer = os.path.join(output_dir, 'dgos', 'rsdynamics.gpkg')
    copy_feature_class(dgos, dgos_layer, epsg=cfg.OUTPUT_EPSG)

    for epoch in epoch_rasters:
        if '30yr' in os.path.dirname(epoch):
            calc_raster_stats(epoch, dgos_layer, '30yr')

    # Create the output Riverscapes project.rs.xml based on the rscontext XML
    rsd_project = build_dynamics_project(rscontext_xml, output_dir)

    nod_realization = rsd_project.add_realization('Riverscapes Dynamics', 'REALIZATION1', cfg.version)
    rsd_project.add_project_raster(nod_realization, LayerTypes['HILLSHADE'])

    for i, epoch in enumerate(epoch_rasters):
        layer_name = os.path.splitext(os.path.basename(os.path.dirname(epoch)))[0]
        layer_path = rsd_project.get_relative_path(epoch)
        layer_rs = RSLayer(layer_name, f'EPOCH_{i}', 'Raster', layer_path)
        rsd_project.add_project_raster(nod_realization, layer_rs)

    for i, raster in enumerate(input_rasters):
        layer_name = os.path.splitext(os.path.basename(raster))[0]
        layer_path = rsd_project.get_relative_path(raster)
        layer_rs = RSLayer(layer_name, f'FREQ_{i}', 'Raster', layer_path)
        rsd_project.add_project_raster(nod_realization, layer_rs)

    return rsd_project


def build_dynamics_project(rscontext_xml: str, output_dir: str) -> RSProject:
    """
    Create a new Riverscapes project for Riverscapes Dynamics based on the input
    Riverscapes Context project.
    """

    # Create the output Riverscapes project
    rsc_project = RSProject(None, rscontext_xml)
    rsc_project_meta = rsc_project.get_metadata_dict()
    rsc_project_name = rsc_project.XMLBuilder.find('Name')

    rsd_project_meta = copy.deepcopy(rsc_project_meta)
    rsd_project_meta['Model Documentation'] = 'https://tools.riverscapes.net/rsdynamics'
    rsd_project_meta['Model Version'] = __version__

    # Build the Riverscapes Context project
    project_name = rsc_project_name('Context', 'Dynamics')
    rsd_project = RSProject(cfg, output_dir)
    rsd_project.create(project_name, 'rsdynamics', rsd_project_meta)

    # Reuse the project extents from the input Riverscapes Context project
    rsd_project.rs_copy_project_extents(rscontext_xml)

    return rsd_project


def copy_hillshade(rscontext_xml: str, output_dir: str) -> str:
    """
    Copy the DEM hillshade from the input Riverscapes Context project to the output folder.
    """

    log = Logger('DEM Hillshade')

    rsc_project = RSProject(None, rscontext_xml)
    nod_hillshade = rsc_project.XMLBuilder.find('Raster[@id="HILLSHADE"]')
    if nod_hillshade is None:
        log.warning('No DEM Hillshade found in the input Riverscapes Context project')
        return

    input_hillshade = os.path.join(os.path.dirname(rscontext_xml), nod_hillshade.find('Path').text)
    output_hillshade = os.path.join(output_dir, 'topography', 'dem_hillshade.tif')
    raster_warp(input_hillshade, output_hillshade, cfg.OUTPUT_EPSG)
    return output_hillshade


def project_input_rasters(raster_folder: str, output_dir: str, dgos: str) -> List[str]:
    """
    Copy the input classified rasters for alluvial, vegetated, wetted to the output folder
    and clip them to the DGO polygons.
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

            out_raster = os.path.join(output_dir, sub_dir, os.path.basename(raster_file))
            raster_warp(out_raster, out_raster, cfg.OUTPUT_EPSG, dgos)
            raster_paths.append(out_raster)

    return raster_paths


def project_epoch_rasters(raster_folder: str, output_dir: str, dgos: str) -> List[str]:
    """
    Copy the epoch rasters (5yr, 30yr etc) to the output folder and clip them to the
    DGO ppolygons.
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

                out_raster = os.path.join(output_dir, 'frequency_outputs', epoch, os.path.basename(raster_file))
                raster_warp(os.path.join(epoch_dir, raster_file), out_raster, cfg.OUTPUT_EPSG, dgos)
                raster_paths.append(out_raster)

    return raster_paths


def calc_raster_stats(raster_path: str, polygon_path: str, prefix: str) -> None:
    """
    Calculate zonal statistics for a raster based on polygons in a vector file.
    The stats are added as new columns to the input vector file and saved to a new
    output file (GeoPackage or Shapefile).
    """

    # Load polygons from the feature class that is already in the RS Dynamics project
    gdf = gpd.read_file(polygon_path)
    with rasterio.open(raster_path) as src:
        results = []
        for _, row in gdf.iterrows():
            # Mask raster by polygon
            out_image, __out_transform = rasterio.mask.mask(src, [row.geometry], crop=True)
            data = out_image[0]
            data = data[data != src.nodata]  # remove nodata

            # Compute stats
            if data.size > 0:
                stats = {
                    "mean": float(data.mean()),
                    "min": float(data.min()),
                    "max": float(data.max()),
                    "std": float(data.std())
                }
            else:
                stats = None

            results.append(stats)

    # Add each stat as a new column
    for stat_name in stats[0].keys():
        gdf[f'{prefix}_{stat_name}'] = [s[stat_name] for s in stats]

    # Write polygons to the feature class that is already in the RS Dynamics project
    gdf.to_file(polygon_path, driver="GPKG")


def main():
    """ Main entry point for New Zealand RS Context"""
    parser = argparse.ArgumentParser(description='Riverscapes Context Tool for New Zealand')
    parser.add_argument('watershed_id', help='Watershed/HUC identifier', type=int)
    parser.add_argument('rscontext_xml', help='Path to Riverscapes project XML file.', type=str)
    parser.add_argument('dgos', help='Path to the DGOs vector feature class', type=str)
    parser.add_argument('raster_folder', help='Path to the folder containing the river dynamics raster files', type=str)
    parser.add_argument('output_dir', help='Path to the output folder', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about things like memory usage. There is a performance cost', action='store_true', default=False)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)

    args = dotenv.parse_args_env(parser)

    log = Logger('RS Context')
    log.setup(logPath=os.path.join(args.output_dir, 'RSContextNZ.log'), verbose=args.verbose)
    log.title(f'Riverscapes Context NZ For Watershed: {args.watershed_id}')

    log.info(f'Watershed ID: {args.watershed_id}')
    log.info(f'Model Version: {__version__}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')
    log.info(f'Output folder: {args.output_dir}')

    try:
        rsdynamics(args.watershed_id, args.rscontext_xml, args.dgos, args.raster_folder, args.output)
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
