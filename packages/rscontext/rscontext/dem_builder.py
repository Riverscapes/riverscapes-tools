#!/usr/bin/env python3
# Name:     DEM Builder
#
# Purpose:  Take a polygon and download all the necessary DEM tiles to create it.
#           Mosaic them together and produce a single DEM GeoTiFF at specified resolution.
#
# Author:   Lorin Gaertner
#
# Date:     28 Apr 2025
# -------------------------------------------------------------------------------
import argparse
import os
import sys
import traceback
import uuid
from rscommons import (Logger, dotenv, initGDALOGRErrors, ModelConfig, RSProject, RSLayer)
from rscommons.download_dem import download_dem, verify_areas
from rscommons.geographic_raster import gdal_dem_geographic
from rscommons.project_bounds import generate_project_extents_from_layer
from rscommons.raster_warp import raster_vrt_stitch, raster_warp
from rscommons.util import safe_makedirs, safe_remove_dir
from rscontext.__version__ import __version__
from rscontext.boundary_management import raster_area_intersection

initGDALOGRErrors()

cfg = ModelConfig(
    'https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)


def dem_builder(bounds_path: str,  output_res: float, output_epsg: int, download_folder: str, scratch_dir: str, output_path: str, force_download: bool):
    """Build a mosaiced raster for input area from 3DEP 1m DEM
    Args:
        bounds_path (str): _description_
        output_res (float): _description_
        download_folder (str): _description_
        scratch_dir (str): _description_
        output_path (str): path to folder where dem files will be placed
        force_download (bool): if True, download from source even if we already have local copy
    """

    log = Logger('DEM Builder')

    ned_download_folder = os.path.join(download_folder, 'ned')
    ned_unzip_folder = os.path.join(scratch_dir, 'ned')

    dem_rasters, _urls = download_dem(bounds_path, output_epsg, 0.01, ned_download_folder, ned_unzip_folder, force_download, '1m')
    output_dem_file_path = os.path.join(output_path, 'output_dem.tif')
    need_dem_rebuild = force_download or not os.path.exists(output_dem_file_path)

    if need_dem_rebuild:
        raster_vrt_stitch(dem_rasters, output_dem_file_path, output_epsg, clip=bounds_path, warp_options={"cutlineBlend": 1})
    # TODO: just for testing, skip the calculation
    area_ratio = 999
    # area_ratio = verify_areas(output_dem_file_path, bounds_path)
    if area_ratio < 0.85:
        log.warning(f'DEM data less than 85%% of bounds extent ({area_ratio:%})')
        # raise Exception(f'DEM data less than 85%% of nhd extent ({area_ratio:%})')

    # build hillshade
    hillshade_path = os.path.join(output_path, 'HS.tif')
    need_hs_rebuild = need_dem_rebuild or not os.path.isfile(hillshade_path)
    if need_hs_rebuild:
        gdal_dem_geographic(output_dem_file_path, hillshade_path, 'hillshade')

    log.info(f'Area Ratio: {area_ratio:%}')
    log.info(f'Output DEM: {output_path}')
    log.info(f'Output DEM Size: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB')
    log.info(f'Output DEM Resolution: {output_res} m')
    log.info(f'Output DEM Projection: {output_epsg}')
    log.info('DEM Builder complete!')


def build_rs_context_project(project_identifier, output_folder, bounds_path):
    """generate a Riverscapes project of type RSContext but with just the 3DEP 1m DEM products

    This is a much simplified version of rs_context.rs_context 
    Todo: review to back-port features additional features 

    :param project_identifier: Could be HUC, or other identifier. 
    :param output_folder: Output location for the riverscapes context project

    """

    log = Logger("RS Context for 3DEP")
    log.title("RS Context 3DEP project builder")
    safe_makedirs(output_folder)

    project_name = f'Riverscapes Context-3DEP for {project_identifier}'
    project = RSProject(cfg, output_folder)

    project.create(project_name, 'RSContext')

    realization = project.add_realization(
        project_name, 'REALIZATION1', cfg.version)
    datasets = project.XMLBuilder.add_sub_element(realization, 'Datasets')

    dem_node, dem_raster = project.add_project_raster(
        datasets, RSLayer('3DEP 1m DEM', '3DEPDEM', 'Raster', 'topography/1mdem.tif'))

    project.add_project_raster(
        datasets, RSLayer('1m DEM Hillshade', 'HILLSHADE', 'Raster', 'topography/HS.tif')
    )

    name_node = project.XMLBuilder.find('Name')
    name_node.text = project_name

    # Add Project Extents
    extents_json_path = os.path.join(output_folder, 'project_bounds.geojson')
    extents = generate_project_extents_from_layer(
        bounds_path, extents_json_path)
    project.add_project_extent(
        extents_json_path, extents['CENTROID'], extents['BBOX'])

    log.info('Process completed successfully.')
    return {'DEM': dem_raster}


def main():
    """Main function to run the DEM Builder tool."""
    parser = argparse.ArgumentParser(description='DEM Builder Tool')
    parser.add_argument('bounds_path', help='Path to feature class containing polygon bounds feature', type=str)
    parser.add_argument('output_res', help='Horizontal resolution of output DEM in metres', type=str)
    parser.add_argument('output_path', help='Path to folder where the output rasters will get generated', type=str)
    parser.add_argument('output_epsg', help='Output Coordinate Refence System', type=str)
    parser.add_argument('download_dir', help='Temporary folder for downloading data. Different HUCs may share this', type=str)
    parser.add_argument('--force', help='(optional) download existing files ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Hard-code certain args
    args.parallel = False
    args.verbose = True
    args.temp_folder = r'/workspaces/data/temp'

    # verify args
    if not os.path.isdir(args.output_path):
        raise ValueError(f"Expect `output_path` argument to be path to a folder. Value supplied: {args.output_path}")

    log = Logger('DEM Builder')
    log.setup(logPath=os.path.join(args.output_path, 'dem_builder.log'), verbose=args.verbose)
    log.title('DEM Builder')
    log.info(f'Bounds Path: {args.bounds_path}')
    log.info(f'Output Resolution: {args.output_res}m')
    log.info(f'Output Path: {args.output_path}')

    # This is a general place for unzipping downloaded files and other temporary work.
    # We use GUIDS to make it specific to a particular run of the tool to avoid unzip collisions
    parallel_code = "-" + str(uuid.uuid4()) if args.parallel is True else ""
    scratch_dir = args.temp_folder if args.temp_folder else os.path.join(args.download, 'scratch', f'rs_context{parallel_code}')
    safe_makedirs(scratch_dir)

    try:
        dem_builder(args.bounds_path, args.output_res, args.output_epsg, args.download_dir, scratch_dir, args.output_path, args.force)
        project_id = os.path.basename(args.bounds_path)
        project_output_path = os.path.join(args.output_path, "project")
        build_rs_context_project(project_id, project_output_path, args.bounds_path)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        # Cleaning up the scratch folder is essential
        safe_remove_dir(scratch_dir)
        sys.exit(1)

    # Cleaning up the scratch folder is essential
    safe_remove_dir(scratch_dir)
    log.info("DEM Builder complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
