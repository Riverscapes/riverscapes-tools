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
from rscommons import (Logger, dotenv, initGDALOGRErrors)
from rscommons.util import (safe_makedirs, safe_remove_dir)
from rscommons.download_dem import download_dem, verify_areas
from rscommons.vector_ops import get_geometry_unary_union
from rscontext.boundary_management import raster_area_intersection
from rscommons.raster_warp import raster_vrt_stitch, raster_warp


def dem_builder(bounds_path: str, parent_guid: str, output_res: float, output_epsg: int, download_folder: str, scratch_dir: str, output_path: str, force_download: bool):
    """_summary_

    Args:
        bounds_path (str): _description_
        parent_guid (str): _description_
        output_res (float): _description_
        download_folder (str): _description_
        scratch_dir (str): _description_
        output_path (str): _description_
    """

    log = Logger('DEM Builder')

    # Load the geometries from the bounds feature class and union them into a single geometry
    # Note that this function can do other things, such as force the geometry onto a specific projection
    # download_dem also does something similar -- i don't know if we need to
    bounds_polygon = get_geometry_unary_union(bounds_path)

    ned_download_folder = os.path.join(download_folder, 'ned')
    ned_unzip_folder = os.path.join(scratch_dir, 'ned')

    dem_rasters, _urls = download_dem(bounds_path, output_epsg, 0.01, ned_download_folder, ned_unzip_folder, force_download)

    raster_vrt_stitch(dem_rasters, output_path, output_epsg, clip=bounds_path, warp_options={"cutlineBlend": 1})
    area_ratio = verify_areas(output_path, bounds_polygon)
    if area_ratio < 0.85:
        log.warning(f'DEM data less than 85%% of bounds extent ({area_ratio:%})')
        # raise Exception(f'DEM data less than 85%% of nhd extent ({area_ratio:%})')

    log.info(f'Area Ratio: {area_ratio:%}')
    log.info(f'Output DEM: {output_path}')
    log.info(f'Output DEM Size: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB')
    log.info(f'Output DEM Resolution: {output_res} m')
    log.info(f'Output DEM Projection: {output_epsg}')
    log.info('DEM Builder complete!')


def main():
    """Main function to run the DEM Builder tool."""
    parser = argparse.ArgumentParser(description='DEM Builder Tool')
    parser.add_argument('bounds_path', help='Path to feature class containing polygon bounds feature', type=str)
    parser.add_argument('parent_guid', help='GUID of the parent DEM record in the National Map Catalog', type=str)
    parser.add_argument('output_res', help='Horizontal resolution of output DEM in metres', type=str)
    parser.add_argument('output_path', help='Path where the output raster will get generated', type=str)
    parser.add_argument('output_epsg', help='Output Coordinate Refence System', type=str)
    parser.add_argument('download_dir', help='Temporary folder for downloading data. Different HUCs may share this', type=str)
    parser.add_argument('--force', help='(optional) download existing files ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Hard-code args.parallel and args.verbose
    args.parallel = False
    args.verbose = True
    args.temp_folder = r'/workspaces/data/temp'

    log = Logger('DEM Builder')
    log.setup(logPath=os.path.join(os.path.dirname(args.output_path), 'dem_builder.log'), verbose=args.verbose)
    log.title('DEM Builder')
    log.info(f'Bounds Path: {args.bounds_path}')
    log.info(f'Parent GUID: {args.parent_guid}')
    log.info(f'Output Resoluation: {args.output_res}m')
    log.info(f'Output Path: {args.output_path}')

    # This is a general place for unzipping downloaded files and other temporary work.
    # We use GUIDS to make it specific to a particular run of the tool to avoid unzip collisions
    parallel_code = "-" + str(uuid.uuid4()) if args.parallel is True else ""
    scratch_dir = args.temp_folder if args.temp_folder else os.path.join(args.download, 'scratch', f'rs_context{parallel_code}')
    safe_makedirs(scratch_dir)

    try:
        dem_builder(args.bounds_path, args.parent_guid, args.output_res, args.output_epsg, args.download_dir, scratch_dir, args.output_path, args.force)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        # Cleaning up the scratch folder is essential
        safe_remove_dir(scratch_dir)
        sys.exit(1)

    # Cleaning up the scratch folder is essential
    safe_remove_dir(scratch_dir)
    sys.exit(0)


if __name__ == "__main__":
    main()
