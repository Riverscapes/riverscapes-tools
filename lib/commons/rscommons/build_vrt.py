# Name:     Build VRT
#
# Purpose:  Build a GDAL virtual dataset file for images found in a directory.
#           This script crawls a directory, including subdirectories, for any
#           ERDAS (*.img) and GeoTIF (*.tif) files and then produces a single
#           VRT file that references all the raster files. This VRT can then
#           be used as the input to other GDAL processing tools.
#           https://gdal.org/drivers/raster/vrt.html
#
# Author:   Philip Bailey
#
# Date:     24 May 2019
# -------------------------------------------------------------------------------
import argparse
import os
import sys
import traceback
from osgeo import gdal
from rscommons import Logger


def build_vrt(search_dir, vrt):
    """
    Build a VRT file for images found in a directory and subdirectories.
    :param search_dir: Top level directory that will be searched for *.img and *.tif raster files
    :param vrt:  Output VRT file
    :return: None
    """

    log = Logger("Build VRT")

    if not os.path.isdir(search_dir):
        raise Exception('Directory specified does not exist: {}'.format(search_dir))

    rasters = []
    for root, _sub_folder, files in os.walk(search_dir):
        for item in files:
            if item.endswith('.img') or item.endswith('.tiff'):
                rasters.append(os.path.join(root, item))

    log.info('{} rasters found in {}'.format(len(rasters), search_dir))
    log.info('Generating VRT file to {}'.format(vrt))

    gdal.BuildVRT(vrt, rasters)

    log.info('Process completed successfully.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir', help='Folder to search for image files', type=str)
    parser.add_argument('vrt', help='Output path for VRT file', type=str)
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)

    args = parser.parse_args()

    # make sure the output folder exists
    results_folder = os.path.dirname(args.vrt)
    if not os.path.isdir(results_folder):
        os.mkdir(results_folder)

    # Initiate the log file
    logg = Logger("Build VRT")
    logfile = os.path.join(results_folder, "build_vrt.log")
    logg.setup(logPath=logfile, verbose=args.verbose)

    try:
        build_vrt(args.dir, args.vrt)

    except Exception as e:
        logg.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
