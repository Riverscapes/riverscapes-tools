# Name:     Download DEM
#
# Purpose:  Identify all the NED 10m DEM rasters that intersect with a HUC8
#           boundary polygon. Download and unzip all the rasters then mosaic
#           them into a single compressed GeoTIF raster possessing a specific
#           spatial reference.
#
# Author:   Philip Bailey
#
# Date:     15 Jun 2019
# -------------------------------------------------------------------------------
import argparse
import sys
import os
import traceback
from rscommons import Logger
from rscommons.raster_warp import raster_warp
from rscommons.util import safe_makedirs
from rscommons.download import download_file

base_url = 'https://web.corral.tacc.utexas.edu/nfiedata/HAND'


def download_hand(huc6, epsg, download_folder, boundary, raster_path, force_download=False, warp_options: dict = {}):
    """
    Identify rasters within HUC8, download them and mosaic into single GeoTIF
    :param vector_path: Path to bounding polygon ShapeFile
    :param epsg: Output spatial reference
    :param buffer_dist: Distance in DEGREES to buffer the bounding polygon
    :param output_folder: Temporary folder where downloaded rasters will be saved
    :param force_download: The download will always be performed if this is true.
    :param warp_options: Extra options to pass to raster warp
    :return:
    """

    log = Logger('HAND')

    file_path = os.path.join(download_folder, huc6 + 'hand.tif')
    raster_url = 'unknown'
    # First check the cache of 6 digit HUC rasters (if it exists)
    if os.path.isfile(file_path):
        temp_path = file_path
        log.info('Hand download file found. Skipping download: {}'.format(file_path))
    else:
        safe_makedirs(download_folder)
        # Download the HAND raster
        raster_url = '/'.join(s.strip('/') for s in [base_url, huc6, huc6 + 'hand.tif'])
        log.info('HAND URL {}'.format(raster_url))
        temp_path = download_file(raster_url, download_folder)

    # Reproject to the desired SRS and clip to the watershed boundary
    raster_warp(temp_path, raster_path, epsg, clip=boundary, warp_options=warp_options)

    return raster_path, raster_url


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('huc', help='HUC for which HAND will be downloaded', type=str)
    parser.add_argument('download_folder', help='Download folder where original raster will be downloaded', type=str)
    parser.add_argument('boundary', help='Watershed boundary to clip the final raster', type=argparse.FileType('r'))
    parser.add_argument('raster_path', help='Path where final HAND raster will be produced', type=str)
    parser.add_argument('--epsg', help='EPSG spatial reference of the final HAND raster', type=int, default=4326)
    args = parser.parse_args()

    try:
        download_hand(args.huc, args.epsg, args.download_folder, args.boundary.name, args.raster_path)

    except Exception as e:
        print(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
