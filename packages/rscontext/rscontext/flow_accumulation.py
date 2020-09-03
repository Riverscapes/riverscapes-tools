# Name:     Flow Accumulation
#
# Purpose:  Calculates a flow accumulation using PyGeoProcessing
#           This uses David Tarboton's multi-flow-direction algorithm
#
# Author:   Philip Bailey
#
# Date:     23 May 2019
#
# Notes:    PyGeoprocessing
#           https://bitbucket.org/natcap/pygeoprocessing/src/develop/
# -------------------------------------------------------------------------------
import argparse
import os
import sys
import traceback
import shutil
import gdal
# Pygeoprocessing has a bug where it looks like fill_pits doesn't exist
# No idea what to do about this
from pygeoprocessing.routing import fill_pits, flow_accumulation_d8, flow_accumulation_mfd, flow_dir_mfd, flow_dir_d8
import rasterio
from rscommons import Logger, dotenv


def flow_accumulation(dem, flow_accum, cleanup=True, dinfinity=False, pitfill=False):
    """
    Calculate reach length, slope, drainage area, min and max elevations
    and write them as attributes to the network
    :param dem: Absolute path to a DEM raster.
    :param flow_accum: Absolute path to a flow accumulation raster (cell counts)
    :param cleanup: determines whether intermediate rasters are deleted.
    :param dinfinity: If true then the dInfinity otherwise d8 algorithm
    :param pitfill: If true then DEM is pit filled before flow accumulation
    :return: None
    """

    log = Logger('Flow Accum')

    if os.path.isfile(flow_accum):
        log.info('Skipping flow accumulation because output exists at {}'.format(flow_accum))
        return None

    tempfolder = os.path.join(os.path.dirname(flow_accum), 'temp')
    cleanup_temp_folder(tempfolder)
    if not os.path.isdir(tempfolder):
        os.mkdir(tempfolder)

    outputDir = os.path.dirname(flow_accum)
    tempPitFill = os.path.join(tempfolder, 'temp_pitfill.tif')
    tempFlowDir = os.path.join(tempfolder, 'temp_flowDir.tif')

    prepared_dem = dem
    if pitfill:
        log.info('Filling pits in DEM and writing to {}'.format(tempPitFill))
        fill_pits((dem, 1), tempPitFill, working_dir=outputDir)
        prepared_dem = tempPitFill

    log.info('Calculating flow direction in pit filled raster and writing to: {}'.format(tempFlowDir))
    if dinfinity:
        flow_dir_mfd((prepared_dem, 1), tempFlowDir)
    else:
        flow_dir_d8((prepared_dem, 1), tempFlowDir)

    log.info('Calculating flow accumulation raster and writing to: {}'.format(flow_accum))
    if dinfinity:
        flow_accumulation_mfd((tempFlowDir, 1), flow_accum)
    else:
        flow_accumulation_d8((tempFlowDir, 1), flow_accum)

    if cleanup:
        cleanup_temp_folder(tempfolder)

    log.info('Flow accumulation completed successfully.')


def cleanup_temp_folder(folder):

    if os.path.isdir(folder):
        print('Cleaning up temporary files.')
        shutil.rmtree(folder)


def flow_accum_to_drainage_area(flow_accum, drainage_area):

    log = Logger('Flow Accum')

    if os.path.isfile(drainage_area):
        log.info('Skipping conversion of flow accumulation to drainage area because file exists.')
        return

    log.info('Converting flow accumulation to drainage area raster.')

    with rasterio.open(flow_accum) as src:
        chl_meta = src.meta
        chl_meta['compress'] = 'deflate'
        with rasterio.open(drainage_area, 'w', **chl_meta) as dst:
            affine = src.meta['transform']
            cell_area = abs(affine[0] * affine[4]) / 1000000

            for ji, window in src.block_windows(1):
                array = src.read(1, window=window, masked=True)
                result = array * cell_area
                dst.write(result, window=window, indexes=1)

            # TODO: write some basic statistics of the drainage area raster to the log file.

    log.info('Drainage area raster created at {}'.format(drainage_area))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dem', help='DEM raster', type=str)
    parser.add_argument('flowaccum', help='Flow accumulation raster', type=str)
    parser.add_argument('drainagearea', help='Drainage Area output raster', type=str)
    parser.add_argument('--cleanup', help='Deletes temporary files', action='store_true', default=False)
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)
    parser.add_argument('--dinfinity', help='(optional) Use the Dinifinity algorthim. D8 used if omitted', action='store_true', default=False)
    parser.add_argument('--pitfill', help='(optional) Fill DEM pits before flow direction', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("Flow Accum")
    log.setup(logPath=os.path.join(os.path.dirname(args.flowaccum), "flow_accum.log"))

    if os.path.isfile(args.flowaccum):
        log.info('Deleting existing output raster {}'.format(args.flowaccum))
        driver = gdal.GetDriverByName('GTiff')
        gdal.Driver.Delete(driver, args.flowaccum)

    try:
        flow_accumulation(args.dem, args.flowaccum, args.cleanup, args.dinfinity, args.pitfill)
        flow_accum_to_drainage_area(args.flowaccum, args.drainagearea)

    except Exception as e:
        print(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
