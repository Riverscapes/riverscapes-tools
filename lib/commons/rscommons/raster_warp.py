# Name:     Raster Warp
#
# Purpose:  Reproject a raster to a different spatial reference
#           https://www.gdal.org/gdalwarp.html
#
#           Note that GDAL warp is known to produce poorly compressed rasters.
#           Therefore generate the WARP to a virtual dataset (VRT) and then
#           send this VRT file to GDAL translate for improved compression.
#           In tests a single Warp process that produced a 4Gb raster instead
#           produced a 300Mg raster using this two step process. Also, the warp
#           to VRT will be quick. The translate will take the time.
#
# Author:   Philip Bailey
#
# Date:     17 May 2019
# -------------------------------------------------------------------------------
import argparse
import tempfile
import os
import sys
import traceback
import gdal
from rscommons.download import get_unique_file_path
from rscommons import Logger


def raster_vrt_stitch(inrasters, outraster, epsg, clip=None):
    """[summary]
    https://gdal.org/python/osgeo.gdal-module.html#BuildVRT
    Keyword arguments are :
        options --- can be be an array of strings, a string or let empty and filled from other keywords..
        resolution --- 'highest', 'lowest', 'average', 'user'.
        outputBounds --- output bounds as (minX, minY, maxX, maxY) in target SRS.
        xRes, yRes --- output resolution in target SRS.
        targetAlignedPixels --- whether to force output bounds to be multiple of output resolution.
        separate --- whether each source file goes into a separate stacked band in the VRT band.
        bandList --- array of band numbers (index start at 1).
        addAlpha --- whether to add an alpha mask band to the VRT when the source raster have none.
        resampleAlg --- resampling mode.
            near: nearest neighbour resampling (default, fastest algorithm, worst interpolation quality).
            bilinear: bilinear resampling.
            cubic: cubic resampling.
            cubicspline: cubic spline resampling.
            lanczos: Lanczos windowed sinc resampling.
            average: average resampling, computes the average of all non-NODATA contributing pixels.
            mode: mode resampling, selects the value which appears most often of all the sampled points.
            max: maximum resampling, selects the maximum value from all non-NODATA contributing pixels.
            min: minimum resampling, selects the minimum value from all non-NODATA contributing pixels.
            med: median resampling, selects the median value of all non-NODATA contributing pixels.
            q1: first quartile resampling, selects the first quartile value of all non-NODATA contributing pixels.
            q3: third quartile resampling, selects the third quartile value of all non-NODATA contributing pixels.
        outputSRS --- assigned output SRS.
        allowProjectionDifference --- whether to accept input datasets have not the same projection. Note: they will *not* be reprojected.
        srcNodata --- source nodata value(s).
        VRTNodata --- nodata values at the VRT band level.
        hideNodata --- whether to make the VRT band not report the NoData value.
        callback --- callback method.
        callback_data --- user data for callback.
    """
    log = Logger('Raster Stitch')

    # Build a virtual dataset that points to all the rasters then mosaic them together
    # clipping out the HUC boundary and reprojecting to the output spatial reference
    path_vrt = get_unique_file_path(os.path.dirname(outraster), os.path.basename(outraster).split('.')[0] + '.vrt')

    log.info('Building temporary vrt: {}'.format(path_vrt))
    vrt_options = gdal.BuildVRTOptions()
    gdal.BuildVRT(path_vrt, inrasters, options=vrt_options)

    raster_warp(path_vrt, outraster, epsg, clip)

    # Clean up the VRT
    # if os.path.isfile(path_vrt):
    #     log.info('Cleaning up VRT file: {}'.format(path_vrt))
    #     os.remove(path_vrt)


def raster_warp(inraster, outraster, epsg, clip=None):
    """
    Reproject a raster to a different coordinate system.
    :param inraster: Input dataset
    :param outraster: Output dataset
    :param epsg: Output spatial reference EPSG identifier
    :param log: Log file object
    :param clip: Optional Polygon dataset to clip the output.
    :return: None
    """

    log = Logger('Raster Warp')

    if os.path.isfile(outraster):
        log.info('Skipping raster warp because output exists {}'.format(outraster))
        return None

    log.info('Raster Warp input raster {}'.format(inraster))
    log.info('Raster Warp output raster {}'.format(outraster))
    log.info('Output spatial reference EPSG: {}'.format(epsg))

    output_folder = os.path.dirname(outraster)
    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)
    warpvrt = os.path.join(os.path.dirname(outraster), 'temp_gdal_warp_output.vrt')

    log.info('Performing GDAL warp to temporary VRT file.')
    warp_options = gdal.WarpOptions(dstSRS='EPSG:{}'.format(epsg), format='vrt')
    if clip:
        log.info('Clipping to polygons using {}'.format(clip))
        warp_options = gdal.WarpOptions(dstSRS='EPSG:{}'.format(epsg), format='vrt', cutlineDSName=clip, cropToCutline=True)

    ds = gdal.Warp(warpvrt, inraster, options=warp_options)

    log.info('Using GDAL translate to convert VRT to compressed raster format.')
    translateoptions = gdal.TranslateOptions(gdal.ParseCommandLine("-of Gtiff -co COMPRESS=DEFLATE"))
    gdal.Translate(outraster, ds, options=translateoptions)

    # Cleanup the temporary VRT file
    os.remove(warpvrt)

    if ds:
        log.info('Process completed successfully.')
    else:
        log.error('Error running GDAL Warp')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('inraster', help='Input raster', type=str)
    parser.add_argument('outraster', help='Output raster', type=str)
    parser.add_argument('epsg', help='Output spatial reference EPSG', type=int)
    parser.add_argument('clip', help='Polygon ShapeFile to clip the output raster', type=argparse.FileType('r'))
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)

    args = parser.parse_args()

    # Initiate the log file
    log = Logger("Raster Warp")
    log.setup(logPath=os.path.join(os.path.dirname(args.outraster), "raster_warp.log"))

    # make sure the output folder exists
    results_folder = os.path.dirname(args.outraster)
    if not os.path.isdir(results_folder):
        os.mkdir(results_folder)

    if os.path.isfile(args.outraster):
        log.info('Deleting existing output raster: {}'.format(args.outraster))
        driver = gdal.GetDriverByName('GTiff')
        gdal.Driver.Delete(driver, args.outraster)

    try:
        raster_warp(args.inraster, args.outraster, args.epsg, args.clip.name if args.clip else None)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
