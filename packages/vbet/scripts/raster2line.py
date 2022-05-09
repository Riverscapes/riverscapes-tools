#
import os
import sys
import argparse
import itertools
from math import sqrt, ceil

import ogr
import gdal
import numpy as np

from rscommons import dotenv
from rscommons.util import safe_makedirs
from vbet. vbet_raster_ops import raster2array


def pixelOffset2coord(rasterfn, xOffset, yOffset):
    raster = gdal.Open(rasterfn)
    geotransform = raster.GetGeoTransform()
    originX = geotransform[0]
    originY = geotransform[3]
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    coordX = originX + pixelWidth * xOffset + pixelWidth / 2
    coordY = originY + pixelHeight * yOffset + pixelHeight / 2
    return coordX, coordY


def array2shp(array, outSHPfn, rasterfn, pixelValue):

    multiline = array2geom(array, rasterfn, pixelValue)

    # wkbMultiLineString2shp
    shpDriver = ogr.GetDriverByName("ESRI Shapefile")
    if os.path.exists(outSHPfn):
        shpDriver.DeleteDataSource(outSHPfn)
    outDataSource = shpDriver.CreateDataSource(outSHPfn)
    outLayer = outDataSource.CreateLayer(outSHPfn, geom_type=ogr.wkbMultiLineString)
    featureDefn = outLayer.GetLayerDefn()
    outFeature = ogr.Feature(featureDefn)
    outFeature.SetGeometry(multiline)
    outLayer.CreateFeature(outFeature)


def array2geom(array, rasterfn, pixelValue):

    # max distance between points
    raster = gdal.Open(rasterfn)
    geotransform = raster.GetGeoTransform()
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    maxDistance = sqrt((pixelHeight ** 2 + pixelWidth ** 2))  # sqrt(2 * pixelWidth * pixelWidth)  # ceil() # pixelwidth * sqrt(2)
    maxDistance = maxDistance + maxDistance * 0.01
    print(maxDistance)

    # array2dict
    count = 0
    roadList = np.where(array == pixelValue)
    # multipoint = ogr.Geometry(ogr.wkbMultiLineString)
    pointDict = {}
    for indexY in roadList[0]:
        indexX = roadList[1][count]
        Xcoord, Ycoord = pixelOffset2coord(rasterfn, indexX, indexY)
        pointDict[count] = (Xcoord, Ycoord)
        count += 1

    # dict2wkbMultiLineString
    multiline = ogr.Geometry(ogr.wkbMultiLineString)
    for i in itertools.combinations(pointDict.values(), 2):
        point1 = ogr.Geometry(ogr.wkbPoint)
        point1.AddPoint(i[0][0], i[0][1])
        point2 = ogr.Geometry(ogr.wkbPoint)
        point2.AddPoint(i[1][0], i[1][1])

        distance = point1.Distance(point2)

        if distance < maxDistance:
            line = ogr.Geometry(ogr.wkbLineString)
            line.AddPoint(i[0][0], i[0][1])
            line.AddPoint(i[1][0], i[1][1])
            multiline.AddGeometry(line)

    return multiline


def raster2line(rasterfn, outSHPfn, pixelValue):
    array = raster2array(rasterfn)
    array2shp(array, outSHPfn, rasterfn, pixelValue)


def raster2line_geom(rasterfn, pixelValue):
    array = raster2array(rasterfn)
    geom = array2geom(array, rasterfn, pixelValue)
    return geom


def main():

    parser = argparse.ArgumentParser(
        description='Riverscapes VBET Tool',
        # epilog="This is an epilog"
    )
    # CostSurfacefn = 'CostSurface.tif'
    # startCoord = (345387.871, 1267855.277)
    # stopCoord = (345479.425, 1267799.626)
    # outputPathfn = 'Path.tif'

    parser.add_argument('cost_surface', help='cost surface', type=str)
    parser.add_argument('start_coord', help='from coordinate', type=str)
    parser.add_argument('end_coord', help='to coordinate', type=str)
    parser.add_argument('outname', type=str)
    parser.add_argument('output_dir', help='Folder where output VBET project will be created', type=str)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='Add debug tools for tracing things like memory usage at a performance cost.', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # make sure the output folder exists
    safe_makedirs(args.output_dir)

    # try:
    #     if args.debug is True:
    #         from rscommons.debug import ThreadRun
    #         memfile = os.path.join(args.output_dir, 'vbet_mem.log')
    #         retcode, max_obj = ThreadRun(vbet, memfile, args.huc, args.scenario_code, inputs, args.vaa_table, args.output_dir, reach_codes, meta)
    #         log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))

    #     else:
    out_path = os.path.join(args.output_dir, args.outname)
    rasterfn = 'test.tif'
    outSHPfn = 'test.shp'
    pixelValue = 0
    raster2line(rasterfn, outSHPfn, pixelValue)

    # except Exception as e:
    #     log.error(e)
    #     traceback.print_exc(file=sys.stdout)
    #     sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
