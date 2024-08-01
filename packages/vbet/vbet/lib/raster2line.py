""" Raster 2 Line

    Purpose:  Generate a line geometry from raster
    Author:   Kelly Whitehead
    Date:     Mar 28, 2022
    Source:   https://pcjericks.github.io/py-gdalogr-cookbook/raster_layers.html#raster-to-vector-line
"""

import os
import itertools
from math import sqrt

from osgeo import gdal, ogr
import numpy as np

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


def array2geom(array, rasterfn, pixelValue, precision=13):

    # max distance between points
    raster = gdal.Open(rasterfn)
    geotransform = raster.GetGeoTransform()
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    maxDistance = sqrt((pixelHeight ** 2 + pixelWidth ** 2))  # sqrt(2 * pixelWidth * pixelWidth)  # ceil() # pixelwidth * sqrt(2)
    maxDistance = maxDistance + maxDistance * 0.01
    # print(maxDistance)

    # array2dict
    count = 0
    roadList = np.where(array == pixelValue)
    # multipoint = ogr.Geometry(ogr.wkbMultiLineString)
    pointDict = {}
    for indexY in roadList[0]:
        indexX = roadList[1][count]
        Xcoord, Ycoord = pixelOffset2coord(rasterfn, indexX, indexY)
        pointDict[count] = (round(Xcoord, precision), round(Ycoord, precision))
        count += 1

    # dict2wkbMultiLineString
    line_segs = []
    coords = []
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
            # multiline.AddGeometry(line)
            coords.append(i[0])
            coords.append(i[1])
            line_segs.append(line)

    repeated_coords = []
    for coord in coords:
        if coords.count(coord) > 2:
            repeated_coords.append(coord)

    for line in line_segs:
        if (line.GetPoint(0)[0], line.GetPoint(0)[1]) in repeated_coords:
            if (line.GetPoint(1)[0], line.GetPoint(1)[1]) in repeated_coords:
                continue
        multiline.AddGeometry(line)

    return multiline


def raster2line(rasterfn, outSHPfn, pixelValue):
    array = raster2array(rasterfn)
    array2shp(array, outSHPfn, rasterfn, pixelValue)


def raster2line_geom(rasterfn, pixelValue):
    array = raster2array(rasterfn)
    geom = array2geom(array, rasterfn, pixelValue)
    return geom
