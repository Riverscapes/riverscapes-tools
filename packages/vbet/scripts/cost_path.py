# Name:     Least Cost Path
#
# Purpose:  Find the least cost path between tow points through a raster surface
#
#
# Author:   Kelly Whitehead
#
# Date:     Mar 28, 2022
#
# Source:   https://pcjericks.github.io/py-gdalogr-cookbook/raster_layers.html#create-least-cost-path
# -------------------------------------------------------------------------------

import sys
import os
import argparse

import gdal
# import osr
from skimage.graph import route_through_array
import numpy as np

from rscommons import dotenv
from rscommons.util import safe_makedirs

from vbet.vbet_raster_ops import raster2array, array2raster


def coord2pixelOffset(rasterfn, x, y):
    raster = gdal.Open(rasterfn)
    geotransform = raster.GetGeoTransform()
    originX = geotransform[0]
    originY = geotransform[3]
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    xOffset = int((x - originX) / pixelWidth)
    yOffset = int((y - originY) / pixelHeight)
    return xOffset, yOffset


def createPath(CostSurfacefn, costSurfaceArray, startCoord, stopCoord):

    # coordinates to array index
    startCoordX = startCoord[0]
    startCoordY = startCoord[1]
    startIndexX, startIndexY = coord2pixelOffset(CostSurfacefn, startCoordX, startCoordY)

    stopCoordX = stopCoord[0]
    stopCoordY = stopCoord[1]
    stopIndexX, stopIndexY = coord2pixelOffset(CostSurfacefn, stopCoordX, stopCoordY)

    # create path
    indices, _weight = route_through_array(costSurfaceArray, (startIndexY, startIndexX), (stopIndexY, stopIndexX), geometric=True, fully_connected=True)
    indices = np.array(indices).T
    path = np.zeros_like(costSurfaceArray)
    path[indices[0], indices[1]] = 1
    return path


def least_cost_path(CostSurfacefn, outputPathfn, startCoord, stopCoord):

    costSurfaceArray = raster2array(CostSurfacefn)  # creates array from cost surface raster

    pathArray = createPath(CostSurfacefn, costSurfaceArray, startCoord, stopCoord)  # creates path array

    array2raster(outputPathfn, CostSurfacefn, pathArray)  # converts path array to raster


def main():

    parser = argparse.ArgumentParser(
        description='Riverscapes VBET Tool',
        # epilog="This is an epilog"
    )

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

    out_path = os.path.join(args.output_dir, args.outname)
    least_cost_path(args.cost_surface, out_path, args.startCoord, args.endCoord)

    sys.exit(0)


if __name__ == '__main__':
    main()
