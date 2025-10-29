# Name:     Reach flow Accumulation
#
# Purpose:  Calculates drainage area for each reach polyine.
#
# Author:   Philip Bailey
#
# Date:     3 June 2019
# -------------------------------------------------------------------------------
import argparse
import os
import sys
import traceback
import json
import numpy as np
import rasterio
from rasterio.mask import mask
from osgeo import ogr
from osgeo import osr
from shapely.geometry import *
from rsxml import Logger, dotenv
from rscommons import Raster
from rscommons.shapefile import create_field

# BRAT drainage area field
drainfld = 'iGeo_DA'


def reach_drainage_area(network, flow_accum_path, buffer_distance):
    """
    Calculate and store drainage area for each reach in a feature class
    :param network: Absolute path to a network ShapeFile
    :param flow_accum_path: Absolute path to a flow accumulation raster (cell counts)
    :param buffer_distance: Distance to buffer reach midpoint for raster sample
    :return: None
    """

    # Get the input NHD flow lines layer
    log = Logger("Reach Flow Accum")
    log.info("Loading network {0}".format(network))
    driver = ogr.GetDriverByName("ESRI Shapefile")
    networkDS = driver.Open(network, 1)
    networkLayer = networkDS.GetLayer()
    networkSRS = networkLayer.GetSpatialRef()

    # Create all the necessary output fields
    create_field(networkLayer, drainfld, log)

    log.info("Loading flow accumulation raster {0}".format(flow_accum_path))
    flow = Raster(flow_accum_path)

    if not networkSRS.IsSame(osr.SpatialReference(wkt=flow.proj)):
        log.info('Raster spatial Reference: {}'.format(flow.proj))
        log.info('Network spatial Reference: {}'.format(networkSRS))
        raise Exception('The network and raster spatial reference are not the same.')

    with rasterio.open(flow_accum_path) as flowsrc:

        # Loop over all polyline network features
        log.info('Looping over {:,} features in network ShapeFile...'.format(networkLayer.GetFeatureCount()))
        for inFeature in networkLayer:

            # get a Shapely representation of the line and buffer it
            featobj = json.loads(inFeature.ExportToJson())
            polyline = shape(featobj['geometry']) if not featobj["geometry"] is None else None

            # Retrieve the max flow accumulation value at the midpoint of the reach
            midpoint = polyline.interpolate(0.5, True)
            polygon = midpoint.buffer(buffer_distance)
            raw_raster, out_transform = mask(flowsrc, [polygon], crop=True)
            mask_raster = np.ma.masked_values(raw_raster, flowsrc.nodata)
            max_cell_count = np.ma.max(mask_raster)

            # Convert cell count to square kilometers
            drainarea = max_cell_count * abs(flow.cellWidth * flow.cellHeight) * 10**-6

            if drainarea >= 0.0:
                # Write the drainage area to the feature class
                inFeature.SetField(drainfld, drainarea)
                networkLayer.SetFeature(inFeature)

            inFeature = None

    networkDS = None
    log.info('Process completed successfully.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('network', help='Output network ShapeFile path', type=argparse.FileType('r'))
    parser.add_argument('flowaccum', help='Flow accumulation raster', type=argparse.FileType('r'))
    parser.add_argument('buffer', help='Distance to buffer reach midpoint', type=float)
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    logg = Logger("Reach Flow Accum")
    logfile = os.path.join(os.path.dirname(args.network.name), "reach_flowaccum.log")
    logg.setup(log_path=logfile, verbose=args.verbose)

    try:
        reach_drainage_area(args.network.name, args.flowaccum.name, args.buffer)

    except Exception as e:
        logg.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
