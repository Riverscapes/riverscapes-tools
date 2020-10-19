# Name:     Vegation Summary
#
# Purpose:  Summarizes vegetation for each polyline feature within a buffer distance
#           on a raster. Inserts the area of each vegetation type into the BRAT database
#
# Author:   Philip Bailey
#
# Date:     28 Aug 2019
# -------------------------------------------------------------------------------
import argparse
import csv
import os
import sys
import traceback
import gdal
import ogr
from ogr import osr
from rscommons import ProgressBar, Logger, dotenv
from rscommons.shapefile import _rough_convert_metres_to_raster_units
from rscommons.database import load_geometries
import rasterio
from rasterio.mask import mask
import numpy as np
import sqlite3


def vegetation_summary(database, veg_raster, buffer):
    """ Loop through every reach in a BRAT database and 
    retrieve the values from a vegetation raster within
    the specified buffer. Then store the tally of 
    vegetation values in the BRAT database.

    Arguments:
        database {str} -- Path to BRAT database
        veg_raster {str} -- Path to vegetation raster
        buffer {float} -- Distance to buffer the reach polylines
    """

    log = Logger('Vegetation')
    log.info('Summarizing {}m vegetation buffer from {}'.format(int(buffer), veg_raster))

    # Retrieve the raster spatial reference and geotransformation
    dataset = gdal.Open(veg_raster)
    gt = dataset.GetGeoTransform()
    gdalSRS = dataset.GetProjection()
    raster_srs = osr.SpatialReference(wkt=gdalSRS)
    raster_buffer = _rough_convert_metres_to_raster_units(veg_raster, buffer)

    # Calculate the area of each raster cell in square metres
    conversion_factor = _rough_convert_metres_to_raster_units(veg_raster, 1.0)
    cell_area = abs(gt[1] * gt[5]) / conversion_factor**2

    # Load the reach geometries and ensure they are in the same projection as the vegetation raster
    geometries = load_geometries(database, raster_srs)

    # Open the raster and then loop over all polyline features
    veg_counts = []
    with rasterio.open(veg_raster) as src:

        progbar = ProgressBar(len(geometries), 50, "Unioning features")
        counter = 0

        for reachID, polyline in geometries.items():

            counter += 1
            progbar.update(counter)

            polygon = polyline.buffer(raster_buffer)

            try:
                # retrieve an array for the cells under the polygon
                raw_raster = mask(src, [polygon], crop=True)[0]
                mask_raster = np.ma.masked_values(raw_raster, src.nodata)
                # print(mask_raster)

                # Reclass the raster to dam suitability. Loop over present values for performance
                for oldvalue in np.unique(mask_raster):
                    if oldvalue is not np.ma.masked:
                        cell_count = np.count_nonzero(mask_raster == oldvalue)
                        veg_counts.append([reachID, int(oldvalue), buffer, cell_count * cell_area, cell_count])
            except Exception as ex:
                log.warning('Error obtaining vegetation raster values for ReachID {}'.format(reachID))
                log.warning(ex)

        progbar.finish()

    conn = sqlite3.connect(database)
    conn.executemany('INSERT INTO ReachVegetation (ReachID, VegetationID, Buffer, Area, CellCount) VALUES (?, ?, ?, ?, ?)', veg_counts)
    conn.commit()

    log.info('Vegetation summary complete')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='BRAT database path', type=argparse.FileType('r'))
    parser.add_argument('raster', help='vegetation raster', type=argparse.FileType('r'))
    parser.add_argument('buffer', help='buffer distance', type=float)
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    logg = Logger('Veg Summary')
    logfile = os.path.join(os.path.dirname(args.database.name), 'vegetation_summary.log')
    logg.setup(logPath=logfile, verbose=args.verbose)

    try:
        vegetation_summary(args.database.name, args.raster.name, args.buffer)

    except Exception as e:
        logg.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
