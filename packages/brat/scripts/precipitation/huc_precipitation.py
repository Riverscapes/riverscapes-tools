# -------------------------------------------------------------------------------
# Name:     HUC Precipitation
#
# Purpose:  Calculate Mean Annual Precipitation for each HUC in Continental US
#
# Author:   Philip Bailey
#
# Date:     31 Oct 2019
#
# -------------------------------------------------------------------------------
import os
import json
import argparse
import sqlite3
import numpy as np
from statistics import mean
from osgeo import ogr, osr
from rsxml import dotenv
from rscommons.shapefile import get_transform_from_epsg
from shapely.geometry import shape, Point, mapping


def huc_precipitation(watersheds, database):
    """Loop over each eight digit HUC watershed and
    calculate the mean annual precipitation of all
    PRISM locations that exist within the watershed
    boundary.

    Arguments:
        watersheds {str} -- Path to the national watershed
        boundary file geodatabase.
        database {str} -- Path to BRAT SQLite database that
        contains the PRISM locations and mean precipiation
        at each location.

    Raises:
        Exception: [description]
        Exception: [description]
    """

    # National HUC8 watershed boundary dataset
    driver = ogr.GetDriverByName("OpenFileGDB")
    datasource = driver.Open(watersheds, 0)
    layer = datasource.GetLayer('WBDHU8')
    vector_srs = layer.GetSpatialRef()
    feature_count = layer.GetFeatureCount()

    # Transformation ensuring watershed polygons are in geographic coords
    out_spatial_ref, transform = get_transform_from_epsg(vector_srs, 4296)

    # BRAT SQLite database
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    processing = 0
    for feature in layer:
        processing += 1
        huc = feature.GetField('HUC8')
        print('Processing {}, {:,} of {:,}'.format(huc, processing, feature_count))
        geom = feature.GetGeometryRef()

        geom.Transform(transform)
        envelope = geom.GetEnvelope()
        polygon = shape(json.loads(geom.ExportToJson()))

        try:
            # Select PRISM locations with the watershed boundary envelope
            curs.execute('SELECT Longitude, Latitude, MeanPrecip FROM Precipitation'
                         ' WHERE (Latitude >= ?) AND (Latitude <= ?) AND (Longitude >= ?) AND (Longitude <= ?)',
                         [envelope[2], envelope[3], envelope[0], envelope[1]])

            # Filter to just those PRISM locations that are actually within the HUC boundary polygon
            monthly_precip = []
            for row in curs.fetchall():
                point = Point(row[0], row[1])
                if polygon.contains(point):
                    monthly_precip.append(row[2])

            if len(monthly_precip) < 1:
                raise Exception('No monthly precipitation values found.')

            if sum(monthly_precip) < 1:
                raise Exception('Zero monthly precipitation.')

            curs.execute('UPDATE HUCs SET MeanPrecip = ? WHERE HUC = ?', [mean(monthly_precip), huc])
            conn.commit()

        except Exception as e:
            print('Error processing {}'.format(huc))


print('Calculating HUC precipitation complete.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('watersheds', help='National watershed boundary file geodatabase', type=str)
    parser.add_argument('database', help='Path to SQLite database', type=argparse.FileType('r'))

    args = dotenv.parse_args_env(parser)

    huc_precipitation(args.watersheds, args.database.name)


if __name__ == '__main__':
    main()
