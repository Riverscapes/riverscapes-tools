# -------------------------------------------------------------------------------
# Name:     Watershed Topology Build Script
#
# Purpose:  Builds a database of US HUC12 watersheds, their areas (in square km) as
#           well as which HUC12 each flows into. This can then be used to query
#           the contributing upstream area for any individual HUC8.
#
# Author:   Philip Bailey
#
# Date:     18 Jul 2019
#
# -------------------------------------------------------------------------------
import argparse
import sqlite3
import sys
import traceback
import os
from osgeo import ogr
from rscommons import dotenv


def build_watershed_topology(shapefile, database):

    # Open connection to SQLite database. This will create the file if it does not already exist.
    conn = sqlite3.connect(database)

    # Create the table to store the raw HUC12 info
    conn.execute('DROP TABLE IF EXISTS HUC12')
    conn.execute('CREATE TABLE HUC12 (HUC12 TEXT NOT NULL, AreaSqKm REAL NOT NULL, ToHUC TEXT NOT NULL)')
    conn.execute('CREATE INDEX IX_HUC12 ON HUC12 (HUC12)')
    conn.execute('CREATE INDEX IX_ToHUC ON HUC12 (ToHUC)')

    # Get the input flow lines layer
    driver = ogr.GetDriverByName("ESRI Shapefile")
    inDataSource = driver.Open(shapefile, 0)
    inLayer = inDataSource.GetLayer()
    print('{:,} features in HUC12 shapefile {}'.format(inLayer.GetFeatureCount(), shapefile))

    # Add features to the ouput Layer
    # huc12 = {}
    values = []
    for inFeature in inLayer:

        # id = inFeature.GetField('HUC12')
        # if id in huc12:
        #     raise Exception('HUC already exists')

        # huc12[id] = None
        values.append((inFeature.GetField('HUC12'), inFeature.GetField('AreaSqKm'), inFeature.GetField('ToHUC')))

    # Fill the table using bulk operation
    print('{:,} features about to be written to database {}'.format(len(values), database))
    conn.executemany("INSERT INTO HUC12 (HUC12, AreaSqKm, ToHUC) values (?, ?, ?)", values)
    conn.commit()
    conn.close()

    print('Process completed successfully')


def get_contributing_area(huc8, database):

    if not os.path.isfile(database):
        raise Exception('Database path does not exist: {}'.format(database))

    if len(huc8) != 8:
        raise Exception('HUC code must be eight (8) characters long.')

    # Retrieve all HUC12s that are not in this HUC8 but flow into one of its HUC12s
    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute("SELECT * FROM HUC12 WHERE ToHUC Like '{}%' AND HUC12 NOT LIKE '{}%".format(huc8))

    huc12_inflows = {}
    for row in curs.fetchall():

        print(row)


def get_contributing_area2(curs, huc12):

    if len(huc12) != 12:
        raise Exception('HUC identifier must be 12 characters long.')

    area = 0.0
    curs.execute("SELECT HUC12, AreaSqKm FROM HUC12 WHERE ToHUC = ?", [huc12])
    for row in curs.fetchall():
        area += row['AreaSqKm']
        print(row)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('wbdhu12', help='WBD HUC 12 ShapeFile path', type=argparse.FileType('r'))
    parser.add_argument('database', help='Output SQLite database path', type=str)

    args = dotenv.parse_args_env(parser)

    try:
        build_watershed_topology(args.wbdhu12.name, args.database)

    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
