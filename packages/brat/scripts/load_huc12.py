import sqlite3
import os
from osgeo import ogr
import osgeo.osr as osr
import argparse


def load_hucs(gdb, database):

    # Get the input flow lines layer
    driver = ogr.GetDriverByName('OpenFileGDB')
    data_source = driver.Open(gdb, 0)

    # Open connection to SQLite database. This will create the file if it does not already exist.
    conn = sqlite3.connect(database)

    for huc_level in [4, 8, 12]:
        layer = data_source.GetLayer('WBDHU{}'.format(huc_level))
        print('{:,} features in HUC{} polygon layer'.format(huc_level, layer.GetFeatureCount()))

        values = []
        for inFeature in layer:
            values.append((inFeature.GetField('HUC{}'.format(huc_level)), inFeature.GetField('NAME'), inFeature.GetField('AREASQKM'), inFeature.GetField('STATES')))

        # Fill the table using bulk operation
        print('{:,} features about to be written to database {}'.format(len(values), database))
        conn.executemany('INSERT INTO HUC{} (HUC{}, Name, AreaSqKm, States) values (?, ?, ?, ?)'.format(huc_level, huc_level), values)

    conn.commit()
    conn.close()

    print('Process completed successfully')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('gdb', help='Path to national watershed boundary geodatabase', type=str)
    parser.add_argument('database', help='Path to SQLite database', type=argparse.FileType('r'))
    args = parser.parse_args()

    load_hucs(args.gdb, args.database.name)


if __name__ == '__main__':
    main()
