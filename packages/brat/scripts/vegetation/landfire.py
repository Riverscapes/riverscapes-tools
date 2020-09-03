# -------------------------------------------------------------------------------
# Name:     Landfire
#
# Purpose:  Read the master list of landfire types and put them in the SQLite
#           database.
#
# Author:   Philip Bailey
#
# Date:     24 Oct 2019
# -------------------------------------------------------------------------------

import os
import sqlite3
import csv
import argparse

landfire_version = 'Landfire 2.0.0'

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')

landfire_csv = '/SOMEPATH/Landfire/landfire_2_0_0_evt_type.csv'
database = '/SOMEPATH/beaver/pyBRAT4/data/vegetation.sqlite'


def landfire_vegetation_types(landfire, database):

    types = {}
    unique = []

    values = []
    veg_types_found = 0
    veg_types_missing = 0
    with open(landfire, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        for row in reader:
            id = row['VALUE']
            name = row['EVT_Name']
            landuse = row['EVT_GP_N']
            phys = row['EVT_PHYS']

            if id in types:
                raise Exception('Duplicate ID {}'.format(id))

            types[id] = name
            unique.append((id, name, landuse, phys))

    print(len(types), 'unique landfire types found in ShapeFile')

    data = [(phys, int(id)) for id, name, landuse, phys in unique]

    conn = sqlite3.connect(database)
    curs = conn.executemany('UPDATE VegetationTypes SET EVT_PHYS = ? WHERE VegetationID = ?', data)
    conn.commit()

    print('Process complete')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('landfire', help='Path to Landfire CSV containing veg types', type=argparse.FileType('r'))
    parser.add_argument('database', help='Path to SQLite database', type=argparse.FileType('r'))
    args = parser.parse_args()

    landfire_vegetation_types(args.landfire.name, args.database.name)


if __name__ == '__main__':
    main()
