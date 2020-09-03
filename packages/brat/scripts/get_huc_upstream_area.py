# -------------------------------------------------------------------------------
# Name:     Watershed Topology Query Script
#
# Purpose:  Looks up a HUC8 in a database of HUC12 topologies that's built
#           with another script and returns a dictionary of all the HUC12a
#           that flow into the argument HUC8.
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


def get_contributing_area(database, huc8):

    if not os.path.isfile(database):
        raise Exception('Database path does not exist: {}'.format(database))

    if len(huc8) != 8:
        raise Exception('HUC code must be eight (8) characters long.')

    # Retrieve all HUC12s that flow into this HUC8 from outside
    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute("SELECT HUC12, ToHUC, AreaSqKm FROM HUC12 WHERE (ToHUC Like '{}%') AND (HUC12 NOT LIKE '{}%')".format(huc8, huc8))

    destinations = {}
    for row in curs.fetchall():

        huc12 = row[0]
        into = row[1]
        area = row[2]

        if into in destinations:
            if huc12 not in destinations[into]['Upstream']:
                destinations[into]['Area'] += area
                destinations[into]['Upstream'].append(huc12)
        else:
            destinations[into] = {'Area': area, 'Upstream': [huc12]}

    # Loop over each HUC12 that is inside this HUC8 and receives flow from the outside
    for into, inflows in destinations.items():
        # print('Looking at HUCs that flow into {}'.format(into))
        _append_inflowing_huc12s(curs, inflows)
        print('Area upstream of {} is {:.2f}km\u00b2'.format(into, inflows['Area']))

    # Return dictionary where keys are HUC12 IDs that receive external flow
    return destinations


def _append_inflowing_huc12s(curs, inflows):

    if len(inflows['Upstream']) < 1:
        return

    # Remove the item we are assessing. It's area is already accounted for
    huc12 = inflows['Upstream'].pop(0)
    # print('\tProcessing HUC12 {}'.format(huc12))

    # Select all HUC12s that flow into this HUC12
    curs.execute('SELECT DISTINCT HUC12, AreaSqKm FROM HUC12 WHERE ToHUC = ?', [huc12])
    for row in curs.fetchall():
        inflows['Upstream'].append(row[0])
        inflows['Area'] += row[1]

    # Continue traversing upstream
    while len(inflows['Upstream']) > 0:
        _append_inflowing_huc12s(curs, inflows)


# def get_contributing_area(curs, huc12):
#     if len(huc12) != 12:
#         raise Exception('HUC identifier must be 12 characters long.')
#
#     area = 0.0
#     curs.execute("SELECT HUC12, AreaSqKm FROM HUC12 WHERE ToHUC = ?", [huc12])
#     for row in curs.fetchall():
#         area += row['AreaSqKm']
#         print(row)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Output SQLite database path', type=argparse.FileType('r'))
    parser.add_argument('huc8', help='WBD HUC 8 identifier', type=str)
    args = parser.parse_args()

    try:
        get_contributing_area(args.database.name, args.huc8)

    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
