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
import networkx as nx


def get_contributing_area(database, huc8):

    # Identify HUC12s that are outside and flow into the HUC8
    inflows = getInflowHUC12s(database, huc8)

    # Build national networkX graph
    graph = build_graph(database)

    for downstream, inflow in inflows.items():
        for huc12 in inflow:
            results = nx.algorithms.dag.ancestors(graph, huc12['HUC12'])
            area = 0.0
            for n in results:
                area += graph.nodes[n]['Area']

            huc12['Area'] = area

            print('{} receives from flow {} with an upstream drainage area of {:.2f}km\u00b2'.format(downstream, huc12['HUC12'], huc12['Area']))


def getInflowHUC12s(database, huc8):
    """
    Get a list of HUC12s that are outside the HUC8 and flow into the HUC8
    :param curs: SQLite database cursor
    :param huc8: HUC8
    :return:
    """

    if len(huc8) != 8:
        raise Exception('HUC code must be eight (8) characters long.')

    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute("SELECT HUC12, ToHUC FROM HUC12 WHERE (ToHUC Like '{}%') AND (HUC12 NOT LIKE '{}%')".format(huc8, huc8))
    inflows = {}
    for row in curs.fetchall():
        if row[1] not in inflows:
            inflows[row[1]] = []
        inflows[row[1]].append({'HUC12': row[0]})

    return inflows


def build_graph(database):

    if not os.path.isfile(database):
        raise Exception('Database path does not exist: {}'.format(database))

    G = nx.DiGraph()

    # Retrieve all HUC12s that flow into this HUC8 from outside
    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute('SELECT HUC12, AreaSqKm FROM HUC12')
    for row in curs.fetchall():
        G.add_node(row[0], Area=row[1])

    curs.execute('SELECT HUC12, ToHUC FROM HUC12')
    for row in curs.fetchall():
        G.add_edge(row[0], row[1])

    print('Graph has {:,} edges and {:,} nodes'.format(G.number_of_nodes(), G.number_of_edges()))
    return G

# def _append_inflowing_huc12s(curs, inflows):
#
#     if len(inflows['Upstream']) < 1:
#         return
#
#     # Remove the item we are assessing. It's area is already accounted for
#     huc12 = inflows['Upstream'].pop(0)
#     #print('\tProcessing HUC12 {}'.format(huc12))
#
#     # Select all HUC12s that flow into this HUC12
#     curs.execute('SELECT DISTINCT HUC12, AreaSqKm FROM HUC12 WHERE ToHUC = ?', [huc12])
#     for row in curs.fetchall():
#         inflows['Upstream'].append(row[0])
#         inflows['Area'] += row[1]
#
#     # Continue traversing upstream
#     while len(inflows['Upstream']) > 0:
#         _append_inflowing_huc12s(curs, inflows)


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
