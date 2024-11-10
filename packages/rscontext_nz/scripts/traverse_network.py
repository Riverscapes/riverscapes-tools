"""
Author:         Philip Bailey

Date:           8 Nov 2024

Description:    Proof of concept script to traverse a river network from a given HydroID to identify all upstream and downstream
                features. This script was developed to quickly identify watersheds by picking a headwater feature and then finding
                all connected features. 

                This was then used to select catchment wings with the output HydroID and then unioning these into watershed polygons.
"""
from typing import List
import sqlite3
import argparse


def traverse_network(db_path: str, hydro_id: int) -> List[int]:
    """
    Traverse a river network to identify all upstream and downstream features
    db_path: str - The path to the GeoPackage database containing the river network
    hydro_id: int - The HydroID of the feature to start the traversal from
    """

    visited_ids = []
    to_visit_ids = [hydro_id]

    with sqlite3.connect(db_path) as conn:
        curs = conn.cursor()
        while len(to_visit_ids) > 0:

            active_id = to_visit_ids.pop(0)
            visited_ids.append(active_id)

            # print(f'Processing HydroID {active_id}, to visit: {len(to_visit_ids)}, visited: {len(visited_ids)}')

            curs.execute('SELECT FROM_NODE, TO_NODE FROM riverlines WHERE HydroID = ?', [active_id])
            from_node, to_node = curs.fetchone()

            curs.execute('SELECT HydroID FROM riverlines WHERE FROM_NODE = ? AND HydroID <> ?', [to_node, active_id])
            to_visit_ids.extend([hid for (hid,) in curs.fetchall() if hid not in visited_ids])

            curs.execute('SELECT HydroID FROM riverlines WHERE TO_NODE = ? AND HydroID <> ?', [from_node, active_id])
            to_visit_ids.extend([hid for (hid,) in curs.fetchall() if hid not in visited_ids])

    return visited_ids


def main():
    """Traverse a river network to identify all upstream and downstream features"""

    parser = argparse.ArgumentParser(description='Traverse a river network to identify all upstream and downstream features')
    parser.add_argument('db_path', type=str, help='The path to the GeoPackage database containing the river network')
    parser.add_argument('hydro_id', type=int, help='The HydroID of the feature to start the traversal from')
    args = parser.parse_args()

    visited_ids = traverse_network(args.db_path, args.hydro_id)

    # Simply print the visited HydroIDs for use in desktop GIS
    print(visited_ids)


if __name__ == '__main__':
    main()
