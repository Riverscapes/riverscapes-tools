import argparse
import os
import sqlite3
from osgeo import ogr

NEXT_REACH_QUERY = 'SELECT us.Shape_length, ds.HydroID FROM riverlines us LEFT JOIN riverlines ds on us.To_NODE = ds.FROM_NODE WHERE us.HydroID = ?'


def level_path(gpkg_path, curs: sqlite3.Cursor, watershed_id: int, reset_first: bool) -> None:

    if reset_first is True:
        curs.execute("UPDATE riverlines SET level_path = NULL WHERE watershed_id = ?", [watershed_id])

    # Find all the headwaters in the watershed
    headwater_id = -1

    curs.execute("SELECT HydroID FROM riverlines WHERE Headwater <> 0 and WatershedHydroID = ? and level_path IS NULL", [watershed_id])
    level_path_lengths = {row[0]: 0.00 for row in curs.fetchall()}
    print(f'Found {len(level_path_lengths)} headwaters in watershed {watershed_id}')

    for hydro_id in level_path_lengths.keys():
        level_path_lengths[hydro_id] = calculate_length(curs, hydro_id)

    sorted_dict = dict(sorted(level_path_lengths.items(), key=lambda item: item[1], reverse=True))
    print(f'Calculated level path flow lengths {len(sorted_dict)} headwaters in watershed {watershed_id}')

    num_processed = 0
    ds = ogr.Open(gpkg_path)
    for hydro_id, _length in sorted_dict.items():
        num_processed += 1
        level_path = float(watershed_id * 10 ^ 6 + num_processed)
        num_reaches = assign_level_path(ds, curs, hydro_id, level_path)
        print(f'Assigned level path {level_path} to {num_reaches} reaches starting at HydroID {hydro_id}')

    ds = None
    # print(row)
    # headwater_id = row[0]

    # curs.execute("SELECT * FROM riverlines WHERE Headwater <> 0 and watershed_id = ? and level_path IS NULL", [watershed_id])
    # curs.execute(f"SELECT * FROM {level}")
    # rows = curs.fetchall()
    # for row in rows:
    #     print(row)


def calculate_length(curs: sqlite3.Cursor, hydro_id: int):
    """Calculate the cumulative length of a riverline from the headwater to the mouth."""

    cum_length = 0
    while hydro_id is not None:
        # Get the length of the current, upstream, riverline as well as the HydroID of the downstream reach.
        curs.execute(NEXT_REACH_QUERY, [hydro_id])
        row = curs.fetchall()
        if len(row) == 1:
            cum_length += row[0][0]
            hydro_id = row[0][1]
        elif row is None or len(row) == 0:
            return cum_length
        else:
            raise Exception(f"More than one downstream reach found for HydroID {hydro_id}")

    return cum_length


def assign_level_path(ds, curs: sqlite3.Cursor, hydro_id, level_path: float) -> int:
    """Assign a level path to each reach starting at the headwater down to the ocean."""

    num_reaches = 0
    while hydro_id is not None:
        num_reaches += 1
        ds.ExecuteSQL(f'UPDATE riverlines SET level_path = {level_path} WHERE HydroID = {hydro_id}')
        curs.execute(NEXT_REACH_QUERY, [hydro_id])
        row = curs.fetchall()
        if len(row) == 1:
            hydro_id = row[0][1]
        elif row is None or len(row) == 0:
            return num_reaches
        else:
            raise Exception(f"More than one downstream reach found for HydroID {hydro_id}")

    return num_reaches


def main():
    """Calculate the level path for each reach in a single watershed."""
    parser = argparse.ArgumentParser()
    parser.add_argument('hydro_gpkg', type=str)
    parser.add_argument('watershed_id', type=int)
    parser.add_argument('reset_first', type=str)
    args = parser.parse_args()

    with sqlite3.connect(args.hydro_gpkg) as conn:
        curs = conn.cursor()
        try:
            level_path(args.hydro_gpkg, curs, args.watershed_id, args.reset_first.lower() == 'true')
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e


if __name__ == '__main__':
    main()
