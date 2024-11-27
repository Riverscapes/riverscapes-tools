import argparse
import os
import sqlite3

NEXT_REACH_QUERY = 'SELECT us.Shape_len, ds.HydroID FROM riverlines us LEFT JOIN riverlines ds on us.To_NODE = ds.FROM_NODE WHERE us.HydroID = ?'


def level_path(hydro_gpkg: str, watershed_id: int):

    conn = sqlite3.connect(hydro_gpkg)
    curs = conn.cursor()

    # Find all the headwaters in the watershed
    headwater_id = -1

    curs.execute("SELECT HydroID FROM riverlines WHERE Headwater <> 0 and watershed_id = ? and level_path IS NULL", [headwater_id, watershed_id])
    level_path_lengths = {row[0]: 0.00 for row in curs.fetchall()}

    for hydro_id in level_path_lengths.keys():
        level_path_lengths[hydro_id] = calculate_length(curs, row[0])

    sorted_dict = dict(sorted(level_path_lengths.items(), key=lambda item: item[1]))
    print(f'Identified {len(sorted_dict)} headwaters in watershed {watershed_id}')

    num_processed = 0
    for hydro_id, length in sorted_dict.items():
        num_processed += 1
        level_path = float(watershed_id * 10 ^ 6 + num_processed)
        num_reaches = assign_level_path(curs, hydro_id, level_path)
        print(f'Assigned level path {level_path} to {num_reaches} reaches starting at HydroID {hydro_id}')

    print(row)
    headwater_id = row[0]

    c.execute("SELECT * FROM riverlines WHERE Headwater <> 0 and watershed_id = ? and level_path IS NULL", [watershed_id])
    c.execute(f"SELECT * FROM {level}")
    rows = c.fetchall()
    for row in rows:
        print(row)
    conn.close()

    def calculate_length(curs: sqlite3.Cursor, hydro_id: int):

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

    def assign_level_path(curs: sqlite3.Cursor, hydro_id, level_path: float) -> int:

        num_reaches = 0
        while hydro_id is not None:
            num_reaches += 1
            curs.execute("UPDATE riverlines SET level_path = ? WHERE HydroID = ?", [level_path, hydro_id])
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
    parser = argparse.ArgumentParser()
    parser.add_argument('hydro_gpkg', type=str, required=True)
    parser.add_argument('watershed_id', type=int, required=True)
    args = parser.parse_args()

    level_path(args.hydro_gpkg, args.watershed_id)


if __name__ == '__main__':
    main()
