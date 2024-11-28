import os
import argparse
import sqlite3
from rscommons import Logger

NEXT_REACH_QUERY = 'SELECT us.Shape_length, ds.HydroID FROM riverlines us LEFT JOIN riverlines ds on us.To_NODE = ds.FROM_NODE WHERE us.HydroID = ?'


def calc_level_path(curs: sqlite3.Cursor, watershed_id: int, reset_first: bool) -> None:

    log = Logger('Calc Level Path')
    log.info(f'Calculating level path for watershed {watershed_id}')

    curs.execute('SELECT Count(*) FROM riverlines WHERE WatershedHydroID = ?', [watershed_id])
    row = curs.fetchone()
    log.info(f'Found {row[0]} reaches in watershed {watershed_id}')

    curs.execute('SELECT Count(*) FROM riverlines WHERE WatershedHydroID = ? AND level_path IS NULL', [watershed_id])
    row = curs.fetchone()
    log.info(f'Found {row[0]} reaches without level path in watershed {watershed_id}')

    if reset_first is True:
        log.info(f'Resetting level paths for watershed {watershed_id}')
        curs.execute("UPDATE riverlines SET level_path = NULL WHERE WatershedHydroID = ?", [watershed_id])

    curs.execute("SELECT HydroID FROM riverlines WHERE Headwater <> 0 and WatershedHydroID = ? and level_path IS NULL", [watershed_id])
    level_path_lengths = {row[0]: 0.00 for row in curs.fetchall()}
    print(f'Found {len(level_path_lengths)} headwaters in watershed {watershed_id}')

    for hydro_id in level_path_lengths.keys():
        level_path_lengths[hydro_id] = calculate_length(curs, hydro_id)

    num_processed = 0
    print('Assigning level paths to reaches...')
    for hydro_id, _length in sorted(level_path_lengths.items(), key=lambda item: item[1], reverse=True):
        # print(f"{key}: {value}")
        num_processed += 1
        new_level_path = float(watershed_id * 10**9 + num_processed)
        num_reaches = assign_level_path(curs, hydro_id, new_level_path)
        log.info(f'Assigned level path {calc_level_path} to {num_reaches} reaches starting at HydroID {hydro_id}')

    print(f'Assigned level paths to {num_processed} headwaters in watershed {watershed_id}')


def get_triggers(curs: sqlite3.Cursor, table: str):
    """Get the triggers for the watershed."""
    curs.execute("SELECT * FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?", [table])
    return curs.fetchall()


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


def assign_level_path(curs: sqlite3.Cursor, headwater_hydro_id, level_path: float) -> int:
    """Assign a level path to each reach starting at the headwater down to the ocean."""

    num_reaches = 0
    hydro_id = headwater_hydro_id
    while hydro_id is not None:
        num_reaches += 1
        curs.execute('UPDATE riverlines SET level_path = ? WHERE HydroID = ? AND level_path IS NULL', [level_path, hydro_id])
        curs.execute(NEXT_REACH_QUERY, [hydro_id])
        row = curs.fetchall()
        if len(row) == 1:
            hydro_id = row[0][1]
        elif row is None or len(row) == 0:
            print(f'Assigned path {level_path} to {num_reaches} reaches starting at HydroID {headwater_hydro_id}')
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

    # Initiate the log file
    log = Logger("RS Context")
    log.setup(logPath=os.path.join(os.path.dirname(args.hydro_gpkg), "nz_calc_level_path.log"), verbose=args.verbose)
    log.title(f'Calculate level path For NZ Watershed: {args.watershed_id}')

    log.info(f'HUC: {args.watershed_id}')
    log.info(f'Hydro GPKG: {args.hydro_gpkg}')
    log.info(f'Reset First: {args.reset_first}')

    with sqlite3.connect(args.hydro_gpkg) as conn:
        curs = conn.cursor()
        try:
            triggers = get_triggers(curs, 'riverlines')

            for trigger in triggers:
                curs.execute(f"DROP TRIGGER {trigger[1]}")

            calc_level_path(curs, args.watershed_id, args.reset_first.lower() == 'true')
            conn.commit()
            log.info('Calculation complete')

            for trigger in triggers:
                curs.execute(trigger[4])

        except Exception as e:
            conn.rollback()
            raise e


if __name__ == '__main__':
    main()
