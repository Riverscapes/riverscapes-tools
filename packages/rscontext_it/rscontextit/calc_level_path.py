import os
import argparse
import sqlite3
from rscommons import Logger, ProgressBar

# us is upstream, ds is downstream
# NEXT_REACH_QUERY = 'SELECT us.LENGTH, ds.OBJECT_ID FROM riverlines us LEFT JOIN riverlines ds on us.TNODE = ds.FNODE WHERE us.OBJECT_ID = ?'
NEXT_REACH_QUERY = 'SELECT us.LENGTH, ds.OBJECT_ID FROM riverlines us LEFT JOIN riverlines ds on us.NEXTDOWNID = ds.OBJECT_ID WHERE us.OBJECT_ID = ?'


def calc_level_path(curs: sqlite3.Cursor, watershed_id: int, reset_first: bool) -> None:
    """
    Calculate the level path for each reach in a watershed.
    note if watershed_id is a string, it better not have spaces in it... not thoroughly tested
    """
    log = Logger('Calc Level Path')
    log.info(f'Calculating level path for watershed {watershed_id}')

    # Get the number of reaches in the watershed (sanity check)
    curs.execute('SELECT Count(*) FROM riverlines WHERE CatchID = ?', [watershed_id])
    row = curs.fetchone()
    log.info(f'Found {row[0]} reaches in watershed {watershed_id}')

    # Get the number of reaches without level path (also a sanity check)
    curs.execute('SELECT Count(*) FROM riverlines WHERE CatchID = ? AND level_path IS NULL', [watershed_id])
    row = curs.fetchone()
    log.info(f'Found {row[0]} reaches without level path in watershed {watershed_id}')

    # Reset the level path for the watershed if requested
    if reset_first is True:
        log.info(f'Resetting level paths for watershed {watershed_id}')
        curs.execute("UPDATE riverlines SET level_path = NULL WHERE CatchID = ?", [watershed_id])

    curs.execute("SELECT OBJECT_ID, LONGPATH FROM riverlines WHERE CatchID = ? AND level_path IS NULL", [watershed_id])
    level_path_lengths = {row[0]: row[1] for row in curs.fetchall()}

    # I'm going to assume LONGPATH is populated correctly, so we don't need this
    # # a headwater is a reach with no upstream reach
    # curs.execute("SELECT OBJECT_ID FROM riverlines WHERE NEXTUPID IS NULL and CatchID = ? and level_path IS NULL", [watershed_id])
    # # instantiate a dictionary to hold the for each headwater
    # level_path_lengths = {row[0]: 0.00 for row in curs.fetchall()}
    # log.info(f'Found {len(level_path_lengths)} headwaters in watershed {watershed_id}')

    # for hydro_id in level_path_lengths.keys():
    #     level_path_lengths[hydro_id] = calculate_length(curs, hydro_id)

    num_processed = 0
    log.info('Assigning level paths to reaches...')
    progbar = ProgressBar(len(level_path_lengths), text="Assigning level paths to reaches")
    for hydro_id, _length in sorted(level_path_lengths.items(), key=lambda item: item[1], reverse=True):
        # print(f"{key}: {value}")
        num_processed += 1
        new_level_path = float(watershed_id * 10**8 + num_processed)
        num_reaches = assign_level_path(curs, hydro_id, new_level_path)
        progbar.update(num_processed)
        # rather noisy even for debug
        # log.debug(f'Assigned level path {new_level_path} to {num_reaches} reaches starting at HydroID {hydro_id}')

    log.info(f'Assigned level paths to {num_processed} headwaters in watershed {watershed_id}')


def get_triggers(curs: sqlite3.Cursor, table: str):
    """Get the triggers for the watershed."""
    curs.execute("SELECT * FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?", [table])
    return curs.fetchall()


def calculate_length(curs: sqlite3.Cursor, hydro_id: int):
    """Calculate the cumulative length of a riverline from the headwater to the mouth.
    Not used for Italy, because the LONGPATH field is already populated.
    """

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
    num_reaches_updated = 0
    hydro_id = headwater_hydro_id
    while hydro_id is not None:
        num_reaches += 1
        curs.execute('UPDATE riverlines SET level_path = ? WHERE OBJECT_ID = ? AND level_path IS NULL', [level_path, hydro_id])
        if curs.rowcount:
            num_reaches_updated += curs.rowcount  # this better be one, given we're selecting on OBJECT_ID
            # noisy even for debug
            # Logger('Calc Level Path').debug(f'Assigned level path {level_path} to reach {hydro_id}')
        curs.execute(NEXT_REACH_QUERY, [hydro_id])
        row = curs.fetchall()
        if len(row) == 1:
            hydro_id = row[0][1]
        elif row is None or len(row) == 0:
            # log.debug(f'Assigned path {level_path} to {num_reaches} reaches starting at HydroID {headwater_hydro_id}')
            return num_reaches_updated
        else:
            # get the shortest of the downstream reaches -- assume that is the predominant way for the water to flow
            # (at least in one example 'RL24016313' the longer loop looked like man-made diversion)
            curs.execute('SELECT OBJECT_ID FROM riverlines WHERE FNode = (SELECT TNode FROM riverlines WHERE OBJECT_ID=?) ORDER BY LENGTH', [hydro_id])
            Logger('Calc Level Path').warning(f"More than one downstream reach found for OBJECTID {hydro_id}. Using shortest for level_path {level_path}.")
            hydro_id = curs.fetchone()[0]
            # raise Exception(f"More than one downstream reach found for HydroID {hydro_id}")

    return num_reaches_updated


def main():
    """
    Main function to calculate the level path for each reach in a single watershed.

    This function parses command-line arguments, sets up logging, and performs
    level path calculations on a specified watershed within a hydrological GeoPackage.

    Command-line Arguments:
        hydro_gpkg (str): Path to the hydrological GeoPackage file.
        watershed_id (int): Identifier for the watershed to process.
        reset_first (str): Whether to reset the level path calculation before starting 
                           ('true' or 'false').

    Steps:
    1. Parses command-line arguments.
    2. Sets up a log file for tracking the process.
    3. Logs the input parameters.
    4. Connects to the GeoPackage database and retrieves existing triggers.
    5. Drops triggers, performs level path calculations, and restores triggers.
    6. Commits changes to the database or rolls back in case of an error.

    Raises:
        Exception: If an error occurs during the level path calculation or database operations.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('hydro_gpkg', type=str)
    parser.add_argument('watershed_id', type=int)
    parser.add_argument('reset_first', type=str)
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging', default=False)
    args = parser.parse_args()

    # Initiate the log file
    log = Logger("RS Context")
    log.setup(logPath=os.path.join(os.path.dirname(args.hydro_gpkg), "it_calc_level_path.log"), verbose=args.verbose)
    log.title(f'Calculate level path For Watershed: {args.watershed_id}')

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
