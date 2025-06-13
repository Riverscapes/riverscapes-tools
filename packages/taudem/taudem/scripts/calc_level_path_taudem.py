"""
Calculates the level path for each reach in a watershed by traversing
the river network from headwaters downstream, assigning a unique level path
to each reach.

This version is intended for use on the network produced by the tauDEM streamnet.
command. However, the input network to this script must be in GeoPackage format.
Simply convert the ShapeFile produced by the tauDEM streamnet operation to a
GeoPackage using QGIS or GDAL.

The script takes three arguments:
1. The path to the GeoPackage containing the network.
2. The name of the feature class in the GeoPackage that contains the hydro network.
3. A boolean value indicating whether to reset the level paths before calculating them (default is False).

Philip Bailey
12 Jun 2025
"""
from typing import List
import argparse
import sqlite3


NEXT_REACH_QUERY = 'SELECT us.Length, ds.LINKNO FROM {} us LEFT JOIN {} ds on us.DSLINKNO = ds.LINKNO WHERE us.LINKNO = ?'


def calc_level_path(curs: sqlite3.Cursor, feature_class: str, reset_first: bool) -> None:
    """Calculate the level path for each reach in the watershed."""

    curs.execute(f'SELECT Count(*) FROM {feature_class}')
    row = curs.fetchone()

    curs.execute(f'SELECT Count(*) FROM {feature_class} WHERE level_path IS NULL')
    row = curs.fetchone()
    print(f'Found {row[0]} reaches without level path')

    if reset_first is True:
        print(f'Resetting level paths')
        curs.execute(f"UPDATE {feature_class} SET level_path = NULL")

    curs.execute(f"SELECT LINKNO FROM {feature_class} WHERE USLINKNO1 = -1 AND level_path IS NULL")
    level_path_lengths = {row[0]: 0.00 for row in curs.fetchall()}
    print(f'Found {len(level_path_lengths)} headwaters')

    for hydro_id in level_path_lengths.keys():
        level_path_lengths[hydro_id] = calculate_length(curs, feature_class, hydro_id)

    num_processed = 0
    for hydro_id, _length in sorted(level_path_lengths.items(), key=lambda item: item[1], reverse=True):
        # print(f"{key}: {value}")
        num_processed += 1
        new_level_path = float(10**9 + num_processed)
        num_reaches = assign_level_path(curs, feature_class, hydro_id, new_level_path)
        # log.debug(f'Assigned level path {new_level_path} to {num_reaches} reaches starting at HydroID {hydro_id}')
        #


def get_triggers(curs: sqlite3.Cursor, table: str):
    """Get the triggers for the watershed."""
    curs.execute("SELECT * FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?", [table])
    return curs.fetchall()


def calculate_length(curs: sqlite3.Cursor, feature_class: str, hydro_id: int):
    """Calculate the cumulative length of a riverline from the headwater to the mouth."""

    cum_length = 0
    while hydro_id is not None:
        # Get the length of the current, upstream, riverline as well as the HydroID of the downstream reach.
        curs.execute(NEXT_REACH_QUERY.format(feature_class, feature_class), [hydro_id])
        row = curs.fetchall()
        if len(row) == 1:
            cum_length += row[0][0]
            hydro_id = row[0][1]
        elif row is None or len(row) == 0:
            return cum_length
        else:
            raise Exception(f"More than one downstream reach found for HydroID {hydro_id}")

    return cum_length


def assign_level_path(curs: sqlite3.Cursor, feature_class: str, headwater_hydro_id, level_path: float) -> int:
    """Assign a level path to each reach starting at the headwater down to the ocean."""

    num_reaches = 0
    hydro_id = headwater_hydro_id
    while hydro_id is not None:
        num_reaches += 1
        curs.execute(f'UPDATE {feature_class} SET level_path = ? WHERE LINKNO = ? AND level_path IS NULL', [level_path, hydro_id])
        curs.execute(NEXT_REACH_QUERY.format(feature_class, feature_class), [hydro_id])
        row = curs.fetchall()
        if len(row) == 1:
            hydro_id = row[0][1]
        elif row is None or len(row) == 0:
            # log.debug(f'Assigned path {level_path} to {num_reaches} reaches starting at HydroID {headwater_hydro_id}')
            return num_reaches
        else:
            raise Exception(f"More than one downstream reach found for HydroID {hydro_id}")

    return num_reaches


def create_index(curs: sqlite3.Cursor, table: str, columns: List[str]) -> None:
    """Create an index on the specified columns of the table if it does not already exist."""

    columns = columns if isinstance(columns, list) else [columns]
    index_name = f"ix_{table}_{'_'.join(columns)}"
    curs.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE ?", [index_name])
    if curs.fetchone() is None:
        curs.execute(f"CREATE INDEX {index_name} ON {table}({','.join(columns)})")


def add_column(curs: sqlite3.Cursor, table: str, column_name: str, column_type: str) -> None:
    """Add a column to the specified table if it does not already exist."""
    curs.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in curs.fetchall()]
    if column_name not in columns:
        curs.execute(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}")
        print(f"Added column {column_name} to table {table}")
    else:
        print(f"Column {column_name} already exists in table {table}")


def main():
    """Calculate the level path for each reach in a single watershed."""
    parser = argparse.ArgumentParser()
    parser.add_argument('hydro_gpkg', type=str)
    parser.add_argument('feature_class', type=str)
    parser.add_argument('reset_first', type=str)
    args = parser.parse_args()

    with sqlite3.connect(args.hydro_gpkg) as conn:
        curs = conn.cursor()
        try:
            # Hack because feature class tables have SpatialLite triggers that prevent us from altering the table.
            triggers = get_triggers(curs, args.feature_class)

            for trigger in triggers:
                curs.execute(f"DROP TRIGGER {trigger[1]}")

            # Prepare the GeoPackage by adding the level paths column and indexes (if they do not already exist)
            add_column(curs, args.feature_class, 'level_path', 'INTEGER')
            create_index(curs, args.feature_class, ['level_path'])
            create_index(curs, args.feature_class, ['LINKNO'])
            create_index(curs, args.feature_class, ['USLINKNO1'])
            create_index(curs, args.feature_class, ['DSLINKNO'])

            calc_level_path(curs, args.feature_class, args.reset_first.lower() == 'true')
            conn.commit()

            for trigger in triggers:
                curs.execute(trigger[4])

            print(f"Level paths calculated successfully for feature class {args.feature_class} in {args.hydro_gpkg}")

            curs.execute(f'SELECT level_path IS NULL, Count(*) FROM {args.feature_class} GROUP BY level_path IS NULL ORDER BY level_path IS NULL'.format(args.feature_class))
            row = curs.fetchone()
            without_level_path, with_level_path = row[0], row[1]
            print(f'{with_level_path} reaches with level path.')
            print(f'{without_level_path} reaches without level path.')

        except Exception as e:
            conn.rollback()
            raise e

    print('Process completed successfully.')


if __name__ == '__main__':
    main()
