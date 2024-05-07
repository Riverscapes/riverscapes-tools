"""Script for exporting BRAT parameters from local SQLite
and writing them to CSV in this repository for use in sqlBRAT.

This script was adapted from the original that performed the
same operation but from AWS Postgres database.

The new workflow is described here:
https://tools.riverscapes.net/brat/advanced/parameters/
"""
from typing import List
import re
import csv
import os
import sqlite3
import argparse
from rscommons import ProgressBar

# These are currently the only tables that are editable and written to CSV files
tables = [
    # primary tables
    'Watersheds',
    'HydroParams',
    'VegetationTypes',
    # intersect tables
    'VegetationOverrides',
    'WatershedHydroParams',
]


def write_brat_params_to_csv(sqlite_path, csv_dir):
    """ Pull the BRAT parameters from SQLite database and save them to CSV
    files in the BRAT source code folder where they can be used by BRAT Build
    and also commited to git.

    Arguments:
        sqlite_path {str} -- Absolute path to local SQLite database with updated BRAT params
        csv_dir {[type]} -- Absolute path to the directory where the csv files will be saved
    """

    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = dict_factory
    curs = conn.cursor()

    for table in tables:

        # Get list of column names and primary keys
        curs.execute(f'PRAGMA table_info({table})')
        col_info = curs.fetchall()
        cols = [row['name'] for row in col_info]
        keys = [row['name'] for row in col_info if row['pk'] != 0]

        # Ensure column names and keys are in predictable or to reduce git churn
        # Update: commented out because the order they are defined in SQL schema
        # matches the old Postgres order and creates less git churn. Ideally
        # these would be sorted, but it's not worth the churn.
        # cols.sort()
        # keys.sort()

        # Load all the data from the table
        curs.execute(f'SELECT * FROM {table} ORDER BY {",".join(keys)}')
        data = [{col: row[col] for col in row.keys()} for row in curs.fetchall()]

        # write the data to the CSV
        output_csv = os.path.join(csv_dir, 'intersect' if len(keys) > 1 else '', f'{table}.csv')
        write_values_to_csv(output_csv, cols, data)


def write_values_to_csv(csv_file: str, cols: List[str], values) -> None:
    """
    Actually write the values to the CSV file

    Args:
        csv_file (_type_): _description_
        cols (_type_): _description_
        values (_type_): _description_
    """

    progBar = ProgressBar(len(values), 50, f'Writing to {os.path.basename(csv_file)}')
    with open(csv_file, 'w', encoding='utf8') as file:
        writer = csv.writer(file)
        writer.writerow(cols)
        counter = 0
        for vals in values:
            counter += 1
            progBar.update(counter)
            writer.writerow([vals[col] for col in cols])

    progBar.finish()


def check_hydrology_equations(sqlite_path: str) -> None:
    """
    Check the hydrology equations in the SQLite database for syntax errors by 
    evaluating them with a placeholder for drainage area. This will catch any
    missing parameters or syntax errors in the equations.

    Args:
        sqlite_path (str): absolute path to local SQLite database
    """

    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = dict_factory
    curs = conn.cursor()

    # Load all the watersheds from the database in a PREDICTABLE ORDER (so git diff is useful for previewing changes)
    curs.execute('SELECT * FROM Watersheds WHERE (Q2 IS NOT NULL) OR (QLow IS NOT NULL)')
    watersheds = [row for row in curs.fetchall()]

    # Validate the hydrologic equations. The following dictionary will be keyed by python exception concatenated to produce
    # a unique string for each type of error for each equation. These will get printed to the screen for easy cut and paste
    # into a GitHub issue for USU to resolve.
    unique_errors = {}
    check_count = 0
    for q in ['Qlow', 'Q2']:
        progbar = ProgressBar(len(watersheds), 50, f'Verifying {q} equations')
        counter = 0
        for values in watersheds:
            watershed = values['WatershedID']
            counter += 1
            progbar.update(counter)

            # proceed if the watershed has a hydrologic formula defined
            if not values[q]:
                continue

            # Load the hydrologic parameters for this watershed and substitute a placeholder for drainage area
            curs.execute('SELECT * FROM vwWatershedHydroParams WHERE WatershedID = ?', [watershed])
            params = {row['Name']: row['Value'] * row['Conversion'] for row in curs.fetchall()}
            params['DRNAREA'] = 1.0

            try:
                equation = values[q]
                equation = equation.replace('^', '**')

                # Verify that all characters are legal. Note initial carrat returns inverse matches
                m = None
                m = re.findall(r'[^0-9a-z+-/* _)(]', equation, re.IGNORECASE)
                if m is not None and len(m) > 0:
                    illegal = ''.join(m)
                    illegal = illegal.replace('\n', 'NEWLINE')
                    raise Exception(f'The equation contains illedgal characters (might include white space):{illegal}')

                value = eval(equation, {'__builtins__': None}, params)
                _float_val = float(value)
                check_count += 1

            except Exception as ex:
                # NoneType is not subscriptable means a watershed parameter is missing.
                exception_id = repr(ex) + values[q]
                if exception_id in unique_errors:
                    unique_errors[exception_id]['watersheds'][watershed] = params
                else:
                    unique_errors[exception_id] = {
                        'watersheds': {watershed: params},
                        'exception': repr(ex),
                        'equation': values[q],
                    }

        progbar.finish()

    if len(unique_errors) > 0:
        for exception_id, values in unique_errors.items():
            print('\n## Hydrologic equation Error\n```')
            print('Equation:', values['equation'])
            print('Exception:', values['exception'])
            print('Watersheds:')
            for watershed, params in values['watersheds'].items():
                print(f'\t{watershed}:')
                for key, val in params.items():
                    print(f'\t\t{key}: {val}')
            print('```')
        raise Exception(f'Aborting due to {len(unique_errors)} hydrology equation errors')

    print(f'{check_count} watershed hydrology valid equations. No errors found.')


def dict_factory(cursor, row) -> dict:
    """
    Utility function to convert a row into a dictionary
    for refereencing columns by name instead of index.
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def main():
    """
    Dump BRAT parameters from a local SQLite database to CSV files in this repo
    for commiting and pushing to GitHub.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('sqlite_path', help='Absolute path to SQLite database with latest BRAT parameters.', type=str)
    args = parser.parse_args()

    if not os.path.exists(args.sqlite_path):
        raise FileNotFoundError(f'Could not find SQLite database at {args.sqlite_path}')

    csv_path = os.path.join(os.path.dirname(__file__), '../../database/data')
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f'Could not find CSV directory at {csv_path}')

    try:
        # Check the hydrology equations first, because it will abort if there are errors
        check_hydrology_equations(args.sqlite_path)

        # Now dump the editable tables to CSV
        write_brat_params_to_csv(args.sqlite_path, csv_path)

        print('Processing completed successfully. Review changes using git before commiting and pushing.')
    except Exception as ex:
        print('Errors occurred:', str(ex))


if __name__ == "__main__":
    main()
