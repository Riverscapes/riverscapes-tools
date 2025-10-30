"""
Creates a new SQLite database from CSV files in this repo containing BRAT parameters.
The database schema is defined in the SQL file in this directory.

This replaces the old NARDev Postgres database that was used to store BRAT parameters.

The new workflow is described here:
https://tools.riverscapes.net/brat/advanced/parameters/
"""
import csv
import os
import sqlite3
import argparse
from datetime import datetime
from rsxml import dotenv
from rsxml.util import safe_makedirs


def load_brat_parameters(sqlite_path: str, csv_dir: str) -> None:
    """
    Loop over the CSV files in the specified directory and load
    their contents into the SQLite database using the file name
    as the database table name.

    Args:
        sqlite_path (str): Absolute path to the SQLite database
        csv_dir (csv): Absolute folder where the CSV files are located
    """

    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    for file in os.listdir(csv_dir):
        if not (file.endswith('.csv') and os.path.isfile(os.path.join(csv_dir, file))):
            continue

        file_path = os.path.join(csv_dir, file)
        table_name = os.path.basename(file).replace('.csv', '')

        print(f'Processing database table {table_name} from file {file_path}')
        with open(file_path, mode='r', encoding='utf8') as f:
            reader = csv.DictReader(f)
            data = [row for row in reader]
            # data = [None if x == '' else x for x in data]

            for row in data:
                for key in row.keys():
                    if row[key] == '':
                        row[key] = None

        sql_insert = f'INSERT INTO {table_name} ({", ".join(data[0].keys())}) VALUES ({", ".join(["?" for _ in data[0].keys()])})'
        cursor.executemany(sql_insert, [tuple(row.values()) for row in data])

    conn.commit()


def create_database(output_dir: str) -> str:
    """
    Create the SQLite database and tables for BRAT parameters
    using the schema definition provided in the SQL file in this directory
    """

    schema_path = os.path.join(os.path.dirname(__file__), 'brat_params_schema.sql')
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f'Could not find schema file at {schema_path}')

    with open(schema_path, 'r', encoding='utf8') as file:
        schema_statements = file.read()

    sqlite_path = os.path.join(output_dir, f'brat_params_{datetime.now().strftime("%Y_%m_%d_%H_%M")}.sqlite')

    safe_makedirs(output_dir)

    # Don't want to accidentally overwrite an existing database that might have
    # had effort put into updating BRAT params. Force user to delete file first.
    # Moot as we are incorporating timestamp into file name for now.
    if os.path.exists(sqlite_path):
        raise FileExistsError(f'File already exists at {sqlite_path}')

    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    cursor.executescript(schema_statements)
    return sqlite_path


def main():
    """
    Generates local SQLite database from CSV files containing BRAT parameters
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('output_dir', help='Local dir where SQLite db will be created', type=str)
    args = dotenv.parse_args_env(parser)

    csv_dir = os.path.join(os.path.dirname(__file__), '../../database/data')
    if not os.path.exists(csv_dir):
        raise FileNotFoundError(f'Could not find CSV files at {csv_dir}')

    try:
        # Create a new, fresh, local SQLite database each time
        sqlite_path = create_database(args.output_dir)

        # Load the BRAT parameters in two steps. First the parent folder of
        # primary tables. Then the subfolders of intersect tables.
        load_brat_parameters(sqlite_path, csv_dir)
        load_brat_parameters(sqlite_path, os.path.join(csv_dir, 'intersect'))

        print(f'Processing completed successfully. SQLite database at {sqlite_path}')
    except Exception as ex:
        print('Errors occurred:', str(ex))


if __name__ == '__main__':
    main()
