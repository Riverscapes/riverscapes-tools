"""[summary]
"""
import sys
import os
import traceback
import argparse
import sqlite3
# import json
from datetime import date
# import requests
# from cybercastor import CybercastorAPI
from rsxml import Logger, dotenv
# from rsxml import safe_makedirs


def dump_geom(sqlite_db_path: str, geom_template_db: str):
    """ DUmp all projects to a DB

    Args:
        output_folder ([type]): [description]
    """
    log = Logger('DUMP Geometry to SQlite')
    log.title('Dump Geometry to SQLITE')

    conn = sqlite3.connect(sqlite_db_path)
    curs = conn.cursor()

    # Initialize our API and log in
    # Open source and destination databases
    source_conn = sqlite3.connect(geom_template_db)
    dest_conn = sqlite3.connect(sqlite_db_path)

    # Set up cursor for both databases
    source_cursor = source_conn.cursor()
    dest_cursor = dest_conn.cursor()

    # Get list of tables from source database
    source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = [row[0] for row in source_cursor.fetchall()]

    # Iterate over table names and copy each table to destination database
    for table_name in table_names:
        if table_name == 'sqlite_master' or table_name == 'sqlite_sequence':
            continue

        # Get the schema of the source table
        source_cursor.execute(f"PRAGMA table_info({table_name})")
        schema = source_cursor.fetchall()

        # Set up the SQL query to create the destination table
        dest_columns = []
        for column in schema:
            name = column[1]
            datatype = column[2]
            dest_columns.append(f"{name} {datatype}")
        dest_columns_str = ','.join(dest_columns)

        dest_drop_query = f"DROP TABLE IF EXISTS {table_name}"
        dest_cursor.execute(dest_drop_query)

        dest_create_query = f"CREATE TABLE {table_name} ({dest_columns_str})"

        # Execute the create query
        dest_cursor.execute(dest_create_query)

        # Select data from source table and insert into destination table
        data = source_cursor.execute(f"SELECT * FROM {table_name}").fetchall()
        if len(data) > 0:
            dest_cursor.executemany(f"INSERT INTO {table_name} VALUES ({','.join(['?' for i in range(len(data[0]))])})", data)

    # Commit changes to the destination database
    dest_conn.commit()

    # Close database connections
    source_conn.close()
    dest_conn.close()

    log.info(f"Finished Writing: {sqlite_db_path}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('hucs_json', help='JSON with array of HUCS', type=str)
    parser.add_argument('output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument('template_db_path', help='the template database of geometry', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    today_date = date.today().strftime("%d-%m-%Y")

    # No way to separate out production from staging in cybercastor.
    sqlite_db_path_input = os.path.join(args.output_db_path, f'production_{today_date}.gpkg')

    # Initiate the log file
    mainlog = Logger("SQLite DB GEOMETRY Dump")
    mainlog.setup(logPath=os.path.join(sqlite_db_path_input, "dump_geometry.log"), verbose=args.verbose)

    try:
        dump_geom(args.output_db_path, args.template_db_path)

    except Exception as e:
        mainlog.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
