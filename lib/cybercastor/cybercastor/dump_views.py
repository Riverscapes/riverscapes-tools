"""[summary]
"""
import sys
import os
import traceback
import argparse
import sqlite3
import requests
import json
from datetime import datetime
from datetime import date
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI
from rscommons import Logger, dotenv
from rscommons.util import safe_makedirs


def dump_views(sqlite_db_dir):
    today_date = date.today().strftime("%d-%m-%Y")

    # No way to separate out production from staging in cybercastor.
    sqlite_db_path = os.path.join(
        sqlite_db_dir, f'production_{today_date}.gpkg')
    conn = sqlite3.connect(sqlite_db_path)
    curs = conn.cursor()

    curs.execute("DROP VIEW IF EXISTS vw_exchange_projects;")
    curs.execute('''
      CREATE VIEW vw_exchange_projects AS
      SELECT Huc10_conus.*, 
            CASE WHEN m.project_id IS NOT NULL THEN 1 ELSE 0 END AS has_matching_project
      FROM Huc10_conus
      LEFT JOIN riverscapes_project_meta m ON m.key = 'HUC' AND m.value = Huc10_conus.HUC10
      WHERE m.project_id IS NOT NULL;
    ''')
    conn.commit()

    # Now query and get the row from gpkg_contents where the table_name is "Huc10_conus"
    curs.execute("SELECT * FROM gpkg_contents WHERE table_name = 'Huc10_conus';")
    row = curs.fetchone()
    # Now insert a new row into gpkg_contents with a new name corresponding to the view above
    curs.execute('''
    INSERT INTO gpkg_contents (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    ''',
    ('vw_exchange_projects', row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))

    conn.commit()

    log.info("Finished Writing: {}".format(sqlite_db_path))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ',
                        action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("SQLite Riverscapes Dump")
    log.setup(logPath=os.path.join(args.output_db_path,
              "dump_sqlite.log"), verbose=args.verbose)

    try:
        dump_views(args.output_db_path)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
