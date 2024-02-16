"""[summary]
"""
import sys
import os
import traceback
import argparse
import sqlite3
# from datetime import date
from rsxml import Logger, dotenv
# from rsxml import safe_makedirs


def dump_views(sqlite_db_path):
    """_summary_

    Args:
        sqlite_db_path (_type_): _description_
    """
    log = Logger('DUMP views to SQlite')
    log.title('Dump Views to SQLITE')

    conn = sqlite3.connect(sqlite_db_path)
    curs = conn.cursor()

    curs.execute("DROP VIEW IF EXISTS vw_exchange_projects;")
    # Here's a view that shows only where there ARE matching projects in the Data Exchange
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
    gpkg_contents_row = curs.fetchone()
    # Now insert a new row into gpkg_contents with a new name corresponding to the view above
    curs.execute('''
    INSERT INTO gpkg_contents (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    ''',
                 ('vw_exchange_projects',
                  gpkg_contents_row[1],
                  gpkg_contents_row[2],
                  gpkg_contents_row[3],
                  gpkg_contents_row[4],
                  gpkg_contents_row[5],
                  gpkg_contents_row[6],
                  gpkg_contents_row[7],
                  gpkg_contents_row[8],
                  gpkg_contents_row[9]))

    conn.commit()

    log.info(f"Finished Writing: {sqlite_db_path}")


def make_gpkgrows(conn, table_name: str):
    """This adds in the rows for the new view into the gpkg_contents, gpkg_extensions, and gpkg_geometry_columns tables.
    Which makes views visible in QGIS as geometry layers

    Args:
        conn (_type_): _description_
        table_name (str): _description_
    """
    curs = conn.cursor()
    curs.execute("SELECT * FROM gpkg_contents WHERE table_name = 'Huc10_conus';")
    cnt_row = curs.fetchone()
    cnt_row[0] = table_name
    # Now insert a new row into gpkg_contents with a new name corresponding to the view above
    curs.execute('''INSERT INTO gpkg_contents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''', cnt_row)

    curs.execute("SELECT * FROM gpkg_extensions WHERE table_name = 'Huc10_conus';")
    ext_row = curs.fetchone()
    ext_row[0] = table_name
    # Now insert a new row into gpkg_contents with a new name corresponding to the view above
    curs.execute('''INSERT INTO gpkg_extensions VALUES (?, ?, ?, ?, ?);''', ext_row)

    curs.execute("SELECT * FROM gpkg_geometry_columns WHERE table_name = 'Huc10_conus';")
    geom_row = curs.fetchone()
    geom_row[0] = table_name
    # Now insert a new row into gpkg_contents with a new name corresponding to the view above
    curs.execute('''INSERT INTO gpkg_geometry_columns VALUES (?, ?, ?, ?, ?, ?);''', geom_row)

    conn.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ',
                        action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    logmain = Logger("SQLite Riverscapes Dump")
    logmain.setup(logPath=os.path.join(args.output_db_path,
              "dump_sqlite.log"), verbose=args.verbose)

    try:
        dump_views(args.output_db_path)

    except Exception as e:
        logmain.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
