"""[summary]
"""
import sys
import os
import traceback
import argparse
import sqlite3
from rscommons import Logger, dotenv
from cybercastor.lib.dump.dump_cybercastor import dump_cybercastor
from cybercastor.lib.dump.dump_geom import dump_geom
from cybercastor.lib.dump.dump_riverscapes import dump_riverscapes
from cybercastor.lib.dump.dump_views import dump_views


def dump_all(sqlite_db_dir, cybercastor_api_url, username, password, template_geom, stage):
    """_summary_

    Args:
        sqlite_db_dir (_type_): _description_
        cybercastor_api_url (_type_): _description_
        username (_type_sqlite_db_path): _description_
        password (_type_): _description_
        template_geom (_type_): _description_
        stage (_type_): _description_
    """
    log = Logger('Dump all Riverscapes and Cybercastor data to sqlite')

    if not os.path.exists(template_geom):
        log.error(f'The GeoPackge with HUC geoemtry does not exist: {template_geom}')
        raise Exception(f'The GeoPackge with HUC geoemtry does not exist: {template_geom}')

    sqlite_db_path = os.path.join(sqlite_db_dir, f'DataExchange_{stage}.gpkg')

    # TODO: TEMPORARY Cleanup DB File
    # if os.path.exists(sqlite_db_path):
    #     os.remove(sqlite_db_path)

    # If there is no DB there then create a fresh one
    if not os.path.exists(sqlite_db_path):
        log.info(f'Creating new sqlite db: {sqlite_db_path}')
        # First copy the geometry in. This will give us the gpkg tables the schema depends on
        dump_geom(sqlite_db_path, template_geom)
        # Now create our schema
        create_database('cybercastor/lib/dump/schema.sql', sqlite_db_path)

    # Then add the cybercastor data
    dump_cybercastor(sqlite_db_path, cybercastor_api_url, username, password, stage)
    # Then add the riverscapes data (authentication will be a browser popup)
    # dump_riverscapes(template_geom, stage)
    # # Then write any additional views
    # dump_views(sqlite_db_path)

    log.info("Finished Writing: {}".format(template_geom))


def create_database(schema_file_path: str, db_path: str):
    """ Create a new database from the schema file

    Args:
        schema_file (_type_): _description_
        db_name (_type_): _description_

    Raises:
        Exception: _description_
    """
    if not os.path.exists(schema_file_path):
        raise Exception(f'The schema file does not exist: {schema_file_path}')
    # Read the schema from the file
    with open(schema_file_path, 'r') as file:
        schema = file.read()

    # Connect to a new database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Execute the schema to create tables
    cursor.executescript(schema)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument('cybercastor_api_url',
                        help='URL to the cybercastor API', type=str)
    parser.add_argument(
        'username', help='Cybercastor API URL Username', type=str)
    parser.add_argument(
        'password', help='Cybercastor API URL Password', type=str)
    parser.add_argument(
        'template_geom', help='the template gpkg of huc10 geometry', type=str)
    parser.add_argument(
        'stage', help='Riverscapes stage', type=str, default='production')
    parser.add_argument('--verbose', help='(optional) a little extra logging ',
                        action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("SQLite Riverscapes Dump")
    log.setup(logPath=os.path.join(args.output_db_path,
              "dump_sqlite.log"), verbose=args.verbose)

    fixedurl = args.cybercastor_api_url.replace(':/', '://')

    try:
        dump_all(args.output_db_path, fixedurl, args.username,
                 args.password, args.template_geom, args.stage)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
