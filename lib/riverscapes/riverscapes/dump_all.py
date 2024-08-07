"""[summary]
"""
import sys
import os
import traceback
import argparse
from rsxml import Logger, dotenv, safe_makedirs
from riverscapes import RiverscapesAPI
from riverscapes.lib.dump.dump_geom import dump_geom
from riverscapes.lib.dump.dump_riverscapes import dump_riverscapes


def dump_data_exchange_projects(rs_api: RiverscapesAPI, sqlite_db_path: str, template_geom):
    """ Dump all projects from the Data Exchange to the SQLite database

    Args:
        rs_api (RiverscapesAPI): The Riverscapes Data Exchange API
        sqlite_db_path (string): Absolute path to the output SQLite database
        template_geom (string): Absolute path to GeoPackage with HUC10 geometry
    """
    log = Logger('Dump all Riverscapes Data Exchange Projects to SQLite')

    if not os.path.exists(template_geom):
        log.error(f'The GeoPackge with HUC geoemtry does not exist: {template_geom}')
        raise Exception(f'The GeoPackge with HUC geoemtry does not exist: {template_geom}')

    if os.path.exists(sqlite_db_path):
        os.remove(sqlite_db_path)

    # If there is no DB there then create a fresh one
    if not os.path.exists(sqlite_db_path):
        log.info(f'Creating new sqlite db: {sqlite_db_path}')
        # First copy the HUC10 geometry in. This will give us the GeoPackage tables the schema depends on
        dump_geom(sqlite_db_path, template_geom)

    # Add the Riverscapes Data Exchange data (authentication will be a browser popup)
    dump_riverscapes(rs_api, sqlite_db_path)
    # Write any additional views
    # dump_views(sqlite_db_path)

    log.info(f"Finished writing: {template_geom}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument('rs_stage', help='Riverscapes stage', type=str, default='production')
    parser.add_argument('template_geom', help='the template gpkg of huc10 geometry', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    logmain = Logger("SQLite Riverscapes Dump")
    db_dir = os.path.dirname(args.output_db_path)
    safe_makedirs(db_dir)
    logmain.setup(log_path=os.path.join(db_dir, "dump_sqlite.log"), verbose=args.verbose)

    try:
        with RiverscapesAPI(stage=args.rs_stage) as _rs_api:
            dump_data_exchange_projects(_rs_api, args.output_db_path, args.template_geom)

    except Exception as e:
        logmain.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
