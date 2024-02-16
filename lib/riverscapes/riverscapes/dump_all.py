"""[summary]
"""
import sys
import os
import traceback
import argparse
from rsxml import Logger, dotenv, safe_makedirs
# Pull in the cybercastor API from that library
from cybercastor.lib.dump.dump_cybercastor import dump_cybercastor
from cybercastor import CybercastorAPI
# This is the current library
from riverscapes import RiverscapesAPI
from riverscapes.lib.dump.dump_geom import dump_geom
from riverscapes.lib.dump.dump_riverscapes import dump_riverscapes
from riverscapes.lib.dump.dump_views import dump_views


def dump_all(rs_api: RiverscapesAPI, cc_api: CybercastorAPI, sqlite_db_path: str, template_geom):
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

    if os.path.exists(sqlite_db_path):
        os.remove(sqlite_db_path)

    # If there is no DB there then create a fresh one
    if not os.path.exists(sqlite_db_path):
        log.info(f'Creating new sqlite db: {sqlite_db_path}')
        # First copy the geometry in. This will give us the gpkg tables the schema depends on
        dump_geom(sqlite_db_path, template_geom)

    # Then add the cybercastor data
    dump_cybercastor(cc_api, sqlite_db_path)
    # Then add the riverscapes data (authentication will be a browser popup)
    dump_riverscapes(rs_api, sqlite_db_path)
    # # Then write any additional views
    # dump_views(sqlite_db_path)

    log.info(f"Finished Writing: {template_geom}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument('cc_stage', help='Cybercastor API stage', type=str, default='production')
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
        with RiverscapesAPI(stage=args.rs_stage) as _rs_api, CybercastorAPI(stage=args.cc_stage) as _cc_api:
            dump_all(_rs_api, _cc_api, args.output_db_path, args.template_geom)

    except Exception as e:
        logmain.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
