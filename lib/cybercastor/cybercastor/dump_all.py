"""[summary]
"""
import sys
import os
import traceback
import argparse
from datetime import date
from rscommons import Logger, dotenv
from rscommons.util import safe_makedirs
from cybercastor.lib.dump.dump_cybercastor import dump_cybercastor
from cybercastor.lib.dump.dump_geom import dump_geom
from cybercastor.lib.dump.dump_riverscapes import dump_riverscapes
from cybercastor.lib.dump.dump_views import dump_views


def dump_all(sqlite_db_dir, cybercastor_api_url, username, password, template_geom, stage):
    """_summary_

    Args:
        sqlite_db_dir (_type_): _description_
        cybercastor_api_url (_type_): _description_
        username (_type_): _description_
        password (_type_): _description_
        template_geom (_type_): _description_
        stage (_type_): _description_
    """
    log = Logger('Dump all Riverscapes and Cybercastor data to sqlite')
    today_date = date.today().strftime("%d-%m-%Y")

    if not os.path.exists(template_geom):
        log.error(f'The GeoPackge with HUC geoemtry does not exist: {template_geom}')
        raise Exception(f'The GeoPackge with HUC geoemtry does not exist: {template_geom}')

    # First copy the geometry in
    # dump_geom(sqlite_db_path, template_geom)
    # Then add the cybercastor data
    dump_cybercastor(template_geom, cybercastor_api_url, username, password)
    # Then add the riverscapes data (authentication will be a browser popup)
    dump_riverscapes(sqlite_db_path, stage)
    # Then write any additional views
    dump_views(sqlite_db_path)

    log.info("Finished Writing: {}".format(sqlite_db_path))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument('cybercastor_api_url', help='URL to the cybercastor API', type=str)
    parser.add_argument('username', help='Cybercastor API URL Username', type=str)
    parser.add_argument('password', help='Cybercastor API URL Password', type=str)
    parser.add_argument('template_geom', help='the template gpkg of huc10 geometry', type=str)
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
        dump_all(args.output_db_path, fixedurl, args.username, args.password, args.template_geom, args.stage)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
