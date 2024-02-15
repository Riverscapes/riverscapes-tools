"""[summary]
"""
import sys
import os
import traceback
import argparse
import sqlite3
from datetime import datetime, date
from rsxml import Logger, dotenv
from cybercastor import RiverscapesAPI, RiverscapesSearchParams


def dump_riverscapes(db_path: str, stage: str):
    """ DUmp all projects to a DB

    Args:
        output_folder ([type]): [description]
    """
    log = Logger('DUMP Riverscapes to SQlite')
    log.title('Dump Riverscapes to SQLITE')

    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA foreign_keys = ON')
    curs = conn.cursor()

    riverscapes_api = RiverscapesAPI(stage=stage)
    riverscapes_api.refresh_token()

    searchParams = RiverscapesSearchParams({})

    # Determine last created date projects in the database.
    # Delete all projects that were in that same day and then start the download
    # for that day over again. This will ensure we don't have duplicates.
    curs.execute("SELECT MAX(created_on) FROM rs_projects")
    last_inserted_row = curs.fetchone()
    if last_inserted_row[0] is not None:
        # Convert milliseconds to seconds and create a datetime object
        last_inserted = datetime.fromtimestamp(last_inserted_row[0] / 1000)
        searchParams.createdOnFrom = last_inserted

    # Create a timedelta object with a difference of 1 day
    for project, _stats in riverscapes_api.search(searchParams):

        # Insert project data
        curs.execute('''
            INSERT INTO rs_projects(project_id, name, tags, project_type_id, created_on, owned_by_id, owner_by_name, owner_by_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
                     (
                         project.id,
                         project.name,
                         ','.join(project.tags),
                         project.project_type,
                         project.created_on.timestamp() * 1000,
                         project['json']['ownedBy']['id'],
                         project['json']['ownedBy']['name'],
                         project['json']['ownedBy']['__typename']
                     )
                     )

        # Insert project meta data
        curs.executemany('INSERT INTO rs_project_meta(project_id, key, value) VALUES (?, ?, ?)', [
            (project.id, key, value) for key, value in project.project_meta.items()
        ])

    conn.commit()
    # Shut down the API since we don;t need it anymore
    riverscapes_api.shutdown()

    log.info(f"Finished Writing: {db_path}")


def create_views(sqlite_db_dir):
    """_summary_

    Args:
        sqlite_db_dir (_type_): _description_
    """
    today_date = date.today().strftime("%d-%m-%Y")

    # No way to separate out production from staging in cybercastor.
    sqlite_db_path = os.path.join(
        sqlite_db_dir, f'production_{today_date}.gpkg')
    conn = sqlite3.connect(sqlite_db_path)
    curs = conn.cursor()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('hucs_json', help='JSON with array of HUCS', type=str)
    parser.add_argument('output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument('stage', help='URL to the cybercastor API', type=str, default='production')
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    today_date = date.today().strftime("%d-%m-%Y")

    # No way to separate out production from staging in cybercastor.
    sqlite_db_path = os.path.join(args.output_db_path, f'production_{today_date}.gpkg')

    # Initiate the log file
    mainlog = Logger("SQLite Riverscapes Dump")
    mainlog.setup(logPath=os.path.join(args.output_db_path, "dump_sqlite.log"), verbose=args.verbose)

    try:
        dump_riverscapes(args.output_db_path, args.stage)

    except Exception as e:
        mainlog.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
