"""[summary]
"""
import sys
import os
import traceback
import argparse
import sqlite3
from datetime import datetime, timedelta
from datetime import date
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI
from rscommons import Logger, dotenv
from rscommons.util import safe_makedirs


def dump_riverscapes(sqlite_db_path, stage):
    """ DUmp all projects to a DB

    Args:
        output_folder ([type]): [description]
    """
    log = Logger('DUMP Riverscapes to SQlite')
    log.title('Dump Riverscapes to SQLITE')

    conn = sqlite3.connect(sqlite_db_path)
    conn.execute('PRAGMA foreign_keys = ON')
    curs = conn.cursor()

    riverscapes_api = RiverscapesAPI(stage=stage)
    search_query = riverscapes_api.load_query('searchProjects')
    # Only refresh the token if we need to
    if riverscapes_api.access_token is None:
        riverscapes_api.refresh_token()

    searchParams = {}
    #     "meta": [{
    #         "key": "Runner",
    #         "value": "Cybercastor",
    #     }]
    # }

    # The warehouse came online in April 2023
    start_date = datetime(2023, 4, 11)
    one_day = timedelta(days=1)
    # Get the current timestamp
    current_date = datetime.now()
    grand_total = 0

    # Determine last created date projects in the database.
    # Delete all projects that were in that same day and then start the download
    # for that day over again. This will ensure we don't have duplicates.
    curs.execute("SELECT MAX(created_on) FROM rs_projects")
    last_inserted_row = curs.fetchone()
    if last_inserted_row[0] is not None:
        # Convert milliseconds to seconds and create a datetime object
        last_inserted = datetime.fromtimestamp(last_inserted_row[0] / 1000)

        # Roll back to midnight to ensure that we get all projects on this day.
        start_date = last_inserted.replace(hour=0, minute=0, second=0, microsecond=0)

        # Delete all projects created after the last full day at midnight
        curs.execute("DELETE FROM rs_projects WHERE created_on >= ?", [int(start_date.timestamp() * 1000)])

    conn.commit()
    conn.execute('VACUUM')

    # Create a timedelta object with a difference of 1 day
    while start_date <= current_date:
        searchParams['createdOn'] = {
            # Format the datetime and use the same date for from and to
            # This will mean "anything that day" and should avoid duplicates
            "from": start_date.strftime('%Y-%m-%d'),
            "to": start_date.strftime('%Y-%m-%d')
        }
        log.info(
            f"{start_date.strftime('%B %-d, %Y')}")

        limit = 500
        offset = 0
        total = 0
        day_progress = 0

        while offset == 0 or offset < total:
            # log.info(f"   Fetching projects {offset} to {offset + limit}")
            results = riverscapes_api.run_query(
                search_query, {"searchParams": searchParams, "limit": limit, "offset": offset})
            total = results['data']['searchProjects']['total']
            offset += limit

            projects = results['data']['searchProjects']['results']
            log.info(
                f"        Found {len(projects)} projects. Day Progress: {day_progress +len(projects) }/{total} Grand total: {grand_total}")
            for search_result in projects:

                project = search_result['item']
                meta = project.pop('meta', None)

                # Convert the string to datetime object
                createdOnDate = datetime.strptime(
                    project['createdOn'], '%Y-%m-%dT%H:%M:%S.%fZ')
                # Convert datetime object to Unix timestamp in milliseconds
                createOnTs = int(createdOnDate.timestamp() * 1000)

                # Insert project data
                curs.execute('''
                INSERT INTO rs_projects(project_id, name, tags, project_type_id, created_on, owned_by_id, owner_by_name, owner_by_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                             (
                                 project['id'],
                                 project['name'],
                                 ','.join(project['tags']),
                                 project['projectType']['id'],
                                 createOnTs,
                                 project['ownedBy']['id'],
                                 project['ownedBy']['name'],
                                 project['ownedBy']['__typename']
                             )
                             )
                grand_total += 1
                day_progress += 1
                project_id = curs.lastrowid

                # Insert project meta data
                curs.executemany('INSERT INTO rs_project_meta(project_id, key, value) VALUES (?, ?, ?)', [
                    (project_id, meta_item['key'], meta_item['value']) for meta_item in meta])

        # Increment the start date by one day
        start_date += one_day

    conn.commit()
    # Shut down the API since we don;t need it anymore
    riverscapes_api.shutdown()

    log.info("Finished Writing: {}".format(sqlite_db_path))


def create_views(sqlite_db_dir):
    today_date = date.today().strftime("%d-%m-%Y")

    # No way to separate out production from staging in cybercastor.
    sqlite_db_path = os.path.join(
        sqlite_db_dir, f'production_{today_date}.gpkg')
    conn = sqlite3.connect(sqlite_db_path)
    curs = conn.cursor()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('hucs_json', help='JSON with array of HUCS', type=str)
    parser.add_argument(
        'output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument(
        'stage', help='URL to the cybercastor API', type=str, default='production')
    parser.add_argument('--verbose', help='(optional) a little extra logging ',
                        action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    today_date = date.today().strftime("%d-%m-%Y")

    # No way to separate out production from staging in cybercastor.
    sqlite_db_path = os.path.join(
        args.output_db_path, f'production_{today_date}.gpkg')

    # Initiate the log file
    log = Logger("SQLite Riverscapes Dump")
    log.setup(logPath=os.path.join(args.output_db_path,
              "dump_sqlite.log"), verbose=args.verbose)

    try:
        dump_riverscapes(args.output_db_path, args.stage)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
