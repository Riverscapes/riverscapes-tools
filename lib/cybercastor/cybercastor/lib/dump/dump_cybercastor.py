"""[summary]
"""
import sys
import os
import traceback
import argparse
import sqlite3
import json
from datetime import date
import requests
from rscommons import Logger, dotenv
from cybercastor.classes.CybercastorAPI import CybercastorAPI


def dump_cybercastor(db_path, cc_api_url, username, password, stage):
    """ DUmp all projects to a DB

    Args:
        output_folder ([type]): [description]
    """
    raise NotImplementedError("This function is not ready for the new cybercastor API.  It needs to be updated.")

    log = Logger('DUMP Cybercastor to SQlite')
    log.title('Dump Cybercastor to SQLITE')

    # Initialize CC API and log in
    ccAPI = CybercastorAPI(cc_api_url, username, password)

    # Connect to the DB and ensure foreign keys are enabled so that cascading deletes work
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA foreign_keys = ON')
    curs = conn.cursor()

    resp = requests.get(url="https://cybercastor.northarrowresearch.com/engines/manifest.json", timeout=30)
    data = resp.json()  # Check the JSON Response Content documentation below

    engine_data = [(ts['id'],
                    ts['name'],
                    ts['description'],
                    ts['localScriptPath'],
                    json.dumps(ts['taskVars'])) for ts in data[0]['taskScripts']]

    # We reload everything every time
    curs.execute("DELETE FROM engine_scripts;")
    curs.execute("DELETE FROM cc_jobs;")
    curs.execute("DELETE FROM cc_tasks;")
    curs.execute("DELETE FROM cc_jobenv;")
    curs.execute("DELETE FROM cc_taskenv;")
    curs.execute("DELETE FROM cc_job_metadata;")

    curs.executemany("""
        INSERT INTO engine_scripts
        (guid, name, description, local_script_path, task_vars)
        VALUES (?,?,?,?,?)
        ON CONFLICT (guid) DO UPDATE SET
          name = excluded.name,
          description = excluded.description,
          local_script_path = excluded.local_script_path,
          task_vars = excluded.task_vars""", engine_data)

    # Cascade delete all CC data and then vacuum the DB to reclaim space
    curs.execute('DELETE FROM cc_jobs')
    conn.commit()
    conn.execute('VACUUM')

    # Reset the autoincrement counters for all tables to keep the IDs in reasonable ranges
    # for table_name in ['cc_jobs', 'cc_job_metadata', 'cc_tasks', 'cc_task_metadata', 'cc_jobenv', 'cc_taskenv']:
    #     curs.execute(f"DELETE FROM sqlite_sequence WHERE name = '{table_name}'")

    job_next_token = None
    task_next_token = None
    page = 0
    num_projs = 0
    while job_next_token or page == 0:
        log.info(f"Getting page {page} of projects")
        page += 1
        # Get all project
        result = ccAPI.run_query(ccAPI.load_query('GetProfile'), {
            'jobNextToken': job_next_token,
        })
        if 'nextToken' in result:
            nexttoken = result['nextToken']
        else:
            nexttoken = None

        for job in result['jobs']:
            job_guid = job['id']
            job_env = dict(json.loads(job['env']).items())

            # "RS_API_URL": "https://api.data.riverscapes.net/staging",
            # A little hacky but anything without RS_API_URL is probably an old warehouse job
            if 'RS_API_URL' not in job_env:
                continue
            is_staging = 'staging' in job_env['RS_API_URL']
            if stage == 'staging' and not is_staging or stage == 'production' and is_staging:
                continue

            insert_sql = """
                INSERT INTO cc_jobs (
                    guid, created_by, created_on, description, name, status, task_def_id, task_script_id
                    )
                VALUES(?,?,?,?,?,?,?,?)
            """
            curs.execute(insert_sql, (
                job_guid,
                job['createdBy'],
                int(job['createdOn']),
                job['description'],
                job['name'],
                job['status'],
                job['taskDefId'],
                job['taskScriptId'],
            ))
            jid = curs.lastrowid

            curs.executemany('INSERT INTO cc_job_metadata (job_id, key, value) VALUES (?,?,?)', [
                (jid, key, value) for key, value in json.loads(job['meta']).items()])

            curs.executemany('INSERT INTO cc_jobenv (job_id, key, value) VALUES (?,?,?)', [
                (jid, key, value) for key, value in job_env.items()])

            for task in job['tasks']['items']:
                insert_sql = """
                    INSERT INTO cc_tasks (
                        job_id, guid, created_by, created_on, ended_on, log_stream, log_url, cpu, memory, name,
                        queried_on, started_on, status, task_def_props
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
                task_guid = task['id']
                curs.execute(insert_sql, (
                    jid,
                    task_guid,
                    task['createdBy'],
                    int(task['createdOn']
                        ) if task['createdOn'] is not None else None,
                    int(task['endedOn']) if task['endedOn'] is not None else None,
                    task['logStream'],
                    task['logUrl'],
                    task['cpu'],
                    task['memory'],
                    task['name'],
                    int(task['queriedOn']
                        ) if task['queriedOn'] is not None else None,
                    int(task['startedOn']
                        ) if task['startedOn'] is not None else None,
                    task['status'],
                    json.dumps(task['taskDefProps']),
                ))
                tid = curs.lastrowid

                curs.executemany('INSERT INTO cc_taskenv (task_id, key, value) VALUES (?,?,?)', [
                    (tid, key, value) for key, value in json.loads(task['env']).items()])

            conn.commit()

    log.info(f"Finished Writing: {db_path}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('hucs_json', help='JSON with array of HUCS', type=str)
    parser.add_argument('output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument('api_url', help='URL to the cybercastor API', type=str)
    parser.add_argument('username', help='API URL Username', type=str)
    parser.add_argument('password', help='API URL Password', type=str)
    parser.add_argument('stage', help='URL to the cybercastor API', type=str, default='production')
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Stupid slash parsing
    fixedurl = args.api_url.replace(':/', '://')

    # Initiate the log file
    mainlog = Logger("Cybercastor DB Dump")
    mainlog.setup(logPath=os.path.join(args.output_db_path, "dump_cybercastor.log"), verbose=args.verbose)

    today_date = date.today().strftime("%d-%m-%Y")

    # No way to separate out production from staging in cybercastor.
    sqlite_db_path = os.path.join(args.output_db_path, f'production_{today_date}.gpkg')

    try:
        dump_cybercastor(sqlite_db_path, fixedurl, args.username, args.password, args.stage)

    except Exception as e:
        mainlog.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
