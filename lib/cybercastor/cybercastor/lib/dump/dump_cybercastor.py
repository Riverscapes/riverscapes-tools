"""[summary]
"""
import sys
import os
import traceback
import argparse
import sqlite3
import requests
import json
from datetime import date
from cybercastor.classes.CybercastorAPI import CybercastorAPI
from rscommons import Logger, dotenv


def dump_cybercastor(sqlite_db_path, cc_api_url, username, password):
    """ DUmp all projects to a DB

    Args:
        output_folder ([type]): [description]
    """
    log = Logger('DUMP Cybercastor to SQlite')
    log.title('Dump Cybercastor to SQLITE')

    conn = sqlite3.connect(sqlite_db_path)
    curs = conn.cursor()

    # Initialize our API and log in
    ccAPI = CybercastorAPI(cc_api_url, username, password)

    curs.execute("""
        CREATE TABLE IF NOT EXISTS engine_scripts (
            sid integer PRIMARY KEY,
            guid TEXT,
            name TEXT,
            description TEXT,
            localScriptPath TEXT,
            taskVars TEXT
        );""")
    curs.execute("""
        CREATE TABLE IF NOT EXISTS cybercastor_jobs (
            jid integer PRIMARY KEY,
            id TEXT,
            createdBy TEXT,
            createdOn INTEGER,
            description TEXT,
            name TEXT,
            status TEXT,
            taskDefId TEXT,
            taskScriptId TEXT
        );""")
    curs.execute("""
        CREATE TABLE IF NOT EXISTS cybercastor_job_metadata (
            mdid integer PRIMARY KEY,
            jid INTEGER,
            key TEXT,
            value TEXT
        );""")
    curs.execute("""
        CREATE TABLE IF NOT EXISTS cybercastor_tasks (
            tid integer PRIMARY KEY,
            id TEXT,
            jid INTEGER,
            jobid TEXT,
            createdBy TEXT,
            createdOn INTEGER,
            endedOn INTEGER,
            env TEXT,
            logStream TEXT,
            logUrl TEXT,
            cpu INTEGER,
            memory INTEGER,
            meta TEXT,
            name TEXT,
            queriedOn INTEGER,
            startedOn INTEGER,
            status TEXT,
            taskDefProps TEXT
        );""")
    curs.execute("""
        CREATE TABLE IF NOT EXISTS cybercastor_jobenv (
            eid integer PRIMARY KEY,
            jid INTEGER,
            key TEXT,
            value TEXT
        );""")
    curs.execute("""
        CREATE TABLE IF NOT EXISTS cybercastor_taskenv (
            mdid integer PRIMARY KEY,
            tid INTEGER,
            key TEXT,
            value TEXT
        );""")

    conn.commit()

    resp = requests.get(
        url="https://cybercastor.northarrowresearch.com/engines/manifest.json")
    data = resp.json()  # Check the JSON Response Content documentation below

    engine_data = [(ts['id'],
                    ts['name'],
                    ts['description'],
                    ts['localScriptPath'],
                    json.dumps(ts['taskVars'])) for ts in data[0]['taskScripts']]
    curs.executemany("""INSERT INTO engine_scripts
        (guid, name, description, local_script_path, task_vars)
        VALUES (?,?,?,?,?)
        ON CONFLICT (guid) DO UPDATE SET
          name = excluded.name,
          description = excluded.description,
          local_script_path = excluded.local_script_path,
          task_vars = excluded.task_vars""", engine_data)
    conn.commit()

    curs.execute('DELETE FROM cc_jobs')

    for table_name in ['cc_jobs', 'cc_job_metadata', 'cc_tasks', 'cc_task_metadata', 'cc_jobenv', 'cc_taskenv']:
        curs.execute(
            f"UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME='{table_name}'")

    nexttoken = None
    page = 0
    num_projs = 0
    while nexttoken or page == 0:
        log.info(f"Getting page {page} of projects")
        page += 1
        # Get all project
        result = ccAPI.get_jobs(None, None, 50, nexttoken)
        if 'nextToken' in result:
            nexttoken = result['nextToken']
        else:
            nexttoken = None

        for job in result['jobs']:
            insert_sql = """
                INSERT INTO cc_jobs (guid, created_by, created_on, description, name, status, task_def_id, task_script_id)
                VALUES(?,?,?,?,?,?,?,?)
            """
            curs.execute(insert_sql, (
                job['id'],
                job['createdBy'],
                int(job['createdOn']),
                job['description'],
                job['name'],
                job['status'],
                job['taskDefId'],
                job['taskScriptId'],
            ))
            job_id = curs.lastrowid

            curs.executemany('INSERT INTO cc_job_metadata (job_id, key, value) VALUES (?,?,?)', [
                (job_id, key, value) for key, value in json.loads(job['meta']).items()])

            curs.executemany('INSERT INTO cc_jobenv (job_id, key, value) VALUES (?,?,?)', [
                (job_id, key, value) for key, value in json.loads(job['env']).items()])

            for task in job['tasks']:
                insert_sql = """
                    INSERT INTO cc_tasks (job_id, guid, created_by, created_on, ended_on, log_stream, log_url, cpu, memory, meta, name, queried_on, started_on, status, task_def_props)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
                curs.execute(insert_sql, (
                    job_id,
                    task['id'],
                    task['createdBy'],
                    int(task['createdOn']
                        ) if task['createdOn'] is not None else None,
                    int(task['endedOn']) if task['endedOn'] is not None else None,
                    task['logStream'],
                    task['logUrl'],
                    task['cpu'],
                    task['memory'],
                    task['meta'],
                    task['name'],
                    int(task['queriedOn']
                        ) if task['queriedOn'] is not None else None,
                    int(task['startedOn']
                        ) if task['startedOn'] is not None else None,
                    task['status'],
                    json.dumps(task['taskDefProps']),
                ))
                task_id = curs.lastrowid

                curs.executemany('INSERT INTO cc_taskenv (task_id, key, value) VALUES (?,?,?)', [
                    (task_id, key, value) for key, value in json.loads(task['env']).items()])

            conn.commit()

    log.info("Finished Writing: {}".format(sqlite_db_path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('hucs_json', help='JSON with array of HUCS', type=str)
    parser.add_argument(
        'output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument('api_url', help='URL to the cybercastor API', type=str)
    parser.add_argument('username', help='API URL Username', type=str)
    parser.add_argument('password', help='API URL Password', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ',
                        action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Stupid slash parsing
    fixedurl = args.api_url.replace(':/', '://')

    # Initiate the log file
    log = Logger("SQLite DB Dump")
    log.setup(logPath=os.path.join(args.output_db_path,
              "dump_sqlite.log"), verbose=args.verbose)

    today_date = date.today().strftime("%d-%m-%Y")

    # No way to separate out production from staging in cybercastor.
    sqlite_db_path = os.path.join(
        args.output_db_path, f'production_{today_date}.gpkg')

    try:
        dump_cybercastor(sqlite_db_path, fixedurl,
                         args.username, args.password)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
