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
    curs.execute("DROP TABLE IF EXISTS cybercastor_tasks;")
    curs.execute("DROP TABLE IF EXISTS cybercastor_jobs;")
    curs.execute("DROP TABLE IF EXISTS engine_scripts;")

    curs.execute("DROP TABLE IF EXISTS cybercastor_job_metadata;")
    curs.execute("DROP TABLE IF EXISTS cybercastor_jobenv;")
    curs.execute("DROP TABLE IF EXISTS cybercastor_taskenv;")

    curs.execute("""
        CREATE TABLE IF NOT EXISTS engine_scripts (
            sid integer PRIMARY KEY,
            id TEXT,
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

    resp = requests.get(url="https://cybercastor.northarrowresearch.com/engines/manifest.json")
    data = resp.json()  # Check the JSON Response Content documentation below

    # a little hard coded but oh well
    for ts in data[0]['taskScripts']:
        insert_sql = """
            INSERT INTO engine_scripts(id, name, description, localScriptPath, taskVars)
            VALUES(?,?,?,?,?)
        """
        curs.execute(insert_sql, (
            ts['id'],
            ts['name'],
            ts['description'],
            ts['localScriptPath'],
            json.dumps(ts['taskVars']),
        ))
    conn.commit()

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
                INSERT INTO cybercastor_jobs(id, createdBy, createdOn, description, name, status, taskDefId, taskScriptId)
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
            jid = curs.lastrowid
            insert_jobmeta_sql = """
                INSERT INTO cybercastor_job_metadata(jid, key, value)
                VALUES(?,?,?)
            """
            job_meta = json.loads(job['meta'])
            for key, value in job_meta.items():
                curs.execute(insert_jobmeta_sql, (
                    jid,
                    key,
                    value,
                ))
            insert_jobenv_sql = """
                INSERT INTO cybercastor_jobenv(jid, key, value)
                VALUES(?,?,?)
            """
            job_env = json.loads(job['env'])
            for key, value in job_env.items():
                curs.execute(insert_jobenv_sql, (
                    jid,
                    key,
                    value,
                ))

            for task in job['tasks']:
                insert_sql = """
                    INSERT INTO cybercastor_tasks(id, jid, jobid, createdBy, createdOn, endedOn, logStream, logUrl, cpu, memory, meta, name, queriedOn, startedOn, status, taskDefProps)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
                curs.execute(insert_sql, (
                    task['id'],
                    jid,
                    job['id'],
                    task['createdBy'],
                    int(task['createdOn']) if task['createdOn'] is not None else None,
                    int(task['endedOn']) if task['endedOn'] is not None else None,
                    task['logStream'],
                    task['logUrl'],
                    task['cpu'],
                    task['memory'],
                    task['meta'],
                    task['name'],
                    int(task['queriedOn']) if task['queriedOn'] is not None else None,
                    int(task['startedOn']) if task['startedOn'] is not None else None,
                    task['status'],
                    json.dumps(task['taskDefProps']),
                ))
                tid = curs.lastrowid
                insert_taskenv_sql = """
                    INSERT INTO cybercastor_taskenv(tid, key, value)
                    VALUES(?,?,?)
                """
                task_env = json.loads(task['env'])
                for key, value in task_env.items():
                    curs.execute(insert_taskenv_sql, (
                        tid,
                        key,
                        value,
                    ))                
            conn.commit()
    log.info("Finished Writing: {}".format(sqlite_db_path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('hucs_json', help='JSON with array of HUCS', type=str)
    parser.add_argument('output_db_path', help='The final resting place of the SQLITE DB', type=str)
    parser.add_argument('api_url', help='URL to the cybercastor API', type=str)
    parser.add_argument('username', help='API URL Username', type=str)
    parser.add_argument('password', help='API URL Password', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Stupid slash parsing
    fixedurl = args.api_url.replace(':/', '://')

    # Initiate the log file
    log = Logger("SQLite DB Dump")
    log.setup(logPath=os.path.join(args.output_db_path, "dump_sqlite.log"), verbose=args.verbose)


    today_date = date.today().strftime("%d-%m-%Y")

    # No way to separate out production from staging in cybercastor.
    sqlite_db_path = os.path.join(args.output_db_path, f'production_{today_date}.gpkg')


    try:
        dump_cybercastor(sqlite_db_path, fixedurl, args.username, args.password)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
