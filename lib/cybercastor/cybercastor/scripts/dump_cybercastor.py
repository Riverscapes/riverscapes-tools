"""[summary]
"""
import sys
import os
import traceback
import argparse
import sqlite3
import json
from datetime import date
from dateutil.parser import parse as dateparse
from rsxml import Logger, dotenv
from cybercastor import CybercastorAPI

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), '../lib/dump/cybercastor_schema.sql')


def dump_cybercastor(cc_api: CybercastorAPI, db_path: str):
    """ Dump cyber castor runs to SQLite database

    Args:
        output_folder ([type]): [description]
    """

    log = Logger('DUMP Cybercastor to SQlite')
    log.title('Dump Cybercastor to SQLITE')

    # We can safely run this every time since we're going to delete everything
    create_database(db_path)

    # Connect to the DB and ensure foreign keys are enabled so that cascading deletes work
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA foreign_keys = ON')
    curs = conn.cursor()

    cc_engines = cc_api.get_engines()
    # Find our engine
    riverscapes_tools_engine = next((e for e in cc_engines if e['id'] == 'riverscapesTools'), None)
    if riverscapes_tools_engine is None:
        log.error("Could not find the riverscapesTools engine")
        return
    # Find the task scripts
    engine_data = [(ts['id'],
                    ts['name'],
                    ts['description'],
                    ts['localScriptPath'],
                    json.dumps(ts['taskVars'])) for ts in riverscapes_tools_engine['taskScripts']]

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

    ALL_JOB_STATUSES = ['ACTIVE', 'COMPLETE', 'RESTART_REQUESTED', 'DELETE_REQUESTED', 'STOP_REQUESTED']

    for status in ALL_JOB_STATUSES:
        results = cc_api.get_jobs_by_status(status)

        for job in results:
            job_guid = job['id']
            job_env = job['env']

            # "RS_API_URL": "https://api.data.riverscapes.net/staging",
            # A little hacky but anything without RS_API_URL is probably an old warehouse job
            if 'RS_API_URL' not in job_env:
                continue
            is_staging = 'staging' in job_env['RS_API_URL']
            # A little hack-y for now but let's just filter out all Riverscapes staging projects
            if is_staging:
                continue

            insert_sql = """
                INSERT INTO cc_jobs (
                    guid, created_by, created_on, description, name, status, task_def_id, task_script_id
                    )
                VALUES(?,?,?,?,?,?,?,?)
            """
            curs.execute(insert_sql, (
                job_guid,
                job['createdBy']['id'],
                int(dateparse(job['createdOn']).timestamp() * 1000),
                job['description'],
                job['name'],
                job['status'],
                job['taskDefId'],
                job['taskScriptId'],
            ))
            jid = curs.lastrowid

            curs.executemany('INSERT INTO cc_job_metadata (job_id, key, value) VALUES (?,?,?)', [
                (jid, key, value) for key, value in job['meta'].items()])

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
                task_def_props = json.dumps(task['taskDefProps'])
                task_def_props_sql = task_def_props if task_def_props != 'null' else None

                curs.execute(insert_sql, (
                    jid,
                    task_guid,
                    task['createdBy']['id'],
                    int(dateparse(task['createdOn']).timestamp() * 1000) if task['createdOn'] is not None else None,
                    int(dateparse(task['endedOn']).timestamp() * 1000) if task['endedOn'] is not None else None,
                    task['logStream'],
                    task['logUrl'],
                    task['cpu'],
                    task['memory'],
                    task['name'],
                    int(dateparse(task['queriedOn']).timestamp() * 1000) if task['queriedOn'] is not None else None,
                    int(dateparse(task['startedOn']).timestamp() * 1000) if task['startedOn'] is not None else None,
                    task['status'],
                    task_def_props_sql,
                ))
                tid = curs.lastrowid

                curs.executemany('INSERT INTO cc_taskenv (task_id, key, value) VALUES (?,?,?)', [
                    (tid, key, value) for key, value in task['env'].items()])

            conn.commit()

    log.info(f"Finished Writing: {db_path}")


def create_database(db_path: str):
    """ Create a new database from the schema file

    Args:
        schema_file (_type_): _description_
        db_name (_type_): _description_

    Raises:
        Exception: _description_
    """
    log = Logger('CreateCybercastorDatabase')

    if not os.path.exists(db_path) and not os.path.exists(SCHEMA_FILE):
        raise Exception(f'The schema file does not exist: {SCHEMA_FILE}')

    # Read the schema from the file
    with open(SCHEMA_FILE, 'r', encoding='utf8') as file:
        schema = file.read()

    # Connect to a new (or existing) database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Execute the schema to create tables
    log.info(f'Creating CYBERCASTOR database tables (if not exist): {db_path}')
    cursor.executescript(schema)

    conn.commit()
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output_db_path', help='Absolute path to output SQLite database', type=str)
    parser.add_argument('cc_stage', help='The Cybercastor stage', type=str, default='production')
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    mainlog = Logger("Cybercastor DB Dump")
    mainlog.setup(log_path=os.path.join(os.path.dirname(args.output_db_path), "dump_cybercastor.log"), verbose=args.verbose)

    # today_date = date.today().strftime("%d-%m-%Y")

    try:
        with CybercastorAPI(stage=args.cc_stage) as api:
            dump_cybercastor(api, args.output_db_path)

    except Exception as e:
        mainlog.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
