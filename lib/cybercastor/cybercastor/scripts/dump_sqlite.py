import traceback
import argparse
import time
import os
import sys
from datetime import datetime
import json
from termcolor import cprint
from rsxml import Logger, dotenv
from cybercastor.lib.monitor import print_job
from cybercastor import CybercastorAPI
from cybercastor.lib.cloudwatch import download_job_logs


def get_job_diff(old, new):
    """Has any task had a change of status since the last run?

    Args:
        old ([type]): [description]
        new ([type]): [description]

    Returns:
        [type]: [description]
    """
    old_tasks = {t['id']: t for t in old['tasks']['items']}
    new_tasks = {t['id']: t for t in new['tasks']['items']}
    status_change = {}
    for tid, task in old_tasks.items():
        if tid in new_tasks:
            if (new_tasks[tid]['status'] != task['status']):
                status_change[tid] = {
                    "task": task,
                    "status": (task['status'], new_tasks[tid]['status'])
                }
        else:
            status_change[tid] = {
                "task": task,
                "status": (task['status'], None)
            }

    return status_change


def main(cc_api: CybercastorAPI, download_running: str, output_db_path: str):
    """_summary_

    Args:
        cc_stage (str): _description_
        download_running (_type_): _description_
    """

    ##############################
    # Monitoring
    ##############################

    # Now start a job loop
    monitor_json = {}
    monitor_json_path = os.path.join(os.path.dirname(__file__), 'monitor.output.json')
    monitor_logs_path = os.path.join(os.path.dirname(__file__), 'logs')
    if os.path.isfile(monitor_json_path):
        with open(monitor_json_path, 'r', encoding='utf8') as f:
            monitor_json = json.load(f)
    known_jobs = list(monitor_json.keys())

    while True:
        # Make an API query for the job that is in the output json file
        paginated_jobs = cc_api.get_job_paginated()
        print(chr(27) + "[2J")
        print(datetime.utcnow())

        active_jobs = []
        for job in paginated_jobs['jobs']:
            active_jobs.append(job['id'])
            monitor_json[job['id']] = job
            if job['id'] not in known_jobs:
                known_jobs.append(job['id'])

        # Go and get any jobs we know abotu that may not be active
        for jid in known_jobs:
            if jid not in active_jobs:
                lost_job = cc_api.get_job(jid)
                monitor_json[lost_job['id']] = lost_job

        if len(monitor_json.keys()) == 0:
            cprint('(No Active Jobs)', 'red')

        with open(monitor_json_path, 'w', encoding='utf8') as outfile:
            json.dump(monitor_json, outfile, indent=4, sort_keys=True)

        for job in monitor_json.values():
            print_job(job)

        # Now do some reporting
        for job in monitor_json.values():
            download_job_logs(job, os.path.join(monitor_logs_path, monitor_logs_path), cc_api.stage, download_running)

        if download_running:
            print("DOWNLOAD RUNNING DOESN'T LOOP")
            exit(0)

        print("Pausing 30s")
        time.sleep(30)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # See Riverscapes API in this repo for script to create the SQLite database
    parser.add_argument('output_db_path', help='Existing output SQLite database', type=str)
    parser.add_argument('cc_stage', help='Cybercastor API stage', type=str, default='production')
    parser.add_argument('download_running', help='Boolean determining whether to download running jobs', type=str, default='False')
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '.env.python'))

    # Initiate the log file
    log = Logger("Cybercastor Monitor")
    log.setup(verbose=args.verbose)
    log.title('Cybercastor Monitor')

    try:
        with CybercastorAPI(stage=args.stage) as api:
            main(api, bool(args.download_running), args.output_db_path)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
