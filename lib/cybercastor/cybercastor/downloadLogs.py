import traceback
import argparse
import time
import os
import sys
from termcolor import cprint
from datetime import datetime
import json
from rscommons import Logger, dotenv
from cybercastor.lib.monitor import print_job
from cybercastor.lib import CybercastorAPI
from cybercastor.lib.cloudwatch import download_job_logs


def get_job_diff(old, new):
    """Has any task had a change of status since the last run?

    Args:
        old ([type]): [description]
        new ([type]): [description]

    Returns:
        [type]: [description]
    """
    old_tasks = {t['id']: t for t in old['tasks']}
    new_tasks = {t['id']: t for t in new['tasks']}
    status_change = {}
    for id, task in old_tasks.items():
        if id in new_tasks:
            if (new_tasks[id]['status'] != task['status']):
                status_change[id] = {
                    "task": task,
                    "status": (task['status'], new_tasks[id]['status'])
                }
        else:
            status_change[id] = {
                "task": task,
                "status": (task['status'], None)
            }

    return status_change


def main(cc_stage, download_running):

    # Initialize our API and log in
    CybercastorAPI = CybercastorAPI.CybercastorAPI(stage=cc_stage)

    ##############################
    # Monitoring
    ##############################

    # Now start a job loop
    monitor_json = {}
    monitor_json_path = os.path.join(os.path.dirname(__file__), '..', 'logs', 'monitor.output.json')
    monitor_logs_path = os.path.join(os.path.dirname(__file__), '..', 'logs')
    if os.path.isfile(monitor_json_path):
        with open(monitor_json_path) as f:
            monitor_json = json.load(f)
    known_jobs = list(monitor_json.keys())

    while True:
        # Make an API query for the job that is in the output json file
        paginated_jobs = CybercastorAPI.get_jobs('ACTIVE')
        print(chr(27) + "[2J")
        print(datetime.utcnow())

        active_jobs = []
        for job in paginated_jobs['jobs']:
            job['meta'] = json.loads(job['meta'])
            job['env'] = json.loads(job['env'])

            active_jobs.append(job['id'])
            monitor_json[job['id']] = job
            if job['id'] not in known_jobs:
                known_jobs.append(job['id'])

        # Go and get any jobs we know abotu that may not be active
        for jid in known_jobs:
            if jid not in active_jobs:
                lost_job = CybercastorAPI.get_job(jid)
                monitor_json[lost_job['id']] = lost_job

        if len(monitor_json.keys()) == 0:
            cprint('(No Active Jobs)', 'red')

        with open(monitor_json_path, 'w') as outfile:
            json.dump(monitor_json, outfile, indent=4, sort_keys=True)

        for job in monitor_json.values():
            print_job(job)

        # Now do some reporting
        for job in monitor_json.values():
            download_job_logs(job, os.path.join(monitor_logs_path, monitor_logs_path), cc_stage, download_running)

        if download_running:
            print("DOWNLOAD RUNNING DOESN'T LOOP")
            exit(0)

        print("Pausing 30s")
        time.sleep(30)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Cybercastor API stage', type=str, default='production')
    parser.add_argument('log', help='log', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--download_running', help='(optional) download running logs. This is expensive so try to use sparingly', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Stupid slash parsing
    fixedurl = args.api_url.replace(':/', '://')

    # Initiate the log file
    log = Logger("Cybercastor Monitor")
    log.setup(verbose=args.verbose)
    log.title('Cybercastor Monitor')

    try:
        main(fixedurl, args.username, args.password, args.download_running, args.log)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
