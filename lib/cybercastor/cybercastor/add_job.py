import traceback
import argparse
import time
import glob
from copy import copy
import os
import sys
import json
from datetime import datetime
import inquirer
from termcolor import colored
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from cybercastor import CybercastorAPI
from cybercastor.lib.monitor import print_job, possible_states
from cybercastor.lib.cloudwatch import download_job_logs
from cybercastor.lib.rs_project_finder import find_upstream_projects

# All the master values for
# taskDefId and taskScriptId comes from here:
# https://cybercastor.northarrowresearch.com/engines/manifest.json


def get_params(job_obj):
    """_summary_

    Args:
        job_obj (_type_): _description_

    Raises:
        Exception: _description_
        Exception: _description_
        Exception: _description_

    Returns:
        _type_: _description_
    """

    # Job Environment variables are common to all tasks
    new_job_env = copy(job_obj['env']) if 'env' in job_obj else {}
    new_job_meta = copy(job_obj['meta']) if 'meta' in job_obj else {}

    if 'TAGS' in new_job_env and new_job_env['TAGS'] == 'TEST_ALL':
        new_job_env['TAGS'] += f',{datetime.now().strftime("%b%d").upper()}'

    # NO_UI keeps the progress bars at bay value doesn't matter
    # Note: This only applies to Riverscapes Tools. --no-ui is harcoded
    # Into the automation jobs for rscli
    new_job_env['NO_UI'] = 'true'

    # Our input JSON is huc-specific but the Cybercastor interface is generic
    def create_task(huc, resources=None):

        # Make a dictionary of the environment variables for this task and include the HUC
        env_variables = job_obj['lookups'][huc].copy()
        env_variables['HUC'] = huc

        ret_obj = {
            "name": huc,  # Every Task needs a unique name so we can find it in the system
            # Task environment variables are unique to each task
            "env": json.dumps(env_variables)
        }

        if resources is not None:
            ret_obj['taskDefProps'] = {
                "cpu": resources["cpu"],
                "memoryLimitMiB": resources["memory"],
                "ephemeralStorageGiB": resources["disk"]
            }
        return ret_obj

    # If resources exists we need to pull it out
    job_resources = job_obj["resources"] if "resources" in job_obj else None
    if job_resources is not None:
        if "cpu" not in job_resources or "memory" not in job_resources or "disk" not in job_resources:
            raise Exception(
                'If you use "resources" then all of "cpu", "memory" and "disk" are required')
        elif job_resources["cpu"] % 256 != 0 or job_resources["memory"] % 256 != 0:
            raise Exception('cpu and memory must be multiples of 256')
        elif job_resources["disk"] < 20 or job_resources["disk"] > 200:
            raise Exception('disk must be an integer between 20 and 200')

    taskDefId = job_obj.get('taskDefId', 'riverscapesTools')

    params = {
        "job": {
            "name": job_obj['name'],
            "description": job_obj['description'],
            "taskDefId": taskDefId,  # we're hardcoding the Docker machine to this
            "taskScriptId": job_obj['taskScriptId'],
            # Turn meta and env into stringified JSON which is what the API requires
            "meta": json.dumps(new_job_meta),
            "env": json.dumps(new_job_env)
        },
        "tasks": [create_task(x, job_resources) for x in job_obj['hucs']]
    }
    return params


def main(cc_api: CybercastorAPI, job_json_dir) -> bool:
    """_summary_

    Args:
        job_json_dir (_type_): _description_
        stage (_type_): _description_

    Raises:
        Exception: _description_
        Exception: _description_
        Exception: _description_

    Returns:
        bool: _description_
    """
    log = Logger('AddJob')

    job_choices = {}
    repeat = False
    monitor_logs_path = os.path.join(os.path.dirname(__file__), '..', 'logs')
    for f in glob.glob(os.path.join(job_json_dir, '*.json')):
        fsplit = os.path.basename(f).split('.')
        if fsplit[0] not in job_choices:
            job_choices[fsplit[0]] = {"status": "Not Run"}
        if '.output.' in f:
            with open(f, 'r', encoding='utf8') as fi:
                try:
                    job_choices[fsplit[0]]["status"] = json.load(fi)['status']
                except Exception:
                    job_choices[fsplit[0]]["status"] = 'Unknown'
        else:
            with open(f, 'r', encoding='utf8') as fi:
                job_choices[fsplit[0]]["path"] = os.path.basename(f)
                try:
                    job_choices[fsplit[0]]["name"] = json.load(fi)['name']
                except Exception:
                    job_choices[fsplit[0]]["name"] = '???'

    jobs_sorted = sorted(job_choices.values(), key=lambda k: k['path'])

    questions = [
        inquirer.List('job',
                      message="Choose a Job:",
                      choices=[(f"[{jj['status']}] {jj['name']} </{jj['path']}>", jj['path']) for jj in jobs_sorted] + [('Quit', 'quit')]),
    ]
    answers = inquirer.prompt(questions)

    if answers['job'] == 'quit':
        return False

    job_path = os.path.join(job_json_dir, answers['job'])
    # Clean the directory to put logs into
    safe_makedirs(monitor_logs_path)
    safe_makedirs(job_json_dir)

    # Load our JSON configuration file
    with open(job_path, 'r', encoding='utf8') as f:
        job_obj = json.load(f)

    if 'server' not in job_obj:
        job_obj['server'] = 'PRODUCTION'
    elif job_obj['server'] not in ['PRODUCTION', 'STAGING', 'DEVELOPMENT']:
        raise Exception(
            'server must be one of PRODUCTION, STAGING, DEVELOPMENT')

    # Initialize a connection to the riverscapes API
    upstream_results = find_upstream_projects(job_obj)

    # Write the lookups back to the input file so it's remembered for next time
    with open(job_path, 'w', encoding='utf8') as f:
        json.dump(job_obj, f, indent=2)

    if upstream_results is False:
        return False

    if job_obj['server'] == 'PRODUCTION':
        job_obj['env']['RS_API_URL'] = 'https://api.data.riverscapes.net'
    elif job_obj['server'] == 'STAGING':
        job_obj['env']['RS_API_URL'] = 'https://api.data.riverscapes.net/staging'
    else:
        server = job_obj['server']
        raise Exception(f'Unknown server: {server}')

    outputFile = os.path.splitext(job_path)[0] + '.output.json'

    # Check if we have an output file to read.
    # If we already have an output file then skip the job creation step and just continue through to monitoring
    job_monitor = None
    if os.path.isfile(outputFile):
        with open(outputFile, 'r', encoding='utf8') as f:
            try:
                job_monitor = json.load(f)
            except Exception as err:
                log.error(f'error parsing: {outputFile}. Recreating it \n\n{err}')

    ##############################
    # Job Creation
    ##############################
    if job_monitor is None:
        # Make our params what the cybercastor  API needs
        params = get_params(job_obj)

        with open(outputFile, 'w', encoding='utf8') as outfile:
            # Add the job to the API
            add_job_mutation = cc_api.load_mutation('addJob')
            result = cc_api.run_query(add_job_mutation, params)
            if result is None:
                raise Exception('Error')
            job = cc_api.get_job_paginated(result['data']['addJob']['id'])
            json.dump(job, outfile, indent=4, sort_keys=True)
            job_monitor = job

    ##############################
    # Monitoring
    ##############################
    # Open the results of the addition step above and print
    # the status until everything is done

    # Now start a job loop
    while True:
        # Only refresh the token if we need to
        if cc_api.accessToken is None:
            cc_api.refresh_token()

        # Make an API query for the job that is in the output json file
        job_monitor = cc_api.get_job_paginated(job_monitor['id'])
        # Immediately write the new state to the file
        with open(outputFile, 'w', encoding='utf8') as outfile:
            json.dump(job_monitor, outfile, indent=4, sort_keys=True)

        # Clear the screen and start printing our report
        print(chr(27) + "[2J")
        print_job(job_monitor)

        menu_choice = inquirer.list_input(
            message="Actions:",
            choices=[
                ('Reload', 'reload'),
                ('Download Logs', 'download_logs'),
                ('Manage Tasks', 'task_manage'),
                ('Back to Jobs', 'menu_back'),
                ('Quit', 'quit')
            ],
            default=['reload']
        )

        if menu_choice == 'quit':
            break

        elif menu_choice == 'menu_back':
            repeat = True
            break

        elif menu_choice == 'download_logs':
            download_job_logs(job_monitor, monitor_logs_path, cc_api.stage, download_running=True)
            log.info('DONE!')
            time.sleep(3)
        elif menu_choice == 'task_manage':
            # Get a fresh copy to work with
            manage_tasks(cc_api, job_monitor['id'])

    print('Goodbye!!')
    return repeat


def manage_tasks(cc_api: CybercastorAPI, job_id):
    """_summary_

    Args:
        CybercastorAPI (_type_): _description_
        jid (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Get a fresh version
    job_monitor = cc_api.get_job_paginated(job_id)
    total_tasks = len(job_monitor['tasks']['items'])
    ts = {
        'restartable': [t for t in job_monitor['tasks']['items'] if t['status'] in ['STOPPED', 'SUCCEEDED', 'FAILED']],
        'stoppable': [t for t in job_monitor['tasks']['items'] if t['status'] in ['QUEUED', 'RUNNING']],
        'failed': [t for t in job_monitor['tasks']['items'] if t['status'] in ['FAILED']],
        'stopped': [t for t in job_monitor['tasks']['items'] if t['status'] in ['STOPPED']]
    }
    change_state = {
        'restart_failed': 'failed',
        'restart_stopped': 'stopped',
        'restart_all': 'restartable',
        'stop_all': 'stoppable'
    }

    menu_choice = inquirer.list_input(
        message="Choose?",
        choices=[
            (f"Restart tasks from list ({len(ts['restartable'])}/{total_tasks} available)", 'restart_tasks'),
            (f"Restart FAILED tasks ({len(ts['failed'])}/{total_tasks} available)", 'restart_failed'),
            (f"Restart STOPPED tasks ({len(ts['stopped'])}/{total_tasks} available)", 'restart_stopped'),
            (f"Restart ALL {len(ts['restartable'])} Restartable tasks (Stopped, Succeeded and Failed)", 'restart_all'),
            (f"Stop tasks from list ({len(ts['stoppable'])}/{total_tasks} available)", 'stop_tasks'),
            (f"Stop ALL {len(ts['stoppable'])} Running/Queued tasks", 'stop_all'),
            ('<== Menu Back', 'back')
        ],
        default=['reload']
    )

    # Tasks from list Chooser
    if menu_choice == 'restart_tasks' or menu_choice == 'stop_tasks':
        if menu_choice == 'restart_tasks':
            op_text = 'restart'
            dict_key = 'restartable'
            op = VALID_OPS['START']
        else:
            op_text = 'stop'
            dict_key = 'stoppable'
            op = VALID_OPS['STOP']
        questions = [
            inquirer.Checkbox('tasks',
                              message=f"Which tasks to {op_text}? <space> to select. <enter> to approve",
                              choices=[
                                  (colored(f"{t['name']} ({t['status']})", possible_states[t['status']]), t) for t in ts[dict_key]
                              ])
        ]
        answers = inquirer.prompt(questions)['tasks']
        if (len(answers) == 0) or not inquirer.confirm(f'This will restart {len(answers)} tasks:'):
            return
        return change_task_status(cc_api, job_id, answers, op)

    # Affected all tasks of a certain type
    elif menu_choice in change_state:
        if 'restart' in menu_choice:
            op_text = 'restart'
            op = VALID_OPS['START']
        else:
            op_text = 'stop'
            op = VALID_OPS['STOP']

        dict_key = change_state[menu_choice]
        num_available_tasks = len(ts[dict_key])

        if num_available_tasks == 0:
            return

        if inquirer.confirm(f"This will {op_text} {dict_key} {num_available_tasks} tasks:"):
            return change_task_status(cc_api, job_id, ts[dict_key], op)
        return

    print('DONE')


VALID_OPS = {'START': 'START', 'STOP': 'STOP'}


def change_task_status(cc_api: CybercastorAPI, job_id, tasks, op):
    """ Change the status of a task

    Arguments:
        CybercastorAPI {[type]} -- [description]
        tasks {[type]} -- [description]
        op {[type]} -- [description]
    """
    log = Logger('change_task_status')
    if op not in VALID_OPS.values():
        raise Exception('op not found')

    counter = 0
    for t in tasks:
        counter += 1
        try:
            if op == VALID_OPS['START']:
                start_qry = cc_api.load_mutation('startTask')
                cc_api.run_query(
                    start_qry, {'jobId': job_id, 'taskIds': t['id']})

            elif op == VALID_OPS['STOP']:
                stop_qry = cc_api.load_mutation('stopTask')
                cc_api.run_query(
                    stop_qry, {'jobId': job_id, 'taskIds': t['id']})

            log.info(f"   -Completed {op} on task: {t['name']}  ({counter}/{len(tasks)})")

        except Exception as err:
            log.error(f"   -Error {op} on task: {t['name']}  ({counter}/{len(tasks)})")
            log.error(err)

    log.info('Pausing for 10 seconds')
    time.sleep(10)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('job_json', help='The job specification JSON file', type=str)
    parser.add_argument('stage', help='Cybercastor API stage', type=str, default='production')
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    outerlog = Logger("CCAddJob")
    outerlog.setup(verbose=args.verbose)
    outerlog.title('Cybercastor Add JOB')

    try:
        with CybercastorAPI(stage=args.stage) as api:
            RETRY = True
            while RETRY is True:
                RETRY = main(api, args.job_json)

    except Exception as e:
        outerlog.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
