from datetime import datetime
from termcolor import colored, cprint
# import json

possible_states = {
    'QUEUED': 'cyan',
    'STARTING': 'cyan',
    'RUNNING': 'yellow',
    'STOPPED': 'red',
    'SUCCEEDED': 'green', 
    'FAILED': 'red',
    'DELETE_REQUESTED': 'red', 
    'STOP_REQUESTED': 'red'
    }
env_no_print = ['NO_UI']


def print_job(job):
    """ Print a job to the console

    Args:
        job (_type_): _description_
    """
    # Clear the screen and start printing our report

    org = ""
    script = f"{job['taskScript']['name']} ({job['taskScript']['id']})" if 'taskScript' in job else '???'
    # tags = ''
    try:
        if 'ORG_ID' in job['env']:
            org = f"Org: {job['env']['ORG_ID']}"
        tags = colored(', '.join(
            [f'{k}: {v}' for k, v in job['env'].items() if 'TAG' in k]), 'blue')
    except Exception as e:
        print(e)

    title = f"{job['name']}  < {org} / {script} / {job['id']} >"

    cprint(title, 'white', attrs=['bold', 'underline'])

    # cprint('=' * title_length, 'magenta')

    if 'description' in job and len(job['description']) > 0:
        cprint(f'Description: {job["description"]}', 'white')

    job_summary = {}
    print('{tags}')

    def print_status(status, color):
        queued_names = [t['name']
                        for t in job['tasks']['items'] if t['status'] == status]
        job_summary[status] = len(queued_names)
        if len(queued_names) > 0:
            cprint(f'\n{status}: ({len(queued_names)})', color)
            cprint(', '.join(queued_names), color)

    for s, c in possible_states.items():
        print_status(s, c)
    print('')


def print_date(timestamp_ms_str):
    """ Print the date in a friendly manner

    Args:
        timestamp_ms_str (_type_): _description_

    Returns:
        _type_: _description_
    """
    return datetime.utcfromtimestamp(int(timestamp_ms_str) / 1000).strftime('%b %d, %Y %H:%M:%S') + ' ({})'.format(timestamp_ms_str)


def report_job(job):
    """ Generate the report for this job

    Args:
        job (_type_): _description_

    Returns:
        _type_: _description_
    """
    output = ['']
    # Clear the screen and start printing our report
    output.append(f"{job['name']}  (id:{job['id']})")
    output.append('=================================================================')
    if 'description' in job and len(job['description']) > 0:
        output.append(job['description'])

    output.append(f"createdOn: {job['createdOn']}")
    output.append(f"updatedOn: {job['updatedOn']}")

    job_summary = {}

    def print_status(status, color):
        queuedNames = [t['name']
                       for t in job['tasks']['items'] if t['status'] == status]
        job_summary[status] = len(queuedNames)
        if len(queuedNames) > 0:
            output.append(f'\n{status}: ({len(queuedNames)})')
            output.append(', '.join(queuedNames))

    for s, c in possible_states.items():
        print_status(s, c)

    output.append('\nJobs:')
    output.append('----------------')
    output.append('\t'.join(['jobname', 'taskname', 'status', 'cpu', 'memory',
                  'ellapsedS', 'startedOn', 'endedOn', 'logUrl', 'jobid', 'taskid']))
    for t in job['tasks']['items']:
        if t['startedOn'] is None:
            ellapsed = 0
        elif t['endedOn'] is None:
            queriedOnTs = datetime.strptime(
                t['queriedOn'], "%Y-%m-%dT%H:%M:%S.%fZ")
            startedOnTs = datetime.strptime(
                t['startedOn'], "%Y-%m-%dT%H:%M:%S.%fZ")
            ellapsed = (queriedOnTs - startedOnTs) / 1000
        else:
            endedOnTs = datetime.strptime(
                t['endedOn'], "%Y-%m-%dT%H:%M:%S.%fZ")
            startedOnTs = datetime.strptime(
                t['startedOn'], "%Y-%m-%dT%H:%M:%S.%fZ")
            ellapsed = (endedOnTs - startedOnTs) / 1000

        output.append('\t'.join([job['name'], t['name'], t['status'], str(t['cpu']), str(t['memory']), str(
            ellapsed), str(t['startedOn']), str(t['endedOn']), str(t['logUrl']), job['id'], t['id']]))

    return '\n'.join(output)
