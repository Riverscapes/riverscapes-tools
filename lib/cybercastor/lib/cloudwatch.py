import boto3
from datetime import datetime, timedelta
from rscommons.util import safe_makedirs, safe_remove_file
from lib.monitor import report_job
import time
from glob import glob
import re
from datetime import datetime
import json
import os


# export enum TaskStatusEnum {
#     QUEUED = 'QUEUED',
#     RUNNING = 'RUNNING',
#     STARTING = 'STARTING',
#     STOPPED = 'STOPPED',
#     SUCCEEDED = 'SUCCEEDED',
#     FAILED = 'FAILED',
#     DELETE_REQUESTED = 'DELETE_REQUESTED',
#     STOP_REQUESTED = 'STOP_REQUESTED'
# }
READY = ['STOPPED', 'SUCCEEDED', 'FAILED']
READY_RUNNING = ['STOPPED', 'SUCCEEDED', 'FAILED', 'RUNNING']


def download_job_logs(job, outdir, download_running=False):
    """Download all the Cloudwatch logs for a given job

    Args:
        job (_type_): _description_
        outdir (_type_): _description_
        download_running (bool, optional): _description_. Defaults to False.
    """
    # Get some safe names for the log output
    job_dir_name = "{}_{}".format(re.sub("[^\w]", "_", job['name']), job['id'])
    job_dir = os.path.join(outdir, job_dir_name)

    # Clean the directory to put logs into
    safe_makedirs(job_dir)

    # Write the text report
    monitor_report_path = os.path.join(job_dir, 'report.txt')
    with open(monitor_report_path, 'w') as f:
        f.write(report_job(job))

    valid_states = READY if not download_running else READY_RUNNING
    tasks = [j for j in job['tasks'] if j['status'] in valid_states and j['logStream'] is not None]
    for t in tasks:
        # Running logs always download
        task_log_path = os.path.join(job_dir, '{}-{}.log'.format(t['status'], t['name']))
        task_log_glob = os.path.join(job_dir, '*-{}.log'.format(t['name']))
        if (t['status'] == 'RUNNING' and t['logStream']) or not os.path.isfile(task_log_path):
            # Clean out any other logs for this that may exist
            for filePath in glob(task_log_glob):
                safe_remove_file(filePath)
            download_logs(job, t, 'CybercastorLogs_prod', t['logStream'], task_log_path)


def download_logs(job, task, group_name, stream, file_path):
    """ Download a single Cloudwatch log

    Args:
        job (_type_): _description_
        task (_type_): _description_
        group_name (_type_): _description_
        stream (_type_): _description_
        file_path (_type_): _description_
    """
    client = boto3.client('logs')

    with open(file_path, 'w') as out_to:
        # Every log gets a copy of the task object too
        revised_job = {k: v for k, v in job.items() if k in ['id', 'name', 'decription', 'taskDefId', 'taskScriptId', 'meta']}
        out_to.write('Cybercastor Job\n------------------------------------------------------------------------\n')
        out_to.write(json.dumps(revised_job, indent=4, sort_keys=True) + '\n')
        out_to.write('\nCybercastor Task\n------------------------------------------------------------------------\n')
        out_to.write(json.dumps(task, indent=4, sort_keys=True) + '\n')
        out_to.write('\nCybercastor Task Log\n------------------------------------------------------------------------\n')
        try:
            logs_batch = client.get_log_events(logGroupName=group_name, logStreamName=stream, startTime=0)
            for event in logs_batch['events']:
                event.update({'group': group_name, 'stream': stream})
                date_string = datetime.utcfromtimestamp(event['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                message = "{}:: {}\n".format(date_string, event['message'])
                out_to.write(message)
            print(file_path, ":", len(logs_batch['events']))

            while 'nextToken' in logs_batch:
                logs_batch = client.get_log_events(logGroupName=group_name, logStreamName=stream, nextToken=logs_batch['nextToken'])
                for event in logs_batch['events']:
                    event.update({'group': group_name, 'stream': stream})
                    out_to.write(json.dumps(event) + '\n')
        except Exception as e:
            print(e)
            out_to.write('ERROR RETRIEVING LOGS: Group: {} Stream: {}'.format(group_name, stream))
            out_to.write(e)
