from glob import glob
import json
import os
import re
from datetime import datetime
import boto3
from rsxml.util import safe_makedirs, safe_remove_file
from cybercastor.lib.monitor import report_job


# export enum TaskStatusEnum {
#     DELETE_REQUESTED = 'DELETE_REQUESTED',
#     FAILED = 'FAILED',
#     QUEUED = 'QUEUED',
#     RUNNING = 'RUNNING',
#     STARTING = 'STARTING',
#     STOPPED = 'STOPPED',
#     STOP_REQUESTED = 'STOP_REQUESTED'
#     SUCCEEDED = 'SUCCEEDED',
# }
READY = ['STOPPED', 'SUCCEEDED', 'FAILED']
READY_RUNNING = ['STOPPED', 'SUCCEEDED', 'FAILED', 'RUNNING']


def download_job_logs(job, outdir: str, stage: str, download_running=False, download_success=False, download_failure=False):
    """Download all the Cloudwatch logs for a given job

    Args:
        job (_type_): _description_
        outdir (_type_): _description_
        download_running (bool, optional): _description_. Defaults to False.
    """
    # Get some safe names for the log output
    job_dir_name = "{}_{}".format(re.sub(r"[^\w]", "_", job['name']), job['id'])
    job_dir = os.path.join(outdir, job_dir_name)

    # Clean the directory to put logs into
    safe_makedirs(job_dir)

    # Write the text report
    monitor_report_path = os.path.join(job_dir, 'report.txt')
    with open(monitor_report_path, 'w', encoding='utf8') as f:
        f.write(report_job(job))

    valid_states = READY if not download_running else READY_RUNNING
    tasks = [j for j in job['tasks']['items'] if j['status'] in valid_states and j['logStream'] is not None]
    for t in tasks:
        # Running logs always download
        task_log_path = os.path.join(
            job_dir, f"{t['status']}-{t['name']}.log")
        task_log_glob = os.path.join(job_dir, f"*-{t['name']}.log")

        task_status = t['status']
        if 'logStream' not in t or not t['logStream']:
            continue
        if task_status == 'RUNNING' and not download_running:
            continue
        if task_status == 'SUCCEEDED' and not download_success:
            continue
        if task_status == 'FAILED' and not download_failure:
            continue
        if os.path.isfile(task_log_path):
            continue

        # Clean out any other logs for this that may exist
        for filePath in glob(task_log_glob):
            safe_remove_file(filePath)
        # TODO: I don't love this but for now it will need to do
        log_group = 'CybercastorLogs_staging' if stage == 'STAGING' else 'CybercastorLogs_production'
        download_logs(job, t, log_group, t['logStream'], task_log_path)


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

    with open(file_path, 'w', encoding='utf8') as out_to:
        # Every log gets a copy of the task object too
        revised_job = {k: v for k, v in job.items(
        ) if k in ['id', 'name', 'decription', 'taskDefId', 'taskScriptId', 'meta']}
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
                message = f"{date_string}:: {event['message']}\n"
                out_to.write(message)
            print(file_path, ":", len(logs_batch['events']))

            while 'nextToken' in logs_batch:
                logs_batch = client.get_log_events(logGroupName=group_name, logStreamName=stream, nextToken=logs_batch['nextToken'])
                for event in logs_batch['events']:
                    event.update({'group': group_name, 'stream': stream})
                    out_to.write(json.dumps(event) + '\n')
        except Exception as e:
            print(e)
            out_to.write(f'ERROR RETRIEVING LOGS: Group: {group_name} Stream: {stream}')
            out_to.write(str(e))
