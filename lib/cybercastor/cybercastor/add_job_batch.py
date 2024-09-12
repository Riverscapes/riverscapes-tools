"""
Create a cyber castor job file for a batch of HUCs
This script uses Philip's warehouse database to find HUCs that have not been processed by a given job type
"""
import argparse
import os
import sqlite3
import inquirer
import json
from cybercastor import CybercastorAPI
from rsxml import dotenv
from .add_job import get_params
from cybercastor.lib.rs_project_finder import fargate_env_keys

# Maximum number of tasks that can be submitted in a single job
MAX_TASKS = 500

# Top level keys are the job types listed in the CC engine manifest
# https://cybercastor.riverscapes.net/engines/manifest.json
# The output value is the project type ID for the project type that the job creates
# The upstream value is a list of project type IDs that the job requires to run
job_types = {
    'rs_context': {
        'output': 'rscontext',
        'upstream': []
    },
    'channel': {
        'output': 'channelarea',
        'upstream': ['rscontext'],
    },
    'taudem': {
        'output': 'taudem',
        'upstream': ['rscontext', 'channelarea'],
    },
    'rs_context_channel_taudem': {
        'output': 'rscontext',
        'upstream': [],
    },
    'vbet': {
        'output': 'vbet',
        'upstream': ['rscontext', 'channelarea', 'taudem'],
    },
    'rcat': {
        'output': 'rcat',
        'upstream': ['rscontext', 'anthro', 'taudem', 'vbet'],
    },
    'rs_metric_engine': {
        'output': 'rs_metric_engine',
        'upstream': ['rscontext', 'rcat', 'brat', 'confinement', 'vbet'],
    },
    'anthro': {
        'output': 'anthro',
        'upstream': ['rscontext', 'vbet'],
    },
    'confinement': {
        'output': 'confinement',
        'upstream': ['rscontext', 'vbet'],
    },
    'hydro_context': {
        'output': 'hydro_context',
        'upstream': ['rscontext', 'vbet'],
    },
    'brat':
    {
        'output': 'brat',
        'upstream': ['rscontext', 'vbet', 'hydro', 'anthro'],
    }
}

job_template = {
    "$schema": "../job.schema.json",
    "name": None,
    "description": None,
    "taskScriptId": None,
    "server": "PRODUCTION",
    "env": {
        "TAGS": None,
        "VISIBILITY": "PUBLIC",
        "ORG_ID": "5d5bcccc-6632-4054-85f1-19501a6b3cdf"
    },
    "hucs": [],
    "lookups": {}
}


def create_and_run_batch_job(api: CybercastorAPI, stage: str, db_path: str) -> None:

    conn = sqlite3.connect(db_path)
    curs = conn.cursor()

    questions = [
        inquirer.List(
            "engine",
            message="Cybercastor engine?",
            choices=job_types.keys(),
        ),
        inquirer.Text("name", message="Job name?"),
        inquirer.Text("description", message="Job description?"),
        inquirer.Text("batch_id", message="Database Batch ID?"),
        inquirer.Text("tags", message="Tags?", default="2024CONUS"),
        inquirer.Confirm("omit_existing", message="Omit HUCs that already exist?", default=True),
        inquirer.Confirm("start_job", message="Start job?", default=False),
    ]

    answers = inquirer.prompt(questions)

    # Note how this  uses the vw_conus_projects view to filter only to 2024 CONUS run projects (ignoring legacy model runs)
    sql_base = 'SELECT b.huc10 FROM batch_hucs b INNER JOIN vw_conus_hucs h ON b.huc10 = h.huc10 {0} WHERE b.batch_id = ? {1}'
    sql_part1 = "LEFT JOIN (SELECT huc10 FROM vw_conus_projects WHERE project_type_id = ?) vcp ON b.huc10 = vcp.huc10" if answers["omit_existing"] else ''
    sql_part2 = "AND vcp.huc10 IS NULL" if answers["omit_existing"] else ''
    sql_final = sql_base.format(sql_part1, sql_part2)

    sql_parms = [job_types[answers['engine']]['output']] if answers["omit_existing"] else []
    sql_parms.append(answers['batch_id'])

    curs.execute(sql_final, sql_parms)
    hucs = [row[0] for row in curs.fetchall()]

    if len(hucs) == 0:
        print(f'No HUCs found for the given batch ID ({answers['batch_id']}). Exiting.')
        return

    if (len(hucs) > MAX_TASKS):
        task_questions = [
            inquirer.Confirm("cap_tasks", message=f'More than {MAX_TASKS} runs. Queue the first {MAX_TASKS} HUCs?', default=True),
        ]
        task_answers = inquirer.prompt(task_questions)
        if task_answers['cap_tasks']:
            hucs = hucs[:MAX_TASKS]

    lookups = {huc: {} for huc in hucs}
    skipped_hucs = {}
    if len(job_types[answers["engine"]]['upstream']) > 0:
        for huc in hucs:
            upstream_projects, missing_project_types = get_upstream_projects(huc, answers['engine'], curs)
            if len(missing_project_types) > 0:
                skipped_hucs[huc] = missing_project_types
                lookups.pop(huc)
            else:
                lookups[huc] = upstream_projects

        print(f'Found {len(lookups)} of {len(hucs)} HUCs with all upstream projects.')
        if len(skipped_hucs) > 0:
            print(f'{len(skipped_hucs)} HUCs skipped because of missing upstream projects.')

            partial_batch_questions = [
                inquirer.Confirm('partial_batch',
                                 message=f'Continue partial batch ({len(lookups)} of {len(hucs)})?', default=False),
            ]

            missing_file = os.path.join(os.path.dirname(__file__), "..", "jobs", answers["name"] + "_missing.json")
            print(f'Writing skipped HUCs to {missing_file}')
            with open(missing_file, "w") as f:
                json.dump(skipped_hucs, f, indent=4)

            partial_batch_answers = inquirer.prompt(partial_batch_questions)
            if not partial_batch_answers['partial_batch']:
                return

    job_path = os.path.join(os.path.dirname(__file__), "..", "jobs", answers["name"] + ".json")
    job_path = get_unique_filename(job_path)

    job_obj = job_template.copy()
    job_obj["name"] = answers["name"]
    job_obj["description"] = answers["description"]
    job_obj["taskScriptId"] = answers["engine"]
    job_obj["env"]["TAGS"] = answers["tags"]
    job_obj["hucs"] = list(lookups.keys())
    job_obj["lookups"] = lookups

    if stage == 'production':
        job_obj['env']['RS_API_URL'] = 'https://api.data.riverscapes.net'
    elif stage == 'staging':
        job_obj['env']['RS_API_URL'] = 'https://api.data.riverscapes.net/staging'
    else:
        raise Exception(f'Unknown server enviornment')

    with open(job_path, "w") as f:
        json.dump(job_obj, f, indent=4)

    print(f"Job file created: {job_path}")

    if answers["start_job"]:
        print("Starting job...")
        params = get_params(job_obj)
        add_job_mutation = api.load_mutation('addJob')
        result = api.run_query(add_job_mutation, params)
        if result is None:
            raise Exception('Error')
        job_out = api.get_job_paginated(result['data']['addJob']['id'])

        outputFile = os.path.splitext(job_path)[0] + '.output.json'
        with open(outputFile, 'w', encoding='utf8') as outfile:
            json.dump(job_out, outfile, indent=4, sort_keys=True)


def get_unique_filename(filepath: str) -> str:
    # Split the file path into directory, file name, and extension
    directory, filename = os.path.split(filepath)
    name, ext = os.path.splitext(filename)

    # Start with no suffix
    counter = 1
    new_filepath = filepath

    # Increment the counter until we find a unique file name
    while os.path.exists(new_filepath):
        new_filename = f"{name}_{counter}{ext}"
        new_filepath = os.path.join(directory, new_filename)
        counter += 1

    return new_filepath


def get_upstream_projects(huc: str, job_type: str, curs: sqlite3.Cursor) -> list:

    missing_project_types = []
    upstream_projects = {}
    for upstream_type in job_types[job_type]['upstream']:
        curs.execute("SELECT project_id FROM vw_conus_projects WHERE huc10 = ? AND project_type_id = ? ORDER BY created_on DESC LIMIT 1", (huc, upstream_type))
        row = curs.fetchone()
        if row is None:
            missing_project_types.append(upstream_type)
        else:
            upstream_projects[fargate_env_keys[upstream_type]] = row[0]

    return upstream_projects, missing_project_types


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create a job file for the cybercastor job scheduler')
    parser.add_argument('stage', help='Cybercastor API stage', type=str, default='production')
    parser.add_argument('db_path', type=str, help='Path to batch database')
    args = dotenv.parse_args_env(parser)

    with CybercastorAPI(stage=args.stage) as api:
        create_and_run_batch_job(api, args.stage, args.db_path)
