import argparse
import os
import sqlite3
import inquirer
import json
from cybercastor import CybercastorAPI
from rsxml import dotenv
from .add_job import get_params

job_types = ['rs_context', 'channel', 'taudem', 'rs_context_channel_taudem', 'vbet', 'rcat', 'rs_metric_engine', 'anthro', 'confinement', 'hydro_context', 'blm_context']

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
    "lookups": {}
}


def create_and_run_batch_job(api: CybercastorAPI, stage: str, db_path: str) -> None:

    conn = sqlite3.connect(db_path)
    curs = conn.cursor()
    # curs.execute("SELECT batch_id, name FROM batches")
    # batches =

    questions = [
        inquirer.List(
            "engine",
            message="Cybercastor engine?",
            choices=job_types,
        ),
        inquirer.Text("name", message="Job name?"),
        inquirer.Text("description", message="Job description?"),
        inquirer.Text("batch_id", message="Database Batch ID?"),
        inquirer.Text("tags", message="Tags?", default="2024CONUS"),
        inquirer.Confirm("omit_existing", message="Omit HUCs that already exist?", default=True),
        inquirer.Confirm("start_job", message="Start job?", default=False),
    ]

    answers = inquirer.prompt(questions)

    sql_base = """
        SELECT b.huc10
        FROM batch_hucs b
            INNER JOIN vw_conus_hucs h ON b.huc10 = h.huc10
            {0}
        WHERE b.batch_id = ? {1}
        """

    # TODO: add inquirer prompt for the user to select where to omit existing projects
    sql_part1 = "LEFT JOIN vw_conus_projects vcp ON b.huc10 = vcp.huc10" if answers["omit_existing"] else ""
    sql_part2 = "AND vcp.huc10 IS NULL" if answers["omit_existing"] else ""

    sql_final = sql_base.format(sql_part1, sql_part2)

    curs.execute(sql_final, [answers["batch_id"]])
    hucs = [row[0] for row in curs.fetchall()]

    if len(hucs) == 0:
        print(f'No HUCs found for the given batch ID ({answers['batch_id']}). Exiting')
        return

    if (len(hucs) > 500):
        print(f'Too many HUCs {len(hucs)} found for the given batch ID. Exiting.')
        return

    job_path = os.path.join(os.path.dirname(__file__), "..", "jobs", answers["name"] + ".json")
    job_path = get_unique_filename(job_path)

    job_obj = job_template.copy()
    job_obj["name"] = answers["name"]
    job_obj["description"] = answers["description"]
    job_obj["taskScriptId"] = answers["engine"]
    job_obj["env"]["TAGS"] = answers["tags"]
    job_obj["hucs"] = hucs
    job_obj["lookups"] = {huc: {} for huc in hucs}

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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create a job file for the cybercastor job scheduler')
    parser.add_argument('stage', help='Cybercastor API stage', type=str, default='production')
    parser.add_argument('db_path', type=str, help='Path to batch database')
    args = dotenv.parse_args_env(parser)

    with CybercastorAPI(stage=args.stage) as api:
        create_and_run_batch_job(api, args.stage, args.db_path)
