""" helper functin to generate a job file for the cybercastor job scheduler
"""

import os
import json
import argparse

job_types = ['rs_context', 'channel', 'taudem', 'rs_context_channel_taudem', 'vbet', 'rcat', 'rs_metric_engine', 'anthro', 'confinement', 'hydro_context', 'blm_context']
org_ids = {'BLM Riverscapes': '5d5bcccc-6632-4054-85f1-19501a6b3cdf'}

# create a job file
def create_job_file(job_name: str, job_type: str, hucs, tags, org_id, visibility='PUBLIC', server="PRODUCTION", description=None, meta=None, git_ref=None):
    """_summary_

    Args:
        job_name (_type_): _description_
        job_type (_type_): _description_
        description (_type_): _description_
        hucs (_type_): _description_
        tags (_type_): _description_
        orig_id (_type_): _description_
        visibility (str, optional): _description_. Defaults to 'PUBLIC'.
        server (str, optional): _description_. Defaults to "PRODUCTION".
        meta (_type_, optional): _description_. Defaults to None.
    """

    job_name_clean = job_name.replace(" ", "_")

    job_file = os.path.join(os.getcwd(), "jobs", job_name_clean + ".json")

    # remove all quotes from hucs
    hucs = hucs.replace('"', '')
    hucs = hucs.replace("'", "")

    if isinstance(hucs, str):
        if "," in hucs:
            hucs = hucs.split(",")
        else:
            hucs = hucs.split(" ")
            # keep leading zero

        hucs = [str(int(huc)) for huc in hucs]
        hucs = [huc.zfill(10) for huc in hucs]

    env = {"TAGS": tags,
           "VISIBILITY": visibility,
           "ORG_ID": org_id}
    
    if git_ref is not None:
        env['GIT_REF'] = git_ref

    job_json = {"$schema": "../job.schema.json",
                "name": job_name,
                "description": description,
                "taskScriptId": job_type,
                "meta": meta,
                "server": server,
                "env": env,
                "hucs": hucs}
    with open(job_file, "w", encoding="utf-8") as f:
        json.dump(job_json, f, indent=2)


if __name__ == "__main__":

    # argparse
    parser = argparse.ArgumentParser(description='Create a job file for the cybercastor job scheduler')
    parser.add_argument('huc_group', type=str, help='name of the job')
    parser.add_argument('job_type', type=str, help='type of job')
    parser.add_argument('hucs', type=str, help='hucs to run the job on')
    parser.add_argument('tags', type=str, help='tags for the job')
    parser.add_argument('org_id', type=str, help='orginization id for the job')
    parser.add_argument('--description', type=str, help='description of the job')
    parser.add_argument('--visibility', type=str, help='visibility of the job', default='PUBLIC')
    parser.add_argument('--server', type=str, help='server to run the job on', default='PRODUCTION')
    parser.add_argument('--git_ref', type=str, help="optional run on different git branch", default='master')
    parser.add_argument('--meta', type=str, help='meta data for the job', default=None)

    args = parser.parse_args()

    jobs = [args.job_type] if args.job_type != "all tools (as individual jobs)" else ['rs_context_channel_taudem', 'vbet', 'brat', 'anthro', 'rcat', 'rs_metric_engine', 'confinement', 'hydro_context']

    for job in jobs:
        # some string manipulation for the big run
        job_name = f'{job.upper()} {args.server.capitalize()} {args.huc_group}'
        meta = {"PROCESSING_GROUP": str(args.huc_group), "INITIATIVE": "NRCS,CEAP"}
        # meta = None
        description = f"{job.upper()} run for {args.server.lower()} using all huc10s in {args.huc_group}"
        git_ref = args.git_ref if args.git_ref != 'master' else None

        # create the job file
        create_job_file(job_name, job, args.hucs, args.tags, args.org_id, args.visibility, args.server, description, meta, git_ref)