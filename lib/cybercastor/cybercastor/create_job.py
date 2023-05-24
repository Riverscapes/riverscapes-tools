""" helper functin to generate a job file for the cybercastor job scheduler
"""

import os
import enum
import json
import argparse

job_types = enum.Enum('rs_context', 'vbet')


# create a job file
def create_job_file(job_name, job_type, hucs, tags, orig_id, visibility='PUBLIC', server="PRODUCTION", description=None, meta=None):
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

    if isinstance(hucs, str):
        hucs = hucs.split(",")
        hucs = [str(int(huc)) for huc in hucs]

    env = {"TAGS": tags,
           "VISIBILITY": visibility,
           "ORIG_ID": orig_id}

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
    parser.add_argument('job_name', type=str, help='name of the job')
    parser.add_argument('job_type', type=str, help='type of job')
    parser.add_argument('hucs', type=str, help='hucs to run the job on')
    parser.add_argument('tags', type=str, help='tags for the job')
    parser.add_argument('orig_id', type=str, help='original id for the job')
    parser.add_argument('--description', type=str, help='description of the job')
    parser.add_argument('--visibility', type=str, help='visibility of the job', default='PUBLIC')
    parser.add_argument('--server', type=str, help='server to run the job on', default='PRODUCTION')
    parser.add_argument('--meta', type=str, help='meta data for the job', default=None)

    args = parser.parse_args()

    meta = {"PROCESSING_GROUP": "1005-1008", "INITIATIVE": "NRCS,CEAP"}
    description = "VBET run for production using all huc10s in 1003 and 1004"

    create_job_file(args.job_name, args.job_type, args.hucs, args.tags, args.orig_id, args.visibility, args.server, args.description, args.meta)

