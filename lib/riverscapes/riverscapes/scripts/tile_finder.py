""" This script demonstrates how to search for projects on the server

NOTE: We set max_results=1234 on all these queries for demo purposes. You probably don't want to do that in production

    To run this file in VSCode choose "Python: Current File (Cybercastor)" from the command palette


"""
import os
import json
import time
import logging
import boto3
import inquirer
from rsxml import Logger
from termcolor import colored
from riverscapes import RiverscapesAPI, RiverscapesProject, RiverscapesSearchParams

log = Logger('Search Projects')
TILEABLE_TYPES = [
    'Dem',
    'Geopackage',
    'HillShade',
    'Raster',
    'HtmlFile',
    'Vector'
]
RE_TILEABLE_STATES = [
    # 'CREATING',
    # 'FETCHING',
    # 'FETCH_ERROR',
    'INDEX_NOT_FOUND',
    # 'LAYER_NOT_FOUND',
    # 'NOT_APPLICABLE',
    # 'NO_GEOMETRIES',
    'QUEUED',
    # 'SUCCESS',
    # 'TILING_ERROR',
    'TIMEOUT',
    'UNKNOWN',
]


def tile_patcher(api: RiverscapesAPI):
    """ Finding duplicate projects

    Args:
        api (RiverscapesAPI): _description_

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """

    log.title("Finding project and tiles")
    s3 = boto3.client('s3')

    if not os.environ.get('TILE_BUCKET'):
        raise Exception("You need to set the TILE_BUCKET environment variable to the name of the bucket where the tiles are stored")
    BUCKET_NAME = os.environ.get('TILE_BUCKET')

    log.warning("THIS SCRIPT WILL NOT WORK IF YOU DO NOT HAVE AWS API ACCESS TO THE TILING BUCKET (OAUTH is still required but isn't enough)")
    inquirer.confirm("Do you want to continue?", default=True)

    input_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'tile_hole_patch.json'))
    if not os.path.exists(input_path):
        raise Exception(f"Input file {input_path} not found")
    with open(input_path, 'r', encoding='utf-8') as f:
        search_params = RiverscapesSearchParams(json.load(f))

    for project, _stats, _total in api.search(search_params, progress_bar=False, page_size=10, sort=['DATE_CREATED_ASC']):
        if project.project_type is None:
            raise Exception(f"Project {project.id} has no project type. This is likely a query error")

        project_type = project.project_type
        rspaths = []

        # s3://TILE_BUCKET/tileMeta/PROJECT_TYTPE/PROJECT_ID/SANITIZED_RSXPATH/index.json
        # Output the created At date in ISO format like the search params require
        log.info(f'Project [{project_type}][{project.id}] created_at: {project.created_date.isoformat()}\n')
        full_project = api.get_project_full(project.id)
        if full_project is None:
            raise Exception(f"Project {project.id} not found")
        total_datasetnum = len(full_project.json['datasets']['items'])
        tileable_num = 0
        for ds in full_project.json['datasets']['items']:
            if ds['datasetType'] in TILEABLE_TYPES:
                tileable_num += 1
                # Create the S3 path we need to search for:
                sanitized_rs_path = ds['rsXPath'].replace('#', '_')
                s3_path = f"tileMeta/{project_type}/{project.id}/{sanitized_rs_path}/index.json"
                # Now do an S3 head to see if it exists
                try:
                    # Download the file, parse it as JSON and
                    file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_path)
                    file_content = file_obj['Body'].read().decode('utf-8')
                    tile_meta = json.loads(file_content)
                    state = tile_meta.get('state', 'UNKNOWN')
                    if state in RE_TILEABLE_STATES:
                        rspaths.append(ds['rsXPath'])
                except Exception as e:
                    log.error(f"Could not find {s3_path}")
                    rspaths.append(ds['rsXPath'])

        if len(rspaths) > 0:
            # Now we can queue all the missing tiles again
            log.info(f"Found {len(rspaths)} missing tiles in project {project.id}")

        print(f'     RESULT: {tileable_num}/{tileable_num} datasets are tileable and {len(rspaths)} are missing')
        pass


if __name__ == '__main__':
    log = Logger('Search Projects')
    log.setup(verbose=True)
    with RiverscapesAPI() as riverscapes_api:
        tile_patcher(riverscapes_api)

    log.info("Done!")
