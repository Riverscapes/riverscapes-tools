""" This script finds projects without tiles and puts their building in the queue


"""
import os
import json
import time
import threading
import random
import boto3
import inquirer
from rsxml import Logger
from termcolor import colored

from riverscapes import RiverscapesAPI, RiverscapesProject, RiverscapesSearchParams

# Initialize the lock
lock = threading.Lock()

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
    # 'TIMEOUT',
    'UNKNOWN',
]
# HACKY: This is a list of project types that we want to include in the search
# INCLUDE_TYPES = [
#     "rscontext",
#     "channelarea",
#     "taudem",
#     "anthro",
#     "confinement",
#     # "vbet",
#     # "rcat",
# ]


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

    do_check = inquirer.confirm("Check for missing tiles (Saying no will rebuild everyhting!)?", default=True)

    params_json_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'tile_hole_patch.json'))
    if not os.path.exists(params_json_path):
        raise Exception(f"Input file {params_json_path} not found")
    with open(params_json_path, 'r', encoding='utf-8') as f:
        search_params = RiverscapesSearchParams(json.load(f))

    rebuild_mutation = api.load_mutation('rebuildWebTiles')

    # Date string in the format of 2021-01-01
    curr_day = None
    projects_processed = 0
    projects_with_needs = 0
    tiles_queued = 0

    def project_processor(project, stats, total, _prg):
        nonlocal projects_processed, projects_with_needs, tiles_queued, curr_day
        threadLog = Logger(f"Thread: {project.id}")
        # threadLog.info(f"START")

        if project.project_type is None:
            raise Exception(f"Project {project.id} has no project type. This is likely a query error")

        projects_processed += 1
        parsed_day = project.created_date.strftime('%Y-%m-%d')
        if curr_day != parsed_day:
            curr_day = parsed_day
            threadLog.info('\n')
            threadLog.title(f"NEW DAY: Processing projects created on {parsed_day}")

        project_type = project.project_type
        rspaths = []

        # s3://TILE_BUCKET/tileMeta/PROJECT_TYTPE/PROJECT_ID/SANITIZED_RSXPATH/index.json
        # Output the created At date in ISO format like the search params require
        # threadLog.info(f'Project [{project_type}][{project.id}] created_at: {project.created_date.isoformat()}')
        if do_check:
            try:
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
                    projects_with_needs += 1
                    tiles_queued += len(rspaths)
                    # Now we can queue all the missing tiles again
                    # threadLog.info(f"Found {len(rspaths)} missing tiles in project {project.id}")
                    result = api.run_query(rebuild_mutation, {
                        'projectId': project.id,
                        'rsXPaths': [],
                        "force": True
                    })
                _prg.text = f"Incomplete Projects: {projects_with_needs} Tiles Queued: {tiles_queued} Last Date: {project.created_date.isoformat()}"
            except Exception as e:
                threadLog.info("\n")
                threadLog.error(f"Error processing project {project.id}: {e}")
        else:
            projects_with_needs += 1
            # Now we can queue all the missing tiles again
            # threadLog.info(f"Found {len(rspaths)} missing tiles in project {project.id}")
            try:
                result = api.run_query(rebuild_mutation, {
                    'projectId': project.id,
                    'rsXPaths': rspaths,
                    "force": True
                })
                tiles_queued += len(result['data']['rebuildWebTiles']['queued'])
                _prg.text = f"Projects: {projects_with_needs} Tiles Queued: {tiles_queued} Last Date: {project.created_date.isoformat()}"
            except Exception as e:
                threadLog.info("\n")
                threadLog.error(f"rebuildWebTiles Error processing project {project.id}: {e}")

        # Now update the createdOn: { to: "2021-01-01" in the search_params.json file}
        with open(params_json_path, 'w', encoding='utf-8') as f:
            new_search_params = {
                **search_params.original_json,
                "createdOn": {
                    "to": project.created_date.isoformat()
                }
            }
            json.dump(new_search_params, f, indent=2)

        # threadLog.info(f"END")
        pass

    api.process_search_results_async(project_processor, search_params, progress_bar=True, page_size=100, sort=['DATE_CREATED_DESC'], max_workers=20)

    log.info(f"Processed {projects_processed} projects with {projects_with_needs} needing tiles and {tiles_queued} tiles queued")


if __name__ == '__main__':
    log = Logger('Search Projects')
    log_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'tile_finder.log'))
    log.setup(log_path=log_path, verbose=True)
    with RiverscapesAPI(None, {
            'clientId': os.environ['RS_CLIENT_ID'],
            'secretId': os.environ['RS_CLIENT_SECRET']
        }
    ) as riverscapes_api:
        tile_patcher(riverscapes_api)

    log.info("Done!")
