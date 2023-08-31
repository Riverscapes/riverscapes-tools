"""
Demo script to download files from Data Exchange
"""
import sys
import os
import traceback
import argparse
from rscommons import Logger, dotenv
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI
from rscommons import Logger, dotenv
from rscommons.util import safe_makedirs


def download_files(stage, local_folder):
    """[summary]"""

    riverscapes_api = RiverscapesAPI(stage=stage)
    search_query = riverscapes_api.load_query('searchProjects')
    # Only refresh the token if we need to
    if riverscapes_api.accessToken is None:
        riverscapes_api.refresh_token()

    project_files_query = riverscapes_api.load_query('projectFiles')

    # Add items to this dictionary to filter which projects are retrieved.
    # For example, to only get projects for a certain HUC, project type or model version etc.
    search_params = {
        'projectTypeId': 'VBET',
        'meta': [{
            'key': 'HUC',
            'value': '1707030107'
        }]
    }

    project_limit = 500
    project_offset = 0
    total = 0
    while project_offset == 0 or project_offset < total:
        results = riverscapes_api.run_query(search_query, {"searchParams": search_params, "limit": project_limit, "offset": project_offset})
        total = results['data']['searchProjects']['total']
        project_offset += project_limit

        projects = results['data']['searchProjects']['results']

        for search_result in projects:

            project = search_result['item']

            file_limit = 500
            file_offset = 0
            file_total = 0

            # Query for datasets in the project. Then loop over the files within the dataset
            while file_offset == 0 or file_offset < file_total:
                file_results = riverscapes_api.run_query(project_files_query, {"projectId": project['id'], "limit": file_limit, "offset": file_offset}) 
                project_datasets = file_results['data']['project']['datasets']
                file_total = project_datasets['total']
                file_offset += file_limit

                for dataset in project_datasets['items']:
                    for file in dataset['files']:
                        local_path = f"{local_folder}/{project['id']}/{dataset['localPath']}"
                        safe_makedirs(os.path.dirname(local_path))
                        riverscapes_api.download_file(file['downloadUrl'], local_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Riverscapes stage', type=str, default='production')
    parser.add_argument('local_folder', help='Top level folder where to download files', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    try:
        download_files(args.stage, args.local_folder)

    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
