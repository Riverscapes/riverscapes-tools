"""
Demo script to download files from Data Exchange
"""
import sys
import os
import traceback
import argparse
import datetime
import sqlite3
from shutil import rmtree
import semantic_version
from osgeo import ogr
from typing import Dict
from rscommons import Logger, dotenv, GeopackageLayer
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI
from rscommons.util import safe_makedirs

fields = {
    'LevelPathI': ogr.OFTString,
    'seg_distance': ogr.OFTReal,
    'stream_size': ogr.OFTReal,
    'window_size': ogr.OFTReal,
    'centerline_length': ogr.OFTReal,
    'window_area': ogr.OFTReal,
    'integrated_width': ogr.OFTReal,
    'active_floodplain_area': ogr.OFTReal,
    'active_floodplain_proportion': ogr.OFTReal,
    'active_floodplain_itgr_width': ogr.OFTReal,
    'active_channel_area': ogr.OFTReal,
    'active_channel_proportion': ogr.OFTReal,
    'active_channel_itgr_width': ogr.OFTReal,
    'inactive_floodplain_area': ogr.OFTReal,
    'inactive_floodplain_proportion': ogr.OFTReal,
    'inactive_floodplain_itgr_width': ogr.OFTReal,
    'floodplain_area': ogr.OFTReal,
    'floodplain_proportion': ogr.OFTReal,
    'floodplain_itgr_width': ogr.OFTReal,
    'vb_acreage_per_mile': ogr.OFTReal,
    'vb_hectares_per_km': ogr.OFTReal,
    'active_acreage_per_mile': ogr.OFTReal,
    'active_hectares_per_km': ogr.OFTReal,
    'inactive_acreage_per_mile': ogr.OFTReal,
    'inactive_hectares_per_km': ogr.OFTReal
}


def download_files(stage, project_types, hucs, dataset_xml_id, output_db):
    """[summary]"""

    # Create the output feature class fields. Only those listed here will get copied from the source
    with GeopackageLayer(output_db, layer_name='vbet_igos', delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbPoint, epsg=4326, fields=fields)

    riverscapes_api = RiverscapesAPI(stage=stage)
    # Only refresh the token if we need to
    if riverscapes_api.accessToken is None:
        riverscapes_api.refresh_token()

    queries = {
        'projects': riverscapes_api.load_query('searchProjects'),
        'files': riverscapes_api.load_query('projectFiles'),
        'datasets': riverscapes_api.load_query('projectDatasets')
    }
    # search_query = riverscapes_api.load_query('searchProjects')
    # project_files_query = riverscapes_api.load_query('projectFiles')
    # project_datasets_query = riverscapes_api.load_query('projectDatasets')

    for project_type in project_types:
        for huc in hucs:
            print('Processing HUC {} ({} of {})'.format(huc, hucs.index(huc), len(hucs)))
            latest_project = get_projects(riverscapes_api, queries['projects'], project_type, huc)
            if latest_project is None:
                continue

            project_id = latest_project['id']

            for local_path, download_file_meta in get_dataset_files(riverscapes_api, queries['files'], queries['datasets'], project_id, dataset_xml_id).items():
                actual_path = download_project_files(riverscapes_api, os.path.dirname(output_db), project_id, local_path, download_file_meta)

                process_vbet_gpkg(actual_path, output_db, project_id, local_path)

                project_dir = os.path.join(os.path.dirname(output_db), project_id)
                rmtree(project_dir)

            print('here')
            # project_type_search(local_folder, riverscapes_api, queries, project_type, huc)


def process_vbet_gpkg(input_gpkg, output_gpkg, project_id, local_path):

    with GeopackageLayer(input_gpkg, layer_name='vbet_igos') as input_lyr:
        with GeopackageLayer(output_db, layer_name='vbet_igos', write=True) as output_lyr:

            for in_feature, _counter, _progbar in input_lyr.iterate_features():
                field_values = {field: in_feature.GetField(field) for field in fields}
                field_values['project_id'] = project_id

                output_lyr.create_feature(in_feature.GetGeometryRef(), field_values)
  


 

def get_projects(riverscapes_api, projects_query, project_type: str, huc: str) -> Dict:
    """[summary]"""

    search_params = {
        'projectTypeId': project_type,
        'meta': [{
            'key': 'HUC',
            'value': huc
        }]
    }

    project_limit = 500
    project_offset = 0
    total = 0
    projects = {}
    while project_offset == 0 or project_offset < total:
        results = riverscapes_api.run_query(projects_query, {"searchParams": search_params, "limit": project_limit, "offset": project_offset})
        total = results['data']['searchProjects']['total']
        project_offset += project_limit

        projects.update({project['item']['id']: project['item'] for project in results['data']['searchProjects']['results']})

    if len(projects) == 0:
        return None
    elif len(projects) == 1:
        return projects[list(projects.keys())[0]]
    else:
        # Find the model with the greatest version number
        project_versions = {}
        for project_id, project_info in projects.items():
            for key, val in {meta_item['key']: meta_item['value'] for meta_item in project_info['meta']}.items():
                if key.replace(' ','').lower() == 'modelversion' and val is not None:
                    project_versions[semantic_version.Version(val)] = project_id
                    break
            
        project_versions_list = list(project_versions)
        project_versions_list.sort(reverse=True)
        return projects[project_versions[project_versions_list[0]]]


def get_dataset_files(riverscapes_api, files_query, datasets_query, project_id: str, dataset_xml_id: str) -> Dict:

    # Build a dictionary of files in the project keyed by local path to downloadUrl
    file_results = riverscapes_api.run_query(files_query, {"projectId": project_id}) 
    files = {file['localPath']: file for file in file_results['data']['project']['files']}
    
    dataset_limit = 500
    dataset_offset = 0
    dataset_total = 0

    # Query for datasets in the project. Then loop over the files within the dataset
    datasets = {}
    while dataset_offset == 0 or dataset_offset < dataset_total:

        dataset_results = riverscapes_api.run_query(datasets_query, {"projectId": project_id, "limit": dataset_limit, "offset": dataset_offset}) 

        project_datasets = dataset_results['data']['project']['datasets']
        dataset_total = project_datasets['total']
        dataset_offset += dataset_limit

        for dataset in project_datasets['items']:
            datasets[dataset['datasetXMLId']] = dataset['localPath']

    localPath = datasets.get(dataset_xml_id)
    if localPath is not None:
        return {localPath: files[localPath]}
    else:
        return None


def download_project_files(riverscapes_api, local_folder, project_id, local_path, download_file_meta) -> str:
    """[summary]"""

    download_path = os.path.join(local_folder, project_id, local_path)
    safe_makedirs(os.path.dirname(download_path))
    riverscapes_api.download_file(download_file_meta, download_path, True)
    return download_path


# def project_type_search(local_folder, riverscapes_api, queries, project_type: str, huc: str):
#     """[summary]"""

#     # Add items to this dictionary to filter which projects are retrieved.
#     # For example, to only get projects for a certain HUC, project type or model version etc.
#     search_params = {
#         'projectTypeId': project_type,
#         'meta': [{
#             'key': 'HUC',
#             'value': huc
#         }]
#     }

#     project_limit = 500
#     project_offset = 0
#     total = 0
#     while project_offset == 0 or project_offset < total:
#         results = riverscapes_api.run_query(queries['projects'], {"searchParams": search_params, "limit": project_limit, "offset": project_offset})
#         total = results['data']['searchProjects']['total']
#         project_offset += project_limit

#         projects = results['data']['searchProjects']['results']

#         for search_result in projects:

#             project = search_result['item']

#             # Build a dictionary of files in the project keyed by local path to downloadUrl
#             file_results = riverscapes_api.run_query(queries['files'], {"projectId": project['id']}) 
#             files = {file['localPath']: file['downloadUrl'] for file in file_results['data']['project']['files']}
            
#             dataset_limit = 500
#             dataset_offset = 0
#             dataset_total = 0

#             # Query for datasets in the project. Then loop over the files within the dataset
#             datasets = {}
#             while dataset_offset == 0 or dataset_offset < dataset_total:

#                 dataset_results = riverscapes_api.run_query(queries['datasets'], {"projectId": project['id'], "limit": dataset_limit, "offset": dataset_offset}) 

#                 project_datasets = dataset_results['data']['project']['datasets']
#                 dataset_total = project_datasets['total']
#                 dataset_offset += dataset_limit

#                 for dataset in project_datasets['items']:
#                     datasets[dataset['datasetXMLId']] = dataset['localPath']

#             print('here')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Riverscapes stage', type=str, default='production')
    parser.add_argument('project_types', help='Comma separated list of case insensitive project type machine codes', type=str)
    parser.add_argument('HUCs', help='Comma separated list of HUC codes', type=str)
    parser.add_argument('dataset_xml_id', help='Dataset XMLId to download', type=str)
    parser.add_argument('local_folder', help='Top level folder where to download files', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    project_types = args.project_types.split(',')
    hucs = args.HUCs.split(',')

    safe_makedirs(args.local_folder)
    output_db = os.path.join(args.local_folder, f'output_{datetime.datetime.now().strftime("%Y_%m_%d_%H_%M")}.sqlite')

    try:
        download_files(args.stage, project_types, hucs, args.dataset_xml_id, output_db)

    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
