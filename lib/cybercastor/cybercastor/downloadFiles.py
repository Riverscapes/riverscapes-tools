"""
Demo script to download files from Data Exchange
"""
import sys
import os
import json
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


def download_files(stage, project_types, hucs, dataset_xml_id, output_db, reset):
    """[summary]"""

    # Update the HUCs that are required
    hucs_to_process = update_all_hucs_status(output_db, hucs, reset)

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

    count = 0
    queued_hucs = True
    while queued_hucs is True:
        huc = get_next_huc(output_db)
        if huc is None:
            print(f'No HUCs left to process for project type VBET')
            queued_hucs = False
            break

        count += 1
        print(f'Processing HUC {huc} ({count} of {hucs_to_process})')
        latest_project = get_projects(riverscapes_api, queries['projects'], 'VBET', huc)
        if latest_project is None:
            update_huc_status(output_db, huc, 'no project')
            continue

        project_guid = latest_project['id']
        project_id = upsert_project(output_db, project_guid, 'VBET', huc, get_project_model_version(latest_project), 'queued')

        try:

            for local_path, download_file_meta in get_dataset_files(riverscapes_api, queries['files'], queries['datasets'], project_guid, dataset_xml_id).items():
                actual_path = download_project_files(riverscapes_api, os.path.dirname(output_db), project_guid, local_path, download_file_meta)

                process_vbet_gpkg(actual_path, output_db, project_id)

                project_dir = os.path.join(os.path.dirname(output_db), project_guid)
                rmtree(project_dir)

            update_project_status(output_db, project_id, 'complete')
        except Exception as ex:
            print('Error processing project')
            update_project_status(output_db, project_id, 'error')

    print('Processing complete')


def get_project_model_version(project_info: Dict) -> str:
    """Get the model version from the project metadata"""

    if 'meta' in project_info:
        for meta_item in project_info['meta']:
            if meta_item['key'].replace(' ', '').lower() == 'modelversion':
                return meta_item['value']

    return None


def update_huc_status(output_gpkg: str, huc: str, status: str) -> None:

    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()
        curs.execute('UPDATE hucs SET status = ? WHERE huc10 = ?', [status, huc])
        conn.commit()


def update_project_status(output_gpkg: str, project_id: str, status: str) -> int:
    """Update the status of the project"""

    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()
        curs.execute('UPDATE projects SET status = ? WHERE id = ?', [status, project_id])
        conn.commit()


def update_all_hucs_status(output_gpkg: str, hucs: list, reset: bool) -> None:
    """Set the status of each HUC to required (1) or not required (0)"""

    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()

        if reset is True:
            if len(hucs) < 1:
                curs.execute("UPDATE hucs SET status = 'queued'")
            else:
                curs.execute('UPDATE hucs SET status = NULL')
                curs.execute("UPDATE hucs SET status = 'queued' WHERE huc10 IN ({})".format(','.join(['?'] * len(hucs))), hucs)

            conn.commit()

        curs.execute("""SELECT count(*)
                    FROM hucs h
                        LEFT JOIN projects p ON h.huc10 = p.huc10
                    WHERE h.status = 'queued'
                        AND (p.status IS NULL OR p.status = 'error')""")
        to_process = curs.fetchone()[0]

    print(f'{to_process} HUCs to process')
    return to_process


def get_next_huc(output_gpkg: str) -> str:
    """Get the next HUC to process"""

    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()
        curs.execute("""SELECT h.huc10, p.id, p.guid, p.status
                    FROM hucs h
                        LEFT JOIN projects p ON h.huc10 = p.huc10
                    WHERE h.status = 'queued'
                        AND (p.status IS NULL OR p.status = 'error')
                    ORDER BY h.huc10 limit 1""")

        huc = curs.fetchone()

    if huc is None:
        return None
    else:
        return huc[0]


def upsert_project(output_gpkg: str, project_guid: str, project_type: str, huc: str, model_version: str, status: str) -> int:

    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()
        curs.execute("""INSERT INTO projects (guid, huc10, project_type, model_version, status)
                    VALUES (?, ?, ?, ?, ?) ON CONFLICT (guid) DO UPDATE SET status = ?""", [project_guid, huc, project_type, model_version, status, status])

        curs.execute('SELECT id FROM projects WHERE guid = ?', [project_guid])
        project_id = curs.fetchone()[0]
        conn.commit()

    return project_id


def process_vbet_gpkg(input_gpkg, output_gpkg, project_id: int):
    """Note project ID is the SQLite integer ID, not the GUID string"""

    with GeopackageLayer(input_gpkg, layer_name='vbet_igos') as input_lyr:
        with GeopackageLayer(output_gpkg, layer_name='vbet_igos', write=True) as output_lyr:

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
                if key.replace(' ', '').lower() == 'modelversion' and val is not None:
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


def build_output(output_gpkg: str, huc10_attributes: str, reset: bool) -> None:
    """Build the output GeoPackage and feature class fields
    or optionally purge the existing output and start from scratch"""

    if os.path.isfile(output_gpkg):
        if reset is True:
            with sqlite3.connect(output_gpkg) as conn:
                curs = conn.cursor()
                curs.execute('DELETE FROM vbet_igos')
                conn.commit()
        return

    # Create the output Geopackage and feature class fields and include the project_id
    vbet_igo_fields = {'project_id': ogr.OFTInteger}
    vbet_igo_fields.update(fields)
    with GeopackageLayer(output_gpkg, layer_name='vbet_igos', delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbPoint, epsg=4326, fields=vbet_igo_fields)

    out_lyr = None

    # Create lookup table
    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()
        curs.execute('CREATE TABLE IF NOT EXISTS hucs (huc10 TEXT PRIMARY KEY, name TEXT, states TEXT, areasqkm REAL, status TEXT)')

        # Load HUC 10 attributes
        huc_data = json.load(open(huc10_attributes, encoding='utf-8'))
        curs.executemany('INSERT INTO hucs (huc10, name, states, areasqkm) VALUES (?, ?, ?, ?)', [(huc['HUC10'], huc['NAME'], huc['STATES'], huc['AREASQKM']) for huc in huc_data])

        # Index the HUCS in one go
        curs.execute('CREATE UNIQUE INDEX IF NOT EXISTS hucs_huc10_idx ON hucs (huc10)')

        # Create table to track projects
        curs.execute('CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, guid TEXT NOT NULL, huc10 TEXT, project_type TEXT, model_version TEXT, status TEXT, metadata TEXT)')
        curs.execute('CREATE INDEX IF NOT EXISTS projects_huc10_idx ON projects (huc10)')
        curs.execute('CREATE UNIQUE INDEX IF NOT EXISTS projects_guid_idx ON projects (guid)')

        # Index the project ID in the vbet_igos table
        curs.execute('CREATE INDEX IF NOT EXISTS vbet_igos_project_id_idx ON vbet_igos (project_id)')

        # Create a summary view of all the VBET metrics
        field_list = []
        for process in ['min', 'max', 'avg']:
            for field, data_type in fields.items():
                if data_type != ogr.OFTReal:
                    continue
                field_list.append(f'{process}({field}) AS {process}_{field}')

        view_name = 'vw_huc_summary_stats'
        huc_polygons = 'Huc10_conus'
        curs.execute(f"""CREATE VIEW {view_name} AS
                     SELECT
                        h.fid,
                        h.geom,
                        {', '.join(field_list)}
                    FROM {huc_polygons} h
                        INNER JOIN projects p on h.HUC10 = p.huc10
                        INNER JOIN vbet_igos vi on p.id = vi.project_id
                    GROUP BY h.fid, h.geom""")

        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
                     SELECT ?, features, ?, min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = ?""", [view_name, view_name, huc_polygons])

        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
                     SELECT ?, 'geom', geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = ?""", [view_name, huc_polygons])

        conn.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Riverscapes stage', type=str, default='production')
    parser.add_argument('project_types', help='Comma separated list of case insensitive project type machine codes', type=str)
    parser.add_argument('HUCs', help='Comma separated list of HUC codes', type=str)
    parser.add_argument('dataset_xml_id', help='Dataset XMLId to download', type=str)
    parser.add_argument('local_folder', help='Top level folder where to download files', type=str)
    parser.add_argument('output_gpkg', help='Path where to existing output Geopackage or where it will be created', type=str)
    parser.add_argument('huc10_attributes', help='Name of JSON file in local_folder that contains HUC 10 definitions', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--reset', help='(optional) delete all existing outputs before running and run from scratch', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    project_types = args.project_types.split(',')
    hucs = list() if args.HUCs == '.' else args.HUCs.split(',')

    safe_makedirs(args.local_folder)
    # output_db = os.path.join(args.local_folder, f'output_{datetime.datetime.now().strftime("%Y_%m_%d_%H_%M")}.sqlite')

    # try:
    build_output(args.output_gpkg, args.huc10_attributes, args.reset)
    download_files(args.stage, project_types, hucs, args.dataset_xml_id, args.output_gpkg, args.reset)

    # except Exception as e:
    #     traceback.print_exc(file=sys.stdout)
    #     sys.exit(1)

    sys.exit(0)
