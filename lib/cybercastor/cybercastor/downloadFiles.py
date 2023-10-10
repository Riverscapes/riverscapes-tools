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
from osgeo import ogr, osr
from typing import Dict
from rscommons import Logger, dotenv, GeopackageLayer, ShapefileLayer
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI
from rscommons.util import safe_makedirs

igo_fields = {
    'HUC10': ogr.OFTString,
    'LevelPathI': ogr.OFTString,
    'seg_distance': ogr.OFTReal,
}

dgo_fields = {
    'HUC10': ogr.OFTString,
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

huc_fields = {
    'huc10': ogr.OFTString,
    'name': ogr.OFTString,
    'states': ogr.OFTString,
    'areasqkm': ogr.OFTReal,
    'status': ogr.OFTString
}

lookups = {
    'us_states': {
        'in_field_name': 'STATE_ABBR',
        'out_field_name': 'us_state'
    },
    'ownership': {
        'in_field_name': 'ADMIN_AGEN',
        'out_field_name': 'ownership',
    },
    'ecoregions': {
        'in_field_name': 'L4_KEY',
        'out_field_name': 'ecoregion_iv'
    }
}


def download_files(stage, hucs, us_states: str,
                   ownership: str,
                   ecoregions: str,
                   output_db, reset):
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

    # Insert the path to each lookup shapefile
    lookups['us_states']['shapefile'] = us_states
    lookups['ownership']['shapefile'] = ownership
    lookups['ecoregions']['shapefile'] = ecoregions

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
            # Cleanup by removing the entire project folder
            project_dir = os.path.join(os.path.dirname(output_db), project_guid)
            rmtree(project_dir)

            # Get the inputs, intermediates and output VBET GeoPackages
            vbet_input_gpkg = download_gpkg(riverscapes_api, queries, os.path.dirname(output_db), project_guid, 'INPUTS')
            vbet_inter_gpkg = download_gpkg(riverscapes_api, queries, os.path.dirname(output_db), project_guid, 'Intermediates')
            vbet_outpt_gpkg = download_gpkg(riverscapes_api, queries, os.path.dirname(output_db), project_guid, 'VBET_OUTPUTS')

            process_vbet_igos(vbet_outpt_gpkg, output_db, project_id)
            process_vbet_dgos(vbet_inter_gpkg, output_db)

            for __layer_name, layer_info in lookups.items():
                process_polygon_layer_attribute(layer_info['shapefile'], layer_info['in_field_name'], vbet_inter_gpkg, output_db)

            process_fcodes(vbet_input_gpkg, vbet_inter_gpkg, output_db)

            # Cleanup by removing the entire project folder
            project_dir = os.path.join(os.path.dirname(output_db), project_guid)
            rmtree(project_dir)

            update_project_status(output_db, project_id, 'complete')
        except Exception as ex:
            print('Error processing project')
            update_project_status(output_db, project_id, 'error')

    print('Processing complete')


def download_gpkg(riverscapes_api: str, queries: dict, top_level_dir: str, project_guid: str, dataset_xml_id: str) -> str:

    # Get the local path for the file and the URL from which to download it
    local_path, download_file_meta = get_dataset_files(riverscapes_api, queries['files'], queries['datasets'], project_guid, dataset_xml_id)

    # Download the file to the local path
    actual_path = download_project_files(riverscapes_api, top_level_dir, project_guid, local_path, download_file_meta)

    return actual_path


def process_polygon_layer_attribute(polygon_layer: str, field_name: str, vbet_inter_gpkg: str, output_db: str) -> str:
    """ Determine the value of the field_name attribute for the polygon_layer that intersects the dgo_polygon
    with the largest area. Use this for determining the US state, ownership or ecoregion of the dgo_polygon"""

    # DGO polygon layer
    dgo_dataset = ogr.Open(vbet_inter_gpkg)
    dgo_layer = dgo_dataset.GetLayerByName('vbet_dgos')
    dgo_srs = dgo_layer.GetSpatialRef()

    # Polygon layer
    poly_dataset = ogr.Open(polygon_layer)
    poly_layer = poly_dataset.GetLayerByIndex(0)
    poly_srs = poly_layer.GetSpatialRef()

    transform = osr.CoordinateTransformation(dgo_srs, poly_srs)

    # Loop over the DGOs, transform the polygon and find the largest intersection
    dgo_results = {}
    for dgo_feature in dgo_layer:
        clone_dgo = dgo_feature.Clone()
        clone_dgo.Transform(transform)
        dgo_huc = clone_dgo.GetField('HUC10')
        dgo_level_path = clone_dgo.GetField('LevelPathI')
        dgo_seg_distance = clone_dgo.GetField('seg_distance')

        # Use a spatial attribute filter to get just the polygons that intersect the DGO
        intersection_areas = {}
        poly_layer.SetSpatialFilter(clone_dgo)
        for poly_feature in poly_layer:
            geom = poly_feature.GetGeometryRef()
            output_value = poly_feature.GetField(field_name)

            intersection_result = geom.Intersection(clone_dgo)
            if intersection_result is not None:
                if output_value in intersection_areas:
                    intersection_areas[output_value] += intersection_result.Area()
                else:
                    intersection_areas[output_value] = intersection_result.Area()

        if len(intersection_areas) > 0:
            max_area = max(intersection_areas, key=lambda k: intersection_areas[k])
            dgo_results.append((dgo_huc, dgo_level_path, dgo_seg_distance, max_area))

    # Update the DGO table with the results
    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        curs.executemany(f'UPDATE vbet_dgos SET {field_name} = ? WHERE HUC10 = ? AND LevelPathI = ? AND seg_distance = ?', dgo_results)
        conn.commit()


def process_fcodes(vbet_input_gpkg: str, vbet_inter_gpkg: str, output_gpkg: str) -> None:

    # DGO polygon layer
    dgo_dataset = ogr.Open(vbet_inter_gpkg)
    dgo_layer = dgo_dataset.GetLayerByName('vbet_dgos')
    dgo_srs = dgo_layer.GetSpatialRef()

    # NHD flowline layer
    poly_dataset = ogr.Open(vbet_input_gpkg)
    poly_layer = poly_dataset.GetLayerByIndex(0)
    poly_srs = poly_layer.GetSpatialRef()

    transform = osr.CoordinateTransformation(dgo_srs, poly_srs)

    # Loop over the DGOs, transform the polygon and find the largest intersection
    dgo_results = {}
    for dgo_feature in dgo_layer:
        clone_dgo = dgo_feature.Clone()
        clone_dgo.Transform(transform)
        dgo_huc = clone_dgo.GetField('HUC10')
        dgo_level_path = clone_dgo.GetField('LevelPathI')
        dgo_seg_distance = clone_dgo.GetField('seg_distance')

        # Use a spatial attribute filter to get just the polygons that intersect the DGO
        intersection_areas = {}
        poly_layer.SetSpatialFilter(clone_dgo)
        for poly_feature in poly_layer:
            geom = poly_feature.GetGeometryRef()
            output_value = poly_feature.GetField('FCode')

            intersection_result = geom.Intersection(clone_dgo)
            if intersection_result is not None:
                if output_value in intersection_areas:
                    intersection_areas[output_value] += intersection_result.Length()
                else:
                    intersection_areas[output_value] = intersection_result.Length()

        if len(intersection_areas) > 0:
            max_length = max(intersection_areas, key=lambda k: intersection_areas[k])
            dgo_results.append((dgo_huc, dgo_level_path, dgo_seg_distance, max_length))

    # Update the DGO table with the results
    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()
        curs.executemany('UPDATE vbet_dgos SET FCode = ? WHERE HUC10 = ? AND LevelPathI = ? AND seg_distance = ?', dgo_results)
        conn.commit()


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


def process_vbet_igos(vbet_output_gpkg, scrape_output_gpkg, project_id: int) -> None:
    """Note project ID is the SQLite integer ID, not the GUID string"""

    with GeopackageLayer(vbet_output_gpkg, layer_name='vbet_igos') as input_lyr:
        with GeopackageLayer(scrape_output_gpkg, layer_name='vbet_igos', write=True) as output_lyr:

            for in_feature, _counter, _progbar in input_lyr.iterate_features():
                field_values = {field: in_feature.GetField(field) for field in dgo_fields}
                field_values['project_id'] = project_id

                output_lyr.create_feature(in_feature.GetGeometryRef(), field_values)


def process_vbet_dgos(vbet_intermediate_gpkg: str, scrape_output_gpkg: str) -> None:

    with sqlite3.connect(scrape_output_gpkg) as conn:
        curs = conn.cursor()
        with GeopackageLayer(vbet_intermediate_gpkg, layer_name='vbet_dgos') as input_lyr:
            for in_feature, _counter, _progbar in input_lyr.iterate_features():

                field_key_values = {field: in_feature.GetField(field) for field in dgo_fields}
                field_names = list(field_key_values.keys())
                curs.execute(f'INSERT INTO vbet_dgos ({", ".join(field_names)}) VALUES ({", ".join(["?"] * len(field_names))})', [field_key_values[field] for field in field_names])

        conn.commit()


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


def get_dataset_files(riverscapes_api, files_query, datasets_query, project_id: str, dataset_xml_id: str) -> tuple:

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
        return (localPath, files[localPath])
    else:
        return None


def get_fcodes(vbet_input_gpkg: str) -> Dict:
    """Get the FCode that has the longest total length for each LevelPathI"""

    fcodes = {}
    conn = sqlite3.connect(vbet_input_gpkg)
    curs = conn.cursor()
    curs.execute("""select fl.FCode, vaa.LevelPathI, sum(fl.LengthKM) length
        from flowlines fl
            inner join NHDPlusFlowlineVAA vaa on fl.NHDPlusID = vaa.NHDPlusID
        group by fl.FCode, vaa.LevelPathI
        order by length desc""")
    conn.row_factory = sqlite3.Row

    for row in curs.fetchall():
        if row['LevelPathI'] not in fcodes:
            fcodes[row['LevelPathI']] = row['FCode']

    return fcodes


def download_project_files(riverscapes_api, local_folder, project_id, local_path, download_file_meta) -> str:
    """[summary]"""

    download_path = os.path.join(local_folder, project_id, local_path)
    safe_makedirs(os.path.dirname(download_path))
    riverscapes_api.download_file(download_file_meta, download_path, True)
    return download_path


def build_output(output_gpkg: str, huc10_path: str, hucs: list, huc_attributes: str, reset: bool) -> None:
    """Build the output GeoPackage and feature class fields
    or optionally purge the existing output and start from scratch"""

    if os.path.isfile(output_gpkg):
        if reset is True:
            with sqlite3.connect(output_gpkg) as conn:
                curs = conn.cursor()
                curs.execute('DELETE FROM vbet_igos')
                curs.execute('DELETE FROM vbet_dgos')
                conn.commit()
        return

    # Create the output Geopackage and feature class fields and include the project_id
    vbet_igo_fields = {'project_id': ogr.OFTInteger, 'HUC10': ogr.OFTString, 'LevelPathI': ogr.OFTString, 'seg_distance': ogr.OFTReal}
    with GeopackageLayer(output_gpkg, layer_name='vbet_igos', delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbPoint, epsg=4326, fields=vbet_igo_fields)

    with GeopackageLayer(output_gpkg, layer_name='hucs', delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPolygon, epsg=4326, fields=huc_fields)

    # Load the HUC polygons
    with ShapefileLayer(huc10_path) as huc_lyr:
        with GeopackageLayer(output_gpkg, layer_name='hucs', write=True) as out_lyr:
            for in_feature, _counter, _progbar in huc_lyr.iterate_features():
                out_lyr.create_feature(in_feature.GetGeometryRef(), {field: in_feature.GetField(field) for field in ['huc10']})

    out_lyr = None

    # Create database tables
    with sqlite3.connect(output_gpkg) as conn:

        # curs.execute('SELECT load_extension("mod_spatialite")')
        # curs.execute('CREATE TABLE IF NOT EXISTS hucs (huc10 TEXT PRIMARY KEY, name TEXT, states TEXT, areasqkm REAL, status TEXT)')

        # DGO metrics table. No geometry, just the identifiers and metrics
        field_list = []
        for name, ogr_type in dgo_fields.items():
            sql_type = 'TEXT' if ogr_type == ogr.OFTString else 'REAL'
            field_list.append(f'{name} {sql_type}')

        conn.execute(f'CREATE TABLE vbet_dgos ({",".join(field_list)})')
        conn.execute('ALTER TABLE vbet_dgos ADD COLUMN project_id INTEGER')
        conn.execute('CREATE UNIQUE INDEX pk_vbet_dgos ON vbet_dgos (HUC10, LevelPathI, seg_distance)')
        conn.execute('CREATE INDEX vbet_dgos_project_id_idx ON vbet_dgos (project_id)')

        # Add the polygon lookup fields (US state, ecoregion, ownership) to the DGO table
        for __lookup_name, lookup_info in lookups.items():
            conn.execute(f'ALTER TABLE vbet_dgos ADD COLUMN {lookup_info["out_field_name"]} TEXT')

        # Incorporate the HUC attribute columns
        igo_field_list = []
        for field_name, d_type in igo_fields.items():
            field_type = 'TEXT' if d_type == ogr.OFTString else 'REAL'
            igo_field_list.append(f'{field_name} {field_type}')
        conn.execute(f'CREATE TABLE vbet_igos ({",".join(igo_field_list)})')
        conn.execute('ALTER TABLE vbet_igos ADD COLUMN project_id INTEGER')
        conn.execute('CREATE INDEX vbet_igos_project_id_idx ON vbet_igos (project_id)')

        # Create table to track projects
        conn.execute('CREATE TABLE projects (id INTEGER PRIMARY KEY, guid TEXT NOT NULL, huc10 TEXT, project_type TEXT, model_version TEXT, status TEXT, metadata TEXT)')
        conn.execute('CREATE INDEX projects_huc10_idx ON projects (huc10)')
        conn.execute('CREATE UNIQUE INDEX projects_guid_idx ON projects (guid)')

        # Triggers need to be off for the updates to feature class
        triggers = drop_triggers(conn)

        # Enrich the HUCs with attributes from the JSON file
        with open(huc_attributes) as fhuc:
            huc_json = json.load(fhuc)
            for huc in huc_json:
                conn.execute('UPDATE hucs SET name = ?, states = ?, areasqkm = ? WHERE huc10 = ?', [huc['NAME'], huc['STATES'], huc['AREASQKM'], huc['HUC10']])

        # Update which HUCs are required
        if len(hucs) < 1:
            conn.execute("UPDATE hucs SET status = 'queued'")
        else:
            conn.execute('UPDATE hucs SET status = NULL')
            conn.execute("UPDATE hucs SET status = 'queued' WHERE huc10 IN ({})".format(','.join(['?'] * len(hucs))), hucs)

        add_triggers(conn, triggers)
        conn.commit()


def drop_triggers(conn: sqlite3.Connection) -> None:
    """Drop all triggers from the database"""

    curs = conn.cursor()
    curs.execute("SELECT type, name, tbl_name, rootpage, sql FROM sqlite_master WHERE type='trigger'")
    triggers = curs.fetchall()
    for trigger in triggers:
        curs.execute(f'DROP TRIGGER {trigger[1]}')

    return triggers


def add_triggers(conn: sqlite3.Connection, triggers: list) -> None:
    """Add triggers to the database"""

    curs = conn.cursor()
    for trigger in triggers:
        curs.execute(trigger[4])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('environment', help='Riverscapes stage', type=str, default='production')
    parser.add_argument('HUCs', help='Comma separated list of HUC codes', type=str)
    parser.add_argument('huc_attributes', help='JSON of HUC attributes', type=str)
    parser.add_argument('local_folder', help='Top level folder where to download files', type=str)
    parser.add_argument('output_gpkg', help='Path where to existing output Geopackage or where it will be created', type=str)
    parser.add_argument('huc10', help='ShapeFile of HUC10 geometries', type=str)
    parser.add_argument('states', help='ShapeFile US State geometries', type=str)
    parser.add_argument('ownership', help='ShapeFile of ownership', type=str)
    parser.add_argument('ecoregions', help='ShapeFile of ecoregions', type=str)

    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--reset', help='(optional) delete all existing outputs before running and run from scratch', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    hucs = list() if args.HUCs == '.' else args.HUCs.split(',')

    safe_makedirs(args.local_folder)

    build_output(args.output_gpkg, args.huc10, hucs, args.huc_attributes, args.reset)
    download_files(args.environment, hucs,
                   args.states,
                   args.ownership,
                   args.ecoregions,
                   args.output_gpkg, args.reset)

    sys.exit(0)
