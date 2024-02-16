"""
Demo script to download files from Data Exchange


NOTE: THIS SCRIPT IS A MESS AND WILL NEED TO BE REWORKED A BIT

"""
import sys
import os
import json
import argparse
import sqlite3
from typing import Dict
from shutil import rmtree
import semver
from termcolor import colored
from dateutil.parser import parse as dateparse
from osgeo import ogr, osr
from rsxml import safe_makedirs, dotenv, Logger
from rsxml.project_xml import GeopackageLayer
from rscommons import ShapefileLayer
from riverscapes import RiverscapesAPI

igo_fields = {
    'HUC10': ogr.OFTString,
    'level_path': ogr.OFTReal,
    'seg_distance': ogr.OFTReal,
    'stream_size': ogr.OFTReal
}

dgo_fields = {
    'HUC10': ogr.OFTString,
    'level_path': ogr.OFTReal,
    'seg_distance': ogr.OFTReal,
    'rme_igo_prim_channel_gradient': ogr.OFTReal,
    'rme_igo_valleybottom_gradient': ogr.OFTReal,
    'nhd_dgo_streamorder': ogr.OFTReal,
    'nhd_dgo_headwater': ogr.OFTReal,
    'nhd_dgo_streamtype': ogr.OFTReal,
    'vbet_dgo_lowlying_area': ogr.OFTReal,
    'vbet_dgo_elevated_area': ogr.OFTReal,
    'vbet_dgo_channel_area': ogr.OFTReal,
    'vbet_dgo_floodplain_area': ogr.OFTReal,
    'vbet_igo_integrated_width': ogr.OFTReal,
    'vbet_igo_active_channel_ratio': ogr.OFTReal,
    'vbet_igo_low_lying_ratio': ogr.OFTReal,
    'vbet_igo_elevated_ratio': ogr.OFTReal,
    'vbet_igo_floodplain_ratio': ogr.OFTReal,
    'vbet_igo_acres_vb_per_mile': ogr.OFTReal,
    'vbet_igo_hect_vb_per_km': ogr.OFTReal,
    'rme_igo_rel_flow_length': ogr.OFTReal,
    'vbet_dgo_streamsize': ogr.OFTReal,
    'epa_dgo_ecoregion3': ogr.OFTReal,
    'rme_dgo_confluences': ogr.OFTReal,
    'rme_dgo_diffluences': ogr.OFTReal,
    'rme_igo_planform_sinuosity': ogr.OFTReal,
    'rme_dgo_drainage_area': ogr.OFTReal,
    'epa_dgo_ecoregion4': ogr.OFTReal,
    'conf_igo_confinement_ratio': ogr.OFTReal,
    'conf_igo_constriction_ratio': ogr.OFTReal,
    'conf_dgo_confining_margins': ogr.OFTReal,
    'rme_igo_trib_per_km': ogr.OFTReal,
    'anthro_igo_road_dens': ogr.OFTReal,
    'anthro_igo_rail_dens': ogr.OFTReal,
    'anthro_igo_land_use_intens': ogr.OFTReal,
    'rcat_igo_fldpln_access': ogr.OFTReal,
    'rme_dgo_ownership': ogr.OFTReal,
    'rme_dgo_state': ogr.OFTReal,
    'rme_dgo_county': ogr.OFTReal
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


def scrape_projects(stage, hucs, output_db, reset):
    """[summary]"""
    log = Logger('ScrapeProjects')

    # Update the HUCs that are required
    hucs_to_process = update_all_hucs_status(output_db, hucs, reset)

    riverscapes_api = RiverscapesAPI(stage=stage)
    riverscapes_api.refresh_token()

    ds_qry = riverscapes_api.load_query('projectDatasets')

    count = 0
    queued_hucs = True
    while queued_hucs is True:
        huc = get_next_huc(output_db)
        if huc is None:
            print('No HUCs left to process for project type VBET')
            queued_hucs = False
            break

        count += 1
        print(f'Processing HUC {huc} ({count} of {hucs_to_process})')
        latest_project = get_projects(riverscapes_api, queries['projects'], 'rs_metric_engine', huc)
        if latest_project is None:
            update_huc_status(output_db, huc, 'no project')
            continue

        project_guid = latest_project['id']
        project_id = upsert_project(
            output_db, project_guid, 'RME', huc, get_project_model_version(latest_project), 'queued')

        try:
            # Cleanup by removing the entire project folder
            project_dir = os.path.join(
                os.path.dirname(output_db), project_guid)
            if os.path.isdir(project_dir):
                rmtree(project_dir)

            # Get the inputs, intermediates and output VBET GeoPackages
            rme_output_gpkg = download_gpkg(riverscapes_api, queries, os.path.dirname(
                output_db), project_guid, 'RME_OUTPUTS')

            process_igos(rme_output_gpkg, output_db, project_id, huc)
            process_dgos(rme_output_gpkg, output_db, project_id, huc)

            # for __layer_name, layer_info in lookups.items():
            #     process_polygon_layer_attribute(
            #         layer_info['shapefile'], layer_info['in_field_name'], layer_info['out_field_name'], vbet_inter_gpkg, output_db, huc)

            # process_fcodes(vbet_input_gpkg, vbet_inter_gpkg, output_db, huc)

            # Cleanup by removing the entire project folder
            project_dir = os.path.join(
                os.path.dirname(output_db), project_guid)
            rmtree(project_dir)

            update_project_status(output_db, project_id, 'complete')
        except Exception as ex:
            log.error('Error processing project')
            log.error(ex)
            update_project_status(output_db, project_id, 'error')

    print('Processing complete')


def download_gpkg(riverscapes_api: str, queries: dict, top_level_dir: str, project_guid: str, dataset_xml_id: str) -> str:

    # Get the local path for the file and the URL from which to download it
    local_path, download_file_meta = get_dataset_files(
        riverscapes_api, queries['files'], queries['datasets'], project_guid, dataset_xml_id)

    # Download the file to the local path
    actual_path = download_project_files(
        riverscapes_api, top_level_dir, project_guid, local_path, download_file_meta)

    if os.path.isfile(actual_path) is False:
        raise Exception(
            f'Unable to download file {local_path} from {download_file_meta["downloadUrl"]}')

    return actual_path


# def process_polygon_layer_attribute(polygon_layer: str, in_field_name: str, out_field_name: str, vbet_inter_gpkg: str, output_db: str, huc: str) -> str:
#     """ Determine the value of the field_name attribute for the polygon_layer that intersects the dgo_polygon
#     with the largest area. Use this for determining the US state, ownership or ecoregion of the dgo_polygon"""

#     # DGO polygon layer
#     dgo_dataset = ogr.Open(vbet_inter_gpkg)
#     dgo_layer = dgo_dataset.GetLayerByName('dgos')
#     dgo_srs = dgo_layer.GetSpatialRef()

#     # Polygon layer
#     poly_dataset = ogr.Open(polygon_layer)
#     poly_layer = poly_dataset.GetLayerByIndex(0)
#     poly_srs = poly_layer.GetSpatialRef()

#     transform = osr.CoordinateTransformation(dgo_srs, poly_srs)

#     # Loop over the DGOs, transform the polygon and find the largest intersection
#     dgo_results = []
#     for dgo_feature in dgo_layer:
#         clone_dgo = dgo_feature.GetGeometryRef().Clone()
#         clone_dgo.Transform(transform)
#         dgo_level_path = dgo_feature.GetField('LevelPathI')
#         dgo_seg_distance = dgo_feature.GetField('seg_distance')

#         # Use a spatial attribute filter to get just the polygons that intersect the DGO
#         intersection_areas = {}
#         poly_layer.SetSpatialFilter(clone_dgo)
#         for poly_feature in poly_layer:
#             geom = poly_feature.GetGeometryRef()
#             output_value = poly_feature.GetField(in_field_name)

#             intersection_result = geom.Intersection(clone_dgo)
#             if intersection_result is not None:
#                 if output_value in intersection_areas:
#                     intersection_areas[output_value] += intersection_result.Area()
#                 else:
#                     intersection_areas[output_value] = intersection_result.Area(
#                     )

#         if len(intersection_areas) > 0:
#             max_area = max(intersection_areas,
#                            key=lambda k: intersection_areas[k])
#             dgo_results.append(
#                 (max_area, huc, dgo_level_path, dgo_seg_distance))

#     # Update the DGO table with the results
#     if len(dgo_results) > 0:
#         with sqlite3.connect(output_db) as conn:
#             curs = conn.cursor()
#             curs.executemany(
#                 f'UPDATE vbet_dgos SET {out_field_name} = ? WHERE HUC10 = ? AND LevelPathI = ? AND seg_distance = ?', dgo_results)
#             conn.commit()


# def process_fcodes(vbet_input_gpkg: str, vbet_inter_gpkg: str, output_gpkg: str, huc: str) -> None:

#     # DGO polygon layer
#     dgo_dataset = ogr.Open(vbet_inter_gpkg)
#     dgo_layer = dgo_dataset.GetLayerByName('vbet_dgos')
#     dgo_srs = dgo_layer.GetSpatialRef()

#     # NHD flowline layer
#     poly_dataset = ogr.Open(vbet_input_gpkg)
#     poly_layer = poly_dataset.GetLayerByIndex(0)
#     poly_srs = poly_layer.GetSpatialRef()

#     transform = osr.CoordinateTransformation(dgo_srs, poly_srs)

#     # Loop over the DGOs, transform the polygon and find the largest intersection
#     dgo_results = []
#     for dgo_feature in dgo_layer:
#         clone_dgo = dgo_feature.GetGeometryRef().Clone()
#         clone_dgo.Transform(transform)
#         dgo_level_path = dgo_feature.GetField('LevelPathI')
#         dgo_seg_distance = dgo_feature.GetField('seg_distance')

#         # Use a spatial attribute filter to get just the polygons that intersect the DGO
#         intersection_areas = {}
#         poly_layer.SetSpatialFilter(clone_dgo)
#         for poly_feature in poly_layer:
#             geom = poly_feature.GetGeometryRef()
#             output_value = poly_feature.GetField('FCode')

#             intersection_result = geom.Intersection(clone_dgo)
#             if intersection_result is not None:
#                 if output_value in intersection_areas:
#                     intersection_areas[output_value] += intersection_result.Length()
#                 else:
#                     intersection_areas[output_value] = intersection_result.Length(
#                     )

#         if len(intersection_areas) > 0:
#             max_length = max(intersection_areas,
#                              key=lambda k: intersection_areas[k])
#             dgo_results.append(
#                 (max_length, huc, dgo_level_path, dgo_seg_distance))

#     # Update the DGO table with the results
#     if len(dgo_results) > 0:
#         with sqlite3.connect(output_gpkg) as conn:
#             curs = conn.cursor()
#             curs.executemany(
#                 'UPDATE vbet_dgos SET FCode = ? WHERE HUC10 = ? AND LevelPathI = ? AND seg_distance = ?', dgo_results)
#             conn.commit()


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
        curs.execute(
            'UPDATE hucs SET status = ? WHERE huc10 = ?', [status, huc])
        conn.commit()


def update_project_status(output_gpkg: str, project_id: str, status: str) -> int:
    """Update the status of the project"""

    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()
        curs.execute('UPDATE projects SET status = ? WHERE id = ?', [
                     status, project_id])
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
                curs.execute("UPDATE hucs SET status = 'queued' WHERE huc10 IN ({})".format(
                    ','.join(['?'] * len(hucs))), hucs)

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


def process_igos(vbet_output_gpkg, scrape_output_gpkg, project_id: int, huc10: str) -> None:
    """Note project ID is the SQLite integer ID, not the GUID string"""

    with GeopackageLayer(vbet_output_gpkg, layer_name='vw_igo_metrics') as input_lyr:
        with GeopackageLayer(scrape_output_gpkg, layer_name='igos', write=True) as output_lyr:

            for in_feature, _counter, _progbar in input_lyr.iterate_features():
                field_values = {field: in_feature.GetField(
                    field) for field in igo_fields if field != 'HUC10'}
                field_values['project_id'] = project_id
                field_values['HUC10'] = huc10
                output_lyr.create_feature(
                    in_feature.GetGeometryRef(), field_values)


def process_dgos(vbet_intermediate_gpkg: str, scrape_output_gpkg: str, project_id: int, huc10: str) -> None:
    """_summary_

    Args:
        vbet_intermediate_gpkg (str): _description_
        scrape_output_gpkg (str): _description_
        project_id (int): _description_
        huc10 (str): _description_
    """

    with sqlite3.connect(scrape_output_gpkg) as conn:
        curs = conn.cursor()
        with GeopackageLayer(vbet_intermediate_gpkg, layer_name='vw_dgo_metrics') as input_lyr:
            for in_feature, _counter, _progbar in input_lyr.iterate_features():

                field_key_values = {field: in_feature.GetField(
                    field) for field in dgo_fields if field != 'HUC10'}
                field_key_values['HUC10'] = huc10
                field_key_values['project_id'] = project_id

                if field_key_values['seg_distance'] is None:
                    continue

                field_names = list(field_key_values.keys())
                curs.execute(f'INSERT INTO dgos ({", ".join(field_names)}) VALUES ({", ".join(["?"] * len(field_names))})', [
                             field_key_values[field] for field in field_names])

        conn.commit()


def get_model_version(project: RiverscapesProject):
    for meta_item in project['meta']:
        if meta_item['key'].replace(' ', '').lower() == 'modelversion':
            return meta_item['value']
    return '0.0.0'


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
        results = riverscapes_api.run_query(projects_query, {
                                            "searchParams": search_params, "limit": project_limit, "offset": project_offset})
        total = results['data']['searchProjects']['total']
        project_offset += project_limit

        projects.update({project['item']['id']: project['item']
                        for project in results['data']['searchProjects']['results']})

    if len(projects) == 0:
        return None
    elif len(projects) == 1:
        return projects[list(projects.keys())[0]]
    else:
        # Find the newest model (defined by the highest modelVersion and then the newest createdOn date)
        # {
        #   "createdOn": "2024-02-08T17:04:22.920Z",
        #   "id": "f45301d2-b0b5-4a2f-97b7-477776d9bfe5",
        #   "meta": [
        #     {
        #       "key": "ModelVersion",
        #       "value": "1.1.1"
        #     }
        #   ],
        #   "name": "Channel Area for HUC 17060304",
        # }
        newest_project = None
        newest_project_version = None
        print(colored(f"    Found {len(projects.keys())} possible projects for HUC: {huc}. Attempting to find newest:", 'yellow'))
        for project in projects.values():
            project_version = get_model_version(project)
            print(colored(f"       Project: '{project['id']}' Model Version: '{project_version}' Created On: '{project['createdOn']}", 'cyan'))
            if newest_project is None:
                newest_project = project
                newest_project_version = project_version
            else:
                if semver.compare(newest_project_version, project_version) < 0:
                    newest_project = project
                    newest_project_version = project_version
                elif dateparse(newest_project['createdOn']) < dateparse(project['createdOn']):
                    newest_project = project
                    newest_project_version = project_version

        print(colored(f"    Choosing: '{newest_project['id']}' as newest project for HUC: '{huc}'' Model Version: '{newest_project_version}' Created On: '{newest_project['createdOn']}", 'yellow'))
        return newest_project


def get_dataset_files(riverscapes_api, files_query, datasets_query, project_id: str, dataset_xml_id: str) -> tuple:

    # Build a dictionary of files in the project keyed by local path to downloadUrl
    file_results = riverscapes_api.run_query(
        files_query, {"projectId": project_id})
    files = {file['localPath']: file for file in file_results['data']['project']['files']}

    dataset_limit = 500
    dataset_offset = 0
    dataset_total = 0

    # Query for datasets in the project. Then loop over the files within the dataset
    datasets = {}
    while dataset_offset == 0 or dataset_offset < dataset_total:

        dataset_results = riverscapes_api.run_query(datasets_query, {
                                                    "projectId": project_id, "limit": dataset_limit, "offset": dataset_offset})

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


# def get_fcodes(vbet_input_gpkg: str) -> Dict:
#     """Get the FCode that has the longest total length for each LevelPathI"""

#     fcodes = {}
#     conn = sqlite3.connect(vbet_input_gpkg)
#     curs = conn.cursor()
#     curs.execute("""select fl.FCode, vaa.LevelPathI, sum(fl.LengthKM) length
#         from flowlines fl
#             inner join NHDPlusFlowlineVAA vaa on fl.NHDPlusID = vaa.NHDPlusID
#         group by fl.FCode, vaa.LevelPathI
#         order by length desc""")
#     conn.row_factory = sqlite3.Row

#     for row in curs.fetchall():
#         if row['LevelPathI'] not in fcodes:
#             fcodes[row['LevelPathI']] = row['FCode']

#     return fcodes


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
                curs.execute('DELETE FROM igos')
                curs.execute('DELETE FROM dgos')
                conn.commit()
        return

    # Create the output Geopackage and feature class fields and include the project_id
    vbet_igo_fields = {'project_id': ogr.OFTInteger, 'HUC10': ogr.OFTString,
                       'level_path': ogr.OFTString, 'seg_distance': ogr.OFTReal}
    with GeopackageLayer(output_gpkg, layer_name='igos', delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbPoint, epsg=4326, fields=vbet_igo_fields)

    with GeopackageLayer(output_gpkg, layer_name='hucs', write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPolygon, epsg=4326, fields=huc_fields)
        with ShapefileLayer(huc10_path) as huc_lyr:
            transform = osr.CoordinateTransformation(
                huc_lyr.spatial_ref, out_lyr.spatial_ref)
            for in_feature, _counter, _progbar in huc_lyr.iterate_features():
                geom = in_feature.GetGeometryRef()
                geom.Transform(transform)
                out_lyr.create_feature(geom, {
                                       field: in_feature.GetField(field) for field in ['huc10']})

    # Create database tables
    with sqlite3.connect(output_gpkg) as conn:

        # curs.execute('SELECT load_extension("mod_spatialite")')
        # curs.execute('CREATE TABLE IF NOT EXISTS hucs (huc10 TEXT PRIMARY KEY, name TEXT, states TEXT, areasqkm REAL, status TEXT)')

        # DGO metrics table. No geometry, just the identifiers and metrics
        dgo_field_list = []
        for name, ogr_type in dgo_fields.items():
            sql_type = 'TEXT' if ogr_type == ogr.OFTString else 'REAL'
            dgo_field_list.append(f'{name} {sql_type}')

        # conn.execute("""CREATE TABLE metrics (
        #         metric_id       INTEGER not null primary key,
        #         name            TEXT unique not null,
        #         machine_code    TEXT unique not null,
        #         data_type       TEXT,
        #         field_name      TEXT,
        #         description     TEXT,
        #         method          TEXT,
        #         small           REAL,
        #         medium          REAL,
        #         large           REAL,
        #         metric_group_id INTEGER,
        #         is_active       BOOLEAN,
        #         docs_url        TEXT
        # )""")

        # conn.execute("""CREATE TABLE dgo_metric_values (
        #     dgo_id       INTEGER not null references vbet_dgos (dgo_id) on delete cascade,
        #     metric_id    INTEGER not null constraint fk_metric_id references metrics on delete cascade,
        #     metric_value TEXT,
        #     metadata     TEXT,
        #     qaqc_date    TEXT,
        #     primary key (dgo_id, metric_id)
        # )""")

        # conn.execute(
        #     'CREATE INDEX ix_dgo_metric_values_metric_id on dgo_metric_values (metric_id)')

        # conn.execute("""create table igo_metric_values (
        #     igo_id       INTEGER not null references igos (fid) ON DELETE CASCADE,
        #     metric_id    INTEGER not null constraint fk_metric_id references metrics ON DELETE CASCADE,
        #     metric_value TEXT,
        #     metadata     TEXT,
        #     qaqc_date    TEXT,
        #     primary key (igo_id, metric_id)
        # )""")

        # conn.execute(
        #     'create index main.ix_igo_metric_values_metric_id on igo_metric_values (metric_id)')

        # conn.execute("""CREATE TABLE measurements (
        #     measurement_id INTEGER not null primary key,
        #     name           TEXT unique not null,
        #     machine_code   TEXT unique not null,
        #     data_type      TEXT,
        #     description    TEXT,
        #     is_active      INTEGER
        # )""")

        # conn.execute("""create table measurement_values (
        #     dgo_id            INTEGER not null references dgos (id),
        #     measurement_id    INTEGER not null constraint fk_measurement_id references measurements ON DELETE CASCADE,
        #     measurement_value REAL,
        #     metadata          TEXT,
        #     qaqc_date         TEXT,
        #     primary key (dgo_id, measurement_id)
        # )""")

        conn.execute( f'CREATE TABLE dgos (dgo_id integer not null primary key, {",".join(dgo_field_list)})')
        conn.execute('ALTER TABLE dgos ADD COLUMN project_id INTEGER')
        conn.execute('CREATE INDEX pk_vbet_dgos ON dgos (HUC10, level_path, seg_distance)')
        conn.execute('CREATE INDEX vbet_dgos_project_id_idx ON dgos (project_id)')

        # conn.execute('ALTER TABLE dgos ADD COLUMN FCode TEXT')

        # conn.execute(
        #     'CREATE INDEX vbet_igos_project_id_idx ON igos (project_id)')

        # conn.execute(
        #     'CREATE INDEX vbet_igos_huc10_idx ON igos (huc10, levelPathI, seg_distance)')

        # Create table to track projects
        conn.execute('CREATE TABLE projects (id INTEGER PRIMARY KEY, guid TEXT NOT NULL, huc10 TEXT, project_type TEXT, model_version TEXT, status TEXT, metadata TEXT)')
        conn.execute('CREATE INDEX projects_huc10_idx ON projects (huc10)')
        conn.execute('CREATE UNIQUE INDEX projects_guid_idx ON projects (guid)')

        # Triggers need to be off for the updates to feature class
        triggers = drop_triggers(conn)

        conn.execute('CREATE INDEX ix_hucs_huc10 ON hucs (huc10)')

        # Enrich the HUCs with attributes from the JSON file
        with open(huc_attributes, encoding='utf8') as fhuc:
            huc_json = json.load(fhuc)
            for huc in huc_json:
                conn.execute('UPDATE hucs SET name = ?, states = ?, areasqkm = ? WHERE huc10 = ?', [huc['NAME'], huc['STATES'], huc['AREASQKM'], huc['HUC10']])

        # Update which HUCs are required
        if len(hucs) < 1:
            conn.execute("UPDATE hucs SET status = 'queued'")
        else:
            conn.execute('UPDATE hucs SET status = NULL')
            conn.execute(f"UPDATE hucs SET status = 'queued' WHERE huc10 IN ({','.join(['?'] * len(hucs))})", hucs)

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
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--reset', help='(optional) delete all existing outputs before running and run from scratch', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    hucs = list() if args.HUCs == '' else args.HUCs.split(',')

    safe_makedirs(args.local_folder)

    build_output(args.output_gpkg, args.huc10, hucs, args.huc_attributes, args.reset)
    scrape_projects(args.environment, hucs, args.output_gpkg, args.reset)

    sys.exit(0)
