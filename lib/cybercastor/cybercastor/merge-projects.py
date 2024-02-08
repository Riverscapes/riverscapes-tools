"""
Demo script to download files from Data Exchange
"""
from typing import Dict, List, Tuple
from datetime import datetime
import sys
import os
import subprocess
import json
import argparse
from osgeo import gdal, ogr
from rscommons import Raster
import xml.etree.ElementTree as ET
from rsxml.project_xml import (
    Project,
    MetaData,
    Meta,
    ProjectBounds,
    Coords,
    BoundingBox,
)
from rscommons import dotenv, Logger
from rscommons.util import safe_makedirs
from rscommons import Raster
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI


def search_projects(riverscapes_api, project_type: str, collection_id: str) -> List[str]:
    """_summary_

    Args:
        riverscapes_api (_type_): _description_
        project_type (str): _description_
        collection_id (str): _description_

    Returns:
        List[str]: _description_
    """

    # search_params = {
    #     'projectTypeId': project_type,
    #     # 'meta': [{'key': 'ModelVersion', 'value': '1234'}],
    #     'tags': project_tags
    # }

    log = Logger('Search')
    log.info(f'Searching for projects with type {project_type} in collection {collection_id}')

    project_limit = 500
    project_offset = 0
    total = 0
    projects = []
    while project_offset == 0 or project_offset < total:
        results = riverscapes_api.run_query(riverscapes_api.load_query('collectionProjects'), {'collectionId': collection_id, 'limit': project_limit, 'offset': project_offset})
        total = results['data']['collection']['projects']['total']
        project_offset += project_limit

        for project in results['data']['collection']['projects']['items']:
            if project['projectType']['id'] == project_type:
                projects.append(project)

    log.info(f'Found {len(projects)} {project_type} project(s) in collection {collection_id}')
    return projects

    # if len(projects) == 0:
    #     return None
    # elif len(projects) == 1:
    #     return projects[list(projects.keys())[0]]
    # else:
    #     # Find the model with the greatest version number
    #     project_versions = {}
    #     for project_id, project_info in projects.items():
    #         for key, val in {meta_item['key']: meta_item['value'] for meta_item in project_info['meta']}.items():
    #             if key.replace(' ', '').lower() == 'modelversion' and val is not None:
    #                 project_versions[semver.VersionInfo(val)] = project_id
    #                 break

    #     project_versions_list = list(project_versions)
    #     project_versions_list.sort(reverse=True)
    #     return projects[project_versions[project_versions_list[0]]]


def download_project(riverscapes_api, output_folder, project_id: str, force_download: bool) -> List[str]:
    """_summary_

    Args:
        riverscapes_api (_type_): _description_
        output_folder (_type_): _description_
        project_id (str): _description_
        force_download (bool): _description_

    Returns:
        List[str]: _description_
    """

    # Build a dictionary of files in the project keyed by local path to downloadUrl
    files_query = riverscapes_api.load_query('projectFiles')
    file_results = riverscapes_api.run_query(files_query, {"projectId": project_id})
    files = {file['localPath']: file for file in file_results['data']['project']['files']}

    project_file_path = None
    for rel_path, file in files.items():
        download_path = os.path.join(output_folder, project_id, rel_path)

        if rel_path.endswith('project.rs.xml'):
            project_file_path = download_path

        safe_makedirs(os.path.dirname(download_path))
        riverscapes_api.download_file(file, download_path, force_download)

    log = Logger('Download')
    log.info(f'Downloaded {len(files)} file(s) to project folder {os.path.join(output_folder, project_id)}')
    return project_file_path


def merge_projects(projects: List[str], merged_dir: str, name: str, project_type: str, collection_id: str) -> None:

    log = Logger('Merging')
    log.info(f'Merging {len(projects)} project(s)')

    project_rasters = {}
    project_vectors = {}
    bounds_geojson_files = []
    for project in projects:

        project_xml = project['localPath']
        if project_xml is None:
            print(f'Skipping project with no project.rs.xml file {project["id"]}')
            continue

        get_raster_datasets(project_xml, project_rasters)
        get_vector_datasets(project_xml, project_vectors)
        get_bounds_geojson_file(project_xml, bounds_geojson_files)

    process_rasters(project_rasters, merged_dir)
    process_vectors(project_vectors, merged_dir)

    # build union of project bounds
    output_bounds_path = os.path.join(merged_dir, 'project_bounds.geojson')
    centroid, bounding_rect = union_polygons(bounds_geojson_files, output_bounds_path)

    # Generate a new project.rs.xml file for the merged project based
    # on the first project in the list
    merge_project = Project.load_project(projects[0]['localPath'])
    merge_project.name = name

    merge_project.description = f"""This project was generated by merging {len(projects)} {project_type} projects together,
            using the merge-projects.py script.  The project bounds are the union of the bounds of the
            individual projects."""

    coords = Coords(centroid[0], centroid[1])
    bounding_box = BoundingBox(bounding_rect[0], bounding_rect[2], bounding_rect[1], bounding_rect[3])
    merge_project.bounds = ProjectBounds(coords, bounding_box, os.path.basename(output_bounds_path))

    project_urls = [f'https://data.riverscapes.net/p/{project["id"]}' for project in projects]

    merge_project.meta_data = MetaData([Meta('projects', json.dumps(project_urls), 'json', None)])
    merge_project.meta_data.add_meta('Date Created', str(datetime.now().isoformat()), meta_type='isodate', ext=None)
    merge_project.meta_data.add_meta('Collection ID', collection_id)
    merge_project.warehouse = None

    merged_project_xml = os.path.join(merged_dir, 'project.rs.xml')
    merge_project.write(merged_project_xml)
    replace_log_file(merged_project_xml)
    delete_unmerged_paths(merged_project_xml)


def delete_unmerged_paths(merged_project_xml):
    """
    Reports, ShapeFiles and logs are not included in the merge.
    Look for all elements called <Path> and remove their parents
    """
    log = Logger('Delete')

    # Load the XML file and search for any tag called Path
    tree = ET.parse(merged_project_xml)

    # create a dictionary that maps from each element to its parent
    root = tree.getroot()
    parent_map = {c: p for p in root.iter() for c in p}

    for path_element in tree.findall('.//Path'):
        file_ext = ['gpkg', 'geojson', 'tif', 'tiff', 'log']
        matches = [ext for ext in file_ext if path_element.text.lower().endswith(ext)]
        if len(matches) == 0:
            log.info(f'Removing non GeoPackage, raster or log with contents {path_element.text}')
            # Get and remove the parent of the Path element
            parent = parent_map[path_element]
            grandparent = parent_map[parent]
            grandparent.remove(parent)

    tree.write(merged_project_xml)


def replace_log_file(merged_project_xml) -> None:
    """
    Load the merged project.rs.xml and search for all occurences
    of a log file and replace them with the merged log file
    """

    log = Logger('Log')

    tree = ET.parse(merged_project_xml)
    for log_file in tree.findall('.//LogFile/Path'):
        log_file.text = os.path.basename(log.instance.logpath)
    tree.write(merged_project_xml)


def union_polygons(input_geojson_files, output_geojson_file) -> Tuple[str, str]:

    # Create a new OGR memory data source
    mem_driver = ogr.GetDriverByName('Memory')
    mem_ds = mem_driver.CreateDataSource('')

    # Create a new layer in the memory data source
    mem_layer = mem_ds.CreateLayer('union', geom_type=ogr.wkbPolygon)

    # Iterate over input GeoJSON files
    for input_file in input_geojson_files:
        # Open the GeoJSON file
        with open(input_file, 'r', encoding='utf8') as file:
            geojson_data = json.load(file)

        # Create an OGR feature and set its geometry

        # Extract coordinates from the GeoJSON structure
        coordinates = geojson_data['features'][0]['geometry']['coordinates']

        # Create an OGR feature and set its geometry
        feature_defn = mem_layer.GetLayerDefn()
        feature = ogr.Feature(feature_defn)
        geometry = ogr.CreateGeometryFromJson(json.dumps({
            "type": "Polygon",
            "coordinates": coordinates
        }))
        feature.SetGeometry(geometry)

        # Add the feature to the layer
        mem_layer.CreateFeature(feature)

    # Perform the union operation on the layer
    union_result = None
    for feature in mem_layer:
        if union_result is None:
            union_result = feature.GetGeometryRef().Clone()
        else:
            union_result = union_result.Union(feature.GetGeometryRef())

    # Remove any donuts (typically slivers caused by rounding the individual Polygon extents)
    clean_polygon = ogr.Geometry(ogr.wkbPolygon)
    ring = union_result.GetGeometryRef(0)
    clean_polygon.AddGeometry(ring)

    # Get centroid coordinates
    centroid = clean_polygon.Centroid().GetPoint()

    # Get bounding rectangle coordinates (min_x, max_x, min_y, max_y)
    bounding_rect = clean_polygon.GetEnvelope()

    # Create a new GeoJSON file for the union result
    output_driver = ogr.GetDriverByName('GeoJSON')
    output_ds = output_driver.CreateDataSource(output_geojson_file)
    output_layer = output_ds.CreateLayer('union', geom_type=ogr.wkbPolygon)

    # Create a feature and set the geometry for the union result
    feature_defn = output_layer.GetLayerDefn()
    feature = ogr.Feature(feature_defn)
    feature.SetGeometry(clean_polygon)

    # Add the feature to the output layer
    output_layer.CreateFeature(feature)

    # Clean up resources
    mem_ds = None
    output_ds = None

    return centroid, bounding_rect


def get_bounds_geojson_file(project_xml_path: str, bounds_files):
    """
    Get the GeoJSON file for the project bounds
    project_xml_path: str - Path to the project.rs.xml file
    bounds_files: List - List of GeoJSON files
    """

    tree = ET.parse(project_xml_path)
    rel_path = tree.find('.//ProjectBounds/Path').text
    abs_path = os.path.join(os.path.dirname(project_xml_path), rel_path)
    if os.path.isfile(abs_path):
        bounds_files.append(abs_path)


def get_vector_datasets(project_xml_path: str, master_project: Dict) -> None:
    """
    Discover all the vector datasets in the project.rs.xml file and incorporate them
    intro the master project dictionary.
    project: str - Path to the project.rs.xml file
    master_project: Dict - The master list of GeoPackages and feature classes
    """

    tree = ET.parse(project_xml_path)
    # find each geopackage in the project
    for geopackage in tree.findall('.//Geopackage'):
        gpkg_id = geopackage.attrib['id']
        path = geopackage.find('Path').text
        name = geopackage.find('Name').text

        if (gpkg_id not in master_project):
            master_project[gpkg_id] = {'rel_path': path, 'abs_path': os.path.join(os.path.dirname(project_xml_path), path), 'name': name, 'id': gpkg_id, 'layers': {}}

        # find each layer in the geopackage
        for layer in geopackage.findall('.//Vector'):
            fc_name = layer.attrib['lyrName']
            layer_name = layer.find('Name').text

            if fc_name not in master_project[gpkg_id]['layers']:
                master_project[gpkg_id]['layers'][fc_name] = {'fc_name': fc_name, 'name': layer_name, 'occurences': []}

            master_project[gpkg_id]['layers'][fc_name]['occurences'].append({'path': os.path.join(os.path.dirname(project_xml_path), path)})


def process_vectors(master_project: Dict, output_dir: str) -> None:
    """
    Process the vector datasets in the master project dictionary.  This will
    merge all the vector datasets within each GeoPackage into new GeoPackages
    in the output directory.
    master_project: Dict - The master list of GeoPackages and feature classes
    output_dir: str - The top level output directory
    """

    log = Logger('Vectors')

    for gpkg_info in master_project.values():
        log.info(f'Processing {gpkg_info["name"]} GeoPackage at {gpkg_info["rel_path"]} with {len(gpkg_info["layers"])} layers.')

        # output GeoPackage
        output_gpkg = os.path.join(output_dir, gpkg_info['rel_path'])
        output_gpkg_file = os.path.basename(output_gpkg)
        output_gpkg_dir = os.path.dirname(output_gpkg)
        safe_makedirs(output_gpkg_dir)

        if os.path.isfile(output_gpkg):
            os.remove(output_gpkg)

        for feature_class, feature_class_info in gpkg_info['layers'].items():

            for input_gpkg in feature_class_info['occurences']:
                input_gpkg_file = input_gpkg['path']

                # -nlt {geometry_type}
                input_gpkg_file = input_gpkg['path']
                cmd = f'ogr2ogr -f GPKG -makevalid -append  -nln {feature_class} {output_gpkg_file} {input_gpkg_file} {feature_class}'
                subprocess.call([cmd], shell=True, cwd=output_gpkg_dir)


def process_rasters(master_project: Dict, output_dir: str) -> None:
    """
    Process the raster datasets in the master project dictionary.  This will
    merge all occurances of each type of raster into a single raster for each type.
    master_project: Dict - The master list of rasters in the project
    output_dir: str - The top level output directory
    """

    log = Logger('Rasters')

    for raster_info in master_project.values():
        log.info(f'Merging {len(raster_info["occurences"])} {raster_info["name"]} rasters.')

        raster_path = os.path.join(output_dir, raster_info['path'])
        safe_makedirs(os.path.dirname(raster_path))

        raster = Raster(raster_info['occurences'][0]['path'])
        integer_raster_enums = [gdal.GDT_Byte, gdal.GDT_UInt16, gdal.GDT_UInt32, gdal.GDT_Int16, gdal.GDT_Int32]
        compression = f'COMPRESS={"DEFLATE" if raster.dataType in integer_raster_enums else "LZW" }'
        no_data = f'-a_nodata {raster.nodata}' if raster.nodata is not None else ''

        input_rasters = [rp['path'] for rp in raster_info['occurences']]

        params = ['gdal_merge.py', '-o', raster_path, '-co', compression, no_data] + input_rasters
        print(params)
        params_flat = ' '.join(params)
        subprocess.call(params_flat, shell=True)


def get_raster_datasets(project, master_project) -> None:
    """
    Discover all the rasters in the project.rs.xml file and incorporate them
    intro the master project dictionary.
    project: str - Path to the project.rs.xml file
    master_project: Dict - The master list of rasters across all projects
    """

    tree = ET.parse(project)
    rasters = tree.findall('.//Raster') + tree.findall('.//DEM')
    for raster in rasters:
        raster_id = raster.attrib['id']
        path = raster.find('Path').text
        name = raster.find('Name').text
        if raster_id not in master_project:
            master_project[raster_id] = {'path': path, 'name': name, 'id': raster_id, 'occurences': []}
        master_project[raster_id]['occurences'].append({'path': os.path.join(os.path.dirname(project), path)})


def main():
    """
    Merge projects
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('environment', help='Riverscapes stage', type=str, default='production')
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('project_type', help='project type', type=str)
    parser.add_argument('collection_id', help='ID of the collection containing the projects', type=str)
    parser.add_argument('name', help='Output project name', type=str)
    parser.add_argument('merged_folder', help='Subfolder inside working folder where output will be stored', type=str)
    args = dotenv.parse_args_env(parser)

    # This is the folder where the merged project will be created
    merged_folder = os.path.join(args.working_folder, args.merged_folder)

    log = Logger('Setup')
    log.setup(logPath=os.path.join(merged_folder, 'merge-projects.log'))

    riverscapes_api = RiverscapesAPI(stage=args.environment)
    if riverscapes_api.access_token is None:
        riverscapes_api.refresh_token()

    projects = search_projects(riverscapes_api, args.project_type, args.collection_id)

    if (len(projects) < 2):
        log.error(f'Insufficient number of projects ({len(projects)}) found with type {args.project_type} and tags {args.project_tags}. 2 or more needed.')
        sys.exit(1)

    for project in projects:
        project_id = project['id']
        project_local = download_project(riverscapes_api, args.working_folder, project_id, False)
        project['localPath'] = project_local

    merge_projects(projects, merged_folder, args.name, args.project_type, args.collection_id)

    log.info('Process complete')

    sys.exit(0)


if __name__ == '__main__':
    main()
