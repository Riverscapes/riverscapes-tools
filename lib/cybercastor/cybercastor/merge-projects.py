"""
Demo script to download files from Data Exchange
"""
from typing import Dict, List
import sys
import os
import subprocess
import json
import argparse
import sqlite3
import semantic_version
from typing import Dict
from shutil import rmtree
from osgeo import ogr, osr
from rscommons import dotenv, Logger, GeopackageLayer, ShapefileLayer
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI
from rscommons.util import safe_makedirs
import xml.etree.ElementTree as ET
# from osgeo_utils.gdal_merge import *


def merge_projects(projects: List[str], output_dir: str):

    log = Logger()
    log.info(f'Merging {len(projects)} project(s)')

    project_rasters = {}
    project_vectors = {}
    for project in projects:
        get_raster_datasets(project, project_rasters)
        get_vector_datasets(project, project_vectors)

    # process_rasters(project_rasters, output_dir)
    process_vectors(project_vectors, output_dir)


def get_vector_datasets(project, master_project):

    tree = ET.parse(project)
    # find each geopackage in the project
    for geopackage in tree.findall('.//Geopackage'):
        gpkg_id = geopackage.attrib['id']
        path = geopackage.find('Path').text
        name = geopackage.find('Name').text

        if (gpkg_id not in master_project):
            master_project[gpkg_id] = {'rel_path': path, 'abs_path': os.path.join(os.path.dirname(project), path), 'name': name, 'id': gpkg_id, 'layers': {}}

        # find each layer in the geopackage
        for layer in geopackage.findall('.//Vector'):
            fc_name = layer.attrib['lyrName']
            layer_name = layer.find('Name').text

            if fc_name not in master_project[gpkg_id]['layers']:
                master_project[gpkg_id]['layers'][fc_name] = {'fc_name': fc_name, 'name': layer_name, 'occurences': []}

            master_project[gpkg_id]['layers'][fc_name]['occurences'].append({'path': os.path.join(os.path.dirname(project), path)})


def process_vectors(master_project: Dict, output_dir: str):

    for gpkg_id, gpkg_info in master_project.items():

        # output GeoPackage
        output_gpkg = os.path.join(output_dir, gpkg_info['rel_path'])
        output_gpkg_file = os.path.basename(output_gpkg)
        output_dir = os.path.dirname(output_gpkg)
        safe_makedirs(output_dir)

        if os.path.isfile(output_gpkg):
            os.remove(output_gpkg)

        # input_gpkg = gpkg_info['abs_path']

        for feature_class, feature_class_info in gpkg_info['layers'].items():

            for input_gpkg in feature_class_info['occurences']:
                input_gpkg_file = input_gpkg['path']

                # -nlt {geometry_type}
                input_gpkg_file = input_gpkg['path']
                cmd = f'ogr2ogr -f GPKG -makevalid -append  -nln {feature_class} {output_gpkg_file} {input_gpkg_file} {feature_class}'
                subprocess.call([cmd], shell=True, cwd=output_dir)


def process_rasters(master_project: Dict, output_dir: str):

    # Merge all the rasters together
    for raster_id, raster_info in master_project.items():

        raster_path = os.path.join(output_dir, raster_info['path'])
        safe_makedirs(os.path.dirname(raster_path))

        input_rasters = [rp['path'] for rp in raster_info['occurences']]
        input_files_path = ','.join(input_rasters)
        # parameters = ['', '-o', raster_path] + input_files_path + ['-separate', '-co', 'COMPRESS=LZW']

        gm = os.path.join(os.path.dirname(__file__), '..', '..', '..', '.venv', 'bin', 'gdal_merge.py')
        params = ['python', gm, '-o', raster_path, '-co', 'COMPRESS=LZW'] + input_rasters
        subprocess.call(params, shell=False)

    print(master_project)
    print('hi')


def get_raster_datasets(project, master_project):

    # Load the project XML file and search for all raster items anywhere in the XPath
    # tree.  This will return a list of all raster items in the project.

    # Load the project XML and use element tree to search for all raster tags
    # anywhere in the tree.  This will return a list of all raster items in the
    # project.
    tree = ET.parse(project)
    for raster in tree.findall('.//Raster'):
        id = raster.attrib['id']
        path = raster.find('Path').text
        name = raster.find('Name').text
        if id not in master_project:
            master_project[id] = {'path': path, 'name': name, 'id': id, 'occurences': []}
        master_project[id]['occurences'].append({'path': os.path.join(os.path.dirname(project), path)})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('environment', help='Riverscapes stage', type=str, default='production')
    parser.add_argument('output_folder', help='top level output folder', type=str)
    args = dotenv.parse_args_env(parser)

    # hucs = list() if args.HUCs == '' else args.HUCs.split(',')

    # safe_makedirs(args.output_folder)

    projects = [
        '/Users/philipbailey/GISData/riverscapes/VBET/VBET-Valley_Bottom_for_Summit_Creek/project.rs.xml',
        '/Users/philipbailey/GISData/riverscapes/VBET/Valley_Bottom_Extraction_Tool_VBET-Valley_Bottom_for_Scofield_Reservoir/project.rs.xml']

    merge_projects(projects, args.output_folder)

    sys.exit(0)
