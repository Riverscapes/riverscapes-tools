"""
      Name: LTPBR Export Tool

   Purpose: Retrieve all projects from the LTPBR Explorer web site (https://bda-explorer.herokuapp.com/)
            and store them as a Riverscapes project

    Author: Philip Bailey

      Date: 27 Mar 2024
"""
from typing import Dict
import argparse
import os
import sys
import traceback
import json
import sqlite3
from datetime import datetime
import requests
from osgeo import ogr

from rscommons.util import safe_makedirs, parse_metadata
from rscommons import ModelConfig, Logger, dotenv, initGDALOGRErrors
from rscommons import GeopackageLayer as RSGeopackageLayer

from rsxml.project_xml import (
    Project,
    Meta,
    MetaData,
    ProjectBounds,
    Coords,
    BoundingBox,
    Realization,
    Geopackage,
    GeopackageLayer
)

from ltpbrexport.__version__ import __version__

LTPBR_EXPLORER_URL = 'https://bda-explorer.herokuapp.com'


initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

project_fields = {
    "name": ogr.OFTString,
    "stream_name": ogr.OFTString,
    "watershed": ogr.OFTString,
    "implementation_date": ogr.OFTString,
    "narrative": ogr.OFTString,
    "length": ogr.OFTReal,
    "primary_contact": ogr.OFTString,
    "number_of_structures": ogr.OFTInteger,
    "structure_description": ogr.OFTString,
    "state_id": ogr.OFTInteger,
    "created_at": ogr.OFTDateTime,
    "updated_at": ogr.OFTDateTime,
    "url": ogr.OFTString
}


def ltpbr_export(project_folder: str, epsg=4326, meta: Dict[str, str] = None) -> None:
    """
    Retrieve all projects from the LTPBR Explorer web site (https://bda-explorer.herokuapp.com/)
    and store them as a Riverscapes project

    Args:
        project_folder (str): absolute path where the riverscapes project will get created
        epsg: Coordinate reference system for the incoming LTPBR Explorer project points
        meta (Dict[str, str], optional): metadata key-value pairs. Defaults to None.
    """

    log = Logger('LTPBRExport')
    log.info(f'Starting LTPBR Export v.{cfg.version}')

    geopkg_path = os.path.join(project_folder, 'outputs', 'ltpbr_export.gpkg')
    bounds_path = os.path.join(project_folder, 'project_bounds.geojson')
    project_xml = os.path.join(project_folder, 'project.rs.xml')

    # Create the output feature class fields. Only those listed here will get copied from the source
    with RSGeopackageLayer(geopkg_path, layer_name='projects', delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbPoint, epsg=epsg, fields=project_fields)

    # Add the lookup tables for Organizations and US States
    schema_path = os.path.join(os.path.dirname(__file__), 'database', 'ltpbr_export_schema.sql')
    with open(schema_path, 'r', encoding='utf8') as file:
        sql_statements = file.read()
        with sqlite3.connect(geopkg_path) as conn:
            conn.execute('PRAGMA foreign_keys = ON;')
            curs = conn.cursor()
            curs.executescript(sql_statements)

            insert_lookup_data(curs, 'organizations', ['id', 'name', 'description', 'contact', 'website', 'created_at', 'updated_at', 'url'])
            insert_lookup_data(curs, 'states', ['id', 'name', 'iso_code', 'created_at', 'updated_at', 'url'])
            conn.commit()

    # Create a new Geometry Collection of all the project points
    geometry_collection = ogr.Geometry(ogr.wkbGeometryCollection)

    # Retrieve the projects from the LTPBR Explorer API endpoint and write them to GeoPackage
    projects = get_json_data('projects')
    with RSGeopackageLayer(geopkg_path, 'projects', write=True) as out_lyr:
        layer_defn: ogr.FeatureDefn = out_lyr.ogr_layer.GetLayerDefn()
        out_lyr.ogr_layer.StartTransaction()

        for proj in projects:
            out_ftr = ogr.Feature(layer_defn)
            out_ftr.SetFID(proj['id'])
            geom: ogr.Geometry = ogr.CreateGeometryFromWkt(proj['lonlat'])
            out_ftr.SetGeometry(geom)
            geometry_collection.AddGeometry(geom)

            for field_name in project_fields:
                if field_name in proj:
                    field_index = layer_defn.GetFieldIndex(field_name)
                    if field_index >= 0:
                        out_ftr.SetField(field_name, proj[field_name])

            out_lyr.ogr_layer.CreateFeature(out_ftr)
        out_lyr.ogr_layer.CommitTransaction()

    log.info(f'Exported {len(projects)} projects to {geopkg_path}')

    # Build a bounding box from the geometry collection
    bounding_rect = geometry_collection.GetEnvelope()
    centroid = geometry_collection.Centroid()

    # Create a polygon representing the bounding rectangle
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint_2D(bounding_rect[0], bounding_rect[2])  # Lower-left corner
    ring.AddPoint_2D(bounding_rect[1], bounding_rect[2])  # Lower-right corner
    ring.AddPoint_2D(bounding_rect[1], bounding_rect[3])  # Upper-right corner
    ring.AddPoint_2D(bounding_rect[0], bounding_rect[3])  # Upper-left corner
    ring.AddPoint_2D(bounding_rect[0], bounding_rect[2])  # Close the polygon

    polygon = ogr.Geometry(ogr.wkbPolygon)
    polygon.AddGeometry(ring)

    # Write the polygon to a GeoJSON text file
    geojson_data = {
        "type": "Feature",
        "geometry": json.loads(polygon.ExportToJson())
    }

    with open(bounds_path, 'w', encoding='utf8') as file:
        json.dump(geojson_data, file, indent=2)

    # Prepare some project metadata
    meta_formated = MetaData(values=[
        Meta(name='ModelVersion', value=__version__),
        Meta(name='LTPBR Explorer', value=LTPBR_EXPLORER_URL, type='url'),
        Meta(name='Date Created', value=datetime.now().isoformat(), type='isodate')
    ])

    if meta is not None:
        for key, value in meta.items():
            meta_formated.add_meta(key, value)

    # Construct the Riverscapes Project data structure and write to XML file
    project = Project(
        project_type='ltpbrprojects',
        name='LTPBR Projects',
        summary='LTPBR Projects exported from the LTPBR Explorer',
        description='This project was built by retrieving all the projects from the LTPBR Explorer.',
        citation=None,
        meta_data=meta_formated,
        bounds=ProjectBounds(
            centroid=Coords(centroid.GetX(), centroid.GetY()),
            bounding_box=BoundingBox(bounding_rect[0], bounding_rect[2], bounding_rect[1], bounding_rect[3]),
            filepath=os.path.relpath(bounds_path, os.path.dirname(project_xml))
        ),
        realizations=[Realization(
            name='LTPBR Explorer Export',
            product_version=__version__,
            xml_id='LTPBR',
            date_created=datetime.now(),
            datasets=[Geopackage(
                xml_id='OUTPUTS',
                name='LTPBR Export',
                path=os.path.relpath(geopkg_path, os.path.dirname(project_xml)),
                layers=[GeopackageLayer(
                    name='LTPBR Projects',
                    lyr_name='projects',
                    ds_type='Vector',
                    description='LTPBR Explorer Projects',
                )],
            )]
        )]
    )
    project.write(project_xml)
    log.info('Riverscapes Project XML written to ' + project_xml)

    log.info('LTPBR Explorer Export complete')


def insert_lookup_data(curs: sqlite3.Cursor, table: str, fields) -> None:
    """
    Retrieves JSON data for an individual database table and
    inserts it into the GeoPackage.

    Args:
        curs (sqlite3.Cursor): Open SQLite Cursor
        table (str): Name of the API endpoint and also the GeoPackage database table
        fields (_type_): List of fields to retrieve from the endpoint and insert into
                the GeoPackage
    """

    data = get_json_data(table)
    count = 0
    for json_obj in data:
        insert_query = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({', '.join(['?'] * len(fields))})"
        values = tuple(json_obj[field] for field in fields)
        curs.execute(insert_query, values)
        count += 1

    log = Logger('Lookup Data')
    log.info(f'Inserted {count} records into {table}')


def get_json_data(endpoint: str) -> dict:
    """Call the LTPBR Explorer API and retrieve JSON data from a single
    endpoint.

    Args:
        endpoint (str): Name of the endpoint to call (e.g. projects, organizations, states)

    Returns:
        dict: the JSON data returned from the API converted to Python data structure
    """

    log = Logger('API Request')

    try:
        # Send a GET request to the specified URL
        url = f'https://bda-explorer.herokuapp.com/{endpoint}.json'
        log.info(f'Retrieving data from {url}')
        response = requests.get(url, timeout=100)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse JSON response
            json_data = response.json()
            return json_data
        else:
            # Print an error message if the request was not successful
            print("Error:", response.status_code)
            raise Exception(f"An error occurred: {e}") from e
    except Exception as e:
        print("An error occurred:", e)
        raise Exception(f"An error occurred: {e}") from e


def main():
    """ Export LTPBR Explorer Data to a Riverscapes Project
    """
    parser = argparse.ArgumentParser(description='LTPBR Export Tool')
    parser.add_argument('output_dir', help='Folder where output VBET project will be created', type=str)
    parser.add_argument('--epsg', help='EPSG for output feature class', type=int, default=4326)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='Add debug tools for tracing things like memory usage at a performance cost.', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # make sure the output folder exists
    safe_makedirs(args.output_dir)

    # Initiate the log file
    log = Logger('LTPBR Export')
    log.setup(logPath=os.path.join(args.output_dir, 'ltpbr_export.log'), verbose=args.verbose)
    log.title('LTPBR Export')

    meta = parse_metadata(args.meta)

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_dir, 'vbet_mem.log')
            retcode, max_obj = ThreadRun(ltpbr_export, memfile, args.output_dir, meta=meta)
            log.debug(f'Return code: {retcode}, [Max process usage] {max_obj}')

        else:
            ltpbr_export(args.output_dir, args.epsg, meta=meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
