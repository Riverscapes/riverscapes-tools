# Name:     Conflict Potential Attributes
#
# Purpose:  Calculate the conflict potential attributes for a BRAT network
#
# Author:   Philip Bailey
#
# Date:     17 Oct 2019
#
# Remarks:
#           BLM National Surface Management Agency Area Polygons
#           https://catalog.data.gov/dataset/blm-national-surface-management-agency-area-polygons-national-geospatial-data-asset-ngda
# -------------------------------------------------------------------------------
import argparse
import os
import sys
import traceback
import json
import time
import sqlite3
import gdal
import shutil
from osgeo import ogr
from osgeo import osr
from pygeoprocessing import geoprocessing
from shapely.ops import unary_union
from shapely.geometry import shape, mapping
import rasterio.shutil

from rscommons import ProgressBar, Logger, ModelConfig, dotenv
from rscommons.raster_buffer_stats import raster_buffer_stats2
from rscommons.shapefile import _rough_convert_metres_to_shapefile_units
from rscommons.shapefile import intersect_feature_classes
from rscommons.shapefile import intersect_geometry_with_feature_class
from rscommons.shapefile import delete_shapefile
from rscommons.shapefile import get_transform_from_epsg
from rscommons.shapefile import copy_feature_class
from rscommons.util import safe_makedirs

from sqlbrat.lib.database import load_geometries, get_metadata
from sqlbrat.lib.database import write_attributes


def conflict_attributes(database, valley_bottom, roads, rail, canals, ownership, buffer_distance_metres, cell_size_meters, epsg):
    """Calculate conflict attributes and write them back to a BRAT database

    Arguments:
        database {str} -- Path to BRAT database
        valley_bottom {str} -- Path to valley bottom polygon ShapeFile
        roads {str} -- Path to roads polyline ShapeFile
        rail {str} -- Path to railway polyline ShapeFile
        canals {str} -- Path to canals polyline ShapeFile
        ownership {str} -- Path to land ownership polygon ShapeFile
        buffer_distance_metres {float} -- Distance (meters) to buffer reaches when calculating distance to conflict
        cell_size_meters {float} -- Size of cells (meters) when rasterize Euclidean distance to conflict features
        epsg {str} -- Spatial reference in which to perform the analysis
    """

    # Calculate conflict attributes
    values = calc_conflict_attributes(database, valley_bottom, roads, rail, canals, ownership, buffer_distance_metres, cell_size_meters, epsg)

    # Write float and string fields separately
    write_attributes(database, values, ['iPC_Road', 'iPC_RoadVB', 'iPC_Rail', 'iPC_RailVB', 'iPC_Canal', 'iPC_DivPts', 'iPC_RoadX', 'iPC_Privat', 'oPC_Dist'])
    write_attributes(database, values, ['AgencyID'], summarize=False)


def calc_conflict_attributes(database, valley_bottom, roads, rail, canals, ownership, buffer_distance_metres, cell_size_meters, epsg):

    start_time = time.time()
    log = Logger('Conflict')
    log.info('Calculating conflict attributes')

    # Load all the stream network polylines
    db_meta = get_metadata(database)
    reaches = load_geometries(database)

    # Union all the reach geometries into a single geometry
    reach_union = unary_union(reaches.values())
    reach_union_no_canals = reach_union

    # If the user specified --canal_codes then we use them
    if 'Canal_Codes' in db_meta:
        reaches_no_canals = load_geometries(database, None, 'ReachCode NOT IN ({})'.format(db_meta['Canal_Codes']))
        reach_union_no_canals = unary_union(reaches_no_canals.values())

    # These files are temporary. We will clean them up afterwards
    tmp_folder = os.path.join(os.path.dirname(database), 'tmp_conflict')
    if os.path.isdir(tmp_folder):
        shutil.rmtree(tmp_folder)
    safe_makedirs(tmp_folder)

    crossings = os.path.join(tmp_folder, 'road_crossings.shp')
    intersect_geometry_with_feature_class(reach_union, roads, epsg, crossings, ogr.wkbMultiPoint)

    roads_vb = os.path.join(tmp_folder, 'road_valleybottom.shp')
    intersect_feature_classes(valley_bottom, roads, epsg, roads_vb, ogr.wkbMultiLineString)

    rail_vb = os.path.join(tmp_folder, 'rail_valleybottom.shp')
    intersect_feature_classes(valley_bottom, rail, epsg, rail_vb, ogr.wkbMultiLineString)

    diversions = os.path.join(tmp_folder, 'diversions.shp')
    intersect_geometry_with_feature_class(reach_union_no_canals, canals, epsg, diversions, ogr.wkbMultiPoint)

    private = os.path.join(tmp_folder, 'private.shp')
    copy_feature_class(ownership, epsg, private, None, "ADMIN_AGEN = 'PVT' OR ADMIN_AGEN = 'UND'")

    # Buffer all reaches (being careful to use the units of the Shapefile)
    buffer_distance = _rough_convert_metres_to_shapefile_units(roads, buffer_distance_metres)
    polygons = {reach_id: polyline.buffer(buffer_distance) for reach_id, polyline in reaches.items()}

    results = {}

    cell_size = _rough_convert_metres_to_shapefile_units(roads, cell_size_meters)
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, roads, 'Mean', 'iPC_Road')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, roads_vb, 'Mean', 'iPC_RoadVB')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, crossings, 'Mean', 'iPC_RoadX')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, rail, 'Mean', 'iPC_Rail')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, rail_vb, 'Mean', 'iPC_RailVB')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, canals, 'Mean', 'iPC_Canal')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, diversions, 'Mean', 'iPC_DivPts')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, private, 'Mean', 'iPC_Privat')

    # Calculate minimum distance to conflict
    min_keys = ['iPC_Road', 'iPC_RoadX', 'iPC_RoadVB', 'iPC_Rail', 'iPC_RailVB']
    for values in results.values():
        values['oPC_Dist'] = min([values[x] for x in min_keys if x in values])

    # Retrieve the agency responsible for administering the land at the midpoint of each reach
    admin_agency(database, reaches, ownership, epsg, results)

    log.info('Conflict attribute calculation complete in {:04}s.'.format(time.time() - start_time))

    # Cleanup temporary feature classes
    if os.path.isdir(tmp_folder):
        log.info('Cleaning up temporary data')
        shutil.rmtree(tmp_folder)

    return results


def admin_agency(database, reaches, ownership, epsg, results):

    start_time = time.time()
    log = Logger('Conflict')
    log.info('Calculating land ownership administrating agency for {:,} reach(es)'.format(len(reaches)))

    # Load the administration agency types and key by abbreviation
    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute('SELECT AgencyID, Name, Abbreviation FROM Agencies')
    agencies = {row[2]: {'AgencyID': row[0], 'Name': row[1], 'RawGeometries': [], 'GeometryUnion': None} for row in curs.fetchall()}

    # Load and transform ownership polygons by adminstration agency
    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.Open(ownership, 0)
    layer = data_source.GetLayer()
    # data_srs = layer.GetSpatialRef()
    # output_srs, transform = get_transform_from_epsg(data_srs, epsg)

    progbar = ProgressBar(len(reaches), 50, "Calc administration agency")
    counter = 0

    # Loop over stream reaches and assign agency
    for reach_id, polyline in reaches.items():
        counter += 1
        progbar.update(counter)

        if reach_id not in results:
            results[reach_id] = {}

        mid_point = polyline.interpolate(0.5, normalized=True)
        results[reach_id]['AgencyID'] = None

        layer.SetSpatialFilter(ogr.CreateGeometryFromWkb(mid_point.wkb))
        layer = data_source.GetLayer()
        for feature in layer:
            agency = feature.GetField('ADMIN_AGEN')
            if agency not in agencies:
                raise Exception('The ownership agency "{}" is not found in the BRAT SQLite database'.format(agency))
            results[reach_id]['AgencyID'] = agencies[agency]['AgencyID']

    progbar.finish()
    log.info('Adminstration agency assignment complete in {:04}s'.format(time.time() - start_time))


def distance_from_features(polygons, tmp_folder, bounds, cell_size_meters, cell_size_degrees, output, features, statistic, field):
    """[summary]

    Feature class rasterization
    https://gdal.org/programs/gdal_rasterize.html

    Euclidean distance documentation
    https://www.pydoc.io/pypi/pygeoprocessing-1.0.0/autoapi/geoprocessing/index.html#geoprocessing.distance_transform_edt

    Arguments:
        polygons {[type]} -- [description]
        features_shapefile {[type]} -- [description]
        temp_folder {[type]} -- [description]
    """

    start_time = time.time()
    log = Logger('Conflict')

    if not features:
        log.warning('Skipping distance calculation for {} because feature class does not exist.'.format(field))
        return

    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.Open(features, 0)
    layer = data_source.GetLayer()
    if layer.GetFeatureCount() < 1:
        log.warning('Skipping distance calculation for {} because feature class is empty.'.format(field))
        data_source = None
        return
    data_source = None

    root_path = os.path.join(tmp_folder, os.path.splitext(os.path.basename(features))[0])
    features_raster = root_path + '_features.tif'
    distance_raster = root_path + '_euclidean.tif'

    if os.path.isfile(features_raster):
        rasterio.shutil.delete(features_raster)

    if os.path.isfile(distance_raster):
        rasterio.shutil.delete(distance_raster)

    progbar = ProgressBar(100, 50, "Rasterizing ")

    def poly_progress(progress, msg, data):
        # double dfProgress, char const * pszMessage=None, void * pData=None
        progbar.update(int(progress * 100))

    # Rasterize the features (roads, rail etc) and calculate a raster of Euclidean distance from these features
    log.info('Rasterizing {:,} features at {}m cell size for generating {} field using {} distance.'.format(len(polygons), cell_size_meters, field, statistic))
    progbar.update(0)
    gdal.Rasterize(
        features_raster, features,
        xRes=cell_size_degrees, yRes=cell_size_degrees,
        burnValues=1, outputType=gdal.GDT_Int16,
        creationOptions=['COMPRESS=LZW'],
        outputBounds=bounds,
        callback=poly_progress
    )
    progbar.finish()

    log.info('Calculating Euclidean distance for {}'.format(field))
    geoprocessing.distance_transform_edt((features_raster, 1), distance_raster)

    # Calculate the Euclidean distance statistics (mean, min, max etc) for each polygon
    values = raster_buffer_stats2(polygons, distance_raster)

    # Retrieve the desired statistic and store it as the specified output field
    log.info('Extracting {} statistic for {}.'.format(statistic, field))

    progbar = ProgressBar(len(values), 50, "Extracting Statistics")
    counter = 0

    for reach_id, statistics in values.items():
        counter += 1
        progbar.update(counter)
        if reach_id not in output:
            output[reach_id] = {}
        output[reach_id][field] = round(statistics[statistic] * cell_size_meters, 0)

    progbar.finish()

    # Cleanup the temporary datasets
    rasterio.shutil.delete(features_raster)
    rasterio.shutil.delete(distance_raster)

    log.info('{} distance calculation complete in {:04}s.'.format(field, time.time() - start_time))


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='BRAT SQLite database', type=str)
    parser.add_argument('valley_bottom', help='Valley bottom shapefile', type=str)
    parser.add_argument('roads', help='road network shapefile', type=str)
    parser.add_argument('rail', help='rail network shapefile', type=str)
    parser.add_argument('canals', help='Canals network shapefile', type=str)
    parser.add_argument('ownership', help='Land ownership shapefile', type=str)
    parser.add_argument('--buffer', help='(optional) distance to buffer roads, canalas and rail (metres)', type=float, default=30)
    parser.add_argument('--cell_size', help='(optional) cell size (metres) to rasterize features for Euclidean distance', type=float, default=5)
    parser.add_argument('--epsg', help='(optional) EPSG of the reach geometries', type=str, default=4326)
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("Conflict Attributes")
    logfile = os.path.join(os.path.dirname(args.database), "conflict_attributes.log")
    log.setup(logPath=logfile, verbose=args.verbose)

    try:
        conflict_attributes(args.database, args.valley_bottom, args.roads, args.rail, args.canals, args.ownership, args.buffer, args.cell_size, args.epsg)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
