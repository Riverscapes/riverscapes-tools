# Name:     Conflict Potential Attributes
#
# Purpose:  Calculate the conflict potential attributes for a BRAT network
#
# Author:   Philip Bailey
#
# Date:     17 Oct 2019
#
# Remarks:  BLM National Surface Management Agency Area Polygons
#           https://catalog.data.gov/dataset/blm-national-surface-management-agency-area-polygons-national-geospatial-data-asset-ngda
# -------------------------------------------------------------------------------
import os
import shutil
from typing import List
from osgeo import ogr, gdal
from pygeoprocessing import geoprocessing
import rasterio.shutil
from rscommons import ProgressBar, Logger
from rscommons.raster_buffer_stats import raster_buffer_stats2
from rscommons.util import safe_makedirs, safe_remove_dir
from rscommons.database import write_db_attributes
from rscommons.vector_ops import intersect_feature_classes, get_geometry_unary_union, load_geometries, intersect_geometry_with_feature_class, copy_feature_class
from rscommons.classes.vector_classes import get_shp_or_gpkg, GeopackageLayer
from rscommons.database import SQLiteCon


def conflict_attributes(
        output_gpkg: str,
        flowlines_path: str,
        valley_bottom: str,
        roads: str,
        rail: str,
        canals: str,
        ownership: str,
        buffer_distance_metres: float,
        cell_size_meters: float,
        epsg: int,
        canal_codes: List[int],
        intermediates_gpkg_path: str):
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
    values = calc_conflict_attributes(flowlines_path, valley_bottom, roads, rail, canals, ownership, buffer_distance_metres, cell_size_meters, epsg, canal_codes, intermediates_gpkg_path)

    # Write float and string fields separately with log summary enabled
    write_db_attributes(output_gpkg, values, ['iPC_Road', 'iPC_RoadVB', 'iPC_Rail', 'iPC_RailVB', 'iPC_Canal', 'iPC_DivPts', 'iPC_RoadX', 'iPC_Privat', 'oPC_Dist'])
    write_db_attributes(output_gpkg, values, ['AgencyID'], summarize=False)


def calc_conflict_attributes(flowlines_path, valley_bottom, roads, rail, canals, ownership, buffer_distance_metres, cell_size_meters, epsg, canal_codes, intermediates_gpkg_path):

    log = Logger('Conflict')
    log.info('Calculating conflict attributes')

    # Create union of all reaches and another of the reaches without any canals
    reach_union = get_geometry_unary_union(flowlines_path)
    if canal_codes is None:
        reach_union_no_canals = reach_union
    else:
        reach_union_no_canals = get_geometry_unary_union(flowlines_path, attribute_filter='FCode NOT IN ({})'.format(','.join(canal_codes)))

    crossin = intersect_geometry_to_layer(intermediates_gpkg_path, 'road_crossings', ogr.wkbMultiPoint, reach_union, roads, epsg)
    if reach_union_no_canals is not None:
        diverts = intersect_geometry_to_layer(intermediates_gpkg_path, 'diversions', ogr.wkbMultiPoint, reach_union_no_canals, canals, epsg)
    else:
        diverts = None

    road_vb = intersect_to_layer(intermediates_gpkg_path, valley_bottom, roads, 'road_valleybottom', ogr.wkbMultiLineString, epsg)
    rail_vb = intersect_to_layer(intermediates_gpkg_path, valley_bottom, rail, 'rail_valleybottom', ogr.wkbMultiLineString, epsg)

    private = os.path.join(intermediates_gpkg_path, 'private_land')
    copy_feature_class(ownership, private, epsg, "ADMIN_AGEN = 'PVT' OR ADMIN_AGEN = 'UND'")

    # Buffer all reaches (being careful to use the units of the Shapefile)
    reaches = load_geometries(flowlines_path, epsg=epsg)
    with get_shp_or_gpkg(flowlines_path) as lyr:
        buffer_distance = lyr.rough_convert_metres_to_vector_units(buffer_distance_metres)
        cell_size = lyr.rough_convert_metres_to_vector_units(cell_size_meters)
        geopackage_path = lyr.filepath

    polygons = {reach_id: polyline.buffer(buffer_distance) for reach_id, polyline in reaches.items()}

    results = {}
    tmp_folder = os.path.join(os.path.dirname(intermediates_gpkg_path), 'tmp_conflict')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, road_vb, 'Mean', 'iPC_RoadVB')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, crossin, 'Mean', 'iPC_RoadX')
    if diverts is not None:
        distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, diverts, 'Mean', 'iPC_DivPts')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, private, 'Mean', 'iPC_Privat')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, rail_vb, 'Mean', 'iPC_RailVB')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, canals, 'Mean', 'iPC_Canal')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, roads, 'Mean', 'iPC_Road')
    distance_from_features(polygons, tmp_folder, reach_union.bounds, cell_size_meters, cell_size, results, rail, 'Mean', 'iPC_Rail')

    # Calculate minimum distance to conflict
    min_keys = ['iPC_Road', 'iPC_RoadX', 'iPC_RoadVB', 'iPC_Rail', 'iPC_RailVB']
    for values in results.values():
        values['oPC_Dist'] = min([values[x] for x in min_keys if x in values])

    # Retrieve the agency responsible for administering the land at the midpoint of each reach
    admin_agency(geopackage_path, reaches, ownership, results)

    log.info('Conflict attribute calculation complete')

    # Cleanup temporary feature classes
    safe_remove_dir(tmp_folder)

    return results


def intersect_geometry_to_layer(gpkg_path, layer_name, geometry_type, geometry, feature_class, epsg):

    geom = intersect_geometry_with_feature_class(geometry, feature_class, geometry_type, epsg)
    if geom is None:
        return None

    with GeopackageLayer(gpkg_path, layer_name=layer_name, write=True) as out_lyr:
        out_lyr.create_layer(geometry_type, epsg=epsg)
        feature = ogr.Feature(out_lyr.ogr_layer_def)
        feature.SetGeometry(GeopackageLayer.shapely2ogr(geom))
        out_lyr.ogr_layer.CreateFeature(feature)

    return os.path.join(gpkg_path, layer_name)


def intersect_to_layer(gpkg_path, feature_class1, feature_class2, layer_name, geometry_type, epsg):

    geom = intersect_feature_classes(feature_class1, feature_class2, geometry_type, epsg)
    if geom is None:
        return None

    with GeopackageLayer(gpkg_path, layer_name=layer_name, write=True) as out_lyr:
        out_lyr.create_layer(geometry_type, epsg=epsg)
        feature = ogr.Feature(out_lyr.ogr_layer_def)
        feature.SetGeometry(GeopackageLayer.shapely2ogr(geom))
        out_lyr.ogr_layer.CreateFeature(feature)

    return os.path.join(gpkg_path, layer_name)


def admin_agency(database, reaches, ownership, results):

    log = Logger('Conflict')
    log.info('Calculating land ownership administrating agency for {:,} reach(es)'.format(len(reaches)))

    # Load the agency lookups
    with SQLiteCon(database) as database:
        database.curs.execute('SELECT AgencyID, Name, Abbreviation FROM Agencies')
        agencies = {row['Abbreviation']: {'AgencyID': row['AgencyID'], 'Name': row['Name'], 'RawGeometries': [], 'GeometryUnion': None} for row in database.curs.fetchall()}

    with get_shp_or_gpkg(ownership) as ownership_lyr:

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

            for feature, _counter, _progbar in ownership_lyr.iterate_features(clip_shape=mid_point):
                agency = feature.GetField('ADMIN_AGEN')
                if agency not in agencies:
                    raise Exception('The ownership agency "{}" is not found in the BRAT SQLite database'.format(agency))
                results[reach_id]['AgencyID'] = agencies[agency]['AgencyID']

    progbar.finish()
    log.info('Adminstration agency assignment complete')


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

    log = Logger('Conflict')

    if features is None:
        log.warning('Skipping distance calculation for {} because feature class does not exist.'.format(field))
        return

    with get_shp_or_gpkg(features) as lyr:
        if lyr.ogr_layer.GetFeatureCount() < 1:
            log.warning('Skipping distance calculation for {} because feature class is empty.'.format(field))
            return

    safe_makedirs(tmp_folder)

    root_path = os.path.join(tmp_folder, os.path.splitext(os.path.basename(features))[0])
    features_raster = root_path + '_features.tif'
    distance_raster = root_path + '_euclidean.tif'

    if os.path.isfile(features_raster):
        rasterio.shutil.delete(features_raster)

    if os.path.isfile(distance_raster):
        rasterio.shutil.delete(distance_raster)

    progbar = ProgressBar(100, 50, "Rasterizing ")

    def poly_progress(progress, _msg, _data):
        progbar.update(int(progress * 100))

    # Rasterize the features (roads, rail etc) and calculate a raster of Euclidean distance from these features
    log.info('Rasterizing {:,} features at {}m cell size for generating {} field using {} distance.'.format(len(polygons), cell_size_meters, field, statistic))
    progbar.update(0)
    gdal.Rasterize(
        features_raster, os.path.dirname(features),
        layers=os.path.basename(features),
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

    log.info('{} distance calculation complete'.format(field))
