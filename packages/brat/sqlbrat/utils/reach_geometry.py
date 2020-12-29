# Name:     Reach Geometry
#
# Purpose:  Calculates several properties of each network polyline:
#           Slope, length, min and max elevation.
#
# Author:   Philip Bailey
#
# Date:     23 May 2019
# -------------------------------------------------------------------------------
import argparse
import os
import sys
import traceback
from osgeo import gdal, ogr
import json
from osgeo import osr
from shapely.geometry import Point, mapping
import rasterio
from rasterio.mask import mask
import numpy as np
from rscommons import Logger, ProgressBar, dotenv, VectorBase
from rscommons.shapefile import _rough_convert_metres_to_raster_units
from rscommons.shapefile import get_utm_zone_epsg
from rscommons.shapefile import get_transform_from_epsg
from rscommons.database import load_geometries, write_attributes_NEW, get_db_srs
from rscommons.raster_buffer_stats import raster_buffer_stats2
from rscommons.classes.vector_classes import get_shp_or_gpkg

Path = str


def reach_geometry_NEW(flow_lines: Path, dem_path: Path, buffer_distance: float, epsg: str):

    log = Logger('Reach Geometry')

    # Buffer the start and end point of each reach
    line_start_polygons = {}
    line_end_polygons = {}
    reaches = {}
    with get_shp_or_gpkg(flow_lines) as lyr:

        vector_buffer = lyr.rough_convert_metres_to_vector_units(buffer_distance)
        _srs, transform_to_raster = lyr.get_transform_from_raster(dem_path)
        _srs, transform_to_output = lyr.get_transform_from_epsg(epsg)

        for feature, _counter, _progbar in lyr.iterate_features("Processing reaches"):
            reach_id = feature.GetFID()
            geom = feature.GetGeometryRef()

            # Calculate the reach length in the output spatial reference
            if transform_to_output is not None:
                geom.Transform(transform_to_output)

            reaches[reach_id] = {'iGeo_Len': geom.Length(), 'iGeo_Slope': 0.0, 'iGeo_ElMin': None, 'IGeo_ElMax': None}

            # Buffer the ends of the reach polyline in the raster spatial reference
            if transform_to_raster is not None:
                geom.Transform(transform_to_raster)

            line_start_polygons[reach_id] = Point(VectorBase.ogr2shapely(geom, transform_to_raster).coords[0]).buffer(vector_buffer)
            line_end_polygons[reach_id] = Point(VectorBase.ogr2shapely(geom, transform_to_raster).coords[-1]).buffer(vector_buffer)

    # Retrieve the mean elevation of start and end of point
    line_start_elevations = raster_buffer_stats2(line_start_polygons, dem_path)
    line_end_elevations = raster_buffer_stats2(line_end_polygons, dem_path)

    for reach_id, data in reaches.items():
        if reach_id in line_start_elevations and reach_id in line_end_elevations:
            sta_data = line_start_elevations[reach_id]
            end_data = line_end_elevations[reach_id]

            data['iGeo_ElMax'] = _max_ignore_none(sta_data['Maximum'], end_data['Maximum'])
            data['iGeo_ElMin'] = _min_ignore_none(sta_data['Minimum'], end_data['Minimum'])

            if sta_data['Mean'] is not None and end_data['Mean'] is not None and sta_data['Mean'] != end_data['Mean']:
                data['iGeo_Slope'] = abs(sta_data['Mean'] - end_data['Mean']) / data['iGeo_Len']
        else:
            log.warning('{:,} features skipped because one or both ends of polyline not on DEM raster'.format(dem_path))

    write_attributes_NEW(os.path.dirname(flow_lines), reaches, ['iGeo_Len', 'iGeo_ElMax', 'iGeo_ElMin', 'iGeo_Slope'])


def _max_ignore_none(val1, val2):

    if val1 is not None:
        if val2 is not None:
            return max(val1, val2)
        else:
            return val1
    else:
        if val2 is not None:
            return val2
        else:
            return None


def _min_ignore_none(val1, val2):

    if val1 is not None:
        if val2 is not None:
            return min(val1, val2)
        else:
            return val1
    else:
        if val2 is not None:
            return val2
        else:
            return None


# def reach_geometry(database, dem_path, buffer_distance, epsg):
#     """Calculate reach geometry attributes for each reach in a
#     BRAT SQLite database

#     Arguments:
#         database {str} -- Path to BRAT SQLite database
#         dem_path {str} -- Path to DEM Raster
#         buffer_distance {float} -- Distance (meters) to buffer reaches when extracting form the DEM
#     """

#     log = Logger('Reach Geometry')
#     log.info('Database: {}'.format(database))
#     log.info('DEM: {}'.format(dem_path))
#     log.info('Buffer distance: {}m'.format(buffer_distance))

#     dataset = gdal.Open(dem_path)
#     gt = dataset.GetGeoTransform()
#     db_srs = get_db_srs(database)
#     gdalSRS_wkt = dataset.GetProjection()
#     rasterSRS = osr.SpatialReference(wkt=gdalSRS_wkt)
#     # https://github.com/OSGeo/gdal/issues/1546
#     rasterSRS.SetAxisMappingStrategy(db_srs.GetAxisMappingStrategy())

#     geometries = load_geometries(database, rasterSRS)
#     log.info('{:,} geometries loaded'.format(len(geometries)))

#     results = calculate_reach_geometry(geometries, dem_path, rasterSRS, buffer_distance)
#     log.info('{:,} reach attributes calculated.'.format(len(results)))

#     if len(geometries) != len(results):
#         log.warning('{:,} features skipped because one or both ends of polyline not on DEM raster'.format(
#             len(geometries) - len(results)))

#     write_attributes(database, results, [slopefld, minElfld, maxElfld, lenfld])
#     log.info('Process completed successfully.')


# def calculate_reach_geometry(polylines, dem_path, polyline_srs, buffer_distance):
#     """
#     Calculate reach length, slope, drainage area, min and max elevations
#     :param polylines: Dictionary of geometries keyed by ReachID
#     :param dem_path: Absolute path to a DEM raster.
#     :param polyline_srs: What SRS does the polyline use.
#     :param buffer_distance: Distance to buffer reach end points to sample raster
#     :return: None
#     """

#     log = Logger("Calculate Reach Geometries")

#     buff_degrees = _rough_convert_metres_to_raster_units(dem_path, buffer_distance)

#     dataset = gdal.Open(dem_path)
#     gt = dataset.GetGeoTransform()
#     gdalSRS = dataset.GetProjection()
#     rasterSRS = osr.SpatialReference(wkt=gdalSRS)
#     # https://github.com/OSGeo/gdal/issues/1546
#     rasterSRS.SetAxisMappingStrategy(polyline_srs.GetAxisMappingStrategy())

#     xcentre = gt[0] + (dataset.RasterXSize * gt[1]) / 2.0
#     epsg = get_utm_zone_epsg(xcentre)
#     projectedSRS, transform = get_transform_from_epsg(rasterSRS, epsg)

#     results = {}

#     log.info('Starting')
#     with rasterio.open(dem_path) as demsrc:

#         progbar = ProgressBar(len(polylines), 50, "Calculating reach geometries")
#         counter = 0

#         for reachid, polyline in polylines.items():
#             counter += 1
#             progbar.update(counter)

#             ogr_polyline = VectorBase.shapely2ogr(polyline, transform)
#             length = ogr_polyline.Length()

#             try:
#                 elev1 = mean_raster_value(demsrc, polyline.coords[0], buff_degrees)
#                 elev2 = mean_raster_value(demsrc, polyline.coords[-1], buff_degrees)
#                 if elev1 and elev2:
#                     slope = abs(elev1 - elev2) / length if elev1 != elev2 else 0.0
#                     results[reachid] = {
#                         slopefld: slope,
#                         maxElfld: max(elev2, elev1),
#                         minElfld: min(elev2, elev1),
#                         lenfld: length
#                     }
#             except Exception as ex:
#                 log.warning('Error obtaining raster values for ReachID {}'.format(reachid))
#                 log.warning(ex)

#         progbar.finish()

#     log.info('Complete')
#     return results


# def mean_raster_value(raster, point, distance):
#     """
#     Sample a raster with a circular buffer from a point
#     :param raster: Rasterio raster object
#     :param point: Shapely point at centre of location to sample
#     :param distance: Distance to buffer point
#     :return: Average raster value within the circular buffer
#     """

#     polygon = Point(point).buffer(distance)
#     raw_raster = mask(raster, [polygon], crop=True)[0]
#     mask_raster = np.ma.masked_values(raw_raster, raster.nodata)
#     return mask_raster.mean() if not mask_raster.mask.all() else None


# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument('network', help='Output network ShapeFile path', type=argparse.FileType('r'))
#     parser.add_argument('dem', help='DEM raster', type=argparse.FileType('r'))
#     parser.add_argument('--buffer', help='Buffer distance in metres for sampling rasters', default=100)
#     parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)
#     args = dotenv.parse_args_env(parser)

#     # Initiate the log file
#     logg = Logger('Reach Geometry')
#     logfile = os.path.join(os.path.dirname(args.network.name), 'reach_geometry.log')
#     logg.setup(logPath=logfile, verbose=args.verbose)

#     try:
#         reach_geometry(args.network.name, args.dem.name, args.buffer, 4326)

#     except Exception as e:
#         logg.error(e)
#         traceback.print_exc(file=sys.stdout)
#         sys.exit(1)

#     sys.exit(0)


# if __name__ == '__main__':
#     main()
