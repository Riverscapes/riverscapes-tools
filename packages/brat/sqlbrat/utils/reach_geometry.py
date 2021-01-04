""" Calculates several properties of each network polyline:
    Slope, length, min and max elevation.

    Philip Bailey
    23 May 2019
"""
import os
from osgeo import gdal
from shapely.geometry import Point
from rscommons import Logger, VectorBase
from rscommons.raster_buffer_stats import raster_buffer_stats2
from rscommons.classes.vector_classes import get_shp_or_gpkg
from rscommons.database import write_db_attributes
from rscommons.classes.vector_base import get_utm_zone_epsg

Path = str


def reach_geometry(flow_lines: Path, dem_path: Path, buffer_distance: float):
    """ Calculate reach geometry BRAT attributes

    Args:
        flow_lines (Path): [description]
        dem_path (Path): [description]
        buffer_distance (float): [description]
    """

    log = Logger('Reach Geometry')

    # Determine the best projected coordinate system based on the raster
    dataset = gdal.Open(dem_path)
    geo_transform = dataset.GetGeoTransform()
    xcentre = geo_transform[0] + (dataset.RasterXSize * geo_transform[1]) / 2.0
    epsg = get_utm_zone_epsg(xcentre)

    # Buffer the start and end point of each reach
    line_start_polygons = {}
    line_end_polygons = {}
    reaches = {}
    with get_shp_or_gpkg(flow_lines) as lyr:

        # Transformations from original flow line features to metric EPSG, and to raster spatial reference
        _srs, transform_to_metres = VectorBase.get_transform_from_epsg(lyr.spatial_ref, epsg)
        _srs, transform_to_raster = VectorBase.get_transform_from_raster(lyr.spatial_ref, dem_path)

        # Buffer distance converted to the units of the raster spatial reference
        vector_buffer = VectorBase.rough_convert_metres_to_raster_units(dem_path, buffer_distance)

        for feature, _counter, _progbar in lyr.iterate_features("Processing reaches"):
            reach_id = feature.GetFID()
            geom = feature.GetGeometryRef()
            geom_clone = geom.Clone()

            # Calculate the reach length in the output spatial reference
            if transform_to_metres is not None:
                geom.Transform(transform_to_metres)

            reaches[reach_id] = {'iGeo_Len': geom.Length(), 'iGeo_Slope': 0.0, 'iGeo_ElMin': None, 'IGeo_ElMax': None}

            if transform_to_raster is not None:
                geom_clone.Transform(transform_to_raster)

            # Buffer the ends of the reach polyline in the raster spatial reference
            line_start_polygons[reach_id] = Point(VectorBase.ogr2shapely(geom_clone, transform_to_raster).coords[0]).buffer(vector_buffer)
            line_end_polygons[reach_id] = Point(VectorBase.ogr2shapely(geom_clone, transform_to_raster).coords[-1]).buffer(vector_buffer)

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

    write_db_attributes(os.path.dirname(flow_lines), reaches, ['iGeo_Len', 'iGeo_ElMax', 'iGeo_ElMin', 'iGeo_Slope'])


def _max_ignore_none(val1: float, val2: float) -> float:

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


def _min_ignore_none(val1: float, val2: float) -> float:

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
