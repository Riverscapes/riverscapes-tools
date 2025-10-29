import os
import rasterio
from rasterio.mask import mask
from osgeo import ogr, gdal
from shapely.ops import linemerge
from shapely.geometry import Point, box
import numpy as np

from rscommons import GeopackageLayer, VectorBase
from rsxml import Logger
from rscommons.geometry_ops import reduce_precision, get_endpoints
from rscommons.classes.vector_base import get_utm_zone_epsg
from rscommons.database import SQLiteCon, write_db_attributes, write_db_dgo_attributes
from rscommons.raster_buffer_stats import raster_buffer_stats2


default_field_names = {'Length': 'Length_m', 'Gradient': 'Slope', 'MinElevation': 'ElevMin', 'MaxElevation': 'ElevMax'}
default_dgo_field_names = {'Length': 'Length_m', 'Gradient': 'Slope', 'MinElevation': 'ElevMin', 'MaxElevation': 'ElevMax', 'DrainArea': 'DrainArea'}


def reach_geometry(gpk_path: str, dem_path: str, buffer_distance: float, field_names=default_field_names):

    log = Logger('Reach Geometry')
    log.info('Calculating reach geometry attributes')

    # Determine the best projected coordinate system based on the raster
    dataset = gdal.Open(dem_path)
    geo_transform = dataset.GetGeoTransform()
    xcentre = geo_transform[0] + (dataset.RasterXSize * geo_transform[1]) / 2.0
    epsg = get_utm_zone_epsg(xcentre)

    with rasterio.open(dem_path) as raster:
        bounds = raster.bounds
        extent = box(*bounds)

    # Buffer the start and end point of each reach
    line_start_polygons = {}
    line_end_polygons = {}
    reaches = {}
    with GeopackageLayer(gpk_path, 'ReachGeometry') as lyr:

        # Transformations from original flow line features to metric EPSG, and to raster spatial reference
        _srs, transform_to_metres = VectorBase.get_transform_from_epsg(lyr.spatial_ref, epsg)
        _srs, transform_to_raster = VectorBase.get_transform_from_raster(lyr.spatial_ref, dem_path)

        # Buffer distance converted to the units of the raster spatial reference
        vector_buffer = VectorBase.rough_convert_metres_to_raster_units(dem_path, buffer_distance)

        for feature, _counter, _progbar in lyr.iterate_features("Processing reaches"):
            reach_id = feature.GetFID()
            geom = feature.GetGeometryRef()
            if geom is None or geom.IsEmpty():
                log.warning('Reach ID {} has no geometry, skipping'.format(reach_id))
                continue
            if geom.GetGeometryType() in (ogr.wkbMultiLineString, ogr.wkbMultiLineStringM, ogr.wkbMultiLineStringZM):
                # If the geometry is a MultiLineString, merge it into a single LineString
                geom = linemerge([VectorBase.ogr2shapely(g) for g in geom])
                geom = VectorBase.shapely2ogr(geom)
            if geom.GetGeometryName() != 'LINESTRING':
                log.warning('Reach ID {} has geometry type {}, expected LineString, skipping'.format(reach_id, geom.GetGeometryType()))
                continue

            geom_clone = geom.Clone()

            # Calculate the reach length in the output spatial reference
            if transform_to_metres is not None:
                geom.Transform(transform_to_metres)

            reaches[reach_id] = {field_names['Length']: geom.Length(), field_names['Gradient']: 0.0, field_names['MinElevation']: None, field_names['MaxElevation']: None}

            if transform_to_raster is not None:
                geom_clone.Transform(transform_to_raster)

            # Buffer the ends of the reach polyline in the raster spatial reference
            pt_start = Point(VectorBase.ogr2shapely(geom_clone, transform_to_raster).coords[0])
            pt_end = Point(VectorBase.ogr2shapely(geom_clone, transform_to_raster).coords[-1])
            if extent.contains(pt_start) and extent.contains(pt_end):
                line_start_polygons[reach_id] = pt_start.buffer(vector_buffer)
                line_end_polygons[reach_id] = pt_end.buffer(vector_buffer)

    # Retrieve the mean elevation of start and end of point
    line_start_elevations = raster_buffer_stats2(line_start_polygons, dem_path)
    line_end_elevations = raster_buffer_stats2(line_end_polygons, dem_path)

    for reach_id, data in reaches.items():
        if reach_id in line_start_elevations and reach_id in line_end_elevations:
            sta_data = line_start_elevations[reach_id]
            end_data = line_end_elevations[reach_id]

            data[field_names['MaxElevation']] = _min_ignore_none(sta_data['Minimum'], sta_data['Mean'])
            data[field_names['MinElevation']] = _min_ignore_none(end_data['Minimum'], end_data['Mean'])

            if sta_data['Mean'] is not None and end_data['Mean'] is not None and sta_data['Mean'] != end_data['Mean']:
                data[field_names['Gradient']] = abs(sta_data['Mean'] - end_data['Mean']) / data[field_names['Length']]
        else:
            log.warning('Reach ID {} skipped because one or both ends of polyline not on DEM raster'.format(reach_id))

    write_db_attributes(gpk_path, reaches, [field_names['Length'], field_names['MaxElevation'], field_names['MinElevation'], field_names['Gradient']])


def dgo_geometry(gpk_path: str, dem_path: str, field_names=default_dgo_field_names):
    """Copy hydrology attributes over from the reaches feature class to the DGO and IGO feature classes"""

    log = Logger('Reach Geometry')
    log.info('Calculating DGO geometry attributes')

    dgo_atts = {}

    # convert buffer in m to dataset units
    dem = gdal.Open(dem_path)
    gt = dem.GetGeoTransform()
    buffer_size = gt[1] * 5

    with GeopackageLayer(gpk_path, 'DGOGeometry') as dgo_lyr, \
        GeopackageLayer(gpk_path, 'vwReaches') as reaches_lyr, \
            rasterio.open(dem_path) as dem_src:
        # get transform
        long = dgo_lyr.ogr_layer.GetExtent()[0]
        proj_epsg = get_utm_zone_epsg(long)
        srs, transform = VectorBase.get_transform_from_epsg(dgo_lyr.spatial_ref, proj_epsg)
        for dgo_ftr, *_ in dgo_lyr.iterate_features('Processing DGOs'):
            dgoid = dgo_ftr.GetFID()
            dgo_geom = dgo_ftr.GetGeometryRef()

            # Get the reach attributes for this DGO
            drain_area = [0]
            ftrs = []
            # choose only primary channel
            da = 0
            lp = None
            for reach_ftr, *_ in reaches_lyr.iterate_features(clip_shape=dgo_geom):
                drnarea = reach_ftr.GetField('DrainArea')
                if drnarea is None:
                    continue
                if drnarea > da:
                    lp = reach_ftr.GetField('level_path')
            for reach_ftr, *_ in reaches_lyr.iterate_features(clip_shape=dgo_geom):
                if reach_ftr.GetGeometryRef() is None:
                    continue
                if reach_ftr.GetField('level_path') != lp:
                    continue
                geom_clipped = dgo_geom.Intersection(reach_ftr.GetGeometryRef())
                if geom_clipped.GetGeometryName() == 'MULTILINESTRING':
                    # geom_clipped = reduce_precision(geom_clipped, 6)
                    # geom_clipped = ogr.ForceToLineString(geom_clipped)
                    for geom in geom_clipped:
                        geom_shapely = VectorBase.ogr2shapely(geom)
                        if geom_shapely.geom_type == 'LineString' and geom_shapely.length > 0:
                            ftrs.append(geom_shapely)
                else:
                    geom_shapely = VectorBase.ogr2shapely(geom_clipped)
                    if geom_shapely.geom_type == 'LineString' and geom_shapely.length > 0:
                        ftrs.append(geom_shapely)

                da = reach_ftr.GetField('DrainArea')
                if da is not None:
                    drain_area.append(da)
            if len(ftrs) == 0:
                continue
            else:
                if len(ftrs) > 1:
                    line = linemerge(ftrs)
                else:
                    line = ftrs[0]
            line = VectorBase.shapely2ogr(line)

            endpoints = get_endpoints(line)
            elevations = []
            for pnt in endpoints:
                point = Point(pnt)
                polygon = point.buffer(buffer_size)
                raw_raster, _out_transform = mask(dem_src, [polygon], crop=True)
                mask_raster = np.ma.masked_values(raw_raster, dem_src.nodata)
                value = float(mask_raster.min())
                elevations.append(value)
            elevations.sort()
            # geom_clipped.Transform(transform)
            # stream_length = geom_clipped.Length()
            line.Transform(transform)
            stream_length = line.Length()
            dgo_atts[dgoid] = {field_names['Length']: stream_length,
                               field_names['MaxElevation']: elevations[-1],
                               field_names['MinElevation']: elevations[0],
                               field_names['Gradient']: (elevations[-1] - elevations[0]) / stream_length,
                               field_names['DrainArea']: max(drain_area)}

    write_db_dgo_attributes(gpk_path, dgo_atts, [field_names['Length'], field_names['MaxElevation'], field_names['MinElevation'], field_names['Gradient'], field_names['DrainArea']])


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
