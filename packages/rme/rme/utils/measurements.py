import numpy as np
from osgeo import ogr
import rasterio
from rasterio.mask import mask
from shapely.geometry import Point, LineString
from rscommons.geometry_ops import reduce_precision, get_endpoints
from rscommons import VectorBase


def get_segment_measurements(geom_line: ogr.Geometry, src_raster: rasterio.DatasetReader, geom_window: ogr.Geometry, buffer: float, transform) -> tuple:
    """ return length of segment and endpoint elevations of a line

    Args:
        geom_line (ogr.Geometry): unclipped line geometry
        raster (rasterio.DatasetReader): open dataset reader of elevation raster
        geom_window (ogr.Geometry): analysis window for clipping line
        buffer (float): buffer of endpoints to find min elevation
        transform(CoordinateTransform): transform used to obtain length
    Returns:
        float: stream length
        float: maximum elevation
        float: minimum elevation
    """

    geom_clipped = geom_window.Intersection(geom_line)
    if geom_clipped.GetGeometryName() == "MULTILINESTRING":
        geom_clipped = reduce_precision(geom_clipped, 6)
        geom_clipped = ogr.ForceToLineString(geom_clipped)
    endpoints = get_endpoints(geom_clipped)
    elevations = [None, None]
    straight_length = None
    if len(endpoints) >= 2:
        elevations = []
        for pnt in endpoints:
            point = Point(pnt)
            # BRAT uses 100m here for all stream sizes?
            polygon = point.buffer(buffer)
            raw_raster, _out_transform = mask(src_raster, [polygon], crop=True)
            mask_raster = np.ma.masked_values(raw_raster, src_raster.nodata)
            value = float(mask_raster.min())  # BRAT uses mean here
            elevations.append(value)
    combined = zip(elevations, endpoints)
    sorted_combined = sorted(combined)
    sorted_elevs, sorted_epts = zip(*sorted_combined)
    # elevations.sort()
    straight = LineString([sorted_epts[0], sorted_epts[-1]])
    straight_ogr = VectorBase.shapely2ogr(straight)
    straight_ogr.Transform(transform)
    straight_length = straight_ogr.Length()
    geom_clipped.Transform(transform)
    stream_length = geom_clipped.Length()

    return stream_length, straight_length, sorted_elevs[0], sorted_elevs[-1]
