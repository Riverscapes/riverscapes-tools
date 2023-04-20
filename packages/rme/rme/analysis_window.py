""" RME Classes

    Purpose:    Classes to work with analysis windows to generate metrics and measurements
    Author:     Kelly Whitehead
    Date:       August 2022
"""

import math
from functools import cached_property

import numpy as np
from shapely.geometry import Point
from rasterio.mask import mask
from osgeo import ogr

from rscommons.geometry_ops import reduce_precision, get_endpoints


class AnalysisWindow:
    """ class to support Riverscapes Metric calculations using polygon windows"""

    lyr_segments = None
    buffer_raster_clip = None

    def __init__(self, window_size: float, segment_dist: float, geom_flowline: ogr.Geometry, geom_centerline: ogr.Geometry, level_path: str, buffer_elevation: float):

        self.level_path = level_path
        self.window_size = window_size
        self.segment_distance = segment_dist
        self.buffer_elevation = buffer_elevation
        self.geom_window = self.generate_window()
        self.geom_flowline_level_path = geom_flowline
        self.geom_centerline_level_path = geom_centerline

    def generate_window(self, buffer=0):
        """generate the polygon geometry of the moving window for a level path"""

        min_dist = self.segment_distance - 0.5 * self.window_size
        max_dist = self.segment_distance + 0.5 * self.window_size
        sql = f'LevelPathI = {self.level_path} AND seg_distance >= {min_dist} AND seg_distance <={max_dist}'
        geom_window_sections = ogr.Geometry(ogr.wkbMultiPolygon)
        for feat, *_ in self.lyr_segments.iterate_features(attribute_filter=sql):
            geom = feat.GetGeometryRef()
            if geom.GetGeometryName() in ['MULTIPOLYGON', 'GEOMETRYCOLLECTION']:
                for i in range(0, geom.GetGeometryCount()):
                    geo = geom.GetGeometryRef(i)
                    if geo.GetGeometryName() == 'POLYGON':
                        geom_window_sections.AddGeometry(geo)
            else:
                geom_window_sections.AddGeometry(geom)
        geom_window = geom_window_sections.Buffer(buffer)
        return geom_window

    @cached_property
    def window_flowline(self):
        """create a cached flowline instance of WindowedLine"""
        return AnalysisLine(self.geom_flowline_level_path, self.geom_window, self.buffer_elevation)

    @cached_property
    def window_centerline(self):
        """create a cached centerline instance of WindowedLine"""
        return AnalysisLine(self.geom_centerline_level_path, self.geom_window, self.buffer_elevation)

    def stream_gradient(self) -> float:
        """return gradient metric for flowline"""
        if self.window_flowline.max_elevation is None or self.window_flowline.min_elevation is None:
            return None
        gradient = (self.window_flowline.max_elevation - self.window_flowline.min_elevation) / self.window_flowline.length
        return gradient

    def valley_gradient(self):
        """return gradient metric for valley centerline"""
        if self.window_flowline.max_elevation is None or self.window_flowline.min_elevation is None:
            return None
        gradient = (self.window_flowline.max_elevation - self.window_flowline.min_elevation) / self.window_centerline.length
        return gradient


class AnalysisLine():
    """ generates Riverscapes Metrics for lines"""

    transform = None
    src_raster = None

    def __init__(self, geom_line: ogr.Geometry, geom_window, buffer_elevation=None) -> None:
        """ clip geometry and create new instance of AnalysisLine

        Args:
            geom_line (ogr.Geometry): unclipped line
            geom_window (ogr.Geometry): polygon or multipolygon window to clip the line
            buffer_elevation (float, optional): buffer distance to obtain elevation values. Defaults to None
        """

        self.geom_line = self.clip_line(geom_line, geom_window)
        self.buffer_elevation = buffer_elevation

    def clip_line(self, geom_line: ogr.Geometry, geom_window: ogr.Geometry):
        """clip line to the window"""

        geom_clipped = geom_window.Intersection(geom_line)
        if geom_clipped.GetGeometryName() == "MULTILINESTRING":
            geom_clipped = reduce_precision(geom_clipped, 6)
            geom_clipped = ogr.ForceToLineString(geom_clipped)

        return geom_clipped

    @ cached_property
    def length(self):
        """return the transformed length of line"""
        geom = self.geom_line.Clone()
        geom.Transform(self.transform)
        stream_length = geom.Length()
        return stream_length

    @ cached_property
    def elevations(self):
        """return a list of elevations for the endpoints"""

        endpoints = self.endpoints
        elevations = [None, None]
        if len(endpoints) == 2:
            elevations = []
            for pnt in endpoints:
                point = Point(pnt)
                polygon = point.buffer(self.buffer_elevation)  # BRAT uses 100m here for all stream sizes?
                raw_raster, _out_transform = mask(self.src_raster, [polygon], crop=True)
                mask_raster = np.ma.masked_values(raw_raster, self.src_raster.nodata)
                value = float(mask_raster.min())  # BRAT uses mean here
                elevations.append(value)
            elevations.sort()

        return elevations[0], elevations[1]

    @ cached_property
    def min_elevation(self):
        """return the min elevation of the end of the line"""

        return self.elevations[0]

    @ cached_property
    def max_elevation(self):
        """return the max elevation of the end of the line"""

        return self.elevations[1]

    def gradient(self):
        """return the gradient of the line"""

        if self.max_elevation is None or self.min_elevation is None:
            return None
        gradient = (self.max_elevation - self.min_elevation) / self.length
        return gradient

    @cached_property
    def endpoints(self):
        """ return the endpoints of the line """

        return get_endpoints(self.geom_line)

    @cached_property
    def endpoint_distance(self):
        """return the straight line distance of the endpoints"""

        endpoints = self.endpoints
        if len(endpoints) != 2:
            return None
        geom_line = ogr.Geometry(ogr.wkbLineString)
        geom_line.AddPoint(*endpoints[0])
        geom_line.AddPoint(*endpoints[1])
        geom_line.Transform(self.transform)
        distance = geom_line.Length()

        return distance

    def sinuosity(self):
        """return the sinuosity"""

        if self.endpoint_distance is None:
            return None
        sinuosity = self.length / self.endpoint_distance
        return sinuosity

    def azimuth(self):
        """return direction of straight line in degrees"""

        endpoints = self.endpoints
        if len(endpoints) != 2:
            return None
        pnt1, pnt2 = endpoints
        azimuth = math.atan2(pnt2[1] - pnt1[1], pnt2[0] - pnt1[0])
        degrees = (90 - math.degrees(azimuth)) % 360
        return degrees
