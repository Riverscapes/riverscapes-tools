

from functools import cached_property
from collections import Counter

import numpy as np
from shapely.geometry import Point
from rasterio.mask import mask
from osgeo import ogr

from gnat.geometry_ops import reduce_precision


class GNATWindow:
    """ class to support GNAT metric calculations using polygon windows
    """

    lyr_segments = None
    buffer_raster_clip = None

    def __init__(self, window_size, segment_distance, geom_flowline, geom_centerline, level_path, buffer_elevation):

        self.geom_window = self.generate_window(window_size, level_path, segment_distance)
        self.geom_flowline_level_path = geom_flowline
        self.geom_centerline_level_path = geom_centerline
        self.window_size = window_size
        self.buffer_elevation = buffer_elevation
        self.level_path = level_path

    def generate_window(self, window, level_path, segment_dist, buffer=0):
        """_summary_

        Args:
            lyr (_type_): _description_
            window (_type_): _description_
            dem (_type_): _description_
        """
        min_dist = segment_dist - 0.5 * window
        max_dist = segment_dist + 0.5 * window
        sql = f'LevelPathI = {level_path} AND seg_distance >= {min_dist} AND seg_distance <={max_dist}'
        geom_window_sections = ogr.Geometry(ogr.wkbMultiPolygon)
        for feat, *_ in self.lyr_segments.iterate_features(attribute_filter=sql):
            geom = feat.GetGeometryRef()
            if geom.GetGeometryName() in ['MULTIPOLYGON', 'GEOMETRYCOLLECTION']:
                for i in range(0, geom.GetGeometryCount()):
                    g = geom.GetGeometryRef(i)
                    if g.GetGeometryName() == 'POLYGON':
                        geom_window_sections.AddGeometry(g)
            else:
                geom_window_sections.AddGeometry(geom)
        geom_window = geom_window_sections.Buffer(buffer)  # ogr.ForceToPolygon(geom_window_sections)
        return geom_window

    @cached_property
    def gnat_flowline(self):
        """_summary_
        """
        gnat_flowline = GNATLine(self.geom_flowline_level_path, self.geom_window, self.buffer_elevation)
        return gnat_flowline

    @cached_property
    def gnat_centerline(self):
        """_summary_
        """
        gnat_centerline = GNATLine(self.geom_centerline_level_path, self.geom_window, self.buffer_elevation)
        return gnat_centerline

    def stream_gradient(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        if self.gnat_flowline.max_elevation is None or self.gnat_flowline.min_elevation is None:
            return None

        gradient = (self.gnat_flowline.max_elevation - self.gnat_flowline.min_elevation) / self.gnat_flowline.length
        return gradient

    def valley_gradient(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        if self.gnat_flowline.max_elevation is None or self.gnat_flowline.min_elevation is None:
            return None
        gradient = (self.gnat_flowline.max_elevation - self.gnat_flowline.min_elevation) / self.gnat_centerline.length
        return gradient


class GNATLine():
    """_summary_

    Returns:
        _type_: _description_
    """
    transform = None
    src_raster = None

    def __init__(self, geom_line, geom_window, buffer_elevation) -> None:

        self.geom_line = self.clip_line(geom_line, geom_window)
        self.buffer_elevation = buffer_elevation

    def clip_line(self, geom_line, geom_window):
        """_summary_

        Args:
            geom_line (_type_): _description_

        Returns:
            _type_: _description_
        """
        geom_clipped = geom_window.Intersection(geom_line)
        if geom_clipped.GetGeometryName() == "MULTILINESTRING":
            geom_clipped = reduce_precision(geom_clipped, 6)
            geom_clipped = ogr.ForceToLineString(geom_clipped)

        return geom_clipped

    @ cached_property
    def length(self):
        """_summary_

        Args:
            geom (_type_): _description_

        Returns:
            _type_: _description_
        """
        geom = self.geom_line.Clone()
        geom.Transform(self.transform)
        stream_length = geom.Length()
        return stream_length

    @ cached_property
    def elevations(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        coords = []
        geoms = ogr.ForceToMultiLineString(self.geom_line)
        for geom in geoms:
            for pt in [geom.GetPoint(0), geom.GetPoint(geom.GetPointCount() - 1)]:
                coords.append(pt)
        counts = Counter(coords)
        endpoints = [pt for pt, count in counts.items() if count == 1]
        elevations = [None, None]
        if len(endpoints) == 2:
            elevations = []
            for pt in endpoints:
                point = Point(pt)
                polygon = point.buffer(self.buffer_elevation)  # BRAT uses 100m here for all stream sizes?
                raw_raster, _out_transform = mask(self.src_raster, [polygon], crop=True)
                mask_raster = np.ma.masked_values(raw_raster, self.src_raster.nodata)
                value = float(mask_raster.min())  # BRAT uses mean here
                elevations.append(value)
            elevations.sort()

        return elevations[0], elevations[1]

    @ cached_property
    def min_elevation(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        return self.elevations[0]

    @ cached_property
    def max_elevation(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        return self.elevations[1]
