

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
    transform = None
    src_raster = None
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
        geom_window = geom_window_sections.Buffer(self.buffer_raster_clip)  # ogr.ForceToPolygon(geom_window_sections)

        return geom_window

    def clip_line(self, geom_line):
        """_summary_

        Args:
            geom_line (_type_): _description_

        Returns:
            _type_: _description_
        """
        geom_clipped = self.geom_window.Intersection(geom_line)
        if geom_clipped.GetGeometryName() == "MULTILINESTRING":
            geom_clipped = reduce_precision(geom_clipped, 6)
            geom_clipped = ogr.ForceToLineString(geom_clipped)

        return geom_clipped

    def calculate_length(self, geom):
        """_summary_

        Args:
            geom (_type_): _description_

        Returns:
            _type_: _description_
        """
        geom = self.geom_flowline.Clone()
        geom.Transform(self.transform)
        stream_length = geom.Length()
        return stream_length

    @cached_property
    def geom_flowline(self):
        """_summary_
        """
        return self.clip_line(self.geom_flowline_level_path)

    @cached_property
    def geom_centerline(self):
        """_summary_
        """
        return self.clip_line(self.geom_centerline_level_path)

    @cached_property
    def flowline_length(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        return self.calculate_length(self.geom_flowline)

    @cached_property
    def centerline_length(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        return self.calculate_length(self.geom_centerline)

    @ cached_property
    def elevations(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        coords = []
        geoms = ogr.ForceToMultiLineString(self.geom_flowline)
        for geom in geoms:
            for pt in [geom.GetPoint(0), geom.GetPoint(geom.GetPointCount() - 1)]:
                coords.append(pt)
        counts = Counter(coords)
        endpoints = [pt for pt, count in counts.items() if count == 1]
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
