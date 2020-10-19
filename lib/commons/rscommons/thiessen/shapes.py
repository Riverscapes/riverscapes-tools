import math
import json
import os
import ogr
import numpy as np
from shapely.wkb import loads as wkbload
from shapely.geometry import shape, mapping, Point, MultiPoint, LineString, MultiLineString, GeometryCollection, Polygon, MultiPolygon

from rscommons import Logger, ProgressBar
from rscommons.shapefile import get_transform_from_epsg


class RiverPoint:

    def __init__(self, pt, interior=False, side=None, island=None):
        self.point = pt
        self.side = side
        self.interior = interior
        self.island = island


def get_riverpoints(inpath, epsg, attribute_filter=None):
    """[summary]

    Args:
        inpath ([type]): Path to a ShapeFile
        epsg ([type]):  Desired output spatial reference
        attribute_filter ([type], optional): [description]. Defaults to None.

    Returns:
        [type]: List of RiverPoint objects
    """

    log = Logger('Shapefile')
    points = []

    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.Open(inpath, 0)
    layer = data_source.GetLayer()
    in_spatial_ref = layer.GetSpatialRef()

    _out_spatial_ref, transform = get_transform_from_epsg(in_spatial_ref, epsg)

    progbar = ProgressBar(layer.GetFeatureCount(), 50, "Getting points for use in Thiessen")
    counter = 0
    for feature in layer:
        counter += 1
        progbar.update(counter)

        new_geom = feature.GetGeometryRef()

        if new_geom is None:
            progbar.erase()  # get around the progressbar
            log.warning('Feature with FID={} has no geometry. Skipping'.format(feature.GetFID()))
            continue

        new_geom.Transform(transform)
        new_shape = wkbload(new_geom.ExportToWkb())

        if new_shape.type == 'Polygon':
            new_shape = MultiPolygon([new_shape])

        for poly in new_shape:
            # Exterior is the shell and there is only ever 1
            for pt in list(poly.exterior.coords):
                points.append(RiverPoint(pt, interior=False))

            # Now we consider interiors. NB: Interiors are only qualifying islands in this case
            for idx, island in enumerate(poly.interiors):
                for pt in list(island.coords):
                    points.append(RiverPoint(pt, interior=True, island=idx))

    progbar.finish()
    data_source = None

    return points
