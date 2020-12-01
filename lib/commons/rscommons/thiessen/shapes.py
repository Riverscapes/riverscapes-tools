import math
import json
import os
from osgeo import ogr
import numpy as np
from shapely.wkb import loads as wkbload
from shapely.geometry import shape, mapping, Point, MultiPoint, LineString, MultiLineString, GeometryCollection, Polygon, MultiPolygon
from shapely.ops import unary_union
from rscommons import LoopTimer

from rscommons import Logger, ProgressBar, get_shp_or_gpkg
from rscommons.shapefile import get_transform_from_epsg

from typing import List, Dict, Any
Path = str
Transform = ogr.osr.CoordinateTransformation


class RiverPoint:

    def __init__(self, pt, interior=False, side=None, island=None, properties: Dict[str, Any] = None):
        self.point = pt
        self.side = side
        self.interior = interior
        self.island = island
        self.properties = properties


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


def midpoints(in_lines):

    driver = driver_shp = ogr.GetDriverByName("ESRI Shapefile")
    data_in = driver_shp.Open(in_lines, 0)
    lyr = data_in.GetLayer()
    out_points = []
    for feat in lyr:
        geom = feat.GetGeometryRef()
        line = wkbload(geom.ExportToWkb())
        out_points.append(RiverPoint(line.interpolate(0.5, True)))
        feat = None

    return out_points


def centerline_points(in_lines: Path, distance: float = 0.0, transform: Transform = None) -> Dict[int, List[RiverPoint]]:
    """Generates points along each line feature at specified distances from the end as well as quarter and halfway

    Args:
        in_lines (Path): path of shapefile with features
        distance (float, optional): distance from ends to generate points. Defaults to 0.0.
        transform (Transform, optional): coordinate transformation. Defaults to None.

    Returns:
        [type]: [description]
    """

    with get_shp_or_gpkg(in_lines) as in_lyr:
        out_group = {}

        for feat, _counter, _progbar in in_lyr.iterate_features(""):
            fid = feat.GetFID()
            geom = feat.GetGeometryRef()
            if transform:
                geom.Transform(transform)
            line = wkbload(geom.ExportToWkb())
            out_points = []
            # Attach the FID in case we need it later
            props = {'fid': fid}

            out_points.append(RiverPoint(line.interpolate(distance), properties=props))
            out_points.append(RiverPoint(line.interpolate(0.5, True), properties=props))
            out_points.append(RiverPoint(line.interpolate(-distance), properties=props))

            if line.project(line.interpolate(0.25, True)) > distance:
                out_points.append(RiverPoint(line.interpolate(0.25, True), properties=props))
                out_points.append(RiverPoint(line.interpolate(-0.25, True), properties=props))

            out_group[int(fid)] = out_points
            feat = None
        return out_group


def centerline_vertex_between_distance(in_lines, distance=0.0):
    driver = driver_shp = ogr.GetDriverByName("ESRI Shapefile")
    data_in = driver_shp.Open(in_lines, 0)
    lyr = data_in.GetLayer()
    out_group = []
    for feat in lyr:
        geom = feat.GetGeometryRef()
        line = wkbload(geom.ExportToWkb())
        out_points = []
        out_points.append(RiverPoint(line.interpolate(distance)))
        out_points.append(RiverPoint(line.interpolate(-distance)))
        max_distance = line.length - distance
        for vertex in list(line.coords):
            test_dist = line.project(Point(vertex))
            if test_dist > distance and test_dist < max_distance:
                out_points.append(RiverPoint(Point(vertex)))
        out_group.append(out_points)
        feat = None
    return out_group


def load_geoms(in_lines):
    driver = driver_shp = ogr.GetDriverByName("ESRI Shapefile")
    data_in = driver_shp.Open(in_lines, 0)
    lyr = data_in.GetLayer()
    out = []
    for feat in lyr:
        geom = feat.GetGeometryRef()
        out.append(wkbload(geom.ExportToWkb()))

    return out


def clip_polygons(clip_poly, polys):

    progbar = ProgressBar(len(polys), 50, "Clipping Polygons...")
    counter = 0
    progbar.update(counter)
    out_polys = {}
    for key, poly in polys.items():
        counter += 1
        progbar.update(counter)
        out_polys[key] = clip_poly.intersection(poly.buffer(0))

    progbar.finish()
    return out_polys


def dissolve_by_intersection(lines, polys):

    progbar = ProgressBar(len(polys), 50, "Dissolving Polygons...")
    counter = 0
    progbar.update(counter)
    dissolved_polys = []
    for line in lines:
        counter += 1
        progbar.update(counter)
        intersected = [p for p in polys if line.intersects(p)]
        dissolved_polys.append(unary_union(intersected))

    return dissolved_polys


def dissolve_by_points(groups, polys):

    progbar = ProgressBar(len(groups), 50, "Dissolving Polygons...")
    counter = 0
    progbar.update(counter)
    dissolved_polys = {}

    # Original method
    # for key, group in groups.items():
    #     counter += 1
    #     progbar.update(counter)
    #     intersected = [p for p in polys if any([p.contains(pt.point) for pt in group])]
    #     dissolved_polys[key] = unary_union(intersected)

    # This method gradulally speeds up processing by removing polygons from the list.
    for key, group in groups.items():
        counter += 1
        progbar.update(counter)
        intersected = []
        indexes = []
        for i, p in enumerate(polys):
            if any([p.contains(pt.point) for pt in group]):
                intersected.append(p)
                indexes.append(i)
        dissolved_polys[key] = unary_union(intersected)  # MultiPolygon(intersected) #intersected
        polys = [p for i, p in enumerate(polys) if i not in indexes]

    progbar.finish()
    return dissolved_polys
