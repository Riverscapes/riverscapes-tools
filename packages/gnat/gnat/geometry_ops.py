# Utilities to support OGR geometry operations.
#
# Usage:    Tools should only use geometry objects.
#
# Author:   Kelly Whitehead
#
# Date: August 26 2022

from collections import Counter

from osgeo import ogr


def reduce_precision(geom_multiline, rounding_precision=13):
    """ reduce the precision of vertex coordinates in line features

    Args:
        geom_multiline (_type_): _description_
        rounding_precision (int, optional): _description_. Defaults to 14.

    Returns:
        _type_: _description_
    """
    geom = ogr.Geometry(ogr.wkbMultiLineString)
    for i in range(0, geom_multiline.GetGeometryCount()):
        out_line = ogr.Geometry(ogr.wkbLineString)
        g = geom_multiline.GetGeometryRef(i)
        for i2 in range(0, g.GetPointCount()):
            pt = g.GetPoint(i2)
            out_line.AddPoint(round(pt[0], rounding_precision), round(pt[1], rounding_precision))
        clean_line = out_line.MakeValid()
        if clean_line.GetGeometryName() == 'LINESTRING':
            geom.AddGeometry(clean_line)

    geom.FlattenTo2D()
    out_geom = geom.MakeValid()

    return out_geom


def get_endpoints(geom):
    """_summary_

    Args:
        geom (_type_): _description_

    Returns:
        _type_: _description_
    """

    coords = []
    geoms = ogr.ForceToMultiLineString(geom)
    for geom in geoms:
        if geom.GetPointCount() == 0:
            continue
        for pt in [geom.GetPoint(0), geom.GetPoint(geom.GetPointCount() - 1)]:
            coords.append(pt)
    counts = Counter(coords)
    endpoints = [pt for pt, count in counts.items() if count == 1]

    return endpoints
