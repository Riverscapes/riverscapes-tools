""" Name:       Geometry Operations

    Purpose:    Utilities to support OGR geometry operations.
    Usage:      Tools should only use geometry objects.
    Author:     Kelly Whitehead
    Date:       August 26 2022
"""

from collections import Counter

from osgeo import ogr


def reduce_precision(geom_multiline: ogr.Geometry, rounding_precision: int = 13) -> ogr.Geometry:
    """ reduce the precision of vertex coordinates in line features

    Args:
        geom_multiline (ogr.Geometry): linestring or multilinestring to reduce precision
        rounding_precision (int, optional): number of decimals to round coodinates. Defaults to 14.

    Returns:
        ogr.Geometry: geometry with reduced precision
    """

    geom_out = ogr.Geometry(ogr.wkbMultiLineString)
    for i in range(0, geom_multiline.GetGeometryCount()):
        out_line = ogr.Geometry(ogr.wkbLineString)
        geom_2 = geom_multiline.GetGeometryRef(i)
        for i_2 in range(0, geom_2.GetPointCount()):
            pnt = geom_2.GetPoint(i_2)
            out_line.AddPoint(round(pnt[0], rounding_precision), round(pnt[1], rounding_precision))
        clean_line = out_line.MakeValid()
        if clean_line.GetGeometryName() == 'LINESTRING':
            geom_out.AddGeometry(clean_line)

    geom_out.FlattenTo2D()
    geom_out = geom_out.MakeValid()

    return geom_out


def get_endpoints(geom) -> list:
    """return a list of endpoints for a linestring or multilinestring

    Args:
        geom (ogr.Geometry): linestring or multilinestring geometry

    Returns:
        list: coords of points
    """

    coords = []
    geoms = ogr.ForceToMultiLineString(geom)
    for geom in geoms:
        if geom.GetPointCount() == 0:
            continue
        for pnt in [geom.GetPoint(0), geom.GetPoint(geom.GetPointCount() - 1)]:
            coords.append(pnt)
    counts = Counter(coords)
    endpoints = [pt for pt, count in counts.items() if count == 1]

    return endpoints


def get_extent_as_geom(geom: ogr.Geometry()) -> ogr.Geometry():
    """return the geometry extent as a rectangluar polygon

    Args:
        geom (ogr.Geometry): input geometry

    Returns:
        ogr.Geometry(): rectangluar extent polgon
    """
    envelope = geom.GetEnvelope()
    geom_envelope = get_rectangle_as_geom(envelope)
    return geom_envelope


def get_rectangle_as_geom(envelope: tuple) -> ogr.Geometry():

    (minX, maxX, minY, maxY) = envelope
    # Create ring
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(minX, minY)
    ring.AddPoint(maxX, minY)
    ring.AddPoint(maxX, maxY)
    ring.AddPoint(minX, maxY)
    ring.AddPoint(minX, minY)
    geom_envelope = ogr.Geometry(ogr.wkbPolygon)
    geom_envelope.AddGeometry(ring)
    return geom_envelope
