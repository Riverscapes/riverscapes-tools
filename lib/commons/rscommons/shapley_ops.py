#!/usr/bin/env python3
# Name:     Shapely Utility Functions
#
# Purpose:  Bunch of helper tools for working with shapely geometries
#
# Author:   Kelly Whitehead
#
# Date:     11 Oct 2021
# -------------------------------------------------------------------------------
from shapely.geometry import Point, LineString


def line_segments(curve: LineString) -> list:
    """generate a list of linestring segments from input curve

    Args:
        curve (LineString): Line to get segments from

    Returns:
        list(LineString): list of all linestring segments of input curve
    """
    return list(map(LineString, zip(curve.coords[:-1], curve.coords[1:])))


def select_geoms_by_intersection(in_geoms, select_geoms, buffer=None, inverse=False):
    """select geometires based on intersection (or non-intersection) of midpoint

    Args:
        in_geoms (list): list of LineString geometires to select from
        select_geoms ([type]): list of LineString geometries to make the selection
        buffer ([type], optional): apply a buffer on the midpoint selectors. Defaults to None.
        inverse (bool, optional): return list of LineStrings that Do Not intersect the selecion geometries. Defaults to False.

    Returns:
        list: List of LineString geometries that meet the selection criteria
    """
    out_geoms = []

    for geom in in_geoms:
        if inverse:
            if all([geom_select.disjoint(geom.interpolate(0.5, True).buffer(buffer) if buffer else geom.interpolate(0.5, True)) for geom_select in select_geoms]):
                out_geoms.append(geom)
        else:
            if any([geom_select.intersects(geom.interpolate(0.5, True).buffer(buffer) if buffer else geom.interpolate(0.5, True)) for geom_select in select_geoms]):
                out_geoms.append(geom)

    return out_geoms


def cut(line, distance):
    """Cuts a line in two at a distance from its starting point

    Args:
        line (LineString): line to cut
        distance (float): distance to make the cut

    Returns:
        tuple: two linestrings on either side of cut
    """
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [
                LineString(coords[:i + 1]),
                LineString(coords[i:])]
        if pd > distance:
            cp = line.interpolate(distance)
            return [
                LineString(coords[:i] + [(cp.x, cp.y)]),
                LineString([(cp.x, cp.y)] + coords[i:])]


def cut_line_segment(line, distance_start, distance_stop):
    """Cuts a line in two at a distance from its starting point

    Args:
        line (LineString): linestring to cut
        distance_start (float): starting distance of segment
        distance_stop (float): ending distance of segment

    Returns:
        LineString: linestring segment between distances
    """
    if distance_start > distance_stop:
        raise ValueError(f"Cut Line Segment: Distance Start ({distance_start}) greater than Distance stop ({distance_stop})")
    if distance_start < 0.0 or distance_stop > line.length:
        return [LineString(line)]
    if distance_start == distance_stop:
        # raise ValueError(f"Cut Line Segment: Distance Start ({distance_start}) same as Distance stop ({distance_stop})")
        distance_start = distance_start - 0.01
        distance_stop = distance_stop + 0.01

    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance_start:
            segment = LineString(coords[i:])
            break

        if pd > distance_start:
            cp = line.interpolate(distance_start)
            segment = LineString([(cp.x, cp.y)] + coords[i:])
            break

    coords = list(segment.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance_stop:
            return LineString(coords[: i + 1])

        if pd > distance_stop:
            cp = line.interpolate(distance_stop)
            return LineString(coords[: i] + [(cp.x, cp.y)])
