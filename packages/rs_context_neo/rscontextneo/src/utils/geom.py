"""
Shared geometry utilities for RS Context Neo.

Lightweight helpers for loading GeoJSON files as Shapely geometries.
These exist because rscommons's ``get_shp_or_gpkg`` routes through the OGR
GPKG driver and does not accept raw GeoJSON paths.  When rscommons is
updated to support GeoJSON natively, the callers of
:func:`load_geojson_geometry` can be replaced with
``rscommons.vector_ops.get_geometry_unary_union(path)`` and this module
retired.

Author:     Matt Reimer
Date:       2026-06-02
"""

import json

from shapely.geometry import shape
from shapely.ops import unary_union


def load_geojson_geometry(geojson_path: str):
    """
    Load all geometries from a GeoJSON file and return their union as a
    single Shapely geometry.

    Handles ``FeatureCollection``, ``Feature``, and bare geometry objects.
    The returned geometry is the ``unary_union`` of all features so
    multi-polygon AOIs are merged into one object.

    Parameters
    ----------
    geojson_path : str
        Absolute path to a GeoJSON file (typically the WGS84 project bounds).

    Returns
    -------
    shapely.geometry.base.BaseGeometry
        Union of all geometries in the file.

    Raises
    ------
    ValueError
        If the file contains no valid geometries.
    """
    with open(geojson_path, encoding="utf-8") as f:
        data = json.load(f)

    geoj_type = data.get("type", "")
    if geoj_type == "FeatureCollection":
        geoms = [
            shape(feat["geometry"])
            for feat in data.get("features", [])
            if feat.get("geometry")
        ]
    elif geoj_type == "Feature":
        geoms = [shape(data["geometry"])] if data.get("geometry") else []
    else:
        # Bare geometry object (Polygon, MultiPolygon, etc.)
        geoms = [shape(data)]

    if not geoms:
        raise ValueError(f"No geometries found in GeoJSON: {geojson_path}")

    return unary_union(geoms)
