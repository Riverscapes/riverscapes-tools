"""Low-level geometry helper functions used across vector_prep modules."""
from __future__ import annotations

from shapely.geometry.base import BaseGeometry
from shapely import make_valid  # shapely >=1.8
from shapely.ops import transform, unary_union
from rsxml import Logger


def _geom_type_str(geom) -> str:
    """Return the geometry type string, or 'Unknown' for None / errors."""
    if geom is None:
        return "Unknown"
    try:
        t = geom.geom_type
        return t if t else "Unknown"
    except Exception:
        return "Unknown"


def safe_make_valid(geom: BaseGeometry):
    """Try make_valid then fallback to buffer(0), or return None if can't fix."""
    if geom is None:
        return None
    try:
        return make_valid(geom)
    except Exception as e:
        log = Logger("Error")
        log.debug(f"make_valid/buffer(0) failed: {e}")
        try:
            return geom.buffer(0)
        except Exception as e2:
            log.debug(f"fallback buffer(0) failed too: {e2}")
            return None


def _strip_z(geom: BaseGeometry) -> BaseGeometry:
    """Strip Z (and M) coordinates from a geometry using shapely.ops.transform."""
    return transform(lambda x, y, *args: (x, y), geom)


def _has_z(geom: BaseGeometry) -> bool:
    """Return True if the geometry has Z coordinates."""
    if geom is None:
        return False
    try:
        return bool(geom.has_z)
    except Exception:
        return False


def _geom_size(geom: BaseGeometry) -> float:
    """Return area for polygon types, length for linear types, 0 for others."""
    try:
        a = geom.area
        if a > 0:
            return float(a)
    except Exception:
        pass
    try:
        return float(geom.length)
    except Exception:
        return 0.0


def _extract_dominant_from_collection(geom: BaseGeometry, dominant_type: str | None) -> BaseGeometry:
    """Extract parts matching dominant_type from a GeometryCollection, falling back to largest."""
    _base_map = {
        "Polygon": "Polygon",
        "MultiPolygon": "Polygon",
        "LineString": "LineString",
        "MultiLineString": "LineString",
        "LinearRing": "LineString",
        "Point": "Point",
        "MultiPoint": "Point",
    }
    if dominant_type is None:
        return max(geom.geoms, key=_geom_size)

    dominant_base = _base_map.get(dominant_type, dominant_type)
    parts = [g for g in geom.geoms if _base_map.get(g.geom_type, g.geom_type) == dominant_base]
    if parts:
        return parts[0] if len(parts) == 1 else unary_union(parts)
    # Fallback: keep largest sub-geometry of any type
    return max(geom.geoms, key=_geom_size)


def _has_duplicate_vertices(geom: BaseGeometry) -> bool:
    """Return True if any consecutive coordinate pair in the geometry is identical (x, y)."""

    def _check_coords(coords: list) -> bool:
        for i in range(len(coords) - 1):
            # compare only x, y to ignore potential Z
            if coords[i][:2] == coords[i + 1][:2]:
                return True
        return False

    if geom is None:
        return False
    try:
        if geom.is_empty:
            return False
    except Exception:
        return False

    gtype = geom.geom_type
    if gtype in ("LineString", "LinearRing"):
        return _check_coords(list(geom.coords))
    if gtype == "Polygon":
        if _check_coords(list(geom.exterior.coords)):
            return True
        return any(_check_coords(list(ring.coords)) for ring in geom.interiors)
    if gtype in ("MultiLineString", "MultiPolygon", "MultiPoint", "GeometryCollection"):
        return any(_has_duplicate_vertices(g) for g in geom.geoms)
    return False


def _has_unclosed_rings(geom: BaseGeometry) -> bool:
    """Return True if any exterior or interior ring of a polygon geometry is unclosed."""
    if geom is None:
        return False
    try:
        if geom.is_empty:
            return False
    except Exception:
        return False

    gtype = geom.geom_type
    if gtype == "Polygon":
        if not geom.exterior.is_ring:
            return True
        return any(not ring.is_ring for ring in geom.interiors)
    if gtype == "MultiPolygon":
        return any(_has_unclosed_rings(p) for p in geom.geoms)
    return False
