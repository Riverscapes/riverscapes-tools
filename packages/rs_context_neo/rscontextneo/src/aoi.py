"""
AOI GeoJSON validation and staging for RS Context Neo.

Validates a user-supplied AOI GeoJSON file (size, structure, geometry type and
topology) and copies it into the project output folder ready for downstream use.
"""
import json
import os
import shutil

from shapely.geometry import shape
from shapely.validation import explain_validity

from rsxml import Logger

# Maximum permitted file size in bytes (600 KB)
_MAX_SIZE_BYTES = 600 * 1024
_MAX_SIZE_LABEL = '600 KB'

# GeoJSON geometry types accepted as an area-of-interest
_POLYGON_TYPES = {'Polygon', 'MultiPolygon'}

# WGS84 coordinate bounds used for a quick sanity check
_LON_MIN, _LON_MAX = -180.0, 180.0
_LAT_MIN, _LAT_MAX = -90.0, 90.0


def validate_copy_aoi(aoi_path: str, output_folder: str, filename: str = 'project_bounds.geojson') -> str:
    """
    Validate a user-supplied AOI GeoJSON file and copy it to the output folder.

    Checks performed (in order):
        1. The file exists.
        2. The file is no larger than 600 KB.
        3. The file parses as valid JSON.
        4. The JSON is a valid GeoJSON FeatureCollection, Feature, or
           Polygon/MultiPolygon geometry.
        5. Every geometry is a Polygon or MultiPolygon (no points, lines, etc.).
        6. Every geometry is topologically valid according to Shapely.
        7. Every coordinate pair falls within WGS84 bounds
           (longitude −180…180, latitude −90…90).

    Parameters:
        aoi_path (str): Path to the input AOI GeoJSON file.
        output_folder (str): Directory to copy the validated file into.
        filename (str): Destination filename. Defaults to 'project_bounds.geojson'.

    Returns:
        str: Absolute path to the copied GeoJSON file inside the output folder.

    Raises:
        FileNotFoundError: If aoi_path does not exist.
        ValueError: If any validation check fails.
    """
    log = Logger('AOI Validation')

    # ── 1. Existence ──────────────────────────────────────────────────────────
    if not os.path.exists(aoi_path):
        raise FileNotFoundError(f'AOI file not found: {aoi_path}')

    # ── 2. File size ──────────────────────────────────────────────────────────
    size_bytes = os.path.getsize(aoi_path)
    log.info(f'AOI file size: {size_bytes / 1024:.1f} KB  (limit: {_MAX_SIZE_LABEL})')
    if size_bytes > _MAX_SIZE_BYTES:
        raise ValueError(
            f'AOI file is too large ({size_bytes / 1024:.1f} KB). '
            f'Maximum permitted size is {_MAX_SIZE_LABEL}. '
            'Simplify the geometry before using it with this tool.'
        )

    # ── 3. JSON parse ─────────────────────────────────────────────────────────
    log.info(f'Parsing AOI GeoJSON: {aoi_path}')
    try:
        with open(aoi_path, encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f'AOI file is not valid JSON: {exc}') from exc

    if not isinstance(data, dict) or 'type' not in data:
        raise ValueError('AOI file does not look like a GeoJSON object (missing "type" key).')

    # ── 4. GeoJSON structure → collect geometry dicts ─────────────────────────
    geom_dicts = _collect_geometries(data)

    if not geom_dicts:
        raise ValueError('AOI GeoJSON contains no features or geometries.')

    # ── 5–7. Per-geometry checks ───────────────────────────────────────────────
    for idx, geom_dict in enumerate(geom_dicts):
        label = f'geometry {idx + 1}/{len(geom_dicts)}'

        if geom_dict is None:
            raise ValueError(f'AOI contains a feature with a null geometry ({label}).')

        geom_type = geom_dict.get('type', '<missing>')

        # 5. Must be a polygon type
        if geom_type not in _POLYGON_TYPES:
            raise ValueError(
                f'AOI contains a {geom_type!r} geometry ({label}). '
                f'Only Polygon and MultiPolygon are accepted as an area of interest.'
            )

        # 6. Shapely topological validity
        try:
            shp = shape(geom_dict)
        except Exception as exc:
            raise ValueError(f'Could not parse {label} with Shapely: {exc}') from exc

        if not shp.is_valid:
            reason = explain_validity(shp)
            raise ValueError(
                f'AOI {label} is not a topologically valid geometry: {reason}. '
                'Try running the geometry through a GIS "repair geometry" operation.'
            )

        # 7. WGS84 coordinate bounds
        min_x, min_y, max_x, max_y = shp.bounds
        if min_x < _LON_MIN or max_x > _LON_MAX or min_y < _LAT_MIN or max_y > _LAT_MAX:
            raise ValueError(
                f'AOI {label} has coordinates outside WGS84 bounds '
                f'(lon {min_x:.4f}…{max_x:.4f}, lat {min_y:.4f}…{max_y:.4f}). '
                'Reproject the AOI to EPSG:4326 before using it with this tool.'
            )

    log.info(f'AOI passed all validation checks ({len(geom_dicts)} geometry/ies)')

    # ── Copy to output folder ─────────────────────────────────────────────────
    dest = os.path.join(output_folder, filename)
    shutil.copy2(aoi_path, dest)
    log.info(f'AOI copied to: {dest}')

    return dest


# ── Internal helpers ──────────────────────────────────────────────────────────

def _collect_geometries(data: dict) -> list[dict | None]:
    """
    Extract all geometry dicts from a GeoJSON object.

    Handles FeatureCollection, Feature, and bare Geometry objects.
    Returns a list of raw geometry dicts (may include None for null geometries).
    """
    geoj_type = data.get('type')

    if geoj_type == 'FeatureCollection':
        features = data.get('features')
        if not isinstance(features, list):
            raise ValueError('GeoJSON FeatureCollection is missing a "features" array.')
        return [feat.get('geometry') for feat in features]

    if geoj_type == 'Feature':
        return [data.get('geometry')]

    # Bare geometry (Polygon, MultiPolygon, etc.)
    return [data]
