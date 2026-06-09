"""
DEM boundary extraction for RS Context Neo.

Extracts the spatial footprint of a DEM raster (non-nodata areas) and writes
it as a buffered, smoothed GeoJSON MultiPolygon in WGS84 (EPSG:4326).

Author:     Matt Reimer
Date:       2026-05-25
"""

import json
import os
import traceback
from collections import Counter

import numpy as np
import rasterio
import rasterio.warp
from osgeo import gdal, osr
from rasterio.features import shapes
from rscommons.geographic_raster import gdal_dem_geographic
from rsxml import Logger
from rsxml.util import safe_makedirs
from shapely.geometry import MultiPolygon, mapping, shape
from shapely.ops import unary_union

_OUTPUT_EPSG = "EPSG:4326"
_BUFFER_PIXELS = (
    3  # outward buffer radius in pixel-widths: closes gaps, rounds jagged corners
)
_SIMPLIFY_PIXELS = 1  # simplification tolerance in pixel-widths: reduces vertex count


def dem_to_geojson(
    dem_path: str, output_folder: str, filename: str = "project_bounds.geojson"
) -> str:
    """
    Extract the non-nodata footprint of a DEM raster and write it as a
    buffered, smoothed GeoJSON MultiPolygon in WGS84 (EPSG:4326).

    The smoothing pipeline is:
        1. Polygonize the valid-data mask
        2. Dissolve all polygons into a single geometry
        3. Buffer outward by (_BUFFER_PIXELS * pixel_size) to close small gaps
           and round jagged pixel-edge corners
        4. Simplify with tolerance (_SIMPLIFY_PIXELS * pixel_size) to reduce
           vertex count and smooth the outline
        5. Reproject to WGS84

    Parameters:
        dem_path (str): Path to the input DEM raster file.
        output_folder (str): Directory to write the output GeoJSON file.
        filename (str): Output filename. Defaults to 'project_bounds.geojson'.

    Returns:
        str: Absolute path to the written GeoJSON file.

    Raises:
        FileNotFoundError: If dem_path does not exist.
        ValueError: If the raster has no valid (non-nodata) pixels, or if the
                    geometry type after processing is unexpected.
    """
    log = Logger("DEM Bounds")

    if not os.path.exists(dem_path):
        raise FileNotFoundError(f"DEM file not found: {dem_path}")

    log.info(f"Extracting footprint from DEM: {dem_path}")

    with rasterio.open(dem_path) as src:
        band = src.read(1)
        nodata = src.nodata
        raster_transform = src.transform
        src_crs = src.crs
        pixel_size = abs(src.res[0])  # use x-resolution; assumes square pixels

    # Build a uint8 mask: 1 = valid data, 0 = nodata
    if nodata is not None:
        mask = (band != nodata).astype(np.uint8)
    elif np.issubdtype(band.dtype, np.floating):
        mask = (~np.isnan(band)).astype(np.uint8)
    else:
        # No nodata defined and not float — treat entire raster as valid
        log.warning("DEM has no nodata value set; treating all pixels as valid.")
        mask = np.ones(band.shape, dtype=np.uint8)

    valid_count = int(mask.sum())
    if valid_count == 0:
        raise ValueError(f"DEM has no valid (non-nodata) pixels: {dem_path}")

    log.info(f"Vectorizing valid-data mask ({valid_count:,} valid pixels)")

    # Polygonize the valid-data mask; shapes() yields (geojson_geom, pixel_value) pairs
    polys = [
        shape(geom)
        for geom, val in shapes(mask, mask=mask, transform=raster_transform)
        if val == 1
    ]

    if not polys:
        raise ValueError("No polygons could be extracted from the DEM mask.")

    log.info(f"Dissolving {len(polys):,} polygon(s)")
    merged = unary_union(polys)

    # Buffer outward to fill small holes and round pixel-edge jaggedness,
    # then simplify to smooth the outline and reduce vertex count
    buffer_dist = pixel_size * _BUFFER_PIXELS
    simplify_tol = pixel_size * _SIMPLIFY_PIXELS
    log.info(
        f"Buffering by {buffer_dist:.4f} units, simplifying with tolerance {simplify_tol:.4f} units"
    )

    smoothed = merged.buffer(buffer_dist).simplify(simplify_tol, preserve_topology=True)

    # Normalise to MultiPolygon
    if smoothed.geom_type == "Polygon":
        smoothed = MultiPolygon([smoothed])
    elif smoothed.geom_type != "MultiPolygon":
        raise ValueError(
            f"Unexpected geometry type after processing: {smoothed.geom_type}"
        )

    # Reproject to WGS84 for GeoJSON output
    geom_json = mapping(smoothed)
    if src_crs:
        log.info(f"Reprojecting from {src_crs.to_string()} to {_OUTPUT_EPSG}")
        geom_json = rasterio.warp.transform_geom(src_crs, _OUTPUT_EPSG, geom_json)

    # Write GeoJSON FeatureCollection
    dest = os.path.join(output_folder, filename)
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": geom_json,
                "properties": {},
            }
        ],
    }

    with open(dest, "w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=2)

    log.info(f"Project bounds written to: {dest}")
    return dest


def generate_hillshade(
    dem_path: str,
    hillshade_path: str,
    *,
    epsg: int | None = None,
    force: bool = False,
    log: Logger | None = None,
) -> None:
    """
    Generate a hillshade raster from *dem_path*.

    For geographic (lat/lon) DEMs a haversine z-factor is applied via
    ``gdal_dem_geographic``; projected DEMs use ``gdal.DEMProcessing`` directly.

    Parameters
    ----------
    dem_path : str
        Path to the source DEM.
    hillshade_path : str
        Destination path for the hillshade raster.
    epsg : int or None
        EPSG code of *dem_path*.  When ``None`` the CRS is read directly from
        the raster.
    force : bool
        If ``True``, regenerate even when the output already exists.
    log : Logger or None
        rsxml Logger.  Falls back to a module-level logger when ``None``.
    """
    _log = log or Logger("Hillshade")

    if epsg is None:
        epsg = get_epsg(dem_path)

    if not force and os.path.isfile(hillshade_path):
        _log.info("Hillshade already exists - skipping rebuild")
        return

    _log.info("Generating hillshade ...")
    safe_makedirs(os.path.dirname(hillshade_path))
    if epsg is not None and is_geographic_epsg(epsg):
        gdal_dem_geographic(dem_path, hillshade_path, "hillshade")
    else:
        gdal.DEMProcessing(
            hillshade_path,
            dem_path,
            "hillshade",
            creationOptions=["COMPRESS=DEFLATE"],
        )
    _log.info(f"Hillshade: {hillshade_path}")


def generate_slope(
    dem_path: str,
    slope_path: str,
    *,
    force: bool = False,
    log: Logger | None = None,
) -> None:
    """
    Generate a Horn-method slope raster (degrees) from *dem_path*.

    Uses ``gdal.DEMProcessing`` with Horn's method, matching the SLOPE layer
    produced by ``rs_context`` and expected by BRAT, RME, and other downstream
    tools.  This is **not** the D8 rise/run slope produced by TauDEM, which is
    an internal flow-routing intermediate.

    The input must be the raw DEM, **not** a pit-filled or breach-conditioned
    version: hydrological conditioning raises depression cells, which would
    artificially flatten areas and misrepresent true terrain slope.

    Parameters
    ----------
    dem_path : str
        Path to the raw source DEM.
    slope_path : str
        Destination path for the slope raster.
    force : bool
        If ``True``, regenerate even when the output already exists.
    log : Logger or None
        rsxml Logger.  Falls back to a module-level logger when ``None``.
    """
    _log = log or Logger("Slope")

    if not force and os.path.isfile(slope_path):
        _log.info("Slope already exists - skipping rebuild")
        return

    _log.info("Generating slope raster (gdal.DEMProcessing, degrees) ...")
    safe_makedirs(os.path.dirname(slope_path))
    result = gdal.DEMProcessing(
        slope_path,
        dem_path,
        "slope",
        creationOptions=[
            "COMPRESS=DEFLATE",
            "PREDICTOR=2",
            "TILED=YES",
            "BIGTIFF=IF_SAFER",
        ],
    )
    if result is None:
        _log.warning(f"gdal.DEMProcessing slope failed: {gdal.GetLastErrorMsg()}")
    else:
        result = None  # flush GDAL handle
        _log.info(f"Slope:     {slope_path}")


def get_epsg(raster_path: str) -> int | None:
    """
    Return the EPSG code embedded in a raster file, or None if it cannot be
    determined.

    Adapted from rscontext_3dep/rscontext_3dep/dem_builder.py.
    """
    log = Logger("get_epsg")
    dataset = None
    try:
        dataset = gdal.Open(raster_path, gdal.GA_ReadOnly)
        if dataset is None:
            log.error(f"Could not open {raster_path} with GDAL.")
            return None

        wkt = dataset.GetProjection()
        if not wkt:
            log.warning(f"No CRS information found in {raster_path}")
            return None

        srs = osr.SpatialReference()
        if srs.ImportFromWkt(wkt) != 0:
            log.error(f"Failed to import WKT projection from {raster_path}")
            return None

        srs.AutoIdentifyEPSG()
        code_str = srs.GetAuthorityCode(None)
        if not code_str:
            log.warning(f"Could not auto-identify EPSG for {raster_path}")
            return None

        try:
            return int(code_str)
        except ValueError:
            log.error(f"Non-integer authority code '{code_str}' for {raster_path}")
            return None

    except Exception as exc:
        log.error(f"Unexpected error reading EPSG from {raster_path}: {exc}")
        log.debug(traceback.format_exc())
        return None

    finally:
        dataset = None


def get_best_crs(raster_paths: list[str]) -> int | None:
    """
    Return the EPSG code used by the majority of the supplied rasters.

    Ties are broken by choosing the lowest EPSG number.  Returns None if no
    valid EPSG code can be read from any raster.

    Adapted from rscontext_3dep/rscontext_3dep/dem_builder.py.

    .. note::
        Not called within ``rs_context_neo`` — CRS selection was merged into
        :func:`~rscontextneo.src.fetch_dem._inspect_tiles`, which performs a
        single pass over all tiles rather than opening each file twice.
        Retained here for reference alongside the other ``dem_builder.py``
        helpers it was adapted from.
    """
    log = Logger("get_best_crs")

    valid_codes = [c for c in (get_epsg(p) for p in raster_paths) if c is not None]
    if not valid_codes:
        log.error("Could not determine a valid EPSG from any of the input rasters.")
        return None

    counts = Counter(valid_codes)
    max_freq = counts.most_common(1)[0][1]
    candidates = [code for code, freq in counts.items() if freq == max_freq]
    best = min(candidates)  # tie-break: lowest EPSG number

    log.info(f"Best CRS: EPSG:{best} (frequency {max_freq}/{len(valid_codes)})")
    return best


def is_geographic_epsg(epsg_code: int) -> bool:
    """
    Return True if the EPSG code refers to a geographic (lat/lon) CRS.

    Adapted from rscontext_3dep/rscontext_3dep/dem_builder.py.
    """
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg_code)
    return srs.IsGeographic() == 1
