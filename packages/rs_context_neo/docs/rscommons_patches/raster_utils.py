"""
Proposed additions to rscommons/classes/raster.py
==================================================

Add these functions to ``lib/commons/rscommons/classes/raster.py``.

They are currently duplicated across rs_context_neo (``utils/dem.py`` and
``utils/rasters.py``) and at least one other package (rscontext_3dep).
Centralising them in rscommons eliminates the duplication.

Once merged, rs_context_neo can replace:
    from rscontextneo.src.utils.dem import get_epsg, is_geographic_epsg
    from rscontextneo.src.utils.rasters import compress_inplace, cast_to_float32_inplace
with:
    from rscommons.classes.raster import (
        get_raster_epsg, is_geographic_crs,
        compress_raster_inplace, cast_raster_dtype_inplace,
    )
"""

import os
import traceback

from osgeo import gdal, osr
from rsxml import Logger


# ---------------------------------------------------------------------------
# CRS helpers
# ---------------------------------------------------------------------------


def get_raster_epsg(raster_path: str) -> int | None:
    """Return the EPSG code embedded in a raster file, or None if it cannot
    be determined.

    Parameters
    ----------
    raster_path : str
        Absolute path to a GeoTIFF or other GDAL-readable raster.

    Returns
    -------
    int or None
        Integer EPSG code (e.g. ``32611``) or ``None``.
    """
    log = Logger("get_raster_epsg")
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
        dataset = None  # noqa: F841  (explicit dereference for GDAL)


def is_geographic_crs(epsg_code: int) -> bool:
    """Return ``True`` if the EPSG code refers to a geographic (lat/lon) CRS.

    Parameters
    ----------
    epsg_code : int
        EPSG code to test (e.g. 4326 → True, 32611 → False).

    Returns
    -------
    bool
    """
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg_code)
    return srs.IsGeographic() == 1


# ---------------------------------------------------------------------------
# In-place raster utilities
# ---------------------------------------------------------------------------


def compress_raster_inplace(path: str, log: Logger) -> None:
    """Re-compress a GeoTIFF in-place using DEFLATE+PREDICTOR=2.

    Writes to a sibling ``.tmp.tif`` file then atomically replaces the
    original so that a failed compression never leaves a corrupt file.

    Parameters
    ----------
    path : str
        Absolute path to the GeoTIFF to compress.
    log : Logger
        Caller-supplied rsxml Logger.
    """
    tmp = path + ".tmp.tif"
    try:
        result = gdal.Translate(
            tmp,
            path,
            creationOptions=[
                "COMPRESS=DEFLATE",
                "PREDICTOR=2",
                "TILED=YES",
                "BIGTIFF=IF_SAFER",
            ],
        )
        if result is None:
            raise RuntimeError(
                f"gdal.Translate returned None for {path}: {gdal.GetLastErrorMsg()}"
            )
        result = None  # flush / dereference
        os.replace(tmp, path)
        log.info(f"  compressed → {os.path.basename(path)}")
    except Exception:
        if os.path.isfile(tmp):
            os.remove(tmp)
        raise


def cast_raster_dtype_inplace(
    path: str, gdal_dtype: int, log: Logger, suffix: str = ".cast.tmp.tif"
) -> None:
    """Cast a GeoTIFF to a different GDAL data type in-place, atomically.

    Useful for converting float64 output from tools like WhiteboxTools to
    float32 so it aligns with other rasters in the pipeline.

    Parameters
    ----------
    path : str
        Absolute path to the source GeoTIFF.
    gdal_dtype : int
        GDAL data type constant (e.g. ``gdal.GDT_Float32``).
    log : Logger
        Caller-supplied rsxml Logger.
    suffix : str
        Temporary file suffix.  Default: ``'.cast.tmp.tif'``.

    Example
    -------
    ::

        from osgeo import gdal
        from rscommons.classes.raster import cast_raster_dtype_inplace
        cast_raster_dtype_inplace(breach_dem_path, gdal.GDT_Float32, log)
    """
    tmp = path + suffix
    try:
        ds = gdal.Translate(tmp, path, outputType=gdal_dtype)
        if ds is None:
            raise RuntimeError(
                f"cast_raster_dtype_inplace failed for {path}: {gdal.GetLastErrorMsg()}"
            )
        ds = None  # flush
        os.replace(tmp, path)
        log.info(f"  cast dtype → {os.path.basename(path)}")
    except Exception:
        if os.path.isfile(tmp):
            os.remove(tmp)
        raise
