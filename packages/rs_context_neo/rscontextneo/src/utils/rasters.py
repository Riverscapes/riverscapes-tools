"""
Shared utility functions for the RS Context Neo hydrology pipeline.

These helpers are used by both the TauDEM (taudem.py) and WhiteboxTools
(wbt.py) step modules and are kept here to avoid circular imports.

Author:     Matt Reimer
Date:       2026-05-25
"""

import os

from osgeo import gdal
from rsxml import Logger


def skip_if_exists(path: str, force: bool, step_name: str, log: Logger) -> bool:
    """
    Return ``True`` (and log a skip message) if *path* exists and *force* is
    ``False``.  Returns ``False`` otherwise (step should run).
    """
    if not force and os.path.isfile(path):
        log.info(
            f"{step_name}: output already exists at {os.path.basename(path)}"
            " — skipping (use force=True to re-run)"
        )
        return True
    return False


def compress_inplace(path: str, log: Logger, type="DEFLATE") -> None:
    """
    Re-compress a GeoTIFF in-place using LZW.

    Writes to a sibling ``.tmp.tif`` file then atomically replaces the
    original so that a failed compression never leaves a corrupt file.

    Parameters
    ----------
    path : str
        Absolute path to the GeoTIFF to compress.
    log : Logger
        Caller-supplied logger.
    type : str
        Compression type, e.g. "DEFLATE" or "LZW".
    """
    tmp = path + ".tmp.tif"
    try:
        result = gdal.Translate(
            tmp,
            path,
            creationOptions=[
                f"COMPRESS={type}",
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


def cast_to_float32_inplace(path: str, log: Logger) -> None:
    """
    Convert a GeoTIFF to float32 in-place.

    WhiteboxTools always writes float64; casting to float32 aligns the breach
    DEM with every other raster in the pipeline and avoids DEFLATE PREDICTOR=2
    incompatibility with 64-bit samples.
    """
    tmp = path + ".f32.tmp.tif"
    try:
        ds = gdal.Translate(tmp, path, outputType=gdal.GDT_Float32)
        if ds is None:
            raise RuntimeError(
                f"float32 cast failed for {path}: {gdal.GetLastErrorMsg()}"
            )
        ds = None
        os.replace(tmp, path)
        log.info("  cast float64 → float32")
    except Exception:
        if os.path.isfile(tmp):
            os.remove(tmp)
        raise
