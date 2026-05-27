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
            f'{step_name}: output already exists at {os.path.basename(path)}'
            ' — skipping (use force=True to re-run)'
        )
        return True
    return False


def compress_inplace(path: str, log: Logger) -> None:
    """
    Re-compress a GeoTIFF in-place using DEFLATE.

    Writes to a sibling ``.tmp.tif`` file then atomically replaces the
    original so that a failed compression never leaves a corrupt file.

    Parameters
    ----------
    path : str
        Absolute path to the GeoTIFF to compress.
    log : Logger
        Caller-supplied logger.
    """
    tmp = path + '.tmp.tif'
    try:
        result = gdal.Translate(
            tmp,
            path,
            creationOptions=['COMPRESS=DEFLATE', 'PREDICTOR=2', 'TILED=YES', 'BIGTIFF=IF_SAFER'],
        )
        if result is None:
            raise RuntimeError(f'gdal.Translate returned None for {path}: {gdal.GetLastErrorMsg()}')
        result = None  # flush / dereference
        os.replace(tmp, path)
        log.info(f'  compressed → {os.path.basename(path)}')
    except Exception:
        if os.path.isfile(tmp):
            os.remove(tmp)
        raise
