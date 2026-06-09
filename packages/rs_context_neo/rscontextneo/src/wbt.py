"""
WhiteboxTools step functions for the RS Context Neo D8 hydrology pipeline.

Each function in this module wraps a single WhiteboxTools operation.  They
are called by :func:`~rscontextneo.src.hydrology.run_d8_hydrology` and are
not intended to be called directly.

Note on naming
--------------
This module is named ``wbt.py`` rather than ``whitebox.py`` to avoid
shadowing the ``whitebox`` PyPI package on the import path.

Author:     Matt Reimer
Date:       2026-05-25
"""

import os

import whitebox
from rsxml import Logger

from rscontextneo.src.utils.rasters import (
    cast_to_float32_inplace,
    compress_inplace,
    skip_if_exists,
)


def breach_depressions_least_cost(
    dem_path: str,
    breach_dem_path: str,
    breach_dist: int,
    force: bool,
    log: Logger,
    debug: bool = False,
) -> None:
    """
    Hydrologically condition the DEM via least-cost depression
    breaching (WhiteboxTools ``BreachDepressionsLeastCost``).

    Unlike simple pit-filling (which raises all cells in a depression to the
    pour-point elevation), least-cost breaching carves a channel through the
    barrier at the minimum cumulative elevation cost, better preserving the
    DEM's true morphology.  Any depressions that cannot be breached within
    *breach_dist* cells are filled conventionally (``fill=True``) so that the
    output is always fully hydrologically conditioned.

    The result is written alongside ``dem_filled.tif`` (TauDEM pitremove) so
    both conditioning approaches can be compared visually before committing to
    one for production.

    Parameters
    ----------
    dem_path : str
        Input DEM raster.
    breach_dem_path : str
        Output breach-conditioned DEM (``hydrology/dem_breach.tif``).
    breach_dist : int
        Maximum search distance for breach paths, in cells.
    force : bool
        Re-run even if the output already exists.
    log : Logger
        Caller-supplied logger.

    Raises
    ------
    RuntimeError
        If WhiteboxTools returns a non-zero exit code or fails to produce the
        expected output file.
    """
    if skip_if_exists(breach_dem_path, force, "BreachDepressionsLeastCost", log):
        return

    log.info(f"Breach depressions least-cost (WhiteboxTools, dist={breach_dist} cells)")

    wbt = whitebox.WhiteboxTools()
    wbt.set_verbose_mode(True if debug else False)

    err = wbt.breach_depressions_least_cost(
        dem=dem_path,
        output=breach_dem_path,
        dist=breach_dist,
        fill=True,
    )
    if err != 0:
        raise RuntimeError(
            f"WhiteboxTools BreachDepressionsLeastCost failed with exit code {err}. "
            "Check the log above for error messages."
        )
    if not os.path.isfile(breach_dem_path):
        raise RuntimeError(
            f"BreachDepressionsLeastCost returned success but expected output was not created: "
            f"{breach_dem_path}"
        )
    log.info(f"  → {breach_dem_path}")
    # WhiteboxTools writes breach DEMs as float64, which is overkill and causes
    # DEFLATE PREDICTOR=2 compression to fail in some GIS software.
    # Cast to float32 and recompress in-place so it works better with our other rasters.
    cast_to_float32_inplace(breach_dem_path, log)
    compress_inplace(breach_dem_path, log)
