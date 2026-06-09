"""
D8 Hydrology workflow for RS Context Neo.

This module is the public entry point for the hydrology pipeline.  It owns
all output-path constants and the single public function
:func:`run_d8_hydrology`, which sequences the individual processing steps
imported from the tool-specific sub-modules:

    taudem.py   — TauDEM MPI steps (pitremove, d8flowdir, aread8, threshold,
                  streamnet, vectorize_subwatersheds) and MPI helpers
    wbt.py      — WhiteboxTools steps (breach_depressions_least_cost)
    hydro_utils.py — Shared utilities (skip_if_exists, compress_inplace)

Processing steps
----------------
    Step 1a  BreachDepressionsLeastCost (WhiteboxTools)
             Hydrological conditioning via least-cost breaching — prefers
             carving over filling where possible.

    Step 1b  pitremove (TauDEM)
             Fill any remaining topographic sinks so flow routes to the
             domain edge.

    Step 2   d8flowdir (TauDEM)
             Assign each cell a single downstream direction (1-8) and slope.

    Step 3   aread8 (TauDEM)
             Accumulate upstream cell counts (flow accumulation).

    Step 4   threshold (TauDEM)
             Classify cells with contributing area ≥ threshold as stream.

    Step 5   streamnet (TauDEM)
             Extract reach network, Strahler order, and subwatershed rasters.

    Step 6   vectorize_subwatersheds (GDAL/OGR)
             Polygonise subwatershed raster into the hydrology GeoPackage.

    Step 7   calc_level_paths
             Walk the river network from headwaters to outlets, assigning a
             unique integer level-path ID to each reach.  The value is stored
             in the ``level_path`` column of the ``network_intersected`` layer
             inside the hydrology GeoPackage.

Output layout (all relative to *output_folder*):
    hydrology/dem_filled.tif            pit-filled DEM (TauDEM pitremove)
    hydrology/dem_breach.tif            breach-conditioned DEM (WBT BreachDepressionsLeastCost)
    hydrology/d8_flow.tif               D8 flow direction
    hydrology/d8_slope.tif              D8 slope (TauDEM d8flowdir, rise/run, internal intermediate)
    hydrology/d8_contributing_area.tif  D8 contributing area
    hydrology/stream_raster.tif         binary stream mask
    hydrology/stream_order.tif          Strahler stream-order raster
    hydrology/hydro_derivatives.gpkg    vector network + subwatersheds (two layers)
                                        network_intersected.level_path  level-path IDs (Step 7)
    hydrology/subwatersheds.tif         subwatershed raster

Author:     Matt Reimer
Date:       2026-05-25
"""

import os
import time
from typing import Dict, List

from rsxml import Logger
from rsxml.util import pretty_duration, safe_makedirs

from rscontextneo.src.level_path import calc_level_paths
from rscontextneo.src.taudem import (
    apply_mpi_env,
    aread8,
    d8flowdir,
    pitremove,
    resolve_cores,
    resolve_mpi_args,
    streamnet,
    vectorize_subwatersheds,
)
from rscontextneo.src.taudem import (
    threshold as taudem_threshold,
)
from rscontextneo.src.wbt import breach_depressions_least_cost

# ── Output relative paths (relative to output_folder) ─────────────────────────
FILLED_DEM_RELPATH = "hydrology/dem_filled.tif"
DEM_BREACH_RELPATH = "hydrology/dem_breach.tif"
D8_FLOW_RELPATH = "hydrology/d8_flow.tif"
D8_SLOPE_RELPATH = "hydrology/d8_slope.tif"
D8_CONTRIB_AREA_RELPATH = "hydrology/d8_contributing_area.tif"
STREAM_RASTER_RELPATH = "hydrology/stream_raster.tif"
STREAM_ORDER_RELPATH = "hydrology/stream_order.tif"
HYDROLOGY_GPKG_RELPATH = "hydrology/hydro_derivatives.gpkg"
SUBWATERSHEDS_RELPATH = "hydrology/subwatersheds.tif"

# Auxiliary TauDEM text outputs written alongside streamnet products
_STREAM_TREE_RELPATH = "hydrology/stream_tree.dat"
_STREAM_COORD_RELPATH = "hydrology/stream_coord.dat"

# ── Tuneable defaults ──────────────────────────────────────────────────────────
# Minimum upstream cell count for stream classification.  At 1 m resolution
# one cell = 1 m², so 50 000 cells ≈ 0.05 km².  Scale by (cell_size_m)² for
# coarser DEMs.  Higher → sparser network; lower → denser network.
DEFAULT_THRESHOLD = 50_000

# Maximum search distance (cells) for the WBT least-cost breach path.
# At 1 m resolution this is a direct distance in metres.  The value needs to
# be large enough to span the widest anthropogenic barrier you expect to
# encounter (road embankment + any backed-up water behind it) but small enough
# that the algorithm doesn't accidentally breach natural ridges or lake outlets.
#
# Typical feature widths at 1 m resolution:
#   gravel / two-lane road          10–15 m
#   highway with shoulders          30–50 m
#   bridge approach embankment      20–150 m
#
# 100 cells (100 m) covers standard roads, rail lines, and most bridge
# approaches without risking breaches through natural terrain features.
# Increase to 200–300 for areas with major highway or rail infrastructure.
# Scale proportionally for coarser DEMs (e.g. 10 cells at 10 m resolution).
DEFAULT_BREACH_DIST = 100


# ── Public entry point ─────────────────────────────────────────────────────────


def run_d8_hydrology(
    dem_path: str,
    output_folder: str,
    *,
    threshold: int = DEFAULT_THRESHOLD,
    breach_dist: int = DEFAULT_BREACH_DIST,
    cores: int | None = None,
    mpi_args: List[str] | None = None,
    force: bool = False,
    debug: bool = False,
) -> Dict[str, str]:
    """
    Run the full D8 hydrology processing chain on a DEM.

    Each step is skipped automatically if its output file already exists and
    *force* is ``False``.  Pass ``force=True`` to unconditionally re-run every
    step (e.g. after changing the threshold or replacing the DEM).

    Parameters
    ----------
    dem_path : str
        Absolute path to the input DEM raster (GeoTIFF, projected CRS).
    output_folder : str
        Root of the RS Context Neo project folder.  All hydrology outputs are
        written under ``<output_folder>/hydrology/``.
    threshold : int
        Minimum contributing-area cell count for stream classification
        (units: cells).  Default: :data:`DEFAULT_THRESHOLD`.
    breach_dist : int
        Maximum search distance in cells for the WBT least-cost breach path.
        Default: :data:`DEFAULT_BREACH_DIST`.
    cores : int or None
        Number of MPI ranks for TauDEM steps.  ``None`` reads the
        ``TAUDEM_CORES`` environment variable; falls back to 2.
    mpi_args : list[str] or None
        Extra flags inserted between ``mpiexec`` and the TauDEM command name.
        ``None`` reads ``TAUDEM_MPI_ARGS`` from the environment.
    force : bool
        Re-run all steps even if outputs already exist.  Default: ``False``.

    Returns
    -------
    dict[str, str]
        Mapping of short product names to absolute file paths::

            {
                'dem_filled':           '<output_folder>/hydrology/dem_filled.tif',
                'dem_breach':           '<output_folder>/hydrology/dem_breach.tif',
                'd8_flow':              '<output_folder>/hydrology/d8_flow.tif',
                'd8_slope':             '<output_folder>/hydrology/d8_slope.tif',
                'd8_contributing_area': '<output_folder>/hydrology/d8_contributing_area.tif',
                'stream_raster':        '<output_folder>/hydrology/stream_raster.tif',
                'stream_order':         '<output_folder>/hydrology/stream_order.tif',
                'hydrology_gpkg':       '<output_folder>/hydrology/hydro_derivatives.gpkg',
                'subwatersheds':        '<output_folder>/hydrology/subwatersheds.tif',
            }

    Raises
    ------
    FileNotFoundError
        If *dem_path* does not exist.
    RuntimeError
        If any processing step fails or does not produce its expected output.
    """
    log = Logger("D8 Hydrology")
    start_time = time.time()

    if not os.path.isfile(dem_path):
        raise FileNotFoundError(f"Input DEM not found: {dem_path}")

    ncores = str(resolve_cores(cores))
    mpi_args = resolve_mpi_args(mpi_args)

    log.info(f"Starting D8 hydrology workflow  |  DEM: {dem_path}")
    log.info(
        f"MPI cores: {ncores}  |  threshold: {threshold:,}  |  breach_dist: {breach_dist}  |  force: {force}"
    )
    if mpi_args:
        log.info(f"Extra MPI args: {' '.join(mpi_args)}")

    apply_mpi_env(log)

    hydro_dir = os.path.join(output_folder, "hydrology")
    safe_makedirs(hydro_dir)
    safe_makedirs(os.path.join(output_folder, "topography"))

    paths = {
        "dem_filled": os.path.join(output_folder, FILLED_DEM_RELPATH),
        "dem_breach": os.path.join(output_folder, DEM_BREACH_RELPATH),
        "d8_flow": os.path.join(output_folder, D8_FLOW_RELPATH),
        "d8_slope": os.path.join(output_folder, D8_SLOPE_RELPATH),
        "d8_contributing_area": os.path.join(output_folder, D8_CONTRIB_AREA_RELPATH),
        "stream_raster": os.path.join(output_folder, STREAM_RASTER_RELPATH),
        "stream_order": os.path.join(output_folder, STREAM_ORDER_RELPATH),
        "hydrology_gpkg": os.path.join(output_folder, HYDROLOGY_GPKG_RELPATH),
        "subwatersheds": os.path.join(output_folder, SUBWATERSHEDS_RELPATH),
    }
    stream_tree = os.path.join(output_folder, _STREAM_TREE_RELPATH)
    stream_coord = os.path.join(output_folder, _STREAM_COORD_RELPATH)

    # ── Step 1a: Breach depressions least-cost (WhiteboxTools) ────────────────
    log.info(
        "Step 1a of 7: Breach depressions (WhiteboxTools BreachDepressionsLeastCost)"
    )
    breach_depressions_least_cost(
        dem_path, paths["dem_breach"], breach_dist, force, log, debug
    )

    # ── Step 1b: Pit removal (TauDEM) ─────────────────────────────────────────
    log.info("Step 1b of 7: Pit removal (TauDEM pitremove)")
    pitremove(
        paths["dem_breach"],
        paths["dem_filled"],
        hydro_dir,
        ncores,
        mpi_args,
        force,
        log,
    )

    # ── Step 2: D8 flow directions & slope (TauDEM) ───────────────────────────
    log.info("Step 2 of 7: D8 flow directions and slope (TauDEM d8flowdir)")
    d8flowdir(
        paths["dem_filled"],
        paths["d8_flow"],
        paths["d8_slope"],
        hydro_dir,
        ncores,
        mpi_args,
        force,
        log,
    )

    # ── Step 3: D8 contributing area (TauDEM) ─────────────────────────────────
    log.info("Step 3 of 7: D8 contributing area (TauDEM aread8)")
    aread8(
        paths["d8_flow"],
        paths["d8_contributing_area"],
        hydro_dir,
        ncores,
        mpi_args,
        force,
        log,
    )

    # ── Step 4: Stream raster (TauDEM) ────────────────────────────────────────
    log.info("Step 4 of 7: Stream raster (TauDEM threshold)")
    taudem_threshold(
        paths["d8_contributing_area"],
        paths["stream_raster"],
        threshold,
        hydro_dir,
        ncores,
        mpi_args,
        force,
        log,
    )

    # ── Step 5: Stream network extraction (TauDEM) ────────────────────────────
    log.info("Step 5 of 7: Stream network extraction (TauDEM streamnet)")
    streamnet(
        paths["d8_flow"],
        paths["dem_filled"],
        paths["d8_contributing_area"],
        paths["stream_raster"],
        paths["hydrology_gpkg"],
        paths["stream_order"],
        paths["subwatersheds"],
        stream_tree,
        stream_coord,
        hydro_dir,
        ncores,
        mpi_args,
        force,
        log,
    )

    # ── Step 6: Vectorise subwatersheds (GDAL/OGR) ────────────────────────────
    log.info("Step 6 of 7: Vectorising subwatersheds (GDAL Polygonize)")
    vectorize_subwatersheds(paths["subwatersheds"], paths["hydrology_gpkg"], force, log)

    # ── Step 7: Level path calculation ────────────────────────────────────────
    log.info("Step 7 of 7: Calculating level paths")
    calc_level_paths(paths["hydrology_gpkg"], "network_intersected", force, log)

    elapsed = time.time() - start_time
    log.info(f"D8 hydrology workflow complete in {pretty_duration(elapsed)}")
    return paths
