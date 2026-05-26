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
    Step 1a  pitremove (TauDEM)
             Fill topographic sinks so flow routes to the domain edge.

    Step 1b  BreachDepressionsLeastCost (WhiteboxTools)
             Alternative hydrological conditioning via least-cost breaching.
             Run in parallel with Step 1a for comparison.

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

Output layout (all relative to *output_folder*):
    hydrology/dem_filled.tif            pit-filled DEM (TauDEM pitremove)
    hydrology/dem_breach.tif            breach-conditioned DEM (WBT BreachDepressionsLeastCost)
    hydrology/d8_flow.tif               D8 flow direction
    hydrology/d8_slope.tif              D8 slope
    hydrology/d8_contributing_area.tif  D8 contributing area
    hydrology/stream_raster.tif         binary stream mask
    hydrology/stream_order.tif          Strahler stream-order raster
    hydrology/hydrology.gpkg            vector network + subwatersheds (two layers)
    hydrology/subwatersheds.tif         subwatershed raster

Author:     Riverscapes
Date:       2026-05-25
"""
import os
import time
from typing import Dict, List

from rsxml import Logger
from rsxml.util import safe_makedirs, pretty_duration

from rscontextneo.src.taudem import (
    pitremove,
    d8flowdir,
    aread8,
    threshold as taudem_threshold,
    streamnet,
    vectorize_subwatersheds,
    resolve_cores,
    resolve_mpi_args,
    apply_mpi_env,
)
from rscontextneo.src.wbt import breach_depressions_least_cost

# ── Output relative paths (relative to output_folder) ─────────────────────────
FILLED_DEM_RELPATH      = 'hydrology/dem_filled.tif'
DEM_BREACH_RELPATH      = 'hydrology/dem_breach.tif'
D8_FLOW_RELPATH         = 'hydrology/d8_flow.tif'
D8_SLOPE_RELPATH        = 'hydrology/d8_slope.tif'
D8_CONTRIB_AREA_RELPATH = 'hydrology/d8_contributing_area.tif'
STREAM_RASTER_RELPATH   = 'hydrology/stream_raster.tif'
STREAM_ORDER_RELPATH    = 'hydrology/stream_order.tif'
HYDROLOGY_GPKG_RELPATH  = 'hydrology/hydrology.gpkg'
SUBWATERSHEDS_RELPATH   = 'hydrology/subwatersheds.tif'

# Auxiliary TauDEM text outputs written alongside streamnet products
_STREAM_TREE_RELPATH  = 'hydrology/stream_tree.dat'
_STREAM_COORD_RELPATH = 'hydrology/stream_coord.dat'

# ── Tuneable defaults ──────────────────────────────────────────────────────────
# Minimum upstream cell count for stream classification.  At 1 m resolution
# one cell = 1 m², so 50 000 cells ≈ 0.05 km².  Scale by (cell_size_m)² for
# coarser DEMs.  Higher → sparser network; lower → denser network.
DEFAULT_THRESHOLD = 50_000

# Maximum search distance (cells) for the WBT least-cost breach path.
# 500 cells ≈ 500 m at 1 m resolution.  Scale proportionally for coarser DEMs.
DEFAULT_BREACH_DIST = 500


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
                'hydrology_gpkg':       '<output_folder>/hydrology/hydrology.gpkg',
                'subwatersheds':        '<output_folder>/hydrology/subwatersheds.tif',
            }

    Raises
    ------
    FileNotFoundError
        If *dem_path* does not exist.
    RuntimeError
        If any processing step fails or does not produce its expected output.
    """
    log = Logger('D8 Hydrology')
    start_time = time.time()

    if not os.path.isfile(dem_path):
        raise FileNotFoundError(f'Input DEM not found: {dem_path}')

    ncores   = str(resolve_cores(cores))
    mpi_args = resolve_mpi_args(mpi_args)

    log.info(f'Starting D8 hydrology workflow  |  DEM: {dem_path}')
    log.info(f'MPI cores: {ncores}  |  threshold: {threshold:,}  |  breach_dist: {breach_dist}  |  force: {force}')
    if mpi_args:
        log.info(f'Extra MPI args: {" ".join(mpi_args)}')

    apply_mpi_env(log)

    hydro_dir = os.path.join(output_folder, 'hydrology')
    safe_makedirs(hydro_dir)

    paths = {
        'dem_filled':           os.path.join(output_folder, FILLED_DEM_RELPATH),
        'dem_breach':           os.path.join(output_folder, DEM_BREACH_RELPATH),
        'd8_flow':              os.path.join(output_folder, D8_FLOW_RELPATH),
        'd8_slope':             os.path.join(output_folder, D8_SLOPE_RELPATH),
        'd8_contributing_area': os.path.join(output_folder, D8_CONTRIB_AREA_RELPATH),
        'stream_raster':        os.path.join(output_folder, STREAM_RASTER_RELPATH),
        'stream_order':         os.path.join(output_folder, STREAM_ORDER_RELPATH),
        'hydrology_gpkg':       os.path.join(output_folder, HYDROLOGY_GPKG_RELPATH),
        'subwatersheds':        os.path.join(output_folder, SUBWATERSHEDS_RELPATH),
    }
    stream_tree  = os.path.join(output_folder, _STREAM_TREE_RELPATH)
    stream_coord = os.path.join(output_folder, _STREAM_COORD_RELPATH)

    # ── Step 1a: Pit removal (TauDEM) ─────────────────────────────────────────
    pitremove(dem_path, paths['dem_filled'], hydro_dir, ncores, mpi_args, force, log)

    # ── Step 1b: Breach depressions least-cost (WhiteboxTools) ────────────────
    breach_depressions_least_cost(dem_path, paths['dem_breach'], breach_dist, force, log)

    # ── Step 2: D8 flow directions & slope (TauDEM) ───────────────────────────
    d8flowdir(paths['dem_filled'], paths['d8_flow'], paths['d8_slope'],
              hydro_dir, ncores, mpi_args, force, log)

    # ── Step 3: D8 contributing area (TauDEM) ─────────────────────────────────
    aread8(paths['d8_flow'], paths['d8_contributing_area'],
           hydro_dir, ncores, mpi_args, force, log)

    # ── Step 4: Stream raster (TauDEM) ────────────────────────────────────────
    taudem_threshold(paths['d8_contributing_area'], paths['stream_raster'],
              threshold, hydro_dir, ncores, mpi_args, force, log)

    # ── Step 5: Stream network extraction (TauDEM) ────────────────────────────
    streamnet(
        paths['d8_flow'],
        paths['dem_filled'],
        paths['d8_contributing_area'],
        paths['stream_raster'],
        paths['hydrology_gpkg'],
        paths['stream_order'],
        paths['subwatersheds'],
        stream_tree,
        stream_coord,
        hydro_dir,
        ncores,
        mpi_args,
        force,
        log,
    )

    # ── Step 6: Vectorise subwatersheds (GDAL/OGR) ────────────────────────────
    vectorize_subwatersheds(paths['subwatersheds'], paths['hydrology_gpkg'], force, log)

    elapsed = time.time() - start_time
    log.info(f'D8 hydrology workflow complete in {pretty_duration(elapsed)}')
    return paths
