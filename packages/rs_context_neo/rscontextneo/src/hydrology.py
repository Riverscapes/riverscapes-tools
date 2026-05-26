"""
D8 Hydrology workflow for RS Context Neo.

Implements the standard TauDEM D8 processing chain:

    Step 1 - Pit removal        pitremove
                                Fills sinks in the DEM so that flow is
                                guaranteed to route to the edge of the domain.

    Step 2 - D8 flow direction  d8flowdir
                                Assigns each cell a single downstream
                                direction (one of 8 compass headings) and
                                records the corresponding slope.

    Step 3 - Contributing area  aread8
                                Counts how many upstream cells drain through
                                each cell — the fundamental measure of
                                accumulated flow.

    Step 4 - Stream raster      threshold
                                Burns a 1 where contributing area exceeds
                                *threshold* cells; 0 elsewhere.  Adjust the
                                threshold to control stream network density.

    Step 5 - Stream network     streamnet
                                Extracts the vector stream network, assigns
                                Strahler stream order to each reach, and
                                delineates subwatersheds.

All TauDEM commands are executed via ``mpiexec``.  The number of MPI ranks
(cores) and any extra MPI flags are resolved through a layered priority scheme
described in :func:`_resolve_cores` and :func:`_resolve_mpi_args`.

macOS note
----------
Open MPI's default OFI (OpenFabrics) fabric provider tries to communicate over
the primary network interface (``en0``).  On macOS this fails during
``MPI_Finalize`` with::

    OFI poll failed (default nic=en0: Input/output error)

causing a non-zero exit code even when the computation itself succeeded.
The fix is applied via environment variables set in :func:`_apply_mpi_env`
(``FI_PROVIDER=tcp`` for MPICH, ``OMPI_MCA_btl=tcp,self`` for Open MPI)
so it works regardless of which MPI flavour is installed — ``--mca`` is
Open MPI-only and rejected by MPICH.
Override by pre-setting these variables in your shell or ``.env`` file.

Output layout (all relative to *output_folder*):
    hydrology/dem_filled.tif            pit-filled DEM  (intermediate)
    hydrology/d8_flow.tif               D8 flow direction  (intermediate)
    hydrology/d8_slope.tif              D8 slope  (intermediate)
    hydrology/d8_contributing_area.tif  D8 contributing area  (intermediate)
    hydrology/stream_raster.tif         binary stream mask  (product)
    hydrology/stream_order.tif          Strahler stream-order raster  (product)
    hydrology/stream_network.gpkg       vector stream network  (product)
    hydrology/subwatersheds.tif         subwatershed raster  (product)

Author:     Riverscapes
Date:       2026-05-25
"""
import os
import sys
import time
from typing import Dict, List

from osgeo import gdal
from rsxml import Logger
from rsxml.util import safe_makedirs, pretty_duration
from rscommons.hand import run_subprocess

# ── Output relative paths (relative to output_folder) ─────────────────────────
FILLED_DEM_RELPATH       = 'hydrology/dem_filled.tif'
D8_FLOW_RELPATH          = 'hydrology/d8_flow.tif'
D8_SLOPE_RELPATH         = 'hydrology/d8_slope.tif'
D8_CONTRIB_AREA_RELPATH  = 'hydrology/d8_contributing_area.tif'
STREAM_RASTER_RELPATH    = 'hydrology/stream_raster.tif'
STREAM_ORDER_RELPATH     = 'hydrology/stream_order.tif'
STREAM_NETWORK_RELPATH   = 'hydrology/stream_network.gpkg'
SUBWATERSHEDS_RELPATH    = 'hydrology/subwatersheds.tif'

# Auxiliary TauDEM text outputs written alongside streamnet products
_STREAM_TREE_RELPATH     = 'hydrology/stream_tree.dat'
_STREAM_COORD_RELPATH    = 'hydrology/stream_coord.dat'

# ── Tuneable defaults ──────────────────────────────────────────────────────────
# The threshold is the minimum number of upstream cells required for a cell to
# be classified as a stream channel (units: cells).  At 1-metre resolution one
# cell = 1 m², so multiply directly to get contributing area in m² (50 000
# cells ≈ 0.05 km²).  At coarser resolutions scale by (cell_size_m)².
# Higher values → coarser, sparser networks; lower values → finer, denser
# networks.  50 000 is a reasonable starting point for 1-metre DEMs — inspect
# the stream raster and adjust as needed.  See docs/STREAM_THRESHOLD.md.
DEFAULT_THRESHOLD = 50_000

# Environment variable that controls MPI parallelism (shared with taudem package)
_CORES_ENV_VAR    = 'TAUDEM_CORES'
_DEFAULT_CORES    = 2

# Environment variable for injecting extra mpiexec flags (space-separated).
# Example: export TAUDEM_MPI_ARGS="--oversubscribe"
_MPI_ARGS_ENV_VAR = 'TAUDEM_MPI_ARGS'

# Environment variables injected into every TauDEM subprocess on macOS to
# route MPI traffic over the loopback interface instead of en0:
#   FI_PROVIDER=tcp   — MPICH / libfabric: use TCP provider, not OFI/verbs
#   OMPI_MCA_btl      — Open MPI: equivalent of --mca btl tcp,self
# Both are set together; whichever MPI flavour is installed uses its own and
# ignores the other.
_MACOS_MPI_ENV: dict[str, str] = {
    'FI_PROVIDER':    'tcp',
    'OMPI_MCA_btl':   'tcp,self',
}


# ── Public entry point ─────────────────────────────────────────────────────────

def run_d8_hydrology(
    dem_path: str,
    output_folder: str,
    *,
    threshold: int = DEFAULT_THRESHOLD,
    cores: int | None = None,
    mpi_args: List[str] | None = None,
    force: bool = False,
) -> Dict[str, str]:
    """
    Run the full D8 hydrology processing chain on a DEM.

    Each step is skipped automatically if its output file already exists and
    *force* is ``False``.  Pass ``force=True`` to unconditionally re-run every
    step from the beginning (e.g. when you have changed the threshold or
    replaced the DEM).

    Parameters
    ----------
    dem_path : str
        Absolute path to the input DEM raster (GeoTIFF, projected CRS).
        This is typically ``<output_folder>/topography/dem.tif`` produced by
        :func:`~rscontextneo.src.fetch_dem.fetch_dem_from_3dep`.
    output_folder : str
        Root of the RS Context Neo project folder.  All hydrology outputs are
        written under ``<output_folder>/hydrology/``.
    threshold : int
        Minimum contributing-area cell count for stream classification
        (units: cells).  At 1-metre resolution, 1 cell = 1 m², so
        50 000 cells ≈ 0.05 km².  At coarser resolutions multiply by
        (cell_size_m)² to convert to m².
        Default: :data:`DEFAULT_THRESHOLD` (50 000 cells).
    cores : int or None
        Number of MPI ranks to use.  ``None`` (default) reads the
        ``TAUDEM_CORES`` environment variable; falls back to 2.
    mpi_args : list[str] or None
        Extra command-line flags inserted between ``mpiexec`` and the TauDEM
        command name (e.g. ``['--oversubscribe']``).  ``None`` (default) reads
        ``TAUDEM_MPI_ARGS`` from the environment; falls back to an empty list.
        The macOS OFI fix is handled via :func:`_apply_mpi_env` (env vars),
        not command-line flags, so no special value is needed here for that.
        Pass an empty list ``[]`` to suppress any ``TAUDEM_MPI_ARGS`` setting.
    force : bool
        Re-run all steps even if outputs already exist.  Default: ``False``.

    Returns
    -------
    dict[str, str]
        Mapping of short product names to absolute file paths::

            {
                'dem_filled':          '<output_folder>/hydrology/dem_filled.tif',
                'd8_flow':             '<output_folder>/hydrology/d8_flow.tif',
                'd8_slope':            '<output_folder>/hydrology/d8_slope.tif',
                'd8_contributing_area':'<output_folder>/hydrology/d8_contributing_area.tif',
                'stream_raster':       '<output_folder>/hydrology/stream_raster.tif',
                'stream_order':        '<output_folder>/hydrology/stream_order.tif',
                'stream_network':      '<output_folder>/hydrology/stream_network.gpkg',
                'subwatersheds':       '<output_folder>/hydrology/subwatersheds.tif',
            }

    Raises
    ------
    FileNotFoundError
        If *dem_path* does not exist.
    RuntimeError
        If any TauDEM command exits with a non-zero return code or fails to
        produce its expected output file.
    """
    log = Logger('D8 Hydrology')
    start_time = time.time()

    # ── Pre-flight checks ──────────────────────────────────────────────────────
    if not os.path.isfile(dem_path):
        raise FileNotFoundError(f'Input DEM not found: {dem_path}')

    ncores   = str(_resolve_cores(cores))
    mpi_args = _resolve_mpi_args(mpi_args)

    log.info(f'Starting D8 hydrology workflow  |  DEM: {dem_path}')
    log.info(f'MPI cores: {ncores}  |  threshold: {threshold:,}  |  force: {force}')
    if mpi_args:
        log.info(f'Extra MPI args: {" ".join(mpi_args)}')

    # Apply platform-specific MPI environment variables so that subprocesses
    # inherit them via os.environ.  On macOS this routes MPI traffic over the
    # loopback interface, avoiding the en0 OFI failure for both MPICH and
    # Open MPI (see _apply_mpi_env for details).
    _apply_mpi_env(log)

    hydro_dir = os.path.join(output_folder, 'hydrology')
    safe_makedirs(hydro_dir)

    # Resolve all output paths up-front so every step can reference them clearly
    paths = {
        'dem_filled':           os.path.join(output_folder, FILLED_DEM_RELPATH),
        'd8_flow':              os.path.join(output_folder, D8_FLOW_RELPATH),
        'd8_slope':             os.path.join(output_folder, D8_SLOPE_RELPATH),
        'd8_contributing_area': os.path.join(output_folder, D8_CONTRIB_AREA_RELPATH),
        'stream_raster':        os.path.join(output_folder, STREAM_RASTER_RELPATH),
        'stream_order':         os.path.join(output_folder, STREAM_ORDER_RELPATH),
        'stream_network':       os.path.join(output_folder, STREAM_NETWORK_RELPATH),
        'subwatersheds':        os.path.join(output_folder, SUBWATERSHEDS_RELPATH),
    }
    stream_tree  = os.path.join(output_folder, _STREAM_TREE_RELPATH)
    stream_coord = os.path.join(output_folder, _STREAM_COORD_RELPATH)

    # ── Step 1: Pit removal ────────────────────────────────────────────────────
    _pitremove(dem_path, paths['dem_filled'], hydro_dir, ncores, mpi_args, force, log)

    # ── Step 2: D8 flow directions & slope ────────────────────────────────────
    _d8flowdir(paths['dem_filled'], paths['d8_flow'], paths['d8_slope'],
               hydro_dir, ncores, mpi_args, force, log)

    # ── Step 3: D8 contributing area ──────────────────────────────────────────
    _aread8(paths['d8_flow'], paths['d8_contributing_area'],
            hydro_dir, ncores, mpi_args, force, log)

    # ── Step 4: Stream raster (threshold) ─────────────────────────────────────
    _threshold(paths['d8_contributing_area'], paths['stream_raster'],
               threshold, hydro_dir, ncores, mpi_args, force, log)

    # ── Step 5: Stream network extraction ─────────────────────────────────────
    _streamnet(
        paths['d8_flow'],
        paths['dem_filled'],
        paths['d8_contributing_area'],
        paths['stream_raster'],
        paths['stream_network'],
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

    elapsed = time.time() - start_time
    log.info(f'D8 hydrology workflow complete in {pretty_duration(elapsed)}')
    return paths


# ── Step implementations ───────────────────────────────────────────────────────

def _pitremove(
    dem_path: str,
    filled_dem_path: str,
    cwd: str,
    ncores: str,
    mpi_args: List[str],
    force: bool,
    log: Logger,
) -> None:
    """
    Step 1 — Fill pits/sinks in the DEM (TauDEM ``pitremove``).

    Removes topographic depressions that would otherwise trap flow and prevent
    the D8 algorithm from routing water to the watershed outlet.

    Parameters
    ----------
    dem_path : str
        Input DEM raster.
    filled_dem_path : str
        Output hydrologically-conditioned (pit-filled) DEM.
    cwd : str
        Working directory passed to the subprocess (TauDEM writes temp files here).
    ncores : str
        Number of MPI ranks as a string (e.g. ``'4'``).
    mpi_args : list[str]
        Extra flags to insert between ``mpiexec`` and the command name.
    force : bool
        Re-run even if the output already exists.
    log : Logger
        Caller-supplied logger.
    """
    if _skip_if_exists(filled_dem_path, force, 'pitremove', log):
        return

    log.info('Step 1 — Pit removal (pitremove)')
    status = run_subprocess(cwd, [
        'mpiexec', '-n', ncores,
        *mpi_args,
        'pitremove',
        '-z', dem_path,
        '-fel', filled_dem_path,
    ])
    _check_result(status, filled_dem_path, 'pitremove')
    log.info(f'  → {filled_dem_path}')


def _d8flowdir(
    filled_dem_path: str,
    d8_flow_path: str,
    d8_slope_path: str,
    cwd: str,
    ncores: str,
    mpi_args: List[str],
    force: bool,
    log: Logger,
) -> None:
    """
    Step 2 — Compute D8 flow directions and slope (TauDEM ``d8flowdir``).

    Assigns each cell a single downstream neighbour (one of 8 directions
    encoded as an integer 1-8) and records the downslope gradient.

    Parameters
    ----------
    filled_dem_path : str
        Pit-filled DEM from Step 1.
    d8_flow_path : str
        Output D8 flow-direction raster.
    d8_slope_path : str
        Output D8 slope raster (rise / run).
    cwd, ncores, mpi_args, force, log
        See :func:`_pitremove`.
    """
    # Both outputs must exist for the step to be considered done
    if _skip_if_exists(d8_flow_path, force, 'd8flowdir', log) and os.path.isfile(d8_slope_path):
        return

    log.info('Step 2 — D8 flow directions and slope (d8flowdir)')
    status = run_subprocess(cwd, [
        'mpiexec', '-n', ncores,
        *mpi_args,
        'd8flowdir',
        '-fel', filled_dem_path,
        '-p',   d8_flow_path,
        '-sd8', d8_slope_path,
    ])
    _check_result(status, d8_flow_path, 'd8flowdir')
    log.info(f'  → {d8_flow_path}')
    log.info(f'  → {d8_slope_path}')


def _aread8(
    d8_flow_path: str,
    contrib_area_path: str,
    cwd: str,
    ncores: str,
    mpi_args: List[str],
    force: bool,
    log: Logger,
) -> None:
    """
    Step 3 — Compute D8 contributing area (TauDEM ``aread8``).

    Accumulates cell counts upstream of each cell following the D8 flow
    directions.  The ``-nc`` flag omits edge contamination correction,
    which is appropriate for normal watershed analysis.

    Parameters
    ----------
    d8_flow_path : str
        D8 flow-direction raster from Step 2.
    contrib_area_path : str
        Output D8 contributing-area (flow accumulation) raster.
    cwd, ncores, mpi_args, force, log
        See :func:`_pitremove`.
    """
    if _skip_if_exists(contrib_area_path, force, 'aread8', log):
        return

    log.info('Step 3 — D8 contributing area (aread8)')
    status = run_subprocess(cwd, [
        'mpiexec', '-n', ncores,
        *mpi_args,
        'aread8',
        '-p',   d8_flow_path,
        '-ad8', contrib_area_path,
        '-nc',
    ])
    _check_result(status, contrib_area_path, 'aread8')
    log.info(f'  → {contrib_area_path}')


def _threshold(
    contrib_area_path: str,
    stream_raster_path: str,
    threshold: int,
    cwd: str,
    ncores: str,
    mpi_args: List[str],
    force: bool,
    log: Logger,
) -> None:
    """
    Step 4 — Threshold contributing area to produce a stream raster (TauDEM ``threshold``).

    Marks cells with contributing area ≥ *threshold* as stream cells (value 1);
    all other cells are 0.

    The *threshold* value is the single most impactful tuning parameter in the
    entire workflow.  Too low → dense, noisy network; too high → coarse network
    that misses small tributaries.  Inspect the stream raster against the DEM
    hillshade to find an appropriate value for your landscape.

    Parameters
    ----------
    contrib_area_path : str
        D8 contributing-area raster from Step 3.
    stream_raster_path : str
        Output binary stream-mask raster.
    threshold : int
        Minimum upstream cell count for stream classification (units: cells).
        Cells with contributing area ≥ *threshold* are marked as stream (1);
        all others are 0.  At 1-metre resolution, 50 000 cells ≈ 0.05 km².
    cwd, ncores, mpi_args, force, log
        See :func:`_pitremove`.
    """
    if _skip_if_exists(stream_raster_path, force, 'threshold', log):
        return

    log.info(f'Step 4 — Stream raster (threshold = {threshold:,} cells)')
    status = run_subprocess(cwd, [
        'mpiexec', '-n', ncores,
        *mpi_args,
        'threshold',
        '-ssa',    contrib_area_path,
        '-src',    stream_raster_path,
        '-thresh', str(threshold),
    ])
    _check_result(status, stream_raster_path, 'threshold')
    log.info(f'  → {stream_raster_path}')


def _streamnet(
    d8_flow_path: str,
    filled_dem_path: str,
    contrib_area_path: str,
    stream_raster_path: str,
    stream_network_path: str,
    stream_order_path: str,
    subwatersheds_path: str,
    stream_tree_path: str,
    stream_coord_path: str,
    cwd: str,
    ncores: str,
    mpi_args: List[str],
    force: bool,
    log: Logger,
) -> None:
    """
    Step 5 — Extract the vector stream network (TauDEM ``streamnet``).

    Vectorises the thresholded stream raster into a reach network, assigns
    Strahler stream order, and delineates subwatersheds for each reach.

    TauDEM writes the network natively as a shapefile.  This function converts
    the result to GeoPackage so that consumers can use a single, self-contained
    vector file without managing auxiliary ``.dbf``/``.prj``/``.shx`` sidecar
    files.

    Parameters
    ----------
    d8_flow_path : str
        D8 flow-direction raster from Step 2.
    filled_dem_path : str
        Pit-filled DEM from Step 1.
    contrib_area_path : str
        D8 contributing-area raster from Step 3.
    stream_raster_path : str
        Binary stream mask from Step 4.
    stream_network_path : str
        Output GeoPackage path for the vector stream network.
    stream_order_path : str
        Output Strahler stream-order raster.
    subwatersheds_path : str
        Output subwatershed raster (one cell-value per reach).
    stream_tree_path : str
        TauDEM auxiliary text file describing tree topology.
    stream_coord_path : str
        TauDEM auxiliary text file with reach end-coordinates.
    cwd, ncores, mpi_args, force, log
        See :func:`_pitremove`.
    """
    # All three raster/vector products must exist for the step to be skipped
    outputs_exist = (
        os.path.isfile(stream_network_path)
        and os.path.isfile(stream_order_path)
        and os.path.isfile(subwatersheds_path)
    )
    if not force and outputs_exist:
        log.info('streamnet: all outputs already exist — skipping (use force=True to re-run)')
        return

    log.info('Step 5 — Stream network extraction (streamnet)')

    # TauDEM writes its network as a shapefile; we use a temporary path and
    # convert to GeoPackage afterwards for a cleaner single-file output.
    shp_path = os.path.splitext(stream_network_path)[0] + '_tmp.shp'

    status = run_subprocess(cwd, [
        'mpiexec', '-n', ncores,
        *mpi_args,
        'streamnet',
        '-p',     d8_flow_path,
        '-fel',   filled_dem_path,
        '-ad8',   contrib_area_path,
        '-src',   stream_raster_path,
        '-net',   shp_path,
        '-ord',   stream_order_path,
        '-tree',  stream_tree_path,
        '-coord', stream_coord_path,
        '-w',     subwatersheds_path,
    ])
    _check_result(status, shp_path, 'streamnet')
    log.info(f'  → {stream_order_path}')
    log.info(f'  → {subwatersheds_path}')

    # Convert shapefile → GeoPackage
    log.info(f'Converting stream network shapefile → GeoPackage: {stream_network_path}')
    _shp_to_gpkg(shp_path, stream_network_path, layer_name='network')
    log.info(f'  → {stream_network_path}')

    # Clean up temporary shapefile sidecar files
    _remove_shapefile(shp_path, log)


# ── Private helpers ────────────────────────────────────────────────────────────

def _resolve_cores(cores: int | None) -> int:
    """
    Determine the number of MPI ranks to use.

    Priority (highest → lowest):
        1. Explicit ``cores`` argument.
        2. ``TAUDEM_CORES`` environment variable.
        3. Built-in default (:data:`_DEFAULT_CORES`).
    """
    if cores is not None:
        return cores
    env_val = os.environ.get(_CORES_ENV_VAR)
    if env_val is not None:
        try:
            return int(env_val)
        except ValueError:
            Logger('D8 Hydrology').warning(
                f'Invalid value for {_CORES_ENV_VAR!r}: {env_val!r} — using default {_DEFAULT_CORES}'
            )
    return _DEFAULT_CORES


def _default_mpi_args() -> List[str]:
    """
    Return any extra ``mpiexec`` command-line flags to use by default.

    The macOS OFI fix is handled via environment variables in
    :func:`_apply_mpi_env` rather than command-line flags, because the
    ``--mca`` flag is Open MPI-specific and rejected by MPICH.  This
    function therefore returns an empty list on all platforms; it exists
    so callers can still inject flags (e.g. ``--oversubscribe``) through
    the ``TAUDEM_MPI_ARGS`` environment variable or the ``mpi_args``
    argument without touching platform-detection logic.
    """
    return []


def _resolve_mpi_args(mpi_args: List[str] | None) -> List[str]:
    """
    Resolve the final list of extra ``mpiexec`` command-line flags.

    Priority (highest → lowest):
        1. Explicit ``mpi_args`` argument to :func:`run_d8_hydrology`.
        2. ``TAUDEM_MPI_ARGS`` environment variable (space-separated string).
        3. :func:`_default_mpi_args` (empty list on all platforms).
    """
    if mpi_args is not None:
        return mpi_args
    env_val = os.environ.get(_MPI_ARGS_ENV_VAR)
    if env_val is not None:
        return env_val.split()
    return _default_mpi_args()


def _apply_mpi_env(log: Logger) -> None:
    """
    Inject platform-specific environment variables so that TauDEM subprocesses
    route MPI traffic over the loopback interface rather than the primary
    network adapter.

    On macOS, ``mpiexec`` (whether MPICH or Open MPI) defaults to the OFI
    (OpenFabrics) fabric provider, which tries to use ``en0`` (Wi-Fi /
    Ethernet) for inter-rank communication.  On macOS this fails during
    ``MPI_Finalize`` with::

        OFI poll failed (default nic=en0: Input/output error)

    Two complementary environment variables are set so the fix works
    regardless of which MPI flavour is installed:

    ``FI_PROVIDER=tcp``
        Tells libfabric (used by MPICH's OFI netmod) to use the portable
        TCP provider instead of the OFI/verbs provider.  Recognised by
        MPICH; silently ignored by Open MPI builds that don't use libfabric.

    ``OMPI_MCA_btl=tcp,self``
        Tells Open MPI to use only the TCP and self byte-transfer layers,
        skipping the OFI BTL entirely.  Equivalent to passing
        ``--mca btl tcp,self`` on the command line but without requiring
        a flag that MPICH would reject.  Silently ignored by MPICH.

    Both variables are written into :data:`os.environ` so that any child
    process spawned afterwards inherits them automatically.  Pre-existing
    values (set by the user or a wrapper script) are never overwritten.
    """
    if sys.platform != 'darwin':
        return

    applied = []
    for key, value in _MACOS_MPI_ENV.items():
        if key not in os.environ:
            os.environ[key] = value
            applied.append(f'{key}={value}')

    if applied:
        log.info(f'macOS MPI env applied: {"  ".join(applied)}')
    else:
        log.debug('macOS MPI env already set by caller — not overriding')


def _skip_if_exists(path: str, force: bool, step_name: str, log: Logger) -> bool:
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


def _check_result(status: int | None, expected_path: str, step_name: str) -> None:
    """
    Raise :class:`RuntimeError` if *status* is non-zero or *expected_path*
    was not created by the subprocess.
    """
    if status is not None and status != 0:
        raise RuntimeError(
            f'TauDEM {step_name} failed with exit code {status}. '
            'Check the log above for MPI/TauDEM error messages.'
        )
    if not os.path.isfile(expected_path):
        raise RuntimeError(
            f'TauDEM {step_name} returned success but expected output was not created: '
            f'{expected_path}'
        )


def _shp_to_gpkg(shp_path: str, gpkg_path: str, layer_name: str = 'network') -> None:
    """
    Convert a shapefile to a single-layer GeoPackage using GDAL VectorTranslate.

    Parameters
    ----------
    shp_path : str
        Source shapefile path (the ``.shp`` file; OGR resolves sidecars).
    gpkg_path : str
        Destination GeoPackage path.  Overwritten if it already exists.
    layer_name : str
        Layer name inside the GeoPackage.  Default: ``'network'``.

    Raises
    ------
    RuntimeError
        If GDAL VectorTranslate returns ``None`` (conversion failed).
    """
    if os.path.exists(gpkg_path):
        os.remove(gpkg_path)

    result = gdal.VectorTranslate(
        gpkg_path,
        shp_path,
        format='GPKG',
        layerName=layer_name,
    )
    if result is None:
        raise RuntimeError(
            f'GDAL VectorTranslate failed converting {shp_path} → {gpkg_path}. '
            f'GDAL error: {gdal.GetLastErrorMsg()}'
        )
    result = None  # dereference / flush


def _remove_shapefile(shp_path: str, log: Logger) -> None:
    """
    Delete a shapefile and all its sidecar files (``.dbf``, ``.prj``,
    ``.shx``, ``.cpg``, ``.qpj``).

    Failures are logged as warnings rather than raising exceptions so that a
    cleanup hiccup does not abort an otherwise-successful run.
    """
    extensions = ['.shp', '.dbf', '.prj', '.shx', '.cpg', '.qpj']
    base = os.path.splitext(shp_path)[0]
    for ext in extensions:
        candidate = base + ext
        if os.path.isfile(candidate):
            try:
                os.remove(candidate)
            except OSError as exc:
                log.warning(f'Could not remove temporary shapefile sidecar {candidate}: {exc}')
