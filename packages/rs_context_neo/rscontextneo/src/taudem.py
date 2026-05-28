"""
TauDEM step functions for the RS Context Neo D8 hydrology pipeline.

NOTE: Eventually we may want to move some of this functionality to the `rs-commons`
package if it can be made generic enough to be reused by other projects. The Taudem
tool and the VBET tool both make calles to the taudem binaries so there is some
nice overlap there. For now, though, this is done internally to rs_context_neo to
keep it self-contained and not affect any other tools.

Each function in this module wraps a single TauDEM MPI command.  They are
called in sequence by :func:`~rscontextneo.src.hydrology.run_d8_hydrology`
and are not intended to be called directly.

All TauDEM commands are executed via ``mpiexec``.  The number of MPI ranks
and any extra flags are resolved through :func:`resolve_cores` and
:func:`resolve_mpi_args` using a layered priority scheme (explicit argument
→ environment variable → built-in default).

macOS note
----------
Open MPI's default OFI provider tries to communicate over ``en0``, which
fails on macOS with::

    OFI poll failed (default nic=en0: Input/output error)

:func:`apply_mpi_env` injects ``FI_PROVIDER=tcp`` (MPICH) and
``OMPI_MCA_btl=tcp,self`` (Open MPI) into ``os.environ`` before any
subprocess is spawned, routing traffic over the loopback interface instead.

Author:     Matt Reimer
Date:       2026-05-25
"""

import os
import sys
from typing import List

from osgeo import gdal, ogr, osr
from rscommons.hand import run_subprocess
from rsxml import Logger

from rscontextneo.src.utils.rasters import compress_inplace, skip_if_exists

# ── MPI tuneable constants ────────────────────────────────────────────────────
# Environment variable that controls MPI parallelism (shared with taudem package)
_CORES_ENV_VAR = "TAUDEM_CORES"
_DEFAULT_CORES = 2

# Environment variable for injecting extra mpiexec flags (space-separated).
# Example: export TAUDEM_MPI_ARGS="--oversubscribe"
_MPI_ARGS_ENV_VAR = "TAUDEM_MPI_ARGS"

# Environment variables injected into every TauDEM subprocess on macOS to
# route MPI traffic over the loopback interface instead of en0:
#   FI_PROVIDER=tcp   — MPICH / libfabric: use TCP provider, not OFI/verbs
#   OMPI_MCA_btl      — Open MPI: equivalent of --mca btl tcp,self
# Both are set together; whichever MPI flavour is installed uses its own and
# ignores the other.
_MACOS_MPI_ENV: dict[str, str] = {
    "FI_PROVIDER": "tcp",
    "OMPI_MCA_btl": "tcp,self",
}


# ── MPI helpers ───────────────────────────────────────────────────────────────


def resolve_cores(cores: int | None) -> int:
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
            Logger("TauDEM").warning(
                f"Invalid value for {_CORES_ENV_VAR!r}: {env_val!r} — using default {_DEFAULT_CORES}"
            )
    return _DEFAULT_CORES


def resolve_mpi_args(mpi_args: List[str] | None) -> List[str]:
    """
    Resolve the final list of extra ``mpiexec`` command-line flags.

    Priority (highest → lowest):
        1. Explicit ``mpi_args`` argument.
        2. ``TAUDEM_MPI_ARGS`` environment variable (space-separated string).
        3. Empty list (no extra flags on any platform).
    """
    if mpi_args is not None:
        return mpi_args
    env_val = os.environ.get(_MPI_ARGS_ENV_VAR)
    if env_val is not None:
        return env_val.split()
    return []


def apply_mpi_env(log: Logger) -> None:
    """
    Inject platform-specific environment variables so that TauDEM subprocesses
    route MPI traffic over the loopback interface rather than the primary
    network adapter.

    Only active on macOS (``sys.platform == 'darwin'``).  Pre-existing values
    set by the caller are never overwritten.
    """
    if sys.platform != "darwin":
        return

    applied = []
    for key, value in _MACOS_MPI_ENV.items():
        if key not in os.environ:
            os.environ[key] = value
            applied.append(f"{key}={value}")

    if applied:
        log.info(f"macOS MPI env applied: {'  '.join(applied)}")
    else:
        log.debug("macOS MPI env already set by caller — not overriding")


# ── TauDEM step functions ─────────────────────────────────────────────────────


def pitremove(
    dem_path: str,
    filled_dem_path: str,
    cwd: str,
    ncores: str,
    mpi_args: List[str],
    force: bool,
    log: Logger,
) -> None:
    """
    Fill pits/sinks in the DEM (TauDEM ``pitremove``).

    Removes topographic depressions that would otherwise trap flow and prevent
    the D8 algorithm from routing water to the watershed outlet.

    Parameters
    ----------
    dem_path : str
        Input DEM raster.
    filled_dem_path : str
        Output hydrologically-conditioned (pit-filled) DEM.
    cwd : str
        Working directory for the subprocess (TauDEM writes temp files here).
    ncores : str
        Number of MPI ranks as a string (e.g. ``'4'``).
    mpi_args : list[str]
        Extra flags to insert between ``mpiexec`` and the command name.
    force : bool
        Re-run even if the output already exists.
    log : Logger
        Caller-supplied logger.
    """
    if skip_if_exists(filled_dem_path, force, "pitremove", log):
        return

    log.info("Pit removal (TauDEM pitremove)")
    status = run_subprocess(
        cwd,
        [
            "mpiexec",
            "-n",
            ncores,
            *mpi_args,
            "pitremove",
            "-z",
            dem_path,
            "-fel",
            filled_dem_path,
        ],
    )
    _check_result(status, filled_dem_path, "pitremove")
    log.info(f"  → {filled_dem_path}")
    compress_inplace(filled_dem_path, log)


def d8flowdir(
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
    Compute D8 flow directions and slope (TauDEM ``d8flowdir``).

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
        See :func:`pitremove`.
    """
    if skip_if_exists(d8_flow_path, force, "d8flowdir", log) and os.path.isfile(
        d8_slope_path
    ):
        return

    log.info("D8 flow directions and slope (TauDEM d8flowdir)")
    status = run_subprocess(
        cwd,
        [
            "mpiexec",
            "-n",
            ncores,
            *mpi_args,
            "d8flowdir",
            "-fel",
            filled_dem_path,
            "-p",
            d8_flow_path,
            "-sd8",
            d8_slope_path,
        ],
    )
    _check_result(status, d8_flow_path, "d8flowdir")
    log.info(f"  → {d8_flow_path}")
    log.info(f"  → {d8_slope_path}")
    compress_inplace(d8_flow_path, log)
    compress_inplace(d8_slope_path, log)


def aread8(
    d8_flow_path: str,
    contrib_area_path: str,
    cwd: str,
    ncores: str,
    mpi_args: List[str],
    force: bool,
    log: Logger,
) -> None:
    """
    Compute D8 contributing area (TauDEM ``aread8``).

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
        See :func:`pitremove`.
    """
    if skip_if_exists(contrib_area_path, force, "aread8", log):
        return

    log.info("D8 contributing area (TauDEM aread8)")
    status = run_subprocess(
        cwd,
        [
            "mpiexec",
            "-n",
            ncores,
            *mpi_args,
            "aread8",
            "-p",
            d8_flow_path,
            "-ad8",
            contrib_area_path,
            "-nc",
        ],
    )
    _check_result(status, contrib_area_path, "aread8")
    log.info(f"  → {contrib_area_path}")
    compress_inplace(contrib_area_path, log)


def threshold(
    contrib_area_path: str,
    stream_raster_path: str,
    threshold_cells: int,
    cwd: str,
    ncores: str,
    mpi_args: List[str],
    force: bool,
    log: Logger,
) -> None:
    """
    Threshold contributing area to produce a stream raster (TauDEM ``threshold``).

    Marks cells with contributing area ≥ *threshold_cells* as stream (1);
    all others are 0.

    Parameters
    ----------
    contrib_area_path : str
        D8 contributing-area raster from Step 3.
    stream_raster_path : str
        Output binary stream-mask raster.
    threshold_cells : int
        Minimum upstream cell count for stream classification.
    cwd, ncores, mpi_args, force, log
        See :func:`pitremove`.
    """
    if skip_if_exists(stream_raster_path, force, "threshold", log):
        return

    log.info(f"Stream raster (TauDEM threshold = {threshold_cells:,} cells)")
    status = run_subprocess(
        cwd,
        [
            "mpiexec",
            "-n",
            ncores,
            *mpi_args,
            "threshold",
            "-ssa",
            contrib_area_path,
            "-src",
            stream_raster_path,
            "-thresh",
            str(threshold_cells),
        ],
    )
    _check_result(status, stream_raster_path, "threshold")
    log.info(f"  → {stream_raster_path}")
    compress_inplace(stream_raster_path, log)


def streamnet(
    d8_flow_path: str,
    filled_dem_path: str,
    contrib_area_path: str,
    stream_raster_path: str,
    gpkg_path: str,
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
    Extract the vector stream network (TauDEM ``streamnet``).

    Vectorises the thresholded stream raster into a reach network, assigns
    Strahler stream order, and delineates subwatersheds for each reach.

    TauDEM writes the network natively as a shapefile.  This function converts
    the result to GeoPackage so that consumers can use a single, self-contained
    vector file without managing auxiliary sidecar files.

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
    gpkg_path : str
        Output GeoPackage path (``network_intersected`` layer).
    stream_order_path : str
        Output Strahler stream-order raster.
    subwatersheds_path : str
        Output subwatershed raster (one value per reach).
    stream_tree_path : str
        TauDEM auxiliary text file describing tree topology.
    stream_coord_path : str
        TauDEM auxiliary text file with reach end-coordinates.
    cwd, ncores, mpi_args, force, log
        See :func:`pitremove`.
    """
    outputs_exist = (
        os.path.isfile(gpkg_path)
        and os.path.isfile(stream_order_path)
        and os.path.isfile(subwatersheds_path)
    )
    if not force and outputs_exist:
        log.info(
            "streamnet: all outputs already exist — skipping (use force=True to re-run)"
        )
        return

    log.info("Stream network extraction (TauDEM streamnet)")

    shp_path = os.path.splitext(gpkg_path)[0] + "_tmp.shp"

    status = run_subprocess(
        cwd,
        [
            "mpiexec",
            "-n",
            ncores,
            *mpi_args,
            "streamnet",
            "-p",
            d8_flow_path,
            "-fel",
            filled_dem_path,
            "-ad8",
            contrib_area_path,
            "-src",
            stream_raster_path,
            "-net",
            shp_path,
            "-ord",
            stream_order_path,
            "-tree",
            stream_tree_path,
            "-coord",
            stream_coord_path,
            "-w",
            subwatersheds_path,
        ],
    )
    _check_result(status, shp_path, "streamnet")
    log.info(f"  → {stream_order_path}")
    log.info(f"  → {subwatersheds_path}")
    compress_inplace(stream_order_path, log)
    compress_inplace(subwatersheds_path, log)

    log.info(f"Converting stream network shapefile → GeoPackage: {gpkg_path}")
    _shp_to_gpkg(shp_path, gpkg_path, layer_name="network_intersected")
    log.info(f"  → {gpkg_path} (layer: network_intersected)")

    _remove_shapefile(shp_path, log)


def vectorize_subwatersheds(
    subwatersheds_raster_path: str,
    gpkg_path: str,
    force: bool,
    log: Logger,
    layer_name: str = "subwatersheds",
) -> None:
    """
    Polygonise the subwatersheds raster and append it as a vector layer
    to an existing GeoPackage.

    TauDEM's ``streamnet`` writes one unique integer value per reach to
    ``subwatersheds.tif``.  This function converts those raster regions to
    polygons using :func:`gdal.Polygonize` and stores them in a layer alongside
    the ``network`` layer so that both the reach lines and their drainage
    polygons live in a single GeoPackage.

    The resulting layer has two fields:

    ``fid``
        OGR auto-assigned feature identifier.
    ``WSNO``
        Integer watershed number — matches ``WSNO`` / ``LINKNO`` in the
        ``network`` layer, allowing a direct table join.

    Parameters
    ----------
    subwatersheds_raster_path : str
        Path to ``hydrology/subwatersheds.tif`` produced by TauDEM.
    gpkg_path : str
        Path to the GeoPackage to append to (must already exist).
    force : bool
        If ``True``, delete and recreate the layer even if it already exists.
    log : Logger
        Caller-supplied logger.
    layer_name : str
        Name for the new layer inside the GeoPackage.  Default: ``'subwatersheds'``.

    Raises
    ------
    FileNotFoundError
        If *subwatersheds_raster_path* does not exist.
    RuntimeError
        If the GeoPackage cannot be opened for update, or if
        :func:`gdal.Polygonize` returns an error.
    """
    if not force and os.path.isfile(gpkg_path):
        check_ds = ogr.Open(gpkg_path)
        if check_ds is not None and check_ds.GetLayerByName(layer_name) is not None:
            log.info(
                f"subwatersheds vector layer already exists in {os.path.basename(gpkg_path)}"
                " — skipping (use force=True to re-run)"
            )
            check_ds = None
            return
        check_ds = None

    if not os.path.isfile(subwatersheds_raster_path):
        raise FileNotFoundError(
            f"Subwatersheds raster not found: {subwatersheds_raster_path}"
        )

    log.info(
        f"Vectorising Catchment Wings "
        f"({os.path.basename(subwatersheds_raster_path)} → layer: {layer_name})"
    )

    raster_ds = gdal.Open(subwatersheds_raster_path, gdal.GA_ReadOnly)
    if raster_ds is None:
        raise RuntimeError(
            f"GDAL could not open subwatersheds raster: {subwatersheds_raster_path}"
        )

    band = raster_ds.GetRasterBand(1)
    mask_band = band.GetMaskBand()  # 255 = valid data, 0 = nodata

    srs = osr.SpatialReference()
    srs.ImportFromWkt(raster_ds.GetProjection())

    vector_ds = ogr.Open(gpkg_path, 1)  # 1 = update
    if vector_ds is None:
        raise RuntimeError(f"Could not open GeoPackage for update: {gpkg_path}")

    if force:
        for i in range(vector_ds.GetLayerCount()):
            if vector_ds.GetLayer(i).GetName() == layer_name:
                vector_ds.DeleteLayer(i)
                break

    layer = vector_ds.CreateLayer(layer_name, srs=srs, geom_type=ogr.wkbMultiPolygon)
    layer.CreateField(ogr.FieldDefn("WSNO", ogr.OFTInteger))

    err = gdal.Polygonize(band, mask_band, layer, 0, [], callback=None)
    if err != gdal.CE_None:
        raise RuntimeError(
            f"gdal.Polygonize failed with error code {err}: {gdal.GetLastErrorMsg()}"
        )

    vector_ds.SyncToDisk()
    vector_ds = None
    raster_ds = None

    log.info(f"  → {gpkg_path} (layer: {layer_name})")


# ── Private helpers ───────────────────────────────────────────────────────────


def _check_result(status: int | None, expected_path: str, step_name: str) -> None:
    """
    Raise :class:`RuntimeError` if *status* is non-zero or *expected_path*
    was not created by the subprocess.
    """
    if status is not None and status != 0:
        raise RuntimeError(
            f"TauDEM {step_name} failed with exit code {status}. "
            "Check the log above for MPI/TauDEM error messages."
        )
    if not os.path.isfile(expected_path):
        raise RuntimeError(
            f"TauDEM {step_name} returned success but expected output was not created: "
            f"{expected_path}"
        )


def _shp_to_gpkg(
    shp_path: str, gpkg_path: str, layer_name: str = "network_intersected"
) -> None:
    """
    Convert a shapefile to a single-layer GeoPackage using GDAL VectorTranslate.

    Parameters
    ----------
    shp_path : str
        Source shapefile path.
    gpkg_path : str
        Destination GeoPackage path.  Overwritten if it already exists.
    layer_name : str
        Layer name inside the GeoPackage.  Default: ``'network_intersected'``.

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
        format="GPKG",
        layerName=layer_name,
    )
    if result is None:
        raise RuntimeError(
            f"GDAL VectorTranslate failed converting {shp_path} → {gpkg_path}. "
            f"GDAL error: {gdal.GetLastErrorMsg()}"
        )
    result = None  # dereference / flush


def _remove_shapefile(shp_path: str, log: Logger) -> None:
    """
    Delete a shapefile and all its sidecar files (``.dbf``, ``.prj``,
    ``.shx``, ``.cpg``, ``.qpj``).

    Failures are logged as warnings rather than raising exceptions so that a
    cleanup hiccup does not abort an otherwise-successful run.
    """
    extensions = [".shp", ".dbf", ".prj", ".shx", ".cpg", ".qpj"]
    base = os.path.splitext(shp_path)[0]
    for ext in extensions:
        candidate = base + ext
        if os.path.isfile(candidate):
            try:
                os.remove(candidate)
            except OSError as exc:
                log.warning(
                    f"Could not remove temporary shapefile sidecar {candidate}: {exc}"
                )
