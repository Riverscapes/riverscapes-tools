"""
Name:       Riverscapes Context Neo

Purpose:    Build a Riverscapes Context project for a single watershed.
            Regional configuration (DEM source, hydrology parameters, optional
            layers) is loaded from a JSON profile file; only the run-specific
            values (AOI, output folder, force/debug flags) are passed on the
            command line.

Author:      Matt Reimer
Date:       2026-05-20
"""

import argparse
import os
import sys
import time
import traceback

from osgeo import gdal
from rscommons import Timer, initGDALOGRErrors
from rsxml import Logger, dotenv
from rsxml.util import parse_metadata, pretty_duration, safe_makedirs

from rscontextneo.__version__ import __version__
from rscontextneo.src.config import (
    AppConfig,
    CogClipLayerConfig,
    LayerConfig,
    RSContextNeoConfig,
    S3TablesLayerConfig,
    WcsRasterLayerConfig,
    WfsLayerConfig,
    load_config,
)
from rscontextneo.src.fetch_dem import (
    DEM_RELPATH,
    HILLSHADE_RELPATH,
    SLOPE_RELPATH,
    fetch_dem_from_3dep,
)
from rscontextneo.src.fetch_dem_wcs import fetch_dem_from_wcs
from rscontextneo.src.hydrology import (
    DEM_BREACH_RELPATH,
    run_d8_hydrology,
)
from rscontextneo.src.utils.aoi import validate_copy_aoi
from rscontextneo.src.utils.breach_diff import (
    BREACH_DIFF_GPKG_RELPATH,
    create_breach_diff_points,
)
from rscontextneo.src.utils.dem import (
    dem_to_geojson,
    generate_hillshade,
    generate_slope,
)
from rscontextneo.src.xml_layers import write_project_xml

initGDALOGRErrors()


def rs_context_neo(
    output_folder: str,
    config: RSContextNeoConfig,
    meta: dict[str, str],
    *,
    force_download: bool = False,
    debug: bool = False,
    download_dir: str | None = None,
    scratch_dir: str | None = None,
) -> None:
    """
    Run the Riverscapes Context Neo tool for a single watershed.

    All data-source configuration (DEM backend, hydrology parameters, optional
    layers) is read from *config*.  Per-run values are passed as keyword
    arguments.

    Parameters
    ----------
    output_folder : str
        Directory where output files will be saved.
    config : RSContextNeoConfig
        Validated regional profile loaded from a JSON config file.
    meta : dict[str, str]
        Extra metadata key=value pairs merged into the project XML.
    force_download : bool
        Re-download all source data and re-run every step even if cached.
    debug : bool
        Retain intermediate files and emit extra diagnostic output.
    download_dir : str or None
        Override the DEM download directory.  Resolution order: this argument
        → ``DOWNLOAD_DIR`` environment variable → ``dem.download_dir`` in the
        config file.  Required when ``dem.source`` is not ``'file'``.
    scratch_dir : str or None
        Override the DEM scratch directory.  Resolution order: this argument
        → ``SCRATCH_DIR`` environment variable → ``dem.scratch_dir`` in the
        config file.  Defaults to a ``scratch/`` sub-folder of the resolved
        download directory when not set.
    """
    log = Logger("RS Context Neo")
    start_time = time.time()

    log.info(f"Starting RS Context Neo v{__version__}")
    log.info(f"Profile:           {config.profile_name or '(unnamed)'}")
    log.info(f"Output folder:     {output_folder}")
    log.info(f"Output resolution: {config.dem.resolution} m")
    log.info(f"DEM source:        {config.dem.source}")
    log.info(f"Stream threshold:  {config.hydrology.threshold:,} cells")
    log.info(f"Breach distance:   {config.hydrology.breach_dist} cells")
    if config.hydrology.cores is not None:
        log.info(f"TauDEM cores:      {config.hydrology.cores}")

    _effective_dl = (
        download_dir or os.environ.get("DOWNLOAD_DIR") or config.dem.download_dir
    )
    _effective_scratch = (
        scratch_dir or os.environ.get("SCRATCH_DIR") or config.dem.scratch_dir
    )
    if config.dem.source != "file":
        if _effective_dl is None:
            raise ValueError(
                "A DEM download directory must be provided. "
                "Use --download-dir on the command line, set the DOWNLOAD_DIR environment variable, "
                "or add 'download_dir' to the config file."
            )

    safe_makedirs(output_folder)

    # Mutate the dem config in-place so AppConfig carries the resolved paths,
    # then populate the singleton.  Both happen only after all input validation
    # has passed and the output directory has been created.
    if config.dem.source != "file":
        config.dem.download_dir = _effective_dl
        if _effective_scratch is not None:
            config.dem.scratch_dir = _effective_scratch
    AppConfig.set(config)

    # ── Step 1: Acquire bounds GeoJSON and DEM ─────────────────────────────────
    log.info("Step 1: Acquiring project bounds and DEM")
    step_timer = Timer()
    if config.dem.source != "file":
        log.info(f"  Input source: Custom AOI GeoJSON — {config.dem.aoi}")
        descriptor = "Custom AOI"
        bounds_geojson = validate_copy_aoi(config.dem.aoi, output_folder)
        dem_path, _hillshade_path, _slope_path = _fetch_dem(
            bounds_geojson,
            output_folder,
            force_download,
            debug=debug,
        )
    else:
        log.info(f"  Input source: User-supplied DEM — {config.dem.filepath}")
        descriptor = "User-supplied DEM"
        bounds_geojson = dem_to_geojson(config.dem.filepath, output_folder)
        # Copy the user-supplied DEM into the topography output directory so that
        # the project structure mirrors the AOI workflow (where _fetch_dem places
        # the assembled DEM at topography/dem.tif).  Downstream steps (hydrology,
        # project XML) all expect the DEM to live inside the output folder.
        dem_path = os.path.join(output_folder, DEM_RELPATH)
        safe_makedirs(os.path.dirname(dem_path))
        if force_download or not os.path.isfile(dem_path):
            # Guard against SameFileError when the user's filepath already points to
            # the project-internal copy (e.g. re-using a previous run's output folder).
            if os.path.isfile(dem_path) and os.path.samefile(
                config.dem.filepath, dem_path
            ):
                log.info(
                    f"  DEM source and destination are the same file — skipping copy: {dem_path}"
                )
            else:
                log.info(f"  Copying DEM to project topography directory: {dem_path}")
                result = gdal.Translate(
                    dem_path,
                    config.dem.filepath,
                    creationOptions=[
                        "COMPRESS=LZW",
                        "PREDICTOR=2",
                        "TILED=YES",
                        "BIGTIFF=IF_SAFER",
                    ],
                )
                if result is None:
                    raise RuntimeError(
                        f"gdal.Translate returned None copying DEM: {gdal.GetLastErrorMsg()}"
                    )
                result = None  # flush / dereference
        else:
            log.info(
                f"  DEM already present in project topography directory: {dem_path}"
            )
        generate_hillshade(
            dem_path,
            os.path.join(output_folder, HILLSHADE_RELPATH),
            force=force_download,
            log=log,
        )
        generate_slope(
            dem_path,
            os.path.join(output_folder, SLOPE_RELPATH),
            force=force_download,
            log=log,
        )
    log.info(f"  Step 1 complete in {pretty_duration(step_timer.ellapsed())}")

    # ── Step 2: D8 Hydrology ───────────────────────────────────────────────────
    log.info("Step 2: Running D8 hydrology processing chain")
    log.info(
        f"  threshold: {config.hydrology.threshold:,} cells  |  "
        f"breach_dist: {config.hydrology.breach_dist} cells"
    )
    step_timer = Timer()
    run_d8_hydrology(
        dem_path,
        output_folder,
        threshold=config.hydrology.threshold,
        breach_dist=config.hydrology.breach_dist,
        cores=config.hydrology.cores,
        force=force_download,
        debug=debug,
    )
    log.info(f"  Step 2 complete in {pretty_duration(step_timer.ellapsed())}")

    # ── Debug: breach difference points ───────────────────────────────────────
    if debug:
        log.info("Debug: generating breach difference points layer")
        create_breach_diff_points(
            dem_path=dem_path,
            dem_breach_path=os.path.join(output_folder, DEM_BREACH_RELPATH),
            output_gpkg=os.path.join(output_folder, BREACH_DIFF_GPKG_RELPATH),
            force=force_download,
            log=log,
        )

    # ── Step 3: Optional layers ────────────────────────────────────────────────
    if config.layers:
        log.info(f"Step 3: Fetching {len(config.layers)} optional layer(s)")
        step_timer = Timer()
        for layer_cfg in config.layers:
            _fetch_layer(layer_cfg, output_folder, bounds_geojson, log)
        log.info(f"  Step 3 complete in {pretty_duration(step_timer.ellapsed())}")

    # ── Step 4: Write project XML ──────────────────────────────────────────────
    log.info("Step 4: Writing Riverscapes project XML")
    elapsed_time = time.time() - start_time
    try:
        write_project_xml(
            output_folder=output_folder,
            descriptor=descriptor,
            bounds_geojson=bounds_geojson,
            meta=meta,
            aoi=config.dem.aoi if config.dem.source != "file" else None,
            dem=config.dem.filepath if config.dem.source == "file" else None,
            elapsed_time=elapsed_time,
            log=log,
            debug=debug,
        )
    except Exception as exc:
        log.error(f"Failed to write project XML: {exc}")
        log.error(
            "All processing completed successfully but the project XML could not be written. "
            "Re-run with --force to regenerate it."
        )
        raise

    log.info(
        f"RS Context Neo complete — total processing time: {pretty_duration(elapsed_time)}"
    )


# ── Internal helpers ───────────────────────────────────────────────────────────


def _fetch_dem(
    bounds_geojson: str,
    output_folder: str,
    force_download: bool,
    debug: bool = False,
) -> tuple[str, str, str]:
    """Delegate DEM acquisition to the backend specified by config.dem.source."""
    cfg = AppConfig.get()
    dem_cfg = cfg.dem

    assert dem_cfg.download_dir is not None, (
        "_fetch_dem called with download_dir=None; caller must resolve this before AppConfig.set()"
    )

    effective_scratch = dem_cfg.scratch_dir or os.path.join(
        dem_cfg.download_dir, "scratch"
    )

    if dem_cfg.source == "wcs":
        if dem_cfg.wcs_options is None:
            raise ValueError("dem.wcs_options is required when dem.source is 'wcs'.")
        wcs = dem_cfg.wcs_options
        return fetch_dem_from_wcs(
            bounds_geojson,
            output_folder,
            dem_cfg.download_dir,
            effective_scratch,
            dem_cfg.resolution,
            force_download,
            debug=debug,
            wcs_url=wcs.url,
            wcs_coverage=wcs.coverage,
            wcs_version=wcs.version,
            wcs_format=wcs.format,
            wcs_tile_pixels=wcs.tile_pixels,
            wcs_download_workers=wcs.download_workers,
            wcs_timeout_s=wcs.timeout_s,
            wcs_max_attempts=wcs.max_attempts,
            wcs_retry_backoff_s=wcs.retry_backoff_s,
        )

    # Default: TNM tile-based approach
    tnm = dem_cfg.tnm_options
    return fetch_dem_from_3dep(
        bounds_geojson,
        output_folder,
        dem_cfg.download_dir,
        effective_scratch,
        dem_cfg.resolution,
        force_download,
        debug=debug,
        download_workers=tnm.download_workers,
        buffer_deg=tnm.buffer_deg,
    )


def _fetch_layer(
    layer_cfg: LayerConfig,
    output_folder: str,
    bounds_geojson: str,
    log: Logger,
) -> None:
    """Dispatch a single optional layer to its handler based on layer type."""
    if isinstance(layer_cfg, S3TablesLayerConfig):
        from rscontextneo.src.transportation import (  # pylint: disable=import-outside-toplevel
            fetch_transportation,
        )

        rail_path = next(
            (s.s3tables_path for s in layer_cfg.sublayers if s.layer_name == "rail"),
            None,
        )
        roads_path = next(
            (s.s3tables_path for s in layer_cfg.sublayers if s.layer_name == "roads"),
            None,
        )
        log.info(f"  [{layer_cfg.type}] {layer_cfg.id} → {layer_cfg.output_path}")
        fetch_transportation(
            output_folder,
            rail_path,
            roads_path,
            layer_cfg.athena_output,
            log,
            aoi_geojson=bounds_geojson,
        )

    elif isinstance(layer_cfg, CogClipLayerConfig):
        log.warning(
            f"  Layer type 'cog_clip' not yet implemented — skipping '{layer_cfg.id}'"
        )

    elif isinstance(layer_cfg, WfsLayerConfig):
        log.warning(
            f"  Layer type 'wfs' not yet implemented — skipping '{layer_cfg.id}'"
        )

    elif isinstance(layer_cfg, WcsRasterLayerConfig):
        log.warning(
            f"  Layer type 'wcs_raster' not yet implemented — skipping '{layer_cfg.id}'"
        )

    else:
        log.warning(
            f"  Unknown layer type — skipping '{getattr(layer_cfg, 'id', '?')}'"
        )


def main():
    """Main entry point for RS Context Neo."""
    parser = argparse.ArgumentParser(
        description="Riverscapes Context Neo Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Regional configuration (DEM source, hydrology parameters, optional\n"
            "layers) lives in the --config profile file. Only run-specific values\n"
            "are given here.\n\n"
            "Example:\n"
            "  rs_context_neo --config config/us_conus.json \\\n"
            "                 --output /results/my_run\n"
            "  # AOI path is set in the config file"
        ),
    )

    parser.add_argument(
        "--config",
        "-c",
        help=(
            "Path to a regional profile JSON file "
            "(e.g. config/us_conus.json or config/global_wcs.json). "
            "Defines the DEM source, hydrology parameters, and optional data layers."
        ),
        type=str,
        required=True,
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Path to the output folder",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--force",
        help="Re-download all source data and re-run every processing step even if cached",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--download-dir",
        help="Override the DEM download directory (takes precedence over config and DOWNLOAD_DIR env var)",
        type=str,
    )
    parser.add_argument(
        "--scratch-dir",
        help="Override the DEM scratch directory (takes precedence over config and SCRATCH_DIR env var)",
        type=str,
    )
    parser.add_argument(
        "--meta",
        help="Riverscapes project metadata as comma-separated key=value pairs",
        type=str,
    )
    parser.add_argument(
        "--verbose",
        help="(optional) extra logging",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--debug",
        help="(optional) retain intermediate files and enable memory-usage profiling",
        action="store_true",
        default=False,
    )

    _env_path = os.path.join(os.path.dirname(__file__), ".env")
    args = dotenv.parse_args_env(parser, env_path=_env_path)

    log = Logger("RS Context Neo")
    log.setup(
        log_path=os.path.join(args.output, "rs_context.log"), verbose=args.verbose
    )
    log.title("Riverscapes Context Neo")
    log.info(f"Model Version:  {__version__}")

    # ── Load and validate config ───────────────────────────────────────────────
    config = load_config(args.config, env_path=_env_path)
    log.info(f"Config profile: {config.profile_name or args.config}")
    log.info(f"Output folder:  {args.output}")

    meta = parse_metadata(args.meta) if args.meta else {}
    main_timer = time.time()

    try:
        # Run the main function, optionally with memory profiling if --debug is set.
        # The ThreadRun wrapper executes the function in a separate thread and monitors
        # memory usage, logging the maximum to a file.
        if args.debug:
            from rscommons.debug import (
                ThreadRun,  # pylint: disable=import-outside-toplevel
            )

            memfile = os.path.join(args.output, "rs_context_neo_memusage.log")
            retcode, max_obj = ThreadRun(
                rs_context_neo,
                memfile,
                args.output,
                config,
                meta,
                force_download=args.force,
                debug=args.debug,
                download_dir=args.download_dir,
                scratch_dir=args.scratch_dir,
            )
            log.debug(f"Return code: {retcode}, [Max process usage] {max_obj}")
        else:
            rs_context_neo(
                args.output,
                config,
                meta,
                force_download=args.force,
                debug=args.debug,
                download_dir=args.download_dir,
                scratch_dir=args.scratch_dir,
            )
    except Exception as e:
        log.error(e)
        traceback.print_exc()
        sys.exit(1)

    log.info(f"Total wall-clock time: {pretty_duration(time.time() - main_timer)}")


if __name__ == "__main__":
    main()
