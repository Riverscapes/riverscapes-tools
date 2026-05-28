"""
Name:       Riverscapes Context Neo

Purpose:    Build a Riverscapes Context project for a single watershed by
            downloading a 1-metre 3DEP DEM and running the standard TauDEM
            D8 hydrology processing chain.

Author:      Matt Reimer
Date:       2026-05-20
"""

import argparse
import os
import sys
import time
import traceback

from rscommons import Timer, initGDALOGRErrors
from rsxml import Logger, dotenv
from rsxml.util import parse_metadata, pretty_duration, safe_makedirs

from rscontextneo.__version__ import __version__
from rscontextneo.src.fetch_dem import (
    HILLSHADE_RELPATH,
    SLOPE_RELPATH,
    fetch_dem_from_3dep,
)
from rscontextneo.src.hydrology import (
    DEFAULT_BREACH_DIST,
    DEFAULT_THRESHOLD,
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
    meta: dict[str, str],
    *,
    aoi: str | None = None,
    dem: str | None = None,
    download_folder: str | None = None,
    scratch_folder: str | None = None,
    output_res: float = 1.0,
    force_download: bool = False,
    threshold: int = DEFAULT_THRESHOLD,
    breach_dist: int = DEFAULT_BREACH_DIST,
    cores: int | None = None,
    debug: bool = False,
) -> None:
    """
    Run the Riverscapes Context Neo tool for a single watershed.

    Writes a complete Riverscapes project (``project.rs.xml``) in
    *output_folder* that references all topography and hydrology outputs.
    Exactly one of *aoi* or *dem* must be provided.

    Parameters:
        output_folder (str): Directory where the output files will be saved.
        meta (dict[str, str]): Optional metadata key=value pairs.
        aoi (str): Path to a GeoJSON defining the area of interest.
        dem (str): Path to an already-downloaded DEM raster file.
        download_folder (str): Cache folder for raw 3DEP tile downloads.
                               Required when using --aoi.
        scratch_folder (str): Temporary folder for unzipping tiles.
                               Defaults to <download_folder>/scratch if not set.
        output_res (float): Target DEM resolution in metres (1–10). Default 1.0.
        force_download (bool): Re-download tiles and re-run all steps even if
                               outputs are already cached. Default False.
        threshold (int): Minimum upstream cell count for stream classification
                         (units: cells). A cell is classified as stream where
                         its contributing area ≥ this value. At 1 m resolution,
                         multiply by 1 m² to get contributing area in m²
                         (e.g. 50 000 cells ≈ 0.05 km²). Default: 50 000 cells.
        breach_dist (int): Maximum search distance in cells for the WhiteboxTools
                           least-cost breach path. Default: DEFAULT_BREACH_DIST.
        cores (int | None): Number of MPI ranks for TauDEM steps. None reads
                            the TAUDEM_CORES env var, falling back to 2.
        debug (bool): If True, intermediate files are not deleted after processing
                        and more verbose logging and diagnostics are enabled.
    """
    log = Logger("RS Context Neo")
    start_time = time.time()

    log.info(f"Starting RS Context Neo v{__version__}")
    log.info(f"Output folder: {output_folder}")
    log.info(f"Output resolution: {output_res} m")
    log.info(f"Stream threshold:  {threshold:,} cells")
    log.info(f"Breach distance:   {breach_dist} cells")
    if cores is not None:
        log.info(f"TauDEM cores:      {cores}")

    if sum(v is not None for v in (aoi, dem)) != 1:
        raise ValueError("Exactly one of aoi or dem must be provided.")

    # ── Early input validation ─────────────────────────────────────────────────
    # Validate before creating any output directories so a bad invocation
    # doesn't leave behind an empty project folder.
    if output_res <= 0:
        raise ValueError(f"output_res must be a positive number, got {output_res}")
    if threshold <= 0:
        raise ValueError(f"threshold must be a positive integer, got {threshold}")
    if breach_dist <= 0:
        raise ValueError(f"breach_dist must be a positive integer, got {breach_dist}")
    if dem is not None and not os.path.isfile(dem):
        raise FileNotFoundError(f"User-supplied DEM not found: {dem}")

    safe_makedirs(output_folder)

    # ── Step 1: Acquire bounds GeoJSON and DEM ─────────────────────────────────
    log.info("Step 1 of 3: Acquiring project bounds and DEM")
    step_timer = Timer()
    if aoi is not None:
        log.info(f"  Input source: Custom AOI GeoJSON — {aoi}")
        descriptor = "Custom AOI"
        bounds_geojson = validate_copy_aoi(aoi, output_folder)
        dem_path, _hillshade_path, _slope_path = _fetch_3dep(
            bounds_geojson,
            output_folder,
            download_folder,
            scratch_folder,
            output_res,
            force_download,
            debug,
        )
    elif dem is not None:
        log.info(f"  Input source: User-supplied DEM — {dem}")
        descriptor = "User-supplied DEM"
        # Now generate a bounds GeoJSON from the DEM data areas.
        # This is buffered and simplified to keep file size down.
        bounds_geojson = dem_to_geojson(dem, output_folder)
        dem_path = dem
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
    else:
        raise AssertionError(
            "Unreachable: runtime guard above ensures exactly one source is set."
        )
    log.info(f"  Step 1 complete in {pretty_duration(step_timer.ellapsed())}")

    # ── Step 2: D8 Hydrology ──────────────────────────────────────────────────
    log.info("Step 2 of 3: Running D8 hydrology processing chain")
    log.info(
        f"  Stream threshold: {threshold:,} cells, breach distance: {breach_dist} cells"
    )
    step_timer = Timer()
    run_d8_hydrology(
        dem_path,
        output_folder,
        threshold=threshold,
        breach_dist=breach_dist,
        cores=cores,
        force=force_download,
    )
    log.info(f"  Step 2 complete in {pretty_duration(step_timer.ellapsed())}")

    # ── Debug: breach difference points ──────────────────────────────────────
    if debug:
        log.info("Debug: generating breach difference points layer")
        create_breach_diff_points(
            dem_path=dem_path,
            dem_breach_path=os.path.join(output_folder, DEM_BREACH_RELPATH),
            output_gpkg=os.path.join(output_folder, BREACH_DIFF_GPKG_RELPATH),
            force=force_download,
            log=log,
        )

    # ── Step 3: Write project XML ─────────────────────────────────────────────
    log.info("Step 3 of 3: Writing Riverscapes project XML")
    elapsed_time = time.time() - start_time
    try:
        write_project_xml(
            output_folder=output_folder,
            descriptor=descriptor,
            bounds_geojson=bounds_geojson,
            meta=meta,
            aoi=aoi,
            dem=dem,
            threshold=threshold,
            output_res=output_res,
            breach_dist=breach_dist,
            elapsed_time=elapsed_time,
            log=log,
            debug=debug,
        )
    except Exception as exc:
        log.error(f"Failed to write project XML: {exc}")
        log.error(
            "All processing completed successfully but the project XML could not be written. "
            "Outputs are present in the output folder. Re-run with --force to regenerate the XML."
        )
        raise

    log.info(
        f"RS Context Neo complete — total processing time: {pretty_duration(elapsed_time)}"
    )


def _fetch_3dep(
    bounds_geojson: str,
    output_folder: str,
    download_folder: str | None,
    scratch_folder: str | None,
    output_res: float,
    force_download: bool,
    debug: bool = False,
) -> tuple[str, str, str]:
    """Validate 3DEP fetch arguments and delegate to fetch_dem_from_3dep."""
    if download_folder is None:
        raise ValueError(
            "download_folder is required when using --aoi. "
            "Provide a persistent cache directory with --download_dir."
        )
    effective_scratch = scratch_folder or os.path.join(download_folder, "scratch")
    return fetch_dem_from_3dep(
        bounds_geojson,
        output_folder,
        download_folder,
        effective_scratch,
        output_res,
        force_download,
        debug,
    )


def main():
    """Main entry point for RS Context Neo."""
    parser = argparse.ArgumentParser(description="Riverscapes Context Neo Tool")

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--aoi", help="Path to a GeoJSON defining the area of interest", type=str
    )
    source_group.add_argument(
        "--dem", help="Path to an already-downloaded DEM raster file", type=str
    )

    parser.add_argument(
        "--output", "-o", help="Path to the output folder", type=str, required=True
    )
    parser.add_argument(
        "--download_dir",
        help="Cache folder for 3DEP tile downloads (required for --aoi)",
        type=str,
    )
    parser.add_argument(
        "--scratch_dir",
        help="Temporary folder for unzipping tiles (default: <download_dir>/scratch)",
        type=str,
    )
    parser.add_argument(
        "--output_res",
        help="Target DEM resolution in metres (1–10, default: 1.0 m)",
        type=float,
        default=1.0,
    )
    parser.add_argument(
        "--force",
        help="Re-download 3DEP tiles and re-run all processing steps even if already cached",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--threshold",
        help=f"Minimum upstream contributing-area cell count for stream classification (units: cells; default: {DEFAULT_THRESHOLD:,} cells). At 1 m resolution, {DEFAULT_THRESHOLD:,} cells ≈ {DEFAULT_THRESHOLD / 1e6:.2f} km². Lower values produce denser networks. See docs/STREAM_THRESHOLD.md.",
        type=int,
        default=DEFAULT_THRESHOLD,
    )
    parser.add_argument(
        "--breach_dist",
        help=f"Maximum search distance in cells for the WhiteboxTools least-cost breach path (default: {DEFAULT_BREACH_DIST} cells = {DEFAULT_BREACH_DIST} m at 1 m resolution). Sized to span typical road and rail embankments without breaching natural ridges. Increase to 200-300 for areas with major highway infrastructure.",
        type=int,
        default=DEFAULT_BREACH_DIST,
    )
    parser.add_argument(
        "--cores",
        help="Number of MPI ranks (parallel processes) for TauDEM steps (default: TAUDEM_CORES env var, or 2)",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--meta",
        help="Riverscapes project metadata as comma separated key=value pairs",
        type=str,
    )
    parser.add_argument(
        "--verbose",
        help="(optional) a little extra logging",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--debug",
        help="(optional) more output about things like memory usage. There is a performance cost",
        action="store_true",
        default=False,
    )

    # Load the .env file sitting next to this script so {env:VAR} tokens in
    # args are resolved even when the variable is not in the shell environment.
    _env_path = os.path.join(os.path.dirname(__file__), ".env")
    args = dotenv.parse_args_env(parser, env_path=_env_path)

    log = Logger("RS Context Neo")
    log.setup(
        log_path=os.path.join(args.output, "rs_context.log"), verbose=args.verbose
    )
    log.title("Riverscapes Context Neo")

    log.info(f"Model Version:     {__version__}")
    log.info(f"Output folder:     {args.output}")
    if args.aoi:
        log.info(f"AOI:               {args.aoi}")
    if args.dem:
        log.info(f"DEM:               {args.dem}")
    if args.download_dir:
        log.info(f"Download cache:    {args.download_dir}")
    if args.scratch_dir:
        log.info(f"Scratch dir:       {args.scratch_dir}")
    log.info(f"Output resolution: {args.output_res} m")
    log.info(f"Stream threshold:  {args.threshold:,} cells")
    log.info(f"Breach dist:       {args.breach_dist} cells")
    if args.cores:
        log.info(f"TauDEM cores:      {args.cores}")
    log.info(f"Force download:    {args.force}")

    meta = parse_metadata(args.meta) if args.meta else {}
    main_timer = time.time()

    try:
        if args.debug is True:
            # Leave this import here so that we don't over-import if not needed
            from rscommons.debug import ThreadRun

            memfile = os.path.join(args.output, "rs_context_neo_memusage.log")
            retcode, max_obj = ThreadRun(
                rs_context_neo,
                memfile,
                args.output,
                meta,
                aoi=args.aoi,
                dem=args.dem,
                download_folder=args.download_dir,
                scratch_folder=args.scratch_dir,
                output_res=args.output_res,
                force_download=args.force,
                threshold=args.threshold,
                breach_dist=args.breach_dist,
                cores=args.cores,
                debug=args.debug,
            )
            log.debug(f"Return code: {retcode}, [Max process usage] {max_obj}")
        else:
            rs_context_neo(
                args.output,
                meta,
                aoi=args.aoi,
                dem=args.dem,
                download_folder=args.download_dir,
                scratch_folder=args.scratch_dir,
                output_res=args.output_res,
                force_download=args.force,
                threshold=args.threshold,
                breach_dist=args.breach_dist,
                cores=args.cores,
                debug=args.debug,
            )
    except Exception as e:
        log.error(e)
        traceback.print_exc()
        sys.exit(1)

    log.info(f"Total wall-clock time: {pretty_duration(time.time() - main_timer)}")


if __name__ == "__main__":
    main()
