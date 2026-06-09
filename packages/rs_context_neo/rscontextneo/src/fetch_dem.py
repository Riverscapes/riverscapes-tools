"""
3DEP DEM fetching and assembly for RS Context Neo.

Downloads 1-metre DEM tiles from USGS 3DEP (via The National Map API),
mosaics and clips them to the supplied bounds polygon, resamples if the
requested output resolution differs meaningfully from the source, verifies
coverage, and generates a hillshade alongside the final DEM.

The helper functions get_epsg / get_best_crs / is_geographic_epsg /
should_resample are adapted from
packages/rscontext_3dep/rscontext_3dep/dem_builder.py so that rs_context_neo
carries no runtime dependency on that package.

Author:     Matt Reimer
Date:       2026-05-25
"""

import os
import shutil
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import shapely.wkt as shapely_wkt
from osgeo import gdal, ogr, osr
from rscommons import GeopackageLayer
from rscommons.download import download_file, download_unzip
from rscommons.download_dem import find_rasters, verify_areas
from rscommons.geographic_raster import gdal_dem_geographic
from rscommons.national_map import get_1m_dem_urls
from rscommons.raster_warp import raster_vrt_stitch
from rsxml import Logger
from rsxml.util import safe_makedirs, safe_remove_file
from shapely.geometry import Polygon
from shapely.geometry import box as shapely_box
from shapely.ops import transform as shapely_transform
from shapely.ops import unary_union

from rscontextneo.src.utils.dem import get_epsg, is_geographic_epsg
from rscontextneo.src.utils.geom import load_geojson_geometry
from rscontextneo.src.utils.gpkg import geojson_to_gpkg

# Output paths (relative to the project output_folder)
DEM_RELPATH = "topography/dem.tif"
HILLSHADE_RELPATH = "topography/dem_hillshade.tif"
SLOPE_RELPATH = "topography/slope.tif"
TILE_FOOTPRINTS_RELPATH = "topography/tile_footprints.gpkg"

# Degrees to buffer the bounds polygon when querying The National Map
_BUFFER_DIST_DEG = 0.01

# Relative difference in resolution below which we skip resampling
_RESAMPLE_THRESHOLD = 0.1

# Nodata value written to the output DEM.  Float32 minimum (most negative
# finite float32) is the standard fill sentinel for elevation rasters and
# is guaranteed to be representable without loss in a 32-bit GeoTIFF band.
_DEM_NODATA: float = float(np.finfo(np.float32).min)

# GDAL creation options applied to the output DEM (PREDICTOR=2 = horizontal
# differencing, ideal for continuous elevation data; shrinks files ~50-70%)
_DEM_CREATION_OPTIONS = [
    "COMPRESS=DEFLATE",
    "PREDICTOR=2",
    "TILED=YES",
    "BIGTIFF=IF_SAFER",
]

# Number of tiles to download simultaneously.  Network I/O is the bottleneck
# so threads (not processes) are the right tool - the GIL doesn't matter here.
# 4 workers is a safe default that saturates a typical connection without
# hammering the USGS TNM servers hard enough to trigger throttling.
_DEFAULT_DOWNLOAD_WORKERS = 4

# Shared field schema for both GeoPackage layers written by
# _write_tile_footprints_gpkg().  Keys are field names; values are OGR types.
_TILE_SCHEMA: dict[str, int] = {
    "filename": ogr.OFTString,
    "source_path": ogr.OFTString,
    "original_epsg": ogr.OFTInteger,
    "original_crs_name": ogr.OFTString,
    "is_reprojected": ogr.OFTInteger,
    "final_epsg": ogr.OFTInteger,
    "final_crs_name": ogr.OFTString,
    "width_px": ogr.OFTInteger,
    "height_px": ogr.OFTInteger,
    "resolution_m": ogr.OFTReal,
    "file_size_mb": ogr.OFTReal,
    "nodata_value": ogr.OFTReal,
    "x_min_native": ogr.OFTReal,
    "y_min_native": ogr.OFTReal,
    "x_max_native": ogr.OFTReal,
    "y_max_native": ogr.OFTReal,
}


# ── Public entry point ────────────────────────────────────────────────────────


def fetch_dem_from_3dep(
    bounds_geojson: str,
    output_folder: str,
    download_folder: str,
    scratch_folder: str,
    output_res: float = 1.0,
    force_download: bool = False,
    cleanup_scratch: bool = True,
    download_workers: int = _DEFAULT_DOWNLOAD_WORKERS,
    debug: bool = False,
    buffer_deg: float = _BUFFER_DIST_DEG,
) -> tuple[str, str, str]:
    """
    Download and assemble a 3DEP 1-metre DEM for the area defined by a GeoJSON
    bounds file, then generate a hillshade.

    Parameters:
        bounds_geojson (str): Path to a WGS84 GeoJSON file containing the AOI
                              polygon (typically project_bounds.geojson).
        output_folder (str): Root of the RS Context Neo project folder.
                             DEM → <output_folder>/topography/dem.tif
                             Hillshade → <output_folder>/topography/dem_hillshade.tif
        download_folder (str): Persistent cache folder for raw 3DEP tile downloads.
                               Different runs / projects can share this folder.
        scratch_folder (str): Temporary folder for unzipping downloaded tiles.
                              Safe to delete after each run.
        output_res (float): Target horizontal resolution in metres (1-10).
                            Defaults to 1.0 (native 3DEP 1 m resolution).
        force_download (bool): If True, re-download tiles even when a local copy
                               already exists. Defaults to False.
        cleanup_scratch (bool): If True (default), delete the unzipped tile
                                directory after mosaicing.  The compressed
                                .zip cache in download_folder is always kept
                                so tiles can be re-unzipped without a fresh
                                network download.
        download_workers (int): Number of tiles to download in parallel
                                (default: 4).  Each tile is an independent
                                HTTPS download so threads give a near-linear
                                speedup up to your available bandwidth.
                                Pass 1 to download sequentially.
        debug (bool): If True, intermediate files are not deleted after processing

    Returns:
        tuple[str, str, str]: (dem_path, hillshade_path, slope_path) - absolute
                         paths to the assembled DEM, hillshade, and slope raster.

    Raises:
        FileNotFoundError: If bounds_geojson does not exist.
        ValueError: If no DEM tiles could be identified or if no valid EPSG
                    code could be determined from the downloaded tiles.
        Exception: Propagated from download / warp / verify steps.
    """
    log = Logger("Fetch DEM")

    if not os.path.exists(bounds_geojson):
        raise FileNotFoundError(f"Bounds GeoJSON not found: {bounds_geojson}")

    # Early-exit: if both outputs already exist and the caller hasn't asked for
    # a forced re-download, skip the entire tile-query / download / mosaic pipeline.
    dem_path = os.path.join(output_folder, DEM_RELPATH)
    hillshade_path = os.path.join(output_folder, HILLSHADE_RELPATH)
    slope_path = os.path.join(output_folder, SLOPE_RELPATH)
    if (
        not force_download
        and os.path.isfile(dem_path)
        and os.path.isfile(hillshade_path)
        and os.path.isfile(slope_path)
    ):
        log.info(
            f"DEM already exists at {dem_path} - skipping download (pass force_download=True to override)"
        )
        return dem_path, hillshade_path, slope_path

    # rscommons functions (download_dem, verify_areas) open vector files via
    # get_shp_or_gpkg which forces the GPKG driver.  GeoJSON isn't supported
    # by that path, so we convert once here and use the GeoPackage throughout.
    bounds_gpkg = os.path.join(scratch_folder, "bounds.gpkg")
    safe_makedirs(scratch_folder)
    geojson_to_gpkg(bounds_geojson, bounds_gpkg)
    # rscommons path_sorter returns (filepath, None) when the file exists,
    # so GeoPackage layer lookup fails.  Append the layer name as a compound
    # path (/path/to/bounds.gpkg/bounds) so the regex branch is used instead.
    bounds_gpkg_layer = bounds_gpkg + "/bounds"

    ned_download_folder = os.path.join(download_folder, "ned")
    ned_unzip_folder = os.path.join(scratch_folder, "ned")

    # ── 1. Identify and download tiles ───────────────────────────────────────
    log.info("Querying The National Map for 3DEP 1 m tiles ...")
    source_urls = get_1m_dem_urls(bounds_gpkg_layer, buffer_deg)
    log.info(f"{len(source_urls)} tile(s) identified on The National Map")

    dem_rasters = _download_tiles_parallel(
        source_urls,
        ned_download_folder,
        ned_unzip_folder,
        force_download,
        workers=download_workers,
    )
    log.info(f"{len(dem_rasters)} tile(s) ready")

    # ── 2. Inspect tiles once: determine CRS and whether resampling is needed ─
    # A single pass avoids opening every raster file twice (previously
    # get_best_crs and should_resample each iterated the full tile list).
    output_epsg, resample = _inspect_tiles(dem_rasters, output_res)
    if output_epsg is None:
        raise ValueError(
            "Could not determine a valid EPSG code from the downloaded DEM tiles. "
            "Check that the tiles are valid GeoTIFF/IMG files with embedded CRS information."
        )
    log.info(f"Output EPSG: {output_epsg}")

    # ── 3. Mosaic / clip / (optionally) resample ─────────────────────────────

    need_dem_rebuild = force_download or not os.path.exists(dem_path) or resample

    if need_dem_rebuild:
        log.info("Mosaicing and clipping DEM tiles ...")
        safe_makedirs(os.path.dirname(dem_path))
        if os.path.exists(dem_path):
            safe_remove_file(dem_path)

        # ── CRS normalisation ─────────────────────────────────────────────
        # 3DEP tiles are distributed in their native UTM zone, so an AOI that
        # straddles a zone boundary will produce a mixed-CRS download set.
        # gdalbuildvrt (used inside raster_vrt_stitch) silently drops any tile
        # whose CRS does not match the first tile it sees, which is why the
        # output can cover only a fraction of the AOI.
        #
        # Strategy:
        #   1. If all tiles are already in the same CRS → nothing to do.
        #   2. Measure what fraction of the AOI each single-CRS subset covers.
        #      Keep the best-coverage subset; discard all other tiles.
        #      No reprojection is ever performed — resampling elevation data
        #      introduces artefacts that are unacceptable for a topo product.
        #      A warning is emitted if the winning subset is not 100% coverage.
        dem_rasters, output_epsg = _resolve_tiles(
            dem_rasters, output_epsg, bounds_geojson, scratch_folder, log
        )

        warp_options: dict = {
            "cutlineBlend": 1,
            "dstNodata": _DEM_NODATA,
            "creationOptions": _DEM_CREATION_OPTIONS,
        }
        if resample:
            log.info(f"Resampling to {output_res} m (bilinear)")
            warp_options.update(
                {
                    "xRes": output_res,
                    "yRes": output_res,
                    "resampleAlg": "bilinear",
                }
            )

        # bounds_geojson works here - raster_vrt_stitch uses gdal.WarpOptions
        # (cutlineDSName) which does native OGR auto-detection, not get_shp_or_gpkg.
        raster_vrt_stitch(
            dem_rasters,
            dem_path,
            output_epsg,
            clip=bounds_geojson,
            warp_options=warp_options,
        )

        # ── Tile footprints GeoPackage ────────────────────────────────────────
        # Record the footprint and provenance of every downloaded tile so the
        # user can inspect which tiles contributed to the mosaic, what CRS
        # each was originally in, and which (if any) were reprojected.
        tile_footprints_path = os.path.join(output_folder, TILE_FOOTPRINTS_RELPATH)
        _write_tile_footprints_gpkg(dem_rasters, output_epsg, tile_footprints_path, log)

        # ── Delete unzipped tiles once the mosaic is built ────────────────────
        # The .zip files in ned_download_folder are kept as a persistent cache;
        # the unzipped copies in ned_unzip_folder are now redundant.
        if cleanup_scratch and os.path.isdir(ned_unzip_folder):
            log.info(f"Removing unzipped tile cache: {ned_unzip_folder}")
            shutil.rmtree(ned_unzip_folder, ignore_errors=True)
    else:
        log.info(
            "DEM already exists and no resample needed - skipping rebuild (pass force_download=True to override)"
        )

    # ── 4. Verify coverage ────────────────────────────────────────────────────
    area_ratio = verify_areas(dem_path, bounds_gpkg_layer)
    if area_ratio < 0.85:
        log.warning(
            f"DEM covers only {area_ratio:.1%} of the AOI bounds (threshold: 85%). "
            "3DEP 1 m data may not be available for this region; consider using the 10 m product."
        )

    # ── 5. Hillshade ─────────────────────────────────────────────────────────
    need_hs_rebuild = need_dem_rebuild or not os.path.isfile(hillshade_path)
    if need_hs_rebuild:
        log.info("Generating hillshade ...")
        if is_geographic_epsg(output_epsg):
            gdal_dem_geographic(dem_path, hillshade_path, "hillshade")
        else:
            gdal.DEMProcessing(
                hillshade_path,
                dem_path,
                "hillshade",
                creationOptions=["COMPRESS=DEFLATE"],
            )
    else:
        log.info("Hillshade already exists - skipping rebuild")

    # ── 6. Slope ──────────────────────────────────────────────────────────────
    # Slope is calculated from the raw assembled DEM using gdal.DEMProcessing,
    # matching exactly how rs_context produces its SLOPE layer.
    #
    # Why not use the TauDEM D8 slope?
    #   - The D8 slope is the gradient along the *single* downhill flow
    #     direction for each cell (rise/run, dimensionless).  It is an
    #     internal TauDEM intermediate used for flow routing, not a general
    #     topographic slope product.
    #   - gdal.DEMProcessing uses Horn's method - the maximum gradient across
    #     all 8 neighbours - and outputs in degrees.  This is the standard
    #     topographic slope used by BRAT, RME, and other downstream tools.
    #
    # Why no z-factor?
    #   - rs_context applies a haversine z-factor because it works with
    #     geographic (lat/lon) DEMs where horizontal units are degrees and
    #     vertical units are metres.  Our DEM is in a projected UTM CRS where
    #     both horizontal and vertical units are metres, so scale=1.0 (the
    #     default) is correct.
    #
    # Input: the raw dem.tif, NOT the pit-filled or breach-conditioned DEM.
    # Hydrological conditioning raises depression cells, which would
    # artificially flatten areas and misrepresent true terrain slope.
    need_slope_rebuild = need_dem_rebuild or not os.path.isfile(slope_path)
    if need_slope_rebuild:
        log.info("Generating slope raster (gdal.DEMProcessing, degrees) ...")
        result = gdal.DEMProcessing(
            slope_path,
            dem_path,
            "slope",
            creationOptions=[
                "COMPRESS=DEFLATE",
                "PREDICTOR=2",
                "TILED=YES",
                "BIGTIFF=IF_SAFER",
            ],
        )
        if result is None:
            log.warning(f"gdal.DEMProcessing slope failed: {gdal.GetLastErrorMsg()}")
        else:
            result = None  # flush
            log.info(f"Slope:     {slope_path}")
    else:
        log.info("Slope already exists - skipping rebuild")

    log.info(
        f"DEM:       {dem_path}  ({os.path.getsize(dem_path) / 1_048_576:.1f} MB, compressed)"
    )
    log.info(f"Hillshade: {hillshade_path}")
    log.info(
        f"Resolution: {output_res} m  |  EPSG: {output_epsg}  |  Coverage: {area_ratio:.1%}  |  Tiles: {len(source_urls)}"
    )

    return dem_path, hillshade_path, slope_path


# ── Internal helpers ──────────────────────────────────────────────────────────


def _download_tiles_parallel(
    urls: list[str],
    download_folder: str,
    unzip_folder: str,
    force_download: bool,
    workers: int = _DEFAULT_DOWNLOAD_WORKERS,
) -> list[str]:
    """
    Download and unzip 3DEP tiles in parallel using a thread pool.

    Each tile is an independent HTTPS download, so threads give a near-linear
    speedup up to available bandwidth without multiprocessing overhead or GIL
    concerns (network I/O releases the GIL automatically).

    The underlying rscommons download_file() already writes a .pending sentinel
    file while a download is in progress, so concurrent workers hitting the
    same shared cache folder (e.g. across projects) are safe.

    Args:
        urls:             List of HTTPS tile URLs from The National Map.
        download_folder:  Persistent cache for .zip files.
        unzip_folder:     Scratch folder where zips are extracted.
        force_download:   Re-download even if the zip already exists.
        workers:          Max simultaneous downloads (default 4).

    Returns:
        List of local raster paths (.tif / .img), one per URL, in an
        arbitrary order (VRT stitching is order-independent).

    Raises:
        Exception: If any individual tile download fails after retries.
    """
    log = Logger("Download Tiles")
    safe_makedirs(download_folder)
    safe_makedirs(unzip_folder)

    def _download_one(url: str) -> str:
        base_path = os.path.basename(os.path.splitext(url)[0])
        final_unzip_path = os.path.join(unzip_folder, base_path)
        if url.lower().endswith(".zip"):
            file_path = download_unzip(
                url, download_folder, final_unzip_path, force_download
            )
            return find_rasters(file_path)
        else:
            return download_file(url, download_folder, force_download)

    effective_workers = min(workers, len(urls))
    log.info(f"Downloading {len(urls)} tile(s) with {effective_workers} worker(s) ...")

    # Build a URL→index map so we can restore input order after parallel download.
    # get_1m_dem_urls returns URLs sorted oldest-to-newest by publication date;
    # preserving that order here ensures GDAL BuildVRT places newer gap-fill tiles
    # LAST, which makes them win for pixels where both old and new surveys have
    # valid (non-nodata) elevation values (GDAL uses the last non-nodata source).
    url_index: dict[str, int] = {url: i for i, url in enumerate(urls)}

    raster_paths_keyed: list[tuple[int, str]] = []  # (original_url_index, local_path)
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        future_to_url = {executor.submit(_download_one, url): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                raster_paths_keyed.append((url_index[url], future.result()))
                log.info(f"  ✓  {os.path.basename(url)}")
            except Exception as exc:
                log.error(f"  ✗  {os.path.basename(url)}: {exc}")
                errors.append(url)

    if errors:
        raise Exception(
            f"{len(errors)} tile(s) failed to download:\n"
            + "\n".join(f"  {u}" for u in errors)
        )

    # Restore original URL order (oldest-survey-first) so downstream VRT
    # construction gives newer gap-fill tiles the highest priority.
    raster_paths_keyed.sort(key=lambda pair: pair[0])
    return [path for _, path in raster_paths_keyed]


def _inspect_tiles(
    dem_rasters: list[str], output_res: float
) -> tuple[int | None, bool]:
    """
    Open each tile *once* and return (best_epsg, needs_resample).

    Previously get_best_crs and should_resample each iterated over the full
    tile list, opening every file twice.  This single pass halves the I/O.

    Resolution check uses only the *first* valid tile - all tiles in a 3DEP
    product tier share the same pixel size, so averaging across all of them
    produces identical results at unnecessary cost.
    """
    log = Logger("Inspect Tiles")
    epsg_codes: list[int] = []
    first_res: float | None = None

    for raster_path in dem_rasters:
        ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
        if ds is None:
            log.warning(f"Could not open {raster_path} - skipping")
            continue

        # CRS
        wkt = ds.GetProjection()
        if wkt:
            srs = osr.SpatialReference()
            if srs.ImportFromWkt(wkt) == 0:
                srs.AutoIdentifyEPSG()
                code_str = srs.GetAuthorityCode(None)
                if code_str:
                    try:
                        epsg_codes.append(int(code_str))
                    except ValueError:
                        pass

        # Resolution - only need one tile
        if first_res is None:
            gt = ds.GetGeoTransform()
            if gt is not None:
                first_res = (abs(gt[1]) + abs(gt[5])) / 2.0

        ds = None  # close file handle

    # Best EPSG (majority vote, tie-break: lowest code)
    best_epsg: int | None = None
    if epsg_codes:
        counts = Counter(epsg_codes)
        max_freq = counts.most_common(1)[0][1]
        candidates = [c for c, f in counts.items() if f == max_freq]
        best_epsg = min(candidates)
        log.info(f"Best CRS: EPSG:{best_epsg} (frequency {max_freq}/{len(epsg_codes)})")
    else:
        log.error("Could not determine a valid EPSG from any tile.")

    # Resample decision
    needs_resample = True  # default: resample if we can't read source res
    if first_res is not None:
        rel_diff = abs(first_res - output_res) / first_res
        log.info(
            f"Source resolution: {first_res:.3f} m → target: {output_res} m (Δ {rel_diff:.1%})"
        )
        needs_resample = rel_diff > _RESAMPLE_THRESHOLD
        log.info(
            "Resampling required."
            if needs_resample
            else "Source resolution close enough - no resample needed."
        )
    else:
        log.warning("No valid source resolution found - resampling by default.")

    return best_epsg, needs_resample


def _resolve_tiles(
    dem_rasters: list[str],
    target_epsg: int,
    bounds_geojson: str,
    scratch_folder: str,
    log: Logger,
) -> tuple[list[str], int]:
    """
    Return a ``(tile_list, epsg)`` pair where every tile in *tile_list* shares
    the same CRS and the set provides the best available coverage of the AOI.

    3DEP tiles are distributed in their native UTM zone, so any AOI that
    straddles a zone boundary will produce tiles in two different projections.
    ``gdalbuildvrt`` (used inside ``raster_vrt_stitch``) silently drops every
    tile whose CRS does not match the first tile it encounters, so we must
    select a single homogeneous tile set before building the mosaic.

    Strategy
    --------
    **Step 1 — fast path**
        All tiles already share one CRS.  Return immediately.
        This is the normal case for AOIs entirely within one UTM zone.

    **Step 2 — best-coverage CRS selection**
        For each CRS group, compute what fraction of the AOI is covered by
        that group's tiles (checked in the tile's native CRS — see
        :func:`_compute_tile_coverage`).  Use the group that covers the most of the
        AOI.  No reprojection is performed; tiles from the losing CRS group(s)
        are silently discarded.

        If the winning group does not achieve 100% coverage the user is warned
        that some parts of the AOI will be missing from the DEM.  This is
        a deliberate trade-off: reprojecting tiles would introduce a resampling
        step that alters elevation values, which is undesirable for a
        topographic product.  The warning makes the gap explicit so the caller
        can decide how to handle it (e.g. fall back to the 10 m product).

    Parameters
    ----------
    dem_rasters : list[str]
        All downloaded tile paths (may span multiple CRSs).
    target_epsg : int
        The majority CRS as determined by :func:`_inspect_tiles`.  Used only
        as a fallback label when a tile's CRS cannot be read.
    bounds_geojson : str
        Path to the WGS84 AOI bounds GeoJSON used for the coverage check.
    scratch_folder : str
        Unused - retained for signature compatibility.
    log : Logger
        Caller-supplied logger.

    Returns
    -------
    tuple[list[str], int]
        ``(tiles, epsg)`` - the winning tile group and its CRS code.
    """

    # ── Step 1: fast path ─────────────────────────────────────────────────────
    # Group tiles by EPSG.  Tiles whose CRS cannot be read are assigned to
    # target_epsg so they are not silently lost.
    groups: dict[int, list[str]] = defaultdict(list)
    for tile_path in dem_rasters:
        epsg = get_epsg(tile_path)
        groups[epsg if epsg is not None else target_epsg].append(tile_path)

    if len(groups) == 1:
        only_epsg = next(iter(groups))
        log.info(
            f"All {len(dem_rasters)} tile(s) are in EPSG:{only_epsg} - no CRS selection needed"
        )
        return dem_rasters, only_epsg

    # ── Multiple CRSs - log the situation clearly before doing anything ───────
    summary = "  |  ".join(
        f"EPSG:{e}: {len(t)} tile(s)"
        for e, t in sorted(groups.items(), key=lambda x: -len(x[1]))
    )
    log.warning("=" * 72)
    log.warning("MIXED-CRS TILE SET DETECTED")
    log.warning(
        f"The downloaded tiles span {len(groups)} different coordinate reference "
        f"systems, most likely because the AOI straddles a UTM zone boundary."
    )
    log.warning(f"Tile breakdown:  {summary}")
    log.warning(
        "Strategy: measure AOI coverage for each CRS group and use the one "
        "with the best coverage.  No reprojection will be performed."
    )
    log.warning("=" * 72)

    # ── Step 2: measure coverage for every CRS group ───────────────────────────
    # Load AOI once - same polygon is tested against every group.
    aoi_polygon = load_geojson_geometry(bounds_geojson)

    best_epsg: int = target_epsg
    best_tiles: list = groups[target_epsg]
    best_coverage: float = 0.0

    for epsg, tiles in sorted(groups.items(), key=lambda x: -len(x[1])):
        log.info(f"Measuring coverage for EPSG:{epsg} ({len(tiles)} tile(s)) ...")
        coverage = _compute_tile_coverage(tiles, epsg, aoi_polygon, log)
        log.info(f"  EPSG:{epsg} covers {coverage:.2%} of the AOI")
        if coverage > best_coverage:
            best_coverage = coverage
            best_epsg = epsg
            best_tiles = tiles

    # ── Report outcome ────────────────────────────────────────────────────────────
    discarded_count = len(dem_rasters) - len(best_tiles)
    log.warning(
        f"Selected EPSG:{best_epsg} ({len(best_tiles)} tile(s), {best_coverage:.2%} AOI coverage) "
        f"as the best available single-CRS tile set."
    )
    log.warning(
        f"Discarding {discarded_count} tile(s) from other CRS group(s) - "
        f"no reprojection will be performed."
    )
    for epsg, tiles in groups.items():
        if epsg != best_epsg:
            for t in tiles:
                log.warning(f"  Discarded: {os.path.basename(t)}  (EPSG:{epsg})")

    if best_coverage < 0.999:
        log.warning(
            f"WARNING: The selected tile set covers only {best_coverage:.2%} of the AOI. "
            f"The remaining {1 - best_coverage:.2%} will have no elevation data in the output DEM. "
            f"This is a consequence of the AOI straddling a UTM zone boundary and the "
            f"decision not to reproject tiles."
        )

    return best_tiles, best_epsg


# ── Private: tile coverage check ────────────────────────────────────────────


def _compute_tile_coverage(
    tiles: list[str],
    tiles_epsg: int,
    aoi_polygon_wgs84,
    log: Logger,
) -> float:
    """
    Return the fraction of the AOI covered by the union of tile extents (0.0-1.0).

    The check is performed in the tiles' native CRS rather than WGS84.
    Transforming tile bounding-box corners to WGS84 and taking the
    axis-aligned bounding box over-estimates coverage near UTM zone boundaries
    because a UTM rectangle does not project to a WGS84 rectangle.  Working
    in the tile's native CRS avoids this: the tiles really are axis-aligned
    rectangles there, and the AOI is reprojected into that space so the
    overlap calculation is geometrically exact.

    Parameters
    ----------
    tiles : list[str]
        Tile paths to measure (all assumed to be in *tiles_epsg*).
    tiles_epsg : int
        The CRS of the tiles.
    aoi_polygon_wgs84 : shapely geometry
        The AOI polygon in WGS84 (from :func:`load_geojson_geometry`).
    log : Logger
        Caller-supplied logger.

    Returns
    -------
    float
        Coverage fraction in [0.0, 1.0].  Returns 0.0 if no tiles can be
        opened or the AOI cannot be projected.
    """
    # ── Project the AOI from WGS84 into the tile's native CRS ──────────────────
    # We need the AOI expressed in the same coordinate space as the tiles so
    # that the overlap calculation is geometrically meaningful.  The tiles are
    # perfectly rectangular in their native CRS; the AOI is an irregular
    # polygon that we densify by transforming all of its vertices.
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)
    wgs84_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    tile_srs = osr.SpatialReference()
    tile_srs.ImportFromEPSG(tiles_epsg)
    tile_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    ct = osr.CoordinateTransformation(wgs84_srs, tile_srs)

    def _reproject_coords(x_arr, y_arr):
        """Transform coordinate arrays from WGS84 to tile CRS using OSR."""
        # shapely.ops.transform passes numpy-like arrays; zip works on both
        out = [ct.TransformPoint(float(x), float(y))[:2] for x, y in zip(x_arr, y_arr)]
        return [p[0] for p in out], [p[1] for p in out]

    # shapely_transform applies _reproject_coords to every coordinate ring in
    # the geometry (exterior + holes), handling MultiPolygons automatically.
    try:
        aoi_in_tile_crs = shapely_transform(_reproject_coords, aoi_polygon_wgs84)
    except Exception as exc:
        log.warning(
            f"  Could not project AOI into EPSG:{tiles_epsg} for coverage check: {exc}"
        )
        return 0.0

    # ── Build the union of tile extents in the tile's native CRS ─────────────
    # Tiles ARE rectangles in their native CRS - read extent directly from
    # the geotransform, no coordinate transformation needed.
    tile_boxes = []
    for tile_path in tiles:
        ds = gdal.Open(tile_path, gdal.GA_ReadOnly)
        if ds is None:
            log.warning(
                f"  Could not open {os.path.basename(tile_path)} for coverage check - skipping"
            )
            continue

        gt = ds.GetGeoTransform()
        width, height = ds.RasterXSize, ds.RasterYSize
        ds = None  # close immediately

        # gt[0], gt[3] = top-left corner (x=easting, y=northing)
        # gt[1] = pixel width (positive); gt[5] = pixel height (negative for north-up)
        x_min = gt[0]
        y_max = gt[3]
        x_max = gt[0] + width * gt[1]
        y_min = gt[3] + height * gt[5]
        tile_boxes.append(shapely_box(x_min, y_min, x_max, y_max))

    if not tile_boxes:
        log.warning("  No valid tiles could be opened for coverage check")
        return 0.0

    # ── Compute coverage fraction ──────────────────────────────────────────────
    tile_union = unary_union(tile_boxes)
    covered_area = aoi_in_tile_crs.intersection(tile_union).area
    aoi_area = aoi_in_tile_crs.area
    coverage = covered_area / aoi_area if aoi_area > 0 else 0.0

    log.info(f"  Coverage (checked in EPSG:{tiles_epsg}): {coverage:.2%}")
    return coverage


def _write_tile_footprints_gpkg(
    dem_rasters: list[str],
    final_epsg: int,
    gpkg_path: str,
    log: Logger,
) -> None:
    """
    Create a GeoPackage with **two layers**, each with one feature per
    downloaded DEM tile, in WGS84.

    **tile_footprints** (``ogr.wkbPolygon``)
        Simple bounding-box polygon for each tile.  This is the authoritative
        record of exactly which tiles contributed to the DEM mosaic, what their
        native CRS was, and whether any would require reprojection.

    **data_footprints** (``ogr.wkbMultiPolygon``)
        Polygonised outline of actual non-nodata pixels in each tile, reprojected
        to WGS84.  Where the tile has no nodata value set (or if polygonization
        yields no valid-data features), this falls back to the bounding-box
        polygon from ``tile_footprints``.

    Both layers share the same attribute schema (see :data:`_TILE_SCHEMA`).

    Parameters
    ----------
    dem_rasters : list[str]
        Tile paths that were actually used in the DEM mosaic.
    final_epsg : int
        The EPSG code chosen for the final mosaic.
    gpkg_path : str
        Output GeoPackage path.  Overwritten if it already exists.
    log : Logger
        Caller-supplied logger.
    """
    log.info(f"Writing tile footprints GeoPackage: {gpkg_path}")
    safe_makedirs(os.path.dirname(gpkg_path))

    # Resolve the final CRS name once — it is the same for every feature.
    final_srs = osr.SpatialReference()
    final_srs.ImportFromEPSG(final_epsg)
    final_crs_name = final_srs.GetName() or f"EPSG:{final_epsg}"

    # WGS84 target SRS used for reprojecting tile corners / data footprints.
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)
    wgs84_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    # ── Layer 1: tile_footprints ──────────────────────────────────────────
    # Simple bounding-box polygon per tile, in WGS84.
    # GeopackageLayer.create() handles deleting the existing GPKG and creating
    # a fresh datasource + layer in one call.
    n_written = n_skipped = 0
    with GeopackageLayer(gpkg_path, "tile_footprints", write=True) as lyr:
        lyr.create(ogr.wkbPolygon, epsg=4326, fields=_TILE_SCHEMA)

        for tile_path in dem_rasters:
            tile_ds = gdal.Open(tile_path, gdal.GA_ReadOnly)
            if tile_ds is None:
                log.warning(
                    f"  Could not open {os.path.basename(tile_path)} - skipping footprint"
                )
                n_skipped += 1
                continue

            gt = tile_ds.GetGeoTransform()
            width = tile_ds.RasterXSize
            height = tile_ds.RasterYSize
            nodata = tile_ds.GetRasterBand(1).GetNoDataValue()

            src_srs = osr.SpatialReference()
            src_srs.ImportFromWkt(tile_ds.GetProjection())
            src_srs.AutoIdentifyEPSG()
            src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            tile_ds = None  # close file handle

            epsg_code_str = src_srs.GetAuthorityCode(None)
            src_epsg = int(epsg_code_str) if epsg_code_str else None
            src_crs_name = src_srs.GetName() or (
                f"EPSG:{src_epsg}" if src_epsg else "Unknown"
            )

            x_min = gt[0]
            y_max = gt[3]
            x_max = gt[0] + width * gt[1]
            y_min = gt[3] + height * gt[5]

            # Transform the four bounding-box corners to WGS84.
            ct = osr.CoordinateTransformation(src_srs, wgs84_srs)
            corners_native = [
                (x_min, y_min),
                (x_min, y_max),
                (x_max, y_max),
                (x_max, y_min),
            ]
            corners_wgs84 = [ct.TransformPoint(x, y)[:2] for x, y in corners_native]
            geom = Polygon(corners_wgs84)  # Shapely closes the ring automatically

            attrs = {
                "filename": os.path.basename(tile_path),
                "source_path": tile_path,
                "original_epsg": src_epsg or 0,
                "original_crs_name": src_crs_name,
                "is_reprojected": 0
                if (src_epsg is None or src_epsg == final_epsg)
                else 1,
                "final_epsg": final_epsg,
                "final_crs_name": final_crs_name,
                "width_px": width,
                "height_px": height,
                "resolution_m": abs(gt[1]),
                "file_size_mb": os.path.getsize(tile_path) / 1_048_576,
                "x_min_native": x_min,
                "y_min_native": y_min,
                "x_max_native": x_max,
                "y_max_native": y_max,
            }
            if nodata is not None:
                attrs["nodata_value"] = nodata

            lyr.create_feature(geom, attrs)
            n_written += 1

    log.info(
        f"Tile footprints layer written: {gpkg_path}  "
        f"({n_written} feature(s)"
        + (f", {n_skipped} skipped" if n_skipped else "")
        + ")"
    )

    # ── Layer 2: data_footprints ─────────────────────────────────────────
    # Polygonised outline of non-nodata pixels per tile, in WGS84.
    # Opening the same GPKG path with a new layer name opens it in update
    # mode via the DatasetRegistry — tile_footprints is preserved.
    mem_drv = ogr.GetDriverByName("Memory")
    n_data_written = n_data_skipped = 0
    with GeopackageLayer(gpkg_path, "data_footprints", write=True) as lyr2:
        lyr2.create(ogr.wkbMultiPolygon, epsg=4326, fields=_TILE_SCHEMA)

        for tile_path in dem_rasters:
            tile_ds = gdal.Open(tile_path, gdal.GA_ReadOnly)
            if tile_ds is None:
                log.warning(
                    f"  [data_footprints] Could not open {os.path.basename(tile_path)} - skipping"
                )
                n_data_skipped += 1
                continue

            gt_d = tile_ds.GetGeoTransform()
            width_d = tile_ds.RasterXSize
            height_d = tile_ds.RasterYSize
            band_d = tile_ds.GetRasterBand(1)
            nodata_d = band_d.GetNoDataValue()

            src_srs_d = osr.SpatialReference()
            src_srs_d.ImportFromWkt(tile_ds.GetProjection())
            src_srs_d.AutoIdentifyEPSG()
            src_srs_d.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

            epsg_d_str = src_srs_d.GetAuthorityCode(None)
            src_epsg_d = int(epsg_d_str) if epsg_d_str else None
            src_crs_d = src_srs_d.GetName() or (
                f"EPSG:{src_epsg_d}" if src_epsg_d else "Unknown"
            )

            x_min_d = gt_d[0]
            y_max_d = gt_d[3]
            x_max_d = gt_d[0] + width_d * gt_d[1]
            y_min_d = gt_d[3] + height_d * gt_d[5]

            arr = band_d.ReadAsArray()
            tile_ds = None  # release file handle

            # Build binary data-presence mask
            if nodata_d is not None and not np.isnan(nodata_d):
                mask_arr = np.where(arr == nodata_d, np.uint8(0), np.uint8(1))
            elif nodata_d is not None and np.isnan(nodata_d):
                mask_arr = (
                    np.where(np.isnan(arr), np.uint8(0), np.uint8(1))
                    if np.issubdtype(arr.dtype, np.floating)
                    else np.ones((height_d, width_d), dtype=np.uint8)
                )
            else:
                mask_arr = np.ones((height_d, width_d), dtype=np.uint8)

            # Polygonize into an in-memory OGR layer
            mem_raster_drv = gdal.GetDriverByName("MEM")
            mask_ds = mem_raster_drv.Create("", width_d, height_d, 1, gdal.GDT_Byte)
            mask_ds.SetGeoTransform(gt_d)
            mask_ds.SetProjection(src_srs_d.ExportToWkt())
            mask_band = mask_ds.GetRasterBand(1)
            mask_band.WriteArray(mask_arr)
            mask_band.SetNoDataValue(0)

            poly_ds = mem_drv.CreateDataSource("")
            poly_layer = poly_ds.CreateLayer("polygons", srs=src_srs_d)
            poly_layer.CreateField(ogr.FieldDefn("val", ogr.OFTInteger))
            val_idx = poly_layer.GetLayerDefn().GetFieldIndex("val")
            gdal.Polygonize(mask_band, None, poly_layer, val_idx, [], callback=None)
            mask_ds = None

            valid_geoms = []
            poly_layer.ResetReading()
            for poly_feat in poly_layer:
                if poly_feat.GetField("val") == 1:
                    geom_ref = poly_feat.GetGeometryRef()
                    if geom_ref is not None:
                        valid_geoms.append(shapely_wkt.loads(geom_ref.ExportToWkt()))
            poly_ds = None

            # Union valid geometries; fall back to the bounding box if empty
            if valid_geoms:
                union_geom = unary_union(valid_geoms)
                use_bbox_fallback = union_geom.is_empty
            else:
                use_bbox_fallback = True

            if use_bbox_fallback:
                log.warning(
                    f"  [data_footprints] No valid-data polygons for "
                    f"{os.path.basename(tile_path)} - using bounding box"
                )
                union_geom = shapely_box(x_min_d, y_min_d, x_max_d, y_max_d)

            # Reproject to WGS84 (as OGR geometry so Transform() can be used in-place)
            wgs84_d = osr.SpatialReference()
            wgs84_d.ImportFromEPSG(4326)
            wgs84_d.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            ct_d = osr.CoordinateTransformation(src_srs_d, wgs84_d)

            ogr_geom = ogr.CreateGeometryFromWkt(union_geom.wkt)
            if ogr_geom is None:
                log.warning(
                    f"  [data_footprints] CreateGeometryFromWkt returned None for {os.path.basename(tile_path)} - skipping"
                )
                n_data_skipped += 1
                continue
            if ogr_geom.Transform(ct_d) != 0:
                log.warning(
                    f"  [data_footprints] Reprojection failed for {os.path.basename(tile_path)} - skipping"
                )
                n_data_skipped += 1
                continue
            ogr_geom = ogr.ForceTo(ogr_geom, ogr.wkbMultiPolygon)
            if ogr_geom is None:
                log.warning(
                    f"  [data_footprints] ForceTo(wkbMultiPolygon) returned None for {os.path.basename(tile_path)} - skipping"
                )
                n_data_skipped += 1
                continue

            attrs_d = {
                "filename": os.path.basename(tile_path),
                "source_path": tile_path,
                "original_epsg": src_epsg_d or 0,
                "original_crs_name": src_crs_d,
                "is_reprojected": 0
                if (src_epsg_d is None or src_epsg_d == final_epsg)
                else 1,
                "final_epsg": final_epsg,
                "final_crs_name": final_crs_name,
                "width_px": width_d,
                "height_px": height_d,
                "resolution_m": abs(gt_d[1]),
                "file_size_mb": os.path.getsize(tile_path) / 1_048_576,
                "x_min_native": x_min_d,
                "y_min_native": y_min_d,
                "x_max_native": x_max_d,
                "y_max_native": y_max_d,
            }
            if nodata_d is not None:
                attrs_d["nodata_value"] = nodata_d

            # create_feature() accepts ogr.Geometry directly
            lyr2.create_feature(ogr_geom, attrs_d)
            n_data_written += 1

    log.info(
        f"Data footprints layer written: {gpkg_path}  "
        f"({n_data_written} feature(s)"
        + (f", {n_data_skipped} skipped" if n_data_skipped else "")
        + ")"
    )


def should_resample(
    dem_rasters: list[str], output_res: float, threshold: float = _RESAMPLE_THRESHOLD
) -> bool:
    """
    Return True if the average source resolution differs from output_res by
    more than *threshold* (relative, default 10%).

    Adapted from rscontext_3dep/rscontext_3dep/dem_builder.py.

    .. deprecated::
        This function is not called anywhere inside ``rs_context_neo``.
        Resolution inspection and the resample decision were merged into
        :func:`_inspect_tiles` (a single-pass approach that opens each tile
        only once).  This function is retained here for reference and to
        keep parity with the original ``rscontext_3dep`` implementation it
        was adapted from.
    """
    log = Logger("Resolution Check")
    resolutions = []

    for raster_path in dem_rasters:
        ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
        if ds is None:
            log.warning(f"Could not open {raster_path} - skipping resolution check")
            continue
        gt = ds.GetGeoTransform()
        ds = None
        if gt is None:
            log.warning(f"No geotransform for {raster_path} - skipping")
            continue
        resolutions.append((abs(gt[1]) + abs(gt[5])) / 2.0)

    if not resolutions:
        log.warning("No valid source resolutions found - will resample by default")
        return True

    avg = sum(resolutions) / len(resolutions)
    rel_diff = abs(avg - output_res) / avg
    log.info(
        f"Source avg resolution: {avg:.3f} m  →  target: {output_res} m  (relative diff: {rel_diff:.1%})"
    )

    if rel_diff > threshold:
        log.info("Resampling required.")
        return True

    log.info("Source resolution close enough to target - resampling not required.")
    return False
