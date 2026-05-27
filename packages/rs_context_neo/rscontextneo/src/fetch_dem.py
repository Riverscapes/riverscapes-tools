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
import traceback
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import numpy as np

from osgeo import gdal, ogr, osr
import shapely.wkt as shapely_wkt
from shapely.geometry import box as shapely_box, shape
from shapely.ops import transform as shapely_transform, unary_union

from rsxml import Logger
from rsxml.util import safe_makedirs, safe_remove_file
from rscommons.download_dem import verify_areas, find_rasters
from rscommons.download import download_unzip, download_file
from rscommons.national_map import get_1m_dem_urls
from rscommons.geographic_raster import gdal_dem_geographic
from rscommons.raster_warp import raster_vrt_stitch

# Output paths (relative to the project output_folder)
DEM_RELPATH             = 'topography/dem.tif'
HILLSHADE_RELPATH       = 'topography/dem_hillshade.tif'
SLOPE_RELPATH           = 'topography/slope.tif'
TILE_FOOTPRINTS_RELPATH = 'topography/tile_footprints.gpkg'

# Degrees to buffer the bounds polygon when querying The National Map
_BUFFER_DIST_DEG = 0.01

# Relative difference in resolution below which we skip resampling
_RESAMPLE_THRESHOLD = 0.1

# GDAL creation options applied to the output DEM (PREDICTOR=2 = horizontal
# differencing, ideal for continuous elevation data; shrinks files ~50-70%)
_DEM_CREATION_OPTIONS = ['COMPRESS=DEFLATE', 'PREDICTOR=2', 'TILED=YES', 'BIGTIFF=IF_SAFER']

# Number of tiles to download simultaneously.  Network I/O is the bottleneck
# so threads (not processes) are the right tool - the GIL doesn't matter here.
# 4 workers is a safe default that saturates a typical connection without
# hammering the USGS TNM servers hard enough to trigger throttling.
_DEFAULT_DOWNLOAD_WORKERS = 4


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
    debug: bool = False
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
    log = Logger('Fetch DEM')

    if not os.path.exists(bounds_geojson):
        raise FileNotFoundError(f'Bounds GeoJSON not found: {bounds_geojson}')

    # Early-exit: if both outputs already exist and the caller hasn't asked for
    # a forced re-download, skip the entire tile-query / download / mosaic pipeline.
    dem_path       = os.path.join(output_folder, DEM_RELPATH)
    hillshade_path = os.path.join(output_folder, HILLSHADE_RELPATH)
    slope_path     = os.path.join(output_folder, SLOPE_RELPATH)
    if not force_download and os.path.isfile(dem_path) and os.path.isfile(hillshade_path) and os.path.isfile(slope_path):
        log.info(f'DEM already exists at {dem_path} - skipping download (pass force_download=True to override)')
        return dem_path, hillshade_path, slope_path

    # rscommons functions (download_dem, verify_areas) open vector files via
    # get_shp_or_gpkg which forces the GPKG driver.  GeoJSON isn't supported
    # by that path, so we convert once here and use the GeoPackage throughout.
    bounds_gpkg = os.path.join(scratch_folder, 'bounds.gpkg')
    safe_makedirs(scratch_folder)
    _geojson_to_gpkg(bounds_geojson, bounds_gpkg)
    # rscommons path_sorter returns (filepath, None) when the file exists,
    # so GeoPackage layer lookup fails.  Append the layer name as a compound
    # path (/path/to/bounds.gpkg/bounds) so the regex branch is used instead.
    bounds_gpkg_layer = bounds_gpkg + '/bounds'

    ned_download_folder = os.path.join(download_folder, 'ned')
    ned_unzip_folder = os.path.join(scratch_folder, 'ned')

    # ── 1. Identify and download tiles ───────────────────────────────────────
    log.info('Querying The National Map for 3DEP 1 m tiles ...')
    source_urls = get_1m_dem_urls(bounds_gpkg_layer, _BUFFER_DIST_DEG)
    log.info(f'{len(source_urls)} tile(s) identified on The National Map')

    dem_rasters = _download_tiles_parallel(
        source_urls, ned_download_folder, ned_unzip_folder,
        force_download, workers=download_workers,
    )
    log.info(f'{len(dem_rasters)} tile(s) ready')

    # ── 2. Inspect tiles once: determine CRS and whether resampling is needed ─
    # A single pass avoids opening every raster file twice (previously
    # get_best_crs and should_resample each iterated the full tile list).
    output_epsg, resample = _inspect_tiles(dem_rasters, output_res)
    if output_epsg is None:
        raise ValueError(
            'Could not determine a valid EPSG code from the downloaded DEM tiles. '
            'Check that the tiles are valid GeoTIFF/IMG files with embedded CRS information.'
        )
    log.info(f'Output EPSG: {output_epsg}')

    # ── 3. Mosaic / clip / (optionally) resample ─────────────────────────────

    need_dem_rebuild = force_download or not os.path.exists(dem_path) or resample

    if need_dem_rebuild:
        log.info('Mosaicing and clipping DEM tiles ...')
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

        warp_options: dict = {'cutlineBlend': 1, 'creationOptions': _DEM_CREATION_OPTIONS}
        if resample:
            log.info(f'Resampling to {output_res} m (bilinear)')
            warp_options.update({
                'xRes': output_res,
                'yRes': output_res,
                'resampleAlg': 'bilinear',
            })

        # bounds_geojson works here - raster_vrt_stitch uses gdal.WarpOptions
        # (cutlineDSName) which does native OGR auto-detection, not get_shp_or_gpkg.
        raster_vrt_stitch(dem_rasters, dem_path, output_epsg, clip=bounds_geojson, warp_options=warp_options)

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
            log.info(f'Removing unzipped tile cache: {ned_unzip_folder}')
            shutil.rmtree(ned_unzip_folder, ignore_errors=True)
    else:
        log.info('DEM already exists and no resample needed - skipping rebuild (pass force_download=True to override)')

    # ── 4. Verify coverage ────────────────────────────────────────────────────
    area_ratio = verify_areas(dem_path, bounds_gpkg_layer)
    if area_ratio < 0.85:
        log.warning(
            f'DEM covers only {area_ratio:.1%} of the AOI bounds (threshold: 85%). '
            '3DEP 1 m data may not be available for this region; consider using the 10 m product.'
        )

    # ── 5. Hillshade ─────────────────────────────────────────────────────────
    need_hs_rebuild = need_dem_rebuild or not os.path.isfile(hillshade_path)
    if need_hs_rebuild:
        log.info('Generating hillshade ...')
        if is_geographic_epsg(output_epsg):
            gdal_dem_geographic(dem_path, hillshade_path, 'hillshade')
        else:
            gdal.DEMProcessing(hillshade_path, dem_path, 'hillshade', creationOptions=['COMPRESS=DEFLATE'])
    else:
        log.info('Hillshade already exists - skipping rebuild')

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
        log.info('Generating slope raster (gdal.DEMProcessing, degrees) ...')
        result = gdal.DEMProcessing(
            slope_path, dem_path, 'slope',
            creationOptions=['COMPRESS=DEFLATE', 'PREDICTOR=2', 'TILED=YES', 'BIGTIFF=IF_SAFER'],
        )
        if result is None:
            log.warning(f'gdal.DEMProcessing slope failed: {gdal.GetLastErrorMsg()}')
        else:
            result = None  # flush
            log.info(f'Slope:     {slope_path}')
    else:
        log.info('Slope already exists - skipping rebuild')

    log.info(f'DEM:       {dem_path}  ({os.path.getsize(dem_path) / 1_048_576:.1f} MB, compressed)')
    log.info(f'Hillshade: {hillshade_path}')
    log.info(f'Resolution: {output_res} m  |  EPSG: {output_epsg}  |  Coverage: {area_ratio:.1%}  |  Tiles: {len(source_urls)}')

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
    log = Logger('Download Tiles')
    safe_makedirs(download_folder)
    safe_makedirs(unzip_folder)

    def _download_one(url: str) -> str:
        base_path = os.path.basename(os.path.splitext(url)[0])
        final_unzip_path = os.path.join(unzip_folder, base_path)
        if url.lower().endswith('.zip'):
            file_path = download_unzip(url, download_folder, final_unzip_path, force_download)
            return find_rasters(file_path)
        else:
            return download_file(url, download_folder, force_download)

    effective_workers = min(workers, len(urls))
    log.info(f'Downloading {len(urls)} tile(s) with {effective_workers} worker(s) ...')

    raster_paths: list[str] = []
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        future_to_url = {executor.submit(_download_one, url): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                raster_paths.append(future.result())
                log.info(f'  ✓  {os.path.basename(url)}')
            except Exception as exc:
                log.error(f'  ✗  {os.path.basename(url)}: {exc}')
                errors.append(url)

    if errors:
        raise Exception(
            f'{len(errors)} tile(s) failed to download:\n'
            + '\n'.join(f'  {u}' for u in errors)
        )

    return raster_paths


def _inspect_tiles(dem_rasters: list[str], output_res: float) -> tuple[int | None, bool]:
    """
    Open each tile *once* and return (best_epsg, needs_resample).

    Previously get_best_crs and should_resample each iterated over the full
    tile list, opening every file twice.  This single pass halves the I/O.

    Resolution check uses only the *first* valid tile - all tiles in a 3DEP
    product tier share the same pixel size, so averaging across all of them
    produces identical results at unnecessary cost.
    """
    log = Logger('Inspect Tiles')
    epsg_codes: list[int] = []
    first_res: float | None = None

    for raster_path in dem_rasters:
        ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
        if ds is None:
            log.warning(f'Could not open {raster_path} - skipping')
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
        log.info(f'Best CRS: EPSG:{best_epsg} (frequency {max_freq}/{len(epsg_codes)})')
    else:
        log.error('Could not determine a valid EPSG from any tile.')

    # Resample decision
    needs_resample = True  # default: resample if we can't read source res
    if first_res is not None:
        rel_diff = abs(first_res - output_res) / first_res
        log.info(f'Source resolution: {first_res:.3f} m → target: {output_res} m (Δ {rel_diff:.1%})')
        needs_resample = rel_diff > _RESAMPLE_THRESHOLD
        log.info('Resampling required.' if needs_resample else 'Source resolution close enough - no resample needed.')
    else:
        log.warning('No valid source resolution found - resampling by default.')

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
        log.info(f'All {len(dem_rasters)} tile(s) are in EPSG:{only_epsg} - no CRS selection needed')
        return dem_rasters, only_epsg

    # ── Multiple CRSs - log the situation clearly before doing anything ───────
    summary = '  |  '.join(
        f'EPSG:{e}: {len(t)} tile(s)'
        for e, t in sorted(groups.items(), key=lambda x: -len(x[1]))
    )
    log.warning('=' * 72)
    log.warning('MIXED-CRS TILE SET DETECTED')
    log.warning(
        f'The downloaded tiles span {len(groups)} different coordinate reference '
        f'systems, most likely because the AOI straddles a UTM zone boundary.'
    )
    log.warning(f'Tile breakdown:  {summary}')
    log.warning(
        'Strategy: measure AOI coverage for each CRS group and use the one '
        'with the best coverage.  No reprojection will be performed.'
    )
    log.warning('=' * 72)

    # ── Step 2: measure coverage for every CRS group ───────────────────────────
    # Load AOI once - same polygon is tested against every group.
    aoi_polygon = _load_aoi_polygon_wgs84(bounds_geojson)

    best_epsg:     int   = target_epsg
    best_tiles:    list  = groups[target_epsg]
    best_coverage: float = 0.0

    for epsg, tiles in sorted(groups.items(), key=lambda x: -len(x[1])):
        log.info(f'Measuring coverage for EPSG:{epsg} ({len(tiles)} tile(s)) ...')
        coverage = _compute_tile_coverage(tiles, epsg, aoi_polygon, log)
        log.info(f'  EPSG:{epsg} covers {coverage:.2%} of the AOI')
        if coverage > best_coverage:
            best_coverage = coverage
            best_epsg     = epsg
            best_tiles    = tiles

    # ── Report outcome ────────────────────────────────────────────────────────────
    discarded_count = len(dem_rasters) - len(best_tiles)
    log.warning(
        f'Selected EPSG:{best_epsg} ({len(best_tiles)} tile(s), {best_coverage:.2%} AOI coverage) '
        f'as the best available single-CRS tile set.'
    )
    log.warning(
        f'Discarding {discarded_count} tile(s) from other CRS group(s) - '
        f'no reprojection will be performed.'
    )
    for epsg, tiles in groups.items():
        if epsg != best_epsg:
            for t in tiles:
                log.warning(f'  Discarded: {os.path.basename(t)}  (EPSG:{epsg})')

    if best_coverage < 0.999:
        log.warning(
            f'WARNING: The selected tile set covers only {best_coverage:.2%} of the AOI. '
            f'The remaining {1 - best_coverage:.2%} will have no elevation data in the output DEM. '
            f'This is a consequence of the AOI straddling a UTM zone boundary and the '
            f'decision not to reproject tiles.'
        )

    return best_tiles, best_epsg


def _load_aoi_polygon_wgs84(bounds_geojson: str):
    """
    Return a Shapely geometry representing the AOI in WGS84.

    Handles GeoJSON FeatureCollections, Features, and bare geometry objects.
    The returned geometry is the union of all features so multi-polygon AOIs
    are handled correctly.
    """
    with open(bounds_geojson, encoding='utf-8') as f:
        data = json.load(f)

    geoj_type = data.get('type', '')
    if geoj_type == 'FeatureCollection':
        geoms = [shape(feat['geometry']) for feat in data.get('features', []) if feat.get('geometry')]
    elif geoj_type == 'Feature':
        geoms = [shape(data['geometry'])] if data.get('geometry') else []
    else:
        # Bare geometry object
        geoms = [shape(data)]

    if not geoms:
        raise ValueError(f'No geometries found in bounds GeoJSON: {bounds_geojson}')

    return unary_union(geoms)


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
        The AOI polygon in WGS84 (from :func:`_load_aoi_polygon_wgs84`).
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
        log.warning(f'  Could not project AOI into EPSG:{tiles_epsg} for coverage check: {exc}')
        return 0.0

    # ── Build the union of tile extents in the tile's native CRS ─────────────
    # Tiles ARE rectangles in their native CRS - read extent directly from
    # the geotransform, no coordinate transformation needed.
    tile_boxes = []
    for tile_path in tiles:
        ds = gdal.Open(tile_path, gdal.GA_ReadOnly)
        if ds is None:
            log.warning(f'  Could not open {os.path.basename(tile_path)} for coverage check - skipping')
            continue

        gt = ds.GetGeoTransform()
        width, height = ds.RasterXSize, ds.RasterYSize
        ds = None  # close immediately

        # gt[0], gt[3] = top-left corner (x=easting, y=northing)
        # gt[1] = pixel width (positive); gt[5] = pixel height (negative for north-up)
        x_min = gt[0]
        y_max = gt[3]
        x_max = gt[0] + width  * gt[1]
        y_min = gt[3] + height * gt[5]
        tile_boxes.append(shapely_box(x_min, y_min, x_max, y_max))

    if not tile_boxes:
        log.warning('  No valid tiles could be opened for coverage check')
        return 0.0

    # ── Compute coverage fraction ──────────────────────────────────────────────
    tile_union    = unary_union(tile_boxes)
    covered_area  = aoi_in_tile_crs.intersection(tile_union).area
    aoi_area      = aoi_in_tile_crs.area
    coverage      = covered_area / aoi_area if aoi_area > 0 else 0.0

    log.info(f'  Coverage (checked in EPSG:{tiles_epsg}): {coverage:.2%}')
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

    Both layers share the same attribute schema (see below).  The GeoPackage is
    written immediately after the mosaic is assembled (before scratch cleanup)
    so it reflects the complete download set including any tiles that were
    ultimately discarded because the majority CRS alone provided full coverage.

    Attribute schema
    ----------------
    filename          Original base filename of the downloaded tile.
    source_path       Absolute path to the tile on disk at mosaic time.
    original_epsg     EPSG code of the tile's native CRS (integer).
    original_crs_name Human-readable name of the native CRS (e.g.
                      ``'NAD83 / UTM zone 11N'``).
    is_reprojected    1 if this tile's CRS differed from *final_epsg* and
                      required reprojection to be included in the mosaic;
                      0 if it was already in the correct CRS.  Only tiles
                      that were actually used in the mosaic are recorded here.
    final_epsg        The EPSG code used for the assembled DEM mosaic.
    final_crs_name    Human-readable name of the final mosaic CRS.
    width_px          Tile width in pixels.
    height_px         Tile height in pixels.
    resolution_m      Pixel size in metres (x-direction; square pixels assumed).
    file_size_mb      File size in megabytes at time of writing.
    nodata_value      Raster nodata sentinel value (NULL if none is set).
    x_min_native      Left edge of the tile extent in its native CRS.
    y_min_native      Bottom edge of the tile extent in its native CRS.
    x_max_native      Right edge of the tile extent in its native CRS.
    y_max_native      Top edge of the tile extent in its native CRS.

    Parameters
    ----------
    dem_rasters : list[str]
        Tile paths that were actually used in the DEM mosaic (after CRS
        resolution — discarded tiles are excluded).
    final_epsg : int
        The EPSG code that was chosen for the final mosaic.
    gpkg_path : str
        Output GeoPackage path.  Overwritten if it already exists.
    log : Logger
        Caller-supplied logger.
    """
    log.info(f'Writing tile footprints GeoPackage: {gpkg_path}')

    # Overwrite any existing file so a force-rebuild always produces a fresh record.
    driver = ogr.GetDriverByName('GPKG')
    if os.path.exists(gpkg_path):
        driver.DeleteDataSource(gpkg_path)
    safe_makedirs(os.path.dirname(gpkg_path))
    ds = driver.CreateDataSource(gpkg_path)
    if ds is None:
        log.warning(f'Could not create tile footprints GeoPackage at {gpkg_path} - skipping')
        return

    # The footprint geometries are stored in WGS84 so they are immediately
    # viewable in any GIS without needing to know the project CRS.
    wgs84 = osr.SpatialReference()
    wgs84.ImportFromEPSG(4326)
    wgs84.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    layer = ds.CreateLayer('tile_footprints', srs=wgs84, geom_type=ogr.wkbPolygon)

    # ── Schema ───────────────────────────────────────────────────────────────────
    fields = [
        ('filename',          ogr.OFTString),   # original base filename
        ('source_path',       ogr.OFTString),   # absolute path on disk at mosaic time
        ('original_epsg',     ogr.OFTInteger),  # native EPSG code
        ('original_crs_name', ogr.OFTString),   # human-readable native CRS name
        ('is_reprojected',    ogr.OFTInteger),  # 1 = would need reprojection; 0 = already in final CRS
        ('final_epsg',        ogr.OFTInteger),  # EPSG of the assembled mosaic
        ('final_crs_name',    ogr.OFTString),   # human-readable mosaic CRS name
        ('width_px',          ogr.OFTInteger),  # tile width in pixels
        ('height_px',         ogr.OFTInteger),  # tile height in pixels
        ('resolution_m',      ogr.OFTReal),     # pixel size in metres
        ('file_size_mb',      ogr.OFTReal),     # file size at write time
        ('nodata_value',      ogr.OFTReal),     # nodata sentinel (may be NULL)
        ('x_min_native',      ogr.OFTReal),     # left edge in native CRS
        ('y_min_native',      ogr.OFTReal),     # bottom edge in native CRS
        ('x_max_native',      ogr.OFTReal),     # right edge in native CRS
        ('y_max_native',      ogr.OFTReal),     # top edge in native CRS
    ]
    for field_name, field_type in fields:
        layer.CreateField(ogr.FieldDefn(field_name, field_type))

    # Resolve the final CRS name once - it is the same for every feature.
    final_srs = osr.SpatialReference()
    final_srs.ImportFromEPSG(final_epsg)
    final_crs_name = final_srs.GetName() or f'EPSG:{final_epsg}'

    feat_defn  = layer.GetLayerDefn()
    n_written  = 0
    n_skipped  = 0

    for tile_path in dem_rasters:

        # ── Open tile and read metadata ─────────────────────────────────────────
        tile_ds = gdal.Open(tile_path, gdal.GA_ReadOnly)
        if tile_ds is None:
            log.warning(f'  Could not open {os.path.basename(tile_path)} - skipping footprint')
            n_skipped += 1
            continue

        gt     = tile_ds.GetGeoTransform()
        width  = tile_ds.RasterXSize
        height = tile_ds.RasterYSize
        band   = tile_ds.GetRasterBand(1)
        nodata = band.GetNoDataValue()   # None if not set

        # Read the tile's native CRS from its embedded WKT.
        src_srs = osr.SpatialReference()
        src_srs.ImportFromWkt(tile_ds.GetProjection())
        src_srs.AutoIdentifyEPSG()
        src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        tile_ds = None  # close file handle as soon as we have what we need

        epsg_code_str = src_srs.GetAuthorityCode(None)
        src_epsg      = int(epsg_code_str) if epsg_code_str else None
        src_crs_name  = src_srs.GetName() or (f'EPSG:{src_epsg}' if src_epsg else 'Unknown')

        # ── Derive tile extent in its native CRS ─────────────────────────────────
        # gt[0], gt[3] = top-left corner (x, y)
        # gt[1]        = pixel width  (positive)
        # gt[5]        = pixel height (negative for north-up rasters)
        x_min = gt[0]
        y_max = gt[3]
        x_max = gt[0] + width  * gt[1]
        y_min = gt[3] + height * gt[5]

        # ── Transform the four corners to WGS84 for the footprint geometry ────
        # We transform the four bounding-box corners rather than the full
        # raster outline.  For UTM projections the bbox is the actual tile
        # footprint (tiles are axis-aligned in their native CRS), and the
        # curvature introduced by reprojecting four corner points to WGS84 is
        # negligible at the scale of a single 3DEP tile.
        wgs84_srs = osr.SpatialReference()
        wgs84_srs.ImportFromEPSG(4326)
        wgs84_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        ct = osr.CoordinateTransformation(src_srs, wgs84_srs)

        # Ring goes: SW → NW → NE → SE → SW (closed)
        corners_native = [
            (x_min, y_min),   # SW
            (x_min, y_max),   # NW
            (x_max, y_max),   # NE
            (x_max, y_min),   # SE
            (x_min, y_min),   # close the ring
        ]
        # TransformPoint returns (x, y, z) - slice to (lon, lat)
        corners_wgs84 = [ct.TransformPoint(x, y)[:2] for x, y in corners_native]
        ring_wkt      = ', '.join(f'{lon} {lat}' for lon, lat in corners_wgs84)
        geom          = ogr.CreateGeometryFromWkt(f'POLYGON (({ring_wkt}))')

        # ── Build and write the feature ───────────────────────────────────────────
        feat = ogr.Feature(feat_defn)
        feat.SetGeometry(geom)
        feat.SetField('filename',          os.path.basename(tile_path))
        feat.SetField('source_path',       tile_path)
        feat.SetField('original_epsg',     src_epsg or 0)
        feat.SetField('original_crs_name', src_crs_name)
        # is_reprojected = 1 means this tile's CRS differs from the mosaic CRS
        # and required reprojection.  Tiles that were discarded by _resolve_tiles
        # are never passed here, so every feature represents an actual contributor.
        feat.SetField('is_reprojected',    0 if (src_epsg is None or src_epsg == final_epsg) else 1)
        feat.SetField('final_epsg',        final_epsg)
        feat.SetField('final_crs_name',    final_crs_name)
        feat.SetField('width_px',          width)
        feat.SetField('height_px',         height)
        feat.SetField('resolution_m',      abs(gt[1]))
        feat.SetField('file_size_mb',      os.path.getsize(tile_path) / 1_048_576)
        feat.SetField('x_min_native',      x_min)
        feat.SetField('y_min_native',      y_min)
        feat.SetField('x_max_native',      x_max)
        feat.SetField('y_max_native',      y_max)
        # nodata may be None if the tile has no nodata value set - leave the
        # field NULL in that case rather than writing a meaningless 0.
        if nodata is not None:
            feat.SetField('nodata_value', nodata)

        layer.CreateFeature(feat)
        feat = None
        n_written += 1

    ds.SyncToDisk()

    log.info(
        f'Tile footprints layer written: {gpkg_path}  '
        f'({n_written} feature(s)'
        + (f', {n_skipped} skipped' if n_skipped else '')
        + ')'
    )

    # ── data_footprints layer ─────────────────────────────────────────────────
    # Each feature's geometry is the polygonised outline of non-nodata pixels
    # in that tile, reprojected to WGS84.  Falls back to the bounding box when
    # no nodata is set or when polygonization yields no valid-data polygons.

    data_layer = ds.CreateLayer('data_footprints', srs=wgs84, geom_type=ogr.wkbMultiPolygon)
    for field_name, field_type in fields:
        data_layer.CreateField(ogr.FieldDefn(field_name, field_type))

    data_feat_defn = data_layer.GetLayerDefn()
    mem_drv = ogr.GetDriverByName('Memory')
    n_data_written = 0
    n_data_skipped = 0

    for tile_path in dem_rasters:

        # ── Open tile ────────────────────────────────────────────────────────
        tile_ds = gdal.Open(tile_path, gdal.GA_ReadOnly)
        if tile_ds is None:
            log.warning(f'  [data_footprints] Could not open {os.path.basename(tile_path)} - skipping')
            n_data_skipped += 1
            continue

        gt_d   = tile_ds.GetGeoTransform()
        width_d  = tile_ds.RasterXSize
        height_d = tile_ds.RasterYSize
        band_d   = tile_ds.GetRasterBand(1)
        nodata_d = band_d.GetNoDataValue()

        src_srs_d = osr.SpatialReference()
        src_srs_d.ImportFromWkt(tile_ds.GetProjection())
        src_srs_d.AutoIdentifyEPSG()
        src_srs_d.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

        epsg_d_str  = src_srs_d.GetAuthorityCode(None)
        src_epsg_d  = int(epsg_d_str) if epsg_d_str else None
        src_crs_d   = src_srs_d.GetName() or (f'EPSG:{src_epsg_d}' if src_epsg_d else 'Unknown')

        x_min_d = gt_d[0]
        y_max_d = gt_d[3]
        x_max_d = gt_d[0] + width_d  * gt_d[1]
        y_min_d = gt_d[3] + height_d * gt_d[5]

        # ── Build binary data-presence mask ──────────────────────────────────
        arr = band_d.ReadAsArray()     # shape (height, width)
        tile_ds = None                 # release file handle

        if nodata_d is not None and not np.isnan(nodata_d):
            mask_arr = np.where(arr == nodata_d, np.uint8(0), np.uint8(1))
        elif nodata_d is not None and np.isnan(nodata_d):
            if np.issubdtype(arr.dtype, np.floating):
                mask_arr = np.where(np.isnan(arr), np.uint8(0), np.uint8(1))
            else:
                mask_arr = np.ones((height_d, width_d), dtype=np.uint8)  # integer dtype cannot hold NaN
        else:
            # No nodata value: treat all pixels as valid.
            mask_arr = np.ones((height_d, width_d), dtype=np.uint8)

        # ── Create in-memory mask raster for gdal.Polygonize ─────────────────
        mem_raster_drv = gdal.GetDriverByName('MEM')
        mask_ds = mem_raster_drv.Create('', width_d, height_d, 1, gdal.GDT_Byte)
        mask_ds.SetGeoTransform(gt_d)
        mask_ds.SetProjection(src_srs_d.ExportToWkt())
        mask_band = mask_ds.GetRasterBand(1)
        mask_band.WriteArray(mask_arr)
        mask_band.SetNoDataValue(0)

        # ── Polygonize valid-data pixels into a Memory OGR layer ──────────────
        poly_ds = mem_drv.CreateDataSource('')
        poly_layer = poly_ds.CreateLayer('polygons', srs=src_srs_d)
        poly_layer.CreateField(ogr.FieldDefn('val', ogr.OFTInteger))
        val_idx = poly_layer.GetLayerDefn().GetFieldIndex('val')
        gdal.Polygonize(mask_band, None, poly_layer, val_idx, [], callback=None)
        mask_ds = None  # release mask raster

        # ── Collect geometries where val == 1 (valid data) ────────────────────
        valid_geoms = []
        poly_layer.ResetReading()
        for poly_feat in poly_layer:
            if poly_feat.GetField('val') == 1:
                geom_ref = poly_feat.GetGeometryRef()
                if geom_ref is not None:
                    valid_geoms.append(shapely_wkt.loads(geom_ref.ExportToWkt()))
        poly_ds = None  # release memory OGR datasource

        # ── Compute union; fall back to bbox if empty ─────────────────────────
        use_bbox_fallback = False
        if valid_geoms:
            union_geom = unary_union(valid_geoms)
            if union_geom.is_empty:
                use_bbox_fallback = True
        else:
            use_bbox_fallback = True

        if use_bbox_fallback:
            log.warning(
                f'  [data_footprints] No valid-data polygons for '
                f'{os.path.basename(tile_path)} - using bounding box'
            )
            union_geom = shapely_box(x_min_d, y_min_d, x_max_d, y_max_d)

        # ── Reproject union to WGS84 ──────────────────────────────────────────
        wgs84_d = osr.SpatialReference()
        wgs84_d.ImportFromEPSG(4326)
        wgs84_d.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        ct_d  = osr.CoordinateTransformation(src_srs_d, wgs84_d)

        ogr_geom = ogr.CreateGeometryFromWkt(union_geom.wkt)
        if ogr_geom is None:
            log.warning(f'  [data_footprints] CreateGeometryFromWkt returned None for '
                        f'{os.path.basename(tile_path)} - skipping')
            n_data_skipped += 1
            continue
        if ogr_geom.Transform(ct_d) != 0:
            log.warning(f'  [data_footprints] Reprojection failed for {os.path.basename(tile_path)} - skipping')
            n_data_skipped += 1
            continue
        ogr_geom = ogr.ForceTo(ogr_geom, ogr.wkbMultiPolygon)
        if ogr_geom is None:
            log.warning(f'  [data_footprints] ForceTo(wkbMultiPolygon) returned None for {os.path.basename(tile_path)} - skipping')
            n_data_skipped += 1
            continue

        # ── Write feature ────────────────────────────────────────────────────
        data_feat = ogr.Feature(data_feat_defn)
        data_feat.SetGeometry(ogr_geom)
        data_feat.SetField('filename',          os.path.basename(tile_path))
        data_feat.SetField('source_path',       tile_path)
        data_feat.SetField('original_epsg',     src_epsg_d or 0)
        data_feat.SetField('original_crs_name', src_crs_d)
        data_feat.SetField('is_reprojected',    0 if (src_epsg_d is None or src_epsg_d == final_epsg) else 1)
        data_feat.SetField('final_epsg',        final_epsg)
        data_feat.SetField('final_crs_name',    final_crs_name)
        data_feat.SetField('width_px',          width_d)
        data_feat.SetField('height_px',         height_d)
        data_feat.SetField('resolution_m',      abs(gt_d[1]))
        data_feat.SetField('file_size_mb',      os.path.getsize(tile_path) / 1_048_576)
        data_feat.SetField('x_min_native',      x_min_d)
        data_feat.SetField('y_min_native',      y_min_d)
        data_feat.SetField('x_max_native',      x_max_d)
        data_feat.SetField('y_max_native',      y_max_d)
        if nodata_d is not None:
            data_feat.SetField('nodata_value', nodata_d)
        data_layer.CreateFeature(data_feat)
        data_feat = None
        n_data_written += 1

    ds.SyncToDisk()
    ds = None

    log.info(
        f'Data footprints layer written: {gpkg_path}  '
        f'({n_data_written} feature(s)'
        + (f', {n_data_skipped} skipped' if n_data_skipped else '')
        + ')'
    )


def _geojson_to_gpkg(geojson_path: str, gpkg_path: str) -> None:
    """
    Convert a GeoJSON file to a single-layer GeoPackage.

    rscommons functions that accept vector bounds (download_dem, verify_areas,
    etc.) route through get_shp_or_gpkg which forces the GPKG OGR driver.
    This helper produces a compatible file from the GeoJSON bounds that are
    the native format for rs_context_neo project bounds.

    Raises:
        RuntimeError: If GDAL VectorTranslate fails to produce an output file.
    """
    # Skip rebuild if the GPKG already exists and is newer than the GeoJSON
    if (
        os.path.exists(gpkg_path)
        and os.path.getmtime(gpkg_path) >= os.path.getmtime(geojson_path)
    ):
        return

    if os.path.exists(gpkg_path):
        os.remove(gpkg_path)

    result = gdal.VectorTranslate(
        gpkg_path,
        geojson_path,
        format='GPKG',
        layerName='bounds',
    )

    # VectorTranslate returns a DataSource on success, None on failure
    if result is None:
        raise RuntimeError(
            f'GDAL VectorTranslate failed to convert {geojson_path} → {gpkg_path}. '
            f'GDAL error: {gdal.GetLastErrorMsg()}'
        )
    result = None  # dereference / flush


def get_epsg(raster_path: str) -> int | None:
    """
    Return the EPSG code embedded in a raster file, or None if it cannot be
    determined.

    Adapted from rscontext_3dep/rscontext_3dep/dem_builder.py.
    """
    log = Logger('get_epsg')
    dataset = None
    try:
        dataset = gdal.Open(raster_path, gdal.GA_ReadOnly)
        if dataset is None:
            log.error(f'Could not open {raster_path} with GDAL.')
            return None

        wkt = dataset.GetProjection()
        if not wkt:
            log.warning(f'No CRS information found in {raster_path}')
            return None

        srs = osr.SpatialReference()
        if srs.ImportFromWkt(wkt) != 0:
            log.error(f'Failed to import WKT projection from {raster_path}')
            return None

        srs.AutoIdentifyEPSG()
        code_str = srs.GetAuthorityCode(None)
        if not code_str:
            log.warning(f'Could not auto-identify EPSG for {raster_path}')
            return None

        try:
            return int(code_str)
        except ValueError:
            log.error(f"Non-integer authority code '{code_str}' for {raster_path}")
            return None

    except Exception as exc:
        log.error(f'Unexpected error reading EPSG from {raster_path}: {exc}')
        log.debug(traceback.format_exc())
        return None

    finally:
        dataset = None


def get_best_crs(raster_paths: list[str]) -> int | None:
    """
    Return the EPSG code used by the majority of the supplied rasters.

    Ties are broken by choosing the lowest EPSG number.  Returns None if no
    valid EPSG code can be read from any raster.

    Adapted from rscontext_3dep/rscontext_3dep/dem_builder.py.
    """
    log = Logger('get_best_crs')

    valid_codes = [c for c in (get_epsg(p) for p in raster_paths) if c is not None]
    if not valid_codes:
        log.error('Could not determine a valid EPSG from any of the input rasters.')
        return None

    counts = Counter(valid_codes)
    max_freq = counts.most_common(1)[0][1]
    candidates = [code for code, freq in counts.items() if freq == max_freq]
    best = min(candidates)  # tie-break: lowest EPSG number

    log.info(f'Best CRS: EPSG:{best} (frequency {max_freq}/{len(valid_codes)})')
    return best


def is_geographic_epsg(epsg_code: int) -> bool:
    """
    Return True if the EPSG code refers to a geographic (lat/lon) CRS.

    Adapted from rscontext_3dep/rscontext_3dep/dem_builder.py.
    """
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg_code)
    return srs.IsGeographic() == 1


def should_resample(dem_rasters: list[str], output_res: float, threshold: float = _RESAMPLE_THRESHOLD) -> bool:
    """
    Return True if the average source resolution differs from output_res by
    more than *threshold* (relative, default 10%).

    Adapted from rscontext_3dep/rscontext_3dep/dem_builder.py.
    """
    log = Logger('Resolution Check')
    resolutions = []

    for raster_path in dem_rasters:
        ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
        if ds is None:
            log.warning(f'Could not open {raster_path} - skipping resolution check')
            continue
        gt = ds.GetGeoTransform()
        ds = None
        if gt is None:
            log.warning(f'No geotransform for {raster_path} - skipping')
            continue
        resolutions.append((abs(gt[1]) + abs(gt[5])) / 2.0)

    if not resolutions:
        log.warning('No valid source resolutions found - will resample by default')
        return True

    avg = sum(resolutions) / len(resolutions)
    rel_diff = abs(avg - output_res) / avg
    log.info(f'Source avg resolution: {avg:.3f} m  →  target: {output_res} m  (relative diff: {rel_diff:.1%})')

    if rel_diff > threshold:
        log.info('Resampling required.')
        return True

    log.info('Source resolution close enough to target - resampling not required.')
    return False
