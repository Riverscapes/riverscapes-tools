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
"""
import os
import shutil
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

from osgeo import gdal, osr

from rsxml import Logger
from rsxml.util import safe_makedirs, safe_remove_file
from rscommons.download_dem import verify_areas, find_rasters
from rscommons.download import download_unzip, download_file
from rscommons.national_map import get_1m_dem_urls
from rscommons.geographic_raster import gdal_dem_geographic
from rscommons.raster_warp import raster_vrt_stitch

# Output paths (relative to the project output_folder)
DEM_RELPATH = 'topography/dem.tif'
HILLSHADE_RELPATH = 'topography/dem_hillshade.tif'

# Degrees to buffer the bounds polygon when querying The National Map
_BUFFER_DIST_DEG = 0.01

# Relative difference in resolution below which we skip resampling
_RESAMPLE_THRESHOLD = 0.1

# GDAL creation options applied to the output DEM (PREDICTOR=2 = horizontal
# differencing, ideal for continuous elevation data; shrinks files ~50-70%)
_DEM_CREATION_OPTIONS = ['COMPRESS=DEFLATE', 'PREDICTOR=2', 'TILED=YES', 'BIGTIFF=IF_SAFER']

# Number of tiles to download simultaneously.  Network I/O is the bottleneck
# so threads (not processes) are the right tool — the GIL doesn't matter here.
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
) -> tuple[str, str]:
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

    Returns:
        tuple[str, str]: (dem_path, hillshade_path) — absolute paths to the
                         assembled DEM and its hillshade.

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
    dem_path = os.path.join(output_folder, DEM_RELPATH)
    hillshade_path = os.path.join(output_folder, HILLSHADE_RELPATH)
    if not force_download and os.path.isfile(dem_path) and os.path.isfile(hillshade_path):
        log.info(f'DEM already exists at {dem_path} — skipping download (pass force_download=True to override)')
        return dem_path, hillshade_path

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
    log.info('Querying The National Map for 3DEP 1 m tiles …')
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
        log.info('Mosaicing and clipping DEM tiles …')
        safe_makedirs(os.path.dirname(dem_path))
        if os.path.exists(dem_path):
            safe_remove_file(dem_path)

        warp_options: dict = {'cutlineBlend': 1, 'creationOptions': _DEM_CREATION_OPTIONS}
        if resample:
            log.info(f'Resampling to {output_res} m (bilinear)')
            warp_options.update({
                'xRes': output_res,
                'yRes': output_res,
                'resampleAlg': 'bilinear',
            })

        # bounds_geojson works here — raster_vrt_stitch uses gdal.WarpOptions
        # (cutlineDSName) which does native OGR auto-detection, not get_shp_or_gpkg.
        raster_vrt_stitch(dem_rasters, dem_path, output_epsg, clip=bounds_geojson, warp_options=warp_options)

        # ── 3a. Delete unzipped tiles once the mosaic is built ────────────────
        # The .zip files in ned_download_folder are kept as a persistent cache;
        # the unzipped copies in ned_unzip_folder are now redundant.
        if cleanup_scratch and os.path.isdir(ned_unzip_folder):
            log.info(f'Removing unzipped tile cache: {ned_unzip_folder}')
            shutil.rmtree(ned_unzip_folder, ignore_errors=True)
    else:
        log.info('DEM already exists and no resample needed — skipping rebuild (pass force_download=True to override)')

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
        log.info('Generating hillshade …')
        if is_geographic_epsg(output_epsg):
            gdal_dem_geographic(dem_path, hillshade_path, 'hillshade')
        else:
            gdal.DEMProcessing(hillshade_path, dem_path, 'hillshade', creationOptions=['COMPRESS=DEFLATE'])
    else:
        log.info('Hillshade already exists — skipping rebuild')

    log.info(f'DEM:       {dem_path}  ({os.path.getsize(dem_path) / 1_048_576:.1f} MB, compressed)')
    log.info(f'Hillshade: {hillshade_path}')
    log.info(f'Resolution: {output_res} m  |  EPSG: {output_epsg}  |  Coverage: {area_ratio:.1%}  |  Tiles: {len(source_urls)}')

    return dem_path, hillshade_path


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
    log.info(f'Downloading {len(urls)} tile(s) with {effective_workers} worker(s) …')

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

    Resolution check uses only the *first* valid tile — all tiles in a 3DEP
    product tier share the same pixel size, so averaging across all of them
    produces identical results at unnecessary cost.
    """
    log = Logger('Inspect Tiles')
    epsg_codes: list[int] = []
    first_res: float | None = None

    for raster_path in dem_rasters:
        ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
        if ds is None:
            log.warning(f'Could not open {raster_path} — skipping')
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

        # Resolution — only need one tile
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
        log.info('Resampling required.' if needs_resample else 'Source resolution close enough — no resample needed.')
    else:
        log.warning('No valid source resolution found — resampling by default.')

    return best_epsg, needs_resample


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
            log.warning(f'Could not open {raster_path} — skipping resolution check')
            continue
        gt = ds.GetGeoTransform()
        ds = None
        if gt is None:
            log.warning(f'No geotransform for {raster_path} — skipping')
            continue
        resolutions.append((abs(gt[1]) + abs(gt[5])) / 2.0)

    if not resolutions:
        log.warning('No valid source resolutions found — will resample by default')
        return True

    avg = sum(resolutions) / len(resolutions)
    rel_diff = abs(avg - output_res) / avg
    log.info(f'Source avg resolution: {avg:.3f} m  →  target: {output_res} m  (relative diff: {rel_diff:.1%})')

    if rel_diff > threshold:
        log.info('Resampling required.')
        return True

    log.info('Source resolution close enough to target — resampling not required.')
    return False
