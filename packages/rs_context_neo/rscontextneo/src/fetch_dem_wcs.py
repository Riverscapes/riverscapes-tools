"""
3DEP DEM fetching via the USGS WCS (OGC Web Coverage Service) endpoint.

The USGS 3DEP Image Service WCS returns a pre-mosaiced, single GeoTIFF for
any requested bounding box.  Unlike the TNM tile-based approach in
``fetch_dem.py``, there is no tile discovery, parallel download, gap-fill
date ordering, or CRS-conflict resolution required — the service assembles
the mosaic server-side and returns one file.

Limitations
-----------
* Best suited for small-to-medium AOIs.  At 1 m resolution, a 10 km × 10 km
  area requires ~100 million pixels spread across ~100 tiles.  For very large
  AOIs at full 1 m resolution, the TNM tile source (``--dem_source tnm``) is
  more efficient because USGS pre-makes the tiles server-side.
* No tile footprints GeoPackage is produced (there is only one source file).
* The same underlying 3DEP data is served, so coverage and accuracy are
  identical to the TNM tile approach.

Author:     Matt Reimer
Date:       2026-05-29
"""

import json
import math
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from osgeo import gdal, osr
from rscommons.download_dem import verify_areas
from rsxml import Logger
from rsxml.util import safe_makedirs, safe_remove_file
from shapely.geometry import shape
from shapely.ops import unary_union

from rscontextneo.src.fetch_dem import (
    _BUFFER_DIST_DEG,
    _DEM_CREATION_OPTIONS,
    _DEM_NODATA,
    DEM_RELPATH,
    HILLSHADE_RELPATH,
    SLOPE_RELPATH,
)
from rscontextneo.src.utils.gpkg import geojson_to_gpkg

# ── WCS endpoint constants ────────────────────────────────────────────────────

# USGS 3DEP seamless mosaic — best available resolution per pixel
_WCS_URL = "https://elevation.nationalmap.gov/arcgis/services/3DEPElevation/ImageServer/WCSServer"
_WCS_COVERAGE = "DEP3Elevation"
_WCS_VERSION = "1.0.0"
_WCS_FORMAT = "GeoTIFF"

# Maximum pixels per dimension for a single WCS GetCoverage request.
# ArcGIS Image Server enforces a hard limit of 4 096 px per dimension and
# returns HTTP 400 Bad Request for anything larger.  2 000 px stays well
# clear of that limit while keeping tile count reasonable (~4 MP per tile
# at 1 m resolution ≈ 16 MB uncompressed per request).
_WCS_TILE_PIXELS = 2_000

# Number of tiles to download simultaneously.  Matches the TNM tile approach.
_WCS_TILE_WORKERS = 4

# HTTP timeout per individual tile request (seconds).  Each tile is at most
# 1 000×1 000 px so responses are small and should arrive well within 120 s.
_WCS_TIMEOUT_S = 120


# ── Public entry point ────────────────────────────────────────────────────────


def fetch_dem_from_wcs(
    bounds_geojson: str,
    output_folder: str,
    download_folder: str,
    scratch_folder: str,
    output_res: float = 1.0,
    force_download: bool = False,
    cleanup_scratch: bool = True,
    debug: bool = False,
) -> tuple[str, str, str]:
    """
    Download and assemble a 3DEP DEM for the AOI using the USGS WCS endpoint,
    then generate a hillshade and slope raster.

    The WCS endpoint issues a single HTTP request and returns one pre-mosaiced
    GeoTIFF, avoiding the tile-discovery, parallel-download, and gap-fill
    ordering steps required by the TNM tile-based approach.

    Parameters
    ----------
    bounds_geojson : str
        Path to a WGS84 GeoJSON file containing the AOI polygon.
    output_folder : str
        Root of the RS Context Neo project folder.
        DEM       → ``<output_folder>/topography/dem.tif``
        Hillshade → ``<output_folder>/topography/dem_hillshade.tif``
        Slope     → ``<output_folder>/topography/slope.tif``
    download_folder : str
        Folder where the raw WCS GeoTIFF will be cached.  The file is named
        ``wcs_raw_dem.tif`` and is retained between runs so subsequent calls
        with ``force_download=False`` skip the network download.
    scratch_folder : str
        Temporary working folder for intermediate files (bounds GeoPackage).
    output_res : float
        Target horizontal resolution in metres (1–10). Defaults to 1.0.
    force_download : bool
        If True, re-download and re-process even when outputs already exist.
    cleanup_scratch : bool
        If True (default), delete the raw WCS GeoTIFF from *download_folder*
        after the final DEM has been built.  Set to False (or pass debug=True)
        to keep it for inspection.
    debug : bool
        Retain all intermediate files and emit more verbose logging.

    Returns
    -------
    tuple[str, str, str]
        ``(dem_path, hillshade_path, slope_path)`` — absolute paths to the
        assembled DEM, hillshade, and slope raster.

    Raises
    ------
    FileNotFoundError
        If *bounds_geojson* does not exist.
    ValueError
        If the WCS endpoint returns a non-raster (XML error) payload.
    requests.HTTPError
        On non-2xx HTTP status from the WCS endpoint.
    """
    log = Logger("Fetch DEM (WCS)")

    if not os.path.exists(bounds_geojson):
        raise FileNotFoundError(f"Bounds GeoJSON not found: {bounds_geojson}")

    dem_path = os.path.join(output_folder, DEM_RELPATH)
    hillshade_path = os.path.join(output_folder, HILLSHADE_RELPATH)
    slope_path = os.path.join(output_folder, SLOPE_RELPATH)

    # Early exit if all outputs already exist and no forced rebuild was requested
    if (
        not force_download
        and os.path.isfile(dem_path)
        and os.path.isfile(hillshade_path)
        and os.path.isfile(slope_path)
    ):
        log.info(
            f"DEM already exists at {dem_path} - skipping WCS download "
            "(pass force_download=True to override)"
        )
        return dem_path, hillshade_path, slope_path

    # Convert bounds GeoJSON → GeoPackage for rscommons verify_areas, which
    # uses get_shp_or_gpkg internally and doesn't accept raw GeoJSON paths.
    safe_makedirs(scratch_folder)
    bounds_gpkg = os.path.join(scratch_folder, "bounds.gpkg")
    geojson_to_gpkg(bounds_geojson, bounds_gpkg)
    bounds_gpkg_layer = bounds_gpkg + "/bounds"

    # ── 1. Load AOI polygon and compute buffered bounding box ─────────────────
    polygon = _load_bounds_polygon(bounds_geojson)
    buffered = polygon.buffer(_BUFFER_DIST_DEG)
    west, south, east, north = buffered.bounds

    # ── 2. Determine target UTM EPSG from AOI centroid ─────────────────────────
    center_lon = (west + east) / 2.0
    center_lat = (south + north) / 2.0
    output_epsg = _utm_epsg_from_latlon(center_lat, center_lon)
    log.info(
        f"Target CRS: EPSG:{output_epsg} (UTM zone derived from AOI centroid {center_lat:.4f}°N {center_lon:.4f}°)"
    )

    # ── 3. Download raw WCS GeoTIFF tiles in the target UTM CRS ──────────────
    # Requesting tiles in the output UTM projection means tiles are already in
    # the final CRS with consistent metre-based pixel sizes.  No reprojection
    # step is needed afterward, eliminating the screen door artefacts that
    # arise when geographic (EPSG:4326) tiles are stitched and then warped.
    safe_makedirs(download_folder)
    wcs_raw_path = os.path.join(download_folder, "wcs_raw_dem.tif")
    _wcs_get_coverage(
        bbox=(west, south, east, north),
        output_res_m=output_res,
        out_path=wcs_raw_path,
        tiles_folder=os.path.join(scratch_folder, "wcs_tiles"),
        force=force_download,
        log=log,
        request_epsg=output_epsg,
        debug=debug,
    )

    # ── 4. Clip to AOI bounds (no reprojection — raster is already in UTM) ───
    need_rebuild = force_download or not os.path.isfile(dem_path)
    if need_rebuild:
        log.info(f"Clipping WCS raster to AOI bounds (EPSG:{output_epsg}, no reprojection) ...")
        safe_makedirs(os.path.dirname(dem_path))
        if os.path.isfile(dem_path):
            safe_remove_file(dem_path)

        # gdal.Warp handles a WGS84 cutline against a UTM raster automatically.
        # Omitting dstSRS preserves the source CRS so no reprojection occurs.
        clip_opts = gdal.WarpOptions(
            xRes=output_res,
            yRes=output_res,
            resampleAlg=gdal.GRA_Bilinear,
            cutlineDSName=bounds_geojson,
            cropToCutline=True,
            dstNodata=_DEM_NODATA,
            creationOptions=_DEM_CREATION_OPTIONS,
            format="GTiff",
        )
        ds = gdal.Warp(dem_path, wcs_raw_path, options=clip_opts)
        if ds is None:
            raise RuntimeError(
                f"gdal.Warp clip failed for DEM: {gdal.GetLastErrorMsg()}"
            )
        ds = None  # flush GDAL reference

        if (cleanup_scratch or not debug) and os.path.isfile(wcs_raw_path):
            log.info(f"Removing raw WCS file: {wcs_raw_path}")
            safe_remove_file(wcs_raw_path)
    else:
        log.info(
            "DEM already exists - skipping clip (pass force_download=True to override)"
        )

    # ── 5. Verify coverage ────────────────────────────────────────────────────
    area_ratio = verify_areas(dem_path, bounds_gpkg_layer)
    if area_ratio < 0.85:
        log.warning(
            f"DEM covers only {area_ratio:.1%} of the AOI bounds (threshold: 85%). "
            "3DEP 1 m data may not be available for this region; "
            "consider using the 10 m product or switching to --dem_source tnm."
        )

    # ── 6. Hillshade ──────────────────────────────────────────────────────────
    need_hs = need_rebuild or not os.path.isfile(hillshade_path)
    if need_hs:
        log.info("Generating hillshade ...")
        # Output is always a UTM projected CRS — use standard DEMProcessing.
        gdal.DEMProcessing(
            hillshade_path,
            dem_path,
            "hillshade",
            creationOptions=["COMPRESS=DEFLATE"],
        )
    else:
        log.info("Hillshade already exists - skipping")

    # ── 7. Slope ──────────────────────────────────────────────────────────────
    # Uses Horn's method (maximum 8-neighbour gradient) in degrees — the same
    # approach as rs_context and expected by downstream tools like BRAT and RME.
    # No z-factor is needed because the output CRS is a projected UTM (metres
    # horizontal, metres vertical → scale = 1.0 is correct).
    need_slope = need_rebuild or not os.path.isfile(slope_path)
    if need_slope:
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
            result = None  # flush GDAL reference
    else:
        log.info("Slope already exists - skipping")

    log.info(
        f"DEM:        {dem_path}  ({os.path.getsize(dem_path) / 1_048_576:.1f} MB, compressed)"
    )
    log.info(f"Hillshade:  {hillshade_path}")
    log.info(f"Slope:      {slope_path}")
    log.info(
        f"Resolution: {output_res} m  |  EPSG: {output_epsg}  |  Coverage: {area_ratio:.1%}"
    )

    return dem_path, hillshade_path, slope_path


# ── Internal helpers ──────────────────────────────────────────────────────────


def _bbox_wgs84_to_epsg(
    bbox: tuple[float, float, float, float],
    epsg: int,
) -> tuple[float, float, float, float]:
    """
    Transform a WGS84 bounding box to the given EPSG and return the axis-
    aligned envelope in that CRS.

    All four corners are transformed so the returned envelope is tight even
    for projections (such as UTM) where meridians curve relative to WGS84.

    Parameters
    ----------
    bbox : (west, south, east, north)
        Input bounding box in WGS84 decimal degrees.
    epsg : int
        Target EPSG code.

    Returns
    -------
    (x_min, y_min, x_max, y_max)
        Axis-aligned bounding box in the target CRS.
    """
    west, south, east, north = bbox

    src_srs = osr.SpatialReference()
    src_srs.ImportFromEPSG(4326)
    src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromEPSG(epsg)
    dst_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    transform = osr.CoordinateTransformation(src_srs, dst_srs)

    corners = [
        transform.TransformPoint(x, y)
        for x, y in [(west, south), (east, south), (east, north), (west, north)]
    ]
    xs = [c[0] for c in corners]
    ys = [c[1] for c in corners]
    return min(xs), min(ys), max(xs), max(ys)


def _load_bounds_polygon(bounds_geojson: str):
    """
    Return a Shapely geometry representing the union of all features in the
    GeoJSON file.  Handles FeatureCollection, Feature, and bare geometry objects.
    """
    with open(bounds_geojson, encoding="utf-8") as f:
        data = json.load(f)

    geoj_type = data.get("type", "")
    if geoj_type == "FeatureCollection":
        geoms = [
            shape(feat["geometry"])
            for feat in data.get("features", [])
            if feat.get("geometry")
        ]
    elif geoj_type == "Feature":
        geoms = [shape(data["geometry"])] if data.get("geometry") else []
    else:
        geoms = [shape(data)]

    if not geoms:
        raise ValueError(f"No geometries found in bounds GeoJSON: {bounds_geojson}")
    return unary_union(geoms)


def _utm_epsg_from_latlon(lat: float, lon: float) -> int:
    """
    Return the WGS84 UTM zone EPSG code for a given latitude / longitude.

    Northern hemisphere zones → 32600 + zone_number (e.g. UTM 11N = 32611)
    Southern hemisphere zones → 32700 + zone_number (e.g. UTM 20S = 32720)
    """
    zone = int((lon + 180) / 6) + 1
    return 32600 + zone if lat >= 0 else 32700 + zone


def _wcs_get_coverage(
    bbox: tuple[float, float, float, float],
    output_res_m: float,
    out_path: str,
    tiles_folder: str,
    force: bool,
    log: Logger,
    request_epsg: int = 4326,
    debug: bool = False,
) -> None:
    """
    Download a WCS coverage for *bbox* and save it to *out_path*.

    Large coverages are automatically split into a grid of tiles that are
    downloaded in parallel (up to ``_WCS_TILE_WORKERS`` concurrent requests)
    and then mosaiced into a single GeoTIFF.  This avoids the 504 Gateway
    Timeout the USGS ArcGIS Server returns when asked for more than
    ~3 000–4 000 px per dimension in a single request.

    Parameters
    ----------
    bbox : (west, south, east, north)
        Bounding box in WGS84 decimal degrees.
    output_res_m : float
        Target pixel size in metres.
    out_path : str
        Destination file path for the assembled GeoTIFF.
    force : bool
        Re-download even if *out_path* already exists.
    log : Logger
        Caller-supplied logger.
    request_epsg : int
        EPSG code to use for the WCS tile requests and the output raster.
        Defaults to 4326 (WGS84).  Passing a UTM zone EPSG (e.g. 32611)
        requests tiles in that projected CRS so all tiles share the same
        metre-based pixel grid — this eliminates the geographic mismatch
        that causes screen door artefacts when tiles are stitched.
    debug : bool
        If True, keep the individual tile files after mosaicing so they can
        be inspected.  If False (default), the tiles folder is deleted once
        the assembled GeoTIFF has been written successfully.
    """
    if not force and os.path.isfile(out_path):
        log.info(f"WCS raw file already cached at {out_path} - skipping download")
        return

    west, south, east, north = bbox

    # ── Derive the tiling grid in the request CRS ─────────────────────────────
    # When a projected UTM EPSG is requested, transform the WGS84 bbox into
    # that CRS first.  Tiling in metres gives perfectly abutting tiles with a
    # consistent pixel size — no degree-to-metre approximation, no seams.
    if request_epsg != 4326:
        req_west, req_south, req_east, req_north = _bbox_wgs84_to_epsg(
            bbox, request_epsg
        )
        total_w = max(1, round((req_east - req_west) / output_res_m))
        total_h = max(1, round((req_north - req_south) / output_res_m))
    else:
        req_west, req_south, req_east, req_north = west, south, east, north
        center_lat = (south + north) / 2.0
        m_per_deg_lat = 111_320.0
        m_per_deg_lon = 111_320.0 * math.cos(math.radians(center_lat))
        total_w = max(1, int(round((east - west) * m_per_deg_lon / output_res_m)))
        total_h = max(1, int(round((north - south) * m_per_deg_lat / output_res_m)))

    n_cols = math.ceil(total_w / _WCS_TILE_PIXELS)
    n_rows = math.ceil(total_h / _WCS_TILE_PIXELS)
    n_tiles = n_cols * n_rows

    log.info(
        f"WCS coverage: {total_w}×{total_h} px total → "
        f"{n_cols}×{n_rows} = {n_tiles} tile(s) of ≤{_WCS_TILE_PIXELS} px/dim"
    )
    log.info(f"  Coverage:  {_WCS_COVERAGE}")
    log.info(f"  Endpoint:  {_WCS_URL}")
    log.info(f"  Request CRS: EPSG:{request_epsg}")

    # Extent of one tile column / row in the request CRS units
    step_x = (req_east - req_west) / n_cols
    step_y = (req_north - req_south) / n_rows

    # ── Tile-cache validation ─────────────────────────────────────────────────
    # Tile files are named tile_rNNN_cNNN.tif (row/col only), so tiles from a
    # previous run with a different AOI would be silently reused if we only
    # checked os.path.isfile().  A small manifest records the bbox, resolution,
    # and grid dimensions for the current download.  If it matches we can safely
    # resume an interrupted download; if it differs (or is absent) we wipe the
    # folder first so no stale tiles bleed through.
    manifest_path = os.path.join(tiles_folder, "_manifest.json")
    current_manifest = {
        "bbox": list(bbox),
        "output_res_m": output_res_m,
        "n_cols": n_cols,
        "n_rows": n_rows,
        "request_epsg": request_epsg,
    }
    if os.path.isdir(tiles_folder):
        try:
            with open(manifest_path, encoding="utf-8") as _mf:
                cached_manifest = json.load(_mf)
        except (FileNotFoundError, json.JSONDecodeError):
            cached_manifest = None

        if cached_manifest != current_manifest:
            log.info(
                "Tile cache manifest mismatch — clearing stale tiles from "
                f"{tiles_folder}"
            )
            shutil.rmtree(tiles_folder, ignore_errors=True)

    safe_makedirs(tiles_folder)
    with open(manifest_path, "w", encoding="utf-8") as _mf:
        json.dump(current_manifest, _mf)

    tile_specs: list[tuple[tuple, int, int, str]] = []
    for row in range(n_rows):
        for col in range(n_cols):
            t_west = req_west + col * step_x
            t_east = req_west + (col + 1) * step_x
            t_south = req_south + row * step_y
            t_north = req_south + (row + 1) * step_y

            # Pixel counts: proportional share of the total, clamped to tile max.
            # In projected CRS units are metres so the calculation is exact;
            # in EPSG:4326 we fall back to the degree-to-metre approximation.
            if request_epsg != 4326:
                t_w = max(1, min(_WCS_TILE_PIXELS, round((t_east - t_west) / output_res_m)))
                t_h = max(1, min(_WCS_TILE_PIXELS, round((t_north - t_south) / output_res_m)))
            else:
                t_w = max(1, min(_WCS_TILE_PIXELS, round((t_east - t_west) * m_per_deg_lon / output_res_m)))
                t_h = max(1, min(_WCS_TILE_PIXELS, round((t_north - t_south) * m_per_deg_lat / output_res_m)))

            t_path = os.path.join(tiles_folder, f"tile_r{row:03d}_c{col:03d}.tif")
            tile_specs.append(((t_west, t_south, t_east, t_north), t_w, t_h, t_path))

    # ── Parallel download ─────────────────────────────────────────────────────
    # Mirrors the pattern used in _download_tiles_parallel (fetch_dem.py):
    # preserve input order through the thread pool so the mosaic is
    # deterministic regardless of which tile finishes first.
    tile_index: dict[str, int] = {spec[3]: i for i, spec in enumerate(tile_specs)}
    tiles_keyed: list[tuple[int, str]] = []
    errors: list[str] = []

    effective_workers = min(_WCS_TILE_WORKERS, n_tiles)
    log.info(f"Downloading {n_tiles} tile(s) with {effective_workers} worker(s) ...")

    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        future_to_path = {
            executor.submit(
                _wcs_request_tile, t_bbox, t_w, t_h, t_path, force, log, request_epsg
            ): t_path
            for t_bbox, t_w, t_h, t_path in tile_specs
        }
        for future in as_completed(future_to_path):
            t_path = future_to_path[future]
            try:
                future.result()
                tiles_keyed.append((tile_index[t_path], t_path))
                log.info(f"  ✓  {os.path.basename(t_path)}")
            except Exception as exc:
                log.error(f"  ✗  {os.path.basename(t_path)}: {exc}")
                errors.append(t_path)

    if errors:
        raise Exception(
            f"{len(errors)} of {n_tiles} WCS tile(s) failed:\n"
            + "\n".join(f"  {p}" for p in errors)
        )

    # Restore row-major order for a deterministic VRT
    tiles_keyed.sort(key=lambda pair: pair[0])
    ordered_tile_paths = [p for _, p in tiles_keyed]

    # ── Mosaic tiles ──────────────────────────────────────────────────────────
    if n_tiles == 1:
        # Single tile: just rename/move rather than building an unnecessary VRT
        shutil.move(ordered_tile_paths[0], out_path)
        log.info(f"Single tile saved: {out_path}")
    else:
        log.info(f"Mosaicing {n_tiles} tile(s) → {out_path}")
        safe_makedirs(os.path.dirname(out_path))
        vrt_path = out_path.replace(".tif", "_tiles.vrt")
        gdal.BuildVRT(vrt_path, ordered_tile_paths)
        gdal.Translate(
            out_path,
            vrt_path,
            creationOptions=["COMPRESS=DEFLATE", "TILED=YES", "BIGTIFF=IF_SAFER"],
        )
        safe_remove_file(vrt_path)
        log.info(
            f"WCS mosaic complete: {out_path}  ({os.path.getsize(out_path) / 1_048_576:.1f} MB)"
        )

    # Clean up tile cache unless the caller asked to keep intermediates.
    if not debug and os.path.isdir(tiles_folder):
        log.info(f"Removing WCS tile cache: {tiles_folder}")
        shutil.rmtree(tiles_folder, ignore_errors=True)


def _wcs_request_tile(
    bbox: tuple[float, float, float, float],
    width_px: int,
    height_px: int,
    out_path: str,
    force: bool,
    log: Logger,
    request_epsg: int = 4326,
) -> None:
    """
    Issue a single WCS 1.0.0 GetCoverage request for one tile and write the
    response GeoTIFF to *out_path*.

    Parameters
    ----------
    bbox : (west, south, east, north)
        Tile bounding box in the *request_epsg* CRS.
    width_px, height_px : int
        Pixel dimensions for this tile (each ≤ ``_WCS_TILE_PIXELS``).
    out_path : str
        Destination file path.
    force : bool
        Re-download even if *out_path* already exists.
    log : Logger
        Caller-supplied logger.
    request_epsg : int
        EPSG code for the WCS ``CRS`` and ``BBOX`` parameters.  Defaults to
        4326 (WGS84).  Supply a UTM EPSG to receive tiles already projected
        into the target CRS.

    Raises
    ------
    ValueError
        If the WCS endpoint returns an XML / plain-text error payload.
    requests.HTTPError
        On non-2xx HTTP status.
    """
    if not force and os.path.isfile(out_path):
        return  # already cached — caller will log the ✓

    x_min, y_min, x_max, y_max = bbox
    params = {
        "SERVICE": "WCS",
        "VERSION": _WCS_VERSION,
        "REQUEST": "GetCoverage",
        "COVERAGE": _WCS_COVERAGE,
        "CRS": f"EPSG:{request_epsg}",
        "BBOX": f"{x_min},{y_min},{x_max},{y_max}",
        "WIDTH": str(width_px),
        "HEIGHT": str(height_px),
        "FORMAT": _WCS_FORMAT,
    }

    response = requests.get(
        _WCS_URL, params=params, stream=True, timeout=_WCS_TIMEOUT_S
    )
    response.raise_for_status()

    # WCS errors come back as XML with a 200 status — detect by Content-Type
    content_type = response.headers.get("Content-Type", "")
    if "xml" in content_type.lower() or "text" in content_type.lower():
        body = response.content[:1_000].decode("utf-8", errors="replace")
        raise ValueError(
            f"WCS endpoint returned a non-raster payload "
            f"(Content-Type: {content_type!r}):\n{body}"
        )

    safe_makedirs(os.path.dirname(out_path))
    with open(out_path, "wb") as fh:
        for chunk in response.iter_content(chunk_size=65_536):
            fh.write(chunk)
