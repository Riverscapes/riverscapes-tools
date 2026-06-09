"""
Raster clipping for RS Context Neo.

Clips a source raster (local file or S3 COG via /vsicurl/ + presigned URL) to the project
extent (supplied as a WGS84 GeoJSON) and writes the result to the project
output folder.

Author:     Matt Reimer
Date:       2026-06-09
"""

from __future__ import annotations

import os

from osgeo import gdal, osr
from rsxml import Logger
from rsxml.util import safe_makedirs

from rscontextneo.src.config import RasterLayerConfig
from rscontextneo.src.utils.geom import load_geojson_geometry


def fetch_raster_layer(
    layer_cfg: RasterLayerConfig,
    output_folder: str,
    bounds_geojson: str,
    log: Logger,
    force: bool = False,
) -> str:
    """
    Clip the source raster described by *layer_cfg* to the project extent and
    write the result to *output_folder*.

    Parameters
    ----------
    layer_cfg : RasterLayerConfig
        Parsed layer configuration carrying the source path/URI and metadata.
    output_folder : str
        Absolute path to the project output folder.
    bounds_geojson : str
        Absolute path to the WGS84 project bounds GeoJSON.
    log : Logger
        Caller-supplied logger.
    force : bool
        If True, re-clip even when the output already exists.

    Returns
    -------
    str
        Absolute path to the written output raster.

    Raises
    ------
    FileNotFoundError
        If the source is a local path that does not exist.
    RuntimeError
        If GDAL fails to open the source or write the output.
    """
    # ── 1. Resolve output path ─────────────────────────────────────────────────
    abs_output = os.path.join(output_folder, layer_cfg.output_path)

    # ── 2. Skip if output exists and force is False ────────────────────────────
    if os.path.isfile(abs_output) and not force:
        log.info(
            f"  [raster] {layer_cfg.layer_id}: output already exists at "
            f"{os.path.basename(abs_output)} — skipping (use force=True to re-clip)"
        )
        return abs_output

    # ── 3. Ensure output directory exists ─────────────────────────────────────
    safe_makedirs(os.path.dirname(abs_output))

    # ── 4. Determine GDAL source path ─────────────────────────────────────────
    raw_input = layer_cfg.input
    if raw_input.startswith("s3://"):
        # Use /vsicurl/ + a presigned HTTPS URL instead of the deprecated /vsis3/ handler.
        # GDAL's /vsicurl/ supports HTTP range requests, which is exactly how COGs
        # deliver tiled data efficiently — no full download required.
        gdal_src = _s3_to_vsicurl(raw_input)
        source_type = "S3 (via presigned /vsicurl/)"
    elif os.path.isabs(raw_input):
        gdal_src = raw_input
        source_type = "local (absolute)"
    else:
        gdal_src = os.path.join(output_folder, raw_input)
        source_type = "local (relative)"

    log.info(f"  [raster] Clipping raster from {source_type}: {gdal_src}")
    log.info(f"  [raster] Output: {abs_output}")

    # ── 5. Guard: skip if source and destination are the same file ─────────────
    if os.path.isfile(gdal_src) and os.path.isfile(abs_output):
        try:
            if os.path.samefile(gdal_src, abs_output):
                log.info(
                    f"  [{layer_cfg.layer_id}] Source and output are the same file — "
                    f"skipping clip (already in project folder): {abs_output}"
                )
                return abs_output
        except OSError:
            pass  # samefile can fail on non-existent paths; proceed normally

    # ── 6. Configure HTTP access options for /vsicurl/ COG reads ─────────────
    # These options apply to GDAL's /vsicurl/ handler (HTTP range requests).
    # They must remain active for BOTH gdal.Open (header/IFD fetch) AND
    # gdal.Translate (lazy COG tile fetch), so we save/restore around the
    # entire operation rather than only around Open.
    _s3_config = {
        "CPL_VSIL_CURL_USE_HEAD": "NO",
        "GDAL_HTTP_TIMEOUT": "60",
        "GDAL_HTTP_MAX_RETRY": "3",
        "GDAL_HTTP_RETRY_DELAY": "5",
    }
    _s3_prev: dict[str, str | None] = {}
    if gdal_src.startswith("/vsicurl/"):
        _s3_prev = {k: gdal.GetConfigOption(k) for k in _s3_config}
        for k, v in _s3_config.items():
            gdal.SetConfigOption(k, v)

    # ── 7. Open source dataset ────────────────────────────────────────────────
    ds = None
    try:
        ds = gdal.Open(gdal_src, gdal.GA_ReadOnly)
    except RuntimeError as exc:
        if gdal_src.startswith("/vsicurl/"):
            _restore_s3_config(_s3_prev)
            raise RuntimeError(
                f"Failed to open S3 raster (via presigned URL) for '{raw_input}'. "
                f"Check AWS credentials and bucket access. "
                f"GDAL error: {gdal.GetLastErrorMsg()}"
            ) from exc
        _restore_s3_config(_s3_prev)
        raise

    if ds is None:
        gdal_msg = gdal.GetLastErrorMsg()
        _restore_s3_config(_s3_prev)
        if gdal_src.startswith("/vsicurl/"):
            raise RuntimeError(
                f"GDAL could not open S3 raster (via presigned URL) for '{raw_input}'. "
                f"Check AWS credentials and bucket access. "
                f"GDAL error: {gdal_msg}"
            )
        raise FileNotFoundError(
            f"GDAL could not open local raster '{gdal_src}'. "
            f"GDAL error: {gdal_msg}"
        )

    # ── 8. Get raster CRS ─────────────────────────────────────────────────────
    src_srs = osr.SpatialReference()
    src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    if not ds.GetProjection():
        ds = None
        _restore_s3_config(_s3_prev)
        raise RuntimeError(
            f"Source raster '{gdal_src}' has no embedded CRS — cannot reproject bounds for clipping."
        )
    err = src_srs.ImportFromWkt(ds.GetProjection())
    if err != 0:  # OGRERR_NONE == 0
        bad_wkt = ds.GetProjection()
        ds = None
        _restore_s3_config(_s3_prev)
        raise RuntimeError(
            f"Source raster '{gdal_src}' has an unreadable CRS. "
            f"WKT: {bad_wkt!r}"
        )

    # ── 9. Load project bounds and reproject bbox to raster CRS ───────────────
    project_geom = load_geojson_geometry(bounds_geojson)
    minx, miny, maxx, maxy = project_geom.bounds  # WGS84 (lng, lat)

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    wgs84_srs.ImportFromEPSG(4326)

    transform = osr.CoordinateTransformation(wgs84_srs, src_srs)

    # Transform the four corners of the bounding box
    corners = [
        transform.TransformPoint(minx, miny),
        transform.TransformPoint(minx, maxy),
        transform.TransformPoint(maxx, miny),
        transform.TransformPoint(maxx, maxy),
    ]
    xs = [c[0] for c in corners]
    ys = [c[1] for c in corners]

    ulx = min(xs)
    lrx = max(xs)
    lry = min(ys)
    uly = max(ys)

    # ── 10. Clip with gdal.Translate ──────────────────────────────────────────
    # S3 HTTP options (_s3_prev) remain active through this block — COG pixel
    # data is fetched lazily during Translate, not during Open.
    translate_succeeded = False
    result = None
    try:
        # projWin is already expressed in the raster's native CRS (transformed in
        # step 9), so we deliberately omit projWinSRS and outputSRS to prevent
        # GDAL from silently reprojecting the output.
        translate_options = gdal.TranslateOptions(
            projWin=[ulx, uly, lrx, lry],
            bandList=[layer_cfg.band],
            noData=layer_cfg.nodata,
            creationOptions=["COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", "BIGTIFF=IF_SAFER"],
            format="GTiff",
        )
        result = gdal.Translate(abs_output, ds, options=translate_options)

        # ── 11. Check result ───────────────────────────────────────────────────
        if result is None:
            raise RuntimeError(
                f"gdal.Translate returned None for '{abs_output}'. "
                f"GDAL error: {gdal.GetLastErrorMsg()}"
            )
        translate_succeeded = True
    finally:
        # ── 12. Flush / close ──────────────────────────────────────────────────
        result = None  # dereference to flush GDAL write buffer
        ds = None      # dereference to close source dataset
        # Remove partial output if translate failed
        if not translate_succeeded and os.path.isfile(abs_output):
            os.remove(abs_output)
        # Restore S3 GDAL config options after all GDAL operations are complete
        _restore_s3_config(_s3_prev)

    return abs_output


def _s3_to_vsicurl(s3_url: str, expiration: int = 3600) -> str:
    """
    Convert an ``s3://bucket/key`` URL into a ``/vsicurl/<presigned-https-url>``
    path that GDAL can open via its HTTP virtual filesystem.

    Using ``/vsicurl/`` instead of the legacy ``/vsis3/`` handler avoids the
    GDAL S3 deprecation warning and works with any standard AWS credentials
    already configured in the environment (env vars, ~/.aws/credentials, IAM
    role, etc.).

    Parameters
    ----------
    s3_url : str
        Full S3 URL in the form ``s3://bucket/path/to/file.tif``.
    expiration : int
        Presigned URL lifetime in seconds (default 1 hour).  The URL only
        needs to stay valid for the duration of the gdal.Open + gdal.Translate
        call, so 3600 s is more than enough.

    Returns
    -------
    str
        A ``/vsicurl/<url>`` string ready to pass to ``gdal.Open``.
    """
    if not s3_url.startswith("s3://"):
        raise ValueError(f"Expected an s3:// URL, got: {s3_url!r}")

    without_scheme = s3_url[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Could not parse bucket/key from S3 URL: {s3_url!r}")

    import boto3  # pylint: disable=import-outside-toplevel
    s3_client = boto3.client("s3")
    presigned_url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expiration,
    )
    return "/vsicurl/" + presigned_url


def _restore_s3_config(prev: dict[str, str | None]) -> None:
    """Restore GDAL config options saved before S3/HTTP access."""
    for k, v in prev.items():
        gdal.SetConfigOption(k, v)  # setting None unsets the option


def get_raster_cell_size(raster_path: str) -> tuple[float, float]:
    """
    Return the (cell_size_x, cell_size_y) of the raster at *raster_path*.

    Both values are returned as positive floats (absolute values of the
    geotransform x/y pixel sizes).

    Parameters
    ----------
    raster_path : str
        Absolute path to the GeoTIFF.

    Returns
    -------
    tuple[float, float]
        (cell_size_x, cell_size_y) in the raster's native units.

    Raises
    ------
    RuntimeError
        If GDAL cannot open the raster.
    """
    ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
    if ds is None:
        raise RuntimeError(
            f"Could not open raster to read cell size: '{raster_path}'. "
            f"GDAL error: {gdal.GetLastErrorMsg()}"
        )
    try:
        gt = ds.GetGeoTransform()
        return abs(gt[1]), abs(gt[5])
    finally:
        ds = None
