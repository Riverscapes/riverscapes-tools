# COG National Landfire Rasters on S3

This document describes how to convert the national Landfire rasters used by `rs_context` into Cloud Optimized GeoTiffs (COGs), host them on S3, and then efficiently extract only the bounding-box region needed for a given area of interest at runtime — replacing the need to ship a full national raster as a local input.

---

## Background: Current Workflow

`rs_context` currently accepts a `landfire_dir` folder containing ten national rasters:

| File | Layer |
|---|---|
| `LC23_EVT_240.tif` | Existing Vegetation Type |
| `LC20_BPS_220.tif` | Biophysical Settings (Historic Veg) |
| `LC23_EVC_240.tif` | Existing Vegetation Cover |
| `LC23_EVH_240.tif` | Existing Vegetation Height |
| `LC23_HDst_240.tif` | Historic Disturbance |
| `LC23_FDst_240.tif` | Fuel Disturbance |
| `LC23_FCCS_240.tif` | Fuel Characteristic Classification System |
| `LC23_VCC_240.tif` | Vegetation Condition Class |
| `LC23_VDep_240.tif` | Vegetation Departure |
| `LC23_SCla_240.tif` | Succession Classes |

These are passed into `clip_vegetation()` in `vegetation.py`, which calls `raster_warp()` using a vector cutline to clip and reproject each one to the processing boundary. The entire national file must be present on disk first.

The new approach replaces local national rasters with COGs hosted on S3. GDAL's virtual filesystem (`/vsicurl/` or `/vsis3/`) fetches only the tiles that overlap the bounding box of the HUC being processed — no full download required.

---

## Part 1: Converting Landfire GeoTiffs to COGs

### What Makes a Valid COG

A COG is a standard GeoTiff with three requirements:

1. **Tiled internally** — data is stored in rectangular tiles (typically 512×512), not strips.
2. **Overviews embedded** — reduced-resolution versions are stored inside the file (like a raster pyramid).
3. **Overviews before data** — the file layout puts overviews at the front so HTTP range requests can quickly fetch low-res tiles without reading the full file.

The `COG` GDAL driver handles all of this automatically.

### Choosing Compression

All Landfire layers are **categorical/integer** rasters (Int16). Use:

- **Compression:** `DEFLATE` or `LZW` (both lossless, good for integer data)
- **Predictor:** `2` (horizontal differencing — dramatically improves compression ratios on integer rasters)
- **Resampling for overviews:** `NEAREST` (preserves categorical values exactly; never use bilinear/cubic on classified data)

### Step-by-Step Conversion

#### Step 1 — Inspect the Source Raster

Before converting, record the existing data type, nodata value, and CRS. This informs the conversion parameters.

```bash
gdalinfo LC23_EVT_240.tif
```

Key fields to note:
- `Data Type` (typically `Int16`)
- `NoData Value`
- Coordinate system (Landfire ships in **Albers Equal Area / NAD83**, EPSG:5070)

#### Step 2 — Build Internal Overviews

Add overviews to the source raster before converting. The overview levels should cover the full zoom range down to a thumbnail (2, 4, 8, 16, 32, 64, 128, 256).

```bash
gdaladdo \
  --config COMPRESS_OVERVIEW DEFLATE \
  --config PREDICTOR_OVERVIEW 2 \
  -r nearest \
  LC23_EVT_240.tif \
  2 4 8 16 32 64 128 256
```

> **Note:** `gdaladdo` modifies the file in-place by default. If you want to keep the original untouched, copy it first or use the external overview file flag (`-ro`), but embedded overviews are required for a valid COG so copy the file.

#### Step 3 — Convert to COG with `gdal_translate`

```bash
gdal_translate \
  -of COG \
  -co COMPRESS=DEFLATE \
  -co PREDICTOR=2 \
  -co BLOCKSIZE=512 \
  -co RESAMPLING=NEAREST \
  -co COPY_SRC_OVERVIEWS=YES \
  -co BIGTIFF=IF_SAFER \
  LC23_EVT_240.tif \
  LC23_EVT_240_cog.tif
```

**Option explanations:**

| Option | Purpose |
|---|---|
| `-of COG` | Use the GDAL COG driver (ensures correct file layout) |
| `COMPRESS=DEFLATE` | Lossless compression suitable for integer rasters |
| `PREDICTOR=2` | Horizontal differencing predictor — greatly improves compression ratios for integer data |
| `BLOCKSIZE=512` | Internal tile size in pixels. 512×512 is a good balance between HTTP request overhead and tile size |
| `RESAMPLING=NEAREST` | Nearest-neighbour resampling for overview generation — preserves categorical values |
| `COPY_SRC_OVERVIEWS=YES` | Copies the overviews built in Step 2 into the output |
| `BIGTIFF=IF_SAFER` | Automatically uses BigTIFF format if the file exceeds 4 GB |

#### Step 4 — Validate the COG

```bash
python3 -m cogeo_mosaic.validate LC23_EVT_240_cog.tif
```

Or using the `rio cogeo` CLI from the `rio-cogeo` package:

```bash
pip install rio-cogeo
rio cogeo validate LC23_EVT_240_cog.tif
```

Expected output: `LC23_EVT_240_cog.tif is a valid cloud optimized GeoTIFF`.

#### Step 5 — Batch Convert All Ten Rasters

```bash
#!/usr/bin/env bash
set -euo pipefail

LANDFIRE_RASTERS=(
  "LC23_EVT_240.tif"
  "LC20_BPS_220.tif"
  "LC23_EVC_240.tif"
  "LC23_EVH_240.tif"
  "LC23_HDst_240.tif"
  "LC23_FDst_240.tif"
  "LC23_FCCS_240.tif"
  "LC23_VCC_240.tif"
  "LC23_VDep_240.tif"
  "LC23_SCla_240.tif"
)

for RASTER in "${LANDFIRE_RASTERS[@]}"; do
  BASE="${RASTER%.tif}"
  COG_OUT="${BASE}_cog.tif"

  echo "Processing ${RASTER} -> ${COG_OUT}"

  # Add overviews in-place (copy first to preserve originals)
  cp "${RASTER}" "${COG_OUT}"

  gdaladdo \
    --config COMPRESS_OVERVIEW DEFLATE \
    --config PREDICTOR_OVERVIEW 2 \
    -r nearest \
    "${COG_OUT}" \
    2 4 8 16 32 64 128 256

  gdal_translate \
    -of COG \
    -co COMPRESS=DEFLATE \
    -co PREDICTOR=2 \
    -co BLOCKSIZE=512 \
    -co RESAMPLING=NEAREST \
    -co COPY_SRC_OVERVIEWS=YES \
    -co BIGTIFF=IF_SAFER \
    "${COG_OUT}" \
    "${BASE}_final_cog.tif"

  rm "${COG_OUT}"
  echo "  Done: ${BASE}_final_cog.tif"
done

echo "All rasters converted."
```

---

## Part 2: Uploading COGs to S3

### Bucket Configuration

The bucket must allow public HTTP GET access (or you must sign URLs) because GDAL's `/vsicurl/` driver uses standard HTTP range requests.

**Recommended bucket policy** for public read-only access to the landfire prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicReadLandfire",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::your-bucket-name/national/landfire/*"
    }
  ]
}
```

> If you prefer to keep the bucket private and use `s3://` paths with AWS credentials, see the `/vsis3/` section below.

### Uploading with AWS CLI

```bash
S3_BUCKET="s3://your-bucket-name/national/landfire"

COGS=(
  "LC23_EVT_240_final_cog.tif"
  "LC20_BPS_220_final_cog.tif"
  "LC23_EVC_240_final_cog.tif"
  "LC23_EVH_240_final_cog.tif"
  "LC23_HDst_240_final_cog.tif"
  "LC23_FDst_240_final_cog.tif"
  "LC23_FCCS_240_final_cog.tif"
  "LC23_VCC_240_final_cog.tif"
  "LC23_VDep_240_final_cog.tif"
  "LC23_SCla_240_final_cog.tif"
)

for COG in "${COGS[@]}"; do
  echo "Uploading ${COG}..."
  aws s3 cp "${COG}" "${S3_BUCKET}/${COG}" \
    --no-progress \
    --content-type "image/tiff"
  echo "  Uploaded."
done
```

> **Important:** Do **not** use `--acl public-read` if your bucket has Block Public Access enabled at the account level. Use a bucket policy instead (see above).

### Recommended S3 URL Format

Once uploaded, each raster is accessible at:

```
https://your-bucket-name.s3.amazonaws.com/national/landfire/LC23_EVT_240_final_cog.tif
```

Store these URLs in a config file or as constants in `rs_context.py` so they can be updated without code changes.

---

## Part 3: Extracting the AOI Bounding Box with GDAL

This replaces the current `clip_vegetation()` call that clips to a vector cutline. Instead, we:

1. Reproject the HUC bounding box into the native CRS of the Landfire raster (EPSG:5070).
2. Use `gdal.Warp()` with `outputBounds` to fetch only the tiles that intersect that bbox — GDAL handles the HTTP range requests automatically.
3. Reproject to the output EPSG as part of the same warp.

---

### ⚠️ Overview Level Selection: Getting Native Resolution, Not Pyramids

This is the most important thing to understand when reading from a COG.

**The problem:** `gdal.Warp()` has an `overviewLevel` parameter that defaults to `'AUTO'`. In `AUTO` mode, GDAL compares the target output pixel size to the pixel sizes of each embedded overview level. If the output pixel size is equal to or larger than an overview level's pixel size, GDAL will **silently read from that pyramid level instead of the full-resolution data**.

This can happen in two situations that are both relevant to `rs_context`:

- **Reprojection changes apparent pixel size.** Landfire is in EPSG:5070 (Albers, metres). If you reproject to geographic coordinates (EPSG:4326, degrees), the pixel size changes units. GDAL's AUTO heuristic compares the computed output degree-pixel size against overview levels and may incorrectly select one.
- **The output extent is large relative to the tile size.** For big HUCs (HUC4/HUC6), the output may span many native pixels and GDAL may consider a coarser overview acceptable.

**The fix:** Always pass `overviewLevel='NONE'` to `gdal.WarpOptions()`. This tells GDAL to read exclusively from the base (full-resolution) level of the COG, regardless of the output pixel size.

```python
# BAD — GDAL may silently use an overview (downsampled) level
warp_options = gdal.WarpOptions(
    dstSRS='EPSG:4326',
    outputBounds=(...),
    resampleAlg='near',
)

# GOOD — always reads from the native full-resolution data
warp_options = gdal.WarpOptions(
    dstSRS='EPSG:4326',
    outputBounds=(...),
    resampleAlg='near',
    overviewLevel='NONE',   # <-- this is the critical line
)
```

The equivalent CLI flag is `-ovr NONE`:

```bash
# Without -ovr NONE: may use a pyramid level
gdalwarp -t_srs EPSG:4326 -te ... /vsicurl/https://... output.tif

# With -ovr NONE: guaranteed full resolution
gdalwarp -ovr NONE -t_srs EPSG:4326 -te ... /vsicurl/https://... output.tif
```

**How overview levels are stored in a Landfire COG:**

Given a native resolution of 30 m and overview levels `2 4 8 16 32 64 128 256`, the embedded pyramid looks like:

| Overview level | Pixel size | Used when output px ≥ |
|---|---|---|
| 0 (native) | 30 m | — (always use with `NONE`) |
| 1 | 60 m | ~60 m |
| 2 | 120 m | ~120 m |
| 3 | 240 m | ~240 m |
| 4 | 480 m | ~480 m |
| … | … | … |

If you forget `overviewLevel='NONE'` and your output happens to be at ~120 m (possible in geographic coordinates for a large HUC), GDAL will read from overview level 2 — every pixel will represent 4×4 original cells blended together, which for a categorical raster like EVT means invented class values.

**Verifying you got native resolution:**

```python
import rasterio

def verify_native_resolution(output_path: str, expected_res_m: float = 30.0, tolerance: float = 1.0):
    """
    Confirms that a clipped raster has the expected native pixel size.
    Raises ValueError if the resolution is coarser than expected (indicating
    an overview level was used by mistake).

    Args:
        output_path:    Path to the clipped output raster.
        expected_res_m: Expected pixel size in metres (30.0 for Landfire).
        tolerance:      Allowed deviation in metres.
    """
    with rasterio.open(output_path) as ds:
        res_x = abs(ds.transform.a)  # pixel width in CRS units
        res_y = abs(ds.transform.e)  # pixel height in CRS units
        crs_units = ds.crs.linear_units if ds.crs.is_projected else 'degrees'

    # If the output CRS is projected (metres), compare directly.
    # If geographic (degrees), convert: 1 degree ≈ 111,320 m at the equator.
    if crs_units == 'degrees':
        res_x_m = res_x * 111_320
        res_y_m = res_y * 111_320
    else:
        res_x_m = res_x
        res_y_m = res_y

    if res_x_m > expected_res_m + tolerance or res_y_m > expected_res_m + tolerance:
        raise ValueError(
            f'{output_path} has pixel size ({res_x_m:.1f} m x {res_y_m:.1f} m), '
            f'which is coarser than the expected native resolution of {expected_res_m} m. '
            f'An overview level was likely used. Add overviewLevel="NONE" to gdal.WarpOptions().'
        )

    print(f'✓ {output_path}: {res_x_m:.1f} m x {res_y_m:.1f} m — native resolution confirmed.')
```

---

### GDAL Environment Setup for COG/HTTP Access

GDAL needs a few environment variables set to efficiently read COGs over HTTP. Set these once at the top of your script or in the calling environment:

```python
from osgeo import gdal

# Enable GDAL VSI cache so repeated range requests to the same tiles
# are served from memory rather than re-fetching from S3.
gdal.SetConfigOption('GDAL_DISABLE_READDIR_ON_OPEN', 'EMPTY_DIR')
gdal.SetConfigOption('CPL_VSIL_CURL_CACHE_SIZE', '200000000')   # 200 MB cache
gdal.SetConfigOption('GDAL_HTTP_MERGE_CONSECUTIVE_RANGES', 'YES')
gdal.SetConfigOption('GDAL_HTTP_MULTIPLEX', 'YES')
gdal.SetConfigOption('GDAL_HTTP_VERSION', '2')
gdal.SetConfigOption('VSI_CACHE', 'TRUE')
gdal.SetConfigOption('VSI_CACHE_SIZE', '10000000')  # 10 MB per-file cache

# If using private S3 (vsis3) instead of public HTTP, also set:
# gdal.SetConfigOption('AWS_REGION', 'us-west-2')
# Credentials are picked up from ~/.aws/credentials or environment variables.
```

### Core Snippet: Extract Bounding Box from a Remote COG

```python
import os
from osgeo import gdal, osr


def get_bbox_in_raster_crs(bbox_epsg4326: tuple, raster_path: str) -> tuple:
    """
    Reprojects a WGS84 bounding box into the native CRS of a raster.

    Args:
        bbox_epsg4326: (min_lon, min_lat, max_lon, max_lat) in WGS84
        raster_path:   Path or /vsicurl/ URL to any GDAL-readable raster

    Returns:
        (xmin, ymin, xmax, ymax) in the raster's native CRS
    """
    ds = gdal.Open(raster_path)
    raster_proj = ds.GetProjection()
    ds = None

    src_srs = osr.SpatialReference()
    src_srs.ImportFromEPSG(4326)
    src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromWkt(raster_proj)

    transform = osr.CoordinateTransformation(src_srs, dst_srs)

    min_lon, min_lat, max_lon, max_lat = bbox_epsg4326

    # Transform all four corners to handle skewed/rotated projections correctly
    corners = [
        transform.TransformPoint(min_lon, min_lat),
        transform.TransformPoint(max_lon, min_lat),
        transform.TransformPoint(max_lon, max_lat),
        transform.TransformPoint(min_lon, max_lat),
    ]

    xs = [c[0] for c in corners]
    ys = [c[1] for c in corners]

    return (min(xs), min(ys), max(xs), max(ys))


def extract_cog_bbox(
    cog_url: str,
    output_path: str,
    bbox_native: tuple,
    output_epsg: int,
    nodata: float = None,
    resampling: str = 'near',
) -> None:
    """
    Extracts a bounding-box region from a remote COG and writes a local GeoTiff.

    Only the HTTP range requests covering the bbox are issued — the full
    national raster is never downloaded.

    Args:
        cog_url:      GDAL-readable URL, e.g.
                        '/vsicurl/https://bucket.s3.amazonaws.com/path/file_cog.tif'
                        or '/vsis3/bucket/path/file_cog.tif'
        output_path:  Local path to write the clipped output raster
        bbox_native:  (xmin, ymin, xmax, ymax) in the COG's native CRS (EPSG:5070)
        output_epsg:  EPSG code for the output raster (e.g. 4326)
        nodata:       NoData value to carry through (read from source if None)
        resampling:   GDAL resampling algorithm string. Use 'near' for categorical data.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    xmin, ymin, xmax, ymax = bbox_native

    # Read source nodata if not provided
    if nodata is None:
        src_ds = gdal.Open(cog_url)
        band = src_ds.GetRasterBand(1)
        nodata = band.GetNoDataValue()
        src_ds = None

    warp_options = gdal.WarpOptions(
        format='GTiff',
        outputBounds=(xmin, ymin, xmax, ymax),   # Clips to the bbox in the src CRS
        outputBoundsSRS=f'EPSG:5070',             # Declare the bbox is in EPSG:5070
        dstSRS=f'EPSG:{output_epsg}',
        resampleAlg=resampling,
        overviewLevel='NONE',                    # Always read from native full-resolution data
        srcNodata=nodata,
        dstNodata=nodata,
        creationOptions=[
            'COMPRESS=DEFLATE',
            'PREDICTOR=2',
            'TILED=YES',
            'BLOCKXSIZE=512',
            'BLOCKYSIZE=512',
        ],
        multithread=True,
    )

    result = gdal.Warp(output_path, cog_url, options=warp_options)

    if result is None:
        raise RuntimeError(f'gdal.Warp failed for {cog_url} -> {output_path}')

    result = None  # Close dataset / flush to disk
```

### Full Replacement for `clip_vegetation()`

This is a drop-in replacement for the current `clip_vegetation()` function in `vegetation.py`. It accepts S3 URLs instead of local file paths and uses the bounding box of the AOI boundary layer to spatially filter what is fetched.

```python
import os
from osgeo import gdal, ogr, osr
from rsxml import Logger


# Central registry of COG URLs — update here when new Landfire versions are released
LANDFIRE_COG_URLS = {
    'LC23_EVT_240': '/vsicurl/https://your-bucket.s3.amazonaws.com/national/landfire/LC23_EVT_240_final_cog.tif',
    'LC20_BPS_220': '/vsicurl/https://your-bucket.s3.amazonaws.com/national/landfire/LC20_BPS_220_final_cog.tif',
    'LC23_EVC_240': '/vsicurl/https://your-bucket.s3.amazonaws.com/national/landfire/LC23_EVC_240_final_cog.tif',
    'LC23_EVH_240': '/vsicurl/https://your-bucket.s3.amazonaws.com/national/landfire/LC23_EVH_240_final_cog.tif',
    'LC23_HDst_240': '/vsicurl/https://your-bucket.s3.amazonaws.com/national/landfire/LC23_HDst_240_final_cog.tif',
    'LC23_FDst_240': '/vsicurl/https://your-bucket.s3.amazonaws.com/national/landfire/LC23_FDst_240_final_cog.tif',
    'LC23_FCCS_240': '/vsicurl/https://your-bucket.s3.amazonaws.com/national/landfire/LC23_FCCS_240_final_cog.tif',
    'LC23_VCC_240': '/vsicurl/https://your-bucket.s3.amazonaws.com/national/landfire/LC23_VCC_240_final_cog.tif',
    'LC23_VDep_240': '/vsicurl/https://your-bucket.s3.amazonaws.com/national/landfire/LC23_VDep_240_final_cog.tif',
    'LC23_SCla_240': '/vsicurl/https://your-bucket.s3.amazonaws.com/national/landfire/LC23_SCla_240_final_cog.tif',
}

# Native CRS of all Landfire rasters
LANDFIRE_EPSG = 5070


def _configure_gdal_for_cog():
    """Set GDAL config options optimised for COG HTTP access. Call once at startup."""
    gdal.SetConfigOption('GDAL_DISABLE_READDIR_ON_OPEN', 'EMPTY_DIR')
    gdal.SetConfigOption('CPL_VSIL_CURL_CACHE_SIZE', '200000000')
    gdal.SetConfigOption('GDAL_HTTP_MERGE_CONSECUTIVE_RANGES', 'YES')
    gdal.SetConfigOption('GDAL_HTTP_MULTIPLEX', 'YES')
    gdal.SetConfigOption('GDAL_HTTP_VERSION', '2')
    gdal.SetConfigOption('VSI_CACHE', 'TRUE')
    gdal.SetConfigOption('VSI_CACHE_SIZE', '10000000')


def _get_layer_bbox_in_epsg(layer_path: str, target_epsg: int) -> tuple:
    """
    Returns the (xmin, ymin, xmax, ymax) extent of a vector layer
    reprojected into the requested EPSG.

    Args:
        layer_path:   Path to a GeoPackage layer or Shapefile
                      (supports 'path/to/file.gpkg|layername=foo' syntax)
        target_epsg:  EPSG code to reproject the extent into

    Returns:
        (xmin, ymin, xmax, ymax) in target_epsg
    """
    # Handle 'file.gpkg|layername=foo' notation
    if '|' in layer_path:
        file_part, layer_part = layer_path.split('|', 1)
        layer_name = layer_part.replace('layername=', '')
    else:
        file_part = layer_path
        layer_name = None

    driver_name = 'GPKG' if file_part.endswith('.gpkg') else 'ESRI Shapefile'
    driver = ogr.GetDriverByName(driver_name)
    ds = driver.Open(file_part, 0)
    layer = ds.GetLayerByName(layer_name) if layer_name else ds.GetLayer(0)

    src_srs = layer.GetSpatialRef()
    extent = layer.GetExtent()  # (xmin, xmax, ymin, ymax) — note GDAL ordering
    ds = None

    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromEPSG(target_epsg)

    transform = osr.CoordinateTransformation(src_srs, dst_srs)

    xmin_src, xmax_src, ymin_src, ymax_src = extent

    # Transform all four corners to handle projection distortion
    corners = [
        transform.TransformPoint(xmin_src, ymin_src),
        transform.TransformPoint(xmax_src, ymin_src),
        transform.TransformPoint(xmax_src, ymax_src),
        transform.TransformPoint(xmin_src, ymax_src),
    ]

    xs = [c[0] for c in corners]
    ys = [c[1] for c in corners]

    return (min(xs), min(ys), max(xs), max(ys))


def clip_vegetation_from_cog(
    boundary_path: str,
    veg_raster_keys: list,
    veg_raster_clips: list,
    output_epsg: int,
    bbox_buffer_m: float = 1000.0,
) -> None:
    """
    Fetches and clips Landfire vegetation rasters from S3-hosted COGs using the
    bounding box of the processing boundary. Replaces the old clip_vegetation()
    that required local national rasters.

    Args:
        boundary_path:    Path to the AOI boundary vector layer used to derive the bbox.
                          Accepts any GDAL-readable vector path including
                          'file.gpkg|layername=layer_name' syntax.
        veg_raster_keys:  List of keys from LANDFIRE_COG_URLS, in the same order
                          as veg_raster_clips.
        veg_raster_clips: List of local output paths for the clipped rasters.
        output_epsg:      EPSG code for the output rasters.
        bbox_buffer_m:    Buffer in metres to add around the AOI bbox before
                          fetching — ensures edge pixels are not clipped.
    """
    log = Logger('Vegetation COG')

    if len(veg_raster_keys) != len(veg_raster_clips):
        raise ValueError('Number of raster keys does not match number of output paths')

    _configure_gdal_for_cog()

    # Derive the AOI bounding box in the Landfire native CRS (EPSG:5070)
    log.info(f'Deriving AOI bounding box from {boundary_path}')
    xmin, ymin, xmax, ymax = _get_layer_bbox_in_epsg(boundary_path, LANDFIRE_EPSG)

    # Add a buffer so we don't clip edge pixels
    xmin -= bbox_buffer_m
    ymin -= bbox_buffer_m
    xmax += bbox_buffer_m
    ymax += bbox_buffer_m

    log.info(f'Buffered AOI bbox in EPSG:{LANDFIRE_EPSG}: ({xmin:.1f}, {ymin:.1f}, {xmax:.1f}, {ymax:.1f})')

    for key, out_path in zip(veg_raster_keys, veg_raster_clips):
        if key not in LANDFIRE_COG_URLS:
            raise KeyError(f'Unknown Landfire raster key: {key}. '
                           f'Valid keys: {list(LANDFIRE_COG_URLS.keys())}')

        cog_url = LANDFIRE_COG_URLS[key]
        log.info(f'Fetching {key} from {cog_url}')
        log.info(f'  -> {out_path}')

        if os.path.isfile(out_path):
            log.info(f'  Skipping — output already exists.')
            continue

        os.makedirs(os.path.dirname(out_path), exist_ok=True)

        # Read nodata from the source
        src_ds = gdal.Open(cog_url)
        if src_ds is None:
            raise RuntimeError(f'Could not open COG: {cog_url}')
        nodata = src_ds.GetRasterBand(1).GetNoDataValue()
        src_ds = None

        warp_options = gdal.WarpOptions(
            format='GTiff',
            outputBounds=(xmin, ymin, xmax, ymax),
            outputBoundsSRS=f'EPSG:{LANDFIRE_EPSG}',
            dstSRS=f'EPSG:{output_epsg}',
            resampleAlg='near',         # Nearest neighbour — required for categorical data
            overviewLevel='NONE',       # Always read from native full-resolution data, never pyramids
            srcNodata=nodata,
            dstNodata=nodata,
            creationOptions=[
                'COMPRESS=DEFLATE',
                'PREDICTOR=2',
                'TILED=YES',
                'BLOCKXSIZE=512',
                'BLOCKYSIZE=512',
            ],
            multithread=True,
        )

        result = gdal.Warp(out_path, cog_url, options=warp_options)

        if result is None:
            raise RuntimeError(f'gdal.Warp failed for {key}')

        result = None
        log.info(f'  Complete.')

    log.info('All vegetation rasters fetched and clipped.')
```

### Calling the New Function from `rs_context.py`

Replace the existing block in `rs_context.py`:

```python
# OLD — requires local national rasters
in_veg_rasters = [
    os.path.join(landfire_dir, 'LC23_EVT_240.tif'),
    os.path.join(landfire_dir, 'LC20_BPS_220.tif'),
    # ... etc
]
out_veg_rasters = [existing_clip, historic_clip, ...]
clip_vegetation(buffered_clip_path100, in_veg_rasters, out_veg_rasters, cfg.OUTPUT_EPSG)
```

With:

```python
# NEW — streams from S3 COGs, only fetching the AOI bounding box
from rscontext.vegetation import clip_vegetation_from_cog, LANDFIRE_COG_URLS

veg_raster_keys = [
    'LC23_EVT_240',
    'LC20_BPS_220',
    'LC23_EVC_240',
    'LC23_EVH_240',
    'LC23_HDst_240',
    'LC23_FDst_240',
    'LC23_FCCS_240',
    'LC23_VCC_240',
    'LC23_VDep_240',
    'LC23_SCla_240',
]
out_veg_rasters = [
    existing_clip, historic_clip, veg_cover_clip, veg_height_clip,
    hdist_clip, fdist_clip, fccs_clip, veg_condition_clip, veg_departure_clip, sclass_clip
]

clip_vegetation_from_cog(
    boundary_path=buffered_clip_path100,
    veg_raster_keys=veg_raster_keys,
    veg_raster_clips=out_veg_rasters,
    output_epsg=cfg.OUTPUT_EPSG,
)
```

The `landfire_dir` argument can then be removed from `rs_context()` entirely, or kept as an optional fallback.

---

## Part 4: Private S3 with `/vsis3/` (Alternative to Public HTTP)

If the bucket is private, replace `/vsicurl/https://...` URLs with `/vsis3/bucket/key` paths and ensure AWS credentials are available to the GDAL process:

```python
# In LANDFIRE_COG_URLS, use:
'LC23_EVT_240': '/vsis3/your-bucket-name/national/landfire/LC23_EVT_240_final_cog.tif',

# And add to _configure_gdal_for_cog():
gdal.SetConfigOption('AWS_REGION', 'us-west-2')
# AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY picked up from environment or ~/.aws/credentials
```

The `/vsis3/` driver bypasses HTTP entirely and uses the AWS SDK for range requests, which is faster and cheaper within AWS infrastructure.

---

## Summary of Changes

| Area | Before | After |
|---|---|---|
| Input | Local `landfire_dir/` folder (~30-50 GB) | S3 URLs in code |
| Data transfer | Full national rasters must be on disk | Only AOI tiles fetched at runtime |
| Clipping method | Vector cutline (`cutlineDSName`) | Bounding box (`outputBounds`) |
| Overview safety | N/A (local file) | `overviewLevel='NONE'` prevents silent pyramid reads |
| Reprojection | Separate warp step | Same `gdal.Warp()` call |
| `rs_context.py` arg | `landfire_dir: str` | Remove or make optional |
| `clip_vegetation()` | Local file paths | COG URL keys |
