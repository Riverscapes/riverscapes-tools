# RS Context Neo

RS Context Neo is a Riverscapes Context tool that builds a complete topographic and hydrological context project for a single watershed. Given either a GeoJSON area-of-interest (AOI) or a pre-downloaded DEM, the tool fetches 1-metre 3DEP elevation data, derives hillshade and slope rasters, and runs a full D8 hydrology processing chain (depression breaching, pit removal, flow directions, flow accumulation, stream network extraction, and subwatershed delineation). All outputs are packaged as a Riverscapes project (`project.rs.xml`) for direct use in the Riverscapes Viewer and downstream tools such as BRAT and RME.

The two-step hydrological conditioning strategy — WhiteboxTools `BreachDepressionsLeastCost` followed by TauDEM `pitremove` — produces a hydrologically enforced DEM that correctly routes flow through road culverts, under bridges, and across other anthropogenic barriers while preserving natural terrain morphology. See [`docs/HYDROLOGY_PIPELINE.md`](docs/HYDROLOGY_PIPELINE.md) for a full explanation.

---

## Inputs and Outputs

### Inputs

| Input | Flag | Description |
|-------|------|-------------|
| Area of interest | `--aoi` | GeoJSON polygon defining the watershed boundary (WGS84). Mutually exclusive with `--dem`. |
| Pre-downloaded DEM | `--dem` | Path to an existing GeoTIFF DEM raster. Mutually exclusive with `--aoi`. |
| Output folder | `--output` / `-o` | Root directory for all project outputs. Created if it does not exist. |
| Download cache | `--download_dir` | Persistent cache for raw 3DEP tile ZIPs. Required when using `--aoi`. |
| Scratch folder | `--scratch_dir` | Temporary folder for unzipped tiles. Defaults to `<download_dir>/scratch`. |

### Outputs

All paths are relative to `--output`.

| Path | Format | Description |
|------|--------|-------------|
| `topography/dem.tif` | GeoTIFF | 1-metre 3DEP DEM, clipped to AOI |
| `topography/dem_hillshade.tif` | GeoTIFF | Shaded-relief hillshade |
| `topography/slope.tif` | GeoTIFF | Slope in degrees (Horn's method, from raw DEM) |
| `topography/tile_footprints.gpkg` | GeoPackage | Polygon footprints of all downloaded 3DEP tiles |
| `hydrology/dem_breach.tif` | GeoTIFF | Breach-conditioned DEM (WBT BreachDepressionsLeastCost) |
| `hydrology/dem_filled.tif` | GeoTIFF | Pit-filled DEM (TauDEM pitremove applied to breach DEM) |
| `hydrology/d8_flow.tif` | GeoTIFF | D8 flow direction raster (1–8 encoding) |
| `hydrology/d8_contributing_area.tif` | GeoTIFF | D8 flow accumulation (upstream cell counts) |
| `hydrology/stream_raster.tif` | GeoTIFF | Binary stream mask (contributing area ≥ threshold) |
| `hydrology/stream_order.tif` | GeoTIFF | Strahler stream-order raster |
| `hydrology/subwatersheds.tif` | GeoTIFF | Subwatershed raster (one value per reach) |
| `hydrology/hydro_derivatives.gpkg` | GeoPackage | Vector stream network (`network_intersected`) and subwatershed polygons (`subwatersheds`) |
| `project.rs.xml` | XML | Riverscapes project manifest |

---

## Quick Start

### From a GeoJSON area of interest (downloads 3DEP tiles)

```bash
rs_context_neo \
  --aoi     /path/to/watershed.geojson \
  --output  /path/to/output_folder \
  --download_dir /path/to/tile_cache
```

### From a pre-downloaded DEM

```bash
rs_context_neo \
  --dem    /path/to/existing_dem.tif \
  --output /path/to/output_folder
```

### With custom parameters

```bash
rs_context_neo \
  --aoi          /path/to/watershed.geojson \
  --output       /path/to/output_folder \
  --download_dir /path/to/tile_cache \
  --threshold    100000 \
  --breach_dist  150 \
  --output_res   1.0 \
  --cores        8
```

---

## Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--threshold` | `50000` | Minimum upstream cell count for stream classification. Higher → sparser network; lower → denser. At 1 m resolution, 50 000 cells ≈ 0.05 km². See [`docs/STREAM_THRESHOLD.md`](docs/STREAM_THRESHOLD.md). |
| `--breach_dist` | `100` | Maximum search distance in cells for the WhiteboxTools least-cost breach path. At 1 m resolution this is metres. Increase for areas with major highway or rail infrastructure. |
| `--output_res` | `1.0` | Target DEM resolution in metres (1–10). Source 3DEP tiles are 1 m; values > 1 trigger bilinear resampling during mosaicing. |
| `--cores` | env / `2` | Number of MPI ranks for TauDEM steps. Reads `TAUDEM_CORES` environment variable; falls back to 2. |
| `--force` | `False` | Re-download tiles and re-run all processing steps even if outputs already exist. |

---

## How the Pipeline Works

The tool runs three high-level steps:

**Step 1 — Acquire DEM.** If `--aoi` is provided, the tool queries The National Map REST API for all 3DEP 1-metre tiles that intersect the AOI, downloads them in parallel, resolves any UTM-zone CRS conflicts, mosaics and clips them to the AOI boundary, and derives hillshade and slope. If `--dem` is provided, this step is skipped entirely. See [`docs/FETCH_DEM.md`](docs/FETCH_DEM.md) for a detailed walkthrough.

**Step 2 — D8 Hydrology.** The downloaded (or user-supplied) DEM is passed through a six-step D8 routing chain: breach conditioning (WhiteboxTools), pit removal (TauDEM), flow directions, flow accumulation, stream thresholding, and stream network / subwatershed extraction. See [`docs/HYDROLOGY_PIPELINE.md`](docs/HYDROLOGY_PIPELINE.md) for a step-by-step explanation, including the rationale for breach-before-pitremove conditioning.

**Step 3 — Project XML.** A `project.rs.xml` manifest is written that registers all outputs with the Riverscapes framework, enabling direct visualisation in Riverscapes Viewer and ingestion by downstream models.

All steps are **idempotent**: outputs that already exist are skipped unless `--force` is specified.

---

## Documentation

| Document | Contents |
|----------|----------|
| [`docs/FETCH_DEM.md`](docs/FETCH_DEM.md) | Stage-by-stage walkthrough of 3DEP tile download, CRS conflict resolution, mosaicing, and hillshade/slope derivation |
| [`docs/HYDROLOGY_PIPELINE.md`](docs/HYDROLOGY_PIPELINE.md) | Step-by-step D8 hydrology pipeline, breach-before-pitremove rationale, tunable parameters, output file reference, idempotency behaviour |
| [`docs/STREAM_THRESHOLD.md`](docs/STREAM_THRESHOLD.md) | How to choose and tune the `--threshold` parameter; post-compute filtering with `USContArea` |
| [`docs/STREAM_NETWORK_FIELDS.md`](docs/STREAM_NETWORK_FIELDS.md) | Full field reference for the `network_intersected` and `subwatersheds` layers in `hydro_derivatives.gpkg` |
