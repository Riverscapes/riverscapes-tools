# RS Context Neo

RS Context Neo is a Riverscapes Context tool that builds a complete topographic and hydrological context project for a single watershed. Given either a GeoJSON area-of-interest (AOI) or a pre-downloaded DEM, the tool fetches 1-metre 3DEP elevation data, derives hillshade and slope rasters, and runs a full D8 hydrology processing chain (depression breaching, pit removal, flow directions, flow accumulation, stream network extraction, and subwatershed delineation). All outputs are packaged as a Riverscapes project (`project.rs.xml`) for direct use in the Riverscapes Viewer and downstream tools such as BRAT and RME.

The two-step hydrological conditioning strategy — WhiteboxTools `BreachDepressionsLeastCost` followed by TauDEM `pitremove` — produces a hydrologically enforced DEM that correctly routes flow through road culverts, under bridges, and across other anthropogenic barriers while preserving natural terrain morphology. See [`docs/HYDROLOGY_PIPELINE.md`](docs/HYDROLOGY_PIPELINE.md) for a full explanation.

---

## Inputs and Outputs

### Inputs

| Input | Flag | Description |
|-------|------|-------------|
| Config profile | `--config` / `-c` | Path to a regional profile JSON file (e.g. `config/us_conus.json`). Specifies the DEM source, AOI or file path, hydrology parameters, and optional data layers. |
| Output folder | `--output` / `-o` | Root directory for all project outputs. Created if it does not exist. |
| Download cache | `--download-dir` | Override the DEM download directory (takes precedence over the config file and `DOWNLOAD_DIR` env var). |
| Scratch folder | `--scratch-dir` | Override the DEM scratch directory (takes precedence over the config file and `SCRATCH_DIR` env var). Defaults to a `scratch/` subfolder of the download directory. |

The AOI (or pre-existing DEM path), DEM source, hydrology parameters, and any optional layers are all configured in the profile JSON file referenced by `--config`. See [`config/README.md`](config/README.md) for profile authoring guidance.

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
| `hydrology/hydro_derivatives.gpkg` | GeoPackage | Vector stream network (`network_intersected`, includes `level_path` field) and subwatershed polygons (`subwatersheds`) |
| `project.rs.xml` | XML | Riverscapes project manifest |

---

## Quick Start

### From a GeoJSON area of interest (downloads 3DEP tiles)

Set the `aoi` path and `download_dir` in your config profile (or in `rscontextneo/.env`), then run:

```bash
rs_context_neo \
  --config /path/to/config/us_conus.json \
  --output /path/to/output_folder
```

### From a pre-downloaded DEM

Set `dem.source` to `"file"` and `dem.filepath` in your config profile, then run:

```bash
rs_context_neo \
  --config /path/to/config/my_file_dem.json \
  --output /path/to/output_folder
```

### Overriding the download cache directory at runtime

```bash
rs_context_neo \
  --config     /path/to/config/us_conus.json \
  --output     /path/to/output_folder \
  --download-dir /path/to/tile_cache
```

### All command-line flags

```bash
rs_context_neo \
  --config     /path/to/profile.json \
  --output     /path/to/output_folder \
  --download-dir /path/to/tile_cache   \ # optional: overrides config / DOWNLOAD_DIR
  --scratch-dir  /tmp/dem_scratch      \ # optional: overrides config / SCRATCH_DIR
  --meta         "key1=val1,key2=val2" \ # optional: extra project XML metadata
  --force                              \ # re-download and re-run all steps
  --debug                              \ # keep intermediates, enable memory logging
  --verbose                              # extra console output
```

---

## Key Parameters

All science parameters live in the config profile JSON file (see [`config/README.md`](config/README.md)). The table below shows the most commonly tuned values and where to find them.

| Config key (`parameters.*`) | Default | Description |
|-----------|---------|-------------|
| `hydrology_threshold` | `50000` | Minimum upstream cell count for stream classification. Higher → sparser network; lower → denser. At 1 m resolution, 50 000 cells ≈ 0.05 km². See [`docs/STREAM_THRESHOLD.md`](docs/STREAM_THRESHOLD.md). |
| `hydrology_breach_dist` | `100` | Maximum search distance in cells for the WhiteboxTools least-cost breach path. At 1 m resolution this is metres. Increase for areas with major highway or rail infrastructure. |
| `dem.resolution` (in the `dem` layer) | `1.0` | Target DEM resolution in metres (1–10). Source 3DEP tiles are 1 m; values > 1 trigger bilinear resampling during mosaicing. |
| `taudem_cpu_cores` | env / `2` | Number of MPI ranks for TauDEM steps. Also reads the `TAUDEM_CORES` environment variable; falls back to 2. |

The `--force` command-line flag re-downloads all source data and re-runs every processing step even if outputs already exist.

---

## How the Pipeline Works

The tool runs three high-level steps:

**Step 1 — Acquire DEM.** If the config profile specifies an AOI, the tool queries The National Map REST API for all 3DEP 1-metre tiles that intersect the AOI, downloads them in parallel, resolves any UTM-zone CRS conflicts, mosaics and clips them to the AOI boundary, and derives hillshade and slope. If the profile specifies a pre-existing DEM file, this step is skipped. See [`docs/FETCH_DEM.md`](docs/FETCH_DEM.md) for a detailed walkthrough.

**Step 2 — D8 Hydrology.** The downloaded (or user-supplied) DEM is passed through a seven-step D8 routing chain: breach conditioning (WhiteboxTools), pit removal (TauDEM), flow directions, flow accumulation, stream thresholding, stream network / subwatershed extraction, and level-path assignment. See [`docs/HYDROLOGY_PIPELINE.md`](docs/HYDROLOGY_PIPELINE.md) for a step-by-step explanation, including the rationale for breach-before-pitremove conditioning.

**Step 3 — Project XML.** A `project.rs.xml` manifest is written that registers all outputs with the Riverscapes framework, enabling direct visualisation in Riverscapes Viewer and ingestion by downstream models.

All steps are **idempotent**: outputs that already exist are skipped unless `--force` is specified.

---

## Documentation

| Document | Contents |
|----------|----------|
| [`docs/FETCH_DEM.md`](docs/FETCH_DEM.md) | Stage-by-stage walkthrough of 3DEP tile download, CRS conflict resolution, mosaicing, and hillshade/slope derivation |
| [`docs/HYDROLOGY_PIPELINE.md`](docs/HYDROLOGY_PIPELINE.md) | Step-by-step D8 hydrology pipeline, breach-before-pitremove rationale, tunable parameters, output file reference, idempotency behaviour |
| [`docs/STREAM_THRESHOLD.md`](docs/STREAM_THRESHOLD.md) | How to choose and tune the `threshold` config parameter; post-compute filtering with `USContArea` |
| [`docs/STREAM_NETWORK_FIELDS.md`](docs/STREAM_NETWORK_FIELDS.md) | Full field reference for the `network_intersected` and `subwatersheds` layers in `hydro_derivatives.gpkg` |
