# RS Context Neo — Configuration Profiles

This directory contains:

| File | Purpose |
|---|---|
| `config.schema.json` | JSON Schema (draft-07) — the contract every profile must satisfy |
| `us_conus.json` | Ready-to-use profile for the contiguous United States |
| `global_wcs.json` | Template for non-US regions where elevation data is served via OGC WCS |

---

## How it works

A **profile** is a JSON file that tells the tool *what layers to produce* and
*where the data comes from*. Everything region-specific lives here — including
the AOI path or DEM file path, DEM source, hydrology parameters, and optional
layers. The command-line stays thin:

```bash
rs_context_neo \
  --config config/us_conus.json \
  --output /results/my_run
```

Per-run flags (`--output`, `--force`, `--debug`, `--download-dir`,
`--scratch-dir`) tell the tool *where* to write results and whether to
override cached downloads. The profile tells it *how* and *what* to acquire.

---

## Creating a new profile

1. Copy the closest existing profile.
2. Change `profile_name` and `description`.
3. Set `dem.source` to `"wcs"` and fill in `wcs_options.url` + `wcs_options.coverage`
   if your region has a national/regional elevation WCS; leave `"tnm"` for US work.
4. Adjust `hydrology.threshold` and `hydrology.breach_dist` for your DEM resolution
   (see the scaling guidance in `schema.json`'s property descriptions).
5. Add, remove, or replace entries in `layers` to match available regional datasets.
6. Run the validator to check your file before a real run:

```bash
python -c "
from rscontextneo.src.config import load_config
cfg = load_config('config/my_region.json', validate=True)
print('Profile OK:', cfg.profile_name)
"
```

---

## Layer types

| `type` | What it does | Required fields |
|---|---|---|
| `s3tables` | Fetches a vector layer from AWS S3 Tables via Athena | `layer_name`, `s3tables_path`, `athena_output` |
| `cog_clip` | Clips a Cloud-Optimized GeoTIFF to the AOI (Landfire, NLCD, precipitation, …) | `url` |
| `wfs` | Downloads features from any OGC WFS endpoint | `url`, `typename`, `layer_name` |
| `wcs_raster` | Downloads a raster coverage from any OGC WCS endpoint | `url`, `coverage`, `layer_name` |

### S3 Tables prerequisites

The `s3tables` layer type requires AWS credentials and an Athena workgroup to be configured:

1. **AWS credentials:** Ensure your environment has valid IAM credentials with permissions to:
   - Read from the S3 Tables catalog
   - Write query results to the Athena output bucket

2. **Athena workgroup:** Create a workgroup in AWS Athena and configure it with:
   - A dedicated output S3 bucket (must end with `/`)
   - Appropriate IAM permissions for your use case

3. **S3 Tables catalog:** Ensure the tables you want to query are registered in an S3 Tables catalog.

Example setup for `us_conus.json`:

```bash
# Set up environment variables
export ATHENA_OUTPUT="s3://my-bucket/athena-results/"
export ROADS_S3TABLES_PATH="s3tablescatalog/riverscapes-data/demo/transportation_roads"
export RAIL_S3TABLES_PATH="s3tablescatalog/riverscapes-data/demo/transportation_rail"
```

See the [AWS Athena documentation](https://docs.aws.amazon.com/athena/latest/ug/workgroups.html) for workgroup setup and the [S3 Tables documentation](https://docs.aws.amazon.com/s3tables/latest/dg/welcome.html) for catalog management.

---

## Environment variables

String values in profiles support `{env:VAR}` substitution. The loader
resolves these from the environment (or from `rscontextneo/.env`) before
validating the document. This keeps secrets and machine-specific paths out
of the committed profile files.

### Variables used by `us_conus.json`

| Variable | Purpose | Example |
|---|---|---|
| `DOWNLOAD_DIR` | Persistent tile cache | `/data/dem_cache` |
| `SCRATCH_DIR` | Temporary working folder | `/tmp/dem_scratch` |
| `ATHENA_OUTPUT` | S3 URI for Athena results (trailing slash required) | `s3://my-bucket/athena-results/` |
| `ROADS_S3TABLES_PATH` | S3 Tables path for the roads table | `s3tablescatalog/my-ns/demo/roads` |
| `RAIL_S3TABLES_PATH` | S3 Tables path for the rail table | `s3tablescatalog/my-ns/demo/rail` |

### Variables used by `global_wcs.json`

| Variable | Purpose | Example |
|---|---|---|
| `DEM_DOWNLOAD_DIR` | Persistent tile cache | `/data/dem_cache` |
| `DEM_WCS_URL` | WCS endpoint base URL | `https://elevation.example.gov/wcs` |
| `DEM_WCS_COVERAGE` | WCS coverage identifier | `NationalDEM_10m` |

Add these to `rscontextneo/.env` (git-ignored) for local development.

---

## Hydrology parameter scaling

The default values are calibrated for a **1 m resolution** DEM:

| Parameter | Default (1 m) | Guidance |
|---|---|---|
| `threshold` | 50 000 cells | ≈ 0.05 km² contributing area. Scale by `1 / resolution²`: at 10 m use **500**, at 30 m use **56**. |
| `breach_dist` | 100 cells | ≈ 100 m at 1 m. Scale by `1 / resolution`: at 10 m use **10**, at 30 m use **3–5**. |

---

## Schema validation

The schema is validated by `jsonschema` (≥ 4.0). Install it if needed:

```bash
pip install jsonschema
```

VS Code and other editors that support JSON Schema will validate profile files
automatically once the `"$schema"` property points to `schema.json`.
