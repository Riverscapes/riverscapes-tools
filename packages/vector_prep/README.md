# Vector Prep Tool

`vector_prep` cleans and standardises vector datasets (Shapefiles, GeoPackages, etc.) for use in the [Riverscapes](https://riverscapes.net) ecosystem. Given a raw input layer it produces a clean GeoPackage in EPSG:4326 with invalid geometries fixed, duplicates removed, slivers dropped, strings normalised, and a full accounting of everything it changed or dropped.

---

## Table of Contents

1. [How it works](#how-it-works)
2. [Setup](#setup)
3. [Running the tool](#running-the-tool)
   - [Direct mode (quick one-off)](#direct-mode-quick-one-off)
   - [Config mode (repeatable / per-layer)](#config-mode-repeatable--per-layer)
   - [Interactive CLI](#interactive-cli)
4. [Config file reference](#config-file-reference)
5. [Field map and layer definitions](#field-map-and-layer-definitions)
6. [Processing pipeline — checks A–M](#processing-pipeline--checks-am)
7. [Output files](#output-files)
8. [Adding a new layer](#adding-a-new-layer)

---

## How it works

The tool runs a **two-pass chunked pipeline** so arbitrarily large datasets can be handled with bounded memory:

**Pass 1 — lightweight hash scan**  
Iterates every feature with `pyogrio` in chunks, reprojects geometries to the target CRS, and builds two sets of SHA-1 hashes: one for geometry-only duplicates and one for full-row (geometry + attributes) duplicates. No geometry objects are kept in memory between chunks.

**Pass 2 — chunked processing**  
Reads `chunk_size` features at a time, runs the full check-and-clean pipeline (checks A–M described below), and appends the results incrementally to the output GeoPackage. Duplicate detection is cross-chunk-accurate because the hash sets from pass 1 are passed in.

The final output is always EPSG:4326. Processing (area/length calculations, simplification, sliver detection) is done in a user-specified Cartesian CRS (`epsg`, default **5070** — NAD83 / Conus Albers) for metric accuracy.

---

## Setup

```bash
# from the repo root
pip install -e packages/vector_prep
```

Requires Python ≥ 3.9. Key dependencies: `geopandas`, `pyogrio`, `shapely`, `pyproj`, `rsxml`, `questionary`.

If you are working with a dataset whose I/O paths vary by machine, create a `.env` file in the package root (it is git-ignored) and set the path variables referenced in your `config.json`:

```bash
# .env  (one per machine — never commit this file)
MY_LAYER_INPUT=/Volumes/data/raw/my_layer.gpkg
MY_LAYER_OUTPUT=/Volumes/data/clean/my_layer_CLEAN.gpkg
MY_LAYER_GARBAGE=/Volumes/data/clean/my_layer_GARBAGE.gpkg
```

---

## Running the tool

There are three ways to invoke `vector_prep`, all backed by the same pipeline.

### Direct mode (quick one-off)

Pass everything on the command line. Good for a one-off exploration of an unfamiliar dataset.

```bash
vector_prep \
  --input  /abs/path/to/input.gpkg \
  --output /abs/path/to/output.gpkg \
  --garbage /abs/path/to/garbage.gpkg \
  --tolerance 20 \
  --epsg 5070 \
  --min_size 1 \
  --verbose
```

All I/O paths **must be absolute**. The tool will exit with a clear error if they are not.

| Flag | Default | Description |
|---|---|---|
| `--input PATH` | *(required)* | Input vector dataset (Shapefile, GeoPackage, etc.) |
| `--output PATH` | *(optional)* | Output cleaned GeoPackage. Omit for a dry-run report. |
| `--garbage PATH` | *(optional)* | GeoPackage of every dropped/changed feature with reason codes. |
| `--layer NAME` | first layer | Layer name for multi-layer GeoPackage inputs. |
| `--filter "WHERE clause"` | *(none)* | SQL WHERE clause to pre-filter features on load (e.g. `"state_code = 'CA'"`). Only matching features are processed; non-matching features are silently skipped. |
| `--tolerance METRES` | `0` | Simplification tolerance in metres. `0` skips simplification. |
| `--epsg CODE` | `5070` | Cartesian CRS used for processing (area/length/simplification). |
| `--min_size N` | *(none)* | Minimum area (m²) for polygons or length (m) for lines. |
| `--min_size_drop` | `false` | Drop features below `--min_size`. Default is to detect-and-report only. |
| `--chunk_size N` | `10000` | Features per chunk. Increase for speed, decrease to save memory. |
| `--log_file PATH` | auto | Log file path. Auto-derives a name beside the output if omitted. |
| `--verbose` | `false` | Extra log output. |

Notes for `--filter`:
- The expression is passed directly to GDAL/OGR as a SQL `WHERE` clause.
- Both `STUSPS NOT IN ('AK', 'AS', 'MP', 'HI', 'PR', 'VI')` and `"STUSPS" NOT IN (...)` are accepted for typical GeoPackage field names.
- `vector_prep` validates the filter at startup with a tiny test read and exits early with the driver error if the expression is invalid.

### Config mode (repeatable / per-layer)

For any dataset you process more than once, create a `config.json` under `vector_prep/layers/<layer_name>/`. This keeps parameters version-controlled and separates the machine-specific paths (in `.env`) from the processing logic (in the config).

```bash
vector_prep --config vector_prep/layers/us_national_roads/config.json
```

`--config` and direct-mode flags (`--input`, `--tolerance`, etc.) are mutually exclusive — the tool will tell you which flags to remove if you accidentally mix them.

### Interactive CLI

If configs exist under `vector_prep/layers/`, you can use the interactive menu instead of typing the path manually:

```bash
vector-prep-cli
```

It presents an arrow-key layer selector, shows all config-supplied parameters, and prompts for any I/O paths that are missing from the config (e.g. because they are not set as env vars on this machine).

---

## Config file reference

Config files live at `vector_prep/layers/<layer_name>/config.json` and are validated against the JSON Schema at `vector_prep/vector_prep_config.schema.json` (set `"$schema": "../../vector_prep_config.schema.json"` in VSCode for inline validation).

```jsonc
{
  "$schema": "../../vector_prep_config.schema.json",
  "parameters": {
    // ── I/O paths ──────────────────────────────────────────────────────────
    // All path strings support ${ENV_VAR} / $ENV_VAR expansion so that
    // machine-specific paths stay in .env, not in version control.
    "input":   "${MY_LAYER_INPUT}",
    "output":  "${MY_LAYER_OUTPUT}",
    "garbage": "${MY_LAYER_GARBAGE}",
    "layer":   "my_layer_name",      // optional; defaults to first layer
    "filter":  "state_code = 'CA'", // optional; SQL WHERE clause to pre-filter features on load

    // ── Processing ─────────────────────────────────────────────────────────
    "tolerance":     20,             // simplification tolerance in metres (0 = skip)
    "epsg":          5070,           // Cartesian CRS for processing (default 5070)
    "min_size":      1,              // min area m² (polygons) or length m (lines)
    "min_size_drop": false,          // true = drop below-threshold features; false = report only
    "chunk_size":    10000,          // features per chunk

    // ── Field selection (optional) ─────────────────────────────────────────
    // When present, only the listed source fields are written to output.
    // See "Field map and layer definitions" below.
    "layer_definitions": "./layer_definitions.json",
    "field_map": {
      "SOURCE_FIELD_NAME": "output_field_name"
    },

    // ── Logging ────────────────────────────────────────────────────────────
    "verbose":  false,
    "log_file": "${MY_LAYER_OUTPUT_DIR}/my_layer_prep.log"  // optional
  }
}
```

**Parameter precedence** when running with `--config`: config file values are used for everything. CLI direct-mode flags cannot be mixed with `--config` (the tool enforces this).

---

## Field map and layer definitions

By default `vector_prep` passes all source fields through to the output unchanged. When you want to **select, rename, or cast** a specific subset of fields, add a `field_map` and a `layer_definitions` to the config.

### `layer_definitions.json`

Defines the expected output schema — column names, data types, and human-readable metadata. Lives beside `config.json`.

```json
{
  "layers": [
    {
      "layer_id": "my_layer",
      "layer_name": "My Layer",
      "source_url": "https://example.gov/data/my_layer",
      "source_title": "My Source Dataset",
      "columns": [
        {
          "name": "admin_agency",
          "dtype": "STRING",
          "friendly_name": "Administering Agency",
          "description": "Agency responsible for administering this feature."
        },
        {
          "name": "area_ha",
          "dtype": "FLOAT",
          "friendly_name": "Area (hectares)"
        }
      ]
    }
  ]
}
```

Supported `dtype` values: `STRING`, `INTEGER`, `FLOAT`, `BOOLEAN`.

### `field_map` in `config.json`

Maps source field names (as they appear in the raw data) → output field names (as defined in `layer_definitions.json`). Only the listed fields appear in the output; all others are dropped.

```json
"field_map": {
  "ADMIN_AGEN": "admin_agency",
  "AREA_HA":    "area_ha"
}
```

The tool validates at startup that every output name in `field_map` exists in `layer_definitions.json` and that no two source fields map to the same output name.

---

## Processing pipeline — checks A–M

These run in order on every chunk in pass 2. Checks that modify geometry record a `CHANGED` entry in the garbage output so you can inspect the repair. Checks that remove features record a `DROPPED` entry.

| Step | Type | What it does |
|---|---|---|
| **A** | FIX | Strip Z/M coordinates. All output is 2-D. |
| **B** | DETECT | Log multi-part geometry counts (no action taken). |
| **C** | DETECT | Log raw `GeometryCollection` input counts (no action taken here; repaired in D). |
| **D** | FIX / CHANGED | Repair invalid polygons (`make_valid`) and unwrap any `GeometryCollection` results into the dominant geometry type. Fixed features appear in the garbage output with `VP_Operation=CHANGED`. |
| **E** | DETECT | Log unclosed ring counts (no action taken). |
| **F** | DETECT | Log duplicate-vertex counts (no action taken). |
| **G** | FIX | Normalise string fields: strip leading/trailing whitespace, remove non-printable control characters, convert empty-after-strip strings to `NULL`. |
| **H** | DETECT | Log schema inconsistencies: year columns stored as float, measurement columns stored as string, string columns whose values look overwhelmingly numeric. Identifier/code columns (FIPS, HUC, route numbers, UUIDs) are explicitly excluded from the numeric-value check. |
| **I** | DROP | Drop polygon features whose bounding box has zero width or height (degenerate geometry). |
| **J** | DROP | Drop duplicate geometries (same WKB bytes after reprojection and Z-strip), keeping the first occurrence. Cross-chunk accurate via pass-1 hash sets. |
| **K** | DROP | Drop fully duplicate rows (identical geometry **and** all attribute values), keeping the first occurrence. Cross-chunk accurate. |
| **L** | DROP / DETECT | Features below `min_size` threshold (area for polygons, length for lines). Drops if `min_size_drop=true`; otherwise counts and reports only. |
| **M** | DROP | Sliver polygons: two-phase filter. Phase 1 — fast isoperimetric quotient pre-filter (IQ < 0.1). Phase 2 — negative buffer confirmation (< 1 m interior width). Only features that fail both phases are dropped, so jagged-but-valid polygons (wetlands, complex parcels) are preserved. |

After checks, `clean_geometries` runs on the surviving features:
- Attempts `make_valid` on any remaining invalid geometry.
- Applies simplification at `tolerance` metres (Douglas-Peucker via Shapely) if `tolerance > 0`.
- Drops any feature whose geometry is `NULL` or empty after cleaning.

---

## Output files

### Cleaned output GeoPackage (`--output`)

- All geometries valid, 2-D, in **EPSG:4326**.
- Only features that passed all checks and cleaning.
- A globally-unique integer `FID` column is added.
- If `field_map` is configured, only the mapped/renamed/cast columns are present. Otherwise all source columns are preserved.
- Pre-existing output file is deleted before writing so re-runs always start clean.

### Garbage GeoPackage (`--garbage`)

Every feature that was **dropped** or whose geometry was **changed** during processing is written here with two extra columns:

| Column | Values |
|---|---|
| `VP_Operation` | `DROPPED` or `CHANGED` |
| `VP_Reason` | Human-readable reason, e.g. `"Duplicate geometry"`, `"Sliver polygon"`, `"Invalid polygon repaired"` |

Geometry is the **pre-fix** state so you can see what was wrong. This file is optional but highly recommended for any dataset you haven't cleaned before.

### Log file

Written automatically beside the output (`<output_stem>_vector_prep.log`), or to the path specified by `log_file` / `--log_file`. Contains per-chunk progress, per-check counters, and the final summary report.

---

## Adding a new layer

1. **Create a layer directory:**
   ```
   vector_prep/layers/<your_layer_name>/
   ```

2. **Add a `config.json`** using the reference above. Set `$schema` to `"../../vector_prep_config.schema.json"` for VSCode validation.

3. **Add path variables to `.env`** for any `${VAR}` references in the config.

4. **Add a `layer_definitions.json`** if you want field selection/renaming/casting. Reference it from `config.json` as `"layer_definitions": "./layer_definitions.json"`.

5. **Run it:**
   ```bash
   # Via config
   vector_prep --config vector_prep/layers/<your_layer_name>/config.json

   # Or via the interactive menu
   vector-prep-cli
   ```

6. **Review the garbage output.** Check `VP_Reason` counts in the log report and spot-check the garbage GeoPackage in QGIS to make sure nothing unexpected was dropped.
