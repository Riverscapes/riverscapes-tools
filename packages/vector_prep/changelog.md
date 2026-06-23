# Vector Prep Changelog


## 0.1.4

### Fixes

- **`field_map` DATETIME support** — added `DATETIME` as a supported `dtype` in `lib/field_map.py`. Field-map casting now parses date/time values with `pd.to_datetime(errors="coerce", utc=True)` and strips timezone to produce timezone-naive `datetime64[ns]` for GeoPackage output compatibility.
- **GeoPackage integer write fix** — `lib/output.py` now uses `engine="pyogrio"` for all `to_file()` calls. Added `_normalise_dtypes_for_gpkg()` pre-write helper that converts pandas nullable `boolean` and `string` extension types to plain Python `object`. Pandas nullable integers (`Int64`, etc.) are passed through unchanged: pyogrio writes them as GeoPackage `INTEGER` with proper `NULL` support, preventing the silent promotion to `float64` that Fiona performed previously.
- **README** updated to document `DATETIME` as a supported `layer_definitions` dtype.


## 0.1.3

### New Features

- **SQL `filter` / `--filter` argument** — optional `WHERE` clause passed to pyogrio at read time so only matching features are loaded into memory. Validated against the source layer at startup (syntax and field-name errors are caught before any long-running processing begins). Report now shows both the total source feature count and the filtered subset count. Config key: `filter`; CLI flag: `--filter`.

### Improvements

- **`vector_prep_cli` enhancements** — parameter-review step displays config-supplied values before prompting for any missing I/O paths; selection menu and resolution logic robustness improved.
- **`lib/report.py` improvements** — report layout updated; source vs. filtered feature counts now surfaced separately.
- **`vector_prep.py` refactors** — `source_feature_count` tracked separately from `total_features` to support `filter` reporting; path-validation now covers all three I/O paths; various code-quality and type-annotation improvements.
- **OS-agnostic launch config** — `.vscode/launch.json` paths made cross-platform.


## 0.1.2

### New Features

- **Windowed / chunked processing** — large datasets are now processed in two passes: pass 1 (pyogrio, hash-only) builds duplicate geometry and row-hash sets; pass 2 processes features in chunks, enabling memory-efficient handling of very large layers.
- **Comprehensive geometry & attribute quality checks** — 13 named checks covering DROP / CHANGED / DETECT / FIX operations (null geometries, self-intersections, duplicates, undersized features, mixed geometry types, and more). New `--min_size` argument; `VP_Operation` / `VP_Reason` garbage columns.
- **Garbage output** — features removed or altered during processing can be written to a separate GPKG via `--garbage`. Includes per-reason / per-geometry-type drop statistics and a printed summary report.
- **`lib/` modularisation** — core logic split into `lib/checks.py`, `lib/clean.py`, `lib/field_map.py`, `lib/garbage.py`, `lib/geometry_utils.py`, `lib/output.py`, and `lib/report.py`.
- **`field_map` and `layer_definitions` support** — new `lib/field_map.py` module applies field renaming, type coercion (including BOOLEAN sparse-index handling), and output-name deduplication driven by config.
- **`--config` wired up** — CLI `--config` flag loads a JSON config file; CLI arguments take precedence over config values. `--tolerance` default changed to `None` so `--tolerance 0` can correctly override config.
- **`vector_prep_fields` robustness** — 9 usability and bug fixes: `set_empty_null` no-op fix, geometry column resolved via `gdf.geometry.name`, `os.remove` warning, duplicate output-field name validation, same-path guard, append mode for output, `--list-layers` pre-parse block, `bool`/`boolean` type support.


## 0.1.1

Beginning to make this a module that can help prepare data for use in reporting data platform (Athena), including enriching it with metadata.