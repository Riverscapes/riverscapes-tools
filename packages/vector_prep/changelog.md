# Vector Prep Changelog


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