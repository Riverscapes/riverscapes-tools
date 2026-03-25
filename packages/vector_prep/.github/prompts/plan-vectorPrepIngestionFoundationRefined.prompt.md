## Plan: Vector Prep Ingestion Foundation

Build a phased ingestion workflow that starts with a local-input Phase 1 pipeline for geometry QA, metadata enrichment, Athena-ready parquet export, and run-level documentation. Use repository-root `dist/` and `logs/` outputs, codify per-layer key recipe validation with entity-to-row modes, and adopt a raw snapshots plus reporting latest Athena strategy. S3 path for US government sources use the `usgov_sources` prefix (consistent with existing S3 layout). QGIS can be used short-term as tool for GeoParquet export -- produces both a `geometry` column (WKB) and a `geometry_bbox` struct column needed by Athena spatial queries.

Run documentation uses a plain `RUNLOG.md` now; explore STAC (SpatioTemporal Asset Catalog) as a formal provenance format in Phase 2.

**Steps**
1. Phase 1: Define run-manifest contract, key policies, and output layout.
2. Create a manifest structure spec (run-level JSON contract) and implement manifest writing in `logs/`, including run id, source URL, timestamps, input path hash/size, layer name, feature counts, geometry cleaning stats, CRS changes, schema snapshot, parquet output paths, and status/error summary.
3. Add per-layer identity policy to layer definitions: candidate key recipe, normalization rules, and entity mode (`ONE_ROW_PER_ENTITY` or `MANY_ROWS_PER_ENTITY`). Validate every run and record metrics in manifest (`null_key_rows`, `duplicate_key_groups`, `largest_duplicate_group`, `distinct_entity_count`, `rows_per_entity_distribution`). *parallel with step 2*
4. Add root output layout conventions: `dist/` for generated artifacts and `logs/` for run logs/manifests; update ignore rules so generated files are never committed. *parallel with step 2*
5. Add a Phase 1 parquet export script that guarantees EPSG:4326 before write and emits GeoParquet under `dist/usgov_sources/{dataset_id}/{snapshot_id}/...`; may use QGIS export to produce both `geometry` (WKB) and `geometry_bbox` columns required for Athena spatial queries; include deterministic column ordering and null-safe type handling. *depends on 2*
6. Add a combined local-input orchestrator script that runs: read input -> geometry clean (`vector_prep`) -> metadata fetch/enrichment -> key-recipe validation -> optional GUID column hook -> parquet export -> manifest write. Initial scope is local input path only; source download is explicitly deferred. *depends on 2, 3, and 5*
7. Introduce GUID strategy abstraction for `rs_reports_rowid` with CLI plumbing, and maintain separate semantics for `entity_id` (logical identity) vs row id (physical row identity). Final `entity_id` strategy decision (UUID4 vs UUID5/derived) remains a checkpoint. *depends on 6*
8. Add verification for Phase 1: smoke run on one known ArcGIS source, schema snapshot comparison to prior run artifact, key uniqueness validation behavior for both entity modes, and checks that all outputs land in `dist/` and `logs/` and are git-ignored. *depends on 6 and 7*
9. Phase 2: Source automation and Athena layout.
10. Add source download mode, S3 upload under `s3://{bucket}/usgov_sources/{dataset_id}/snapshot={snapshot_id}/`, and immutable versioned prefixes keyed by ingestion date/run id.
11. Implement Athena naming convention: in the `rs_raw` database, create snapshot-specific physical tables per layer (`{layer}_snapshot_{snapshot_id}` pattern); in `rs_rpt`, expose latest-only stable views for reporting.
    - Example today: `usgov_sources.pastures_snapshot_20260324`
    - Example latest view: `rs_rpt.pastures`
12. Implement join materialization pattern as bridge tables instead of embedding join keys into base layer tables; support optional persisted `dominant_related_object_id` (largest overlap) in selected materialized reporting outputs for performance-sensitive workflows.
13. Add lifecycle policy recommendations (standard -> IA -> Glacier) and promote latest pointers only for successful non-breaking runs.
14. Phase 3: Drift governance and release hardening.
15. Add schema drift classifier (add/drop/rename/type-change) plus key-recipe drift classifier (uniqueness and entity-fragmentation drift), then define breaking-change gates for reporting compatibility.
16. Add monthly scheduled CI workflow plus manual rerun workflow, with clear rollback to prior successful versioned dataset.

**Relevant files**
- `/home/narlorin/ucode/riverscapes-tools/packages/vector_prep/vector_prep/vector_prep.py` — reuse geometry cleaning flow (`vector_prep`, `clean_geometries`) and emitted stats for manifest fields.
- `/home/narlorin/ucode/riverscapes-tools/packages/vector_prep/vector_prep/fetch_arcgis_metadata.py` — reuse metadata fetch + merge (`fetch_columns_from_hub_url`, `update_layer_definitions`) as enrichment step input.
- `/home/narlorin/ucode/riverscapes-tools/packages/vector_prep/vector_prep/layer_definitions.json` — extend with per-layer key recipe and entity mode declarations.
- `/home/narlorin/ucode/riverscapes-tools/packages/vector_prep/README.md` — update process docs with run-manifest, key validation policy, and phased pipeline behavior.
- `/home/narlorin/ucode/riverscapes-tools/.gitignore` — confirm `dist/` handling and add explicit `logs/` ignore rule.
- `/home/narlorin/ucode/riverscapes-tools/.github/workflows/metadata-catalog.yml` — reference existing publish pattern for later Phase 2/3 CI scheduling and S3 publication conventions.
- `NEW: /home/narlorin/ucode/riverscapes-tools/packages/vector_prep/vector_prep/export_to_parquet.py` — Phase 1 parquet export utility (scripted path; QGIS export used manually now and produces required `geometry` + `geometry_bbox` columns).
- `NEW: /home/narlorin/ucode/riverscapes-tools/packages/vector_prep/vector_prep/run_pipeline.py` — Phase 1 combined local-input orchestration entrypoint.
- `NEW: /home/narlorin/ucode/riverscapes-tools/packages/vector_prep/vector_prep/key_quality.py` — key recipe normalization, validation metrics, and entity-mode checks.

**Verification**
1. Run combined script on one local GeoPackage/Shapefile source and confirm successful completion.
2. Validate final parquet can be read by pyarrow/pandas/geopandas and reports CRS metadata as WGS84-compatible (EPSG:4326 preprocessing enforced before export).
3. Confirm manifest JSON exists in `logs/` and contains required keys, counts, output paths, and key-quality metrics.
4. Re-run same input and compare schema snapshots/manifests to ensure stable column naming/order and deterministic key-quality outputs where expected.
5. Run at least one layer configured as `ONE_ROW_PER_ENTITY` and one as `MANY_ROWS_PER_ENTITY` to verify pass/fail and warning behavior.
6. Confirm `git status` excludes files generated under `dist/` and `logs/`.

**Decisions**
- Confirmed: Phase structure requested, with immediate focus on Phase 1 deliverables.
- Confirmed: Phase 1 combined script starts from local input path; source download automation deferred.
- Confirmed: repository-root `dist/` and repository-root `logs/` are preferred output folders.
- Confirmed: use plain `RUNLOG.md` for human-readable run documentation now; JSON manifest and STAC provenance format deferred to Phase 2.
- Confirmed: every ingested layer must define a candidate key recipe and be revalidated for uniqueness every run.
- Confirmed: entity-to-row modeling is explicit per layer (`ONE_ROW_PER_ENTITY` vs `MANY_ROWS_PER_ENTITY`) and reported in manifests.
- Confirmed: Athena pattern is snapshot-specific physical tables in `rs_raw` database, with latest-only stable reporting views in `rs_rpt`.
- Confirmed: join materialization should use bridge tables; allow optional persisted dominant-related-object key in performance-sensitive reporting outputs.
- Open decision checkpoint: final `rs_reports_rowid` and `entity_id` generation strategy (UUID4, UUID5 deterministic, or source-derived hash) before production automation.

**Further Considerations**
1. Evaluate whether `rs_raw` should use many snapshot-suffixed tables or one partitioned table per layer with `snapshot_id` as a partition column; keep reporting contract in `rs_rpt` unchanged either way.
2. Treat dropped/renamed columns as breaking and treat sustained key uniqueness regression as breaking for `ONE_ROW_PER_ENTITY` layers.
3. Keep all manifests indefinitely in S3 alongside artifacts because they are the primary evidence of traceability and drift over time.
