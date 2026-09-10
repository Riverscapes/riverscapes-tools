# National Wetlands Inventory

## Datasets

1. The codes:

"C:\nardata\datadownload\FWS\NWI-Code-Definitions\NWI_Code_Definitions_Metadata.xml" is an FGDC-STD-001-1998 metadata file for the NWI-Code-Definitions csv table.

2. Wetlands
3. Riparian

## Processing Plan

Data are downloaded at HUC8 level. There are approximately 5000 for CONUS.

### Decisions locked in

1. Source format: use FileGDB from each HUC8 zip (preferred over shapefile).
2. Local outputs: no single combined local GeoPackage.
3. Destination: one Athena table for wetlands and one Athena table for riparian.
4. Execution model: sequential processing only for first production run (no parallelization).

### Config file naming

Yes, use separate config files. Suggested names:

1. config_wetlands.json
2. config_riparian.json

Notes:

1. vector_prep and orchestrate accept any config filename as long as --config points to it.
2. Keep both files in this folder so layer_definitions paths stay simple and relative.

### End-to-end workflow (sequential, restartable)

For each HUC8:

1. Resolve zip path and extract HUC8 from filename (HU8_########_Watershed.zip).
2. Confirm zip exists.
3. Open zip and locate FileGDB.
4. If FileGDB is missing: mark ledger status as source_missing and continue.
5. For each target layer (wetlands, riparian):
6. Run vector prep using layer-specific config.
7. Inject huc8 column into prepared output (required in final schema).
8. Upload to corresponding Athena table.
9. Replace existing rows for that huc8 in destination table (idempotent behavior).
10. Write counts, timing, and status to ledger.

### Ledger design (GeoPackage-backed)

Use a GeoPackage as the run ledger, for example:

1. nwi_processing_ledger.gpkg

Table design (non-spatial table is fine):

1. ledger_runs

Recommended columns:

1. run_id TEXT
2. run_started_utc TEXT
3. run_finished_utc TEXT
4. operator TEXT
5. notes TEXT

1. ledger_huc8_status

Recommended columns:

1. huc8 TEXT NOT NULL
2. zip_path TEXT NOT NULL
3. zip_exists INTEGER NOT NULL (0/1)
4. zip_mtime_utc TEXT
5. download_status TEXT (downloaded, failed, missing, unknown)
6. download_last_checked_utc TEXT
7. fgdb_path_in_zip TEXT
8. wetlands_status TEXT (pending, source_missing, prepped, uploaded, failed)
9. wetlands_rows_prepped INTEGER
10. wetlands_rows_uploaded INTEGER
11. wetlands_last_error TEXT
12. wetlands_updated_utc TEXT
13. riparian_status TEXT (pending, source_missing, prepped, uploaded, failed)
14. riparian_rows_prepped INTEGER
15. riparian_rows_uploaded INTEGER
16. riparian_last_error TEXT
17. riparian_updated_utc TEXT
18. retry_count INTEGER DEFAULT 0
19. run_id TEXT

Suggested key/indexes:

1. Primary key: (huc8)
2. Index: wetlands_status
3. Index: riparian_status
4. Index: download_status

### Post-facto download status backfill

Download is complete except 69 failures. Two acceptable options:

1. Parse existing download log and set download_status per HUC8 in ledger.
2. Re-run downloader with skip-existing behavior and refresh status from that pass.

Either approach is valid. Option 2 is simpler operationally if the rerun is quick.

### Idempotency rules

1. Every write to Athena must be scoped by huc8.
2. Re-run of a HUC8 must produce same final state as first successful run.
3. On rerun, overwrite/replace only that HUC8 partition/slice, not the whole table.
4. Ledger is source of truth for resumability.

### Schema notes

1. Add huc8 as explicit STRING column in both wetlands and riparian output schemas.
2. Keep existing NWI identifiers and attributes unchanged where possible.
3. If deterministic row ids are used, derive with huc8 plus source id to prevent cross-HUC8 collisions.

### Immediate next implementation tasks

1. Create config_wetlands.json and config_riparian.json.
2. Add huc8 to field mappings and layer definitions for both layers.
3. Implement GeoPackage ledger initialization and update routines.
4. Implement sequential orchestrator driver over all HUC8 zips.
5. Implement/verify Athena per-huc8 replace step for wetlands and riparian tables.
6. Run pilot on a small HUC8 sample set, then full CONUS run.