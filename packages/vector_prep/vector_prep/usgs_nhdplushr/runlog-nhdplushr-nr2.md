# Run Log — USGS NHDPlus HR National Release 2

## Source
- **Title**: USGS National Hydrography Dataset Plus High Resolution National Release 2 FileGDB
- **URL**: https://www.sciencebase.gov/catalog/item/679d39cbd34eb38981c9c7a5
- **DOI**: 10.5066/P13V7GVY
- **Publication Date**: 2025-01-31
- **Download**: https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/National/GDB/NHDPlus_H_National_Release_2_GDB.zip
- **User Guide**: https://pubs.usgs.gov/publication/ofr20191096
- **Size**: ~60 GB unzipped
- **CRS**: NAD83 + NAVD88 height (compound; EPSG:4269 horizontal + EPSG:5703 vertical)
- **Driver**: OpenFileGDB (GDAL)
- **Local path**: `F:\nardata\datadownload\usgs\nhdplushr\NHDPlus_H_National_Release_2_GDB\NHDPlus_H_National_Release_2_GDB.gdb`

## GDB Layer Inventory (15 layers)

| Layer | Geometry | Features | Athena layer_id | Priority |
|-------|----------|----------|-----------------|----------|
| NetworkNHDFlowline | 3D Measured Multi Line String | 24,943,827 | `usgs-nhdplushr-network-flowline` | **Phase 1** (attrs only) |
| NHDPlusFlow | None (table) | 34,455,139 | `usgs-nhdplushr-flow` | **Phase 1** (topology/routing) |
| WBDHU12 | Multi Polygon | 89,861 | `usgs-nhdplushr-wbdhu12` | **Phase 1** (with simplified geom) |
| NHDPlusCatchment | Multi Polygon | 25,529,741 | | Phase 2 |
| NHDWaterbody | 3D Multi Polygon | 7,036,369 | | Phase 2 |
| NHDPlusGageSmooth | None (table) | 5,094,532 | | Phase 2 |
| NHDPlusSink | Point | 1,195,899 | | Phase 2 |
| NonNetworkNHDFlowline | 3D Measured Multi Line String | 544,345 | | Phase 2 |
| NHDPoint | 3D Point | 503,262 | | Phase 2 |
| NHDPlusWall | Multi Line String | 386,163 | | Phase 2 |
| NHDArea | 3D Multi Polygon | 169,069 | | Phase 2 |
| NHDLine | 3D Multi Line String | 160,845 | | Phase 2 |
| NHDPlusConnect | None (table) | 983 | | Phase 2 |
| NHDPlusBoundaryUnit | Multi Polygon | 536 | | Phase 2 |
| NHDPlusGage | Point | 6,232 | | Phase 2 |

### Key columns per Phase 1 layer

**NetworkNHDFlowline** (82 fields; unique on `permanent_identifier`)

- Core identifiers: `permanent_identifier`, `nhdplusid`, `reachcode`, `gnis_id`, `gnis_name`
- Network routing VAAs: `hydroseq`, `levelpathi`, `terminalpa`, `fromnode`, `tonode`, `streamorde`, `streamleve`, `divergence`, `startflag`, `terminalfl`
- Upstream/downstream: `uphydroseq`, `uplevelpat`, `dnhydroseq`, `dnlevelpat`, `dnminorhyd`
- Catchment/drainage: `areasqkm`, `totdasqkm`, `divdasqkm`, `arbolatesu`
- Elevation/slope: `maxelevraw`, `minelevraw`, `maxelevsmo`, `minelevsmo`, `slope`, `slopelenkm`
- Flow estimates (multiple methods A–F): `qama`..`qfma`, `vama`..`vema`, etc.
- 53 coded-value domains in GDB (e.g. Resolution, NoYes Domain, HydroFlowDirections, Divergence Domain)

**NHDPlusFlow** (11 fields; no single unique key — each row is a directed edge)

- `fromnhdpid`/`tonhdpid` (NHDPlusID), `frompermid`/`topermid` (PermanentIdentifier)
- `nodenumber`, `deltalevel`, `direction`, `gapdistkm`, `hasgeo`
- `fromvpuid`, `tovpuid`

**WBDHU12** (19 fields; unique on `huc12`)

- `huc12`, `name`, `states`, `hutype`, `tohuc`
- `areasqkm`, `areaacres`, `noncontributingareasqkm`, `noncontributingareaacres`
- `tnmid`, `nhdplusid`, `vpuid`

## TODO

### Phase 1 — Immediate

- [ ] **Build layer_definitions.json entries** — run `fetch_fgdb_metadata.py` for each Phase 1 layer to auto-generate column definitions (field names, aliases, types, coded-value domains) from the GDB
- [ ] **Write `orchestrate-nhdplushr.py`** — multi-layer orchestration script
  - NetworkNHDFlowline: geometry cleaning (vector prep), add end point coordinates, add simplified (11 m) geometry, partition and export to Parquet
  - NHDPlusFlow: non-spatial table → Parquet (no geometry processing needed)
  - WBDHU12: geometry cleaning (vector_prep), simplify at 100 m (TBC) tolerance, add `geometry_bbox`, export to GeoParquet
- [ ] **Evaluate Parquet partitioning strategy** — 25M+ row layers are large; plan to partition by `vpuid` (8-digit HU4 VPU) for largest tables, HUC2 (first two digits of vpu) for medium tables, no partitioning for smaller tables. Also within-parquet ordering matters - be deliberate in how rows are sorted.
- [ ] **Handle compound CRS** — source is NAD83+NAVD88 (3D); strip Z and reproject to EPSG:4326 for output
- [ ] **Address duplicate features** — per USGS docs: "adjacent VPUs often contained duplicate copies of features intersecting the VPU boundaries" with different NHDPlusID but same geometry/attributes; decide dedup strategy
- [ ] **Generate Athena DDL** for each Phase 1 table
- [ ] **Upload Parquet to S3** (`s3://riverscapes-athena/usgov_sources/usgs-nhdplushr-*/2025-01-31/`)
- [ ] **Create Athena tables** and verify with test queries

### Phase 1 — Decisions

- Partition by `vpuid` (natural partition, ~200 VPUs)
- Dedup: check if we can drop duplicates on `permanent_identifier`. If that isn't a clean fix then keep all rows (matching USGS as-published) and document?
- NHDPlusFlow: skip `RS_ROW_ID` (, it is not useful for an edge table. Even though no natural unique key exists; a composite of `fromnhdpid+tonhdpid` is not strictly unique if multiple edges exist between same nodes.)
- skip adding RS_ROW_ID to NHD tables in favour of checking that the business/natural keys are unique and documenting them, including in the json `unique_id_field`.

### Phase 2 — Future layers

- NHDPlusCatchment (25.5M polygons — largest spatial layer; similar treatment to WBDHU12)
- NHDWaterbody (7M polygons)
- Remaining spatial and attribute layers as needed

## Run History

### 2026-04-22 — Initial setup

- Downloaded and unzipped NHDPlus HR NR2 FileGDB (~60 GB)
- Created `usgs_nhdplushr/` directory with `inputs.json` and this runlog
- Inspected GDB with `ogrinfo`: catalogued all 15 layers (12 spatial, 3 attribute tables)
- Created `fetch_fgdb_metadata.py` — new module to extract column metadata (field names, aliases, types, coded-value domains) from FGDB via GDAL OpenFileGDB driver; analogous to `fetch_arcgis_metadata.py` for Hub sources
- Tested `fetch_fgdb_metadata.py` against all 3 Phase 1 layers — produces correct `layer_definitions.json`-compatible column dicts
- ScienceBase is not ArcGIS Hub — no Feature Service REST API available; FGDB metadata is the primary source for field documentation


