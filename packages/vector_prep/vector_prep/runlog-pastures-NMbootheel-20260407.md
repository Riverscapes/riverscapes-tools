# Vector Prep Run Log

## Summary

Updated version of Pasture polygons for New Mexico Bootheel from Meg McLachlan at BLM, to be used for the pilot project April 2026.

I believe they merged the polygons so that now there is one multi-part per ST-allotment-pasture ID.

This entire file is hand-written, eventually automate or partially automate.

Inputs in: `/home/narlorin/udata/blm/pasture_polygons_bootheel_2026-04-02/inputs.json`

## Notes/Description of Inputs & Parameters

(standard definitions of inputs and parameters should go somewhere else someday like a json.schema assuming the inputs are stored in json)

* input-vector-path, help="Input vector (shapefile, gpkg, etc.) Path. Supplied to vector_prep as input (first param)"
* output-vector-path, help="Output vector path". Required by vector_prep, but should be built from /dist + layer_id, snapshot_id
* input-layer-name: help="Layer name (for geopackage). If not provided and input is geopackage, first layer is used.", default=None vector_prep --layer parameter
* "--tolerance", type=float, help="Simplify tolerance in METRES (0 to skip).", default=0.0
* "--epsg", type=int, help="Cartesian CRS EPSG code to reproject to **before** processing (optional). Default is 5070 (NAD83 / Conus Albers).", default=5070
* source_title. this is the layer provenance friendly name
* source_url. this is the layer provenance url
* source_category ("usgov") - determines where in s3://riverscapes-athena/raw/ it goes
* layer_id. This should be unique within our layer_source_category. should be lower case, will be used for athena table names, identifier in layer_definitions. there the namespace is usually repo+tool. perhaps for this "tool" is vectorprep+source_category. In reality we are inconsistent in namespacing athena vs s3 vs layer_definitions hierarchy, but should move towards consistency
* snapshot_id - usually date we downloaded/received the data. default today's date

## Overview of planned steps

1. Run vector_prep.py on the gpkg — (geometry cleaning) + transform to EPSG:4326. 
2. document/address any errors
3. Identify business entity definition (natural key) and entity mode (ONE_ROW_PER_ENTITY vs MANY_ROWS_PER_ENTITY)
4. Add rs_row_id using UUID4 (later consider deterministic based on natural key)
5. populate layer_defs metadata with inputs / scraped from source
6. export to 4326 geoparquet including bbox - either with qgis or same as pipelines/rme_to_athena
7. upload data to s3 (raw/)
8. upload layer_definitions metadata to s3 (use existing gh action/script)
9.  create `rs_raw` athena table - ideally use metadata to add COMMENTs
10. create spatial intersection tables with existing layers
11. create view/materialized tables in rs_rpt

## 1,2 Vector Prep Standard Error Checking

* run `orchestrate.py` / `step_1`
* output copied it below. We'll need to write it somewhere. The log is not designed for long-term, and indeed currently is overwritten whenever run the process (and for expediency when we run into issues we don't always restart the code at step 1 each time, we comment out that part and go to step 2 or whatever)

```text
[INFO] [Vector Prep] Reading input dataset with GeoPandas: /home/narlorin/udata/blm/pasture_polygons_bootheel_2026-04-02/boot_pastures_blm20260402.gpkg
[DEBUG] [Vector Prep] No layer name specified, geopandas will choose default/first layer
[INFO] [Vector Prep] Loaded 159 features. CRS: EPSG:5070
[INFO] [Vector Prep] Geometry type of input: {'MultiPolygon': 159}
[INFO] [Vector Prep] Reprojecting to EPSG:5070 for processing...
[INFO] [Vector Prep] Reprojection complete. New CRS: EPSG:5070
[INFO] [Vector Prep] Cleaning geometries (tolerance=0.0)...
[INFO] [Vector Prep] Input features: 159
[INFO] [Vector Prep] Null/empty geometries found: 0
[INFO] [Vector Prep] Invalid geometries fixed: 0
[INFO] [Vector Prep] Invalid geometries unfixed (dropped): 0
[INFO] [Vector Prep] Features simplified: 0
[INFO] [Vector Prep] Dropped features after cleaning: 0
[INFO] [Vector Prep] Remaining features to write: 159
[INFO] [Output GDF] Reprojecting to EPSG 4326 for output
[INFO] [Output GDF] Writing cleaned layer to /home/narlorin/ucode/riverscapes-tools/dist/usgov_sources/blm-natl-grazing-pasture-polygons-nm-bootheel/2026-04-02/blm-natl-grazing-pasture-polygons-nm-bootheel.gpkg (driver=GPKG)...
[INFO] [Output GDF] Write complete.
[INFO] [Vector Prep Orchestrate] Prepped dataframe with shape (159, 16) and outputed to /home/narlorin/ucode/riverscapes-tools/dist/usgov_sources/blm-natl-grazing-pasture-polygons-nm-bootheel/2026-04-02/blm-natl-grazing-pasture-polygons-nm-bootheel.gpkg

```

* TLDR: no errors found

### 3 Business Entity Definition - Name & ID Field

This step in datagrip - SQL is most convenient. Copy file out of WSL filesystem, bit of pain but works.  
`Pasture_ID` is field added by Megan McLachlan's team at National Operations Center. It is unique (159 records, 159 values)

#### Name field?

* 158 distinct `Pasture_Name`. `Flying W Mountain8` occurs twice.

Skip adding any extra fields - we will use what was provided only.

### build Metadata (layer_definitions)

* fn build_layer_defs in orchestrate (adapted to omit source_url since there is none)
* updated this to include all fields in source, even if there is no metadata (infers data types)

### Export to parquet, with bounding box

* added function to do this
* saved to `"C:\nardata\work\reference_data_prep\blm_pastures\blm-natl-grazing-pasture-polygons-2026-03-24.parquet"`

* upload to s3 manually: 
`s3://riverscapes-athena/usgov_sources/blm-natl-grazing-pasture-polygons-nm-bootheel/2026-04-02/`

### Build Athena table

* ran `build_athena_ddl` 
* [x] executed in Athena

* [ ] upload new layer_definitions (merge with main)

### Spatial intersection

