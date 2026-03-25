---
input-vector-path: /home/narlorin/udata/blm/pasture_polygons_2026-03-24/BLM_Natl_Grazing_Pasture_Polygons_654070438877449879.gpkg
input-layer-name: 
source_category: usgov
source_title: US DOI Bureau of Land Management Geospatial Business Platform
source_url: https://gbp-blm-egis.hub.arcgis.com/datasets/BLM-EGIS::blm-natl-grazing-pasture-polygons/about
layer_id: blm-natl-grazing-pasture-polygons
snapshot-id: 2026-03-24
---

# Vector Prep Run Log

## Summary

Pasture polygons from BLM needed for BLM reports. Updates monthly.

This entire file is hand-written, eventually automate or partially automate.

## Notes/Description of Inputs & Parameters

(standard definitions of inputs and parameters shoudl go somewhere else)

- input-vector-path, help="Input vector (shapefile, gpkg, etc.) Path. Supplied to vector_prep as input (first param)"
- output-vector-path, help="Output vector path". Required by vector_prep, but should be built from /dist + layer_id, snapshot_id
- 
- input-layer-name: help="Layer name (for geopackage). If not provided and input is geopackage, first layer is used.", default=None vector_prep --layer parameter
- "--tolerance", type=float, help="Simplify tolerance in METRES (0 to skip).", default=0.0
- "--epsg", type=int, help="Cartesian CRS EPSG code to reproject to **before** processing (optional). Default is 5070 (NAD83 / Conus Albers).", default=5070
- source_title. this is the layer provenance friendly name
- source_url. this is the layer provenance url 
- source_category ("usgov") - determines where in s3://riverscapes-athena/raw/ it goes
- layer_id. This should be unique within our layer_source_category. should be lower case, will be used for athena table names, identifier in layer_definitions. there the namespace is usually repo+tool. perhaps for this "tool" is vectorprep+source_category.
- snapshot_id - usually date we downloaded/received the data. default today's date

## Overview of planned steps

1. Run vector_prep.py on the gpkg — (geometry cleaning) + transform to EPSG:4326
2. document/address any errors
3. Identify business entity definition (natural key) and entity mode (ONE_ROW_PER_ENTITY vs MANY_ROWS_PER_ENTITY)
4. Add rs_row_id using UUID4 (later consider deterministic based on natural key)
5. populate layer_defs metadata with inputs / scraped from source
6. export to 4326 geoparquet including bbox - either with qgis or same as pipelines/rme_to_athena
7. upload data to s3 (raw/)
8. upload layer_definitions metadata to s3 (use existing gh action/script)
9. create `rs_raw` athena table - ideally use metadata to add COMMENTs
10. create spatial intersection tables with existing layers
11. create view/materialized tables in rs_rpt

### Vector Prep Standard Error Checking

### Business Entity Definition - Name & ID Field

`ST_ALLOT_PAST` should be the unique ID according to Megan McLachlan, National Operations Center. However it does not uniquely identify rows. A file GDB provided by district office appeared to have some errors. 