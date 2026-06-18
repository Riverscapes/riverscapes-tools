# US counties from Census - cartographic boundary 500k

This is an alternative data source to us_cens_county that is designed for small scale mapping and is clipped to coastline. It also happens to have much more user-friendly columns - e.g. state codes.

## Step overview

1. download
2. review. choose columns to keep 
3. filter for conus
4. fetch metadata / build layer_definitions
5. vector prep
6. upload
7. documentation

## Download

Source description:  https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html 

Source: https://www2.census.gov/geo/tiger/GENZ2025/gpkg/cb_2025_us_all_500k.zip 
Status: DONE
Operator: LSG
Output: "C:\nardata\datadownload\uscensus\cb_2025_us_all_500k.gpkg"

## Filter conus

STUSPS NOT IN ('AK', 'AS', 'MP', 'HI', 'PR', 'VI')

## Metadata

Started with the layer_definition for the us_cens_county (TIGER full res version) and modified manually based on the technical documentation

## Vector Prep

*Tool version*: `0.1.3` now includes filtering (in this case, to CONUS, per above)

### Input And Geometry Fixes

| Metric | Value |
| --- | ---: |
| Input filter applied | YES |
| Input features (source/original layer) | 3,235 |
| Input features processed (after filter) | 3,110 |
| Null/empty on input | 0 |
| Invalid geometries fixed | 0 |
| Invalid geometries unfixed | 0 |
| Features simplified | 3,110 |

### Quality Checks

| Check | Count |
| --- | ---: |
| Z/M coordinates stripped | 0 |
| Multi-part features detected | 3,110 |
| GeometryCollection features | 0 |
| Self-touching rings fixed | 0 |
| Unclosed rings detected | 0 |
| Duplicate-vertex features | 0 |
| String cells normalized | 0 |
| Schema inconsistencies detected | 5 |
| Mixed geometry types detected | NO |

### Dropped Features

| Reason | Count |
| --- | ---: |
| Total dropped | 0 |
| null/empty | 0 |
| invalid (unfixed) | 0 |
| zero-area bounding box | 0 |
| duplicate geometry | 0 |
| duplicate row | 0 |
| below minimum size | 0 |
| sliver polygon | 0 |

### Dropped By Geometry Type

| Geometry type | Count |
| --- | ---: |
| (none) | 0 |

### Output

| Metric | Value |
| --- | --- |
| Output features | 3,110 |
| SQL filter | `STUSPS NOT IN ('AK', 'AS', 'MP', 'HI', 'PR', 'VI')` |
| Garbage output | Not written (no dropped/changed features) |

## Upload

