
## 2026-05-27 — data_footprints layer in tile footprints GeoPackage
**Status:** ✅  |  **Iterations:** 3w/3r
**Files changed:** packages/rs_context_neo/rscontextneo/src/fetch_dem.py
**Notes:** Added `data_footprints` wkbMultiPolygon layer (non-nodata pixel outlines via numpy mask + gdal.Polygonize + shapely unary_union) alongside existing `tile_footprints` bbox layer; three rounds of null-safety fixes required (CreateGeometryFromWkt/ForceTo/Transform return codes, np.isnan on int dtype, GetGeometryRef on Polygonize output).
