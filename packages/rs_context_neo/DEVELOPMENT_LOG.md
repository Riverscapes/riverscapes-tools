# Development Log — rs_context_neo

## 2026-05-25 — Mutually exclusive input args (--huc / --extents / --dem)
**Status:** ✅  |  **Iterations:** 3w/2r
**Files changed:** rscontextneo/rs_context_neo.py, .vscode/launch.json
**Notes:** Replaced positional `huc` with a required mutually-exclusive argparse group; function made keyword-only with runtime guard and unreachable-else for static analysis; launch.json updated to new flag; iteration 2 fixed unbound-variable defect and double-logging; iteration 3 fixed `parse_metadata(None)` risk.

## 2026-05-27 — Documentation and consistency pass
**Status:** ✅  |  **Iterations:** 2w/2r
**Files changed:** rscontextneo/src/taudem.py, rscontextneo/rs_context_neo.py, rscontextneo/src/fetch_dem.py, rscontextneo/src/hydrology.py, rscontextneo/src/huc.py, docs/STREAM_NETWORK_FIELDS.md, README.md, docs/HYDROLOGY_PIPELINE.md (new)
**Notes:** Fixed 10 issues: wrong log text ("Catchment Wings"), `_fetch_3dep` return annotation tuple[str,str]→tuple[str,str,str], `return False`→`return 0.0` in _compute_tile_coverage, hydrology.py docstring step order swap + wrong gpkg path, missing `breach_dist` param in docstring, stale layer/path refs in STREAM_NETWORK_FIELDS.md, huc.py unused-module note, step-numbered log.info calls, README rewrite; iteration 2 fixed topography/slope.tif wrongly listed in hydrology docstring, one remaining stale `network` layer name in STREAM_NETWORK_FIELDS.md prose, and factually wrong HYDROLOGY_PIPELINE.md assumption about the streamnet temp shapefile.

## 2026-05-26 — Align rs_context_neo XML output with RSContext template
**Status:** ✅  |  **Iterations:** 3w/3r
**Files changed:** rscontextneo/rs_context_neo.py, rscontextneo/src/hydrology.py, rscontextneo/src/taudem.py
**Notes:** Scout phase identified 5 alignment targets; iterations 2+3 fixed TILE_FOOTPRINTS/HILLSHADE unconditional-registration bugs and missing sub_layers on Geopackage RSLayer.
