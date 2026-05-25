# Development Log — rs_context_neo

## 2026-05-25 — Mutually exclusive input args (--huc / --extents / --dem)
**Status:** ✅  |  **Iterations:** 3w/2r
**Files changed:** rscontextneo/rs_context_neo.py, .vscode/launch.json
**Notes:** Replaced positional `huc` with a required mutually-exclusive argparse group; function made keyword-only with runtime guard and unreachable-else for static analysis; launch.json updated to new flag; iteration 2 fixed unbound-variable defect and double-logging; iteration 3 fixed `parse_metadata(None)` risk.
