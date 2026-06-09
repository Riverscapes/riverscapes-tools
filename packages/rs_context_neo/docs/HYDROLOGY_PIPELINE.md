# `hydrology.py` — D8 Hydrology Pipeline

**Location:** `rscontextneo/src/hydrology.py` (entry point), with step implementations in `taudem.py`, `wbt.py`, `level_path.py`, and `utils/rasters.py`

---

## Overview

The D8 hydrology pipeline takes a bare-earth DEM and derives a topologically correct stream network, Strahler stream-order raster, and subwatershed polygons through a **seven-step** processing chain.  All hydrological routing is based on the **D8 (deterministic eight-direction)** flow model, where each raster cell drains to exactly one of its eight neighbours — the one in the direction of steepest descent.

The outputs power downstream Riverscapes tools (BRAT, RME, etc.) and can be queried directly in any GIS.  Every step is idempotent: if its output file already exists and `force=False` the step is silently skipped, so re-running the pipeline after a crash or parameter change is cheap.

The pipeline is invoked through the single public function:

```python
from rscontextneo.src.hydrology import run_d8_hydrology

paths = run_d8_hydrology(
    dem_path      = '/project/topography/dem.tif',
    output_folder = '/project',
    threshold     = 50_000,   # cells; default
    breach_dist   = 100,      # cells; default
    cores         = 8,        # MPI ranks for TauDEM
    force         = False,
)
```

---

## Why Breach Before Pitremove?

Raw lidar-derived DEMs contain two classes of topographic depressions that block hydrological flow routing:

1. **Anthropogenic barriers** — road embankments, rail berms, levees, and other fill that was placed across the original valley floor.  These create artificial depressions by backing water up against the barrier.  The hydrologically correct behaviour is to *carve through* the barrier (simulating a culvert or bridge that would carry flow in reality).
2. **Natural sinks** — small concavities, pits, and artefacts left by interpolation or survey gaps.  These should usually be *filled* rather than carved.

WhiteboxTools `BreachDepressionsLeastCost` (Step 1a) addresses case 1 first: it traces a least-cost path through each depression and lowers ("breaches") the terrain along that path just enough for flow to escape, preferring carving where carving requires less vertical disturbance than filling.  The `--breach_dist` parameter caps how far the breach path may extend, preventing the algorithm from accidentally breaching natural ridges or lake outlets.

TauDEM `pitremove` (Step 1b) then runs on the *breach-conditioned* DEM, not the raw DEM.  Any remaining depressions after breaching are filled to the nearest overflow elevation.  Running pitremove on the raw DEM first would fill the anthropogenic barriers instead of carving through them, producing a DEM where flow routes *over* road embankments rather than *through* them — a common and significant source of drainage network error.

The two-step strategy therefore yields:

- **Correct flow routing** through culverts and bridges
- **Minimal terrain disturbance** — breaching is preferred over filling for man-made barriers
- **No spurious flat areas** — remaining sinks after breaching are filled cleanly by pitremove

---

## Step-by-Step Detail

### Step 1a — Breach Depressions (WhiteboxTools BreachDepressionsLeastCost)

**What it does:** Carves a least-cost drainage path through each topographic depression in the DEM.  For each closed depression the algorithm traces the shortest path (in terms of depth × distance) from the depression floor to the nearest downstream cell at or below the depression rim, then lowers all cells along that path to create a continuous downward gradient.

**Reads:** `topography/dem.tif` (the raw clipped DEM)

**Writes:** `hydrology/dem_breach.tif` (breach-conditioned DEM)

**Tool:** WhiteboxTools `breach_depressions_least_cost` via `rscontextneo.src.wbt`

**Tunable parameter:**
- `--breach_dist` (default `100`): Maximum search distance in cells for the breach path.  At 1 m resolution this equals metres.  Typical guidance:

  | Feature type | Approximate width at 1 m | Recommended breach_dist |
  |---|---|---|
  | Gravel / two-lane road | 10–15 m | 20–30 |
  | Highway with shoulders | 30–50 m | 60–80 |
  | Bridge approach embankment | 20–150 m | 100–200 |
  | Major highway / rail berm | up to 300 m | 200–350 |

  Scale proportionally for coarser DEMs (e.g. at 10 m resolution: `breach_dist = 10` ≈ 100 m).  Too large a value risks breaching natural terrain features; too small a value leaves anthropogenic barriers unfixed.

---

### Step 1b — Pit Removal (TauDEM pitremove)

**What it does:** Fills all remaining topographic depressions in the breach-conditioned DEM to their lowest overflow elevation, ensuring every cell has a defined downstream path to the domain boundary.

**Reads:** `hydrology/dem_breach.tif`

**Writes:** `hydrology/dem_filled.tif` (pit-filled, hydrologically enforced DEM)

**Tool:** TauDEM `PitRemove` via MPI (`mpiexec -n <cores> PitRemove`)

> **Why not the raw DEM?** As explained above, pitremove must operate on the breach-conditioned DEM so that it does not fill the man-made barriers that were deliberately carved in Step 1a.

---

### Step 2 — D8 Flow Directions and Slope (TauDEM d8flowdir)

**What it does:** For each cell in the pit-filled DEM, assigns a flow direction to the steepest downslope neighbour (encoded 1–8 clockwise from east) and records the slope gradient along that direction.

**Reads:** `hydrology/dem_filled.tif`

**Writes:**
- `hydrology/d8_flow.tif` — integer raster, values 1–8 (cardinal = 1,2,4,8; diagonal = 16,32,64,128 in some encodings; TauDEM uses 1–8 clockwise from east)
- `hydrology/d8_slope.tif` — float raster, dimensionless rise/run ratio

**Tool:** TauDEM `D8FlowDir` via MPI

> **Note on slope:** The D8 slope here is a *routing* intermediate — it measures gradient along the single steepest neighbour.  It is distinct from the topographic slope product at `topography/slope.tif`, which is derived from the raw DEM using Horn's eight-direction method and is in degrees.  Use `topography/slope.tif` for terrain analysis; `hydrology/d8_slope.tif` is an internal hydrology intermediate.

---

### Step 3 — D8 Contributing Area (TauDEM aread8)

**What it does:** Counts the number of upstream cells draining into each cell by following the D8 flow-direction grid from every cell downstream to the outlet.  The result is a flow accumulation raster.

**Reads:** `hydrology/d8_flow.tif`

**Writes:** `hydrology/d8_contributing_area.tif` — integer raster, values = upstream cell count

**Tool:** TauDEM `AreaD8` via MPI

**Converting cell counts to area:**

```
area_m² = cell_count × (cell_size_m)²
```

At 1 m resolution: 50 000 cells = 50 000 m² ≈ 0.05 km².  
At 3 m resolution: 50 000 cells = 50 000 × 9 m² = 450 000 m² ≈ 0.45 km².

---

### Step 4 — Stream Raster (TauDEM threshold)

**What it does:** Applies a threshold to the contributing-area raster to produce a binary stream mask: cells with contributing area ≥ `threshold` are classified as stream (value = 1); all others are non-stream (value = 0).

**Reads:** `hydrology/d8_contributing_area.tif`

**Writes:** `hydrology/stream_raster.tif` — binary raster (0/1)

**Tool:** TauDEM `Threshold` via MPI

**Tunable parameter:**
- `--threshold` (default `50000`): Minimum upstream cell count for stream classification.  Higher values → fewer, larger streams; lower values → more, smaller streams.  See [`docs/STREAM_THRESHOLD.md`](STREAM_THRESHOLD.md) for a detailed guide to choosing this value and for post-compute filtering techniques using the `USContArea` field.

---

### Step 5 — Stream Network Extraction (TauDEM streamnet)

**What it does:** Traces the D8 flow network along stream-raster cells to delineate individual reaches and their associated subwatersheds.  For each reach TauDEM computes topology (upstream / downstream links), Strahler order, length, slope, and contributing area.  Outputs are:

- A vector network (written directly to GeoPackage)
- A Strahler stream-order raster
- A subwatershed raster (one integer value per reach, equal to `LINKNO`)
- Auxiliary text files (`stream_tree.dat`, `stream_coord.dat`) used internally by TauDEM

**Reads:**
- `hydrology/d8_flow.tif`
- `hydrology/dem_filled.tif`
- `hydrology/d8_contributing_area.tif`
- `hydrology/stream_raster.tif`

**Writes:**
- `hydrology/hydro_derivatives.gpkg` — GeoPackage with layer `network_intersected` (LineString features, one per reach)
- `hydrology/stream_order.tif` — Strahler stream-order raster
- `hydrology/subwatersheds.tif` — subwatershed integer raster
- `hydrology/stream_tree.dat`, `hydrology/stream_coord.dat` — TauDEM internal text outputs

**Tool:** TauDEM `StreamNet` via MPI

See [`docs/STREAM_NETWORK_FIELDS.md`](STREAM_NETWORK_FIELDS.md) for a full reference of every field written to `network_intersected`.

---

### Step 6 — Vectorise Subwatersheds (GDAL Polygonize)

**What it does:** Converts the integer subwatershed raster to a vector polygon layer in the same GeoPackage as the stream network.  Each contiguous group of cells with the same `WSNO` value becomes one polygon feature.  Because TauDEM assigns exactly one subwatershed per reach, the resulting layer has one polygon per reach, and the `WSNO` field acts as the foreign key joining the two layers.

**Reads:** `hydrology/subwatersheds.tif`

**Writes:** `hydrology/hydro_derivatives.gpkg` — adds layer `subwatersheds` (MultiPolygon features)

**Tool:** GDAL `gdal.Polygonize` (OGR) via `rscontextneo.src.taudem.vectorize_subwatersheds`

**Joining the two layers:**

```sql
SELECT s.geom, n.strmOrder, n.USContArea, n.Slope
FROM   subwatersheds s
JOIN   network_intersected n ON s.WSNO = n.LINKNO
```

---

### Step 7 — Level Path Calculation

**What it does:** Traverses the stream network from headwaters downstream and assigns a unique integer `level_path` identifier to each reach.  Reaches that share a level path form a single continuous flow line from a headwater to an outlet (or to the point where a longer tributary takes over).  The main stem gets the lowest value; tributary paths are assigned in descending order of their total flow-path length.

The algorithm:
1. Finds all headwater reaches (`USLINKNO1 = -1`).
2. Computes cumulative flow-path length from each headwater to the network outlet.
3. Processes headwaters longest-first so the main stem is stamped before shorter tributaries try to merge into it.
4. Walks downstream from each headwater, writing the level-path value into any reach whose `level_path` column is still `NULL`.

**Reads:** `network_intersected` layer in `hydrology/hydro_derivatives.gpkg` (specifically `LINKNO`, `DSLINKNO`, `USLINKNO1`, `Length` columns)

**Writes:** `level_path` column in `network_intersected` (updated in-place)

**Tool:** `rscontextneo.src.level_path.calc_level_paths`

---

## Output File Reference

All paths are relative to the project `output_folder`.

| Path | Format | Description |
|------|--------|-------------|
| `hydrology/dem_breach.tif` | GeoTIFF (Float32) | Breach-conditioned DEM (Step 1a) |
| `hydrology/dem_filled.tif` | GeoTIFF (Float32) | Pit-filled, hydrologically enforced DEM (Step 1b) |
| `hydrology/d8_flow.tif` | GeoTIFF (Int16) | D8 flow direction (1–8 encoding, Step 2) |
| `hydrology/d8_slope.tif` | GeoTIFF (Float32) | D8 slope (dimensionless rise/run, Step 2) |
| `hydrology/d8_contributing_area.tif` | GeoTIFF (Int32) | D8 contributing area in cells (Step 3) |
| `hydrology/stream_raster.tif` | GeoTIFF (Int16) | Binary stream mask: 1 = stream, 0 = non-stream (Step 4) |
| `hydrology/stream_order.tif` | GeoTIFF (Int16) | Strahler stream-order raster (Step 5) |
| `hydrology/subwatersheds.tif` | GeoTIFF (Int32) | Subwatershed integer raster — one value per reach (Step 5) |
| `hydrology/hydro_derivatives.gpkg` | GeoPackage | Vector stream network (`network_intersected`) and subwatershed polygons (`subwatersheds`) — layers added by Steps 5, 6, and 7. `network_intersected` also carries the `level_path` column populated by Step 7. |
| `hydrology/stream_tree.dat` | Text | TauDEM internal network topology file (Step 5) |
| `hydrology/stream_coord.dat` | Text | TauDEM internal coordinate file (Step 5) |

---

## Idempotency and Caching

Every step checks whether its output file already exists before running.  The skip logic is:

| Condition | Behaviour |
|-----------|-----------|
| Output file exists and `force=False` | Step is skipped; a log message notes the skip |
| Output file is missing | Step runs unconditionally |
| `force=True` | All steps run regardless of whether outputs exist |

For the vector layers in `hydro_derivatives.gpkg`, the check is layer-level: the step is skipped if the GeoPackage exists **and** the target layer name is present in it.

**Partial re-runs:** Because each step is individually idempotent, you can delete a single intermediate and re-run to regenerate it (and all downstream outputs) without re-running earlier steps.  For example, to re-run with a different threshold (set in the config profile's `hydrology_threshold` parameter):

1. Delete `hydrology/stream_raster.tif` (Step 4 output)
2. Delete `hydrology/stream_order.tif`, `hydrology/subwatersheds.tif`, `hydrology/stream_tree.dat`, `hydrology/stream_coord.dat` (Step 5 outputs)
3. Remove the `network_intersected` layer from `hydrology/hydro_derivatives.gpkg` (Step 5 vector output)
4. Remove the `subwatersheds` layer from `hydrology/hydro_derivatives.gpkg` (Step 6 vector output)
5. Re-run with the updated config profile

Step 7 (level paths) will also re-run automatically whenever `network_intersected` is recreated, because the `level_path` column will be absent.

Alternatively, pass `--force` to unconditionally re-run the entire pipeline from scratch.

---

## Key Assumptions

| # | Assumption |
|---|------------|
| 1 | The input DEM is in a projected CRS (e.g. NAD83 / UTM) with horizontal and vertical units both in metres. |
| 2 | The DEM has been clipped to the AOI.  Processing time and memory scale with raster extent; very large DEMs may require more MPI cores. |
| 3 | `breach_dist` is specified in cells, not metres.  Scale for coarser DEMs: `breach_dist_cells = desired_distance_m / cell_size_m`. |
| 4 | TauDEM is installed and `mpiexec` is on `$PATH`.  Core count is read from the `TAUDEM_CORES` environment variable or falls back to 2. |
| 5 | WhiteboxTools (`whitebox_tools` binary) is installed and on `$PATH`. |
| 6 | The `threshold` parameter is in cells.  Post-compute filtering by raising the effective threshold is supported via the `USContArea` field without re-running the pipeline. |
| 7 | TauDEM `streamnet` writes its vector output to a temporary shapefile (`_tmp.shp` alongside the GeoPackage path); `rs_context_neo` converts it to GeoPackage with GDAL `VectorTranslate` and then deletes the temporary shapefile. Only the GeoPackage persists after the step. |
| 8 | `WSNO` equals `LINKNO` for every reach.  This is a TauDEM guarantee, not a derived property. |
| 9 | Flat areas after pit-filling are resolved by TauDEM's default flat-area routing.  No explicit flat-area handling step is needed. |
| 10 | A failed step raises an exception and aborts the pipeline.  Partial outputs from the failed step may exist on disk; delete them before re-running if TauDEM or WBT leave incomplete files. |
