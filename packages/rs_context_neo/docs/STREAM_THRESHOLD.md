# Stream Network Threshold — How It Works and How to Choose a Value

## What the threshold controls

The `--threshold` parameter is the **minimum number of upstream cells** that must drain through a cell for it to be classified as a stream. It is passed directly to TauDEM's `threshold` command (Step 4 of the D8 hydrology chain) and controls how dense or sparse the computed stream network is.

| Threshold value | Effect |
|---|---|
| **Low** (e.g. 5,000) | Dense, fine-grained network — captures small headwater tributaries |
| **High** (e.g. 200,000) | Sparse, coarse network — only major channels appear |

The default is **50,000 cells**. At 1-metre resolution that corresponds to a contributing area of roughly 0.05 km², which is a reasonable starting point for most mountain and foothill landscapes. You will almost certainly need to adjust this for your specific DEM and region.

---

## What gets stored per reach

TauDEM's `streamnet` command writes two contributing-area fields to every feature in `hydrology/hydrology.gpkg`:

| Field | Description |
|---|---|
| `USContArea` | Contributing area in cells at the **upstream end** of the reach |
| `DSContArea` | Contributing area in cells at the **downstream end** of the reach |

`USContArea` represents the *minimum* accumulated flow anywhere along the reach — it is the upstream tip, where the least water has collected. `DSContArea` is the value at the outlet end, where the most water has collected.

The threshold you specify at run time sets a hard floor on `USContArea`: no reach in the network will have `USContArea` below your threshold value, because TauDEM only initiates a stream where contributing area first crosses that threshold.

---

## The key insight: threshold as a compute limit, not a display limit

Because `USContArea` is stored per-reach, the threshold you use at run time does **not** have to be the same as the threshold you use for display or analysis. The recommended workflow is:

1. **Run once with a low threshold** (fine network) — this captures all the detail you might ever want.
2. **Filter at display or analysis time** using `USContArea >= DISPLAY_THRESHOLD` — this lets you dial the density up or down without rerunning the compute.

For example, if you run with `--threshold 10000` you can later display only reaches where `USContArea >= 50000` to get a coarser view, or `USContArea >= 10000` to see everything. You can never go finer than your original compute threshold without re-running.

Think of the compute threshold as setting the **ceiling on detail** you have available. Any display threshold you apply afterward must be ≥ the compute threshold.

---

## How to choose the compute threshold

### Rule of thumb by DEM resolution

These are rough starting points. Inspect the stream raster against a hillshade for your specific landscape and adjust. Contributing area in m² = cell count × (cell size in metres)².

| DEM resolution | Suggested starting threshold | Approx. contributing area |
|---|---|---|
| 1 m | 10,000 – 50,000 cells | 0.01 – 0.05 km² |
| 3 m | 5,000 – 20,000 cells | 0.045 – 0.18 km² |
| 10 m | 500 – 5,000 cells | 0.05 – 0.5 km² |

### Practical guidance

**Start low if you want filtering flexibility later.** If you are building a dataset that multiple downstream tools or viewers will consume, choose a threshold that captures small headwater channels. Users can always filter to a coarser view; they cannot recover reaches that were never computed.

**Inspect the stream raster before committing.** After Step 4 runs, open `hydrology/stream_raster.tif` in a GIS alongside the DEM hillshade. If streams are appearing on hillslopes, ridge lines, or flat agricultural fields they shouldn't be on, the threshold is too low. If obvious valley channels are missing, it is too high.

**Consider the landscape.** Arid or low-relief landscapes tend to need higher thresholds (larger contributing areas before channelisation begins). Steep, high-relief terrain typically channels at smaller areas.

**Consider your downstream use.** If a downstream tool filters by `USContArea >= 50000`, run with `--threshold 10000` or lower so those tools have something to filter against. If you run with `--threshold 50000` and a downstream filter also uses 50,000, you get the full network — but you have no headroom to display a finer view.

---

## Filtering in practice

Once the network is computed, you can filter in any OGR/GDAL-capable tool using standard SQL or field expressions:

**QGIS layer filter:**
```
"USContArea" >= 50000
```

**ogr2ogr / SQL:**
```bash
ogr2ogr \
  -sql "SELECT * FROM network WHERE USContArea >= 50000" \
  filtered_network.gpkg \
  hydrology.gpkg
```

**Python / Fiona:**
```python
import fiona

with fiona.open('hydrology.gpkg', layer='network') as src:
    reaches = [f for f in src if f['properties']['USContArea'] >= 50_000]
```

**Python / GeoPandas:**
```python
import geopandas as gpd

net = gpd.read_file('hydrology.gpkg', layer='network')
coarse = net[net['USContArea'] >= 50_000]
```

---

## Project metadata

The threshold used at run time is recorded in the RS Context Neo project XML under two metadata keys:

| Key | Notes |
|---|---|
| `StreamThreshold` | Hidden field — machine-readable value in cells |
| `Stream Threshold` | Human-readable label shown in project viewers |

Downstream tools that read this project can use `StreamThreshold` to know the finest network available and set sensible default display filters accordingly.

---

## Summary

| Question | Answer |
|---|---|
| What does `--threshold` do? | Sets the minimum contributing-area cell count for stream classification (TauDEM Step 4) |
| Where is it stored per reach? | `USContArea` (upstream end) and `DSContArea` (downstream end) in `hydrology.gpkg` |
| Can I filter to a coarser network after the fact? | Yes — filter on `USContArea >= X` for any X ≥ compute threshold |
| Can I recover a finer network without rerunning? | No — the compute threshold is a hard floor on detail |
| What is a good default? | 50,000 cells at 1 m resolution; inspect the stream raster and adjust |
| Should I run fine or coarse? | Fine (lower threshold) if you want flexibility; coarse if compute time or storage is a concern |
