# Stream Network GeoPackage — Field Reference

`hydrology/hydrology.gpkg` is the primary vector output of RS Context Neo. It
contains two layers:

| Layer | Geometry | Description |
|---|---|---|
| `network` | LineString | One feature per stream reach (TauDEM `streamnet`) |
| `subwatersheds` | MultiPolygon | One polygon per subwatershed, matching each reach (polygonised from `subwatersheds.tif`) |

The two layers share a common join key: `WSNO` / `LINKNO` in `network` equals
`WSNO` in `subwatersheds`, so you can join drainage polygons to their outlet
reach directly.

The GeoPackage is in the projected CRS of the input DEM (e.g. NAD83 / UTM).
All distance and area values are in the linear units of that CRS (metres for
UTM projections).

---

## Quick reference

| Field | Type | Units | Description |
|---|---|---|---|
| `LINKNO` | Integer | — | Unique reach identifier |
| `DSLINKNO` | Integer | — | Downstream reach (`-1` = outlet) |
| `USLINKNO1` | Integer | — | First upstream reach (`-1` = headwater) |
| `USLINKNO2` | Integer | — | Second upstream reach (`-1` = none) |
| `DSNODEID` | Integer | — | TauDEM internal node ID (not used downstream) |
| `strmOrder` | Integer | — | Strahler stream order |
| `Length` | Real | metres | Channel path length along D8 grid |
| `Magnitude` | Integer | reaches | Number of upstream headwater reaches |
| `USContArea` | Real | cells | Contributing area at the upstream end |
| `DSContArea` | Real | cells | Contributing area at the downstream end |
| `strmDrop` | Real | metres | Elevation drop from upstream to downstream end |
| `Slope` | Real | m/m | Average channel gradient (strmDrop / Length) |
| `StraightL` | Real | metres | Straight-line distance between reach endpoints |
| `WSNO` | Integer | — | Subwatershed ID (matches `subwatersheds.tif` values) |
| `DOUTEND` | Real | metres | Distance from downstream end to watershed outlet |
| `DOUTSTART` | Real | metres | Distance from upstream end to watershed outlet |
| `DOUTMID` | Real | metres | Distance from reach midpoint to watershed outlet |

---

## Field details

### `LINKNO` — Reach identifier
**Type:** Integer | **Range (example dataset):** 0 – 1259

A unique integer assigned to each reach by TauDEM during the `streamnet` step.
The numbering is not sequential along the network; it reflects the internal order
in which TauDEM visits cells during watershed delineation.

Used as the join key between:
- `DSLINKNO` / `USLINKNO1` / `USLINKNO2` on other reaches (topology)
- `WSNO` on this reach (links to the subwatershed raster)

---

### `DSLINKNO` — Downstream reach identifier
**Type:** Integer | **Special value:** `-1` = this reach is a watershed outlet

The `LINKNO` of the reach immediately downstream. Together with `USLINKNO1` and
`USLINKNO2` this forms a complete binary-tree topology of the network.

To trace a flow path downstream, follow `DSLINKNO` until you reach `-1`.

---

### `USLINKNO1` — First upstream reach identifier
### `USLINKNO2` — Second upstream reach identifier
**Type:** Integer | **Special value:** `-1` = no upstream reach in that slot

The `LINKNO` values of the one or two reaches that flow into the upstream end of
this reach.

| `USLINKNO1` | `USLINKNO2` | Meaning |
|---|---|---|
| `-1` | `-1` | Headwater reach — no upstream tributaries |
| `N` | `-1` | Single upstream reach (un-branched channel) |
| `N` | `M` | Confluence — two reaches join here |

TauDEM's `streamnet` only produces binary confluences. Where three or more
channels meet at a single cell the network is split into pairs.

---

### `DSNODEID` — Downstream node identifier
**Type:** Integer64 | **Value in practice:** always `-1`

An internal TauDEM node identifier written to the shapefile but not populated
with meaningful values in current TauDEM releases. It can be ignored for
analysis purposes.

---

### `strmOrder` — Strahler stream order
**Type:** Integer | **Range (example dataset):** 1 – 5

Strahler stream order is a classical measure of channel position in the network
hierarchy, calculated by TauDEM as part of the `streamnet` step:

- A headwater reach (no tributaries) has order **1**.
- Where two reaches of order *n* meet, the downstream reach has order **n + 1**.
- Where two reaches of *different* orders meet, the downstream reach takes the
  **higher** of the two orders (not incremented).

Higher order = larger, more downstream river. Order 1 = smallest headwater
channels; the highest order in a dataset is the main stem.

**Use cases:** colouring or filtering the network by size; identifying mainstem
vs. tributary channels; Strahler-based drainage density analysis.

---

### `Length` — Channel path length
**Type:** Real | **Units:** metres | **Range (example dataset):** 0 – 1,915 m

The length of the reach measured along the D8 flow path through the raster
grid — i.e. the sum of D8 step distances (1.0 m for cardinal steps, √2 m for
diagonal steps at 1 m resolution). This is the *channel* distance, not the
straight-line distance between endpoints.

Zero-length reaches can appear at the edge of the domain or where TauDEM
creates a degenerate segment during network extraction.

---

### `Magnitude` — Upstream headwater count
**Type:** Integer | **Range (example dataset):** 1 – 403

The number of first-order (headwater) reaches in the upstream network,
including the reach itself if it is a headwater. Equivalently, the number of
stream sources that drain through this reach before reaching the outlet.

Magnitude grows by 1 at every confluence where a first-order stream joins.
For confluences of higher-order reaches the magnitude is simply the sum of
the two upstream magnitudes.

**Use cases:** a proxy for network complexity or watershed size that is
independent of cell resolution (unlike contributing area).

---

### `USContArea` — Upstream contributing area (upstream end)
**Type:** Real | **Units:** cells | **Range (example dataset):** 50,000 – 82,629,064

The D8 contributing area at the **upstream tip** of the reach — the number of
raster cells that drain through that point. This is taken directly from the
`d8_contributing_area.tif` raster at the first cell of the reach.

Because TauDEM only starts a stream where contributing area first reaches the
`--threshold`, `USContArea` will always be ≥ `StreamThreshold`.

**Converting to area:**

```
contributing area (m²) = USContArea × (cell_size_m)²
```

At 1 m resolution: `USContArea` m² = `USContArea` cells (1 cell = 1 m²).
At 3 m resolution: `USContArea` m² = `USContArea × 9`.

**Primary use case — dynamic display filtering:**  
Filter the network to a coarser density by raising the threshold at display
time without rerunning the model:

```sql
-- Show only reaches where ≥ 200,000 cells drain through the upstream tip
SELECT * FROM network WHERE USContArea >= 200000
```

See [STREAM_THRESHOLD.md](STREAM_THRESHOLD.md) for a full explanation of how
to use `USContArea` for post-compute filtering.

---

### `DSContArea` — Downstream contributing area (downstream end)
**Type:** Real | **Units:** cells | **Range (example dataset):** 50,050 – 82,761,344

The D8 contributing area at the **downstream end** of the reach. Always ≥
`USContArea` because the downstream end has accumulated all the drainage from
the reach itself plus any side-drainage along its length.

The difference `DSContArea − USContArea` represents the incremental drainage
area added along the reach (cells that drain directly into the channel between
the upstream junction and the downstream junction, not through a tributary).

---

### `strmDrop` — Elevation drop
**Type:** Real | **Units:** metres | **Range (example dataset):** 0 – 506 m

The difference in elevation (from the pit-filled DEM) between the upstream end
and the downstream end of the reach:

```
strmDrop = elevation(upstream_end) − elevation(downstream_end)
```

Zero values occur for very short reaches or reaches on essentially flat terrain.
Negative values should not occur; if they do it indicates a DEM artefact that
survived pit-filling.

---

### `Slope` — Average channel gradient
**Type:** Real | **Units:** m/m (dimensionless) | **Range (example dataset):** 0 – 0.597

Average channel gradient calculated as:

```
Slope = strmDrop / Length
```

A slope of 0.01 means the channel drops 1 m per 100 m of channel length (1%).
Zero slope occurs when `strmDrop = 0` or `Length = 0`.

**Note:** This is the mean gradient over the entire reach. It does not capture
within-reach variability (e.g. a reach that is mostly flat with one steep
cascade will show an intermediate average).

---

### `StraightL` — Straight-line reach length
**Type:** Real | **Units:** metres | **Range (example dataset):** 0 – 1,676 m

The Euclidean (straight-line) distance between the upstream endpoint and the
downstream endpoint of the reach.

**Sinuosity** can be derived as:

```
sinuosity = Length / StraightL
```

A sinuosity of 1.0 is a perfectly straight reach; higher values indicate a
more sinuous channel. Values < 1.0 should not occur and indicate a data
artefact (e.g. a zero-length or degenerate reach).

---

### `WSNO` — Subwatershed number
**Type:** Integer | **Range (example dataset):** 0 – 1,259

The identifier of the subwatershed draining to this reach. Matches the cell
values in `hydrology/subwatersheds.tif` — every raster cell in a given
subwatershed has the same `WSNO` value as the reach it drains to.

`WSNO` is always equal to `LINKNO` for the corresponding reach (TauDEM assigns
one subwatershed per reach). Use it to spatially join subwatershed raster cells
to their outlet reach.

---

### `DOUTEND` — Distance to outlet (downstream end)
**Type:** Real | **Units:** metres | **Range (example dataset):** 0 – 19,052 m

Distance along the D8 flow path from the **downstream end** of this reach to
the watershed outlet. The outlet reach has `DOUTEND = 0`.

---

### `DOUTSTART` — Distance to outlet (upstream end)
**Type:** Real | **Units:** metres | **Range (example dataset):** 238 – 19,177 m

Distance along the D8 flow path from the **upstream end** of this reach to the
watershed outlet. Always greater than `DOUTEND` by approximately `Length`.

---

### `DOUTMID` — Distance to outlet (midpoint)
**Type:** Real | **Units:** metres | **Range (example dataset):** 119 – 19,114 m

Distance along the D8 flow path from the **midpoint** of this reach to the
watershed outlet. Approximately the average of `DOUTEND` and `DOUTSTART`.

**Use cases for DOUT* fields:** ordering reaches from headwater to outlet;
computing position-in-network metrics; identifying how far upstream any given
reach is from the pour point.

---

## Derived fields you can compute

These are not stored in the GeoPackage but can be calculated from the fields
above:

| Derived metric | Formula | Units |
|---|---|---|
| Sinuosity | `Length / StraightL` | dimensionless |
| Contributing area (m²) at upstream end | `USContArea × cell_size_m²` | m² |
| Contributing area (km²) at upstream end | `USContArea × cell_size_m² / 1e6` | km² |
| Incremental drainage area | `DSContArea − USContArea` | cells |
| Distance from outlet midpoint | `DOUTMID` | metres |

---

## Topology diagram

```
                    USLINKNO1 (order 1)
                         \
                          \
  USLINKNO2 (order 1) ────╂──── this reach (LINKNO, order 2) ────► DSLINKNO
                          │
                    confluence node
```

`USLINKNO2 = -1` for headwater reaches (no second upstream).  
`DSLINKNO = -1` for the watershed outlet reach(es).

---

## Notes on zero-value fields

Several fields can legitimately be zero:

| Field | Why it can be zero |
|---|---|
| `Length` | Degenerate reach created at the domain edge by TauDEM |
| `strmDrop` | Reach lies on essentially flat terrain |
| `Slope` | Either `strmDrop = 0` or `Length = 0` |
| `StraightL` | Degenerate reach (same start and end point) |
| `DOUTEND` | This reach flows directly to the watershed outlet |

---

## `subwatersheds` layer fields

Produced by `gdal.Polygonize` applied to `hydrology/subwatersheds.tif`.
Each polygon is the drainage area that flows to the corresponding stream reach.

| Field | Type | Description |
|---|---|---|
| `fid` | Integer | OGR auto-assigned feature identifier |
| `WSNO` | Integer | Subwatershed / watershed number — joins to `LINKNO` and `WSNO` in the `network` layer |

### `WSNO` in the subwatersheds layer

The raster value from `subwatersheds.tif` is preserved directly as `WSNO`.
Because TauDEM assigns one unique value per reach (equal to that reach's
`LINKNO`), this field is the natural foreign key for joining the two layers:

```sql
-- Join subwatershed polygons to their outlet reach
SELECT s.geom, n.strmOrder, n.USContArea, n.Slope
FROM   subwatersheds s
JOIN   network n ON s.WSNO = n.LINKNO
```


All fields are written directly by TauDEM `streamnet`. No post-processing
modifies field values; only the output format is changed (shapefile → GeoPackage
via GDAL `VectorTranslate`).

TauDEM documentation: <https://hydrology.usu.edu/taudem/taudem5/documentation.html>
