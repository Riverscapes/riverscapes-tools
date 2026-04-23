"""Orchestrate NHDPlus HR National Release 2 → Parquet/GeoParquet for Athena.

Multi-layer pipeline that reads from a FileGDB and produces Athena-ready
Parquet files, partitioned by vpuid for large layers.

Designed for incremental development:
  - Pass --vpuids 1707,1708 to process only a subset (fast iteration)
  - Omit --vpuids to process everything (production run)
  - Steps can be run individually or all at once

Running this script requires osgeo (GDAL Python bindings), which are NOT
installed in the uv venv — they are system-installed at:
  /usr/local/lib/python3.12/dist-packages

Set PYTHONPATH before running so Python can find them:
  export PYTHONPATH=/usr/local/lib/python3.12/dist-packages:$PYTHONPATH

Then run normally from the repo root:
  uv run --extra geoparquet vector_prep/orchestrate-nhdplushr.py --vpuids 1707,1708

Or one-liner without exporting:
  PYTHONPATH=/usr/local/lib/python3.12/dist-packages uv run --extra geoparquet python orchestrate-nhdplushr.py --vpuids 1707,1708

See usgs_nhdplushr/runlog-nhdplushr-nr2.md for decisions and context.
- Lorin, April 2026
"""

import argparse
import io
import json
from dataclasses import dataclass, field
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rsxml import Logger

from fetch_fgdb_metadata import fetch_columns_from_fgdb, update_layer_definitions
from vector_prep import vector_prep, clean_geometries

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_INPUTS_JSON = Path(__file__).parent / "usgs_nhdplushr" / "inputs.json"


@dataclass
class NHDConfig:
    """Top-level config loaded from inputs.json."""
    source_category: str
    source_title: str
    source_url: str
    doi: str
    publication_date: str
    snapshot_id: str
    input_vector_path: str
    data_prep_operator: str
    special_notes: str
    layers: list[dict] = field(default_factory=list)

    @classmethod
    def from_json(cls, path: str | Path) -> "NHDConfig":
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return cls(**data)


def _repo_root() -> Path:
    return next(p for p in Path(__file__).resolve().parents if (p / "pyproject.toml").exists())


def _dist_dir(cfg: NHDConfig) -> Path:
    source_stub = "usgov_sources" if cfg.source_category == "usgov" else f"raw_{cfg.source_category}"
    d = _repo_root() / "dist" / "usgs_nhdplushr" / source_stub
    d.mkdir(parents=True, exist_ok=True)
    return d


def _layer_cfg(cfg: NHDConfig, gdb_layer_name: str) -> dict:
    """Get the layer config dict from inputs.json for a given GDB layer name."""
    for lyr in cfg.layers:
        if lyr["gdb_layer_name"] == gdb_layer_name:
            return lyr
    raise LookupError(f"Layer '{gdb_layer_name}' not found in inputs.json")


# ---------------------------------------------------------------------------
# Reading helpers — geopandas with ignore_geometry for attrs-only reads
# ---------------------------------------------------------------------------
def read_gdb_attrs(gdb_path: str, layer_name: str, vpuids: list[str] | None = None) -> pd.DataFrame:
    """Read a GDB layer into a pandas DataFrame (no geometry).

    Uses geopandas with ignore_geometry=True which delegates filtering and
    columnar reading to GDAL's Arrow batch interface — much faster than
    row-by-row OGR feature iteration for large layers.
    """
    log = Logger("read_gdb_attrs")
    where = None
    if vpuids:
        where = " OR ".join(f"vpuid = '{v}'" for v in vpuids)
        log.info(f"Filtering {layer_name} where: {where}")

    log.info(f"Reading {layer_name} (attrs only, where={where})...")
    df = gpd.read_file(gdb_path, layer=layer_name, where=where, ignore_geometry=True)
    log.info(f"Read {len(df):,} rows from {layer_name}")
    return df


def read_gdb_layer_gpd(gdb_path: str, layer_name: str, vpuids: list[str] | None = None) -> gpd.GeoDataFrame:
    """Read a spatial GDB layer into a GeoDataFrame, optionally filtered by vpuid."""
    log = Logger("read_gdb_gpd")
    where = None
    if vpuids:
        where = " OR ".join(f"vpuid = '{v}'" for v in vpuids)
        log.info(f"Filtering {layer_name} to vpuids: {vpuids}")

    log.info(f"Reading {layer_name} with geopandas (where={where})...")
    gdf = gpd.read_file(gdb_path, layer=layer_name, where=where)
    log.info(f"Read {len(gdf):,} features from {layer_name}. CRS: {gdf.crs}")
    return gdf


# ---------------------------------------------------------------------------
# NHDPlusFlow read helper — uses fromvpuid/tovpuid instead of vpuid
# ---------------------------------------------------------------------------
def read_nhdplusflow(gdb_path: str, vpuids: list[str] | None = None) -> pd.DataFrame:
    """Read NHDPlusFlow table, filtering by fromvpuid/tovpuid if vpuids given."""
    log = Logger("read_nhdplusflow")
    where = None
    if vpuids:
        # Include edges where either endpoint is in our VPU set
        clauses = []
        for v in vpuids:
            clauses.append(f"fromvpuid = '{v}'")
            clauses.append(f"tovpuid = '{v}'")
        where = " OR ".join(clauses)
        log.info(f"Filtering NHDPlusFlow where: {where}")

    log.info(f"Reading NHDPlusFlow (where={where})...")
    df = gpd.read_file(gdb_path, layer="NHDPlusFlow", where=where, ignore_geometry=True)
    log.info(f"Read {len(df):,} rows from NHDPlusFlow")
    return df


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------
def export_df_to_parquet(df: pd.DataFrame, output_path: Path, sort_by: list[str] | None = None) -> Path:
    """Export a pandas DataFrame to Snappy-compressed Parquet."""
    log = Logger("export_parquet")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if sort_by:
        # Only sort by columns that exist
        sort_cols = [c for c in sort_by if c in df.columns]
        if sort_cols:
            log.info(f"Sorting by {sort_cols}")
            df = df.sort_values(sort_cols, ignore_index=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(output_path), compression="snappy")
    log.info(f"Wrote {len(df):,} rows to {output_path} ({output_path.stat().st_size / 1e6:.1f} MB)")
    return output_path


def export_gdf_to_geoparquet(
    gdf: gpd.GeoDataFrame,
    output_path: Path,
    orig_bounds: pd.DataFrame | None = None,
) -> Path:
    """Export GeoDataFrame to Snappy-compressed GeoParquet with geometry_bbox.

    Adds a struct bbox column for Athena spatial predicate push-down.
    If orig_bounds (a DataFrame with minx/miny/maxx/maxy columns in EPSG:4326)
    is provided, bbox is computed from those rather than the current geometry
    — use this when geometry has already been simplified and you want the bbox
    to reflect the original extents.

    Caller is responsible for reprojecting to EPSG:4326 and stripping Z before
    calling this function.
    """
    log = Logger("export_geoparquet")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        log.warning("GeoDataFrame is not in EPSG:4326 — reprojecting (prefer reprojecting before calling this)")
        gdf = gdf.to_crs(epsg=4326)

    # Idempotency
    if "geometry_bbox" in gdf.columns:
        gdf = gdf.drop(columns=["geometry_bbox"])

    # Use caller-supplied original bounds if provided, else derive from current geometry
    bounds = orig_bounds if orig_bounds is not None else gdf.geometry.bounds
    log.info(f"geometry_bbox source: {'original (unsimplified)' if orig_bounds is not None else 'current geometry'}")

    # Write via geopandas to get proper `geo` metadata, then append bbox
    buf = io.BytesIO()
    gdf.to_parquet(buf, compression="snappy", index=False)
    buf.seek(0)
    table = pq.read_table(buf)

    bbox_type = pa.struct([
        pa.field("xmin", pa.float32()),
        pa.field("ymin", pa.float32()),
        pa.field("xmax", pa.float32()),
        pa.field("ymax", pa.float32()),
    ])
    bbox_col = pa.StructArray.from_arrays(
        [
            pa.array(bounds["minx"].to_numpy(dtype="float32"), type=pa.float32()),
            pa.array(bounds["miny"].to_numpy(dtype="float32"), type=pa.float32()),
            pa.array(bounds["maxx"].to_numpy(dtype="float32"), type=pa.float32()),
            pa.array(bounds["maxy"].to_numpy(dtype="float32"), type=pa.float32()),
        ],
        fields=list(bbox_type),
    )
    table = table.append_column(pa.field("geometry_bbox", bbox_type), bbox_col)

    pq.write_table(table, str(output_path), compression="snappy")
    log.info(f"Wrote {len(gdf):,} features to {output_path} ({output_path.stat().st_size / 1e6:.1f} MB)")
    return output_path


# ---------------------------------------------------------------------------
# Partitioned export — writes one Parquet file per partition value
# ---------------------------------------------------------------------------
def export_partitioned(
    df: pd.DataFrame,
    output_dir: Path,
    partition_col: str,
    filename_prefix: str,
    sort_by: list[str] | None = None,
) -> list[Path]:
    """Write one Parquet file per distinct value of partition_col.

    Files are named {filename_prefix}_{partition_value}.parquet inside
    output_dir/{partition_col}={value}/ (Hive-style partitioning).
    """
    log = Logger("export_partitioned")
    output_dir = Path(output_dir)
    paths = []

    for val, group_df in df.groupby(partition_col, sort=True):
        part_dir = output_dir / f"{partition_col}={val}"
        part_dir.mkdir(parents=True, exist_ok=True)
        # Drop the partition column from the data (Athena infers it from path)
        part_df = group_df.drop(columns=[partition_col])
        out_path = part_dir / f"{filename_prefix}.parquet"
        export_df_to_parquet(part_df, out_path, sort_by=sort_by)
        paths.append(out_path)

    log.info(f"Wrote {len(paths)} partitions to {output_dir}")
    return paths


# ---------------------------------------------------------------------------
# Layer processing steps
# ---------------------------------------------------------------------------
def process_network_flowline(cfg: NHDConfig, vpuids: list[str] | None = None) -> None:
    """Process NetworkNHDFlowline: read attrs, validate unique key, export to partitioned Parquet."""
    log = Logger("NetworkNHDFlowline")
    lcfg = _layer_cfg(cfg, "NetworkNHDFlowline")

    log.info("Reading NetworkNHDFlowline (attributes only)...")
    df = read_gdb_attrs(cfg.input_vector_path, "NetworkNHDFlowline", vpuids=vpuids)

    # Drop system/geometry columns that OGR may include
    drop_cols = [c for c in ("Shape_Length",) if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)
        log.info(f"Dropped columns: {drop_cols}")

    # Validate unique key
    uid = lcfg.get("unique_id_field")
    if uid and uid in df.columns:
        n_total = len(df)
        n_unique = df[uid].nunique()
        n_null = df[uid].isna().sum()
        log.info(f"Unique key '{uid}': {n_unique:,} unique / {n_total:,} total, {n_null:,} null")
        if n_unique < n_total:
            n_dupes = n_total - n_unique
            log.warning(f"  {n_dupes:,} duplicate {uid} values detected (VPU boundary duplicates?)")
            # Log sample duplicates for investigation
            duped_ids = df[df[uid].duplicated(keep=False)][uid].unique()[:5]
            log.info(f"  Sample duplicate IDs: {list(duped_ids)}")

    # Output — partition by vpuid
    out_dir = _dist_dir(cfg) / lcfg["layer_id"] / cfg.snapshot_id
    out_dir.mkdir(parents=True, exist_ok=True)

    if "vpuid" in df.columns:
        export_partitioned(
            df, out_dir,
            partition_col="vpuid",
            filename_prefix=lcfg["layer_id"],
            sort_by=["hydroseq"] if "hydroseq" in df.columns else None,
        )
    else:
        export_df_to_parquet(df, out_dir / f"{lcfg['layer_id']}.parquet")

    log.info("NetworkNHDFlowline processing complete.")


def process_nhdplusflow(cfg: NHDConfig, vpuids: list[str] | None = None) -> None:
    """Process NHDPlusFlow: read topology table, export to partitioned Parquet."""
    log = Logger("NHDPlusFlow")
    lcfg = _layer_cfg(cfg, "NHDPlusFlow")

    df = read_nhdplusflow(cfg.input_vector_path, vpuids=vpuids)

    # Derive huc2 partition column from fromvpuid (first 2 digits)
    # Note: fromvpuid can be null for cross-boundary edges; those rows get huc2=None
    if "fromvpuid" in df.columns:
        df["huc2"] = df["fromvpuid"].str[:2]
        unique_huc2 = sorted((v for v in df["huc2"].unique() if v is not None))
        null_count = df["huc2"].isna().sum()
        log.info(f"Derived huc2 partition: {unique_huc2} ({null_count} null rows)")
        if null_count > 0:
            log.warning(f"{null_count} rows have null fromvpuid — they will be dropped by export_partitioned")

    # Output — partition by huc2
    out_dir = _dist_dir(cfg) / lcfg["layer_id"] / cfg.snapshot_id
    out_dir.mkdir(parents=True, exist_ok=True)

    if "huc2" in df.columns:
        export_partitioned(
            df, out_dir,
            partition_col="huc2",
            filename_prefix=lcfg["layer_id"],
            sort_by=["fromnhdpid", "tonhdpid"],
        )
    else:
        export_df_to_parquet(df, out_dir / f"{lcfg['layer_id']}.parquet")

    log.info("NHDPlusFlow processing complete.")


def process_wbdhu12(cfg: NHDConfig, vpuids: list[str] | None = None) -> None:
    """Process WBDHU12: clean geometry, simplify, add bbox, export to GeoParquet."""
    log = Logger("WBDHU12")
    lcfg = _layer_cfg(cfg, "WBDHU12")
    tolerance = lcfg.get("simplify_tolerance", 50.0)

    # --- Step 1: read original geometry (no simplification) to get true bounding boxes ---
    log.info("Reading original WBDHU12 geometry for bounding box calculation...")
    gdf_orig = read_gdb_layer_gpd(cfg.input_vector_path, "WBDHU12", vpuids=vpuids)

    # Reproject to EPSG:4326 for bbox (matches our output CRS)
    if gdf_orig.crs is None or gdf_orig.crs.to_epsg() != 4326:
        log.info("Reprojecting original geometry to EPSG:4326 for bbox...")
        gdf_orig_4326 = gdf_orig.to_crs(epsg=4326)
    else:
        gdf_orig_4326 = gdf_orig
    orig_bounds = gdf_orig_4326.geometry.bounds  # minx/miny/maxx/maxy in EPSG:4326
    log.info(f"Computed bounding boxes from original geometry for {len(orig_bounds):,} features")
    del gdf_orig, gdf_orig_4326

    # --- Step 2: clean + simplify via vector_prep (reads whole layer, filter post-process) ---
    log.info(f"Running vector_prep (simplify_tolerance={tolerance} m, EPSG:5070)...")
    gdf = vector_prep(cfg.input_vector_path, "WBDHU12", tolerance, 5070)

    # Filter to requested VPUs after cleaning (WBDHU12 is small enough to load all then slice)
    if vpuids and "vpuid" in gdf.columns:
        before = len(gdf)
        # Keep a vpuid series aligned to orig_bounds before we filter gdf
        # orig_bounds was computed from gdf_orig which was already filtered to vpuids,
        # so no further slicing of orig_bounds is needed here.
        gdf = gdf[gdf["vpuid"].isin(vpuids)].reset_index(drop=True)
        orig_bounds = orig_bounds.reset_index(drop=True)
        log.info(f"Filtered simplified GDF to vpuids {vpuids}: {before:,} → {len(gdf):,} features")

    # Validate unique key
    uid = lcfg.get("unique_id_field")
    if uid and uid in gdf.columns:
        n_total = len(gdf)
        n_unique = gdf[uid].nunique()
        log.info(f"Unique key '{uid}': {n_unique:,} unique / {n_total:,} total")

    # Drop system columns
    drop_cols = [c for c in ("Shape_Length", "Shape_Area", "FID") if c in gdf.columns]
    if drop_cols:
        gdf = gdf.drop(columns=drop_cols)
        log.info(f"Dropped columns: {drop_cols}")

    # Reproject simplified geometry to EPSG:4326 and rename to geom_simplified
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        log.info("Reprojecting simplified geometry to EPSG:4326...")
        gdf = gdf.to_crs(epsg=4326)
    gdf = gdf.rename_geometry("geom_simplified")

    # Drop Z if present (compound CRS source)
    if gdf["geom_simplified"].has_z.any():
        from shapely.ops import transform as shp_transform
        log.info("Stripping Z coordinates from simplified geometry...")
        gdf["geom_simplified"] = gdf["geom_simplified"].apply(
            lambda g: shp_transform(lambda x, y, z=None: (x, y), g)
        )

    # --- Step 3: export with bbox from original geometry ---
    out_dir = _dist_dir(cfg) / lcfg["layer_id"] / cfg.snapshot_id
    out_dir.mkdir(parents=True, exist_ok=True)
    export_gdf_to_geoparquet(gdf, out_dir / f"{lcfg['layer_id']}.parquet", orig_bounds=orig_bounds)

    log.info("WBDHU12 processing complete.")


# ---------------------------------------------------------------------------
# Layer definitions + Athena DDL
# ---------------------------------------------------------------------------
def build_layer_defs(cfg: NHDConfig) -> None:
    """Auto-generate layer_definitions.json entries for all Phase 1 layers."""
    log = Logger("build_layer_defs")
    layer_defs_path = Path(__file__).parent / "usgs_nhdplushr" / "layer_definitions.json"

    if not layer_defs_path.exists():
        log.info(f"Creating new {layer_defs_path}")
        doc = {
            "$schema": "https://xml.riverscapes.net/riverscapes_metadata/schema/layer_definitions.schema.json",
            "tool_schema_name": "vector-prep-usgs-nhdplushr",
            "tool_schema_version": "0.0.1",
            "layers": [],
        }
    else:
        with open(layer_defs_path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)

    for lcfg in cfg.layers:
        layer_id = lcfg["layer_id"]
        gdb_layer = lcfg["gdb_layer_name"]

        if any(l.get("layer_id") == layer_id for l in doc.get("layers", [])):
            log.info(f"Layer '{layer_id}' already in layer_definitions.json — skipping.")
            continue

        log.info(f"Extracting column metadata for {gdb_layer} → {layer_id}...")
        columns = fetch_columns_from_fgdb(cfg.input_vector_path, gdb_layer)

        if lcfg.get("include_geometry"):
            tolerance = lcfg.get("simplify_tolerance")

            # fetch_columns_from_fgdb now includes a raw geometry column; rename/replace
            # it with geom_simplified (our processed output column name) and add bbox.
            columns = [c for c in columns if c.get("name") not in ("geometry", "shape")]

            simplify_note = (
                f"Simplified to {tolerance} m tolerance (Douglas-Peucker, topology-preserving) "
                f"in EPSG:5070 then reprojected to EPSG:4326. Added in Riverscapes processing."
                if tolerance
                else "Reprojected to EPSG:4326. Added in Riverscapes processing."
            )
            columns.append({
                "name": "geom_simplified",
                "friendly_name": "Simplified Geometry",
                "dtype": "GEOMETRY",
                "description": simplify_note,
            })
            columns.append({
                "name": "geometry_bbox",
                "friendly_name": "Geometry Bounding Box",
                "dtype": "STRUCTURED",
                "description": (
                    "Bounding box struct (xmin/ymin/xmax/ymax, EPSG:4326, float32) for Athena "
                    "spatial predicate push-down. Computed from original unsimplified geometry. "
                    "Added in Riverscapes processing."
                ),
            })
        else:
            # Non-spatial layer: drop any geometry column the GDB metadata may have returned
            columns = [c for c in columns if c.get("dtype") != "GEOMETRY"]

        new_layer = {
            "layer_id": layer_id,
            "layer_name": gdb_layer,
            "source_title": cfg.source_title,
            "source_url": cfg.source_url,
            "columns": columns,
        }
        doc["layers"].append(new_layer)
        log.info(f"Added '{layer_id}' with {len(columns)} columns.")

    with open(layer_defs_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=4, ensure_ascii=False)
        fh.write("\n")
    log.info(f"Wrote {layer_defs_path}")


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _map_dtype_to_athena(dtype: str, col_name: str) -> str:
    mapping = {
        "STRING": "string",
        "INTEGER": "bigint",
        "FLOAT": "double",
        "DATETIME": "timestamp",
        "GEOMETRY": "binary",
    }
    if dtype == "STRUCTURED" and col_name.lower() == "geometry_bbox":
        return "struct<xmin:float,ymin:float,xmax:float,ymax:float>"
    if dtype == "GEOMETRY":
        return "binary"
    return mapping.get(dtype, "string")


def build_athena_ddl(cfg: NHDConfig) -> list[Path]:
    """Generate Athena CREATE EXTERNAL TABLE DDL for each Phase 1 layer."""
    log = Logger("build_athena_ddl")
    layer_defs_path = Path(__file__).parent / "usgs_nhdplushr" / "layer_definitions.json"
    with open(layer_defs_path, "r", encoding="utf-8") as fh:
        layer_defs = json.load(fh)

    bucket = "riverscapes-athena"
    database = "rs_raw"
    source_stub = "usgov_sources" if cfg.source_category == "usgov" else f"raw_{cfg.source_category}"
    ddl_paths = []

    for lcfg in cfg.layers:
        layer_id = lcfg["layer_id"]
        layer = next((l for l in layer_defs.get("layers", []) if l.get("layer_id") == layer_id), None)
        if layer is None:
            log.warning(f"Skipping DDL for '{layer_id}' — not in layer_definitions.json")
            continue

        columns = layer.get("columns", [])
        if not columns:
            log.warning(f"Skipping DDL for '{layer_id}' — no columns defined")
            continue

        snapshot_stub = cfg.snapshot_id.replace("-", "")
        table_name = f"{layer_id.replace('-', '_')}_snapshot_{snapshot_stub}"
        location = f"s3://{bucket}/{source_stub}/{layer_id}/{cfg.snapshot_id}/"

        table_comment = (
            f"{lcfg['gdb_layer_name']}. Source: {cfg.source_title}. "
            f"URL: {cfg.source_url}. Snapshot: {cfg.snapshot_id}."
        )

        col_lines = []
        for col in columns:
            name = col.get("name", "").lower()
            if not name:
                continue
            dtype = _map_dtype_to_athena(col.get("dtype", "STRING"), name)
            desc = (col.get("description") or "").strip()
            friendly = (col.get("friendly_name") or "").strip()
            comment = desc or friendly
            if desc and friendly and friendly not in desc:
                comment = f"{friendly}. {desc}"
            if comment:
                col_lines.append(f"  `{name}` {dtype} COMMENT '{_sql_escape(comment)}'")
            else:
                col_lines.append(f"  `{name}` {dtype}")

        # Detect if this table will be Hive-partitioned
        partition_col = None
        if layer_id == "usgs-nhdplushr-network-flowline":
            partition_col = "vpuid"
        elif layer_id == "usgs-nhdplushr-flow":
            partition_col = "huc2"

        # If partitioned, the partition column must NOT be in the column list
        if partition_col:
            col_lines = [cl for cl in col_lines if not cl.strip().startswith(f"`{partition_col}`")]

        ddl_parts = [
            f"CREATE EXTERNAL TABLE `{database}`.`{table_name}`(",
            ",\n".join(col_lines),
            ")",
        ]

        if partition_col:
            ddl_parts.append(f"PARTITIONED BY (`{partition_col}` string)")

        ddl_parts.extend([
            "ROW FORMAT SERDE",
            "  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'",
            "STORED AS INPUTFORMAT",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'",
            "OUTPUTFORMAT",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'",
            "LOCATION",
            f"  '{location}'",
            "TBLPROPERTIES (",
            "  'classification'='parquet',",
            f"  'comment'='{_sql_escape(table_comment)}',",
            "  'compressionType'='snappy',",
            "  'typeOfData'='file'",
            ")",
        ])

        ddl = "\n".join(ddl_parts)

        out_dir = _dist_dir(cfg) / layer_id / cfg.snapshot_id
        out_dir.mkdir(parents=True, exist_ok=True)
        ddl_path = out_dir / f"{table_name}.sql"
        ddl_path.write_text(ddl, encoding="utf-8")
        ddl_paths.append(ddl_path)
        log.info(f"Wrote DDL to {ddl_path}")

    return ddl_paths


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="NHDPlus HR → Parquet orchestrator")
    parser.add_argument(
        "--vpuids",
        help="Comma-separated list of vpuids to process (e.g. '1707,1708'). Omit for all.",
    )
    parser.add_argument(
        "--layers",
        help="Comma-separated layer names to process (e.g. 'NetworkNHDFlowline,WBDHU12'). Omit for all Phase 1.",
    )
    parser.add_argument(
        "--skip-data", action="store_true",
        help="Skip data processing; only build layer_definitions and DDL.",
    )
    args = parser.parse_args()

    vpuids = [v.strip() for v in args.vpuids.split(",")] if args.vpuids else None
    target_layers = {v.strip() for v in args.layers.split(",")} if args.layers else None

    cfg = NHDConfig.from_json(_INPUTS_JSON)

    repo_root = _repo_root()
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    log = Logger("NHDPlus HR Orchestrate")
    vpu_suffix = f"_vpus-{'_'.join(vpuids)}" if vpuids else ""
    log_file = logs_dir / f"orchestrate_nhdplushr_{cfg.snapshot_id}{vpu_suffix}.log"
    log.setup(log_path=str(log_file), verbose=True)

    log.info(f"Config: snapshot={cfg.snapshot_id}, vpuids={vpuids}, target_layers={target_layers}")

    if not args.skip_data:
        # --- Data processing ---
        all_phase1 = {"NetworkNHDFlowline", "NHDPlusFlow", "WBDHU12"}
        to_process = all_phase1 if target_layers is None else (target_layers & all_phase1)

        if "NetworkNHDFlowline" in to_process:
            process_network_flowline(cfg, vpuids)

        if "NHDPlusFlow" in to_process:
            process_nhdplusflow(cfg, vpuids)

        if "WBDHU12" in to_process:
            process_wbdhu12(cfg, vpuids)

    # --- Metadata & DDL ---
    build_layer_defs(cfg)
    ddl_paths = build_athena_ddl(cfg)
    for p in ddl_paths:
        log.info(f"DDL: {p}")

    log.info("Done.")


if __name__ == "__main__":
    main()
