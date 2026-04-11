"""Run a sequence of vector preparation steps
e.g. (get data, check, prepare, document, output, upload)
Based on orchestrate-pastures (the original), this one is modified for the bootheel subset sent as GDB
May require user input / view or manual steps ... we may add some questionary prompts
Or run in ipynb notebook? (easier to add interactive visualizations as needed + self documents)
See runlog-pastures-NMbootheel-202600407.md
- Lorin March / April 2026
"""
import io
import json
import uuid
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
from rsxml import Logger

from fetch_arcgis_metadata import fetch_columns_from_hub_url, update_layer_definitions
from vector_prep import output_gdf, vector_prep

# Fixed namespace for RS_ROW_ID derivation — do not change once data is published
_RS_ROW_ID_NAMESPACE = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')  # uuid.NAMESPACE_OID

@dataclass
class RunInputs:
    """Stuff user needs to supply to establish what is being prepared"""
    input_vector_path: Path | str
    input_layer_name: str | None
    source_category: str
    source_title: str
    source_url: str | None
    layer_id: str
    snapshot_id: str
    tolerance: float = 0.0
    epsg: int = 5070
    special_notes: str = ""
    data_prep_operator: str = ""

    @classmethod
    def from_json(cls, path: str) -> "RunInputs":
        with open(path, encoding="utf-8") as f:
            return cls(**json.load(f))


def step_1(cfg: RunInputs, dist_dir):
    """vector prep (error checks) and output to 4326"""
    prepped_gdf = vector_prep(cfg.input_vector_path, None, cfg.tolerance, cfg.epsg)
    source_category_stub = 'usgov_sources' if cfg.source_category == 'usgov' else f'raw_{cfg.source_category}'

    output_dir = dist_dir / source_category_stub / cfg.layer_id / cfg.snapshot_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{cfg.layer_id}.gpkg"
    output_gdf(prepped_gdf, output_file, cfg.input_layer_name)
    return prepped_gdf, output_file

def step_2(gdf, cfg: RunInputs, output_file: Path):
    """Add ST_ALLOT_PAST_NAME, ST_ALLOT_PAST_MULTI, and deterministic RS_ROW_ID from GlobalID."""
    log = Logger("Step2")
    # Uniqueness check on GlobalID
    n_dupes = gdf['GlobalID'].duplicated().sum()
    if n_dupes > 0:
        raise ValueError(f"GlobalID is not unique: {n_dupes} duplicate value(s) found. Cannot derive deterministic RS_ROW_ID.")
    null_count = gdf['GlobalID'].isna().sum()
    if null_count > 0:
        raise ValueError(f"GlobalID has {null_count} null value(s). Cannot derive deterministic RS_ROW_ID.")

    # for idempotence, # Drop derived columns if re-running on already-enriched data
    for col in ('ST_ALLOT_PAST_NAME', 'ST_ALLOT_PAST_MULTI', 'RS_ROW_ID'):
        if col in gdf.columns:
            gdf = gdf.drop(columns=[col])

    # min name combo per ST_ALLOT_PAST entity
    name_min = (
        gdf.assign(_nc=gdf['ADMIN_ST'].fillna('') + '_' + gdf['ALLOT_NAME'].fillna('') + '_' + gdf['PAST_NAME'].fillna(''))
           .groupby('ST_ALLOT_PAST')['_nc']
           .min()
           .rename('ST_ALLOT_PAST_NAME')
    )
    multi_flag = (
        gdf.groupby('ST_ALLOT_PAST')
           .size()
           .gt(1)
           # Use pandas nullable Int64 so NaN rows (null ST_ALLOT_PAST) don't upcast the whole column to float64
           .astype("Int64")
           .rename('ST_ALLOT_PAST_MULTI')
    )
    gdf = gdf.join(name_min, on='ST_ALLOT_PAST').join(multi_flag, on='ST_ALLOT_PAST')

    # Warn if any rows got NaN in derived columns — indicates null ST_ALLOT_PAST values
    for derived_col in ('ST_ALLOT_PAST_NAME', 'ST_ALLOT_PAST_MULTI'):
        n_null = gdf[derived_col].isna().sum()
        if n_null > 0:
            log.warning(f"{derived_col}: {n_null} row(s) have NaN — likely null ST_ALLOT_PAST values. These rows were excluded from the groupby.")

    # Deterministic UUID5 derived from GlobalID
    gdf['RS_ROW_ID'] = gdf['GlobalID'].apply(
        lambda gid: str(uuid.uuid5(_RS_ROW_ID_NAMESPACE, gid))
    )

    output_gdf(gdf, output_file, cfg.input_layer_name)
    log.info(f"step_2 complete: added ST_ALLOT_PAST_NAME, ST_ALLOT_PAST_MULTI, RS_ROW_ID. Shape: {gdf.shape}")
    return gdf


def export_to_geoparquet(gdf: gpd.GeoDataFrame, output_path: Path | str) -> Path:
    """Export a GeoDataFrame to Snappy-compressed GeoParquet in EPSG:4326.

    Produces a valid GeoParquet file (readable by QGIS and other tools) by
    delegating to geopandas for geometry encoding and the required *geo* Parquet
    file metadata. A *geometry_bbox* column typed as
    struct<xmin:float32, ymin:float32, xmax:float32, ymax:float32> is then
    appended for spatial predicate push-down (e.g. Athena partition pruning).

    Returns the resolved output path.
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ImportError(
            "GeoParquet export requires optional dependency 'pyarrow'. "
            "Install with: uv sync --extra geoparquet"
        ) from exc

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Idempotency: drop bbox column if already present
    if "geometry_bbox" in gdf.columns:
        gdf = gdf.drop(columns=["geometry_bbox"])

    bounds = gdf.geometry.bounds  # DataFrame: minx, miny, maxx, maxy

    # Let geopandas write a proper GeoParquet file (including `geo` metadata)
    # into a buffer, then read it back as a PyArrow table so the metadata
    # is preserved when we append the bbox column.
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
    # append_column preserves schema.metadata, keeping the `geo` key intact
    table = table.append_column(pa.field("geometry_bbox", bbox_type), bbox_col)

    pq.write_table(table, str(output_path), compression="snappy")
    return output_path


# Derived columns added by step_2 — hard-coded since they are always the same for this dataset.
_STEP2_COLUMNS: list[dict] = [
    {
        "name": "Pasture_ID",
        "friendly_name": "Pasture ID",
        "dtype": "STRING",
        "description": (
            "Unique ID for pasture"
        ),
    },
]

_GEO_COLUMNS: list[dict] = [
    {
        "name": "geometry",
        "friendly_name": "Geometry (binary)",
        "dtype": "GEOMETRY",
        "description": (
            "Pasture Multi-Polygon geometry"
        ),
    },
    {
        "name": "geometry_bbox",
        "friendly_name": "Geometry Bounding Box",
        "dtype": "STRUCTURED",
        "description": (
            "Used for improved spatial query performance. Added in Riverscapes processing."
        ),
    },
]

def _infer_dtype(series) -> str:
    """Map a pandas/geopandas Series dtype to a layer_definitions dtype string."""
    name = getattr(series.dtype, "name", "").lower()
    if name == "geometry":
        return "GEOMETRY"
    if "int" in name:
        return "INTEGER"
    if "float" in name:
        return "FLOAT"
    if "datetime" in name:
        return "DATETIME"
    return "STRING"


def build_layer_defs(cfg: RunInputs, gdf: gpd.GeoDataFrame) -> None:
    """Add a new layer entry to layer_definitions.json for cfg.layer_id.

    Fetches column definitions from the ArcGIS Hub URL in cfg.source_url when
    available; otherwise (or for any columns not returned by the remote source)
    falls back to inspecting *gdf* directly and inferring dtypes.  The derived
    columns from step_2 and the geometry columns are appended last.

    Skips silently if cfg.layer_id is already present in the file.
    """
    log = Logger("build_layer_defs")
    layer_defs_path = Path(__file__).parent / "layer_definitions.json"

    with open(layer_defs_path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)

    if any(layer.get("layer_id") == cfg.layer_id for layer in doc.get("layers", [])):
        log.warning(f"Layer '{cfg.layer_id}' already exists in layer_definitions.json — skipping.")
        return

    # Add skeleton (columns filled in below via update_layer_definitions)
    new_layer: dict = {
        "layer_id": cfg.layer_id,
        "layer_name": cfg.input_layer_name or cfg.layer_id,
        "source_title": cfg.source_title,
        **({"source_url": cfg.source_url} if cfg.source_url else {}),
        "columns": [],
    }
    doc["layers"].append(new_layer)
    with open(layer_defs_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=4, ensure_ascii=False)
        fh.write("\n")

    columns: list[dict] = []
    if cfg.source_url:
        log.info(f"Fetching column metadata from {cfg.source_url} ...")
        columns = fetch_columns_from_hub_url(cfg.source_url)

    # Add any GDF columns not already covered by the remote fetch or the
    # hard-coded step_2 / geometry entries
    reserved = {col["name"] for col in _STEP2_COLUMNS + _GEO_COLUMNS}
    covered = {col["name"] for col in columns}
    for col_name in gdf.columns:
        if col_name not in covered and col_name not in reserved:
            columns.append({
                "name": col_name,
                "friendly_name": col_name,
                "dtype": _infer_dtype(gdf[col_name]),
                "description": "",
            })

    columns.extend(_STEP2_COLUMNS)
    columns.extend(_GEO_COLUMNS)
    update_layer_definitions(str(layer_defs_path), cfg.layer_id, columns)

    log.info(f"Added layer '{cfg.layer_id}' with {len(columns)} columns to {layer_defs_path}")


def _sql_escape(value: str) -> str:
    """Escape single quotes for SQL string literals/comments."""
    return value.replace("'", "''")


def _athena_table_name(layer_id: str, snapshot_id: str) -> str:
    """Build snapshot table name for rs_raw from layer and snapshot ids."""
    snapshot_stub = snapshot_id.replace("-", "")
    return f"{layer_id.replace('-', '_')}_snapshot_{snapshot_stub}"


def _map_layer_dtype_to_athena(dtype: str, col_name: str) -> str:
    """Map layer_definitions dtype to Athena/Hive type."""
    mapping = {
        "STRING": "string",
        "INTEGER": "bigint",
        "FLOAT": "double",
        "DATETIME": "timestamp",
        "GEOMETRY": "binary",
    }
    if dtype == "STRUCTURED" and col_name.lower() == "geometry_bbox":
        return "struct<xmin:float,ymin:float,xmax:float,ymax:float>"
    return mapping.get(dtype, "string")


def build_athena_ddl(cfg: RunInputs, bucket: str = "riverscapes-athena", database: str = "rs_raw") -> Path:
    """Build Athena CREATE EXTERNAL TABLE DDL from cfg and `layer_definitions.json`

    Returns the path to the generated .sql file.
    TODO (ENHANCEMENT): Add additional metadata with custom table properties using `rs.` namespace)
    """
    log = Logger("build_athena_ddl")
    layer_defs_path = Path(__file__).parent / "layer_definitions.json"
    with open(layer_defs_path, "r", encoding="utf-8") as fh:
        layer_defs = json.load(fh)

    layer = next((lyr for lyr in layer_defs.get("layers", []) if lyr.get("layer_id") == cfg.layer_id), None)
    if layer is None:
        raise LookupError(f"layer_id '{cfg.layer_id}' not found in {layer_defs_path}")

    columns = layer.get("columns", [])
    if not columns:
        raise ValueError(f"layer_id '{cfg.layer_id}' has no columns in {layer_defs_path}")

    source_category_stub = "usgov_sources" if cfg.source_category == "usgov" else f"raw_{cfg.source_category}"
    location = f"s3://{bucket}/{source_category_stub}/{cfg.layer_id}/{cfg.snapshot_id}/"
    table_name = _athena_table_name(cfg.layer_id, cfg.snapshot_id)

    layer_name = layer.get("layer_name") or cfg.input_layer_name or cfg.layer_id
    table_comment = (
        f"{layer_name}. Source: {cfg.source_title}. URL: {cfg.source_url}. "
        f"Snapshot: {cfg.snapshot_id}."
    )

    col_lines: list[str] = []
    for col in columns:
        name = col.get("name")
        if not name:
            continue
        athena_name = name.lower()  # Athena normalises identifiers to lower-case
        dtype = _map_layer_dtype_to_athena(col.get("dtype", "STRING"), name)
        desc = (col.get("description") or "").strip()
        friendly = (col.get("friendly_name") or "").strip()
        comment = desc or friendly
        if desc and friendly and friendly not in desc:
            comment = f"{friendly}. {desc}"

        if comment:
            col_lines.append(f"  `{athena_name}` {dtype} COMMENT '{_sql_escape(comment)}'")
        else:
            col_lines.append(f"  `{athena_name}` {dtype}")

    # TODO: Compression is currently assumed from the manual QGIS export.
    # Capture/read parquet compression from pipeline output metadata so this is not hard-coded.    
    ddl = (
        f"CREATE EXTERNAL TABLE `{database}`.`{table_name}`(\n"
        + ", \n".join(col_lines)
        + "\n)\n"
        + "ROW FORMAT SERDE \n"
        + "  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' \n"
        + "STORED AS INPUTFORMAT \n"
        + "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' \n"
        + "OUTPUTFORMAT \n"
        + "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\n"
        + "LOCATION\n"
        + f"  '{location}'\n"
        + "TBLPROPERTIES (\n"
        + "  'classification'='parquet', \n"
        + f"  'comment'='{_sql_escape(table_comment)}',\n"
        + "  'compressionType'='snappy', \n"
        + "  'typeOfData'='file'\n"
        + ")"
    )

    repo_root = next(p for p in Path(__file__).resolve().parents if (p / "pyproject.toml").exists())
    dist_dir = repo_root / "dist" / source_category_stub / cfg.layer_id / cfg.snapshot_id
    dist_dir.mkdir(parents=True, exist_ok=True)
    ddl_path = dist_dir / f"{table_name}.sql"
    ddl_path.write_text(ddl, encoding="utf-8")
    log.info(f"Wrote Athena DDL to {ddl_path}")
    return ddl_path

def main():
    run_inputs = "/home/narlorin/udata/blm/pasture_polygons_bootheel_2026-04-02/inputs.json"
    repo_root = next(p for p in Path(__file__).resolve().parents if (p / "pyproject.toml").exists())
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    dist_dir = repo_root / "dist"
    dist_dir.mkdir(parents=True, exist_ok=True)
    
    log = Logger("Vector Prep Orchestrate")
    cfg = RunInputs.from_json(run_inputs)
    log_file = logs_dir / f"vector_prep_orchestrate_{cfg.layer_id}_{cfg.snapshot_id}.log"
    log.setup(log_path=str(log_file), verbose=True)
    
    # gdf, output_file = step_1(cfg, dist_dir)
    # log.info(f"Prepped dataframe with shape {gdf.shape} and outputed to {output_file}")
    
    # duplicates logic in step1
    source_category_stub = 'usgov_sources' if cfg.source_category == 'usgov' else f'raw_{cfg.source_category}'
    output_file = dist_dir / source_category_stub / cfg.layer_id / cfg.snapshot_id / f"{cfg.layer_id}.gpkg"

    log.info(f"Loading step_1 output from {output_file}")
    gdf = gpd.read_file(output_file)
    log.info(f"Loaded {len(gdf)} features")

    # enriched_gdf = step_2(gdf, cfg, output_file)
    # log.info(f"Enriched dataframe with shape {enriched_gdf.shape} written to {output_file}")

    build_layer_defs(cfg, gdf)
    export_to_geoparquet(gdf, output_file.with_suffix(".parquet"))
    ddl_path = build_athena_ddl(cfg)
    log.info(f"Athena DDL generated at {ddl_path}")

    
if __name__ == '__main__':
    main()
