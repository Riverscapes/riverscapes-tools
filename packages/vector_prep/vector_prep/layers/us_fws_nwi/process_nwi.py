"""NWI processing pipeline for HUC8 zip archives.

This module implements the non-interactive processing steps for each downloaded
NWI HUC8 zip:

1. Extract the FileGDB payload from zip.
2. Resolve wetlands/riparian layer names in the FileGDB.
3. Run vector_prep for each layer.
4. Add huc8 as a required output field.
5. Upload to Athena Iceberg table (create once, append on later runs).
6. Update status columns in the existing GeoPackage ledger.

The design is intentionally sequential. By default, completed HUC8s are skipped,
and reprocess mode performs delete-then-append per HUC8.

How to use:

Process one zip:
python -m vector_prep.layers.us_fws_nwi.process_nwi --zip HU8_10090206_Watershed.zip

Process all zips in a folder:
python -m vector_prep.layers.us_fws_nwi.process_nwi --zip-dir F:/nardata/datadownload/fws/nwi

Reprocess completed HUC8s (delete old rows for each HUC8 before append):
python -m vector_prep.layers.us_fws_nwi.process_nwi --zip-dir F:/nardata/datadownload/fws/nwi --reprocess-existing

Pilot run on first 25 zips:
python -m vector_prep.layers.us_fws_nwi.process_nwi --zip-dir F:/nardata/datadownload/fws/nwi --limit 25

Note:
* a separate script downloads the zip files.
* this calls athena functions and requires the uv environment for it ie:
uv pip install boto3 pyarrow pyiceberg pyiceberg_core
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import shutil
import sqlite3
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any

import geopandas as gpd
import pyogrio
import pyproj
from rsxml import Logger

from vector_prep.lib.field_map import FieldMapConfig, load_and_validate_field_map
from vector_prep.lib.output import output_gdf
from vector_prep.vector_prep import load_config, vector_prep

LEDGER_FILENAME = "nwi_processing_ledger.gpkg"
DEFAULT_EXTRACT_ROOT = "_extracted_fgdb"
DEFAULT_OUTPUT_ROOT = "_processed"
DEFAULT_GLUE_DATABASE = "ext_raw"
DEFAULT_ATHENA_TABLE_WETLANDS = "us_fws_nwi_wetlands"
DEFAULT_ATHENA_TABLE_RIPARIAN = "us_fws_nwi_riparian"

# Ordered process steps and their success statuses.
STEP_DOWNLOAD = "download"
STEP_UNZIP = "unzip"
STEP_VECTOR_PREP = "vector_prep"
STEP_UPLOAD = "upload"

STATUS_DOWNLOADED = "downloaded"
STATUS_SOURCE_READY = "source_ready"
STATUS_PREPPED = "prepped"
STATUS_UPLOADED = "uploaded"
STATUS_FAILED = "failed"
STATUS_NOT_PRESENT = "not_present"

STEP_SUCCESS_STATUS: dict[str, str] = {
    STEP_DOWNLOAD: STATUS_DOWNLOADED,
    STEP_UNZIP: STATUS_SOURCE_READY,
    STEP_VECTOR_PREP: STATUS_PREPPED,
    STEP_UPLOAD: STATUS_UPLOADED,
}


@dataclass(frozen=True)
class LayerProcessConfig:
    """Configuration for one source/destination layer mapping."""

    key: str
    fgdb_suffix: str
    config_path: Path
    glue_database: str
    table_name: str
    s3_location: str


@dataclass
class ProcessResult:
    """Structured summary from one HUC8/layer processing run."""

    huc8: str
    layer_key: str
    status: str
    output_gpkg: Path | None = None
    rows_prepped: int = 0
    rows_uploaded: int = 0
    table_action: str | None = None
    error: str | None = None


@dataclass
class UploadRuntime:
    """Shared Athena upload resources reused across the full run."""

    athena_upload: ModuleType  # Dynamically loaded Athena helper module.
    catalog: Any  # Shared Glue/Iceberg catalog client.
    table_cache: dict[
        tuple[str, str], Any
    ]  # Cached table handles by (database, table).
    namespaces_ensured: set[str]  # Databases already verified/created in this run.
    comments_synced: set[
        tuple[str, str]
    ]  # Tables whose Glue column comments are already updated.


@dataclass(frozen=True)
class LayerRuntimeConfig:
    """One-time per-layer config and metadata resolved before batch processing."""

    layer_cfg: LayerProcessConfig
    vector_cfg: dict[str, Any]
    field_map_config: FieldMapConfig | None
    layer_definitions_path: Path | None
    layer_id: str | None
    column_comments: dict[str, str]
    column_dtypes: dict[str, str]
    layer_description: str


_VP_SUMMARY_KEYS: tuple[str, ...] = (
    "input_count",
    "output_count",
    "multipart_detected",
    "geocollection_detected",
    "invalid_fixed",
    "invalid_unfixed",
    "null_or_empty",
    "simplified_count",
    "geometry_duplicates_dropped",
    "row_duplicates_dropped",
    "slivers_dropped",
    "below_min_size_detected",
    "below_min_size_dropped",
)


def _int_stat(stats: dict[str, Any], key: str) -> int | None:
    """Read one integer-like stat from a vector_prep stats dict."""
    val = stats.get(key)
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _build_vp_summary_fields(
    layer_key: str,
    stats: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build ledger column updates for vector_prep quality summary metrics."""
    if not stats:
        return {}

    updates: dict[str, Any] = {}

    for key in _VP_SUMMARY_KEYS:
        parsed = _int_stat(stats, key)
        if parsed is None:
            continue
        updates[f"{layer_key}_vp_{key}"] = parsed
    return updates


def _format_step_error(step: str, error: str) -> str:
    """Attach process-step context to error text for clearer ledger diagnostics."""
    return f"[{step}] {error}"


def _status_to_step(status: str) -> str | None:
    """Map a status to its process step when applicable."""
    for step, success_status in STEP_SUCCESS_STATUS.items():
        if success_status == status:
            return step
    return None


def utc_now_iso() -> str:
    """Return current UTC time in ISO-8601 format without microseconds."""
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def extract_huc8_from_zip_name(zip_path: Path) -> str:
    """Extract HUC8 from expected NWI zip names.

    Expected examples:
      HU8_01234567_Watershed.zip
      hu8_01234567_watershed.zip
    """
    stem = zip_path.stem
    parts = stem.split("_")
    if len(parts) < 2:
        raise ValueError(f"Could not parse HUC8 from zip name: {zip_path.name}")
    huc8 = parts[1].strip()
    if len(huc8) != 8 or not huc8.isdigit():
        raise ValueError(f"Invalid HUC8 token '{huc8}' in zip name: {zip_path.name}")
    return huc8


def _safe_member_path(member_name: str) -> Path:
    """Validate zip member path and return a relative Path.

    Reject absolute paths and parent traversal segments.
    """
    rel = Path(member_name)
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError(f"Unsafe zip member path: {member_name}")
    return rel


def _detect_fgdb_prefix(zip_file: zipfile.ZipFile, huc8: str) -> str:
    """Detect the FileGDB directory prefix within a zip archive."""
    prefixes: set[str] = set()
    for member in zip_file.namelist():
        lower = member.lower()
        marker = ".gdb/"
        if marker in lower:
            idx = lower.index(marker)
            prefixes.add(member[: idx + len(marker)])
        elif lower.endswith(".gdb"):
            prefixes.add(member)

    if not prefixes:
        raise FileNotFoundError(f"No FileGDB payload found in zip for HUC8 {huc8}.")

    preferred = [p for p in prefixes if huc8 in p]
    if preferred:
        return min(preferred)
    return min(prefixes)


def extract_filegdb_from_zip(
    zip_path: Path,
    extract_root: Path,
    overwrite: bool = False,
) -> Path:
    """Extract only the FileGDB subtree from one NWI zip file.

    Returns the extracted .gdb directory path.
    """
    log = Logger("extract_filegdb_from_zip")
    huc8 = extract_huc8_from_zip_name(zip_path)

    if not zip_path.is_file():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    target_root = (extract_root / huc8).resolve()
    target_root.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        fgdb_prefix = _detect_fgdb_prefix(zf, huc8)
        fgdb_rel = Path(fgdb_prefix.rstrip("/\\"))
        extracted_fgdb_path = (target_root / fgdb_rel).resolve()

        if extracted_fgdb_path.exists() and not overwrite:
            log.info(f"Reusing existing extracted GDB: {extracted_fgdb_path}")
            return extracted_fgdb_path

        if extracted_fgdb_path.exists() and overwrite:
            shutil.rmtree(extracted_fgdb_path)

        members = [m for m in zf.namelist() if m.startswith(fgdb_prefix)]
        if not members:
            raise FileNotFoundError(
                f"No members found under FileGDB prefix '{fgdb_prefix}' in {zip_path.name}"
            )

        for member in members:
            rel_member = _safe_member_path(member)
            out_path = (target_root / rel_member).resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)

            if member.endswith("/"):
                out_path.mkdir(parents=True, exist_ok=True)
                continue

            with zf.open(member, "r") as src, out_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)

    if not extracted_fgdb_path.exists():
        raise FileNotFoundError(
            f"FileGDB extraction did not produce expected path: {extracted_fgdb_path}"
        )

    log.info(f"Extracted FileGDB for HUC8 {huc8}: {extracted_fgdb_path}")
    return extracted_fgdb_path


def list_fgdb_layer_names(fgdb_path: Path) -> list[str]:
    """List layers available in a FileGDB path."""
    layers = pyogrio.list_layers(str(fgdb_path))
    names: list[str] = []
    for row in layers:
        if len(row) > 0:
            names.append(str(row[0]))
    return names


def resolve_fgdb_layer_name(fgdb_path: Path, huc8: str, layer_key: str) -> str:
    """Resolve source layer name in FileGDB for wetlands or riparian."""
    names = list_fgdb_layer_names(fgdb_path)
    if not names:
        raise ValueError(f"No layers found in FileGDB: {fgdb_path}")

    suffix = "Wetlands" if layer_key == "wetlands" else "Riparian"
    expected = f"HU8_{huc8}_{suffix}"

    # First try exact match.
    if expected in names:
        return expected

    expected_lower = expected.lower()
    for name in names:
        if name.lower() == expected_lower:
            return name

    # Fallback to suffix match when naming varies slightly.
    target_suffix = f"_{suffix.lower()}"
    suffix_matches = [n for n in names if n.lower().endswith(target_suffix)]
    if len(suffix_matches) == 1:
        return suffix_matches[0]

    raise ValueError(
        f"Could not find {layer_key} layer in {fgdb_path.name}. "
        f"Expected '{expected}' or unique '*{target_suffix}'. Found: {names}"
    )


def _load_field_map_config(
    config_path: Path, cfg: dict[str, Any]
) -> FieldMapConfig | None:
    """Build optional FieldMapConfig from vector_prep config parameters."""
    raw_field_map = cfg.get("field_map")
    if not raw_field_map:
        return None

    layer_defs_rel = cfg.get("layer_definitions")
    if not layer_defs_rel:
        raise ValueError("Config specifies field_map but layer_definitions is missing.")

    layer_defs_path = Path(layer_defs_rel)
    if not layer_defs_path.is_absolute():
        layer_defs_path = (config_path.parent / layer_defs_path).resolve()

    layer_id = cfg.get("layer_id")
    return load_and_validate_field_map(
        raw_field_map,
        layer_defs_path,
        layer_id=layer_id,
    )


def _ensure_huc8_column(gpkg_path: Path, layer_name: str, huc8: str) -> int:
    """Write/overwrite a huc8 column in an output GeoPackage layer."""
    gdf = gpd.read_file(gpkg_path, layer=layer_name)
    gdf["huc8"] = str(huc8)
    output_gdf(gdf, str(gpkg_path), layer_name)
    return len(gdf)


def run_vector_prep_for_fgdb_layer(
    fgdb_path: Path,
    huc8: str,
    layer_runtime_cfg: LayerRuntimeConfig,
    output_root: Path,
    garbage_root: Path,
) -> tuple[Path, Path, str, dict[str, Any]]:
    """Run vector_prep for one FileGDB layer and return output artifacts."""
    log = Logger("run_vector_prep_for_fgdb_layer")
    layer_cfg = layer_runtime_cfg.layer_cfg
    cfg = layer_runtime_cfg.vector_cfg

    source_layer_name = resolve_fgdb_layer_name(fgdb_path, huc8, layer_cfg.key)

    huc8_dir = (output_root / huc8).resolve()
    huc8_dir.mkdir(parents=True, exist_ok=True)
    garbage_dir = (garbage_root / huc8).resolve()
    garbage_dir.mkdir(parents=True, exist_ok=True)

    output_gpkg = huc8_dir / f"{huc8}_{layer_cfg.key}.gpkg"
    garbage_gpkg = garbage_dir / f"{huc8}_{layer_cfg.key}_garbage.gpkg"

    stats = vector_prep(
        input_dataset=str(fgdb_path.resolve()),
        layer_name=source_layer_name,
        tolerance=float(cfg.get("tolerance", 0.0)),
        epsg=int(cfg.get("epsg", 5070)),
        output_path=str(output_gpkg.resolve()),
        garbage_path=str(garbage_gpkg.resolve()),
        min_size=(
            float(cfg.get("min_size")) if cfg.get("min_size") is not None else None
        ),
        min_size_drop=bool(cfg.get("min_size_drop", False)),
        chunk_size=int(cfg.get("chunk_size", 10_000)),
        field_map_config=layer_runtime_cfg.field_map_config,
        sql_filter=cfg.get("filter"),
        skip_geometry_dedup=bool(cfg.get("skip_geometry_dedup", False)),
    )

    rows = _ensure_huc8_column(output_gpkg, source_layer_name, huc8)
    log.info(
        f"vector_prep complete for HUC8 {huc8} ({layer_cfg.key}) -> {output_gpkg} rows={rows:,}"
    )
    return output_gpkg, garbage_gpkg, source_layer_name, stats


def _load_athena_upload_module(athena_root: Path) -> ModuleType:
    """Load geopackage_athena_iceberg_upload.py dynamically."""
    upload_script = (
        athena_root / "tools" / "athena_upload" / "geopackage_athena_iceberg_upload.py"
    )
    if not upload_script.is_file():
        raise FileNotFoundError(f"Athena upload script not found: {upload_script}")

    spec = importlib.util.spec_from_file_location("athena_upload_module", upload_script)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module spec from {upload_script}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def initialize_upload_runtime(
    athena_root: Path,
    layer_runtime_configs: list[LayerRuntimeConfig],
) -> UploadRuntime:
    """Initialize one-time Athena resources shared by all uploads in this run."""
    log = Logger("initialize_upload_runtime")
    athena_upload = _load_athena_upload_module(athena_root)
    athena_upload.check_aws_credentials()

    catalog = athena_upload.load_catalog(
        "glue", type="glue", region_name=athena_upload.AWS_REGION
    )

    namespaces_ensured: set[str] = set()
    for layer_runtime_cfg in layer_runtime_configs:
        layer_cfg = layer_runtime_cfg.layer_cfg
        glue_database = layer_cfg.glue_database
        if glue_database in namespaces_ensured:
            continue
        athena_upload.ensure_namespace_glue(catalog, glue_database)
        namespaces_ensured.add(glue_database)

    table_cache: dict[tuple[str, str], Any] = {}
    for layer_runtime_cfg in layer_runtime_configs:
        layer_cfg = layer_runtime_cfg.layer_cfg
        table_name_safe = athena_upload.sanitize_table_name(layer_cfg.table_name)
        table_key = (layer_cfg.glue_database, table_name_safe)
        if table_key in table_cache:
            continue
        try:
            table_cache[table_key] = catalog.load_table(table_key)
        except athena_upload.pyiceberg.exceptions.NoSuchTableError:
            continue

    log.info(
        "Initialized upload runtime: "
        f"databases={len(namespaces_ensured)}, cached_tables={len(table_cache)}"
    )

    return UploadRuntime(
        athena_upload=athena_upload,
        catalog=catalog,
        table_cache=table_cache,
        namespaces_ensured=namespaces_ensured,
        comments_synced=set(),
    )


def _load_layer_metadata(
    layer_definitions_path: Path | None,
    layer_id: str | None,
) -> tuple[dict[str, str], dict[str, str], str]:
    """Load column comments and dtypes from layer_definitions for one layer_id.

    This is intentionally non-interactive for batch automation. If metadata
    loading fails, processing continues with defaults.
    """
    log = Logger("_load_layer_metadata")
    comments: dict[str, str] = {
        "huc8": "Hydrologic Unit Code (HUC8) used for ingestion filtering and partition transforms.",
    }
    dtypes: dict[str, str] = {"huc8": "STRING"}

    if layer_definitions_path is None or layer_id is None:
        return comments, dtypes, ""

    if not layer_definitions_path.is_file():
        log.warning(
            f"Layer definitions file not found: {layer_definitions_path}. Continuing without layer metadata."
        )
        return comments, dtypes, ""

    try:
        with layer_definitions_path.open("r", encoding="utf-8") as f:
            layer_defs = json.load(f)
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        log.error(
            f"Could not parse layer definitions file {layer_definitions_path}: {exc}. Continuing without layer metadata."
        )
        return comments, dtypes, ""

    if not isinstance(layer_defs, dict):
        return comments, dtypes, ""

    selected: dict[str, Any] | None = None
    layers = layer_defs.get("layers", [])
    if isinstance(layers, list) and layers:
        layer_id_norm = str(layer_id).strip().lower()

        exact_id = [
            layer
            for layer in layers
            if isinstance(layer, dict)
            and str(layer.get("layer_id", "")).strip().lower() == layer_id_norm
        ]
        if len(exact_id) == 1:
            selected = exact_id[0]

        if selected is None:
            exact_name = [
                layer
                for layer in layers
                if isinstance(layer, dict)
                and str(layer.get("layer_name", "")).strip().lower() == layer_id_norm
            ]
            if len(exact_name) == 1:
                selected = exact_name[0]

        if selected is None and len(layers) == 1 and isinstance(layers[0], dict):
            selected = layers[0]

    if selected is None:
        return comments, dtypes, ""

    for col in selected.get("columns", []):
        if not isinstance(col, dict):
            continue
        name = str(col.get("name", "")).strip().lower()
        if not name:
            continue
        desc = str(col.get("description", "")).strip()
        dtype = str(col.get("dtype", "")).strip().upper()
        if desc:
            comments[name] = desc[:252] + "..." if len(desc) > 255 else desc
        if dtype:
            dtypes[name] = dtype

    layer_description = str(selected.get("description", "")).strip()
    return comments, dtypes, layer_description


def build_layer_runtime_configs(
    layer_configs: list[LayerProcessConfig],
) -> list[LayerRuntimeConfig]:
    """Load per-layer config and metadata once for reuse across all HUC8s."""
    runtime_configs: list[LayerRuntimeConfig] = []

    for layer_cfg in layer_configs:
        cfg = load_config(layer_cfg.config_path)
        field_map_config = _load_field_map_config(layer_cfg.config_path, cfg)

        layer_defs_path = None
        layer_defs_rel = cfg.get("layer_definitions")
        if layer_defs_rel:
            layer_defs_path = Path(layer_defs_rel)
            if not layer_defs_path.is_absolute():
                layer_defs_path = (
                    layer_cfg.config_path.parent / layer_defs_path
                ).resolve()

        layer_id = str(cfg.get("layer_id")) if cfg.get("layer_id") else None
        comments, dtypes, layer_description = _load_layer_metadata(
            layer_defs_path,
            layer_id,
        )

        runtime_configs.append(
            LayerRuntimeConfig(
                layer_cfg=layer_cfg,
                vector_cfg=cfg,
                field_map_config=field_map_config,
                layer_definitions_path=layer_defs_path,
                layer_id=layer_id,
                column_comments=comments,
                column_dtypes=dtypes,
                layer_description=layer_description,
            )
        )

    return runtime_configs


def _wait_for_athena_query(
    athena_client: Any,
    query_execution_id: str,
    timeout_seconds: int = 600,
) -> None:
    """Wait for Athena query completion and raise on failure/cancel/timeout."""
    started = time.time()
    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status_obj = result.get("QueryExecution", {}).get("Status", {})
        state = str(status_obj.get("State", "")).upper()

        if state == "SUCCEEDED":
            return

        if state in {"FAILED", "CANCELLED"}:
            reason = status_obj.get("StateChangeReason", "")
            raise RuntimeError(
                f"Athena query {query_execution_id} {state.lower()}: {reason}"
            )

        if time.time() - started > timeout_seconds:
            raise TimeoutError(
                f"Athena query {query_execution_id} timed out after {timeout_seconds} seconds"
            )

        time.sleep(1.0)


def _build_huc8_prefix_partition_spec(schema: Any) -> Any:
    """Build an Iceberg hidden partition spec using truncate(2, huc8)."""
    partitioning_module = importlib.import_module("pyiceberg.partitioning")
    transforms_module = importlib.import_module("pyiceberg.transforms")

    PartitionField = partitioning_module.PartitionField
    PartitionSpec = partitioning_module.PartitionSpec
    TruncateTransform = transforms_module.TruncateTransform

    huc8_field = schema.find_field("huc8")
    source_id = int(huc8_field.field_id)
    return PartitionSpec(
        PartitionField(
            source_id=source_id,
            field_id=1000,
            transform=TruncateTransform(2),
            name="huc2",
        )
    )


def _delete_huc8_rows_from_athena(
    athena_upload: ModuleType,
    glue_database: str,
    table_name_safe: str,
    huc8: str,
) -> None:
    """Delete existing rows for one HUC8 before re-appending replacement rows."""
    log = Logger("_delete_huc8_rows_from_athena")

    if len(huc8) != 8 or not huc8.isdigit():
        raise ValueError(f"Invalid HUC8 for Athena delete: {huc8}")

    query = f"DELETE FROM {glue_database}.{table_name_safe} WHERE huc8 = '{huc8}'"

    result_bucket = os.environ.get(
        "ATHENA_QUERY_RESULTS_BUCKET",
        "riverscapes-athena-output",
    )
    output_location = f"s3://{result_bucket}/athena_query_results"

    athena = athena_upload.boto3.client(
        "athena",
        region_name=athena_upload.AWS_REGION,
    )

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": glue_database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    query_execution_id = str(response["QueryExecutionId"])
    _wait_for_athena_query(athena, query_execution_id)
    log.info(
        f"Reprocess cleanup complete for {glue_database}.{table_name_safe} huc8={huc8}."
    )


def upload_gpkg_layer_to_athena_create_or_append(
    gpkg_path: Path,
    gpkg_layer: str,
    layer_runtime_cfg: LayerRuntimeConfig,
    upload_runtime: UploadRuntime,
    target_crs: str = "EPSG:4326",
    reprocess_existing: bool = False,
    huc8_value: str | None = None,
) -> tuple[int, str, str]:
    """Upload one GeoPackage layer to Athena Iceberg with create-or-append behavior.

    Behavior:
    - If table does not exist: create table and append rows.
    - If table exists: append rows directly.
    """
    log = Logger("upload_gpkg_layer_to_athena_create_or_append")
    athena_upload = upload_runtime.athena_upload
    layer_cfg = layer_runtime_cfg.layer_cfg
    glue_database = layer_cfg.glue_database
    table_name = layer_cfg.table_name
    s3_location = layer_cfg.s3_location

    gdf = gpd.read_file(gpkg_path, layer=gpkg_layer)
    gdf.columns = [str(c).lower() for c in gdf.columns]
    if "huc8" not in gdf.columns:
        raise ValueError("Expected required 'huc8' column before Athena upload")
    gdf["huc8"] = gdf["huc8"].astype(str)
    comments = layer_runtime_cfg.column_comments
    dtypes = layer_runtime_cfg.column_dtypes
    layer_description = layer_runtime_cfg.layer_description

    schema = athena_upload.derive_iceberg_schema(
        gdf,
        column_comments=comments,
        column_dtypes=dtypes,
    )

    is_geo = isinstance(gdf, gpd.GeoDataFrame) and hasattr(gdf, "geometry")
    if is_geo:
        crs_wkt = pyproj.CRS(target_crs).to_wkt()
        arrow_table = athena_upload.prepare_arrow_table_spatial(
            gdf,
            schema,
            target_crs,
            column_dtypes=dtypes,
        )
    else:
        crs_wkt = ""
        arrow_table = athena_upload.prepare_arrow_table_tabular(
            gdf,
            schema,
            column_dtypes=dtypes,
        )

    table_name_safe = athena_upload.sanitize_table_name(table_name)
    table_key = (glue_database, table_name_safe)
    table_action = "append"
    iceberg_table = upload_runtime.table_cache.get(table_key)
    if iceberg_table is None:
        if glue_database not in upload_runtime.namespaces_ensured:
            athena_upload.ensure_namespace_glue(upload_runtime.catalog, glue_database)
            upload_runtime.namespaces_ensured.add(glue_database)

        try:
            iceberg_table = upload_runtime.catalog.load_table(table_key)
        except athena_upload.pyiceberg.exceptions.NoSuchTableError:
            partition_spec = _build_huc8_prefix_partition_spec(schema)
            iceberg_table = upload_runtime.catalog.create_table(
                identifier=table_key,
                schema=schema,
                partition_spec=partition_spec,
                location=s3_location,
                properties={
                    "geo.crs_wkt": crs_wkt,
                    "write.format.default": "parquet",
                },
            )
            log.info(
                f"Created {glue_database}.{table_name_safe} with partition spec: truncate(2, huc8)"
            )
            table_action = "create"

        upload_runtime.table_cache[table_key] = iceberg_table

    if table_action != "create" and reprocess_existing:
        if huc8_value is None:
            raise ValueError("reprocess_existing=True requires huc8_value")
        _delete_huc8_rows_from_athena(
            athena_upload,
            glue_database,
            table_name_safe,
            huc8_value,
        )
        # DELETE creates a new snapshot; reload table metadata to avoid stale commits.
        iceberg_table = upload_runtime.catalog.load_table(table_key)
        upload_runtime.table_cache[table_key] = iceberg_table
        table_action = "replace_huc8"

    if table_key not in upload_runtime.comments_synced:
        athena_upload.update_glue_column_comments(
            glue_database,
            table_name_safe,
            comments,
            layer_description,
        )
        upload_runtime.comments_synced.add(table_key)

    athena_upload.append_in_chunks(
        iceberg_table,
        arrow_table,
        f"{glue_database}.{table_name_safe}",
    )

    rows_uploaded = int(arrow_table.num_rows)
    log.info(
        f"Athena upload complete for {glue_database}.{table_name_safe}: rows={rows_uploaded:,}, action={table_action}"
    )
    return rows_uploaded, table_action, table_name_safe


def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    """Return SQLite table columns for the given table."""
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def ensure_processing_columns(conn: sqlite3.Connection) -> None:
    """Ensure ledger_huc8_status has columns needed for transform/load steps."""
    existing = _get_table_columns(conn, "ledger_huc8_status")
    required: dict[str, str] = {
        "fgdb_path": "TEXT",
        "process_last_checked_utc": "TEXT",
    }

    for key in ("wetlands", "riparian"):
        required[f"{key}_status"] = "TEXT"
        required[f"{key}_current_step"] = "TEXT"
        required[f"{key}_rows_prepped"] = "INTEGER"
        required[f"{key}_rows_uploaded"] = "INTEGER"
        required[f"{key}_table_action"] = "TEXT"
        required[f"{key}_last_error"] = "TEXT"
        required[f"{key}_last_note"] = "TEXT"
        required[f"{key}_updated_utc"] = "TEXT"

        for vp_key in _VP_SUMMARY_KEYS:
            required[f"{key}_vp_{vp_key}"] = "INTEGER"

    for col, dtype in required.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE ledger_huc8_status ADD COLUMN {col} {dtype}")

    conn.commit()


def _replace_huc8_row(
    conn: sqlite3.Connection,
    huc8: str,
    updates: dict[str, Any],
) -> None:
    """Merge updates into a single ledger row for a HUC8 via delete+insert."""
    cols = sorted(_get_table_columns(conn, "ledger_huc8_status"))
    if "huc8" not in cols:
        raise ValueError("ledger_huc8_status table missing required 'huc8' column")

    row = conn.execute(
        "SELECT * FROM ledger_huc8_status WHERE huc8 = ? LIMIT 1",
        (huc8,),
    ).fetchone()

    current: dict[str, Any] = {c: None for c in cols}
    current["huc8"] = huc8
    if row is not None:
        # row ordering aligns with PRAGMA table_info order, which matches SELECT *
        pragma_rows = conn.execute("PRAGMA table_info(ledger_huc8_status)").fetchall()
        ordered_cols = [str(r[1]) for r in pragma_rows]
        for i, col_name in enumerate(ordered_cols):
            if col_name in current and i < len(row):
                current[col_name] = row[i]

    for key, value in updates.items():
        if key in current:
            current[key] = value

    conn.execute("DELETE FROM ledger_huc8_status WHERE huc8 = ?", (huc8,))

    placeholders = ", ".join("?" for _ in cols)
    col_sql = ", ".join(cols)
    values = [current[c] for c in cols]
    conn.execute(
        f"INSERT INTO ledger_huc8_status ({col_sql}) VALUES ({placeholders})",
        values,
    )
    conn.commit()


def update_layer_status(
    conn: sqlite3.Connection,
    huc8: str,
    layer_key: str,
    status: str,
    rows_prepped: int | None = None,
    rows_uploaded: int | None = None,
    table_action: str | None = None,
    error: str | None = None,
    error_step: str | None = None,
    note: str | None = None,
    fgdb_path: Path | None = None,
    vp_stats: dict[str, Any] | None = None,
) -> None:
    """Update per-layer process state in ledger_huc8_status."""
    now_utc = utc_now_iso()
    updates: dict[str, Any] = {
        f"{layer_key}_status": status,
        f"{layer_key}_updated_utc": now_utc,
        "process_last_checked_utc": now_utc,
    }

    status_step = _status_to_step(status)
    if status_step is not None:
        updates[f"{layer_key}_current_step"] = status_step

    if rows_prepped is not None:
        updates[f"{layer_key}_rows_prepped"] = int(rows_prepped)
    if rows_uploaded is not None:
        updates[f"{layer_key}_rows_uploaded"] = int(rows_uploaded)
    if table_action is not None:
        updates[f"{layer_key}_table_action"] = table_action
    if error is not None:
        scoped_error = _format_step_error(error_step or "unknown", error)
        updates[f"{layer_key}_last_error"] = scoped_error[:2000]
    elif status != STATUS_FAILED:
        # Clear stale failure text when a step later succeeds or is non-fatal.
        updates[f"{layer_key}_last_error"] = None
    if note is not None:
        updates[f"{layer_key}_last_note"] = note[:2000]
    if fgdb_path is not None:
        updates["fgdb_path"] = str(fgdb_path)

    updates.update(_build_vp_summary_fields(layer_key, vp_stats))

    _replace_huc8_row(conn, huc8, updates)


def process_one_huc8_layer(
    conn: sqlite3.Connection,
    zip_path: Path,
    layer_runtime_cfg: LayerRuntimeConfig,
    extract_root: Path,
    output_root: Path,
    garbage_root: Path,
    upload_runtime: UploadRuntime,
    reprocess_existing: bool = False,
) -> ProcessResult:
    """Process one HUC8 for one layer key (wetlands or riparian)."""
    log = Logger("process_one_huc8_layer")
    layer_cfg = layer_runtime_cfg.layer_cfg
    huc8 = extract_huc8_from_zip_name(zip_path)
    current_step = STEP_UNZIP

    try:
        fgdb_path = extract_filegdb_from_zip(zip_path, extract_root)
        update_layer_status(
            conn,
            huc8,
            layer_cfg.key,
            status=STATUS_SOURCE_READY,
            fgdb_path=fgdb_path,
        )

        try:
            current_step = STEP_VECTOR_PREP
            output_gpkg, _garbage_gpkg, gpkg_layer, vp_stats = (
                run_vector_prep_for_fgdb_layer(
                    fgdb_path,
                    huc8,
                    layer_runtime_cfg,
                    output_root,
                    garbage_root,
                )
            )
        except ValueError as exc:
            err = str(exc)
            if layer_cfg.key == "riparian" and "Could not find riparian layer" in err:
                log.warning(
                    f"HUC8 {huc8} riparian layer missing; recording non-fatal not_present status."
                )
                update_layer_status(
                    conn,
                    huc8,
                    layer_cfg.key,
                    status=STATUS_NOT_PRESENT,
                    fgdb_path=fgdb_path,
                    note="No riparian layer found in source GDB.",
                )
                return ProcessResult(
                    huc8=huc8,
                    layer_key=layer_cfg.key,
                    status=STATUS_NOT_PRESENT,
                )
            raise

        rows_prepped = len(gpd.read_file(output_gpkg, layer=gpkg_layer))
        update_layer_status(
            conn,
            huc8,
            layer_cfg.key,
            status=STATUS_PREPPED,
            rows_prepped=rows_prepped,
            fgdb_path=fgdb_path,
            error=None,
            vp_stats=vp_stats,
        )

        current_step = STEP_UPLOAD
        rows_uploaded, table_action, resolved_table_name = (
            upload_gpkg_layer_to_athena_create_or_append(
                gpkg_path=output_gpkg,
                gpkg_layer=gpkg_layer,
                layer_runtime_cfg=layer_runtime_cfg,
                upload_runtime=upload_runtime,
                target_crs="EPSG:4326",
                reprocess_existing=reprocess_existing,
                huc8_value=huc8,
            )
        )

        update_layer_status(
            conn,
            huc8,
            layer_cfg.key,
            status=STATUS_UPLOADED,
            rows_prepped=rows_prepped,
            rows_uploaded=rows_uploaded,
            table_action=table_action,
            fgdb_path=fgdb_path,
            error=None,
            vp_stats=vp_stats,
        )

        return ProcessResult(
            huc8=huc8,
            layer_key=layer_cfg.key,
            status=STATUS_UPLOADED,
            output_gpkg=output_gpkg,
            rows_prepped=rows_prepped,
            rows_uploaded=rows_uploaded,
            table_action=f"{table_action}:{resolved_table_name}",
        )

    except Exception as exc:
        err = str(exc)
        log.error(f"HUC8 {huc8} {layer_cfg.key} failed: {err}")
        update_layer_status(
            conn,
            huc8,
            layer_cfg.key,
            status=STATUS_FAILED,
            error=err,
            error_step=current_step,
        )
        return ProcessResult(
            huc8=huc8,
            layer_key=layer_cfg.key,
            status=STATUS_FAILED,
            error=err,
        )


def build_layer_configs_from_args(args: argparse.Namespace) -> list[LayerProcessConfig]:
    """Build wetlands/riparian process config list from CLI args."""
    return [
        LayerProcessConfig(
            key="wetlands",
            fgdb_suffix="Wetlands",
            config_path=Path(args.config_wetlands).resolve(),
            glue_database=args.glue_database,
            table_name=args.table_wetlands,
            s3_location=args.s3_location_wetlands,
        ),
        LayerProcessConfig(
            key="riparian",
            fgdb_suffix="Riparian",
            config_path=Path(args.config_riparian).resolve(),
            glue_database=args.glue_database,
            table_name=args.table_riparian,
            s3_location=args.s3_location_riparian,
        ),
    ]


def process_huc8_zip(
    zip_path: Path,
    ledger_path: Path,
    layer_runtime_configs: list[LayerRuntimeConfig],
    extract_root: Path,
    output_root: Path,
    garbage_root: Path,
    upload_runtime: UploadRuntime,
    reprocess_existing: bool = False,
) -> list[ProcessResult]:
    """Process one HUC8 zip sequentially for all configured layers."""
    ensure_parent = ledger_path.parent
    ensure_parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(ledger_path) as conn:
        ensure_processing_columns(conn)

        results: list[ProcessResult] = []
        for layer_runtime_cfg in layer_runtime_configs:
            result = process_one_huc8_layer(
                conn,
                zip_path,
                layer_runtime_cfg,
                extract_root,
                output_root,
                garbage_root,
                upload_runtime,
                reprocess_existing,
            )
            results.append(result)
        return results


def is_nonfatal_result(result: ProcessResult) -> bool:
    """Return True when a layer result should not fail a batch run."""
    return result.status == STATUS_UPLOADED or (
        result.layer_key == "riparian" and result.status == STATUS_NOT_PRESENT
    )


def list_nwi_zip_files(zip_dir: Path, pattern: str) -> list[Path]:
    """Return sorted NWI zip files from a directory using a glob pattern."""
    if not zip_dir.is_dir():
        raise NotADirectoryError(f"Zip directory not found: {zip_dir}")
    return sorted(p for p in zip_dir.glob(pattern) if p.is_file())


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Return True when a SQLite table exists."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def is_huc8_fully_uploaded(
    conn: sqlite3.Connection,
    huc8: str,
    layer_configs: list[LayerProcessConfig],
) -> bool:
    """Return True when all configured layers for a HUC8 are marked uploaded."""
    if not _table_exists(conn, "ledger_huc8_status"):
        return False

    cols = _get_table_columns(conn, "ledger_huc8_status")
    needed_cols = [f"{cfg.key}_status" for cfg in layer_configs]
    if any(col not in cols for col in needed_cols):
        return False

    select_cols = ", ".join(needed_cols)
    row = conn.execute(
        f"SELECT {select_cols} FROM ledger_huc8_status WHERE huc8 = ? LIMIT 1",
        (huc8,),
    ).fetchone()
    if row is None:
        return False

    statuses = [str(val or "").strip().lower() for val in row]
    for cfg, status in zip(layer_configs, statuses):
        if status == STATUS_UPLOADED:
            continue
        if cfg.key == "riparian" and status == STATUS_NOT_PRESENT:
            continue
        return False
    return True


def cleanup_huc8_artifacts(
    huc8: str,
    extract_root: Path,
    output_root: Path,
    garbage_root: Path,
) -> tuple[int, int]:
    """Delete extracted and processed per-HUC8 folders.

    Returns:
        (removed_count, missing_count)
    """
    targets = [
        (extract_root / huc8).resolve(),
        (output_root / huc8).resolve(),
        (garbage_root / huc8).resolve(),
    ]
    removed = 0
    missing = 0

    for target in targets:
        if target.exists() and target.is_dir():
            shutil.rmtree(target)
            removed += 1
        else:
            missing += 1

    return removed, missing


def parse_args() -> argparse.Namespace:
    """Parse CLI args for sequential HUC8 processing."""
    parser = argparse.ArgumentParser(
        description="Process one NWI HUC8 zip into wetlands/riparian Athena tables."
    )
    here = Path(__file__).resolve().parent
    dataroot = Path(os.getenv("DATA_ROOT", r"C:\nardata\pydataroot")) / "process_nwi"

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--zip",
        type=Path,
        help="Path to one HU8_########_Watershed.zip",
    )
    input_group.add_argument(
        "--zip-dir",
        type=Path,
        help="Directory containing HU8 zip archives for batch processing.",
    )
    parser.add_argument(
        "--zip-pattern",
        default="HU8_*_Watershed.zip",
        help="Glob pattern used with --zip-dir (default: HU8_*_Watershed.zip).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of zip files to process in this run.",
    )
    parser.add_argument(
        "--reprocess-existing",
        action="store_true",
        help=(
            "Reprocess HUC8 zip files that are already uploaded by deleting existing "
            "Athena rows for that HUC8 before appending replacement rows."
        ),
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop batch run immediately after first failed zip.",
    )
    parser.add_argument(
        "--cleanup",
        dest="cleanup",
        action="store_true",
        default=None,
        help=(
            "Delete extracted and processed per-HUC8 folders after a zip finishes "
            "successfully."
        ),
    )
    parser.add_argument(
        "--no-cleanup",
        dest="cleanup",
        action="store_false",
        help=(
            "Keep extracted and processed per-HUC8 folders after successful processing."
        ),
    )
    parser.add_argument(
        "--ledger",
        type=Path,
        default=here / LEDGER_FILENAME,
        help="Path to NWI processing ledger GeoPackage.",
    )
    parser.add_argument(
        "--config-wetlands",
        dest="config_wetlands",
        type=Path,
        default=here / "config_wetlands.json",
        help="vector_prep config for wetlands layer.",
    )
    parser.add_argument(
        "--config-riparian",
        dest="config_riparian",
        type=Path,
        default=here / "config_riparian.json",
        help="vector_prep config for riparian layer.",
    )

    parser.add_argument(
        "--glue-database",
        default=DEFAULT_GLUE_DATABASE,
        help="Athena/Glue database name for both destination tables.",
    )
    parser.add_argument(
        "--table-wetlands",
        default=DEFAULT_ATHENA_TABLE_WETLANDS,
        help=(
            "Destination Iceberg table name for wetlands "
            f"(default: {DEFAULT_GLUE_DATABASE}.{DEFAULT_ATHENA_TABLE_WETLANDS})."
        ),
    )
    parser.add_argument(
        "--table-riparian",
        default=DEFAULT_ATHENA_TABLE_RIPARIAN,
        help=(
            "Destination Iceberg table name for riparian "
            f"(default: {DEFAULT_GLUE_DATABASE}.{DEFAULT_ATHENA_TABLE_RIPARIAN})."
        ),
    )
    parser.add_argument(
        "--s3-location-wetlands",
        default="s3://riverscapes-athena/ext_raw/us_fws_nwi_wetlands",
        help="S3 location for wetlands Iceberg table (used if create is needed).",
    )
    parser.add_argument(
        "--s3-location-riparian",
        default="s3://riverscapes-athena/ext_raw/us_fws_nwi_riparian",
        help="S3 location for riparian Iceberg table (used if create is needed).",
    )

    parser.add_argument(
        "--extract-root",
        type=Path,
        default=dataroot / DEFAULT_EXTRACT_ROOT,
        help="Folder for extracted FileGDB content.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=dataroot / DEFAULT_OUTPUT_ROOT,
        help="Folder for per-HUC8 processed outputs.",
    )
    parser.add_argument(
        "--garbage-root",
        type=Path,
        default=dataroot / f"{DEFAULT_OUTPUT_ROOT}_garbage",
        help="Folder for per-HUC8 vector_prep garbage outputs.",
    )
    parser.add_argument(
        "--athena-root",
        type=Path,
        default=Path(r"C:\nardata\localcode\athena"),
        help="Path to athena repository root.",
    )

    return parser.parse_args()


def main() -> None:
    """CLI entry point for one-HUC8 process run."""
    args = parse_args()
    log = Logger("NWI Process")

    layer_configs = build_layer_configs_from_args(args)
    for cfg in layer_configs:
        if not cfg.config_path.is_file():
            raise SystemExit(f"Config file not found: {cfg.config_path}")

    layer_runtime_configs = build_layer_runtime_configs(layer_configs)

    ledger_path = args.ledger.resolve()
    extract_root = args.extract_root.resolve()
    output_root = args.output_root.resolve()
    garbage_root = args.garbage_root.resolve()
    athena_root = args.athena_root.resolve()

    log_path = output_root / "process_nwi.log"
    log.setup(log_path=log_path)

    if args.zip is not None:
        zip_paths = [args.zip.resolve()]
    else:
        zip_paths = list_nwi_zip_files(args.zip_dir.resolve(), args.zip_pattern)
        if args.limit is not None:
            zip_paths = zip_paths[: args.limit]

    if not zip_paths:
        raise SystemExit("No zip files matched the selected input arguments.")

    upload_runtime = initialize_upload_runtime(athena_root, layer_runtime_configs)

    run_id = str(uuid.uuid4())
    processed_zip_count = 0
    skipped_zip_count = 0
    failed_zip_count = 0
    cleaned_zip_count = 0
    all_results: list[ProcessResult] = []

    # Default behavior: cleanup on for batch runs, off for single-zip runs.
    cleanup_enabled = (
        args.cleanup if args.cleanup is not None else (args.zip_dir is not None)
    )

    log.title("NWI Batch Process")
    log.info(f"Run ID: {run_id}")
    log.info(f"Zip files selected: {len(zip_paths):,}")
    log.info(f"Cleanup enabled: {cleanup_enabled}")
    log.info(f"Reprocess existing: {args.reprocess_existing}")

    with sqlite3.connect(ledger_path) as conn:
        ensure_processing_columns(conn)

        for idx, zip_path in enumerate(zip_paths, start=1):
            if not zip_path.is_file():
                log.warning(f"[{idx}/{len(zip_paths)}] Skip missing zip: {zip_path}")
                skipped_zip_count += 1
                continue

            huc8 = extract_huc8_from_zip_name(zip_path)
            already_uploaded = is_huc8_fully_uploaded(conn, huc8, layer_configs)
            if already_uploaded and not args.reprocess_existing:
                log.info(
                    f"[{idx}/{len(zip_paths)}] Skip already uploaded HUC8 {huc8}: {zip_path.name}"
                )
                skipped_zip_count += 1
                continue

            log.info(f"[{idx}/{len(zip_paths)}] Processing {zip_path.name}")
            results = process_huc8_zip(
                zip_path=zip_path,
                ledger_path=ledger_path,
                layer_runtime_configs=layer_runtime_configs,
                extract_root=extract_root,
                output_root=output_root,
                garbage_root=garbage_root,
                upload_runtime=upload_runtime,
                reprocess_existing=args.reprocess_existing,
            )
            processed_zip_count += 1
            all_results.extend(results)

            zip_failed = any(not is_nonfatal_result(r) for r in results)
            if zip_failed:
                failed_zip_count += 1
                if args.fail_fast:
                    log.error("Fail-fast enabled; stopping after first failed zip.")
                    break
            elif cleanup_enabled:
                removed, missing = cleanup_huc8_artifacts(
                    huc8,
                    extract_root,
                    output_root,
                    garbage_root,
                )
                cleaned_zip_count += 1
                log.info(
                    f"[{idx}/{len(zip_paths)}] Cleanup complete for HUC8 {huc8}: "
                    f"removed={removed}, missing={missing}"
                )

    ok_layer_count = sum(1 for r in all_results if is_nonfatal_result(r))
    fail_layer_count = len(all_results) - ok_layer_count

    log.title("NWI Process Summary")
    log.info(f"Run ID: {run_id}")
    log.info(
        "Zip summary: "
        f"processed={processed_zip_count:,}, skipped={skipped_zip_count:,}, failed={failed_zip_count:,}"
    )
    log.info(f"Cleanup summary: cleaned={cleaned_zip_count:,}")
    log.info(f"Layer summary: uploaded={ok_layer_count:,}, failed={fail_layer_count:,}")
    for r in all_results:
        msg = (
            f"huc8={r.huc8} layer={r.layer_key} status={r.status} "
            f"prepped={r.rows_prepped} uploaded={r.rows_uploaded} action={r.table_action}"
        )
        if r.error:
            msg += f" error={r.error}"
        log.info(msg)

    if failed_zip_count > 0 or fail_layer_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
