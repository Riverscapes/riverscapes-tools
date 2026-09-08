"""Generic vector preparation orchestrator.

Orchestrates sequential, idempotent preparation steps for GIS layers before
they are uploaded to the Riverscapes lakehouse.  Steps can be run individually
so you can review output in QGIS and re-run from any point without starting over.

Steps:
    1. fetch_metadata      - Fetch field metadata from ArcGIS Hub; populate layer_definitions_source.json.
    2. dissolve_duplicates - [OPTIONAL] Dissolve same-attribute features into multipart polygons.
    3. vector_prep         - Clean, validate, simplify, and map fields (delegates to vector_prep module).
    4. add_unique_id_and_name - Add deterministic rs_reports_rowid (UUID5) and rs_reports_rowname columns.
    5. upload_instructions - Print the iceberg upload / next-step guidance.
    6. build_legacy_ddl    - [LEGACY] Generate SQL DDL for Athena/Hive (isolated; not in default path).

Config: A single ``config.json`` in the layer folder provides all parameters for
    both orchestrate and vector_prep.  add_unique_id_and_name source fields are auto-added to
        the field_map so they survive vector_prep - no need to list them manually.

Usage:
    uv run python -m vector_prep.orchestrate --config layers/us_blm_natl_wild_horse_burro/config.json
    uv run python -m vector_prep.orchestrate --config ... --from-step add_unique_id_and_name --to-step add_unique_id_and_name
    uv run python -m vector_prep.orchestrate --config ... --only-step add_unique_id_and_name --dry-run

Lorin / GitHub Copilot 2026
"""

from __future__ import annotations

import argparse
import json
import os
import re
import string
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import geopandas as gpd
import pandas as pd
from rsxml import Logger

from vector_prep.fetch_arcgis_metadata import (
    fetch_columns_from_hub_url,
    update_layer_definitions,
)
from vector_prep.lib.field_map import FieldMapConfig, load_and_validate_field_map
from vector_prep.lib.output import output_gdf
from vector_prep.lib.report import write_markdown_report
from vector_prep.vector_prep import vector_prep

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Static OID namespace for reproducing deterministic UUID5s across re-runs.
_RS_ROW_ID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
_MAX_ROWNAME_LENGTH = 150

# Non-breaking space and other invisible characters to strip from string values.
_REPLACE_CHARS = ["\u00a0"]

# Ordered list of all known pipeline steps.
_STEPS: list[str] = [
    "fetch_metadata",
    "dissolve_duplicates",
    "vector_prep",
    "add_unique_id_and_name",
    "upload_instructions",
    "build_legacy_ddl",
]

_DEFAULT_ENABLED_STEPS: list[str] = [
    "fetch_metadata",
    "vector_prep",
    "add_unique_id_and_name",
    "upload_instructions",
]

# ---------------------------------------------------------------------------
# Step registry
# ---------------------------------------------------------------------------

# Registry mapping step name -> handler callable.
_step_registry: dict[str, Callable[[LayerContext], StepReport]] = {}


@dataclass
class StepReport:
    """Structured report payload returned by each step handler."""

    summary: str
    details: list[str] = field(default_factory=list)
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    artifacts: list[str] = field(default_factory=list)
    skipped: bool = False


def step(name: str):
    """Decorator that registers a function as a named pipeline step."""

    def decorator(fn: Callable[[LayerContext], StepReport]):
        if name in _step_registry:
            raise ValueError(f"Duplicate step name: {name}")
        _step_registry[name] = fn
        return fn

    return decorator


def _step_description(name: str) -> str:
    """Return a one-line description for a registered step."""
    handler = _step_registry.get(name)
    if handler is None or not handler.__doc__:
        return "No step description provided."
    for line in handler.__doc__.strip().splitlines():
        clean = line.strip()
        if clean:
            return clean
    return "No step description provided."


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass
class LayerConfig:
    """Unified configuration loaded from a layer's config.json.

    All string values have environment variables expanded (``${VAR}`` syntax).
    Relative paths (layer_definitions) are resolved against the config file's
    parent directory.
    """

    # ---- Identity ----
    layer_id: str
    source_url: str
    source_title: str
    source_category: str
    snapshot_id: str

    # ---- I/O ----
    input_vector_path: str
    input_layer_name: str | None = None
    output_vector_path: Path | None = None
    garbage_vector_path: Path | None = None
    log_file_path: Path | None = None
    layer_definitions_source_path: Path = field(
        default_factory=lambda: Path("layer_definitions_source.json")
    )
    layer_definitions_path: Path = field(
        default_factory=lambda: Path("layer_definitions.json")
    )

    # ---- vector_prep parameters ----
    tolerance: float = 0.0
    epsg: int = 5070
    min_size: float = 1.0
    min_size_drop: bool = False
    field_map: dict[str, str] = field(default_factory=dict)
    dissolve_exclude_columns: list[str] = field(default_factory=list)
    enabled_steps: list[str] | None = None
    orchestrate_report_path: Path | None = None

    # ---- Enrichment ----
    row_id_source: str | None = None
    row_name_source: str | None = None
    name_uniqueness: Literal["warn", "error"] = "warn"

    # ---- Misc ----
    special_notes: str = ""
    data_prep_operator: str = ""
    verbose: bool = False

    @classmethod
    def from_json(cls, config_path: Path) -> LayerConfig:
        """Load config, expanding env vars and resolving relative paths."""
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        def _expand(val: Any) -> Any:
            if isinstance(val, str):
                return os.path.expandvars(val)
            if isinstance(val, dict):
                return {k: _expand(v) for k, v in val.items()}
            if isinstance(val, list):
                return [_expand(v) for v in val]
            return val

        data = _expand(data)

        meta: dict = data.get("metadata", {})
        params: dict = data.get("parameters", {})
        add_unique_cfg: dict = params.get("add_unique_id_and_name") or params.get(
            "enrich", {}
        )

        raw_steps = params.get("steps")
        enabled_steps: list[str] | None = None
        if raw_steps is not None:
            if not isinstance(raw_steps, list) or any(
                not isinstance(s, str) or not s.strip() for s in raw_steps
            ):
                raise ValueError(
                    "parameters.steps must be a list of non-empty step name strings"
                )
            enabled_steps = [s.strip() for s in raw_steps]

        def _resolve_optional_path(path_val: Any) -> Path | None:
            if not path_val:
                return None
            path_obj = Path(path_val)
            if not path_obj.is_absolute():
                path_obj = (config_path.parent / path_obj).resolve()
            return path_obj

        # Resolve metadata definition paths relative to config directory.
        layer_defs_source_val = params.get(
            "layer_definitions_source", "./layer_definitions_source.json"
        )
        layer_defs_source_path = Path(layer_defs_source_val)
        if not layer_defs_source_path.is_absolute():
            layer_defs_source_path = (
                config_path.parent / layer_defs_source_path
            ).resolve()

        layer_defs_val = params.get("layer_definitions", "./layer_definitions.json")
        layer_defs_path = Path(layer_defs_val)
        if not layer_defs_path.is_absolute():
            layer_defs_path = (config_path.parent / layer_defs_path).resolve()

        output_vector_path = _resolve_optional_path(params.get("output"))
        garbage_vector_path = _resolve_optional_path(params.get("garbage"))
        log_file_path = _resolve_optional_path(params.get("log_file"))
        orchestrate_report_path = _resolve_optional_path(
            params.get("orchestrate_report")
        )

        return cls(
            layer_id=meta.get("layer_id") or params.get("layer_id") or "unknown_layer",
            source_url=meta.get("source_url") or params.get("source_url") or "",
            source_title=meta.get("source_title") or params.get("source_title") or "",
            source_category=meta.get("source_category")
            or params.get("source_category")
            or "custom",
            snapshot_id=meta.get("snapshot_id")
            or params.get("snapshot_id")
            or "latest",
            input_vector_path=params.get("input") or "",
            input_layer_name=params.get("layer") or None,
            output_vector_path=output_vector_path,
            garbage_vector_path=garbage_vector_path,
            log_file_path=log_file_path,
            layer_definitions_source_path=layer_defs_source_path,
            layer_definitions_path=layer_defs_path,
            tolerance=float(params.get("tolerance", 0.0)),
            epsg=int(params.get("epsg", 5070)),
            min_size=float(params.get("min_size", 1.0)),
            min_size_drop=bool(params.get("min_size_drop", False)),
            field_map=params.get("field_map") or {},
            dissolve_exclude_columns=[
                str(c).strip()
                for c in (params.get("dissolve_exclude_columns") or [])
                if str(c).strip()
            ],
            enabled_steps=enabled_steps,
            orchestrate_report_path=orchestrate_report_path,
            row_id_source=add_unique_cfg.get("row_id_source"),
            row_name_source=add_unique_cfg.get("row_name_source"),
            name_uniqueness=add_unique_cfg.get("name_uniqueness", "warn"),
            special_notes=meta.get("special_notes", ""),
            data_prep_operator=meta.get("data_prep_operator", ""),
            verbose=bool(params.get("verbose", False)),
        )

    def extract_template_fields(self) -> list[str]:
        """Return field names referenced in ``row_name_source`` format string."""
        if not self.row_name_source:
            return []
        return [
            fname
            for _, fname, _, _ in string.Formatter().parse(self.row_name_source)
            if fname
        ]

    def effective_field_map(self) -> dict[str, str]:
        """Return the field_map augmented with add_unique_id_and_name source fields.

        Fields referenced by ``row_id_source`` and the rowname template are
        auto-added as identity mappings so they survive vector_prep.  The
        original config file is never modified.
        """
        fm = dict(self.field_map)

        # Auto-add row_id_source field if set and not already mapped.
        if self.row_id_source and self.row_id_source not in fm:
            fm[self.row_id_source] = self.row_id_source

        # Auto-add rowname template fields.
        for fname in self.extract_template_fields():
            if fname not in fm:
                fm[fname] = fname

        return fm


# ---------------------------------------------------------------------------
# Runtime context
# ---------------------------------------------------------------------------


@dataclass
class LayerContext:
    """Mutable state carried through the pipeline run."""

    config_path: Path
    cfg: LayerConfig
    dist_root: Path
    log: Logger = field(default_factory=lambda: Logger("Orchestrate"))

    # Computed paths - set after init.
    output_dir: Path = field(init=False)
    output_file: Path = field(init=False)
    garbage_file: Path = field(init=False)
    runtime_input_path: Path = field(init=False)
    runtime_input_layer_name: str | None = field(init=False)
    orchestrate_report_file: Path = field(init=False)

    def __post_init__(self) -> None:
        if self.cfg.output_vector_path is not None:
            self.output_file = self.cfg.output_vector_path
            self.output_dir = self.output_file.parent
        else:
            source_stub = (
                "usgov_sources"
                if self.cfg.source_category == "usgov"
                else f"raw_{self.cfg.source_category}"
            )
            self.output_dir = (
                self.dist_root / source_stub / self.cfg.layer_id / self.cfg.snapshot_id
            )
            self.output_file = self.output_dir / f"{self.cfg.layer_id}.gpkg"

        if self.cfg.garbage_vector_path is not None:
            self.garbage_file = self.cfg.garbage_vector_path
        else:
            self.garbage_file = self.output_dir / f"{self.cfg.layer_id}_garbage.gpkg"

        self.runtime_input_path = Path(self.cfg.input_vector_path).resolve()
        self.runtime_input_layer_name = self.cfg.input_layer_name

        if self.cfg.orchestrate_report_path is not None:
            self.orchestrate_report_file = self.cfg.orchestrate_report_path
        else:
            self.orchestrate_report_file = (
                self.output_dir / f"{self.cfg.layer_id}_orchestrate_report.md"
            )


def _ensure_report_header(ctx: LayerContext) -> None:
    """Create the consolidated orchestrate markdown report if it does not exist."""
    ctx.orchestrate_report_file.parent.mkdir(parents=True, exist_ok=True)
    if ctx.orchestrate_report_file.exists():
        return

    run_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(ctx.orchestrate_report_file, "w", encoding="utf-8") as f:
        f.write(f"# Orchestrate Report: {ctx.cfg.layer_id}\n\n")
        f.write(
            "This file is persistent across independent runs. "
            "Each step section is overwritten in place when that step executes.\n\n"
        )
        f.write(f"- Config: `{ctx.config_path}`\n")
        f.write(f"- Created: {run_utc}\n")
        f.write(f"- Output: `{ctx.output_file}`\n")
        f.write(f"- Known Steps: {', '.join(_STEPS)}\n\n")


def _render_step_report_section(
    step_name: str,
    status: str,
    elapsed_sec: float,
    report: StepReport,
) -> str:
    """Render one step result markdown section."""
    lines: list[str] = [
        f"## {step_name}\n\n",
        f"- Status: {status}\n",
        f"- Duration: {elapsed_sec:.2f} sec\n",
        f"- Summary: {report.summary}\n",
    ]
    if report.inputs:
        lines.append("- Inputs:\n")
        lines.extend(f"  - `{item}`\n" for item in report.inputs)
    if report.outputs:
        lines.append("- Outputs:\n")
        lines.extend(f"  - `{item}`\n" for item in report.outputs)
    if report.artifacts:
        lines.append("- Artifacts:\n")
        lines.extend(f"  - `{item}`\n" for item in report.artifacts)
    if report.details:
        lines.append("\n### Details\n\n")
        lines.extend(f"- {line}\n" for line in report.details)
    lines.append("\n")
    return "".join(lines)


def _append_step_report(
    ctx: LayerContext,
    step_name: str,
    status: str,
    elapsed_sec: float,
    report: StepReport,
) -> None:
    """Upsert one step result section in the consolidated orchestrate report."""
    _ensure_report_header(ctx)

    section = _render_step_report_section(step_name, status, elapsed_sec, report)
    section_pattern = re.compile(rf"(?ms)^## {re.escape(step_name)}\\n.*?(?=^## |\\Z)")

    with open(ctx.orchestrate_report_file, "r", encoding="utf-8") as f:
        content = f.read()

    if section_pattern.search(content):
        updated = section_pattern.sub(section, content, count=1)
    else:
        separator = "" if content.endswith("\n\n") else "\n"
        updated = f"{content}{separator}{section}"

    with open(ctx.orchestrate_report_file, "w", encoding="utf-8") as f:
        f.write(updated)


# ===================================================================
# Helpers
# ===================================================================


def _upsert_layer_columns(
    defs_path: Path,
    layer_id: str,
    new_columns: list[dict[str, str]],
) -> None:
    """Upsert *new_columns* into an existing layer's columns array by name.

    Existing columns with the same name are replaced; new columns are appended.
    The file is updated in-place.
    """
    if not defs_path.exists():
        return

    with open(defs_path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    for layer in doc.get("layers", []):
        if layer.get("layer_id") != layer_id:
            continue
        existing = {col["name"]: col for col in layer.get("columns", [])}
        for nc in new_columns:
            existing[nc["name"]] = nc
        layer["columns"] = list(existing.values())
        break

    with open(defs_path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=4, ensure_ascii=False)
        f.write("\n")


def _record_add_unique_columns(ctx: LayerContext) -> None:
    """Record the add_unique_id_and_name columns in layer_definitions.json so the schema
    doc stays in sync with what was actually produced."""
    defs_path = ctx.cfg.layer_definitions_path
    if not defs_path.exists():
        ctx.log.info(
            "No layer_definitions.json found; skipping enrich metadata recording."
        )
        return

    add_unique_columns: list[dict[str, str]] = []
    if ctx.cfg.row_id_source:
        add_unique_columns.append(
            {
                "name": "rs_reports_rowid",
                "friendly_name": "Riverscapes Reports Unique Row ID",
                "dtype": "STRING",
                "description": (
                    f"Deterministic UUID5 derived from '{ctx.cfg.row_id_source}' "
                    f"using Riverscapes OID namespace."
                ),
            }
        )
    if ctx.cfg.row_name_source:
        add_unique_columns.append(
            {
                "name": "rs_reports_rowname",
                "friendly_name": "Riverscapes Reports Unique Row Name",
                "dtype": "STRING",
                "description": (
                    f"Display name formatted as: {ctx.cfg.row_name_source}"
                ),
            }
        )

    if add_unique_columns:
        _upsert_layer_columns(defs_path, ctx.cfg.layer_id, add_unique_columns)
        ctx.log.info(
            f"Recorded {len(add_unique_columns)} add_unique column(s) in {defs_path.name}."
        )


# ===================================================================
# Pipeline steps
# ===================================================================


@step("fetch_metadata")
def _step_fetch_metadata(ctx: LayerContext) -> StepReport:
    """Pull field metadata from ArcGIS Hub and populate layer_definitions_source.json.

    Skips if the layer_id already exists in the definitions file.
    """
    if not ctx.cfg.source_url:
        ctx.log.warning("No source_url configured; skipping metadata fetch.")
        return StepReport(
            summary="No source_url configured.",
            details=["Metadata fetch skipped."],
            skipped=True,
        )

    defs_path = ctx.cfg.layer_definitions_source_path
    if not defs_path.parent.exists():
        defs_path.parent.mkdir(parents=True, exist_ok=True)

    if not defs_path.exists():
        with open(defs_path, "w", encoding="utf-8") as f:
            json.dump({"layers": []}, f, indent=4)

    with open(defs_path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    if any(
        layer.get("layer_id") == ctx.cfg.layer_id for layer in doc.get("layers", [])
    ):
        ctx.log.info(
            f"Layer '{ctx.cfg.layer_id}' already exists in {defs_path.name} - skipping fetch."
        )
        return StepReport(
            summary="Layer already present in layer_definitions_source; no fetch needed.",
            inputs=[ctx.cfg.source_url],
            outputs=[str(defs_path)],
            artifacts=[str(defs_path)],
            skipped=True,
        )

    ctx.log.info(f"Fetching metadata from {ctx.cfg.source_url} ...")
    columns = fetch_columns_from_hub_url(ctx.cfg.source_url)

    # Only record what this step actually produces: raw source columns.
    # add_unique_id_and_name columns (rs_reports_rowid, rs_reports_rowname) are
    # documented by that step; geometry and bbox columns are produced downstream
    # (vector_prep / iceberg upload) and should be documented there.

    # Write skeleton then update columns.
    new_layer: dict = {
        "layer_id": ctx.cfg.layer_id,
        "layer_name": ctx.cfg.input_layer_name or ctx.cfg.layer_id,
        "source_url": ctx.cfg.source_url,
        "source_title": ctx.cfg.source_title,
        "columns": [],
    }
    doc.setdefault("layers", []).append(new_layer)
    with open(defs_path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=4, ensure_ascii=False)
        f.write("\n")

    update_layer_definitions(str(defs_path), ctx.cfg.layer_id, columns)
    ctx.log.info(f"Metadata recorded under '{ctx.cfg.layer_id}' in {defs_path.name}.")
    return StepReport(
        summary=f"Fetched and recorded {len(columns)} source column definitions.",
        inputs=[ctx.cfg.source_url],
        outputs=[str(defs_path)],
        artifacts=[str(defs_path)],
    )


@step("dissolve_duplicates")
def _step_dissolve_duplicates(ctx: LayerContext) -> StepReport:
    """Optionally dissolve duplicate-attribute polygons into multipart geometries.

    This step groups features by non-geometry columns (excluding any
    configured dissolve_exclude_columns) and unions geometries within each group.
    """
    input_path = ctx.runtime_input_path
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found for dissolve step: {input_path}")

    ctx.output_dir.mkdir(parents=True, exist_ok=True)
    dissolve_path = ctx.output_dir / f"{ctx.cfg.layer_id}_post_dissolve_duplicates.gpkg"
    dissolve_layer = ctx.runtime_input_layer_name or ctx.cfg.layer_id

    if dissolve_path.exists():
        dissolve_path.unlink()

    ctx.log.info(f"Loading input for dissolve step: {input_path}")
    gdf = gpd.read_file(input_path, layer=ctx.runtime_input_layer_name)
    if gdf.empty:
        raise ValueError("Dissolve step found zero features in the input layer.")

    geom_col = gdf.geometry.name
    exclude_lookup = {name.lower() for name in ctx.cfg.dissolve_exclude_columns}
    attr_cols = [
        c for c in gdf.columns if c != geom_col and c.lower() not in exclude_lookup
    ]
    excluded_present = [
        c for c in gdf.columns if c != geom_col and c.lower() in exclude_lookup
    ]

    if ctx.cfg.dissolve_exclude_columns:
        if excluded_present:
            ctx.log.info(
                "Dissolve exclusion columns applied: "
                + ", ".join(sorted(excluded_present))
            )
        missing_excludes = [
            c
            for c in ctx.cfg.dissolve_exclude_columns
            if c.lower() not in {col.lower() for col in gdf.columns}
        ]
        if missing_excludes:
            ctx.log.warning(
                "Requested dissolve exclusion columns not found in input: "
                + ", ".join(sorted(missing_excludes))
            )

    if not attr_cols:
        ctx.log.warning(
            "No grouping columns remain after applying dissolve exclusions; "
            "dissolve would collapse all features to one row. Skipping step."
        )
        return StepReport(
            summary="No grouping columns remain for dissolve.",
            details=["Dissolve skipped to avoid collapsing all rows."],
            skipped=True,
        )

    if not gdf.duplicated(subset=attr_cols, keep=False).any():
        ctx.log.info(
            "No duplicate non-geometry rows found; dissolve step produced no changes."
        )
        return StepReport(
            summary="No duplicate non-geometry rows found.",
            skipped=True,
        )

    try:
        dissolved = gdf.dissolve(by=attr_cols, as_index=False, dropna=False)
    except TypeError:
        # Backward compatibility for geopandas versions without dropna.
        dissolved = gdf.dissolve(by=attr_cols, as_index=False)

    output_gdf(dissolved, str(dissolve_path), dissolve_layer)

    ctx.runtime_input_path = dissolve_path
    ctx.runtime_input_layer_name = dissolve_layer

    removed = len(gdf) - len(dissolved)
    ctx.log.info(
        "Dissolve complete - "
        f"{len(gdf):,} input feature(s), {len(dissolved):,} dissolved feature(s), "
        f"{removed:,} merged duplicate feature(s)."
    )
    ctx.log.info(f"Downstream input switched to dissolved file: {dissolve_path}")
    return StepReport(
        summary="Dissolved duplicate-attribute features into consolidated geometries.",
        inputs=[str(input_path)],
        outputs=[str(dissolve_path)],
        details=[
            f"Grouping columns used: {len(attr_cols):,}",
            f"Excluded columns used: {', '.join(sorted(excluded_present)) if excluded_present else 'none'}",
            f"Input features: {len(gdf):,}",
            f"Output features: {len(dissolved):,}",
            f"Merged duplicate features: {removed:,}",
        ],
        artifacts=[str(dissolve_path)],
    )


@step("vector_prep")
def _step_vector_prep(ctx: LayerContext) -> StepReport:
    """Clean, validate, and simplify geometries; apply field mapping."""
    ctx.output_dir.mkdir(parents=True, exist_ok=True)

    log_file_path = ctx.cfg.log_file_path
    if log_file_path is None:
        log_file_path = ctx.output_dir / f"{ctx.output_file.stem}_vector_prep.log"
    ctx.log.setup(log_path=str(log_file_path), verbose=ctx.cfg.verbose)
    ctx.log.info(f"Vector prep log file: {log_file_path}")

    effective_fm = ctx.cfg.effective_field_map()

    field_map_config: FieldMapConfig | None = None
    if effective_fm and ctx.cfg.layer_definitions_path.exists():
        auto_added = {k for k in effective_fm if k not in (ctx.cfg.field_map or {})}
        enrich_source_fields = set(auto_added)
        if ctx.cfg.row_id_source:
            enrich_source_fields.add(ctx.cfg.row_id_source)
        enrich_source_fields.update(ctx.cfg.extract_template_fields())

        # Allow add_unique-source identity mappings (e.g., GlobalID -> GlobalID)
        # to pass through even if not documented in curated layer_definitions.
        allow_missing_outputs = {
            out_name
            for src_name, out_name in effective_fm.items()
            if src_name in enrich_source_fields and src_name == out_name
        }
        fm_for_validation = {
            src_name: out_name
            for src_name, out_name in effective_fm.items()
            if out_name not in allow_missing_outputs
        }
        try:
            field_map_config = load_and_validate_field_map(
                fm_for_validation,
                ctx.cfg.layer_definitions_path,
                layer_id=ctx.cfg.layer_id,
            )

            # Re-attach add_unique passthrough mappings after typed-schema validation
            # so vector_prep keeps these fields for downstream enrich logic.
            field_map_config.field_map = effective_fm
        except Exception as e:
            ctx.log.warning(
                f"Field map validation warning: {e}. Passing raw properties."
            )

    if field_map_config and effective_fm:
        auto_added = {k for k in effective_fm if k not in (ctx.cfg.field_map or {})}
        if auto_added:
            ctx.log.info(
                f"Auto-added {len(auto_added)} add_unique field(s) to field_map: "
                f"{', '.join(sorted(auto_added))}"
            )

    stats = vector_prep(
        input_dataset=str(ctx.runtime_input_path),
        layer_name=ctx.runtime_input_layer_name,
        tolerance=ctx.cfg.tolerance,
        epsg=ctx.cfg.epsg,
        output_path=str(ctx.output_file),
        garbage_path=str(ctx.garbage_file),
        min_size=ctx.cfg.min_size,
        min_size_drop=ctx.cfg.min_size_drop,
        field_map_config=field_map_config,
    )

    report_path = ctx.output_dir / f"{ctx.output_file.stem}_report.md"
    written_report = write_markdown_report(stats, str(ctx.garbage_file), report_path)
    ctx.log.info(f"Markdown report written: {written_report}")
    ctx.log.info(f"vector_prep complete - output: {ctx.output_file}")
    return StepReport(
        summary="vector_prep completed.",
        inputs=[str(ctx.runtime_input_path)],
        outputs=[str(ctx.output_file), str(ctx.garbage_file)],
        details=[
            f"Input features processed: {stats.get('input_count', 0):,}",
            f"Output features written: {stats.get('output_count', 0):,}",
            f"Geometry duplicates dropped: {stats.get('geometry_duplicates_dropped', 0):,}",
            f"Row duplicates dropped: {stats.get('row_duplicates_dropped', 0):,}",
        ],
        artifacts=[str(ctx.output_file), str(ctx.garbage_file), str(written_report)],
    )


@step("add_unique_id_and_name")
def _step_add_unique_id_and_name(ctx: LayerContext) -> StepReport:
    """Add deterministic rs_reports_rowid (UUID5) and rs_reports_rowname columns.

    Reads the vector_prep output, computes the new unique id/name columns, validates
    uniqueness and length constraints, then writes back to the same file.
    """
    if not ctx.output_file.exists():
        raise FileNotFoundError(
            f"vector_prep output not found. Run vector_prep step first: {ctx.output_file}"
        )

    id_src = ctx.cfg.row_id_source
    name_tpl = ctx.cfg.row_name_source

    if not id_src or not name_tpl:
        ctx.log.warning(
            "Skipping add_unique_id_and_name: row_id_source or row_name_source not configured."
        )
        return StepReport(
            summary="row_id_source or row_name_source not configured.",
            skipped=True,
        )

    ctx.log.info(f"Loading {ctx.output_file} ...")
    gdf = gpd.read_file(ctx.output_file)
    ctx.log.info(f"Loaded {len(gdf):,} features.")

    # Idempotence: drop any existing add_unique columns before re-computing.
    for col in ("rs_reports_rowid", "rs_reports_rowname"):
        if col in gdf.columns:
            gdf = gdf.drop(columns=[col])

    # ---- Resolve row_id_source through field_map to find output column ----
    col_map = {c.lower(): c for c in gdf.columns}
    effective_fm = ctx.cfg.effective_field_map()

    def _resolve_col(source_name: str) -> str:
        """Resolve a source field name to the actual output column name."""
        out_col = effective_fm.get(source_name, source_name)
        resolved = col_map.get(out_col.lower())
        if not resolved:
            raise KeyError(
                f"Field '{source_name}' not found in output columns "
                f"(mapped to '{out_col}' via field_map). "
                f"Available: {sorted(gdf.columns)}\n"
                f"Tip: ensure '{source_name}' is in the field_map or auto-added."
            )
        return resolved

    resolved_id_col = _resolve_col(id_src)

    # Validate uniqueness of the source ID column.
    dupes = gdf[resolved_id_col].duplicated().sum()
    if dupes:
        raise ValueError(
            f"row_id_source column '{resolved_id_col}' has {dupes} duplicate value(s). "
            f"UUID5 derivation requires unique values."
        )
    nulls = gdf[resolved_id_col].isna().sum()
    if nulls:
        raise ValueError(
            f"row_id_source column '{resolved_id_col}' has {nulls} null value(s). "
            f"UUID5 derivation requires non-null values."
        )

    # ---- Compute rowids & rownames ----
    rowids: list[str] = []
    rownames: list[str] = []

    # Resolve template fields through the effective field_map so that
    # config-level source field names (e.g., HMA_NAME) map to the
    # actual output column names (e.g., hma_name) that vector_prep produced.
    tpl_fields = ctx.cfg.extract_template_fields()
    tpl_col_map: dict[str, str] = {}
    for tf in tpl_fields:
        tpl_col_map[tf] = _resolve_col(tf)

    for _, row in gdf.iterrows():
        # UUID5 from source ID value.
        raw_id = str(row[resolved_id_col])
        rowids.append(str(uuid.uuid5(_RS_ROW_ID_NAMESPACE, raw_id)))

        # Build template context - clean strings (strip NBSP, whitespace).
        ctx_dict: dict[str, Any] = {}
        for col_name, val in row.items():
            col_str = str(col_name)
            if isinstance(val, str):
                clean = val
                for ch in _REPLACE_CHARS:
                    clean = clean.replace(ch, " ")
                ctx_dict[col_str] = clean.strip()
            else:
                ctx_dict[col_str] = val

        # Render rowname - use original template field names mapped to
        # actual (possibly renamed) column values.
        render_dict: dict[str, Any] = {}
        for tf in tpl_fields:
            render_dict[tf] = ctx_dict.get(tpl_col_map[tf], "")
        try:
            rownames.append(name_tpl.format(**render_dict))
        except Exception as exc:
            ctx.log.error(
                f"Row failed to evaluate name template '{name_tpl}'. Error: {exc}"
            )
            raise

    # ---- Validate ----
    overlength = [i for i, n in enumerate(rownames) if len(n) > _MAX_ROWNAME_LENGTH]
    if overlength:
        ctx.log.error(
            f"{len(overlength)} rowname(s) exceed {_MAX_ROWNAME_LENGTH} characters:"
        )
        for idx in overlength[:5]:
            ctx.log.error(
                f"  [{idx}] len={len(rownames[idx])}: {rownames[idx][:120]}..."
            )
        raise ValueError(
            f"Rownames exceeded maximum length ({_MAX_ROWNAME_LENGTH} chars)."
        )

    has_dupes = len(set(rownames)) < len(rownames)
    if has_dupes:
        name_counts = pd.Series(rownames).value_counts()
        dupes_info = name_counts[name_counts > 1].to_dict()
        msg = f"Duplicate rownames detected: {dupes_info}"
        if ctx.cfg.name_uniqueness == "error":
            raise ValueError(msg)
        ctx.log.warning(f"Quality warning - {msg}")

    # ---- Write back ----
    gdf["rs_reports_rowid"] = rowids
    gdf["rs_reports_rowname"] = rownames
    output_gdf(gdf, str(ctx.output_file), ctx.cfg.input_layer_name)
    ctx.log.info(
        f"add_unique_id_and_name complete - added rs_reports_rowid and rs_reports_rowname "
        f"to {ctx.output_file}"
    )

    # ---- Record add_unique columns in layer_definitions.json ----
    _record_add_unique_columns(ctx)
    return StepReport(
        summary="Added rs_reports_rowid and rs_reports_rowname columns.",
        inputs=[str(ctx.output_file)],
        outputs=[str(ctx.output_file)],
        details=[
            f"Rows updated: {len(gdf):,}",
            f"row_id_source: {resolved_id_col}",
            f"name template: {name_tpl}",
            f"Duplicate names detected: {'yes' if has_dupes else 'no'}",
        ],
        artifacts=[str(ctx.output_file)],
    )


@step("upload_instructions")
def _step_upload_instructions(ctx: LayerContext) -> StepReport:
    """Print guidance for the next manual step: uploading to the lakehouse."""
    ctx.log.info("-" * 60)
    ctx.log.info("NEXT STEP - Upload to Iceberg")
    ctx.log.info("-" * 60)
    ctx.log.info(f"  Layer:      {ctx.cfg.layer_id}")
    ctx.log.info(f"  Output:     {ctx.output_file}")
    ctx.log.info(f"  Snapshot:   {ctx.cfg.snapshot_id}")
    ctx.log.info("")
    ctx.log.info("  Run the interactive upload script:")
    ctx.log.info("    python tools/athena_upload/geopackage_athena_iceberg_upload.py")
    ctx.log.info("")
    ctx.log.info("  When prompted, point to the output file above.")
    ctx.log.info(f"  Suggested table name: {ctx.cfg.layer_id.replace('-', '_')}")
    ctx.log.info("-" * 60)
    return StepReport(
        summary="Printed next-step upload instructions.",
        inputs=[str(ctx.output_file)],
        artifacts=[str(ctx.output_file)],
    )


@step("build_legacy_ddl")
def _step_build_legacy_ddl(ctx: LayerContext) -> StepReport:
    """[LEGACY] Generate SQL DDL for Athena/Hive systems.

    This step is intentionally isolated from the default pipeline.  Use it
    only when you need to generate legacy DDL for older query engines.
    """
    ctx.log.info("Legacy DDL generation is not implemented in the default path.")
    ctx.log.info(
        "If you need Athena/Hive DDL, use the legacy orchestrator or a custom script."
    )
    return StepReport(
        summary="Legacy DDL step placeholder.",
        details=["No SQL DDL is generated by the default orchestrator path."],
        skipped=True,
    )


# ===================================================================
# Runner
# ===================================================================


def run_pipeline(
    config_path: Path,
    from_step: str = "fetch_metadata",
    to_step: str = "upload_instructions",
    only_step: str | None = None,
    dry_run: bool = False,
) -> None:
    """Execute registered pipeline steps from *from_step* through *to_step*.

    Args:
        config_path: Path to the layer's ``config.json``.
        from_step: First step name to execute.
        to_step: Last step name to execute (inclusive).
        only_step: Optional single-step override for manual reruns.
        dry_run: When True, print resolved selection details and exit.
    """
    if only_step is not None:
        if only_step not in _STEPS:
            raise ValueError(f"Unknown only_step '{only_step}'. Choices: {_STEPS}")
        from_step = only_step
        to_step = only_step

    if from_step not in _STEPS:
        raise ValueError(f"Unknown from_step '{from_step}'. Choices: {_STEPS}")
    if to_step not in _STEPS:
        raise ValueError(f"Unknown to_step '{to_step}'. Choices: {_STEPS}")

    start_idx = _STEPS.index(from_step)
    end_idx = _STEPS.index(to_step)
    if start_idx > end_idx:
        raise ValueError(f"From step '{from_step}' comes after to step '{to_step}'.")

    cfg = LayerConfig.from_json(config_path)

    enabled_steps = cfg.enabled_steps or list(_DEFAULT_ENABLED_STEPS)
    unknown_config_steps = [s for s in enabled_steps if s not in _STEPS]
    if unknown_config_steps:
        raise ValueError(
            f"Unknown step name(s) in parameters.steps: {unknown_config_steps}. "
            f"Known steps: {_STEPS}"
        )

    # Resolve repo root as the nearest parent with pyproject.toml.
    repo_root = next(
        (p for p in config_path.parents if (p / "pyproject.toml").exists()),
        config_path.parent,
    )
    ctx = LayerContext(
        config_path=config_path,
        cfg=cfg,
        dist_root=repo_root / "dist",
    )

    steps_in_range = _STEPS[start_idx : end_idx + 1]
    enabled_set = set(enabled_steps)
    steps_to_run = [s for s in steps_in_range if s in enabled_set]
    if not steps_to_run:
        raise ValueError(
            "No steps selected after applying --from-step/--to-step and parameters.steps filters."
        )

    # Keep independent step runs explicit: if dissolve is enabled for the layer,
    # vector_prep must be chained with dissolve in the same run.
    if "vector_prep" in steps_to_run and "dissolve_duplicates" in enabled_set:
        vector_idx = steps_to_run.index("vector_prep")
        if "dissolve_duplicates" not in steps_to_run[:vector_idx]:
            raise ValueError(
                "vector_prep requires dissolve_duplicates first because dissolve_duplicates "
                "is enabled in parameters.steps. Run --from-step dissolve_duplicates "
                "--to-step vector_prep (or remove dissolve_duplicates from parameters.steps)."
            )

    if dry_run:
        source_stub = (
            "usgov_sources"
            if cfg.source_category == "usgov"
            else f"raw_{cfg.source_category}"
        )
        resolved_output = (
            cfg.output_vector_path
            if cfg.output_vector_path is not None
            else repo_root
            / source_stub
            / cfg.layer_id
            / cfg.snapshot_id
            / f"{cfg.layer_id}.gpkg"
        )
        print("DRY RUN: no steps executed.")
        print("Resolved configuration:")
        print(f"  config_path: {config_path}")
        print(f"  layer_id: {cfg.layer_id}")
        print(f"  snapshot_id: {cfg.snapshot_id}")
        print(f"  input_vector_path: {cfg.input_vector_path}")
        print(f"  output_vector_path: {resolved_output}")
        print(f"  enabled_steps: {', '.join(enabled_steps)}")
        print(f"  selected_range: {from_step} -> {to_step}")
        if only_step:
            print(f"  only_step: {only_step}")
        print(f"  execution_order: {' -> '.join(steps_to_run)}")
        return

    _ensure_report_header(ctx)

    ctx.log.title(f"Orchestrate: {cfg.layer_id}")
    ctx.log.info(f"Config:   {config_path}")
    ctx.log.info(f"Output:   {ctx.output_file}")
    ctx.log.info(f"Steps:    {' -> '.join(steps_to_run)}")
    if cfg.enabled_steps:
        ctx.log.info(
            f"Enabled via config parameters.steps: {', '.join(cfg.enabled_steps)}"
        )
    ctx.log.info(f"Run report: {ctx.orchestrate_report_file}")
    ctx.log.info("")

    for step_name in steps_to_run:
        handler = _step_registry.get(step_name)
        if handler is None:
            raise NotImplementedError(
                f"Step '{step_name}' is registered but has no handler."
            )
        ctx.log.info(f"{'=' * 60}")
        ctx.log.info(f"  STEP: {step_name}")
        ctx.log.info(f"{'=' * 60}")
        start_time = time.perf_counter()
        try:
            step_report = handler(ctx)
            status = "skipped" if step_report.skipped else "ok"
            elapsed = time.perf_counter() - start_time
            _append_step_report(ctx, step_name, status, elapsed, step_report)
        except Exception as exc:
            elapsed = time.perf_counter() - start_time
            _append_step_report(
                ctx,
                step_name,
                "failed",
                elapsed,
                StepReport(
                    summary=f"Step failed: {exc}",
                    details=[
                        "See orchestrate logs/traceback for full diagnostic context."
                    ],
                ),
            )
            raise
        ctx.log.info(f"  COMPLETE: {step_name}\n")

    ctx.log.info(f"Orchestrate markdown report: {ctx.orchestrate_report_file}")


# ===================================================================
# CLI
# ===================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orchestrate vector layer preparation for Riverscapes lakehouse upload."
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        required=True,
        help="Path to the layer's config.json file.",
    )
    parser.add_argument(
        "--from-step",
        default="fetch_metadata",
        choices=_STEPS,
        help="First step to execute (default: fetch_metadata).",
    )
    parser.add_argument(
        "--to-step",
        default="upload_instructions",
        choices=_STEPS,
        help="Last step to execute, inclusive (default: upload_instructions).",
    )
    parser.add_argument(
        "--only-step",
        choices=_STEPS,
        help="Run exactly one step (cannot be combined with --from-step or --to-step).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved step selection and configuration, then exit.",
    )
    parser.add_argument(
        "--list-steps",
        action="store_true",
        help="List all registered steps and exit.",
    )
    args = parser.parse_args()

    if args.list_steps:
        print("Registered Orchestrator Steps:")
        for i, s in enumerate(_STEPS, 1):
            label = " [default terminal]" if s == "upload_instructions" else ""
            print(f"  {i}. {s}{label} - {_step_description(s)}")
        sys.exit(0)

    provided_from = "--from-step" in sys.argv[1:]
    provided_to = "--to-step" in sys.argv[1:]
    if args.only_step and (provided_from or provided_to):
        parser.error("--only-step cannot be combined with --from-step or --to-step.")

    config_path = Path(args.config).resolve()
    if not config_path.exists():
        print(f"ERROR: Configuration file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    try:
        run_pipeline(
            config_path=config_path,
            from_step=args.from_step,
            to_step=args.to_step,
            only_step=args.only_step,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        print(f"\nFATAL: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
