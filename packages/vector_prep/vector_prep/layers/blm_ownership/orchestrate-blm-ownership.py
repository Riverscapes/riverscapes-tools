#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Allow imports from the vector_prep package without requiring an install
_REPO_ROOT = Path(__file__).resolve().parents[5]
if str(_REPO_ROOT / "packages" / "vector_prep") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "packages" / "vector_prep"))

BASE_DIR = Path(__file__).resolve().parent
CATALOG_PATH = BASE_DIR / "step-catalog.json"
HISTORY_PATH = BASE_DIR / "step-history.json"
STEP1_SCRIPT = BASE_DIR / "step-1-download-unzip.sh"
INPUTS_PATH = BASE_DIR / "inputs.json"
RUNS_DIR = BASE_DIR / "runs"

# Default paths / IDs used by steps 3-4
AGOL_ITEM_ID = "6bf2e737c59d4111be92420ee5ab0b46"
GDB_LAYER_NAME = "SurfaceManagementAgency"
METADATA_FGDB_OUT = BASE_DIR / "metadata_fgdb.json"
METADATA_XML_OUT = BASE_DIR / "metadata_xml.json"
METADATA_DIFF_OUT = BASE_DIR / "metadata_diff.json"
LAYER_DEFS_OUT = BASE_DIR / "layer_definitions.json"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def append_history(
    step_id: str,
    status: str,
    execution_mode: str,
    run_id: str,
    notes: str = "",
    artifacts: list[dict[str, str]] | None = None,
    error_summary: dict[str, str] | None = None,
):
    history = load_json(HISTORY_PATH)
    entry: dict[str, Any] = {
        "run_id": run_id,
        "step_id": step_id,
        "status": status,
        "execution_mode": execution_mode,
        "executed_at": datetime.now(timezone.utc).isoformat(),
    }
    if notes:
        entry["notes"] = notes
    if artifacts:
        entry["artifacts"] = artifacts
    if error_summary is not None:
        entry["error_summary"] = error_summary
    history.setdefault("entries", []).append(entry)
    write_json(HISTORY_PATH, history)


def _resolve_cfg_path(cfg_dir: Path, value: str | None) -> str | None:
    """Resolve config path values with env/user expansion and cfg-dir relativity."""
    if not value:
        return None
    expanded = Path(value).expanduser()
    if expanded.is_absolute():
        return str(expanded)
    return str((cfg_dir / expanded).resolve())


def latest_status_by_step():
    history = load_json(HISTORY_PATH)
    latest = {}
    for entry in history.get("entries", []):
        latest[entry["step_id"]] = entry
    return latest


def list_steps():
    catalog = load_json(CATALOG_PATH)
    latest = latest_status_by_step()
    for step in sorted(catalog["steps"], key=lambda s: s["order"]):
        status = latest.get(step["step_id"], {}).get("status", "pending")
        when = latest.get(step["step_id"], {}).get("executed_at", "-")
        print(f"{step['order']}. {step['name']} [{step['step_id']}] status={status} last={when}")


def run_step_1(run_id: str):
    append_history("download_data", "running", "automated", run_id, "Starting download")
    try:
        subprocess.run([str(STEP1_SCRIPT)], check=True)
        append_history("download_data", "done", "automated", run_id, "Downloaded source ZIP")
        append_history("unzip_data", "done", "automated", run_id, "Unzipped source ZIP")
    except subprocess.CalledProcessError as exc:
        append_history("download_data", "failed", "automated", run_id, f"Step 1 script failed: {exc}")
        raise


def run_step_3(run_id: str, gdb_path: str):
    """Step 3 — extract metadata from both the FGDB and the AGOL XML endpoint.

    Writes:
      metadata_fgdb.json  — columns from the File Geodatabase (typed + domains)
      metadata_xml.json   — columns from the FGDC CSDGM XML (descriptions)
      metadata_diff.json  — field-by-field comparison between the two sources
    """
    from vector_prep.fetch_fgdb_metadata import fetch_columns_from_fgdb, fetch_all_layer_names
    from vector_prep.fetch_xml_metadata import (
        fetch_columns_from_xml_url,
        fetch_dataset_info_from_xml_url,
        compare_columns,
    )

    append_history("extract_metadata", "running", "automated", run_id)

    try:
        # --- FGDB ---
        print(f"Listing layers in: {gdb_path}")
        layers = fetch_all_layer_names(gdb_path)
        for lyr in layers:
            print(f"  {lyr['name']:40s}  {lyr['geometry_type']:25s}  {lyr['feature_count']:>12,}")

        print(f"\nExtracting FGDB columns for layer: {GDB_LAYER_NAME}")
        fgdb_columns = fetch_columns_from_fgdb(gdb_path, GDB_LAYER_NAME)
        METADATA_FGDB_OUT.write_text(
            json.dumps({"source": "fgdb", "layer": GDB_LAYER_NAME, "columns": fgdb_columns}, indent=2)
        )
        print(f"  Wrote {len(fgdb_columns)} columns → {METADATA_FGDB_OUT}")

        # --- XML ---
        xml_url = f"https://www.arcgis.com/sharing/rest/content/items/{AGOL_ITEM_ID}/info/metadata/metadata.xml"
        print(f"\nFetching FGDC XML metadata from: {xml_url}")
        xml_columns = fetch_columns_from_xml_url(AGOL_ITEM_ID)
        dataset_info = fetch_dataset_info_from_xml_url(AGOL_ITEM_ID)

        xml_out = {
            "source": "fgdc_xml",
            "item_id": AGOL_ITEM_ID,
            "dataset_info": {
                "title": dataset_info.title,
                "pub_date": dataset_info.pub_date,
                "abstract": dataset_info.abstract[:500] + ("..." if len(dataset_info.abstract) > 500 else ""),
                "purpose": dataset_info.purpose[:300] + ("..." if len(dataset_info.purpose) > 300 else ""),
                "entity_name": dataset_info.entity_name,
                "entity_description": dataset_info.entity_description,
                "bounding_box": {
                    "west": dataset_info.west, "east": dataset_info.east,
                    "north": dataset_info.north, "south": dataset_info.south,
                } if dataset_info.west is not None else None,
                "keywords": dataset_info.keywords[:20],
            },
            "columns": xml_columns,
        }
        METADATA_XML_OUT.write_text(json.dumps(xml_out, indent=2))
        print(f"  Wrote {len(xml_columns)} columns → {METADATA_XML_OUT}")
        print(f"  Dataset title: {dataset_info.title}")

        # --- Diff ---
        diff = compare_columns(fgdb_columns, xml_columns, label_a="fgdb", label_b="xml")
        METADATA_DIFF_OUT.write_text(json.dumps(diff, indent=2))
        print(
            f"\nComparison: {diff['matching']} match, "
            f"{len(diff['differences'])} differ, "
            f"{len(diff.get('only_in_fgdb', []))} only-in-FGDB, "
            f"{len(diff.get('only_in_xml', []))} only-in-XML."
        )
        print(f"  Wrote diff → {METADATA_DIFF_OUT}")

        append_history(
            "extract_metadata", "done", "automated", run_id,
            f"FGDB: {len(fgdb_columns)} cols, XML: {len(xml_columns)} cols, "
            f"diff written to {METADATA_DIFF_OUT.name}"
        )

    except Exception as exc:
        append_history("extract_metadata", "failed", "automated", run_id, str(exc))
        raise


def run_step_4(run_id: str, gdb_path: str):
    """Step 4 — build layer_definitions.json by merging FGDB types with XML descriptions.

    Reads metadata_fgdb.json and metadata_xml.json produced by step 3.
    Writes layer_definitions.json to the blm_ownership layer directory.
    """
    from vector_prep.fetch_xml_metadata import merge_xml_descriptions_into_fgdb_columns

    append_history("build_layer_definitions", "running", "automated", run_id)

    try:
        if not METADATA_FGDB_OUT.exists():
            raise FileNotFoundError(f"Run step 3 first — {METADATA_FGDB_OUT} not found.")
        if not METADATA_XML_OUT.exists():
            raise FileNotFoundError(f"Run step 3 first — {METADATA_XML_OUT} not found.")

        fgdb_data = json.loads(METADATA_FGDB_OUT.read_text())
        xml_data = json.loads(METADATA_XML_OUT.read_text())

        fgdb_columns = fgdb_data["columns"]
        xml_columns = xml_data["columns"]
        dataset_info = xml_data["dataset_info"]

        merged_columns = merge_xml_descriptions_into_fgdb_columns(fgdb_columns, xml_columns)

        inputs = json.loads(INPUTS_PATH.read_text()) if INPUTS_PATH.exists() else {}

        layer_defs = {
            "$schema": "https://xml.riverscapes.net/riverscapes_metadata/schema/layer_definitions.schema.json",
            "tool_schema_name": "vector-prep-blm-ownership",
            "tool_schema_version": "0.0.1",
            "source_title": inputs.get("source_title", dataset_info.get("title", "")),
            "source_url": inputs.get("source_url", ""),
            "snapshot_id": inputs.get("snapshot_id", dataset_info.get("pub_date", "")),
            "layers": [
                {
                    "layer_id": "blm_ownership",
                    "layer_name": "BLM Ownership",
                    "description": dataset_info.get("entity_description") or dataset_info.get("abstract", "")[:500],
                    "layer_type": "Vector",
                    "columns": merged_columns,
                }
            ],
        }

        LAYER_DEFS_OUT.write_text(json.dumps(layer_defs, indent=2, ensure_ascii=False) + "\n")
        print(f"Wrote {len(merged_columns)} columns → {LAYER_DEFS_OUT}")

        append_history(
            "build_layer_definitions", "done", "automated", run_id,
            f"Merged {len(merged_columns)} columns written to {LAYER_DEFS_OUT.name}"
        )

    except Exception as exc:
        append_history("build_layer_definitions", "failed", "automated", run_id, str(exc))
        raise

def run_vector_prep(run_id: str, config_path: str):
    """Step 5 — run vector_prep in config mode and persist effective run parameters."""
    cfg_path = Path(config_path).expanduser().resolve()
    append_history(
        "run_vector_prep",
        "running",
        "automated",
        run_id,
        notes=f"Config: {cfg_path}",
    )

    try:
        if not cfg_path.exists():
            raise FileNotFoundError(f"Config not found: {cfg_path}")

        cfg_params = load_json(cfg_path).get("parameters", {})
        cfg_dir = cfg_path.parent

        # Expand env vars and user home to persist a machine-resolved run record.
        expanded_cfg: dict[str, Any] = {}
        for key, value in cfg_params.items():
            if isinstance(value, str):
                expanded_cfg[key] = os.path.expanduser(os.path.expandvars(value))
            else:
                expanded_cfg[key] = value

        input_path = _resolve_cfg_path(cfg_dir, expanded_cfg.get("input"))
        output_path = _resolve_cfg_path(cfg_dir, expanded_cfg.get("output"))
        garbage_path = _resolve_cfg_path(cfg_dir, expanded_cfg.get("garbage"))
        layer_name = cfg_params.get("layer") or None
        tolerance = float(cfg_params.get("tolerance", 0.0))
        epsg = int(cfg_params.get("epsg", 5070))
        min_size = float(cfg_params["min_size"]) if cfg_params.get("min_size") is not None else None
        min_size_drop = bool(cfg_params.get("min_size_drop", False))
        chunk_size = int(cfg_params.get("chunk_size", 10_000))

        if input_path is None:
            raise ValueError("Missing required config parameter: parameters.input")

        RUNS_DIR.mkdir(parents=True, exist_ok=True)

        effective_parameters = dict(expanded_cfg)
        effective_parameters["input"] = input_path
        if output_path is not None:
            effective_parameters["output"] = output_path
        if garbage_path is not None:
            effective_parameters["garbage"] = garbage_path
        if effective_parameters.get("layer_definitions") is not None:
            effective_parameters["layer_definitions"] = _resolve_cfg_path(
                cfg_dir,
                str(effective_parameters.get("layer_definitions")),
            )
        if effective_parameters.get("log_file") is not None:
            effective_parameters["log_file"] = _resolve_cfg_path(
                cfg_dir,
                str(effective_parameters.get("log_file")),
            )

        effective_cfg_path = RUNS_DIR / f"vector_prep_effective_config_{run_id}.json"
        write_json(
            effective_cfg_path,
            {
                "$schema": "../../vector_prep_config.schema.json",
                "parameters": effective_parameters,
            },
        )

        command = [
            sys.executable,
            "-m",
            "vector_prep.vector_prep.vector_prep",
            "--config",
            str(effective_cfg_path),
        ]
        subprocess.run(command, check=True, cwd=str(_REPO_ROOT))

        report_path = None
        if output_path:
            report_path = str(Path(output_path).parent / f"{Path(output_path).stem}_report.md")

        run_manifest_path = RUNS_DIR / f"vector_prep_run_{run_id}.json"
        run_manifest = {
            "run_id": run_id,
            "step_id": "run_vector_prep",
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "config_path": str(cfg_path),
            "effective_config_path": str(effective_cfg_path),
            "command": command,
            "resolved_parameters": {
                "input": input_path,
                "layer": layer_name,
                "output": output_path,
                "garbage": garbage_path,
                "tolerance": tolerance,
                "epsg": epsg,
                "min_size": min_size,
                "min_size_drop": min_size_drop,
                "chunk_size": chunk_size,
                "field_map_keys": sorted(expanded_cfg.get("field_map", {}).keys()) if isinstance(expanded_cfg.get("field_map"), dict) else None,
                "layer_definitions": _resolve_cfg_path(cfg_dir, expanded_cfg.get("layer_definitions")),
            },
            "artifacts": {
                "report": str(report_path),
                "output": output_path,
                "garbage": garbage_path,
            },
        }
        write_json(run_manifest_path, run_manifest)

        artifacts = [
            {"name": "vector_prep_config", "kind": "config", "path": str(cfg_path)},
            {"name": "vector_prep_effective_config", "kind": "config", "path": str(effective_cfg_path)},
            {"name": "vector_prep_run_manifest", "kind": "run_record", "path": str(run_manifest_path)},
        ]
        if report_path:
            artifacts.append({"name": "vector_prep_report", "kind": "report", "path": str(report_path)})
        if output_path:
            artifacts.append({"name": "vector_prep_output", "kind": "vector", "path": output_path})
        if garbage_path:
            artifacts.append({"name": "vector_prep_garbage", "kind": "vector", "path": garbage_path})

        append_history(
            "run_vector_prep",
            "done",
            "automated",
            run_id,
            notes=(
                f"Ran vector_prep via config mode; "
                f"Output={output_path or '-'}; "
                f"Report={Path(report_path).name if report_path else '-'}; "
                f"Manifest={run_manifest_path.name}"
            ),
            artifacts=artifacts,
        )

        if report_path:
            print(f"Vector prep complete. Report: {report_path}")
        print(f"Run manifest: {run_manifest_path}")

    except Exception as exc:
        append_history(
            "run_vector_prep",
            "failed",
            "automated",
            run_id,
            notes=str(exc),
            error_summary={"message": str(exc)},
        )
        raise



def main():
    parser = argparse.ArgumentParser(description="BLM ownership orchestration helper")
    parser.add_argument("--list", action="store_true", help="List steps and latest status")
    parser.add_argument(
        "--run-step",
        choices=[
            "step1",
            "download_unzip",
            "step3",
            "extract_metadata",
            "step4",
            "build_layer_definitions",
            "step5",
            "run_vector_prep",
        ],
        help="Run an automated step",
    )
    parser.add_argument(
        "--config",
        default=str(BASE_DIR / "config.json"),
        help="Path to vector_prep config.json used by step5/run_vector_prep",
    )
    parser.add_argument(
        "--gdb",
        default=str(Path.home() / "GISData" / "SMA_WM.gdb"),
        help="Path to the downloaded .gdb (required for step3/step4). Default: ~/GISData/SMA_WM.gdb",
    )
    parser.add_argument("--mark-done", help="Mark a step as done manually by step_id")
    parser.add_argument("--notes", default="", help="Optional notes to store in step history")
    parser.add_argument(
        "--run-id",
        default=f"manual-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        help="Run/session identifier",
    )
    args = parser.parse_args()

    if args.list:
        list_steps()
        return

    if args.run_step:
        if args.run_step in {"step1", "download_unzip"}:
            run_step_1(args.run_id)
            return
        if args.run_step in {"step3", "extract_metadata"}:
            run_step_3(args.run_id, args.gdb)
            return
        if args.run_step in {"step4", "build_layer_definitions"}:
            run_step_4(args.run_id, args.gdb)
            return
        if args.run_step in {"step5", "run_vector_prep"}:
            run_vector_prep(args.run_id, args.config)
            return

    if args.mark_done:
        append_history(args.mark_done, "done", "manual", args.run_id, args.notes)
        print(f"Marked done: {args.mark_done}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
