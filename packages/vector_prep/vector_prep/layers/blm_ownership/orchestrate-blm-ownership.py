#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow imports from the vector_prep package without requiring an install
_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "packages" / "vector_prep"))

BASE_DIR = Path(__file__).resolve().parent
CATALOG_PATH = BASE_DIR / "step-catalog.json"
HISTORY_PATH = BASE_DIR / "step-history.json"
STEP1_SCRIPT = BASE_DIR / "step-1-download-unzip.sh"
INPUTS_PATH = BASE_DIR / "inputs.json"

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


def append_history(step_id: str, status: str, execution_mode: str, run_id: str, notes: str = ""):
    history = load_json(HISTORY_PATH)
    entry = {
        "run_id": run_id,
        "step_id": step_id,
        "status": status,
        "execution_mode": execution_mode,
        "executed_at": datetime.now(timezone.utc).isoformat(),
    }
    if notes:
        entry["notes"] = notes
    history.setdefault("entries", []).append(entry)
    write_json(HISTORY_PATH, history)


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


def main():
    parser = argparse.ArgumentParser(description="BLM ownership orchestration helper")
    parser.add_argument("--list", action="store_true", help="List steps and latest status")
    parser.add_argument(
        "--run-step",
        choices=["step1", "download_unzip", "step3", "extract_metadata", "step4", "build_layer_definitions"],
        help="Run an automated step",
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

    if args.mark_done:
        append_history(args.mark_done, "done", "manual", args.run_id, args.notes)
        print(f"Marked done: {args.mark_done}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
