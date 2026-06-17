#!/usr/bin/env python3
import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CATALOG_PATH = BASE_DIR / "step-catalog.json"
HISTORY_PATH = BASE_DIR / "step-history.json"
STEP1_SCRIPT = BASE_DIR / "step-1-download-unzip.sh"


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


def main():
    parser = argparse.ArgumentParser(description="Simple BLM ownership orchestration helper")
    parser.add_argument("--list", action="store_true", help="List steps and latest status")
    parser.add_argument("--run-step", choices=["step1", "download_unzip"], help="Run an automated step")
    parser.add_argument("--mark-done", help="Mark a step as done manually by step_id")
    parser.add_argument("--notes", default="", help="Optional notes to store in step history")
    parser.add_argument("--run-id", default=f"manual-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}", help="Run/session identifier")
    args = parser.parse_args()

    if args.list:
        list_steps()
        return

    if args.run_step:
        if args.run_step in {"step1", "download_unzip"}:
            run_step_1(args.run_id)
            return

    if args.mark_done:
        append_history(args.mark_done, "done", "manual", args.run_id, args.notes)
        print(f"Marked done: {args.mark_done}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
