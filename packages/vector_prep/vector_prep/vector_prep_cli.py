"""
Interactive CLI for vector_prep.

Scans the bundled ``layers/`` directory for config.json files, presents an
arrow-key selection menu via questionary, optionally prompts for any I/O paths
missing from the chosen config, then runs vector_prep with the resolved
parameters.

Entry point: ``vector-prep-cli``
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional
import traceback

from rsxml import Logger, dotenv

import questionary

from .vector_prep import vector_prep, load_config
from .lib.field_map import load_and_validate_field_map
from .lib.report import write_markdown_report


# ---------------------------------------------------------------------------
# Layer discovery
# ---------------------------------------------------------------------------

LAYERS_DIR = Path(__file__).parent / "layers"


def _load_package_env() -> None:
    """Load package-root .env into process env when present.

    This is a fallback for runs where the shell/debug session did not pre-load
    env vars. Existing environment values are preserved.
    """
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.is_file():
        return
    env_values = dotenv.parse_dotenv(str(env_path))
    if not env_values:
        return
    for key, value in env_values.items():
        if key not in os.environ and value is not None:
            os.environ[key] = str(value)


def _discover_configs() -> list[tuple[str, Path]]:
    """Return a sorted list of (layer_name, config_path) tuples.

    Only immediate sub-directories of ``layers/`` that contain a
    ``config.json`` file are included.
    """
    entries: list[tuple[str, Path]] = []
    if not LAYERS_DIR.is_dir():
        return entries
    for subdir in sorted(LAYERS_DIR.iterdir()):
        if not subdir.is_dir() or subdir.name.startswith("."):
            continue
        cfg = subdir / "config.json"
        if cfg.is_file():
            entries.append((subdir.name, cfg))
    return entries


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------


def _select_layer(configs: list[tuple[str, Path]]) -> tuple[str, Path]:
    """Present an arrow-key select box and return the chosen (layer_name, config_path)."""
    log = Logger("vector-prep-cli")
    log.title("Available layers")
    choices = [
        questionary.Choice(title=name, value=i) for i, (name, _) in enumerate(configs)
    ]
    idx = questionary.select(
        "Choose a layer:",
        choices=choices,
        instruction="(↑/↓ to move, Enter to confirm)",
    ).ask()
    if idx is None:
        # user hit Ctrl-C
        log.error("\n  Aborted.")
        sys.exit(0)
    return configs[idx]


# ---------------------------------------------------------------------------
# Parameter resolution: config → prompt for any missing I/O paths
# ---------------------------------------------------------------------------


def _validate_absolute(val: str) -> bool | str:
    """questionary validator: accepts empty (optional) or an absolute path."""
    if not val:
        return True
    return (
        Path(val).is_absolute()
        or "Please enter an absolute path (e.g. /data/myfile.gpkg)"
    )


def _resolve_params(params: dict, config_path: Path) -> dict:
    """Display config-supplied values and prompt for any missing I/O paths."""
    log = Logger("vector-prep-cli")
    log.title("Parameter review")

    for key in (
        "input",
        "output",
        "garbage",
        "layer",
        "filter",
        "tolerance",
        "epsg",
        "min_size",
        "min_size_drop",
        "chunk_size",
        "verbose",
        "log_file",
    ):
        val = params.get(key)
        if val not in (None, False, ""):
            log.info(f"    {key}: {val}")
    print()

    if not params.get("input"):
        val = questionary.path(
            "Input path (shapefile / GeoPackage):",
            validate=_validate_absolute,
        ).ask()
        if not val:
            log.error("  Input path is required.")
            sys.exit(1)
        params["input"] = val

    if not params.get("output"):
        val = questionary.path(
            "Output GeoPackage path (optional — press Enter to skip):",
            default="",
            validate=_validate_absolute,
        ).ask()
        params["output"] = val or None

    if not params.get("garbage"):
        val = questionary.path(
            "Garbage GeoPackage path (optional — press Enter to skip):",
            default="",
            validate=_validate_absolute,
        ).ask()
        params["garbage"] = val or None

    return params


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def _run(params: dict, config_path: Path) -> None:
    """Import vector_prep internals and execute the pipeline."""
    log = Logger("vector-prep-cli")

    cfg_dir = config_path.parent

    def _abs_optional(val: Optional[str]) -> Optional[str]:
        """Resolve log_file (only non-I/O path that may still be relative)."""
        if not val:
            return None
        p = Path(val)
        return str((cfg_dir / p).resolve()) if not p.is_absolute() else str(p)

    input_path = params.get("input") or None
    output_path = params.get("output") or None
    garbage_path = params.get("garbage") or None
    log_file_path = _abs_optional(params.get("log_file"))

    if not input_path:
        log.error("\n  Error: no input path provided.")
        sys.exit(1)

    # Log file default
    if not log_file_path:
        if output_path:
            base = os.path.splitext(os.path.basename(output_path))[0]
            log_dir = os.path.dirname(output_path) or "."
            log_file_path = os.path.join(log_dir, f"{base}_vector_prep.log")
        else:
            log_file_path = "vector_prep.log"

    verbose: bool = bool(params.get("verbose", False))
    log.setup(log_path=log_file_path, verbose=verbose)

    # Field map
    field_map_config = None
    raw_field_map = params.get("field_map")
    layer_defs_rel = params.get("layer_definitions")
    if raw_field_map is not None:
        if layer_defs_rel is None:
            log.error("Config has 'field_map' but 'layer_definitions' is missing.")
            sys.exit(1)
        layer_defs_path = (cfg_dir / layer_defs_rel).resolve()
        try:
            field_map_config = load_and_validate_field_map(
                raw_field_map, layer_defs_path
            )
            log.info(f"Loaded field map: {len(raw_field_map)} field(s)")
        except (ValueError, FileNotFoundError) as exc:
            log.error(f"Field map error: {exc}")
            sys.exit(1)

    tolerance: float = float(params.get("tolerance", 0.0))
    min_size: Optional[float] = (
        float(params["min_size"]) if params.get("min_size") is not None else None
    )
    min_size_drop: bool = bool(params.get("min_size_drop", False))
    epsg: int = int(params.get("epsg", 5070))
    chunk_size: int = int(params.get("chunk_size", 10_000))
    layer_name: Optional[str] = params.get("layer") or None
    sql_filter: Optional[str] = params.get("filter") or None

    log.title("Running vector_prep")
    log.info(f"    input    : {input_path}")
    log.info(f"    output   : {output_path}")
    log.info(f"    garbage  : {garbage_path}")
    log.info(f"    filter   : {sql_filter}")
    log.info(f"    tolerance: {tolerance} m")
    log.info(f"    epsg     : {epsg}")
    print(f"    log      : {log_file_path}")
    print()

    try:
        stats = vector_prep(
            input_dataset=input_path,
            layer_name=layer_name,
            tolerance=tolerance,
            epsg=epsg,
            output_path=output_path,
            garbage_path=garbage_path,
            min_size=min_size,
            min_size_drop=min_size_drop,
            chunk_size=chunk_size,
            field_map_config=field_map_config,
            sql_filter=sql_filter,
        )
        if not output_path:
            log.info("No output path provided; skipping output write.")
        if output_path:
            report_stem = Path(output_path).stem
            report_dir = Path(output_path).parent
        else:
            report_stem = "vector_prep"
            report_dir = Path(".")
        report_path = report_dir / f"{report_stem}_report.md"
        written_report = write_markdown_report(stats, garbage_path, report_path)
        log.info(f"Markdown report written: {written_report}")
        log.info("\n  Done.\n")
    except Exception as exc:
        log.error(f"vector_prep failed: {exc}")
        log.debug(traceback.format_exc())
        sys.exit(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point"""
    log = Logger("vector-prep-cli")
    log.title("Vector Prep CLI")
    _load_package_env()
    configs = _discover_configs()
    if not configs:
        log.error(f"  No config.json files found under {LAYERS_DIR}\n")
        sys.exit(1)

    layer_name, config_path = _select_layer(configs)
    log.info(f"\n  Selected: {layer_name}")

    try:
        params = load_config(config_path)
    except EnvironmentError as exc:
        log.error(str(exc))
        sys.exit(1)

    params = _resolve_params(params, config_path)
    _run(params, config_path)


if __name__ == "__main__":
    main()
