"""Markdown summary report generation for a vector_prep run."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from .. import __version__


def build_markdown_report(stats: Dict, garbage_path: str | None) -> str:
    """Build a markdown summary report for vector_prep output."""

    dropped_by_reason: Dict[str, int] = stats.get("dropped_by_reason", {})
    dropped_by_geom_type: Dict[str, int] = stats.get("dropped_by_geom_type", {})
    garbage_written = bool(stats.get("garbage_written", False))
    total_dropped = sum(dropped_by_reason.values())

    lines = [
        "# Vector Prep Report",
        "",
        "## Input And Geometry Fixes",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Input features (original file) | {stats.get('initial_count', stats.get('input_count', 0)):,} |",
        f"| Null/empty on input | {stats.get('null_or_empty', 0):,} |",
        f"| Invalid geometries fixed | {stats.get('invalid_fixed', 0):,} |",
        f"| Invalid geometries unfixed | {stats.get('invalid_unfixed', 0):,} |",
        f"| Features simplified | {stats.get('simplified_count', 0):,} |",
        "",
        "## Quality Checks",
        "",
        "| Check | Count |",
        "|---|---:|",
        f"| Z/M coordinates stripped | {stats.get('zm_stripped', 0):,} |",
        f"| Multi-part features detected | {stats.get('multipart_detected', 0):,} |",
        f"| GeometryCollection features | {stats.get('geocollection_detected', 0):,} |",
        f"| Self-touching rings fixed | {stats.get('self_touching_fixed', 0):,} |",
        f"| Unclosed rings detected | {stats.get('unclosed_rings_detected', 0):,} |",
        f"| Duplicate-vertex features | {stats.get('duplicate_vertices_detected', 0):,} |",
        f"| String cells normalized | {stats.get('string_cells_normalized', 0):,} |",
        f"| Schema inconsistencies detected | {stats.get('schema_inconsistencies', 0):,} |",
        f"| Mixed geometry types detected | {'YES' if stats.get('mixed_types_detected') else 'NO'} |",
        "",
        "## Dropped Features",
        "",
        "| Reason | Count |",
        "|---|---:|",
        f"| Total dropped | {total_dropped:,} |",
        f"| null/empty | {dropped_by_reason.get('null', 0) + dropped_by_reason.get('empty', 0):,} |",
        f"| invalid (unfixed) | {dropped_by_reason.get('invalid_unfixed', 0):,} |",
        f"| zero-area bounding box | {stats.get('zero_area_bbox_dropped', 0):,} |",
        f"| duplicate geometry | {stats.get('geometry_duplicates_dropped', 0):,} |",
        f"| duplicate row | {stats.get('row_duplicates_dropped', 0):,} |",
        f"| below minimum size | {stats.get('below_min_size_dropped', 0):,} |",
        f"| sliver polygon | {stats.get('slivers_dropped', 0):,} |",
        "",
        "## Dropped By Geometry Type",
        "",
        "| Geometry type | Count |",
        "|---|---:|",
    ]

    if dropped_by_geom_type:
        for geom_type, count in sorted(
            dropped_by_geom_type.items(), key=lambda x: -x[1]
        ):
            lines.append(f"| {geom_type} | {count:,} |")
    else:
        lines.append("| (none) | 0 |")

    lines += [
        "",
        "## Output",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Tool version | {__version__} |",
        f"| Output features | {stats.get('output_count', 0):,} |",
    ]

    if garbage_path and garbage_written:
        lines.append(f"| Garbage written to | {garbage_path} |")
    elif garbage_path:
        lines.append("| Garbage output | Not written (no dropped/changed features) |")
    else:
        lines.append("| Garbage output | Disabled (no --garbage path provided) |")

    return "\n".join(lines) + "\n"


def write_markdown_report(
    stats: Dict, garbage_path: str | None, report_path: str | Path
) -> str:
    """Write markdown report to *report_path* and return absolute path."""
    target = Path(report_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(build_markdown_report(stats, garbage_path), encoding="utf-8")
    return str(target)
