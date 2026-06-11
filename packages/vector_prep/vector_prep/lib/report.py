"""print_report: formatted summary report for a vector_prep run."""
from __future__ import annotations

from typing import Dict

from rsxml import Logger


def print_report(stats: Dict, garbage_path: str | None) -> None:
    """Log a formatted summary report of the vector prep run."""
    log = Logger("Vector Prep Report")

    dropped_by_reason: Dict[str, int] = stats.get("dropped_by_reason", {})
    dropped_by_geom_type: Dict[str, int] = stats.get("dropped_by_geom_type", {})
    total_dropped = sum(dropped_by_reason.values())

    lines = [
        "=== Vector Prep Report ===",
        "",
        "--- Input / Geometry Fixes ---",
        f"  Input features (original file):    {stats.get('initial_count', stats.get('input_count', 0)):>10,}",
        f"  Null/empty on input:               {stats.get('null_or_empty', 0):>10,}",
        f"  Invalid geometries fixed:          {stats.get('invalid_fixed', 0):>10,}",
        f"  Invalid geometries unfixed:        {stats.get('invalid_unfixed', 0):>10,}",
        f"  Features simplified:               {stats.get('simplified_count', 0):>10,}",
        "",
        "--- Quality Checks ---",
        f"  Z/M coordinates stripped:          {stats.get('zm_stripped', 0):>10,}",
        f"  Multi-part features detected:      {stats.get('multipart_detected', 0):>10,}",
        f"  GeometryCollection features:       {stats.get('geocollection_detected', 0):>10,}",
        f"  Self-touching rings fixed:         {stats.get('self_touching_fixed', 0):>10,}",
        f"  Unclosed rings detected:           {stats.get('unclosed_rings_detected', 0):>10,}",
        f"  Duplicate-vertex features:         {stats.get('duplicate_vertices_detected', 0):>10,}",
        f"  String cells normalized:           {stats.get('string_cells_normalized', 0):>10,}",
        f"  Schema inconsistencies detected:   {stats.get('schema_inconsistencies', 0):>10,}",
        f"  Mixed geometry types detected:     {'YES' if stats.get('mixed_types_detected') else 'NO':>10}",
        "",
        "--- Dropped Features ---",
        f"  Total dropped:                     {total_dropped:>10,}",
        f"    null/empty:                      {dropped_by_reason.get('null', 0) + dropped_by_reason.get('empty', 0):>10,}",
        f"    invalid (unfixed):               {dropped_by_reason.get('invalid_unfixed', 0):>10,}",
        f"    zero-area bounding box:          {stats.get('zero_area_bbox_dropped', 0):>10,}",
        f"    duplicate geometry:              {stats.get('geometry_duplicates_dropped', 0):>10,}",
        f"    duplicate row:                   {stats.get('row_duplicates_dropped', 0):>10,}",
        f"    below minimum size:              {stats.get('below_min_size_dropped', 0):>10,}",
        f"    sliver polygon:                  {stats.get('slivers_dropped', 0):>10,}",
        "",
        "  Dropped by geometry type:",
    ]

    if dropped_by_geom_type:
        for geom_type, count in sorted(dropped_by_geom_type.items(), key=lambda x: -x[1]):
            lines.append(f"    {geom_type:<22} {count:>10,}")
    else:
        lines.append("    (none)")

    lines += [
        "",
        f"  Output features:                   {stats.get('output_count', 0):>10,}",
        f"  Garbage written to: {garbage_path if garbage_path else 'N/A'}",
        "==========================",
    ]

    for line in lines:
        log.info(line)
