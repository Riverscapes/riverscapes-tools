"""run_checks: geometry and attribute quality checks A–M for vector_prep."""
from __future__ import annotations

import hashlib
import math
import re
from typing import Dict, List, Optional, Set, Tuple

import geopandas as gpd
import pandas as pd
from shapely.geometry.base import BaseGeometry
from rsxml import Logger

from .geometry_utils import (
    _extract_dominant_from_collection,
    _has_duplicate_vertices,
    _has_unclosed_rings,
    _has_z,
    _strip_z,
    safe_make_valid,
)


def run_checks(
    gdf_proc: gpd.GeoDataFrame,
    min_size: float,
    duplicate_geom_hashes: Optional[Set[str]] = None,
    duplicate_row_hashes: Optional[Set[str]] = None,
    processed_geom_hashes: Optional[Set[str]] = None,
    processed_row_hashes: Optional[Set[str]] = None,
    dominant_type: Optional[str] = None,
) -> Tuple[gpd.GeoDataFrame, Dict, List[Tuple[int, str, str]], List[Tuple[int, str, BaseGeometry]]]:
    """Run geometry and attribute quality checks A–M on gdf_proc.

    Args:
        gdf_proc: GeoDataFrame to check and fix (modified in-place for step D).
        min_size: Minimum area (m²) for polygons or length (m) for lines.
        duplicate_geom_hashes: Set of SHA1 hashes (WKB) that appear in 2+ features across
            all chunks (pass 1 output).  When provided, step J uses this for cross-chunk
            dedup instead of a local dict.  Pass None for single-pass / legacy mode.
        duplicate_row_hashes: Set of SHA1 hashes (WKB + attrs) appearing 2+ times (pass 1).
            Step K uses this for cross-chunk dedup when provided.
        processed_geom_hashes: Mutable set that grows across chunks — tracks the *first*
            occurrence of each geometry hash seen so far in pass 2.  Mutated in-place.
        processed_row_hashes: Mutable set that grows across chunks — first-occurrence
            tracking for row hashes.  Mutated in-place.
        dominant_type: Pre-computed dominant geometry type from pass 1 (optional).  When
            provided, step D uses this value instead of re-deriving it per chunk, ensuring
            consistent GeometryCollection resolution across all chunks.

    Returns:
        gdf_proc: DataFrame with in-place fixes applied (Z stripped, strings normalised,
                  self-touching rings fixed, dropped rows removed).
        extra_stats: Dict of per-check counters.
        dropped_list: List of (idx, "DROPPED", reason) for every dropped feature.
        changed_list: List of (idx, reason, new_geom) for changed-but-kept features.
    """
    log = Logger("Vector Prep")

    # (orig_idx, operation, reason) — operation is always "DROPPED" here
    dropped_list: List[Tuple[int, str, str]] = []
    # (orig_idx, reason, new_geom) — geometry that changed but stays in output
    changed_list: List[Tuple[int, str, BaseGeometry]] = []

    extra_stats: Dict = {
        "zm_stripped": 0,
        "multipart_detected": 0,
        "geocollection_detected": 0,
        "self_touching_fixed": 0,
        "unclosed_rings_detected": 0,
        "duplicate_vertices_detected": 0,
        "string_cells_normalized": 0,
        "schema_inconsistencies": 0,
        "zero_area_bbox_dropped": 0,
        "geometry_duplicates_dropped": 0,
        "row_duplicates_dropped": 0,
        "below_min_size_dropped": 0,
        "slivers_dropped": 0,
    }

    # ------------------------------------------------------------------ #
    # A. Strip Z/M coordinates (in-place, no garbage entry)
    # ------------------------------------------------------------------ #
    log.info("A. Stripping Z/M coordinates...")
    zm_count = 0
    for idx in gdf_proc.index:
        geom = gdf_proc.at[idx, "geometry"]
        if geom is not None and _has_z(geom):
            gdf_proc.at[idx, "geometry"] = _strip_z(geom)
            zm_count += 1
    extra_stats["zm_stripped"] = zm_count
    log.info(f"   Z-stripped: {zm_count:,} features")

    # ------------------------------------------------------------------ #
    # B. Detect multi-part geometries (log only)
    # ------------------------------------------------------------------ #
    log.info("B. Detecting multi-part geometries...")
    multipart_count = int(gdf_proc.geom_type.str.startswith("Multi").sum())
    extra_stats["multipart_detected"] = multipart_count
    log.info(f"   Multi-part features detected: {multipart_count:,}")

    # ------------------------------------------------------------------ #
    # C. Detect GeometryCollection inputs (log only)
    # ------------------------------------------------------------------ #
    log.info("C. Detecting GeometryCollection inputs...")
    geocoll_count = int((gdf_proc.geom_type == "GeometryCollection").sum())
    extra_stats["geocollection_detected"] = geocoll_count
    log.info(f"   GeometryCollection features detected: {geocoll_count:,}")

    # ------------------------------------------------------------------ #
    # D. Fix self-touching rings and strip GeometryCollection results
    #    Changed features go to garbage (CHANGED) but stay in output.
    # ------------------------------------------------------------------ #
    log.info("D. Fixing self-touching rings and GeometryCollection geometries...")

    # Use pre-computed dominant_type from pass 1 when available; otherwise derive
    # from this chunk (legacy / single-pass mode).
    if dominant_type is None:
        _valid_type_mask = gdf_proc.geometry.notna() & (gdf_proc.geom_type != "GeometryCollection")
        _type_counts = gdf_proc.loc[_valid_type_mask, "geometry"].geom_type.value_counts()
        dominant_type = _type_counts.index[0] if len(_type_counts) > 0 else None

    self_touching_fixed = 0
    for idx in gdf_proc.index:
        geom = gdf_proc.at[idx, "geometry"]
        if geom is None:
            continue
        try:
            if geom.is_empty:
                continue
        except Exception:
            continue

        needs_fix = False
        fix_reason = ""

        if geom.geom_type == "GeometryCollection":
            needs_fix = True
            fix_reason = "GeometryCollection stripped"
        elif geom.geom_type in ("Polygon", "MultiPolygon"):
            try:
                if not geom.is_valid:
                    needs_fix = True
                    fix_reason = "Self-touching rings fixed"
            except Exception:
                needs_fix = True
                fix_reason = "Self-touching rings fixed"

        if needs_fix:
            fixed = safe_make_valid(geom)
            try:
                if fixed is not None and not fixed.is_empty:
                    if fixed.geom_type == "GeometryCollection":
                        fixed = _extract_dominant_from_collection(fixed, dominant_type)
                        fix_reason = "GeometryCollection stripped"
                    if not geom.equals(fixed):
                        changed_list.append((idx, fix_reason, fixed))
                        self_touching_fixed += 1
            except Exception as exc:
                log.debug(f"safe_make_valid post-processing failed for feature {idx}: {exc}")

    extra_stats["self_touching_fixed"] = self_touching_fixed
    log.info(f"   Self-touching / collection features fixed: {self_touching_fixed:,}")

    # Apply Step D geometry changes immediately so that subsequent checks
    # (I–M) operate on the fixed geometries.
    for _d_idx, _d_reason, _d_geom in changed_list:
        gdf_proc.at[_d_idx, "geometry"] = _d_geom

    # ------------------------------------------------------------------ #
    # E. Detect unclosed rings (log only)
    # ------------------------------------------------------------------ #
    log.info("E. Detecting unclosed rings...")
    unclosed_count = 0
    for idx in gdf_proc.index:
        geom = gdf_proc.at[idx, "geometry"]
        if geom is not None and _has_unclosed_rings(geom):
            unclosed_count += 1
    extra_stats["unclosed_rings_detected"] = unclosed_count
    log.info(f"   Unclosed ring features detected: {unclosed_count:,}")

    # ------------------------------------------------------------------ #
    # F. Detect duplicate vertices (log only)
    # ------------------------------------------------------------------ #
    log.info("F. Detecting duplicate vertices...")
    dup_vert_count = 0
    for idx in gdf_proc.index:
        geom = gdf_proc.at[idx, "geometry"]
        if geom is not None and _has_duplicate_vertices(geom):
            dup_vert_count += 1
    extra_stats["duplicate_vertices_detected"] = dup_vert_count
    log.info(f"   Features with duplicate vertices: {dup_vert_count:,}")

    # ------------------------------------------------------------------ #
    # G. Normalize string fields (in-place fix, no garbage entry)
    # ------------------------------------------------------------------ #
    log.info("G. Normalizing string fields...")
    _non_print_re = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

    def _normalize_str(val: object) -> object:
        if not isinstance(val, str):
            return val
        cleaned = _non_print_re.sub("", val.strip())
        return None if cleaned == "" else cleaned

    string_cells_modified = 0
    for col in gdf_proc.select_dtypes(include=["object"]).columns:
        original_col = gdf_proc[col].copy()
        gdf_proc[col] = gdf_proc[col].apply(_normalize_str)
        # Count cells that actually changed (handle None carefully)
        null_to_val = original_col.isna() & gdf_proc[col].notna()
        val_to_null = original_col.notna() & gdf_proc[col].isna()
        val_changed = (
            original_col.notna() & gdf_proc[col].notna() & (original_col != gdf_proc[col])
        )
        string_cells_modified += int((null_to_val | val_to_null | val_changed).sum())

    extra_stats["string_cells_normalized"] = string_cells_modified
    log.info(f"   String cells normalized: {string_cells_modified:,}")

    # ------------------------------------------------------------------ #
    # H. Detect schema inconsistencies (log only)
    # ------------------------------------------------------------------ #
    log.info("H. Detecting schema inconsistencies...")
    schema_issues = 0
    for col in gdf_proc.columns:
        if col == "geometry":
            continue
        col_lower = col.lower()
        dtype = gdf_proc[col].dtype
        non_null = gdf_proc[col].dropna()
        already_flagged = False

        # Columns suggesting year/integer but stored as float
        if any(hint in col_lower for hint in ("year", "yr")):
            if pd.api.types.is_float_dtype(dtype):
                log.warning(f"   Schema: column '{col}' suggests integer (year) but has float dtype")
                schema_issues += 1
                already_flagged = True

        # Columns suggesting numeric semantics but stored as object
        if not already_flagged and any(
            hint in col_lower for hint in ("area", "length", "count", "num", "id")
        ):
            if pd.api.types.is_object_dtype(dtype) and len(non_null) > 0:
                log.warning(f"   Schema: column '{col}' suggests numeric but has object dtype")
                schema_issues += 1
                already_flagged = True

        # Any object column whose values appear to be numeric
        if not already_flagged and pd.api.types.is_object_dtype(dtype) and len(non_null) > 0:
            try:
                sample = non_null.head(20)
                coerced = pd.to_numeric(sample, errors="coerce")
                if len(coerced) > 0 and coerced.notna().sum() / len(coerced) > 0.8:
                    log.warning(
                        f"   Schema: column '{col}' values appear numeric but dtype is object"
                    )
                    schema_issues += 1
            except Exception:
                pass

    extra_stats["schema_inconsistencies"] = schema_issues
    log.info(f"   Schema inconsistencies detected: {schema_issues:,}")

    # Set of already-dropped indices (updated incrementally below)
    already_dropped: set = set()

    # ------------------------------------------------------------------ #
    # I. Drop features with zero-area bounding box (polygons only)
    # ------------------------------------------------------------------ #
    log.info("I. Dropping features with zero-area bounding boxes...")
    zero_bbox_count = 0
    for idx in gdf_proc.index:
        if idx in already_dropped:
            continue
        geom = gdf_proc.at[idx, "geometry"]
        if geom is None:
            continue
        try:
            if geom.is_empty:
                continue
        except Exception:
            continue
        if geom.geom_type not in ("Polygon", "MultiPolygon"):
            continue
        try:
            minx, miny, maxx, maxy = geom.bounds
            if (maxx - minx) == 0 or (maxy - miny) == 0:
                dropped_list.append((idx, "DROPPED", "Zero-area bounding box"))
                already_dropped.add(idx)
                zero_bbox_count += 1
        except Exception:
            pass
    extra_stats["zero_area_bbox_dropped"] = zero_bbox_count
    log.info(f"   Zero-area bbox dropped: {zero_bbox_count:,}")

    # ------------------------------------------------------------------ #
    # J. Drop duplicate geometries (keep first occurrence)
    #
    # Two modes:
    #   - Cross-chunk (chunked pipeline): ``duplicate_geom_hashes`` contains every
    #     SHA1-of-WKB that appears 2+ times globally (from pass 1).  We use
    #     ``processed_geom_hashes`` (shared mutable set) to determine whether the
    #     current feature is the first occurrence.
    #   - Legacy / single-pass: fall back to a local WKT dict as before.
    # ------------------------------------------------------------------ #
    log.info("J. Dropping duplicate geometries...")
    geom_dup_count = 0

    if duplicate_geom_hashes is not None and processed_geom_hashes is not None:
        # Cross-chunk dedup using SHA1 of WKB bytes.
        for idx in gdf_proc.index:
            if idx in already_dropped:
                continue
            geom = gdf_proc.at[idx, "geometry"]
            if geom is None:
                continue
            try:
                geom_hash = hashlib.sha1(geom.wkb).hexdigest()
            except Exception:
                continue
            if geom_hash in duplicate_geom_hashes:
                if geom_hash in processed_geom_hashes:
                    # A previous chunk (or earlier feature in this chunk) already kept
                    # the first occurrence — this is a duplicate.
                    dropped_list.append((idx, "DROPPED", "Duplicate geometry"))
                    already_dropped.add(idx)
                    geom_dup_count += 1
                else:
                    # First occurrence of this duplicated geometry — keep it and record.
                    processed_geom_hashes.add(geom_hash)
            # else: hash is unique globally — no tracking needed.
    else:
        # Legacy single-pass: local WKT dict (original behaviour).
        _seen_wkt: Dict[str, int] = {}
        for idx in gdf_proc.index:
            if idx in already_dropped:
                continue
            geom = gdf_proc.at[idx, "geometry"]
            wkt = geom.wkt if geom is not None else "__null__"
            if wkt in _seen_wkt:
                dropped_list.append((idx, "DROPPED", "Duplicate geometry"))
                already_dropped.add(idx)
                geom_dup_count += 1
            else:
                _seen_wkt[wkt] = idx

    extra_stats["geometry_duplicates_dropped"] = geom_dup_count
    log.info(f"   Duplicate geometries dropped: {geom_dup_count:,}")

    # ------------------------------------------------------------------ #
    # K. Drop duplicate rows — identical attributes AND geometry (keep first)
    #
    # Same two-mode approach as step J, but the hash includes attribute values.
    # ------------------------------------------------------------------ #
    log.info("K. Dropping fully duplicate rows (attributes + geometry)...")
    # Use sorted column order so hashes are canonical and match pass-1 row hashes.
    _attr_cols = sorted(c for c in gdf_proc.columns if c != "geometry")
    row_dup_count = 0

    if duplicate_row_hashes is not None and processed_row_hashes is not None:
        # Cross-chunk dedup using SHA1 of (WKB bytes + repr(attr_tuple)).
        for idx in gdf_proc.index:
            if idx in already_dropped:
                continue
            geom = gdf_proc.at[idx, "geometry"]
            if geom is None:
                continue
            try:
                wkb = geom.wkb
            except Exception:
                continue
            attr_vals = tuple(str(gdf_proc.at[idx, c]) for c in _attr_cols)
            row_hash = hashlib.sha1(wkb + repr(attr_vals).encode()).hexdigest()
            if row_hash in duplicate_row_hashes:
                if row_hash in processed_row_hashes:
                    dropped_list.append((idx, "DROPPED", "Duplicate row (attributes + geometry)"))
                    already_dropped.add(idx)
                    row_dup_count += 1
                else:
                    processed_row_hashes.add(row_hash)
            # else: unique row globally — no tracking needed.
    else:
        # Legacy single-pass: local dict (original behaviour).
        _seen_rows: Dict[tuple, int] = {}
        for idx in gdf_proc.index:
            if idx in already_dropped:
                continue
            geom = gdf_proc.at[idx, "geometry"]
            wkt = geom.wkt if geom is not None else "__null__"
            attr_vals = tuple(str(gdf_proc.at[idx, c]) for c in _attr_cols)
            row_key = (attr_vals, wkt)
            if row_key in _seen_rows:
                dropped_list.append((idx, "DROPPED", "Duplicate row (attributes + geometry)"))
                already_dropped.add(idx)
                row_dup_count += 1
            else:
                _seen_rows[row_key] = idx

    extra_stats["row_duplicates_dropped"] = row_dup_count
    log.info(f"   Duplicate rows dropped: {row_dup_count:,}")

    # ------------------------------------------------------------------ #
    # L. Drop features below minimum area / length threshold
    # ------------------------------------------------------------------ #
    if min_size is not None and min_size > 0:
        log.info(f"L. Dropping features below minimum size ({min_size})...")
        min_size_count = 0
        for idx in gdf_proc.index:
            if idx in already_dropped:
                continue
            geom = gdf_proc.at[idx, "geometry"]
            if geom is None:
                continue
            try:
                if geom.is_empty:
                    continue
            except Exception:
                continue
            gtype = geom.geom_type
            try:
                if gtype in ("Polygon", "MultiPolygon"):
                    if geom.area < min_size:
                        dropped_list.append((idx, "DROPPED", "Below minimum size threshold"))
                        already_dropped.add(idx)
                        min_size_count += 1
                elif gtype in ("LineString", "MultiLineString", "LinearRing"):
                    if geom.length < min_size:
                        dropped_list.append((idx, "DROPPED", "Below minimum size threshold"))
                        already_dropped.add(idx)
                        min_size_count += 1
            except Exception:
                pass
        extra_stats["below_min_size_dropped"] = min_size_count
        log.info(f"   Below minimum size dropped: {min_size_count:,}")

    # ------------------------------------------------------------------ #
    # M. Drop sliver polygons (isoperimetric quotient < 0.01)
    # ------------------------------------------------------------------ #
    log.info("M. Dropping sliver polygons...")
    sliver_count = 0
    for idx in gdf_proc.index:
        if idx in already_dropped:
            continue
        geom = gdf_proc.at[idx, "geometry"]
        if geom is None:
            continue
        try:
            if geom.is_empty:
                continue
        except Exception:
            continue
        if geom.geom_type not in ("Polygon", "MultiPolygon"):
            continue
        try:
            area = geom.area
            perimeter = geom.length  # for polygons, .length == perimeter
            if area > 0 and perimeter > 0:
                iq = 4.0 * math.pi * area / (perimeter ** 2)
                if iq < 0.01:
                    dropped_list.append((idx, "DROPPED", "Sliver polygon"))
                    already_dropped.add(idx)
                    sliver_count += 1
        except Exception:
            pass
    extra_stats["slivers_dropped"] = sliver_count
    log.info(f"   Sliver polygons dropped: {sliver_count:,}")

    # ------------------------------------------------------------------ #
    # Apply drops to gdf_proc
    # ------------------------------------------------------------------ #
    drop_indices = {idx for idx, _, _ in dropped_list}
    gdf_proc = gdf_proc[~gdf_proc.index.isin(drop_indices)]

    return gdf_proc, extra_stats, dropped_list, changed_list
