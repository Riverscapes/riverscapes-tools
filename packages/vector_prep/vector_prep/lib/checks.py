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
    min_size_drop: bool = False,
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
        "below_min_size_detected": 0,
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
    # D. Fix invalid polygons and unwrap any GeometryCollection results.
    #    Changed features are recorded in changed_list (VP_Operation=CHANGED)
    #    so they appear in the garbage output for inspection, but they are
    #    kept in the main output with their repaired geometry.
    #
    # WHY THIS STEP EXISTS
    # --------------------
    # Polygon rings that touch themselves ("bowtie" or "figure-eight" shapes)
    # or self-intersect are flagged as invalid by Shapely/GEOS.  Many GIS
    # operations (overlays, area calculations, spatial joins) produce silently
    # wrong results on invalid geometries, so we repair them before any
    # further processing.
    #
    # We also encounter GeometryCollection inputs directly — e.g. a layer
    # that already has mixed geometry types stored as collections.  These
    # need to be unwrapped before downstream tools can use them.
    #
    # HOW THE REPAIR WORKS (two distinct steps)
    # ------------------------------------------
    # Step 1 — safe_make_valid(geom)
    #   Calls Shapely's make_valid(), which resolves self-intersections and
    #   invalid rings by splitting or restructuring the geometry according to
    #   the OGC validity rules.  For example, a bowtie polygon (two triangles
    #   sharing a single vertex) is split into two separate valid polygons.
    #   If make_valid() raises, we fall back to geom.buffer(0) which achieves
    #   a similar result via a different algorithm.  THIS is the step that
    #   actually fixes the geometry.
    #
    # Step 2 — _extract_dominant_from_collection(fixed, dominant_type)
    #   make_valid() sometimes returns a GeometryCollection instead of a
    #   simple Polygon/MultiPolygon.  This happens because the repair can
    #   produce boundary artefacts — stray LineStrings or Points at the
    #   former self-intersection points — alongside the repaired polygon
    #   parts.  _extract_dominant_from_collection() does NOT fix anything;
    #   it simply discards those artefacts and reunites the polygon parts
    #   using unary_union, giving back a clean Polygon or MultiPolygon.
    #   The dominant_type argument tells it which geometry type to keep
    #   (derived from the rest of the layer so we don't change the layer's
    #   overall geometry type).
    #
    # WHY WE DON'T DROP THESE FEATURES
    # ----------------------------------
    # Invalid geometry is usually a digitising or processing artefact, not
    # evidence that the feature itself is wrong.  The repaired shape still
    # represents real-world data so we keep it.  We do record it in the
    # garbage output (VP_Operation=CHANGED) so the user can inspect whether
    # the repair looked reasonable.
    # ------------------------------------------------------------------ #
    log.info("D. Repairing invalid polygons and unwrapping GeometryCollections...")

    # Use the dominant geometry type computed globally in pass 1 when
    # available.  This ensures that GeometryCollection unwrapping is
    # consistent across all chunks (e.g. always keeps Polygon parts, never
    # accidentally switches to LineString on a chunk that happens to contain
    # more lines than polygons).  Fall back to deriving it from this chunk
    # in legacy / single-pass mode.
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
            # Input is already a GeometryCollection — unwrap it directly.
            # No validity repair needed; we just need to extract the useful parts.
            needs_fix = True
            fix_reason = "GeometryCollection unwrapped"
        elif geom.geom_type in ("Polygon", "MultiPolygon"):
            try:
                if not geom.is_valid:
                    # Invalid polygon — needs make_valid() repair (Step 1 above).
                    needs_fix = True
                    fix_reason = "Invalid polygon repaired"
            except Exception:
                # is_valid check itself failed — treat as invalid and attempt repair.
                needs_fix = True
                fix_reason = "Invalid polygon repaired"

        if needs_fix:
            # Step 1: repair the geometry (this is what actually fixes the rings).
            fixed = safe_make_valid(geom)
            try:
                if fixed is not None and not fixed.is_empty:
                    if fixed.geom_type == "GeometryCollection":
                        # Step 2: make_valid returned a GeometryCollection, which means
                        # the repair split the geometry and/or produced boundary artefacts
                        # (stray lines/points at former self-intersection sites).  Unwrap
                        # it to get back a clean polygon (or whichever type dominates).
                        fixed = _extract_dominant_from_collection(fixed, dominant_type)
                        fix_reason = "Invalid polygon repaired (GeometryCollection unwrapped)"
                    if not geom.equals(fixed):
                        changed_list.append((idx, fix_reason, fixed))
                        self_touching_fixed += 1
            except Exception as exc:
                log.debug(f"Geometry repair post-processing failed for feature {idx}: {exc}")

    extra_stats["self_touching_fixed"] = self_touching_fixed
    log.info(f"   Invalid geometries repaired: {self_touching_fixed:,}")

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
    #
    # We look for two patterns:
    #   1. Columns whose name suggests they hold integers (e.g. year) but
    #      whose dtype is float — a sign the data was round-tripped through
    #      a format that has no integer type (e.g. Shapefile stores all
    #      numerics as double).
    #   2. Columns whose name suggests a measurement (area, length, count)
    #      but whose dtype is object (string) — likely a parsing error.
    #   3. Object columns whose *values* look numeric — same cause as above
    #      but caught by inspecting a sample of the data rather than the
    #      column name.
    #
    # IMPORTANT — identifier / code columns are intentionally excluded.
    # Route numbers, FIPS codes, HUC codes, ZIP codes and similar look
    # numeric but are identifiers: they should stay as strings.  Reasons:
    #   - Leading zeros are significant (FIPS "06037" ≠ integer 6037).
    #   - Arithmetic on route numbers is meaningless.
    #   - Mixed values are common ("I-90", "US-101", "14a").
    # Any column whose name contains a pattern from IDENTIFIER_HINTS is
    # skipped for the value-inspection check (check 3) so these columns
    # never produce a false-positive warning.
    # ------------------------------------------------------------------ #
    log.info("H. Detecting schema inconsistencies...")

    # Column name fragments that indicate the column holds identifier /
    # code values that legitimately look numeric but must stay as strings.
    IDENTIFIER_HINTS = (
        "route", "rout",      # road / trail route numbers
        "fips", "fipsc",      # FIPS codes (leading zeros matter)
        "huc",                # hydrologic unit codes
        "zip",                # postal codes (leading zeros matter)
        "code", "cod",        # generic code columns
        "guid", "uuid",       # globally unique identifiers
        "interstate",         # interstate highway identifiers
        "intersta",           # truncated interstate column names
    )

    # Column name fragments that suggest a column *should* hold a numeric
    # measurement.  We intentionally exclude "id" and "num" here because
    # they are too ambiguous: "road_id" or "feature_num" are often string
    # identifiers, not quantities.
    NUMERIC_HINTS = ("area", "length", "count")

    schema_issues = 0
    for col in gdf_proc.columns:
        if col == "geometry":
            continue
        col_lower = col.lower()
        dtype = gdf_proc[col].dtype
        non_null = gdf_proc[col].dropna()
        already_flagged = False

        # Check 1: columns suggesting year/integer but stored as float.
        # Shapefiles in particular store all numbers as double, so year
        # columns come back as 2024.0 instead of 2024.
        if any(hint in col_lower for hint in ("year", "yr")):
            if pd.api.types.is_float_dtype(dtype):
                log.warning(f"   Schema: column '{col}' suggests integer (year) but has float dtype")
                schema_issues += 1
                already_flagged = True

        # Check 2: columns whose name strongly implies a numeric measurement
        # but are stored as object (string).
        if not already_flagged and any(hint in col_lower for hint in NUMERIC_HINTS):
            if pd.api.types.is_object_dtype(dtype) and len(non_null) > 0:
                log.warning(f"   Schema: column '{col}' suggests numeric measurement but has object dtype")
                schema_issues += 1
                already_flagged = True

        # Check 3: object columns whose sampled values are overwhelmingly
        # numeric-looking — but skip known identifier/code columns because
        # those legitimately contain numeric-looking strings (route numbers,
        # FIPS codes, etc.) that must not be converted.
        is_identifier = any(hint in col_lower for hint in IDENTIFIER_HINTS)
        if not already_flagged and not is_identifier and pd.api.types.is_object_dtype(dtype) and len(non_null) > 0:
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
    # L. Drop or detect features below minimum area / length threshold
    # ------------------------------------------------------------------ #
    if min_size is not None and min_size > 0:
        if min_size_drop:
            log.info(f"L. Dropping features below minimum size ({min_size})...")
        else:
            log.info(f"L. Detecting features below minimum size ({min_size})...")
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
                below = False
                if gtype in ("Polygon", "MultiPolygon"):
                    if geom.area < min_size:
                        below = True
                elif gtype in ("LineString", "MultiLineString", "LinearRing"):
                    if geom.length < min_size:
                        below = True
                if below:
                    if min_size_drop:
                        dropped_list.append((idx, "DROPPED", "Below minimum size threshold"))
                        already_dropped.add(idx)
                    min_size_count += 1
            except Exception:
                pass
        if min_size_drop:
            extra_stats["below_min_size_dropped"] = min_size_count
            log.info(f"   Below minimum size dropped: {min_size_count:,}")
        else:
            extra_stats["below_min_size_detected"] = min_size_count
            log.info(f"   Below minimum size detected: {min_size_count:,}")

    # ------------------------------------------------------------------ #
    # M. Drop sliver polygons — two-phase filter
    #
    # A "sliver" is a polygon that is disproportionately thin relative to its
    # area: typically an artefact of overlay operations or digitising errors.
    #
    # WHY TWO PHASES?
    # The naive approach — isoperimetric quotient (IQ) alone — is fast but
    # produces false positives for any polygon with a naturally complex or
    # jagged boundary (e.g. wetlands, parcels with many vertices, polygons
    # with holes).  A high perimeter drives IQ toward zero even for wide,
    # valid shapes, so IQ alone cannot be used as a definitive test.
    #
    # The definitive test is a negative buffer: shrink the polygon inward by
    # half the minimum acceptable width.  If the result is empty, no interior
    # point was more than min_width/2 metres from the boundary — i.e. the
    # polygon is genuinely thin everywhere.  This is geometrically exact and
    # completely unaffected by edge complexity or holes.  However it is
    # significantly more expensive than IQ for complex geometries.
    #
    # TWO-PHASE STRATEGY:
    #   Phase 1 — IQ pre-filter (fast)
    #     IQ = 4π·area / perimeter²  ∈ (0, 1]
    #     IQ → 1 for a circle (most compact); IQ → 0 for very thin shapes.
    #     We use a deliberately LOOSE threshold (IQ_CANDIDATE_THRESHOLD = 0.1)
    #     so that the pre-filter casts a wide net.  Any feature with IQ above
    #     this threshold is provably too compact to be a sliver and is skipped
    #     immediately — no buffer call needed.
    #     A loose threshold means more false positives reach phase 2, but
    #     critically ZERO real slivers are missed: a genuinely thin polygon
    #     always has a low IQ regardless of edge complexity.
    #
    #   Phase 2 — negative buffer confirmation (slow, only for candidates)
    #     For the small subset of features that pass the IQ pre-filter we
    #     shrink the polygon inward by MIN_SLIVER_WIDTH / 2 metres.  If the
    #     buffered result is None or empty the polygon is confirmed as a
    #     sliver and dropped.  Valid complex shapes survive because they have
    #     interior points far from any boundary.
    #
    # THRESHOLD RATIONALE:
    #   IQ_CANDIDATE_THRESHOLD = 0.1  — loose enough to catch all real
    #     slivers; a circle of radius r has IQ = 1.0 so only shapes with
    #     significantly non-circular perimeter reach phase 2.
    #   MIN_SLIVER_WIDTH = 1.0 m  — a polygon narrower than 1 metre at its
    #     widest interior point has no practical value in most GIS contexts.
    #     The buffer offset is half this (0.5 m) because the negative buffer
    #     shrinks from *both* sides simultaneously.
    # ------------------------------------------------------------------ #
    log.info("M. Dropping sliver polygons (two-phase: IQ pre-filter + negative buffer)...")

    # Phase 1 threshold: features with IQ above this are immediately kept.
    # Deliberately loose — we prefer false positives (extra buffer calls) over
    # false negatives (real slivers that slip through unchecked).
    IQ_CANDIDATE_THRESHOLD = 0.1

    # Phase 2 threshold: minimum acceptable interior width in CRS units (metres
    # when the data is in a metric Cartesian CRS such as EPSG:5070).
    MIN_SLIVER_WIDTH = 1.0
    # The negative buffer offset is half the minimum width because the buffer
    # erodes the polygon equally from all sides simultaneously.
    BUFFER_INSET = MIN_SLIVER_WIDTH / 2.0

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
            # Slivers are a polygon-only concept; lines/points are handled by
            # the minimum-length check in step L.
            continue
        try:
            area = geom.area
            perimeter = geom.length  # .length on a polygon returns perimeter

            if area <= 0 or perimeter <= 0:
                # Degenerate geometry — skip (already handled upstream).
                continue

            # ---- Phase 1: IQ pre-filter --------------------------------
            # IQ is O(1) — just arithmetic on cached area/perimeter values.
            # Features above the threshold are provably not slivers and cost
            # nothing further.
            iq = 4.0 * math.pi * area / (perimeter ** 2)
            if iq > IQ_CANDIDATE_THRESHOLD:
                # Compact enough that no sliver check is required.
                continue

            # ---- Phase 2: negative buffer confirmation -----------------
            # Reached only for features with low IQ (thin *or* jagged).
            # The buffer call is the expensive step; we reach this only for
            # the small fraction of features that are plausibly thin.
            buffered = geom.buffer(-BUFFER_INSET)
            if buffered is None or buffered.is_empty:
                # The polygon has no interior point more than BUFFER_INSET
                # metres from its boundary — confirmed sliver.
                dropped_list.append((idx, "DROPPED", "Sliver polygon"))
                already_dropped.add(idx)
                sliver_count += 1
            # else: low IQ but survives the buffer test — complex valid shape
            # (e.g. jagged wetland boundary, heavily indented parcel).  Keep.

        except Exception:
            # If either the IQ arithmetic or the buffer call fails for any
            # reason, leave the feature in place rather than silently dropping
            # something we could not evaluate.
            pass

    extra_stats["slivers_dropped"] = sliver_count
    log.info(f"   Sliver polygons dropped: {sliver_count:,}")

    # ------------------------------------------------------------------ #
    # Apply drops to gdf_proc
    # ------------------------------------------------------------------ #
    drop_indices = {idx for idx, _, _ in dropped_list}
    gdf_proc = gdf_proc[~gdf_proc.index.isin(drop_indices)]

    return gdf_proc, extra_stats, dropped_list, changed_list
