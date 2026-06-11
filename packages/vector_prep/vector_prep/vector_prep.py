"""
Clean a vector layer by fixing invalid geometries and simplifying it. Ostensibly used for
preparing vector layers for use in the Riverscapes Reporting platform both as a picklist
layer, but also for storing in Athena for use in reports.

The input is a single ShapeFile or GeoPackage vector layer. It can be in any projection,
and any fields.

The output is always a GeoPackage layer with cleaned geometries, reprojected to EPSG:4326.

Processing is done in a windowed/chunked fashion so that very large datasets can be handled
without loading the entire file into memory:

  Pass 1 – lightweight hash scan (pyogrio): iterates all features in chunks, builds sets of SHA1 hashes
            for geometries / full rows that appear more than once (for cross-chunk dedup).
            Geometries are reprojected to the target EPSG before hashing so that pass-1
            hashes are computed in the same coordinate space as pass-2 (steps J & K).
  Pass 2 – chunked processing: reads chunk_size features at a time, runs checks + clean,
            then appends to the output GeoPackage incrementally.

Philip Bailey
27 Nov 2025
"""
from __future__ import annotations

import argparse
import hashlib
import math
import os
import re
import sys
import traceback
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

import geopandas as gpd
import pyogrio
from pyproj import CRS as ProjCRS
from pyproj import Transformer
from shapely.ops import transform as shapely_transform
from rsxml import Logger, ProgressBar, dotenv

from .lib.checks import run_checks
from .lib.clean import clean_geometries
from .lib.garbage import write_garbage_chunk
from .lib.geometry_utils import _geom_type_str
from .lib.output import output_gdf, output_gdf_chunk  # output_gdf re-exported for orchestrate scripts
from .lib.report import print_report


# ---------------------------------------------------------------------------
# Pass-1 string-normalisation helper (must match step G in checks.py)
# ---------------------------------------------------------------------------

_non_print_re = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _normalize_str_for_hash(val: object) -> object:
    """Apply the same normalisation as step G (checks.py) so that pass-1
    attribute hashes match the hashes produced after step G in pass 2.

    Non-string values are returned unchanged.
    """
    if not isinstance(val, str):
        return val
    cleaned = _non_print_re.sub("", val.strip())
    return None if cleaned == "" else cleaned


# ---------------------------------------------------------------------------
# Pass-1 helper
# ---------------------------------------------------------------------------


def _build_duplicate_hash_sets(
    input_path: str,
    layer_name: Optional[str],
    epsg: Optional[int],
) -> Tuple[int, Set[str], Set[str], Optional[str]]:
    """Iterate all features with pyogrio and build sets of duplicate hashes.

    Geometries are reprojected to *epsg* before hashing so that the WKB bytes
    computed here are in the same coordinate space as the hashes computed in
    pass 2 (checks J and K).  When *epsg* is None no reprojection is applied.

    Attribute columns are sorted alphabetically before hashing to ensure
    column order is canonical and matches the sorted order used in step K.

    We intentionally do NOT store geometries — only hash strings — so that
    memory usage is O(n_features) in hash strings, not in geometry objects.

    Returns:
        total_features: total feature count from pyogrio.read_info().
        duplicate_geom_hashes: SHA1-of-WKB hashes that appear in 2+ features.
        duplicate_row_hashes: SHA1-of-(WKB + repr(attrs)) hashes appearing 2+ times.
        dominant_type: Most common non-GeometryCollection base geometry type,
            or None if the dataset is empty.
    """
    geom_hash_counts: Dict[str, int] = {}
    row_hash_counts: Dict[str, int] = {}
    geom_type_counts: Dict[str, int] = {}

    # Use pyogrio.read_info() for metadata (total features, CRS, field names).
    layer_kwargs = {"layer": layer_name} if layer_name else {}
    info = pyogrio.read_info(input_path, **layer_kwargs)
    total_features = info["features"]
    # attr_cols: sorted field names (no geometry) — canonical order for row hashing.
    attr_cols = sorted(info["fields"])

    # Build a geometry transformer when a target EPSG is specified.
    transformer: Optional[Transformer] = None
    if epsg is not None and info.get("crs") is not None:
        try:
            src_proj_crs = ProjCRS.from_user_input(info["crs"])
            dst_proj_crs = ProjCRS.from_epsg(epsg)
            transformer = Transformer.from_crs(src_proj_crs, dst_proj_crs, always_xy=True)
        except Exception:
            transformer = None

    # Chunk size for pass 1 — large enough to amortise pyogrio overhead but
    # small enough not to balloon memory.
    _P1_CHUNK = 50_000

    pbar = ProgressBar(total_features, text="Pass 1: hashing features")
    features_hashed = 0
    for offset in range(0, max(total_features, 1), _P1_CHUNK):
        chunk = pyogrio.read_dataframe(
            input_path,
            skip_features=offset,
            max_features=_P1_CHUNK,
            **layer_kwargs,
        )
        for _, row in chunk.iterrows():
            features_hashed += 1
            pbar.update(features_hashed)
            geom = row.geometry
            if geom is None:
                continue

            # Count geometry types for dominant_type computation.
            geom_type = geom.geom_type if geom is not None else ""
            if geom_type:
                geom_type_counts[geom_type] = geom_type_counts.get(geom_type, 0) + 1

            try:
                shp = geom
                # Reproject to target CRS before hashing so WKB bytes match pass 2.
                if transformer is not None:
                    shp = shapely_transform(transformer.transform, shp)
                # Strip Z so WKB bytes match pass-2 hashes (step A drops Z
                # before steps J & K compute their hashes).
                if shp.has_z:
                    shp = shapely_transform(lambda x, y, *args: (x, y), shp)
                wkb = shp.wkb
            except Exception:
                continue

            # Geometry hash — SHA1 of reprojected, 2-D WKB bytes.
            g_hash = hashlib.sha1(wkb).hexdigest()
            geom_hash_counts[g_hash] = geom_hash_counts.get(g_hash, 0) + 1

            # Row hash — geometry + normalised attribute values (sorted column
            # order). Normalisation mirrors step G so hashes match pass 2.
            attr_vals = tuple(
                str(_normalize_str_for_hash(row[c] if c in row.index else None))
                for c in attr_cols
            )
            r_hash = hashlib.sha1(wkb + repr(attr_vals).encode()).hexdigest()
            row_hash_counts[r_hash] = row_hash_counts.get(r_hash, 0) + 1

    pbar.finish()

    # Keep only hashes that appear more than once — these are the candidates
    # that need first-occurrence tracking during pass 2.
    duplicate_geom_hashes: Set[str] = {
        h for h, n in geom_hash_counts.items() if n > 1
    }
    duplicate_row_hashes: Set[str] = {
        h for h, n in row_hash_counts.items() if n > 1
    }

    # Compute dominant geometry type (base type, excluding GeometryCollection).
    non_coll_counts: Dict[str, int] = {}
    for t, n in geom_type_counts.items():
        if t != "GeometryCollection":
            base = t.replace("Multi", "")
            non_coll_counts[base] = non_coll_counts.get(base, 0) + n
    dominant_type: Optional[str] = (
        max(non_coll_counts, key=non_coll_counts.__getitem__) if non_coll_counts else None
    )

    return total_features, duplicate_geom_hashes, duplicate_row_hashes, dominant_type


# ---------------------------------------------------------------------------
# Pass-2 helper
# ---------------------------------------------------------------------------


def _read_chunk(
    input_path: str,
    layer_name: Optional[str],
    offset: int,
    chunk_size: int,
) -> gpd.GeoDataFrame:
    """Read a slice of features starting at *offset* (0-indexed) using pyogrio."""
    layer_kwargs = {"layer": layer_name} if layer_name else {}
    return pyogrio.read_dataframe(
        input_path,
        skip_features=offset,
        max_features=chunk_size,
        **layer_kwargs,
    )


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def vector_prep(
    input_dataset: Path | str,
    layer_name: Optional[str],
    tolerance: float,
    epsg: Optional[int],
    output_path: Optional[str] = None,
    garbage_path: Optional[str] = None,
    min_size: float = None,
    chunk_size: int = 10_000,
) -> Dict:
    """Vector Prep: clean, validate, and optionally simplify a vector dataset.

    Processing is chunked: features are read and processed in slices of
    *chunk_size* so that arbitrarily large datasets can be handled with bounded
    memory.  A two-pass strategy is used so that cross-chunk duplicate detection
    (checks J and K) works correctly:

      * Pass 1 scans every feature with pyogrio (in chunks) to discover which geometry/row
        hashes appear more than once.  Geometries are reprojected to *epsg*
        before hashing so the hash values match those produced during pass 2.
      * Pass 2 processes features in chunks, passing the duplicate-hash sets to
        run_checks() for accurate first-occurrence tracking.

    Args:
        input_dataset: Path to the input vector file.
        layer_name: Layer name for GeoPackage inputs.
        tolerance: Simplification tolerance in metres (0 = skip).
        epsg: EPSG code for the Cartesian CRS used during processing.
        output_path: Optional path to write the cleaned GeoPackage.
        garbage_path: Optional path to write dropped/changed features.
        min_size: Minimum area (m²) for polygons or length (m) for lines.
        chunk_size: Number of features to load at once during pass 2.

    Returns:
        stats dict (suitable for print_report).
    """
    log = Logger("Vector Prep")
    input_dataset = str(Path(input_dataset).resolve())

    if not os.path.exists(input_dataset):
        raise Exception(f"Input file does not exist: {input_dataset}")

    # ------------------------------------------------------------------
    # Delete any pre-existing output / garbage files so that re-runs
    # always start clean rather than appending to stale data.
    # ------------------------------------------------------------------
    for path_to_clean, label in ((output_path, "output"), (garbage_path, "garbage")):
        if path_to_clean and os.path.exists(path_to_clean) and not os.path.isdir(path_to_clean):
            try:
                os.remove(path_to_clean)
                log.info(f"Removed existing {label} file: {path_to_clean}")
            except Exception as rm_err:
                log.warning(f"Could not remove existing {label} file: {rm_err}")

    # ------------------------------------------------------------------
    # Pass 1: scan all features to find duplicate geometry / row hashes.
    # Geometries are reprojected to *epsg* before hashing.
    # ------------------------------------------------------------------
    log.info(f"Pass 1: scanning all features for duplicate hashes: {input_dataset}")
    total_features, duplicate_geom_hashes, duplicate_row_hashes, dominant_type = (
        _build_duplicate_hash_sets(input_dataset, layer_name, epsg)
    )
    log.info(
        f"Pass 1 complete — {total_features:,} features scanned; "
        f"{len(duplicate_geom_hashes):,} duplicate geometry hash(es), "
        f"{len(duplicate_row_hashes):,} duplicate row hash(es)."
    )
    if dominant_type:
        log.info(f"Pass 1 dominant geometry type: {dominant_type}")

    # ------------------------------------------------------------------
    # Pass 2: process features in chunks.
    # ------------------------------------------------------------------
    total_chunks = max(1, math.ceil(total_features / chunk_size))

    # Mutable sets that grow across chunks — track first-seen occurrences
    # so we know which duplicate features to keep vs. drop.
    processed_geom_hashes: Set[str] = set()
    processed_row_hashes: Set[str] = set()

    # Accumulator for all statistics across chunks.
    total_stats: Dict = {
        "initial_count": total_features,
        "input_count": 0,
        "output_count": 0,
        "mixed_types_detected": False,
        # run_checks counters (int)
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
        # clean_geometries counters (int)
        "null_or_empty": 0,
        "invalid_fixed": 0,
        "invalid_unfixed": 0,
        "simplified_count": 0,
    }
    total_dropped_by_reason: Dict[str, int] = {}
    total_dropped_by_geom_type: Dict[str, int] = {}

    def _accum_drop(orig_chunk: gpd.GeoDataFrame, orig_idx: int, reason: str) -> None:
        """Helper: tally a dropped feature into the global reason/type dicts."""
        total_dropped_by_reason[reason] = total_dropped_by_reason.get(reason, 0) + 1
        geom_val = (
            orig_chunk.at[orig_idx, "geometry"] if orig_idx in orig_chunk.index else None
        )
        gt = _geom_type_str(geom_val)
        total_dropped_by_geom_type[gt] = total_dropped_by_geom_type.get(gt, 0) + 1

    # Separate written-flags for output and garbage so that a chunk that
    # produces zero rows (and is therefore skipped) does not advance the flag.
    output_written: bool = False
    garbage_written: bool = False

    # Global FID counter — incremented after each successful output write so
    # FID values are unique across all chunks.
    global_fid_offset: int = 0

    pbar2 = ProgressBar(total_chunks, text="Pass 2: processing chunks")
    for chunk_idx in range(total_chunks):
        offset = chunk_idx * chunk_size
        feat_start = offset + 1
        feat_end = min(offset + chunk_size, total_features)
        log.info(f"Chunk {chunk_idx + 1}/{total_chunks} — features {feat_start:,}-{feat_end:,}")

        # ---- Read chunk ----------------------------------------
        chunk_gdf = _read_chunk(
            input_dataset, layer_name, offset, chunk_size,
        )
        if len(chunk_gdf) == 0:
            pbar2.update(chunk_idx + 1)
            continue

        # Reset index to 0-based so dropped/changed indices are stable
        # within this chunk and match original_chunk.
        chunk_gdf = chunk_gdf.reset_index(drop=True)
        total_stats["input_count"] += len(chunk_gdf)

        # ---- Detect mixed geometry types (log-only) -------------
        geom_types = chunk_gdf.geom_type.value_counts().to_dict()
        base_types = {
            t.replace("Multi", "")
            for t in geom_types.keys()
            if t not in (None, "NoneType", "None", "NaN")
            and t != "GeometryCollection"
        }
        if len(base_types) > 1:
            total_stats["mixed_types_detected"] = True

        # ---- Reproject to Cartesian CRS for processing ----------
        if epsg:
            try:
                chunk_gdf = chunk_gdf.to_crs(epsg=epsg)
            except Exception as e:
                raise Exception(
                    f"Failed to reproject chunk {chunk_idx} to EPSG:{epsg}: {e}"
                ) from e

        # Snapshot before any modifications (needed for garbage output).
        original_chunk = chunk_gdf.copy()

        # ---- Run checks A–M ------------------------------------
        chunk_gdf, extra_stats, dropped_list, changed_list = run_checks(
            chunk_gdf,
            min_size,
            duplicate_geom_hashes=duplicate_geom_hashes,
            duplicate_row_hashes=duplicate_row_hashes,
            processed_geom_hashes=processed_geom_hashes,
            processed_row_hashes=processed_row_hashes,
            dominant_type=dominant_type,
        )

        # If a feature was both changed (Step D) and later dropped (Steps I–M),
        # the DROP takes precedence — remove it from changed_list.
        dropped_indices = {idx for idx, _, _ in dropped_list}
        changed_list = [
            (idx, r, g) for idx, r, g in changed_list if idx not in dropped_indices
        ]

        # ---- Clean geometries ----------------------------------
        cleaned_geom_series, clean_stats, clean_dropped = clean_geometries(
            chunk_gdf.geometry, simplify_tolerance=tolerance
        )
        chunk_gdf["geometry"] = cleaned_geom_series
        chunk_gdf = chunk_gdf[~chunk_gdf["geometry"].isna()]
        chunk_gdf = chunk_gdf[~chunk_gdf["geometry"].is_empty]

        total_stats["output_count"] += len(chunk_gdf)

        # ---- Accumulate stats ----------------------------------
        for key, val in extra_stats.items():
            if isinstance(val, bool):
                total_stats[key] = total_stats.get(key, False) or val
            elif isinstance(val, int):
                total_stats[key] = total_stats.get(key, 0) + val

        for key, val in clean_stats.items():
            if key == "input_count":
                # Already counted above from len(chunk_gdf) pre-run_checks.
                continue
            if isinstance(val, bool):
                total_stats[key] = total_stats.get(key, False) or val
            elif isinstance(val, int):
                total_stats[key] = total_stats.get(key, 0) + val

        for orig_idx, _op, reason in dropped_list:
            _accum_drop(original_chunk, orig_idx, reason)
        for orig_idx, _op, reason in clean_dropped:
            _accum_drop(original_chunk, orig_idx, reason)

        # ---- Write output chunk --------------------------------
        if len(chunk_gdf) > 0 and output_path:
            # Assign globally-unique FID values across all chunks.
            chunk_gdf = chunk_gdf.reset_index(drop=True)
            chunk_gdf["FID"] = range(
                global_fid_offset, global_fid_offset + len(chunk_gdf)
            )
            chunk_gdf["FID"] = chunk_gdf["FID"].astype("int64")
            output_gdf_chunk(
                chunk_gdf, output_path, layer_name, first_chunk=not output_written
            )
            global_fid_offset += len(chunk_gdf)
            output_written = True

        # ---- Write garbage chunk -------------------------------
        if garbage_path:
            n_garbage = write_garbage_chunk(
                original_chunk,
                dropped_list,
                changed_list,
                clean_dropped,
                garbage_path,
                first_chunk=not garbage_written,
            )
            if n_garbage > 0:
                garbage_written = True

        pbar2.update(chunk_idx + 1)

    pbar2.finish()

    # ------------------------------------------------------------------
    # Finalise stats
    # ------------------------------------------------------------------
    total_stats["dropped_by_reason"] = total_dropped_by_reason
    total_stats["dropped_by_geom_type"] = total_dropped_by_geom_type

    log.info(f"Input features:                   {total_stats['input_count']:,}")
    log.info(f"Null/empty geometries found:      {total_stats['null_or_empty']:,}")
    log.info(f"Invalid geometries fixed:         {total_stats['invalid_fixed']:,}")
    log.info(f"Invalid geometries unfixed:       {total_stats['invalid_unfixed']:,}")
    log.info(f"Features simplified ({tolerance} m):  {total_stats['simplified_count']:,}")
    log.info(f"Output features:                  {total_stats['output_count']:,}")

    if total_stats["output_count"] == 0:
        raise Exception("No valid geometries remain after cleaning. Aborting.")

    return total_stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    """CLI entry point for vector_prep."""
    parser = argparse.ArgumentParser(
        description="Vector Prep: Clean and simplify vector datasets."
    )
    parser.add_argument("input", help="Input vector (shapefile, gpkg, etc.)")
    parser.add_argument("--output", help="(OPTIONAL) Output vector path")
    parser.add_argument(
        "--garbage",
        help=(
            "(OPTIONAL) If provided, dropped/changed features will be saved to this GeoPackage "
            "for inspection. Must be a .gpkg file."
        ),
        default=None,
    )
    parser.add_argument(
        "--layer",
        help="Layer name (for geopackage). If not provided and input is geopackage, first layer is used.",
        default=None,
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        help="Simplify tolerance in METRES (0 to skip).",
        default=0.0,
    )
    parser.add_argument(
        "--epsg",
        type=int,
        help=(
            "Cartesian CRS EPSG code to reproject to before processing (optional). "
            "Default is 5070 (NAD83 / Conus Albers)."
        ),
        default=5070,
    )
    parser.add_argument(
        "--min_size",
        type=float,
        help=(
            "Minimum area (m²) for polygons or minimum length (m) for lines. "
            "Features smaller than this are dropped. Default is 1.0."
        )
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        help=(
            "Number of features to process at a time (windowed/chunked mode). "
            "Larger values use more memory but may be faster. Default is 10000."
        ),
        default=10_000,
    )
    parser.add_argument(
        "--verbose",
        help="(optional) a little extra logging",
        action="store_true",
        default=False,
    )
    args = dotenv.parse_args_env(parser)

    log = Logger("Vector Prep")
    log_dir = os.path.dirname(args.output) if args.output else "."

    # Log file name is "<ORIGINAL_BASENAME>_vector_prep.log" if output path provided, otherwise "vector_prep.log".
    if args.output:
        base_name = os.path.splitext(os.path.basename(args.output))[0]
        log_file_name = f"{base_name}_vector_prep.log"
        log_dir = os.path.dirname(args.output)
    else:
        log_file_name = "vector_prep.log"
        log_dir = "."
    log.setup(
        log_path=os.path.join(log_dir, log_file_name),
        verbose=args.verbose,
    )

    try:
        stats = vector_prep(
            args.input,
            args.layer,
            float(args.tolerance),
            int(args.epsg),
            output_path=args.output if args.output else None,
            garbage_path=args.garbage,
            min_size=float(args.min_size) if args.min_size is not None else None,
            chunk_size=int(args.chunk_size),
        )
        if not args.output:
            log.info("No --output path provided; skipping output write.")
        print_report(stats, args.garbage)
    except Exception as e:
        log.error("Vector prep failed: %s", e)
        log.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
