"""
Clean a vector layer by fixing invalid geometries and simplifying it. Ostensibly used for
preparing vector layers for use in the Riverscapes Reporting platform both as a picklist
layer, but also for storing in Athena for use in reports.

The input is a single ShapeFile or GeoPackage vector layer. It can be in any projection,
and any fields.

The output is always a GeoPackage layer with cleaned geometries, reprojected to EPSG:4326.

Processing is done in a windowed/chunked fashion so that very large datasets can be handled
without loading the entire file into memory:

  Pass 1 - lightweight hash scan (pyogrio): iterates all features in chunks, builds sets of SHA1 hashes
            for geometries / full rows that appear more than once (for cross-chunk dedup).
            Geometries are reprojected to the target EPSG before hashing so that pass-1
            hashes are computed in the same coordinate space as pass-2 (steps J & K).
  Pass 2 - chunked processing: reads chunk_size features at a time, runs checks + clean,
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
import json
from pathlib import Path

import geopandas as gpd
import pyogrio
from pyproj import CRS as ProjCRS
from pyproj import Transformer
from shapely.ops import transform as shapely_transform
from rsxml import Logger, ProgressBar, dotenv

from .lib.checks import run_checks
from .lib.clean import clean_geometries
from .lib.field_map import FieldMapConfig, apply_field_map, load_and_validate_field_map
from .lib.garbage import write_garbage_chunk
from .lib.geometry_utils import _geom_type_str
from .lib.output import output_gdf_chunk
from .lib.report import write_markdown_report


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")


def _find_missing_env_vars(value: str) -> list[str]:
    """Return names of every $VAR / ${VAR} reference in *value* not set in the environment."""
    return [
        m.group(1) or m.group(2)
        for m in _ENV_VAR_RE.finditer(value)
        if (m.group(1) or m.group(2)) not in os.environ
    ]


def load_config(config_path: Path) -> dict:
    """Load a vector_prep JSON config, expand env vars, and validate.

    Raises:
        EnvironmentError: if any ``$VAR`` / ``${VAR}`` references in string
            values are not present in the environment.  The message lists
            every unresolved reference so the user can fix them all at once.
    """
    with open(config_path, encoding="utf-8") as fh:
        raw = json.load(fh)
    params: dict = raw.get("parameters", {})

    missing: list[str] = []
    for k, v in params.items():
        if isinstance(v, str):
            missing.extend(f"{k}: ${var}" for var in _find_missing_env_vars(v))

    if missing:
        bullet_list = "\n".join(f"  \u2022 {m}" for m in missing)
        raise EnvironmentError(
            f"The following environment variables are not set:\n{bullet_list}"
        )

    return {
        k: os.path.expandvars(v) if isinstance(v, str) else v for k, v in params.items()
    }


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


def _validate_sql_filter(
    input_path: str,
    layer_name: str | None,
    sql_filter: str | None,
) -> None:
    """Validate an optional SQL WHERE clause against the input layer.

    Validation uses pyogrio/GDAL directly by attempting a tiny read with
    ``max_features=1``. This catches SQL syntax errors and unknown fields
    before any long-running processing starts.
    """
    if not sql_filter:
        return

    layer_kwargs = {"layer": layer_name} if layer_name else {}
    try:
        pyogrio.read_dataframe(
            input_path,
            where=sql_filter,
            max_features=1,
            read_geometry=False,
            **layer_kwargs,
        )
    except Exception as exc:
        raise ValueError(
            f"Invalid SQL filter expression. Filter: {sql_filter}\nDriver error: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Pass-1 helper
# ---------------------------------------------------------------------------


def _build_duplicate_hash_sets(
    input_path: str,
    layer_name: str | None,
    epsg: int | None,
    sql_filter: str | None = None,
) -> tuple[int, set[str], set[str], str | None]:
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
    geom_hash_counts: dict[str, int] = {}
    row_hash_counts: dict[str, int] = {}
    geom_type_counts: dict[str, int] = {}

    # Use pyogrio.read_info() for metadata (CRS, field names).
    layer_kwargs = {"layer": layer_name} if layer_name else {}
    if sql_filter:
        layer_kwargs["where"] = sql_filter
    info = pyogrio.read_info(
        input_path, **({"layer": layer_name} if layer_name else {})
    )
    estimated_total_features = info["features"]
    # attr_cols: sorted field names (no geometry) — canonical order for row hashing.
    attr_cols = sorted(info["fields"])

    # Build a geometry transformer when a target EPSG is specified.
    transformer: Transformer | None = None
    if epsg is not None and info.get("crs") is not None:
        try:
            src_proj_crs = ProjCRS.from_user_input(info["crs"])
            dst_proj_crs = ProjCRS.from_epsg(epsg)
            transformer = Transformer.from_crs(
                src_proj_crs, dst_proj_crs, always_xy=True
            )
        except Exception:
            transformer = None

    # Chunk size for pass 1 — large enough to amortise pyogrio overhead but
    # small enough not to balloon memory.
    _P1_CHUNK = 50_000

    pbar = ProgressBar(
        max(estimated_total_features, 1), text="Pass 1: hashing features"
    )
    features_hashed = 0
    offset = 0
    while True:
        chunk = pyogrio.read_dataframe(
            input_path,
            skip_features=offset,
            max_features=_P1_CHUNK,
            **layer_kwargs,
        )
        if len(chunk) == 0:
            break

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

        offset += len(chunk)

    pbar.finish()

    # Keep only hashes that appear more than once — these are the candidates
    # that need first-occurrence tracking during pass 2.
    duplicate_geom_hashes: set[str] = {h for h, n in geom_hash_counts.items() if n > 1}
    duplicate_row_hashes: set[str] = {h for h, n in row_hash_counts.items() if n > 1}

    # Compute dominant geometry type (base type, excluding GeometryCollection).
    non_coll_counts: dict[str, int] = {}
    for t, n in geom_type_counts.items():
        if t != "GeometryCollection":
            base = t.replace("Multi", "")
            non_coll_counts[base] = non_coll_counts.get(base, 0) + n
    dominant_type: str | None = (
        max(non_coll_counts, key=non_coll_counts.__getitem__)
        if non_coll_counts
        else None
    )

    return features_hashed, duplicate_geom_hashes, duplicate_row_hashes, dominant_type


# ---------------------------------------------------------------------------
# Pass-2 helper
# ---------------------------------------------------------------------------


def _read_chunk(
    input_path: str,
    layer_name: str | None,
    offset: int,
    chunk_size: int,
    sql_filter: str | None = None,
) -> gpd.GeoDataFrame:
    """Read a slice of features starting at *offset* (0-indexed) using pyogrio."""
    layer_kwargs = {"layer": layer_name} if layer_name else {}
    if sql_filter:
        layer_kwargs["where"] = sql_filter
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
    layer_name: str | None,
    tolerance: float,
    epsg: int | None,
    output_path: str | None = None,
    garbage_path: str | None = None,
    min_size: float = None,
    min_size_drop: bool = False,
    chunk_size: int = 10_000,
    field_map_config: FieldMapConfig | None = None,
    sql_filter: str | None = None,
) -> dict:
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
        min_size_drop: If True, features below min_size threshold are dropped. If False (default), they are detected/counted only.
        chunk_size: Number of features to load at once during pass 2.
        sql_filter: Optional SQL WHERE clause to pre-filter features on read
            (e.g. ``"state_code = 'CA'"``).  Applied by pyogrio at read time so
            only matching features are loaded into memory.

    Returns:
        stats dict (suitable for print_report).
    """
    log = Logger("Vector Prep")

    # Validate that all I/O paths are absolute before doing anything else.
    _path_errors: list[str] = []
    for _label, _p in (
        ("input", str(input_dataset)),
        ("output", output_path),
        ("garbage", garbage_path),
    ):
        if _p and not Path(_p).is_absolute():
            _path_errors.append(f"  \u2022 {_label}: '{_p}' is not an absolute path")
    if _path_errors:
        raise ValueError("All I/O paths must be absolute:\n" + "\n".join(_path_errors))

    input_dataset = str(Path(input_dataset).resolve())

    if not os.path.exists(input_dataset):
        raise Exception(f"Input file does not exist: {input_dataset}")

    # Validate SQL WHERE clause early so failures are immediate and explicit.
    _validate_sql_filter(input_dataset, layer_name, sql_filter)

    # Count features in the full source layer (without SQL WHERE) so reports can
    # show both total source features and filtered subset features.
    source_feature_count = 0
    try:
        if layer_name:
            source_info = pyogrio.read_info(input_dataset, layer=layer_name)
        else:
            source_info = pyogrio.read_info(input_dataset)
        source_feature_count = int(source_info.get("features", 0) or 0)
    except Exception as info_err:
        log.warning(
            "Could not read unfiltered source feature count; report will use processed count. "
            f"Reason: {info_err}"
        )

    # ------------------------------------------------------------------
    # Delete any pre-existing output / garbage files so that re-runs
    # always start clean rather than appending to stale data.
    # ------------------------------------------------------------------
    for path_to_clean, label in ((output_path, "output"), (garbage_path, "garbage")):
        if (
            path_to_clean
            and os.path.exists(path_to_clean)
            and not os.path.isdir(path_to_clean)
        ):
            try:
                os.remove(path_to_clean)
                log.info(f"Removed existing {label} file: {path_to_clean}")
            except Exception as rm_err:
                log.warning(f"Could not remove existing {label} file: {rm_err}")

    # ------------------------------------------------------------------
    # Pass 1: scan all features to find duplicate geometry / row hashes.
    # Geometries are reprojected to *epsg* before hashing.
    # ------------------------------------------------------------------
    if sql_filter:
        log.info(f"SQL filter applied: {sql_filter}")
    log.info(f"Pass 1: scanning all features for duplicate hashes: {input_dataset}")
    total_features, duplicate_geom_hashes, duplicate_row_hashes, dominant_type = (
        _build_duplicate_hash_sets(
            input_dataset, layer_name, epsg, sql_filter=sql_filter
        )
    )
    log.info(
        f"Pass 1 complete — {total_features:,} features scanned; "
        f"{len(duplicate_geom_hashes):,} duplicate geometry hash(es), "
        f"{len(duplicate_row_hashes):,} duplicate row hash(es)."
    )
    if sql_filter:
        log.info(f"Features matching SQL filter: {total_features:,}")
    if dominant_type:
        log.info(f"Pass 1 dominant geometry type: {dominant_type}")

    if total_features == 0:
        if sql_filter:
            raise Exception(f"SQL filter matched zero features: {sql_filter}")
        raise Exception("Input dataset contains zero features. Aborting.")

    # ------------------------------------------------------------------
    # Pass 2: process features in chunks.
    # ------------------------------------------------------------------
    total_chunks = max(1, math.ceil(total_features / chunk_size))

    # Mutable sets that grow across chunks — track first-seen occurrences
    # so we know which duplicate features to keep vs. drop.
    processed_geom_hashes: set[str] = set()
    processed_row_hashes: set[str] = set()

    # Accumulator for all statistics across chunks.
    total_stats: dict = {
        "initial_count": total_features,
        "source_count": source_feature_count or total_features,
        "filtered_input_count": total_features,
        "filter_applied": bool(sql_filter),
        "input_filter": sql_filter,
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
        "below_min_size_detected": 0,
        "slivers_dropped": 0,
        # clean_geometries counters (int)
        "null_or_empty": 0,
        "invalid_fixed": 0,
        "invalid_unfixed": 0,
        "simplified_count": 0,
    }
    total_dropped_by_reason: dict[str, int] = {}
    total_dropped_by_geom_type: dict[str, int] = {}

    def _accum_drop(orig_chunk: gpd.GeoDataFrame, orig_idx: int, reason: str) -> None:
        """Helper: tally a dropped feature into the global reason/type dicts."""
        total_dropped_by_reason[reason] = total_dropped_by_reason.get(reason, 0) + 1
        geom_val = (
            orig_chunk.at[orig_idx, "geometry"]
            if orig_idx in orig_chunk.index
            else None
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
        log.info(
            f"Chunk {chunk_idx + 1}/{total_chunks} — features {feat_start:,}-{feat_end:,}"
        )

        # ---- Read chunk ----------------------------------------
        chunk_gdf = _read_chunk(
            input_dataset,
            layer_name,
            offset,
            chunk_size,
            sql_filter=sql_filter,
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
            if t not in (None, "NoneType", "None", "NaN") and t != "GeometryCollection"
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
            min_size_drop=min_size_drop,
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

        # ---- Apply field map (select, rename, cast) ---------------
        # Must run AFTER checks/clean (which need original source fields)
        # and BEFORE FID assignment and output write.
        if field_map_config is not None:
            chunk_gdf = apply_field_map(chunk_gdf, field_map_config)

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
    total_stats["garbage_written"] = garbage_written
    total_stats["dropped_by_reason"] = total_dropped_by_reason
    total_stats["dropped_by_geom_type"] = total_dropped_by_geom_type

    log.info(f"Input features:                   {total_stats['input_count']:,}")
    log.info(f"Null/empty geometries found:      {total_stats['null_or_empty']:,}")
    log.info(f"Invalid geometries fixed:         {total_stats['invalid_fixed']:,}")
    log.info(f"Invalid geometries unfixed:       {total_stats['invalid_unfixed']:,}")
    log.info(
        f"Features simplified ({tolerance} m):  {total_stats['simplified_count']:,}"
    )
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
        description=(
            "Vector Prep: Clean and simplify vector datasets.\n\n"
            "Two mutually exclusive usage modes:\n\n"
            "  Config mode:  vector_prep --config PATH [--verbose]\n"
            "                  All processing parameters are read from the JSON config file.\n\n"
            "  Direct mode:  vector_prep --input PATH [--output PATH] [--garbage PATH]\n"
            "                            [--tolerance N] [--min_size N] [--min_size_drop]\n"
            "                            [--layer NAME] [--epsg N] [--chunk_size N] [--verbose]\n"
            "                  All processing parameters are provided as command-line arguments.\n\n"
            "Mixing --config with any direct-mode argument is an error."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # ---- Always-available arguments (valid in both modes) ----
    parser.add_argument(
        "--config",
        metavar="PATH",
        help=(
            "Path to a JSON config file. When provided, all processing parameters are read "
            "from the file. Cannot be combined with any direct-mode argument (see below)."
        ),
        default=None,
    )
    parser.add_argument(
        "--verbose",
        help="Enable extra logging output (valid in both modes).",
        action="store_true",
        default=False,
    )

    # ---- Direct-mode arguments (invalid when --config is supplied) ----
    direct = parser.add_argument_group(
        "direct mode arguments",
        "The following arguments are only valid when --config is NOT provided.",
    )
    direct.add_argument(
        "--input",
        metavar="PATH",
        help="Input vector dataset (shapefile, GeoPackage, etc.).",
        default=None,
    )
    direct.add_argument(
        "--output",
        metavar="PATH",
        help="(Optional) Output GeoPackage path.",
        default=None,
    )
    direct.add_argument(
        "--garbage",
        metavar="PATH",
        help=(
            "(Optional) GeoPackage path for dropped/changed features. Must be a .gpkg file."
        ),
        default=None,
    )
    direct.add_argument(
        "--layer",
        metavar="NAME",
        help="Layer name for GeoPackage inputs. Defaults to the first layer.",
        default=None,
    )
    direct.add_argument(
        "--filter",
        metavar="WHERE",
        help=(
            "SQL WHERE clause to pre-filter features when reading the input "
            "(e.g. \"state_code = 'CA'\"). Only matching features are processed."
        ),
        default=None,
    )
    direct.add_argument(
        "--tolerance",
        type=float,
        metavar="METRES",
        help="(Optional) Simplification tolerance in metres. Default: 0 (no simplification).",
        default=None,
    )
    direct.add_argument(
        "--epsg",
        type=int,
        metavar="CODE",
        help=(
            "Cartesian CRS EPSG code for reprojection before processing. "
            "Default: 5070 (NAD83 / Conus Albers)."
        ),
        default=None,
    )
    direct.add_argument(
        "--min_size",
        type=float,
        metavar="SIZE",
        help=(
            "Minimum area (m²) for polygons or minimum length (m) for lines. "
            "Features smaller than this are flagged. Use --min_size_drop to also drop them."
        ),
        default=None,
    )
    direct.add_argument(
        "--min_size_drop",
        help=(
            "When --min_size is set, drop features below the threshold instead of just flagging them."
        ),
        action="store_true",
        default=False,
    )
    direct.add_argument(
        "--log_file",
        help=(
            "Path to a log file. When provided, logging output will be written to this file."
        ),
        default=None,
    )
    direct.add_argument(
        "--chunk_size",
        type=int,
        metavar="N",
        help=(
            "Number of features to process per chunk. Larger values use more memory "
            "but may be faster. Default: 10000."
        ),
        default=None,
    )

    args = dotenv.parse_args_env(parser)

    # ---- Enforce mutual exclusivity -------------------------------------------
    # Detect which direct-mode args were explicitly supplied (non-None / non-False).
    _direct_arg_names = [
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
    ]
    if args.config is not None:
        supplied_direct = [
            f"--{name.replace('_', '-')}"
            for name in _direct_arg_names
            if getattr(args, name) not in (None, False)
        ]
        if supplied_direct:
            parser.error(
                f"--config cannot be combined with direct-mode arguments. "
                f"Remove the following: {', '.join(supplied_direct)}"
            )
    else:
        # Direct mode: --input is required.
        if args.input is None:
            parser.error(
                "either --config PATH or --input PATH (direct mode) is required. "
                "Run with --help for usage details."
            )

    # Apply defaults for direct-mode numeric args (only needed in direct mode,
    # but harmless to apply unconditionally since config mode ignores them).
    effective_epsg: int = args.epsg if args.epsg is not None else 5070
    effective_chunk_size: int = (
        args.chunk_size if args.chunk_size is not None else 10_000
    )

    log = Logger("Vector Prep")

    # --- load config if provided -----------------------------------------------
    cfg_params: dict = {}
    cfg_path: Path | None = None
    if args.config:
        cfg_path = Path(args.config)
        if not cfg_path.exists():
            log.error(f"Config file not found: {args.config}")
            sys.exit(1)
        try:
            cfg_params = load_config(cfg_path)
        except EnvironmentError as exc:
            log.error(str(exc))
            sys.exit(1)

    # Load and validate field_map + layer_definitions from config (if specified)
    field_map_config = None
    raw_field_map = cfg_params.get("field_map")
    layer_defs_rel = cfg_params.get("layer_definitions")

    if raw_field_map is not None:
        if layer_defs_rel is None:
            log.error(
                "Config specifies 'field_map' but 'layer_definitions' is missing."
            )
            sys.exit(1)
        if cfg_path is None:
            log.error("Internal error: cfg_path not set when field_map is present")
            sys.exit(1)
        layer_defs_path = (cfg_path.parent / layer_defs_rel).resolve()
        try:
            field_map_config = load_and_validate_field_map(
                raw_field_map, layer_defs_path
            )
            log.info(f"Loaded field map: {len(raw_field_map)} field(s) mapped")
        except (ValueError, FileNotFoundError) as e:
            log.error(f"Field map error: {e}")
            sys.exit(1)
    elif layer_defs_rel is not None:
        log.warning(
            "layer_definitions specified but no field_map; all fields will be passed through."
        )

    # Resolve effective parameter values:
    # In config mode, CLI direct-mode args are all None/False (enforced above),
    # so cfg_params is the only source. In direct mode, cfg_params is empty.
    tolerance = (
        float(args.tolerance)
        if args.tolerance is not None
        else float(cfg_params.get("tolerance", 0.0))
    )
    min_size_val = (
        float(args.min_size)
        if args.min_size is not None
        else (
            float(cfg_params.get("min_size"))
            if cfg_params.get("min_size") is not None
            else None
        )
    )
    min_size_drop_val = args.min_size_drop or bool(
        cfg_params.get("min_size_drop", False)
    )

    input_path = args.input or cfg_params.get("input") or None
    output_path_val = args.output or cfg_params.get("output") or None
    garbage_path_val = args.garbage or cfg_params.get("garbage") or None

    # layer / epsg / chunk_size: CLI wins; fall back to config; then hardcoded defaults.
    effective_layer = args.layer or cfg_params.get("layer") or None
    effective_filter = args.filter or cfg_params.get("filter") or None
    if args.epsg is not None:
        effective_epsg = args.epsg
    elif cfg_params.get("epsg") is not None:
        effective_epsg = int(cfg_params["epsg"])
    # else: already set to 5070 above

    if args.chunk_size is not None:
        effective_chunk_size = args.chunk_size
    elif cfg_params.get("chunk_size") is not None:
        effective_chunk_size = int(cfg_params["chunk_size"])
    # else: already set to 10_000 above

    if input_path is None:
        log.error(
            "No input path provided. Use --input or set 'input' in the config file."
        )
        sys.exit(1)

    # verbose can also come from config (CLI --verbose takes precedence)
    effective_verbose = args.verbose or bool(cfg_params.get("verbose", False))

    # Log file setup
    log_path = None
    if args.log_file:
        log_path = args.log_file
    elif cfg_params.get("log_file"):
        log_path = cfg_params["log_file"]
    else:
        if output_path_val:
            base_name = os.path.splitext(os.path.basename(output_path_val))[0]
            log_file_name = f"{base_name}_vector_prep.log"
            log_dir = os.path.dirname(output_path_val)
        else:
            log_file_name = "vector_prep.log"
            log_dir = "."
        log_path = os.path.join(log_dir, log_file_name)
    log.setup(
        log_path=log_path,
        verbose=effective_verbose,
    )

    try:
        stats = vector_prep(
            input_path,
            effective_layer,
            tolerance,
            effective_epsg,
            output_path=output_path_val,
            garbage_path=garbage_path_val,
            min_size=min_size_val,
            min_size_drop=min_size_drop_val,
            chunk_size=effective_chunk_size,
            field_map_config=field_map_config,
            sql_filter=effective_filter,
        )
        if not output_path_val:
            log.info("No --output path provided; skipping output write.")
        if output_path_val:
            report_stem = Path(output_path_val).stem
            report_dir = Path(output_path_val).parent
        else:
            report_stem = "vector_prep"
            report_dir = Path(".")
        report_path = report_dir / f"{report_stem}_report.md"
        written_report = write_markdown_report(stats, garbage_path_val, report_path)
        log.info(f"Markdown report written: {written_report}")
    except Exception as e:
        log.error("Vector prep failed: %s", e)
        log.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
