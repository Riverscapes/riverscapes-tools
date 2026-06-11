"""
Clean a vector layer by fixing invalid geometries and simplifying it. Ostensibly used for
preparing vector layers for use in the Riverscapes Reporting platform both as a picklist
layer, but also for storing in Athena for use in reports.

The input is a single ShapeFile for GeoPackage vector layer. It can be in any projection,
and any fields.

The output is always a GeoPackage layer with cleaned geometries, reprojected to EPSG:4326.

Philip Bailey
27 Nov 2025
"""
import argparse
import sys
import os
import traceback
from pathlib import Path
from typing import Dict, List, Tuple

import geopandas as gpd
import pandas as pd
from shapely.geometry.base import BaseGeometry
from shapely import make_valid  # shapely >=1.8
from rsxml import Logger, dotenv

# This script always produces the output in GeoPackage format
OUTPUT_DRIVER = "GPKG"


def vector_prep(
    input_dataset: Path | str,
    layer_name: str | None,
    tolerance: float,
    epsg: int | None,
    garbage_path: str | None = None,
) -> Tuple[gpd.GeoDataFrame, Dict]:
    """ Vector Prep

    Args:
        input_dataset (Path | str): _description_
        layer_name (str | None): _description_
        tolerance (float): _description_
        epsg (int | None): _description_
        garbage_path (str | None): Optional path to write dropped features as a GeoPackage.

    Raises:
        Exception: _description_
        Exception: _description_
        Exception: _description_
        Exception: _description_

    Returns:
        Tuple[gpd.GeoDataFrame, Dict]: cleaned GeoDataFrame and stats dict
    """
    log = Logger("Vector Prep")
    input_dataset = Path(input_dataset)
    if not input_dataset.exists():
        raise Exception(f"Input file does not exist: {input_dataset}")

    log.info(f"Reading input dataset with GeoPandas: {input_dataset}")
    try:
        if layer_name:
            gdf = gpd.read_file(input_dataset, layer=layer_name)
        else:
            log.debug("No layer name specified, geopandas will choose default/first layer")
            gdf = gpd.read_file(input_dataset)  # geopandas will choose default/first layer
    except Exception as e:
        raise Exception(f"GeoPandas failed to read input dataset: {e}") from e

    initial_count = len(gdf)
    log.info(f"Loaded {initial_count} features. CRS: {gdf.crs}")

    geom_types = gdf.geom_type.value_counts().to_dict()
    log.info(f"Geometry type of input: {geom_types}")

    # Drop features with null geometry right away (to reduce work)
    # We'll also handle empty/invalid ones in our cleaning step
    # Create a copy to preserve original until final write
    gdf_proc = gdf.copy()
    # Reset index so integer labels are unique and contiguous; avoids .loc
    # returning unexpected extra rows when the source has duplicate index labels.
    gdf_proc = gdf_proc.reset_index(drop=True)

    # Reproject to specified Cartesian CRS for processing
    if epsg:
        log.info(f"Reprojecting to EPSG:{epsg} for processing...")
        try:
            gdf_proc = gdf_proc.to_crs(epsg=epsg)
            log.info(f"Reprojection complete. New CRS: {gdf_proc.crs}")
        except Exception as e:
            raise Exception(f"Failed to reproject to EPSG:{epsg}: {e}") from e

    # Clean geometries (fix invalids, simplify)
    if tolerance and tolerance > 0:
        log.info(f"Fixing invalid geometries and simplifying to {tolerance} m tolerance...")
    else:
        log.info("Fixing invalid geometries (no simplification)...")
    cleaned_geom_series, stats, dropped_list = clean_geometries(gdf_proc.geometry, simplify_tolerance=tolerance)

    # ------------------------------------------------------------------
    # Build garbage GeoDataFrame from the dropped_list BEFORE we overwrite
    # the geometry column so we can read original geometry types.
    # ------------------------------------------------------------------

    dropped_by_reason: Dict[str, int] = {}
    dropped_by_geom_type: Dict[str, int] = {}
    garbage_gdf: gpd.GeoDataFrame | None = None

    if dropped_list:
        dropped_indices = [idx for idx, _ in dropped_list]
        reason_by_idx: Dict = {idx: reason for idx, reason in dropped_list}

        # Look up rows in gdf_proc (original geometries still intact here).
        # Because we reset_index above, .loc is safe against duplicate labels.
        garbage_rows = gdf_proc.loc[dropped_indices].copy()
        garbage_rows["vp_drop_reason"] = [reason_by_idx[idx] for idx in dropped_indices]
        garbage_rows["vp_original_geom_type"] = [
            _geom_type_str(gdf_proc.loc[idx, "geometry"]) for idx in dropped_indices
        ]
        garbage_gdf = garbage_rows

        # Accumulate stats
        for idx, reason in dropped_list:
            dropped_by_reason[reason] = dropped_by_reason.get(reason, 0) + 1
            gt = _geom_type_str(gdf_proc.loc[idx, "geometry"])
            dropped_by_geom_type[gt] = dropped_by_geom_type.get(gt, 0) + 1

    # assign cleaned geometries back
    gdf_proc["geometry"] = cleaned_geom_series

    # Drop rows where geometry is None or empty after cleaning
    before_drop = len(gdf_proc)
    gdf_proc = gdf_proc[~gdf_proc["geometry"].isna()]
    gdf_proc = gdf_proc[~gdf_proc["geometry"].is_empty]
    after_drop = len(gdf_proc)
    dropped = before_drop - after_drop

    log.info(f"Input features:                   {stats['input_count']:,}")
    log.info(f"Null/empty geometries found:      {stats['null_or_empty']:,}")
    log.info(f"Invalid geometries fixed:         {stats['invalid_fixed']:,}")
    log.info(f"Invalid geometries unfixed (dropped): {stats['invalid_unfixed']:,}")
    log.info(f"Features simplified ({tolerance} m):  {stats['simplified_count']:,}")
    log.info(f"Dropped features after cleaning:  {dropped:,}")
    log.info(f"Remaining features:               {len(gdf_proc):,}")

    # Enrich stats with new fields
    stats["output_count"] = len(gdf_proc)
    stats["dropped_by_reason"] = dropped_by_reason
    stats["dropped_by_geom_type"] = dropped_by_geom_type

    # ------------------------------------------------------------------
    # Write garbage GeoPackage if requested
    # ------------------------------------------------------------------
    if garbage_path and garbage_gdf is not None and len(garbage_gdf) > 0:
        log.info(f"Writing {len(garbage_gdf)} dropped features to garbage file: {garbage_path}")
        try:
            # Reproject garbage to EPSG:4326 for consistency
            garbage_out = garbage_gdf.copy()
            # Only reproject if there are non-null geometries
            has_valid_geom = garbage_out["geometry"].notna().any()
            if has_valid_geom:
                try:
                    garbage_out = garbage_out.to_crs(epsg=4326)
                except Exception as reproject_err:
                    log.warning(f"Could not reproject garbage to EPSG:4326: {reproject_err}")

            try:
                garbage_out.to_file(garbage_path, driver="GPKG", layer="garbage")
                log.info(f"Garbage file written: {garbage_path} ({len(garbage_out)} features)")
            except Exception as write_err:
                log.warning(f"GeoPackage write failed for garbage (trying CSV fallback): {write_err}")
                csv_path = str(garbage_path).replace(".gpkg", "_garbage.csv")
                df_fallback = pd.DataFrame(garbage_out.drop(columns="geometry", errors="ignore"))
                df_fallback.to_csv(csv_path, index=False)
                log.info(f"Garbage CSV fallback written: {csv_path} ({len(df_fallback)} rows)")
        except Exception as e:
            log.warning(f"Failed to write garbage file: {e}")
    elif garbage_path and (garbage_gdf is None or len(garbage_gdf) == 0):
        log.info("No dropped features — garbage file not written.")

    # If nothing remains
    if len(gdf_proc) == 0:
        raise Exception("No valid geometries remain after cleaning. Aborting write.")

    # Loop over all string columns and ensure that empty strings are set to None (to avoid issues with some drivers)
    for col in gdf_proc.select_dtypes(include=['object']).columns:
        gdf_proc[col] = gdf_proc[col].apply(lambda x: x if x and str(x).strip() != "" else None)

    # Make sure there is a column called FID (some drivers require it)
    if "FID" not in gdf_proc.columns:
        gdf_proc = gdf_proc.reset_index(drop=True)
        gdf_proc["FID"] = gdf_proc.index.astype('int64')

    return gdf_proc, stats


def output_gdf(gdf: gpd.GeoDataFrame, output_dataset: str, layer_name: str | None):
    """Save the gpd to file (geopackage layer) in EPSG 4326"""
    # Reproject to EPSG for final output
    log = Logger("Output GDF")

    # If output exists and overwrite requested, remove it first (be careful with gpkg)
    if os.path.exists(output_dataset):
        try:
            if os.path.isdir(output_dataset):
                # shapefile's folder? be cautious
                pass
            os.remove(output_dataset)
            log.info(f"Overwrote existing file: {output_dataset}")
        except Exception:
            # for gpkg, removal may be different; try to proceed and fiona may overwrite if allowed
            log.debug("Could not remove existing file prior to write (continuing)...")

    log.info("Reprojecting to EPSG 4326 for output")
    gdf = gdf.to_crs(epsg=4326)

    # Write output
    log.info(f"Writing cleaned layer to {output_dataset} (driver={OUTPUT_DRIVER})...")
    try:
        # For GeoPackage, preserve layer name if provided or derive from filename
        write_kwargs = {}
        if OUTPUT_DRIVER == "GPKG":
            # geopandas.to_file will write a layer named after filename (without ext) by default unless layer arg given
            layername = layer_name if layer_name else os.path.splitext(os.path.basename(output_dataset))[0]
            write_kwargs["layer"] = layername

        gdf.to_file(output_dataset, driver=OUTPUT_DRIVER, **write_kwargs)
        log.info("Write complete.")
    except Exception as e:
        raise Exception(f"Failed to write output: {e}") from e


def _geom_type_str(geom) -> str:
    """Return the geometry type string, or 'Unknown' for None / errors."""
    if geom is None:
        return "Unknown"
    try:
        t = geom.geom_type
        return t if t else "Unknown"
    except Exception:
        return "Unknown"


def safe_make_valid(geom: BaseGeometry):
    """Try make_valid then fallback to buffer(0), or return None if can't fix."""

    if geom is None:
        return None
    try:
        return make_valid(geom)
    except Exception as e:
        log = Logger("Error")
        log.debug(f"make_valid/buffer(0) failed: {e}")
        try:
            return geom.buffer(0)
        except Exception as e2:
            log.debug(f"fallback buffer(0) failed too: {e2}")
            return None


def clean_geometries(
    gseries: gpd.GeoSeries,
    simplify_tolerance: float,
) -> Tuple[gpd.GeoSeries, Dict, List[Tuple]]:
    """
    Process a GeoSeries of geometries:
      - drop null/empty
      - attempt to fix invalid/self-intersecting geometries
      - apply topology-preserving simplify (Shapely's simplify with preserve_topology=True)

    Returns:
        cleaned GeoSeries,
        diagnostics dict,
        list of (original_index, reason) tuples for every geometry set to None (dropped).
        Reason is one of: "null", "empty", "invalid_unfixed".
    """
    cleaned = []
    dropped: List[Tuple] = []  # (original_index, reason)
    stats = {
        "input_count": len(gseries),
        "null_or_empty": 0,
        "invalid_fixed": 0,
        "invalid_unfixed": 0,
        "simplified_count": 0,
    }

    for orig_idx, geom in zip(gseries.index, gseries):
        if geom is None:
            stats["null_or_empty"] += 1
            cleaned.append(None)
            dropped.append((orig_idx, "null"))
            continue
        # some drivers give empty geometries instead of None
        try:
            if geom.is_empty:
                stats["null_or_empty"] += 1
                cleaned.append(None)
                dropped.append((orig_idx, "empty"))
                continue
        except Exception:
            # if .is_empty fails, we'll try to continue
            pass

        # If geometry invalid, try to fix
        try:
            is_valid = geom.is_valid
        except Exception:
            # some malformed geometries might raise; attempt fix
            is_valid = False

        if not is_valid:
            fixed = safe_make_valid(geom)
            if fixed is not None and not fixed.is_empty:
                geom = fixed
                stats["invalid_fixed"] += 1
            else:
                stats["invalid_unfixed"] += 1
                # keep as-is (or set to None) - we'll mark as None to drop later
                cleaned.append(None)
                dropped.append((orig_idx, "invalid_unfixed"))
                continue

        # If simplify tolerance > 0, simplify while trying to preserve topology
        if simplify_tolerance is not None and simplify_tolerance > 0:
            try:
                simplified = geom.simplify(simplify_tolerance, preserve_topology=True)
                # ensure simplification didn't produce empty / invalid geometry
                if simplified is not None and not simplified.is_empty:
                    # if simplification creates invalid geometry, try to make valid again
                    if not simplified.is_valid:
                        simplified = safe_make_valid(simplified)
                    geom = simplified
                    if geom is None:
                        stats["invalid_unfixed"] += 1
                        cleaned.append(None)
                        dropped.append((orig_idx, "invalid_unfixed"))
                        continue
                    stats["simplified_count"] += 1
                elif simplified is not None and simplified.is_empty:
                    stats["invalid_unfixed"] += 1
                    cleaned.append(None)
                    dropped.append((orig_idx, "invalid_unfixed"))
                    continue
            except Exception as e:
                log = Logger("Error")
                log.debug(f"simplify failed on feature {orig_idx}: {e}")
                # keep original geom (already valid)
        cleaned.append(geom)

    return gpd.GeoSeries(cleaned, index=gseries.index, crs=gseries.crs), stats, dropped


def print_report(stats: Dict, garbage_path: str | None) -> None:
    """Log a formatted summary report of the vector prep run."""
    log = Logger("Vector Prep Report")

    dropped_by_reason: Dict[str, int] = stats.get("dropped_by_reason", {})
    dropped_by_geom_type: Dict[str, int] = stats.get("dropped_by_geom_type", {})
    null_empty_dropped = dropped_by_reason.get("null", 0) + dropped_by_reason.get("empty", 0)
    invalid_unfixed_dropped = dropped_by_reason.get("invalid_unfixed", 0)
    total_dropped = sum(dropped_by_reason.values())

    lines = [
        "=== Vector Prep Report ===",
        f"Input features:           {stats.get('input_count', 0):>10,}",
        f"Null/empty on input:      {stats.get('null_or_empty', 0):>10,}",
        f"Invalid geometries fixed: {stats.get('invalid_fixed', 0):>10,}",
        f"Invalid geometries unfixed: {stats.get('invalid_unfixed', 0):>10,}",
        f"Features simplified:      {stats.get('simplified_count', 0):>10,}",
        f"Features dropped (total): {total_dropped:>10,}",
        f"  - null/empty:           {null_empty_dropped:>10,}",
        f"  - invalid_unfixed:      {invalid_unfixed_dropped:>10,}",
        "Dropped geometry types:",
    ]

    if dropped_by_geom_type:
        for geom_type, count in sorted(dropped_by_geom_type.items(), key=lambda x: -x[1]):
            lines.append(f"  - {geom_type:<20} {count:>10,}")
    else:
        lines.append("  (none)")

    lines.append(f"Output features:          {stats.get('output_count', 0):>10,}")
    lines.append(f"Garbage written to: {garbage_path if garbage_path else 'N/A'}")
    lines.append("==========================")

    for line in lines:
        log.info(line)


def main():
    parser = argparse.ArgumentParser(description='Vector Prep: Clean and simplify vector datasets.')
    parser.add_argument("input", help="Input vector (shapefile, gpkg, etc.)")
    parser.add_argument("--output", help="(OPTIONAL) Output vector path")
    parser.add_argument("--garbage", help="(OPTIONAL) If provided, bad geometries will be saved to this GeoPackage for inspection in addition to being dropped from the output. Must be a .gpkg file.", default=None)
    parser.add_argument("--layer", help="Layer name (for geopackage). If not provided and input is geopackage, first layer is used.", default=None)
    parser.add_argument("--tolerance", type=float, help="Simplify tolerance in METRES (0 to skip).", default=0.0)
    parser.add_argument("--epsg", type=int, help="Cartesian CRS EPSG code to reproject to before processing (optional). Default is 5070 (NAD83 / Conus Albers).", default=5070)
    parser.add_argument('--verbose', help='(optional) a little extra logging', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    log = Logger("Vector Prep")
    log.setup(log_path=os.path.join(os.path.dirname(args.output), "vector_prep.log"), verbose=args.verbose)

    try:
        prepped_gdf, stats = vector_prep(args.input, args.layer, float(args.tolerance), int(args.epsg), garbage_path=args.garbage)
        output_gdf(prepped_gdf, args.output, args.layer)
        print_report(stats, args.garbage)
    except Exception as e:
        log.error("Vector prep failed: %s", e)
        log.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
