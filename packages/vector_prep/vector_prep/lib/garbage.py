"""Build and write the garbage GeoDataFrame of dropped/changed features."""
from __future__ import annotations

from typing import List, Tuple

import geopandas as gpd
import pandas as pd
from shapely.geometry.base import BaseGeometry
from rsxml import Logger


def build_garbage_gdf(
    original_gdf_proc: gpd.GeoDataFrame,
    dropped_list: List[Tuple[int, str, str]],
    changed_list: List[Tuple[int, str, BaseGeometry]],
    clean_dropped: List[Tuple[int, str, str]],
) -> gpd.GeoDataFrame | None:
    """Build a GeoDataFrame of dropped/changed features for inspection.

    Args:
        original_gdf_proc: Snapshot of the GeoDataFrame before any modifications.
        dropped_list: List of (idx, "DROPPED", reason) from run_checks.
        changed_list: List of (idx, reason, new_geom) from run_checks (after deduplication).
        clean_dropped: List of (idx, "DROPPED", reason) from clean_geometries.

    Returns:
        A GeoDataFrame combining all garbage entries, or None if there are none.
    """
    garbage_parts = []

    if dropped_list:
        drop_idx_list = [idx for idx, _, _ in dropped_list]
        garbage_dropped = original_gdf_proc.loc[drop_idx_list].copy()
        garbage_dropped["VP_Operation"] = [op for _, op, _ in dropped_list]
        garbage_dropped["VP_Reason"] = [r for _, _, r in dropped_list]
        garbage_parts.append(garbage_dropped)

    if changed_list:
        changed_idx_list = [idx for idx, _, _ in changed_list]
        garbage_changed = original_gdf_proc.loc[changed_idx_list].copy()
        garbage_changed["VP_Operation"] = "CHANGED"
        garbage_changed["VP_Reason"] = [r for _, r, _ in changed_list]
        garbage_parts.append(garbage_changed)

    if clean_dropped:
        valid_clean = [
            (idx, op, r)
            for idx, op, r in clean_dropped
            if idx in original_gdf_proc.index
        ]
        if valid_clean:
            clean_idx_list = [idx for idx, _, _ in valid_clean]
            garbage_clean = original_gdf_proc.loc[clean_idx_list].copy()
            garbage_clean["VP_Operation"] = [op for _, op, _ in valid_clean]
            garbage_clean["VP_Reason"] = [r for _, _, r in valid_clean]
            garbage_parts.append(garbage_clean)

    if not garbage_parts:
        return None

    combined_df = pd.concat(garbage_parts, ignore_index=False)
    return gpd.GeoDataFrame(combined_df, crs=original_gdf_proc.crs)


def write_garbage_chunk(
    original_chunk: gpd.GeoDataFrame,
    dropped_list: List[Tuple[int, str, str]],
    changed_list: List[Tuple[int, str, BaseGeometry]],
    clean_dropped: List[Tuple[int, str, str]],
    garbage_path: str,
    first_chunk: bool,
) -> int:
    """Build and write garbage features for one chunk incrementally.

    Args:
        original_chunk: Snapshot of the chunk GeoDataFrame *before* any
            modifications (used as the geometry/attribute source).
        dropped_list: (idx, "DROPPED", reason) entries from run_checks.
        changed_list: (idx, reason, new_geom) entries from run_checks (deduped).
        clean_dropped: (idx, "DROPPED", reason) entries from clean_geometries.
        garbage_path: Path to the output GeoPackage for garbage features.
        first_chunk: When True the file is overwritten; when False the chunk is
            appended to the existing file.

    Returns:
        Number of garbage features written (0 if nothing to write).
    """
    log = Logger("Vector Prep")

    garbage_gdf = build_garbage_gdf(original_chunk, dropped_list, changed_list, clean_dropped)
    if garbage_gdf is None or len(garbage_gdf) == 0:
        return 0

    count = len(garbage_gdf)

    garbage_out = garbage_gdf.copy()
    has_valid_geom = garbage_out["geometry"].notna().any()
    if has_valid_geom:
        try:
            garbage_out = garbage_out.to_crs(epsg=4326)
        except Exception as reproject_err:
            log.warning(f"Could not reproject garbage chunk to EPSG:4326: {reproject_err}")

    write_mode = "w" if first_chunk else "a"
    try:
        garbage_out.to_file(garbage_path, driver="GPKG", layer="garbage", mode=write_mode)
        log.debug(f"Garbage chunk written ({count} features, mode={write_mode})")
    except Exception as write_err:
        log.warning(f"GeoPackage write failed for garbage chunk: {write_err}")

    return count


def write_garbage(
    garbage_gdf: gpd.GeoDataFrame,
    garbage_path: str,
    crs,
) -> None:
    """Write garbage GeoDataFrame to a GeoPackage (CSV fallback on failure).

    Args:
        garbage_gdf: GeoDataFrame of dropped/changed features.
        garbage_path: Path to write the GeoPackage.
        crs: CRS of the source data (used before reprojection to EPSG:4326).
    """
    log = Logger("Vector Prep")
    log.info(f"Writing {len(garbage_gdf)} garbage features to: {garbage_path}")
    try:
        garbage_out = garbage_gdf.copy()
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
            log.warning(
                f"GeoPackage write failed for garbage (trying CSV fallback): {write_err}"
            )
            csv_path = str(garbage_path).replace(".gpkg", "_garbage.csv")
            df_fallback = pd.DataFrame(garbage_out.drop(columns="geometry", errors="ignore"))
            df_fallback.to_csv(csv_path, index=False)
            log.info(
                f"Garbage CSV fallback written: {csv_path} ({len(df_fallback)} rows)"
            )
    except Exception as e:
        log.warning(f"Failed to write garbage file: {e}")
