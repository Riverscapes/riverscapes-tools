"""clean_geometries: fix invalid geometries and optionally simplify a GeoSeries."""
from __future__ import annotations

from typing import Dict, List, Tuple

import geopandas as gpd
from rsxml import Logger

from .geometry_utils import safe_make_valid


def clean_geometries(
    gseries: gpd.GeoSeries,
    simplify_tolerance: float,
) -> Tuple[gpd.GeoSeries, Dict, List[Tuple[int, str, str]]]:
    """Process a GeoSeries: drop null/empty, fix invalid, optionally simplify.

    Returns:
        cleaned GeoSeries,
        diagnostics dict,
        list of (original_index, operation, reason) for every geometry set to None.
        ``operation`` is always ``"DROPPED"``; reason is one of ``"null"``,
        ``"empty"``, or ``"invalid_unfixed"``.
    """
    cleaned = []
    dropped: List[Tuple[int, str, str]] = []  # (original_index, operation, reason)
    stats: Dict = {
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
            dropped.append((orig_idx, "DROPPED", "null"))
            continue

        try:
            if geom.is_empty:
                stats["null_or_empty"] += 1
                cleaned.append(None)
                dropped.append((orig_idx, "DROPPED", "empty"))
                continue
        except Exception:
            pass

        # Check validity
        try:
            is_valid = geom.is_valid
        except Exception:
            is_valid = False

        if not is_valid:
            fixed = safe_make_valid(geom)
            if fixed is not None and not fixed.is_empty:
                geom = fixed
                stats["invalid_fixed"] += 1
            else:
                stats["invalid_unfixed"] += 1
                cleaned.append(None)
                dropped.append((orig_idx, "DROPPED", "invalid_unfixed"))
                continue

        # Simplify if tolerance > 0
        if simplify_tolerance is not None and simplify_tolerance > 0:
            try:
                simplified = geom.simplify(simplify_tolerance, preserve_topology=True)
                if simplified is not None and not simplified.is_empty:
                    if not simplified.is_valid:
                        simplified = safe_make_valid(simplified)
                    geom = simplified
                    if geom is None:
                        stats["invalid_unfixed"] += 1
                        cleaned.append(None)
                        dropped.append((orig_idx, "DROPPED", "invalid_unfixed"))
                        continue
                    stats["simplified_count"] += 1
                elif simplified is not None and simplified.is_empty:
                    stats["invalid_unfixed"] += 1
                    cleaned.append(None)
                    dropped.append((orig_idx, "DROPPED", "invalid_unfixed"))
                    continue
            except Exception as e:
                _log = Logger("Error")
                _log.debug(f"simplify failed on feature {orig_idx}: {e}")

        cleaned.append(geom)

    return gpd.GeoSeries(cleaned, index=gseries.index, crs=gseries.crs), stats, dropped
