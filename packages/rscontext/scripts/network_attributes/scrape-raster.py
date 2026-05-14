"""
Network Attribute Scraping Script for Raster Data
This script calculates raster-based attributes for NHD catchments and writes results to parquet files. It is designed to be run once per raster theme and HUC2 region, and can be parallelized across multiple HUC2s.
Only pixels whose centroid falls inside a catchment polygon contribute to its statistics, preventing double-counting at VPU boundaries.  The raster is never reprojected; catchment polygons are reprojected to the raster CRS when the two differ. Two modes are supported:

- Float mode (default): calculates count, sum, min, and max of pixel values for each catchment, ignoring nodata pixels.
- Integer mode (--integer flag): counts how many pixels of each unique integer value fall inside each catchment, ignoring nodata pixels. Results are written as a parquet file with columns: HUC2, NHDPlusID, value_counts (map type where keys are unique pixel values and values are the corresponding counts).

The output file is organized in a subdirectory named after the theme (e.g. "landfire") under the specified output directory, with a filename pattern of {theme}_huc_{huc2}.parquet. 


Philip Bailey
May 2026
"""

import argparse
import os
import sys
import traceback

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from rasterio.windows import from_bounds
from rsxml import Logger, dotenv

# Centroid-in-polygon check: use shapely 2.x vectorised API where available,
# fall back to the shapely 1.x vectorized module.
try:
    import shapely as _shapely_module

    def _centroids_in_polygon(polygon, x_flat: np.ndarray, y_flat: np.ndarray) -> np.ndarray:
        return _shapely_module.contains_xy(polygon, x_flat, y_flat)

except AttributeError:
    from shapely.vectorized import contains as _shapely_contains

    def _centroids_in_polygon(polygon, x_flat: np.ndarray, y_flat: np.ndarray) -> np.ndarray:
        return _shapely_contains(polygon, x_flat, y_flat)


def get_huc4s_for_huc2(nhd_gdb: str, huc2: str, log) -> list:
    """Return a sorted list of unique 4-digit VPUID prefixes (HUC4s) within a HUC2."""
    log.info(f'Querying HUC4 codes within HUC2: {huc2}')
    gdf = gpd.read_file(
        nhd_gdb,
        layer='NHDPlusCatchment',
        where=f"SUBSTR(VPUID, 1, 2) = '{huc2}'",
        ignore_geometry=True,
    )
    vpuid_col = next((c for c in gdf.columns if c.lower() == 'vpuid'), None)
    if vpuid_col is None:
        raise RuntimeError('VPUID column not found in NHDPlusCatchment')
    huc4s = sorted(set(str(v)[:4] for v in gdf[vpuid_col].dropna()))
    log.info(f'Found {len(huc4s)} HUC4(s) in HUC2 {huc2}: {", ".join(huc4s)}')
    return huc4s


def scrape_float_raster(huc4: str, theme: str, nhd_gdb: str, raster_path: str, output_dir: str, skip_values: set = None) -> None:
    """Calculate per-catchment raster statistics for a HUC4 region and write results to parquet.

    Only pixels whose centroid falls inside a catchment polygon contribute to
    its statistics, preventing double-counting at VPU boundaries.  The raster
    is never reprojected; catchment polygons are reprojected to the raster CRS
    when the two differ.

    Args:
        huc4:        Four-digit HUC4 code used to filter NHDPlusCatchment by matching
                     the first four characters of the VPUID field.
        theme:       Theme name used as both folder name and prefix for output files.
        nhd_gdb:     Path to the NHD ESRI File Geodatabase.
        raster_path: Path to the source raster.
        output_dir:  Directory where the output parquet file will be written.
    """
    log = Logger(f"Scrape {theme} HUC4 {huc4}")
    huc4 = huc4.zfill(4)

    output_path = os.path.join(output_dir, theme, f'{theme}_huc_{huc4}.parquet')
    if os.path.exists(output_path):
        log.info(f'Output already exists, skipping: {output_path}')
        return

    # ------------------------------------------------------------------
    # 1. Read raster CRS and nodata value
    # ------------------------------------------------------------------
    with rasterio.open(raster_path) as src:
        raster_crs = src.crs
        raster_nodata = src.nodata

    log.info(f'Raster CRS:    {raster_crs.to_string()}')
    log.info(f'Raster NoData: {raster_nodata}')

    # ------------------------------------------------------------------
    # 2. Load NHDPlusCatchment polygons filtered to the requested HUC4
    # ------------------------------------------------------------------
    log.info(f'Loading NHDPlusCatchment for HUC4: {huc4}')
    try:
        gdf = gpd.read_file(nhd_gdb, layer='NHDPlusCatchment', where=f"SUBSTR(VPUID, 1, 4) = '{huc4}'")
    except Exception as exc:
        raise RuntimeError(f'Failed to read NHDPlusCatchment from {nhd_gdb}: {exc}') from exc

    log.info(f'Catchment CRS: {gdf.crs.to_string()}')
    log.info(f'Loaded {len(gdf):,} catchments for HUC4: {huc4}')

    if len(gdf) == 0:
        log.warning(f'No catchments found for HUC4: {huc4}')
        return

    # ------------------------------------------------------------------
    # 3. Reproject polygons to raster CRS if necessary (raster is never
    #    reprojected)
    # ------------------------------------------------------------------
    if gdf.crs != raster_crs:
        log.info(f'Reprojecting catchments from {gdf.crs.to_string()} to {raster_crs.to_string()}')
        gdf = gdf.to_crs(raster_crs)
    else:
        log.info('Catchments and raster share the same CRS — no reprojection needed')

    # ------------------------------------------------------------------
    # 4. Loop over catchments and compute centroid-based statistics
    # ------------------------------------------------------------------
    results = []
    errors = 0
    skipped = 0

    # Resolve the NHDPlusID column name (GDB field names can vary in case)
    id_col = next((c for c in gdf.columns if c.lower() == 'nhdplusid'), None)

    with rasterio.open(raster_path) as src:
        raster_bounds = src.bounds

        for idx, row in gdf.iterrows():
            nhdplus_id = row[id_col] if id_col else idx
            polygon = row.geometry

            # --- Geometry validation ---
            if polygon is None or polygon.is_empty:
                log.warning(f'NHDPlusID {nhdplus_id}: empty geometry, skipping')
                skipped += 1
                continue

            if not polygon.is_valid:
                log.warning(f'NHDPlusID {nhdplus_id}: invalid geometry, skipping')
                skipped += 1
                continue

            try:
                minx, miny, maxx, maxy = polygon.bounds

                # Clamp polygon extent to raster bounds
                cminx = max(minx, raster_bounds.left)
                cminy = max(miny, raster_bounds.bottom)
                cmaxx = min(maxx, raster_bounds.right)
                cmaxy = min(maxy, raster_bounds.top)

                if cminx >= cmaxx or cminy >= cmaxy:
                    log.debug(f'NHDPlusID {nhdplus_id}: polygon outside raster extent, skipping')
                    skipped += 1
                    continue

                # Build a rasterio window aligned to the raster grid
                window = from_bounds(cminx, cminy, cmaxx, cmaxy, src.transform)
                window = window.round_offsets(pixel_precision=0).round_lengths(pixel_precision=0)

                if window.width <= 0 or window.height <= 0:
                    log.debug(f'NHDPlusID {nhdplus_id}: zero-size window, skipping')
                    skipped += 1
                    continue

                # Read the windowed data; masked=True applies the nodata mask
                data = src.read(1, window=window, masked=True)
                win_transform = src.window_transform(window)

                # Generate centroid (x, y) coordinates for every pixel in the window
                nrows, ncols = data.shape
                col_idx, row_idx = np.meshgrid(np.arange(ncols), np.arange(nrows))
                x_centroids = win_transform.c + (col_idx + 0.5) * win_transform.a
                y_centroids = win_transform.f + (row_idx + 0.5) * win_transform.e

                # Keep only pixels whose centroid is strictly inside the polygon
                inside = _centroids_in_polygon(
                    polygon,
                    x_centroids.ravel(),
                    y_centroids.ravel(),
                ).reshape(nrows, ncols)

                # Combine the raster nodata mask with the centroid mask
                existing_mask = np.ma.getmaskarray(data)
                combined_mask = np.logical_or(existing_mask, ~inside)

                valid_data = np.ma.array(data.data, mask=combined_mask)
                valid_data = np.ma.masked_invalid(valid_data)

                if skip_values:
                    for sv in skip_values:
                        valid_data = np.ma.masked_where(valid_data == sv, valid_data)

                count = int(valid_data.count())

                results.append({
                    'NHDPlusID': nhdplus_id,
                    'count': count,
                    'sum': float(valid_data.sum()) if count > 0 else None,
                    'max': float(valid_data.max()) if count > 0 else None,
                    'min': float(valid_data.min()) if count > 0 else None,
                })

            except Exception as exc:
                log.error(f'NHDPlusID {nhdplus_id}: error processing catchment — {exc}')
                log.debug(traceback.format_exc())
                errors += 1
                continue

    log.info(f'Processed {len(results):,} catchments, {skipped:,} skipped, {errors:,} errors')

    if not results:
        log.warning('No results to write')
        return

    # ------------------------------------------------------------------
    # 5. Write results to a parquet file named after the VPU
    # ------------------------------------------------------------------
    df = pd.DataFrame(results)
    output_path = os.path.join(output_dir, theme, f'{theme}_huc_{huc4}.parquet')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    log.info(f'Results written to: {output_path}')


def scrape_integer_raster(huc4: str, theme: str, nhd_gdb: str, raster_path: str, output_dir: str, skip_values: set = None) -> None:
    """Calculate per-catchment pixel-value frequency maps for an integer raster and write to parquet.

    For each catchment polygon, counts how many pixels of each unique integer value fall inside it.
    Results are written as a parquet file with columns: HUC4, NHDPlusID, value_counts (map type).

    Args:
        huc4:        Four-digit HUC4 code used to filter NHDPlusCatchment by matching
                     the first four characters of the VPUID field.
        theme:       Theme name used as both folder name and prefix for output files.
        nhd_gdb:     Path to the NHD ESRI File Geodatabase.
        raster_path: Path to the source integer raster.
        output_dir:  Directory where the output parquet file will be written.
    """
    log = Logger(f"Scrape {theme} HUC4 {huc4}")
    huc4 = huc4.zfill(4)

    output_path = os.path.join(output_dir, theme, f'{theme}_huc_{huc4}.parquet')
    if os.path.exists(output_path):
        log.info(f'Output already exists, skipping: {output_path}')
        return

    # ------------------------------------------------------------------
    # 1. Read raster CRS, nodata value, and cell area in square metres
    # ------------------------------------------------------------------
    _UNIT_TO_METRES = {
        'metre': 1.0,
        'meter': 1.0,
        'foot': 0.3048,
        'us survey foot': 0.30480060960121924,
    }

    with rasterio.open(raster_path) as src:
        raster_crs = src.crs
        raster_nodata = src.nodata
        raster_transform = src.transform

    linear_unit = raster_crs.linear_units.lower()
    unit_factor = _UNIT_TO_METRES.get(linear_unit)
    if unit_factor is None:
        raise RuntimeError(
            f'Cannot convert raster CRS linear unit "{linear_unit}" to metres for area calculation. '
            'Ensure the raster is in a projected CRS with metre or foot units.'
        )
    cell_area_sqm = (abs(raster_transform.a) * unit_factor) * (abs(raster_transform.e) * unit_factor)

    log.info(f'Raster CRS:      {raster_crs.to_string()}')
    log.info(f'Raster NoData:   {raster_nodata}')
    log.info(f'CRS linear unit: {linear_unit} (factor to m: {unit_factor})')
    log.info(f'Cell area:       {cell_area_sqm:.4f} sq metres')

    # ------------------------------------------------------------------
    # 2. Load NHDPlusCatchment polygons filtered to the requested HUC4
    # ------------------------------------------------------------------
    log.info(f'Loading NHDPlusCatchment for HUC4: {huc4}')
    try:
        gdf = gpd.read_file(nhd_gdb, layer='NHDPlusCatchment', where=f"SUBSTR(VPUID, 1, 4) = '{huc4}'")
    except Exception as exc:
        raise RuntimeError(f'Failed to read NHDPlusCatchment from {nhd_gdb}: {exc}') from exc

    log.info(f'Catchment CRS: {gdf.crs.to_string()}')
    log.info(f'Loaded {len(gdf):,} catchments for HUC4: {huc4}')

    if len(gdf) == 0:
        log.warning(f'No catchments found for HUC4: {huc4}')
        return

    # ------------------------------------------------------------------
    # 3. Reproject polygons to raster CRS if necessary
    # ------------------------------------------------------------------
    if gdf.crs != raster_crs:
        log.info(f'Reprojecting catchments from {gdf.crs.to_string()} to {raster_crs.to_string()}')
        gdf = gdf.to_crs(raster_crs)
    else:
        log.info('Catchments and raster share the same CRS — no reprojection needed')

    # ------------------------------------------------------------------
    # 4. Loop over catchments and compute value-area maps
    # ------------------------------------------------------------------
    nhdplus_id_col = []
    value_counts_col = []  # list of list-of-tuples: (pixel_value, area_sqm)
    errors = 0
    skipped = 0

    id_col = next((c for c in gdf.columns if c.lower() == 'nhdplusid'), None)

    with rasterio.open(raster_path) as src:
        raster_bounds = src.bounds

        for idx, row in gdf.iterrows():
            nhdplus_id = row[id_col] if id_col else idx
            polygon = row.geometry

            if polygon is None or polygon.is_empty:
                log.warning(f'NHDPlusID {nhdplus_id}: empty geometry, skipping')
                skipped += 1
                continue

            if not polygon.is_valid:
                log.warning(f'NHDPlusID {nhdplus_id}: invalid geometry, skipping')
                skipped += 1
                continue

            try:
                minx, miny, maxx, maxy = polygon.bounds

                cminx = max(minx, raster_bounds.left)
                cminy = max(miny, raster_bounds.bottom)
                cmaxx = min(maxx, raster_bounds.right)
                cmaxy = min(maxy, raster_bounds.top)

                if cminx >= cmaxx or cminy >= cmaxy:
                    log.debug(f'NHDPlusID {nhdplus_id}: polygon outside raster extent, skipping')
                    skipped += 1
                    continue

                window = from_bounds(cminx, cminy, cmaxx, cmaxy, src.transform)
                window = window.round_offsets(pixel_precision=0).round_lengths(pixel_precision=0)

                if window.width <= 0 or window.height <= 0:
                    log.debug(f'NHDPlusID {nhdplus_id}: zero-size window, skipping')
                    skipped += 1
                    continue

                data = src.read(1, window=window, masked=True)
                win_transform = src.window_transform(window)

                nrows, ncols = data.shape
                col_idx, row_idx = np.meshgrid(np.arange(ncols), np.arange(nrows))
                x_centroids = win_transform.c + (col_idx + 0.5) * win_transform.a
                y_centroids = win_transform.f + (row_idx + 0.5) * win_transform.e

                inside = _centroids_in_polygon(
                    polygon,
                    x_centroids.ravel(),
                    y_centroids.ravel(),
                ).reshape(nrows, ncols)

                existing_mask = np.ma.getmaskarray(data)
                combined_mask = np.logical_or(existing_mask, ~inside)

                valid_data = np.ma.array(data.data, mask=combined_mask)
                valid_pixels = valid_data.compressed().astype(np.int64)

                if skip_values:
                    skip_int = np.array([np.int64(sv) for sv in skip_values], dtype=np.int64)
                    valid_pixels = valid_pixels[~np.isin(valid_pixels, skip_int)]

                if valid_pixels.size == 0:
                    log.debug(f'NHDPlusID {nhdplus_id}: no valid pixels, skipping')
                    skipped += 1
                    continue

                values, counts = np.unique(valid_pixels, return_counts=True)
                areas = (counts.astype(np.float64) * cell_area_sqm).tolist()
                value_counts = list(zip(values.tolist(), areas))

                nhdplus_id_col.append(nhdplus_id)
                value_counts_col.append(value_counts)

            except Exception as exc:
                log.error(f'NHDPlusID {nhdplus_id}: error processing catchment — {exc}')
                log.debug(traceback.format_exc())
                errors += 1
                continue

    log.info(f'Processed {len(nhdplus_id_col):,} catchments, {skipped:,} skipped, {errors:,} errors')

    if not nhdplus_id_col:
        log.warning('No results to write')
        return

    # ------------------------------------------------------------------
    # 5. Write results to parquet using a pyarrow map column
    # ------------------------------------------------------------------
    schema = pa.schema([
        pa.field('NHDPlusID', pa.float64()),
        pa.field('value_counts', pa.map_(pa.int64(), pa.float64())),
    ])
    table = pa.table(
        {
            'NHDPlusID': pa.array(nhdplus_id_col, type=pa.float64()),
            'value_counts': pa.array(value_counts_col, type=pa.map_(pa.int64(), pa.float64())),
        },
        schema=schema,
    )
    output_path = os.path.join(output_dir, theme, f'{theme}_huc_{huc4}.parquet')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    pq.write_table(table, output_path)
    log.info(f'Results written to: {output_path}')


def main():
    parser = argparse.ArgumentParser(
        description='Calculate raster statistics for NHD catchments and write to parquet'
    )
    parser.add_argument('huc2', type=str, help='Comma-separated list of two-digit HUC2 codes used to filter NHDPlusCatchment by matching the first two characters of VPUID')
    parser.add_argument('theme', type=str, help='Theme name used as both folder name and prefix for output files')
    parser.add_argument('nhd', type=str, help='Path to ESRI File Geodatabase containing NHDPlusCatchment')
    parser.add_argument('raster', type=str, help='Path to the raster to scrape statistics from')
    parser.add_argument('output_dir', type=str, help='Directory to save the output parquet file')
    parser.add_argument('--integer', action='store_true', default=False, help='Treat raster as integer: output a value-count map instead of float statistics')
    parser.add_argument('--skip', type=str, default=None, help='Comma-separated list of raster values to ignore (e.g. "-9999,0")')
    parser.add_argument('--verbose', action='store_true', default=False, help='Enable verbose logging')
    args = dotenv.parse_args_env(parser)

    # Parse and zero-fill each HUC2 code in the comma-separated list
    huc2_list = [h.strip().zfill(2) for h in args.huc2.split(',') if h.strip()]

    skip_values = {float(v.strip()) for v in args.skip.split(',') if v.strip()} if args.skip else None

    # output_dir = os.path.join(args.output_dir, args.theme)
    os.makedirs(args.output_dir, exist_ok=True)

    log = Logger(f"Scrape {args.theme}")
    log.setup(log_path=os.path.join(args.output_dir, 'scrape-raster.log'), verbose=args.verbose)
    log.title(f'Raster Scrape For HUC2(s): {", ".join(huc2_list)}')
    log.info(f'HUC2(s):       {", ".join(huc2_list)}')
    log.info(f'Theme:         {args.theme}')
    log.info(f'NHD GDB:       {args.nhd}')
    log.info(f'Raster:        {args.raster}')
    log.info(f'Output folder: {args.output_dir}')
    log.info(f'Mode:          {"integer (value counts)" if args.integer else "float (statistics)"}')
    log.info(f'Skip values:   {", ".join(str(v) for v in sorted(skip_values)) if skip_values else "(none)"}')

    scrape_fn = scrape_integer_raster if args.integer else scrape_float_raster

    errors = []
    for huc2 in huc2_list:
        try:
            huc4s = get_huc4s_for_huc2(args.nhd, huc2, log)
        except Exception as exc:
            log.error(f'Failed to retrieve HUC4s for HUC2 {huc2}: {exc}')
            traceback.print_exc()
            errors.append(huc2)
            continue

        for huc4 in huc4s:
            try:
                scrape_fn(huc4, args.theme, args.nhd, args.raster, args.output_dir, skip_values)
            except Exception as exc:
                log.error(f'Scrape raster failed for HUC4 {huc4}: {exc}')
                traceback.print_exc()
                errors.append(huc4)

    if errors:
        log.error(f'Failed HUC2(s): {", ".join(errors)}')
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
