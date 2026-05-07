import argparse
import os
import sys
import traceback

import geopandas as gpd
import numpy as np
import pandas as pd
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


def scrape_raster(vpuid: str, nhd_gdb: str, raster_path: str, output_dir: str) -> None:
    """Calculate per-catchment raster statistics for a single VPU and write results to parquet.

    Only pixels whose centroid falls inside a catchment polygon contribute to
    its statistics, preventing double-counting at VPU boundaries.  The raster
    is never reprojected; catchment polygons are reprojected to the raster CRS
    when the two differ.

    Args:
        vpuid:       NHD VPU identifier used to filter NHDPlusCatchment.
        nhd_gdb:     Path to the NHD ESRI File Geodatabase.
        raster_path: Path to the source raster.
        output_dir:  Directory where the output parquet file will be written.
    """
    log = Logger("Scrape Raster")

    # ------------------------------------------------------------------
    # 1. Read raster CRS and nodata value
    # ------------------------------------------------------------------
    with rasterio.open(raster_path) as src:
        raster_crs = src.crs
        raster_nodata = src.nodata

    log.info(f'Raster CRS:    {raster_crs.to_string()}')
    log.info(f'Raster NoData: {raster_nodata}')

    # ------------------------------------------------------------------
    # 2. Load NHDPlusCatchment polygons filtered to the requested VPU
    # ------------------------------------------------------------------
    log.info(f'Loading NHDPlusCatchment for VPUID: {vpuid}')
    try:
        gdf = gpd.read_file(nhd_gdb, layer='NHDPlusCatchment', where=f"VPUID = '{vpuid}'")
    except Exception as exc:
        raise RuntimeError(f'Failed to read NHDPlusCatchment from {nhd_gdb}: {exc}') from exc

    log.info(f'Catchment CRS: {gdf.crs.to_string()}')
    log.info(f'Loaded {len(gdf):,} catchments for VPUID: {vpuid}')

    if len(gdf) == 0:
        log.warning(f'No catchments found for VPUID: {vpuid}')
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

                count = int(valid_data.count())

                results.append({
                    'VPUID': vpuid,
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
    output_path = os.path.join(output_dir, f'{vpuid}.parquet')
    df.to_parquet(output_path, index=False)
    log.info(f'Results written to: {output_path}')


def main():
    parser = argparse.ArgumentParser(
        description='Calculate raster statistics for NHD catchments and write to parquet'
    )
    parser.add_argument('vpuid', type=str, help='VPU identifier used to filter NHDPlusCatchment')
    parser.add_argument('nhd', type=str, help='Path to ESRI File Geodatabase containing NHDPlusCatchment')
    parser.add_argument('raster', type=str, help='Path to the raster to scrape statistics from')
    parser.add_argument('output_dir', type=str, help='Directory to save the output parquet file')
    parser.add_argument('--verbose', action='store_true', default=False, help='Enable verbose logging')
    args = dotenv.parse_args_env(parser)

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    log = Logger("Scrape Raster")
    log.setup(log_path=os.path.join(output_dir, 'scrape-raster.log'), verbose=args.verbose)
    log.title(f'Raster Scrape For VPU: {args.vpuid}')
    log.info(f'VPU:           {args.vpuid}')
    log.info(f'NHD GDB:       {args.nhd}')
    log.info(f'Raster:        {args.raster}')
    log.info(f'Output folder: {output_dir}')

    try:
        scrape_raster(args.vpuid, args.nhd, args.raster, output_dir)
        sys.exit(0)
    except Exception as exc:
        log.error(f'Scrape raster failed: {exc}')
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()