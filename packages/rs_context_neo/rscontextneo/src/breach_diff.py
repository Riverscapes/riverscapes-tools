"""
Breach difference debug layer for RS Context Neo.

Computes pixel-wise elevation difference between the original DEM and the
breach-conditioned DEM, and writes point features (one per pixel) for every
pixel where the difference is >= 1 m.

Rasters are processed block-by-block (using each source raster's native tile
layout) so arbitrarily large DEMs never need to be held in memory at once.
"""
import os
from logging import Logger

import numpy as np
import rasterio
from osgeo import ogr, osr
from rsxml import ProgressBar

from rscontextneo.src.hydro_utils import skip_if_exists

# ── Module-level constants ─────────────────────────────────────────────────────
BREACH_DIFF_GPKG_RELPATH = 'hydrology/breach_diff_points.gpkg'
BREACH_DIFF_LAYER_NAME = 'breach_diff_points'
MIN_DIFF_METRES = 0.001


def create_breach_diff_points(
    dem_path: str,
    dem_breach_path: str,
    output_gpkg: str,
    force: bool,
    log: Logger,
) -> str:
    """
    Compute pixel-wise elevation difference (DEM − breach-conditioned DEM) and
    write every pixel with a difference ≥ 1 m as a point feature in a GeoPackage.

    Rasters are read block-by-block using the native tile layout of the DEM so
    that arbitrarily large rasters are never fully loaded into memory.

    Parameters
    ----------
    dem_path : str
        Absolute path to the original DEM raster.
    dem_breach_path : str
        Absolute path to the breach-conditioned DEM raster.
    output_gpkg : str
        Absolute path to the output GeoPackage file to create.
    force : bool
        If ``True``, recreate the output even if it already exists.
    log : Logger
        Caller-supplied logger.

    Returns
    -------
    str
        The path to the output GeoPackage (``output_gpkg``).
    """
    if skip_if_exists(output_gpkg, force, 'Breach difference points', log):
        return output_gpkg

    for label, path in [('DEM', dem_path), ('breach DEM', dem_breach_path)]:
        if not os.path.isfile(path):
            log.warning(f'  Breach difference skipped: {label} not found at {path}')
            return output_gpkg

    log.info(f'  Computing breach difference points: {os.path.basename(dem_path)} '
             f'vs {os.path.basename(dem_breach_path)}')

    # ── Set up OGR output ──────────────────────────────────────────────────────
    driver = ogr.GetDriverByName('GPKG')
    os.makedirs(os.path.dirname(output_gpkg), exist_ok=True)
    if os.path.isfile(output_gpkg):
        driver.DeleteDataSource(output_gpkg)

    try:
        with rasterio.open(dem_path) as src_dem, \
                rasterio.open(dem_breach_path) as src_breach:

            transform = src_dem.transform
            wkt_crs = src_dem.crs.to_wkt()
            dem_nodata = src_dem.nodata
            breach_nodata = src_breach.nodata

            ds = driver.CreateDataSource(output_gpkg)
            srs = osr.SpatialReference()
            srs.ImportFromWkt(wkt_crs)
            layer = ds.CreateLayer(BREACH_DIFF_LAYER_NAME, srs=srs, geom_type=ogr.wkbPoint)

            field_defn = ogr.FieldDefn('diff_m', ogr.OFTReal)
            field_defn.SetWidth(12)
            field_defn.SetPrecision(4)
            layer.CreateField(field_defn)
            feat_defn = layer.GetLayerDefn()

            # ── Iterate over native blocks ─────────────────────────────────────
            windows = list(src_dem.block_windows(1))
            progbar = ProgressBar(len(windows), 50, 'Breach difference points')
            n_pixels = 0

            layer.StartTransaction()
            for counter, (_ji, window) in enumerate(windows):
                progbar.update(counter)

                dem_block = src_dem.read(1, window=window, masked=True)
                # boundless=True fills any pixels that fall outside the breach
                # raster's extent with nodata rather than raising an error;
                # this handles the common case where WBT writes its output at
                # a slightly different size/extent than the input DEM.
                breach_block = src_breach.read(1, window=window, masked=True)

                # Mask out nodata explicitly in case the raster has no mask band
                if dem_nodata is not None:
                    dem_block = np.ma.masked_equal(dem_block, dem_nodata)
                if breach_nodata is not None:
                    breach_block = np.ma.masked_equal(breach_block, breach_nodata)

                diff_block = dem_block - breach_block

                # Pixels where both are valid and difference meets threshold
                valid_mask = (
                    ~np.ma.getmaskarray(diff_block)
                    & (np.ma.getdata(diff_block) >= MIN_DIFF_METRES)
                )

                row_idxs, col_idxs = np.where(valid_mask)
                if row_idxs.size == 0:
                    continue

                n_pixels += int(row_idxs.size)

                # Convert block-local indices to dataset-level row/col then to XY
                win_row_off = window.row_off
                win_col_off = window.col_off
                abs_rows = row_idxs + win_row_off
                abs_cols = col_idxs + win_col_off

                xs, ys = rasterio.transform.xy(
                    transform,
                    abs_rows.tolist(),
                    abs_cols.tolist(),
                )

                diff_vals = np.ma.getdata(diff_block)[row_idxs, col_idxs]

                for x, y, dv in zip(xs, ys, diff_vals):
                    geom = ogr.Geometry(ogr.wkbPoint)
                    geom.AddPoint(float(x), float(y))
                    feat = ogr.Feature(feat_defn)
                    feat.SetGeometry(geom)
                    feat.SetField('diff_m', float(dv))
                    layer.CreateFeature(feat)
                    feat = None

            layer.CommitTransaction()
            progbar.finish()

            ds.FlushCache()
            ds = None

    except Exception as exc:
        # Clean up a partially-written output so a re-run starts fresh
        if os.path.isfile(output_gpkg):
            try:
                driver.DeleteDataSource(output_gpkg)
            except Exception:
                pass
        raise RuntimeError(
            f'Breach difference points failed '
            f'({type(exc).__name__}: {exc})'
        ) from exc

    log.info(f'  Breach difference points written → {os.path.basename(output_gpkg)} '
             f'({n_pixels:,} features)')
    return output_gpkg
