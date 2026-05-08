import argparse
import os
import sys
import traceback
from typing import Optional

import geopandas as gpd
import pandas as pd
from rsxml import Logger, dotenv

# NAD83 / Conus Albers — cartesian CRS for area/length measurements in metres
CARTESIAN_CRS = 'EPSG:5070'

POINT_TYPES = {'Point', 'MultiPoint'}
LINE_TYPES = {'LineString', 'MultiLineString'}
POLY_TYPES = {'Polygon', 'MultiPolygon'}


def scrape_vector_layer(huc2: str, theme: str, nhd_gdb: str, vector_path: str, layer_name: Optional[str], output_dir: str) -> None:
    """Calculate per-catchment vector statistics for a HUC2 region and write results to parquet.

    Geometry-type dispatch:
      - Point/MultiPoint:           count of features per catchment
      - LineString/MultiLineString: total length (metres) of clipped features per catchment
      - Polygon/MultiPolygon:       total area (square metres) of intersection per catchment

    Input polygons and lines are reprojected to EPSG:5070 for measurements.
    Points are reprojected to the NHD catchment CRS for the spatial join.

    Args:
        huc2:        Two-digit HUC2 code used to filter NHDPlusCatchment.
        theme:       Theme name used as folder name and output file prefix.
        nhd_gdb:     Path to the NHD ESRI File Geodatabase.
        vector_path: Path to the input vector dataset (any OGR-readable format).
        layer_name:  Layer name within the dataset (uses first/default layer if None).
        output_dir:  Directory where the output parquet file will be written.
    """
    log = Logger(f"Scrape {theme} HUC2 {huc2}")
    huc2 = huc2.zfill(2)

    # ------------------------------------------------------------------
    # 1. Load NHDPlusCatchment polygons filtered to the requested HUC2
    # ------------------------------------------------------------------
    log.info(f'Loading NHDPlusCatchment for HUC2: {huc2}')
    try:
        gdf_nhd = gpd.read_file(nhd_gdb, layer='NHDPlusCatchment', where=f"SUBSTR(VPUID, 1, 2) = '{huc2}'")
    except Exception as exc:
        raise RuntimeError(f'Failed to read NHDPlusCatchment from {nhd_gdb}: {exc}') from exc

    log.info(f'NHD CRS:        {gdf_nhd.crs.to_string()}')
    log.info(f'Loaded {len(gdf_nhd):,} catchments for HUC2: {huc2}')

    if len(gdf_nhd) == 0:
        log.warning(f'No catchments found for HUC2: {huc2}')
        return

    id_col = next((c for c in gdf_nhd.columns if c.lower() == 'nhdplusid'), None)
    if id_col is None:
        raise RuntimeError('NHDPlusID column not found in NHDPlusCatchment layer')

    # ------------------------------------------------------------------
    # 2. Load input vector layer
    # ------------------------------------------------------------------
    log.info(f'Loading vector layer: {vector_path}' + (f' (layer: {layer_name})' if layer_name else ''))
    try:
        gdf_input = gpd.read_file(vector_path, layer=layer_name)
    except Exception as exc:
        raise RuntimeError(f'Failed to read vector layer from {vector_path}: {exc}') from exc

    log.info(f'Input CRS:      {gdf_input.crs.to_string()}')
    log.info(f'Input features: {len(gdf_input):,}')

    if len(gdf_input) == 0:
        log.warning('Input vector layer has no features')
        return

    # ------------------------------------------------------------------
    # 3. Detect geometry type
    # ------------------------------------------------------------------
    geom_types = set(gdf_input.geom_type.dropna().unique())
    log.info(f'Geometry types: {", ".join(sorted(geom_types))}')

    if geom_types <= POINT_TYPES:
        mode = 'point'
    elif geom_types <= LINE_TYPES:
        mode = 'line'
    elif geom_types <= POLY_TYPES:
        mode = 'polygon'
    else:
        raise RuntimeError(f'Mixed or unsupported geometry types: {", ".join(sorted(geom_types))}')

    log.info(f'Mode:           {mode}')

    # ------------------------------------------------------------------
    # 4. Dispatch to geometry-specific handler
    # ------------------------------------------------------------------
    if mode == 'point':
        df = _process_points(gdf_nhd, gdf_input, id_col, huc2, log)
    elif mode == 'line':
        df = _process_lines(gdf_nhd, gdf_input, id_col, huc2, log)
    else:
        df = _process_polygons(gdf_nhd, gdf_input, id_col, huc2, log)

    if df is None or len(df) == 0:
        log.warning('No results to write')
        return

    # ------------------------------------------------------------------
    # 5. Write results to parquet
    # ------------------------------------------------------------------
    output_path = os.path.join(output_dir, theme, f'{theme}_huc_{huc2}.parquet')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    log.info(f'Results written to: {output_path}')


def _process_points(gdf_nhd: gpd.GeoDataFrame, gdf_input: gpd.GeoDataFrame, id_col: str, huc2: str, log: Logger) -> pd.DataFrame:
    """Count points within each catchment polygon using a spatial join."""
    if gdf_input.crs != gdf_nhd.crs:
        log.info(f'Reprojecting points to NHD CRS: {gdf_nhd.crs.to_string()}')
        gdf_input = gdf_input.to_crs(gdf_nhd.crs)

    joined = gpd.sjoin(
        gdf_input[['geometry']],
        gdf_nhd[[id_col, 'geometry']],
        how='inner',
        predicate='within',
    )
    result = joined.groupby(id_col).size().reset_index(name='count')
    result.insert(0, 'HUC2', huc2)
    result.rename(columns={id_col: 'NHDPlusID'}, inplace=True)
    log.info(f'Point counts computed for {len(result):,} catchments')
    return result


def _process_lines(gdf_nhd: gpd.GeoDataFrame, gdf_input: gpd.GeoDataFrame, id_col: str, huc2: str, log: Logger) -> pd.DataFrame:
    """Sum clipped line length (metres) per catchment in EPSG:5070."""
    log.info(f'Reprojecting to cartesian CRS {CARTESIAN_CRS} for length calculation')
    gdf_nhd = gdf_nhd[[id_col, 'geometry']].to_crs(CARTESIAN_CRS)
    gdf_input = gdf_input[['geometry']].to_crs(CARTESIAN_CRS)

    log.info('Intersecting lines with catchments')
    intersection = gpd.overlay(gdf_input, gdf_nhd, how='intersection', keep_geom_type=True)
    intersection['length_m'] = intersection.geometry.length

    result = intersection.groupby(id_col)['length_m'].sum().reset_index()
    result = result[result['length_m'] > 0].copy()
    result.insert(0, 'HUC2', huc2)
    result.rename(columns={id_col: 'NHDPlusID'}, inplace=True)
    log.info(f'Line lengths computed for {len(result):,} catchments')
    return result


def _process_polygons(gdf_nhd: gpd.GeoDataFrame, gdf_input: gpd.GeoDataFrame, id_col: str, huc2: str, log: Logger) -> pd.DataFrame:
    """Sum intersection area (square metres) per catchment in EPSG:5070."""
    log.info(f'Reprojecting to cartesian CRS {CARTESIAN_CRS} for area calculation')
    gdf_nhd = gdf_nhd[[id_col, 'geometry']].to_crs(CARTESIAN_CRS)
    gdf_input = gdf_input[['geometry']].to_crs(CARTESIAN_CRS)

    log.info('Intersecting polygons with catchments')
    intersection = gpd.overlay(gdf_input, gdf_nhd, how='intersection', keep_geom_type=True)
    intersection['area_sqm'] = intersection.geometry.area

    result = intersection.groupby(id_col)['area_sqm'].sum().reset_index()
    result = result[result['area_sqm'] > 0].copy()
    result.insert(0, 'HUC2', huc2)
    result.rename(columns={id_col: 'NHDPlusID'}, inplace=True)
    log.info(f'Polygon areas computed for {len(result):,} catchments')
    return result


def main():
    parser = argparse.ArgumentParser(
        description='Calculate vector statistics (point count / line length / polygon area) for NHD catchments and write to parquet'
    )
    parser.add_argument('huc2', type=str, help='Comma-separated list of two-digit HUC2 codes used to filter NHDPlusCatchment')
    parser.add_argument('theme', type=str, help='Theme name used as folder name and output file prefix')
    parser.add_argument('nhd', type=str, help='Path to ESRI File Geodatabase containing NHDPlusCatchment')
    parser.add_argument('vector', type=str, help='Path to the input vector dataset (any OGR-readable format)')
    parser.add_argument('output_dir', type=str, help='Directory to save the output parquet file')
    parser.add_argument('--layer', type=str, default=None, help='Layer name within the vector dataset (uses first/default layer if omitted)')
    parser.add_argument('--verbose', action='store_true', default=False, help='Enable verbose logging')
    args = dotenv.parse_args_env(parser)

    huc2_list = [h.strip().zfill(2) for h in args.huc2.split(',') if h.strip()]

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    log = Logger(f"Scrape {args.theme}")
    log.setup(log_path=os.path.join(output_dir, 'scrape-vector.log'), verbose=args.verbose)
    log.title(f'Vector Scrape For HUC2(s): {", ".join(huc2_list)}')
    log.info(f'HUC2(s):       {", ".join(huc2_list)}')
    log.info(f'Theme:         {args.theme}')
    log.info(f'NHD GDB:       {args.nhd}')
    log.info(f'Vector:        {args.vector}')
    log.info(f'Layer:         {args.layer or "(default)"}')
    log.info(f'Output folder: {output_dir}')

    errors = []
    for huc2 in huc2_list:
        try:
            scrape_vector_layer(huc2, args.theme, args.nhd, args.vector, args.layer, output_dir)
        except Exception as exc:
            log.error(f'Scrape vector failed for HUC2 {huc2}: {exc}')
            traceback.print_exc()
            errors.append(huc2)

    if errors:
        log.error(f'Failed HUC2(s): {", ".join(errors)}')
        sys.exit(1)
    sys.exit(0)


if __name__ == '__main__':
    main()
