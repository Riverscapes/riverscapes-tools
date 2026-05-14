"""
Network Attribute script to scrape vector data for NHD catchments and write results to parquet.
The script intersects the input vector layer with NHDPlusCatchment polygons for a specified HUC2 region and calculates one of the following metrics per catchment:
  - Point/MultiPoint:           count of features per catchment
  - LineString/MultiLineString: total length (metres) of clipped features per catchment
  - Polygon/MultiPolygon:       total area (square metres) of intersection per catchment

Input geometries are reprojected to EPSG:5070 for accurate area/length measurements. Points are reprojected to the NHD catchment CRS for the spatial join.
Results are written to a parquet file with one row per catchment (identified by NHDPlusID) and a column for the calculated metric. If additional fields are specified, extra map columns are added where keys are unique field values and values are the corresponding metric (count/length/area) for that field value.
The output file is organized in a subdirectory named after the theme (e.g. "landfire") under the specified output directory, with a filename pattern of {theme}_huc_{huc2}.parquet.

There is a similar script for raster data: scrape-raster.py

Philip Bailey
May 2026
"""

import argparse
import os
import sys
import traceback
from typing import List, Optional

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rsxml import Logger, dotenv
from shapely.geometry import box

# NAD83 / Conus Albers — cartesian CRS for area/length measurements in metres
CARTESIAN_CRS = 'EPSG:5070'

POINT_TYPES = {'Point', 'MultiPoint'}
LINE_TYPES = {'LineString', 'MultiLineString'}
POLY_TYPES = {'Polygon', 'MultiPolygon'}


def _to_pa_map_col(ids: list, maps_dict: dict, value_type) -> pa.Array:
    """Build a PyArrow map array (keyed by str) from a dict of {id: [(key, value), ...]}."""
    return pa.array([maps_dict.get(nid, []) for nid in ids], type=pa.map_(pa.string(), value_type))


def get_huc4s_for_huc2(nhd_gdb: str, huc2: str, log: Logger) -> List[str]:
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
    log.info(f'Found {len(huc4s)} HUC4(s) in HUC2 {huc2}: {', '.join(huc4s)}')
    return huc4s


def scrape_vector_layer(huc4: str, theme: str, nhd_gdb: str, vector_path: str, layer_name: Optional[str], output_dir: str, fields: Optional[List[str]] = None) -> None:
    """Calculate per-catchment vector statistics for a HUC2 region and write results to parquet.

    Geometry-type dispatch:
      - Point/MultiPoint:           count of features per catchment
      - LineString/MultiLineString: total length (metres) of clipped features per catchment
      - Polygon/MultiPolygon:       total area (square metres) of intersection per catchment

    Input polygons and lines are reprojected to EPSG:5070 for measurements.
    Points are reprojected to the NHD catchment CRS for the spatial join.

    Args:
        huc4:        Four-digit HUC4 code used to filter NHDPlusCatchment by VPUID.
        theme:       Theme name used as folder name and output file prefix.
        nhd_gdb:     Path to the NHD ESRI File Geodatabase.
        vector_path: Path to the input vector dataset (any OGR-readable format).
        layer_name:  Layer name within the dataset (uses first/default layer if None).
        output_dir:  Directory where the output parquet file will be written.
    """
    log = Logger(f"Scrape {theme} HUC4 {huc4}")
    huc4 = huc4.zfill(4)

    output_path = os.path.join(output_dir, theme, f'{theme}_huc_{huc4}.parquet')
    if os.path.exists(output_path):
        log.info(f'Output already exists, skipping: {output_path}')
        return

    # ------------------------------------------------------------------
    # 1. Load NHDPlusCatchment polygons filtered to the requested HUC4
    # ------------------------------------------------------------------
    log.info(f'Loading NHDPlusCatchment for HUC4: {huc4}')
    try:
        gdf_nhd = gpd.read_file(nhd_gdb, layer='NHDPlusCatchment', where=f"SUBSTR(VPUID, 1, 4) = '{huc4}'")
    except Exception as exc:
        raise RuntimeError(f'Failed to read NHDPlusCatchment from {nhd_gdb}: {exc}') from exc

    log.info(f'NHD CRS:        {gdf_nhd.crs.to_string()}')
    log.info(f'Loaded {len(gdf_nhd):,} catchments for HUC4: {huc4}')

    if len(gdf_nhd) == 0:
        log.warning(f'No catchments found for HUC4: {huc4}')
        return

    id_col = next((c for c in gdf_nhd.columns if c.lower() == 'nhdplusid'), None)
    if id_col is None:
        raise RuntimeError('NHDPlusID column not found in NHDPlusCatchment layer')

    # ------------------------------------------------------------------
    # 2. Load input vector layer (pre-clipped to the NHD bounding box)
    # ------------------------------------------------------------------
    log.info(f'Loading vector layer: {vector_path}' + (f' (layer: {layer_name})' if layer_name else ''))
    log.info('Applying bbox pre-filter from NHD catchment extent')
    try:
        gdf_sample = gpd.read_file(vector_path, layer=layer_name, rows=0)
        input_crs = gdf_sample.crs
        if input_crs != gdf_nhd.crs:
            log.info(f'Reprojecting NHD bbox from {gdf_nhd.crs.to_string()} to input CRS {input_crs.to_string()} for pre-filter')
            nhd_bbox_gdf = gpd.GeoDataFrame(geometry=[box(*gdf_nhd.total_bounds)], crs=gdf_nhd.crs)
            bbox_filter = tuple(nhd_bbox_gdf.to_crs(input_crs).total_bounds)
        else:
            bbox_filter = tuple(gdf_nhd.total_bounds)
        gdf_input = gpd.read_file(vector_path, layer=layer_name, bbox=bbox_filter)
    except Exception as exc:
        raise RuntimeError(f'Failed to read vector layer from {vector_path}: {exc}') from exc

    log.info(f'Input CRS:      {gdf_input.crs.to_string()}')
    log.info(f'Input features after bbox pre-filter: {len(gdf_input):,}')

    if len(gdf_input) == 0:
        log.warning('Input vector layer has no features')
        return

    if fields:
        missing = [f for f in fields if f not in gdf_input.columns]
        if missing:
            raise ValueError(f'Fields not found in input layer: {missing}')

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
        df, field_maps = _process_points(gdf_nhd, gdf_input, id_col, log, fields)
        map_value_type = pa.int64()
    elif mode == 'line':
        df, field_maps = _process_lines(gdf_nhd, gdf_input, id_col, log, fields)
        map_value_type = pa.float64()
    else:
        df, field_maps = _process_polygons(gdf_nhd, gdf_input, id_col, log, fields)
        map_value_type = pa.float64()

    if df is None or len(df) == 0:
        log.warning('No results to write')
        return

    # ------------------------------------------------------------------
    # 5. Write results to parquet
    # ------------------------------------------------------------------
    output_path = os.path.join(output_dir, theme, f'{theme}_huc_{huc4}.parquet')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if field_maps:
        table = pa.Table.from_pandas(df, preserve_index=False)
        ids = df['NHDPlusID'].tolist()
        for field in fields:
            table = table.append_column(field, _to_pa_map_col(ids, field_maps.get(field, {}), map_value_type))
        pq.write_table(table, output_path)
    else:
        df.to_parquet(output_path, index=False)
    log.info(f'Results written to: {output_path}')


def _process_points(gdf_nhd: gpd.GeoDataFrame, gdf_input: gpd.GeoDataFrame, id_col: str, log: Logger, fields: Optional[List[str]] = None) -> tuple:
    """Count points within each catchment polygon using a spatial join."""
    if gdf_input.crs != gdf_nhd.crs:
        log.info(f'Reprojecting points to NHD CRS: {gdf_nhd.crs.to_string()}')
        gdf_input = gdf_input.to_crs(gdf_nhd.crs)

    input_cols = ['geometry'] + [f for f in (fields or []) if f in gdf_input.columns]
    joined = gpd.sjoin(
        gdf_input[input_cols],
        gdf_nhd[[id_col, 'geometry']],
        how='inner',
        predicate='within',
    )
    log.info(f'Spatial join produced {len(joined):,} matched point(s)')
    if len(joined) == 0:
        log.warning('No points fell within any catchment — returning empty result')
        return pd.DataFrame(columns=['NHDPlusID', 'count']), {}
    result = joined.groupby(id_col).size().reset_index(name='count')
    result.rename(columns={id_col: 'NHDPlusID'}, inplace=True)
    log.info(f'Point counts computed for {len(result):,} catchments')

    field_maps = {}
    for field in (fields or []):
        grouped = joined.groupby([id_col, field]).size().reset_index(name='_cnt')
        maps: dict = {}
        for _, row in grouped.iterrows():
            if pd.isna(row[field]):
                continue
            maps.setdefault(row[id_col], []).append((str(row[field]), int(row['_cnt'])))
        field_maps[field] = maps

    return result, field_maps


def _process_lines(gdf_nhd: gpd.GeoDataFrame, gdf_input: gpd.GeoDataFrame, id_col: str, log: Logger, fields: Optional[List[str]] = None) -> tuple:
    """Sum clipped line length (metres) per catchment in EPSG:5070."""
    log.info(f'Reprojecting to cartesian CRS {CARTESIAN_CRS} for length calculation')
    gdf_nhd = gdf_nhd[[id_col, 'geometry']].to_crs(CARTESIAN_CRS)
    input_cols = ['geometry'] + [f for f in (fields or []) if f in gdf_input.columns]
    gdf_input = gdf_input[input_cols].to_crs(CARTESIAN_CRS)

    log.info('Intersecting lines with catchments')
    intersection = gpd.overlay(gdf_input, gdf_nhd, how='intersection', keep_geom_type=False)
    intersection = intersection.explode(index_parts=False).reset_index(drop=True)
    intersection = intersection[intersection.geom_type.isin(['LineString', 'MultiLineString'])].copy()
    log.info(f'Overlay produced {len(intersection):,} intersection segment(s)')
    if len(intersection) == 0:
        log.warning('No line segments intersected any catchment — returning empty result')
        return pd.DataFrame(columns=['NHDPlusID', 'length_m']), {}
    intersection['length_m'] = intersection.geometry.length

    result = intersection.groupby(id_col)['length_m'].sum().reset_index()
    result = result[result['length_m'] > 0].copy()
    result.rename(columns={id_col: 'NHDPlusID'}, inplace=True)
    log.info(f'Line lengths computed for {len(result):,} catchments')

    field_maps = {}
    for field in (fields or []):
        sub = intersection[intersection['length_m'] > 0]
        grouped = sub.groupby([id_col, field])['length_m'].sum().reset_index()
        maps: dict = {}
        for _, row in grouped.iterrows():
            if pd.isna(row[field]):
                continue
            maps.setdefault(row[id_col], []).append((str(row[field]), float(row['length_m'])))
        field_maps[field] = maps

    return result, field_maps


def _process_polygons(gdf_nhd: gpd.GeoDataFrame, gdf_input: gpd.GeoDataFrame, id_col: str, log: Logger, fields: Optional[List[str]] = None) -> tuple:
    """Sum intersection area (square metres) per catchment in EPSG:5070."""
    log.info(f'Reprojecting to cartesian CRS {CARTESIAN_CRS} for area calculation')
    gdf_nhd = gdf_nhd[[id_col, 'geometry']].to_crs(CARTESIAN_CRS)
    input_cols = ['geometry'] + [f for f in (fields or []) if f in gdf_input.columns]
    gdf_input = gdf_input[input_cols].to_crs(CARTESIAN_CRS)
    gdf_input = gdf_input.assign(_input_fid=range(len(gdf_input)))

    log.info('Intersecting polygons with catchments')
    intersection = gpd.overlay(gdf_input, gdf_nhd, how='intersection', keep_geom_type=False)
    intersection = intersection.explode(index_parts=False).reset_index(drop=True)
    intersection = intersection[intersection.geom_type.isin(['Polygon', 'MultiPolygon'])].copy()
    log.info(f'Overlay produced {len(intersection):,} intersection polygon(s)')
    log.info(f'Input FIDs contributing to intersection: {sorted(intersection["_input_fid"].unique().tolist())}')
    # intersection.to_file('/tmp/debug_intersection.gpkg', layer='intersection', driver='GPKG')
    log.info('DEBUG: intersection written to /tmp/debug_intersection.gpkg')
    if len(intersection) == 0:
        log.warning('No polygons intersected any catchment — returning empty result')
        return pd.DataFrame(columns=['NHDPlusID', 'area_sqm']), {}
    intersection['area_sqm'] = intersection.geometry.area

    result = intersection.groupby(id_col)['area_sqm'].sum().reset_index()
    result = result[result['area_sqm'] > 0].copy()
    result.rename(columns={id_col: 'NHDPlusID'}, inplace=True)
    log.info(f'Polygon areas computed for {len(result):,} catchments')

    field_maps = {}
    for field in (fields or []):
        sub = intersection[intersection['area_sqm'] > 0]
        grouped = sub.groupby([id_col, field])['area_sqm'].sum().reset_index()
        maps: dict = {}
        for _, row in grouped.iterrows():
            if pd.isna(row[field]):
                continue
            maps.setdefault(row[id_col], []).append((str(row[field]), float(row['area_sqm'])))
        field_maps[field] = maps

    return result, field_maps


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
    parser.add_argument('--fields', type=str, default=None, help='Comma-separated field names to aggregate by; adds one map column per field (map keys = field values, map values = metric)')
    parser.add_argument('--verbose', action='store_true', default=False, help='Enable verbose logging')
    args = dotenv.parse_args_env(parser)

    huc2_list = [h.strip().zfill(2) for h in args.huc2.split(',') if h.strip()]
    fields = [f.strip() for f in args.fields.split(',') if f.strip()] if args.fields else None

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
    log.info(f'Fields:        {", ".join(fields) if fields else "(none)"}')
    log.info(f'Output folder: {output_dir}')

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
                scrape_vector_layer(huc4, args.theme, args.nhd, args.vector, args.layer, output_dir, fields)
            except Exception as exc:
                log.error(f'Scrape vector failed for HUC4 {huc4}: {exc}')
                traceback.print_exc()
                errors.append(huc4)

    if errors:
        log.error(f'Failed HUC(s): {", ".join(errors)}')
        sys.exit(1)
    sys.exit(0)


if __name__ == '__main__':
    main()
