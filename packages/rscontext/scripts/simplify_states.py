"""Simplify a US States GeoJSON for use as thumbnail vicinity map backgrounds.

Reads a polygon GeoJSON (expected EPSG:5070, units metres), simplifies each
geometry using the Douglas-Peucker algorithm with topology preservation, and
writes a new GeoJSON file.

Usage:
    python simplify_states.py <input> <output> [--tolerance METRES]

Defaults:
    --tolerance  2000  (2 km — appropriate for state-level thumbnails at 3–10 cm)
"""

import argparse
import sys

import geopandas as gpd


def simplify_states(input_path: str, output_path: str, tolerance: float) -> None:
    gdf = gpd.read_file(input_path)
    print(f'Read {len(gdf)} features from {input_path}')
    print(f'CRS: {gdf.crs}')

    gdf['geometry'] = gdf['geometry'].simplify(tolerance, preserve_topology=True)

    # Drop any features that became empty after simplification
    before = len(gdf)
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]
    dropped = before - len(gdf)
    if dropped:
        print(f'Dropped {dropped} empty geometries after simplification')

    gdf.to_file(output_path, driver='GeoJSON')
    print(f'Written {len(gdf)} simplified features to {output_path}')


def main():
    parser = argparse.ArgumentParser(
        description='Simplify US States GeoJSON for thumbnail vicinity maps'
    )
    parser.add_argument('input', type=str, help='Path to input GeoJSON file')
    parser.add_argument('output', type=str, help='Path for output simplified GeoJSON file')
    parser.add_argument(
        '--tolerance',
        type=float,
        default=2000.0,
        help='Simplification tolerance in map units (metres for EPSG:5070). Default: 2000',
    )
    args = parser.parse_args()

    try:
        simplify_states(args.input, args.output, args.tolerance)
        sys.exit(0)
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
