"""
DEM boundary extraction for RS Context Neo.

Extracts the spatial footprint of a DEM raster (non-nodata areas) and writes
it as a buffered, smoothed GeoJSON MultiPolygon in WGS84 (EPSG:4326).
"""
import json
import os

import numpy as np
import rasterio
import rasterio.warp
from rasterio.features import shapes
from shapely.geometry import mapping, shape, MultiPolygon
from shapely.ops import unary_union

from rsxml import Logger

_OUTPUT_EPSG = 'EPSG:4326'
_BUFFER_PIXELS = 3    # outward buffer radius in pixel-widths: closes gaps, rounds jagged corners
_SIMPLIFY_PIXELS = 1  # simplification tolerance in pixel-widths: reduces vertex count


def dem_to_geojson(dem_path: str, output_folder: str, filename: str = 'project_bounds.geojson') -> str:
    """
    Extract the non-nodata footprint of a DEM raster and write it as a
    buffered, smoothed GeoJSON MultiPolygon in WGS84 (EPSG:4326).

    The smoothing pipeline is:
        1. Polygonize the valid-data mask
        2. Dissolve all polygons into a single geometry
        3. Buffer outward by (_BUFFER_PIXELS * pixel_size) to close small gaps
           and round jagged pixel-edge corners
        4. Simplify with tolerance (_SIMPLIFY_PIXELS * pixel_size) to reduce
           vertex count and smooth the outline
        5. Reproject to WGS84

    Parameters:
        dem_path (str): Path to the input DEM raster file.
        output_folder (str): Directory to write the output GeoJSON file.
        filename (str): Output filename. Defaults to 'project_bounds.geojson'.

    Returns:
        str: Absolute path to the written GeoJSON file.

    Raises:
        FileNotFoundError: If dem_path does not exist.
        ValueError: If the raster has no valid (non-nodata) pixels, or if the
                    geometry type after processing is unexpected.
    """
    log = Logger('DEM Bounds')

    if not os.path.exists(dem_path):
        raise FileNotFoundError(f'DEM file not found: {dem_path}')

    log.info(f'Extracting footprint from DEM: {dem_path}')

    with rasterio.open(dem_path) as src:
        band = src.read(1)
        nodata = src.nodata
        raster_transform = src.transform
        src_crs = src.crs
        pixel_size = abs(src.res[0])  # use x-resolution; assumes square pixels

    # Build a uint8 mask: 1 = valid data, 0 = nodata
    if nodata is not None:
        mask = (band != nodata).astype(np.uint8)
    elif np.issubdtype(band.dtype, np.floating):
        mask = (~np.isnan(band)).astype(np.uint8)
    else:
        # No nodata defined and not float — treat entire raster as valid
        log.warning('DEM has no nodata value set; treating all pixels as valid.')
        mask = np.ones(band.shape, dtype=np.uint8)

    valid_count = int(mask.sum())
    if valid_count == 0:
        raise ValueError(f'DEM has no valid (non-nodata) pixels: {dem_path}')

    log.info(f'Vectorizing valid-data mask ({valid_count:,} valid pixels)')

    # Polygonize the valid-data mask; shapes() yields (geojson_geom, pixel_value) pairs
    polys = [
        shape(geom)
        for geom, val in shapes(mask, mask=mask, transform=raster_transform)
        if val == 1
    ]

    if not polys:
        raise ValueError('No polygons could be extracted from the DEM mask.')

    log.info(f'Dissolving {len(polys):,} polygon(s)')
    merged = unary_union(polys)

    # Buffer outward to fill small holes and round pixel-edge jaggedness,
    # then simplify to smooth the outline and reduce vertex count
    buffer_dist = pixel_size * _BUFFER_PIXELS
    simplify_tol = pixel_size * _SIMPLIFY_PIXELS
    log.info(f'Buffering by {buffer_dist:.4f} units, simplifying with tolerance {simplify_tol:.4f} units')

    smoothed = merged.buffer(buffer_dist).simplify(simplify_tol, preserve_topology=True)

    # Normalise to MultiPolygon
    if smoothed.geom_type == 'Polygon':
        smoothed = MultiPolygon([smoothed])
    elif smoothed.geom_type != 'MultiPolygon':
        raise ValueError(f'Unexpected geometry type after processing: {smoothed.geom_type}')

    # Reproject to WGS84 for GeoJSON output
    geom_json = mapping(smoothed)
    if src_crs:
        log.info(f'Reprojecting from {src_crs.to_string()} to {_OUTPUT_EPSG}')
        geom_json = rasterio.warp.transform_geom(src_crs, _OUTPUT_EPSG, geom_json)

    # Write GeoJSON FeatureCollection
    dest = os.path.join(output_folder, filename)
    geojson = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'geometry': geom_json,
                'properties': {},
            }
        ],
    }

    with open(dest, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2)

    log.info(f'Project bounds written to: {dest}')
    return dest
