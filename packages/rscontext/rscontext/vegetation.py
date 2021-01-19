import os
import json
import argparse
from osgeo import ogr
from osgeo import osr
from shapely.geometry import shape, mapping
from rscommons import Logger, dotenv, get_shp_or_gpkg, ShapefileLayer
import rasterio
from rscommons.raster_warp import raster_warp


def clip_vegetation(boundary_path: str, existing_veg_path: str, existing_clip_path: str, historic_veg_path: str, historic_clip_path: str, output_epsg: int):
    """[summary]

    Args:
        boundary_path (str): Path to layer
        existing_veg_path (str): Path to raster
        existing_clip_path (str): Path to output raster
        historic_veg_path (str): Path to raster
        historic_clip_path (str): Path to output raster
        output_epsg (int): EPSG
    """
    log = Logger('Vegetation Clip')

    with rasterio.open(existing_veg_path) as src:
        meta_existing = src.meta
        log.info('Got existing veg meta: {}'.format(meta_existing['transform']))

    with rasterio.open(historic_veg_path) as src:
        meta_hist = src.meta
        log.info('Got historic veg meta: {}'.format(meta_hist['transform']))

    # 0.01% cell size difference
    # If it's smaller than this we can force it pretty comfortably
    tolerance = 0.0001

    if (meta_existing['transform'][0] != meta_hist['transform'][0]):
        # Sometimes the national rasters have minor projection problems. We tolerate up to a single pixel mismatch
        msg = 'Vegetation raster cell widths do not match: existing {}, historic {}'.format(meta_existing['transform'][0], meta_hist['transform'][0])
        if abs((meta_existing['transform'][0] - meta_hist['transform'][0]) / meta_hist['transform'][0]) > tolerance:
            raise Exception(msg)
        else:
            log.warning(msg)

    if (meta_existing['transform'][4] != meta_hist['transform'][4]):
        # Sometimes the national rasters have minor projection problems. We tolerate up to a single pixel mismatch
        msg = 'Vegetation raster cell heights do not match: existing {}, historic {}'.format(meta_existing['transform'][4], meta_hist['transform'][4])
        if abs((meta_existing['transform'][4] - meta_hist['transform'][4]) / meta_hist['transform'][4]) > tolerance:
            raise Exception(msg)
        else:
            log.warning(msg)

    # The Rasters should generally line up but just in case they don't we force the outputs
    # to both match the existing raster cell sizes
    # https://gdal.org/python/osgeo.gdal-module.html#WarpOptions
    warp_options = {
        "xRes": meta_existing['transform'][0],
        "yRes": meta_existing['transform'][4],
        "targetAlignedPixels": True,
        "cutlineBlend": 2
    }
    # Now do the raster warp
    raster_warp(existing_veg_path, existing_clip_path, output_epsg, clip=boundary_path, warp_options=warp_options)
    raster_warp(historic_veg_path, historic_clip_path, output_epsg, clip=boundary_path, warp_options=warp_options)

    log.info('Complete')
