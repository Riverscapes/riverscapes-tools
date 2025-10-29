import os
from shutil import ExecError
import rasterio
import numpy as np
from rsxml import Logger
from rscommons.raster_warp import raster_warp


def clip_vegetation(boundary_path: str, veg_rasters: list, veg_raster_clips: list, output_epsg: int):
    """[summary]

    Args:
        boundary_path (str): Path to layer
        veg_rasters (list): List of paths to all landfire rasters
        veg_raster_clips (list): List of output paths for clipped vegetation rasters; these two lists must be in the same order
        output_epsg (int): EPSG
    """
    log = Logger('Vegetation Clip')

    widths = []
    heights = []

    if len(veg_rasters) != len(veg_raster_clips):
        msg = 'Number of vegetation rasters does not match number of output vegetation raster clips'
        raise ValueError(msg)

    # https://gdal.org/python/osgeo.gdal-module.html#WarpOptions
    warp_options = {"cutlineBlend": 2}

    for i, rast in enumerate(veg_rasters):

        log.info(f'Clipping vegetation raster {i + 1} of {len(veg_rasters)}: {rast}')

        if not os.path.isfile(rast):
            msg = f'Raster {rast} does not exist'
            raise FileNotFoundError(msg)

        with rasterio.open(rast) as vegraster:
            meta = vegraster.meta
            widths.append(meta['transform'][0])
            heights.append(meta['transform'][4])

        # Now do the raster warp
        raster_warp(rast, veg_raster_clips[i], output_epsg, clip=boundary_path, warp_options=warp_options)

    if len(np.unique(widths)) > 1:
        msg = 'One or more vegetation raster cell widths do not match'
        raise Exception(msg)
    if len(np.unique(heights)) > 1:
        msg = 'One or more vegetation raster cell heights do not match'
        raise Exception(msg)

    log.info('Complete')
