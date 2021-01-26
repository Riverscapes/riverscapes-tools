import rasterio
from rscommons import Logger
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

    with rasterio.open(existing_veg_path) as exist, rasterio.open(historic_veg_path) as hist:
        meta_existing = exist.meta
        meta_hist = hist.meta

        if meta_existing['transform'][0] != meta_hist['transform'][0]:
            msg = 'Vegetation raster cell widths do not match: existing {}, historic {}'.format(meta_existing['transform'][0], meta_hist['transform'][0])
            raise Exception(msg)

        if meta_existing['transform'][4] != meta_hist['transform'][4]:
            msg = 'Vegetation raster cell heights do not match: existing {}, historic {}'.format(meta_existing['transform'][4], meta_hist['transform'][4])
            raise Exception(msg)

    # https://gdal.org/python/osgeo.gdal-module.html#WarpOptions
    warp_options = {"cutlineBlend": 2}
    # Now do the raster warp
    raster_warp(existing_veg_path, existing_clip_path, output_epsg, clip=boundary_path, warp_options=warp_options)
    raster_warp(historic_veg_path, historic_clip_path, output_epsg, clip=boundary_path, warp_options=warp_options)

    log.info('Complete')
