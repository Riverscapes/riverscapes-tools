import os
import shutil
from rsxml import Logger
from rscommons.download_dem import download_dem
from rscommons.geographic_raster import gdal_dem_geographic
from rscommons.raster_warp import raster_vrt_stitch


def build_topography(boundary, epsg, download_folder, unzip_folder, dem, slope=None, hillshade=None, clean_downloads=False, clean_intermediates=False):

    log = Logger('Build Topo')

    dem_parts = []
    if not os.path.isfile(dem) or slope and not os.path.isfile(slope) or hillshade and not os.path.isfile(hillshade):
        dem_parts = download_dem(boundary, epsg, 0.01, download_folder, unzip_folder)

    try:
        if not os.path.isfile(dem):
            raster_vrt_stitch(dem_parts, dem, epsg, boundary)

        if slope:
            build_derived_raster(boundary, epsg, dem_parts, slope, 'slope')

        if hillshade:
            build_derived_raster(boundary, epsg, dem_parts, hillshade, 'hillshade')
    except Exception as e:
        log.error('Error building topography.')

    if clean_intermediates:
        try:
            [shutil.rmtree(os.path.dirname(temp)) for temp in dem_parts]
        except Exception as e:
            log.error('Error cleaning topography intermediate files.')


def build_derived_raster(boundary, epsg, dem_parts, output_path, gdal_process):

    log = Logger('Topography')

    if os.path.isfile(output_path):
        log.info('Skipping building {} raster because file exists.'.format(output_path))

    log.info('Building {} using {} parts.'.format(gdal_process, len(dem_parts)))

    parts = []
    for dem in dem_parts:
        temp = os.path.join(os.path.dirname(dem), 'slope.tif')
        gdal_dem_geographic(dem, temp, gdal_process)
        parts.append(temp)

    raster_vrt_stitch(parts, output_path, epsg, boundary)
