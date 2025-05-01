# Name:     Download DEM
#
# Purpose:  Identify all the NED 10m DEM rasters that intersect with a HUC
#           boundary polygon. Download and unzip all the rasters then mosaic
#           them into a single compressed GeoTIF raster possessing a specific
#           spatial reference.
#
# Author:   Philip Bailey
#
# Date:     15 Jun 2019
#
# -------------------------------------------------------------------------------
import argparse
import statistics
import sys
import math
import os
import traceback
from osgeo import gdal, ogr, osr
import rasterio
from rscommons.download import download_unzip, download_file
from rscommons.national_map import get_dem_urls, get_1m_dem_urls
from rscommons import Logger, Geotransform, ProgressBar, get_shp_or_gpkg
from rscommons.vector_ops import load_geometries


# NED data sometimes has small discrepencies in its cell widths. For this reason we need a tolerance,
# below which value we just treat everything as if its fine. This way we don't need to resample which can
# lead to stitching errors in gdal warp using VRT
# Same-ish values are usually different by 1e-9 and different-ish values are usually different by 9e-6
# So this should be somewhere in between. any more
CELL_SIZE_THRESH_STDDEV = 1e-13
# Also we need a number cell width difference. Above this difference we don't even bother and throw an exception
CELL_SIZE_MAX_STDDEV = 1e-8


def download_dem(vector_path, _epsg, buffer_dist, download_folder, unzip_folder, force_download=False, resolution='10m'):
    """
    Identify rasters within HUC, download them and mosaic into single GeoTIF
    :param vector_path: Path to bounding polygon ShapeFile
    :param epsg: Output spatial reference
    :param buffer_dist: Distance in DEGREES to buffer the bounding polygon
    :param unzip_folder: Temporary folder where downloaded rasters will be saved
    :param force_download: The download will always be performed if this is true.
    :return:
    """

    log = Logger('DEM')

    if resolution == '10m':
        # Query The National Map API for NED 10m DEM rasters within HUC 8 boundary
        urls = get_dem_urls(vector_path, buffer_dist)
    elif resolution == '1m':
        urls = get_1m_dem_urls(vector_path, buffer_dist)
    else:
        raise NotImplementedError("only implemented options are 1m or 10m DEM resolution")
    log.info('{} DEM raster(s) identified on The National Map.'.format(len(urls)))

    rasters = {}

    for url in urls:
        base_path = os.path.basename(os.path.splitext(url)[0])
        final_unzip_path = os.path.join(unzip_folder, base_path)
        if url.lower().endswith('.zip'):
            file_path = download_unzip(url, download_folder, final_unzip_path, force_download)
            raster_path = find_rasters(file_path)
        else:
            raster_path = download_file(url, download_folder, force_download)

        # Sanity check that all rasters going into the VRT share the same cell resolution.
        dataset = gdal.Open(raster_path)
        gt = dataset.GetGeoTransform()

        # Store the geotransform for later
        gtHelper = Geotransform(gt)

        # Create a tuple of useful numbers to use when trying to figure out if we have a problem with cellwidths
        rasters[raster_path] = gtHelper
        dataset = None

    if (len(rasters.keys()) == 0):
        raise Exception('No DEM urls were found')

    # Pick one result to compare with and loop over to see if all the rasters have the same, exact dimensions
    elif len(rasters.keys()) > 1:
        widthStdDev = statistics.stdev([gt.CellWidth() for gt in rasters.values()])
        heightStdDev = statistics.stdev([gt.CellHeight() for gt in rasters.values()])

        # This is the broad-strokes check. If the cell widths are vastly different then this won't work and you'll get stitching artifacts when you resample
        # so we bail out.
        if widthStdDev > CELL_SIZE_MAX_STDDEV or heightStdDev > CELL_SIZE_MAX_STDDEV:
            log.warning('Multiple DEM raster cells widths encountered.')
            for rp, gt in rasters.items():
                log.warning('cell width {} :: ({}, {})'.format(rp, gt.CellWidth(), gt.CellHeight()))
            # raise Exception('Cannot continue. Raster cell sizes are too different and resampling will cause edge effects in the stitched raster')

        # Now that we know we have a problem we need to figure out where the truth is:
        # if widthStdDev > CELL_SIZE_THRESH_STDDEV or heightStdDev > CELL_SIZE_THRESH_STDDEV:
        #     for rpath, gt in rasters.items():
        #         log.warning('Correcting Raster: {} from:({},{}) to:({},{})'.format(rpath, gt.CellWidth(), gt.CellHeight(), widthAvg, heightAvg))
        #         gt.SetCellWidth(widthAvg)
        #         gt.SetCellHeight(heightAvg)
        #         dataset = gdal.Open(raster_path)
        #         dataset.SetGeoTransform(gt.gt)
        #         dataset = None

    return list(rasters.keys()), urls


def find_rasters(search_dir: str) -> str:
    """
    Recursively search a folder for any *.img or *.tif rasters
    :param search_dir: Folder to be searched
    :return: full path to first raster found
    """

    for root, _subFolder, files in os.walk(search_dir):
        for item in files:
            if item.endswith('.img') or item.endswith('.tif'):
                return os.path.join(root, item)

    raise Exception('Failed to find IMG raster in folder {}'.format(search_dir))


def verify_areas(raster_path: str, boundary_shp: str):
    """check and compare the area of the raster vs the boundary

    Arguments:
        raster_path {str} -- path
        boundary_shp {str} -- path to boundary shapefile or geopackage layer

    Raises:
        Exception: [description] if raster area is zero
        Exception: [description] if shapefile area is zero

    Returns:
        [real] -- ratio of raster area over shape file area

    Caution - this will use degrees and square degrees as units of measure if inputs are in a geographic CRS
    """
    log = Logger('Verify Areas')

    log.info('Verifying raster and shape areas')

    # This comes back in the raster's unit
    raster_area = 0
    with rasterio.open(raster_path) as ds:
        cell_count = 0
        gt = ds.get_transform()
        cell_area = math.fabs(gt[1]) * math.fabs(gt[5])
        # Incrememntally add the area of a block to the count
        progbar = ProgressBar(len(list(ds.block_windows(1))), 50, "Calculating Area")
        progcount = 0
        for _ji, window in ds.block_windows(1):
            r = ds.read(1, window=window, masked=True)
            progbar.update(progcount)
            cell_count += r.count()
            progcount += 1

        progbar.finish()
        # Multiply the count by the area of a given cell
        raster_area = cell_area * cell_count
        log.debug('raster area {}'.format(raster_area))

    if (raster_area == 0):
        raise Exception('Raster has zero area: {}'.format(raster_path))

    # We could just use Rasterio's CRS object but it doesn't seem to play nice with GDAL so....
    raster_ds = gdal.Open(raster_path)
    raster_srs = osr.SpatialReference(wkt=raster_ds.GetProjection())
    log.debug(f'Raster Spatial Ref is {raster_srs.GetName()} and linear units are {raster_srs.GetLinearUnitsName()}')

    with get_shp_or_gpkg(boundary_shp) as layer:
        layer = layer.ogr_layer
        in_spatial_ref = layer.GetSpatialRef()

        # https://github.com/OSGeo/gdal/issues/1546
        raster_srs.SetAxisMappingStrategy(in_spatial_ref.GetAxisMappingStrategy())
        transform = osr.CoordinateTransformation(in_spatial_ref, raster_srs)

        shape_area = 0
        for polygon in layer:
            geom = polygon.GetGeometryRef()
            geom.Transform(transform)
            shape_area = shape_area + geom.GetArea()

    log.debug('shape file area {}'.format(shape_area))
    if (shape_area == 0):
        raise Exception('Shapefile has zero area: {}'.format(boundary_shp))

    area_ratio = raster_area / shape_area

    if (area_ratio < 0.99 and area_ratio > 0.9):
        log.warning('Raster Area covers only {0:.2f}% of the shapefile'.format(area_ratio * 100))
    if (area_ratio <= 0.9):
        log.error('Raster Area covers only {0:.2f}% of the shapefile'.format(area_ratio * 100))
    else:
        log.info('Raster Area covers {0:.2f}% of the shapefile'.format(area_ratio * 100))

    return area_ratio


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('vector', help='ShapeFile path to boundary polygon', type=argparse.FileType('r'))
    parser.add_argument('parent', help='Science Base GUID of parent item', type=str)
    parser.add_argument('epsg', help='EPSG spatial reference', type=int)
    parser.add_argument('folder', help='folder where DEM will be produced', type=str)
    args = parser.parse_args()

    try:
        download_dem(args.vector.name, args.epsg, 0.5, args.folder, os.path.join(args.folder, 'dem.tif'))

    except Exception as e:
        print(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
