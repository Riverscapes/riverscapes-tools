"""Floodplain accessibility function

Jordan Gilbert

01/2023
"""

from rcat.lib.accessibility import access
import datetime
import argparse
from rscommons.hand import run_subprocess
from rscommons.util import safe_makedirs, safe_remove_dir
from rscommons import Logger, dotenv
from osgeo import gdal, ogr, osr
import numpy as np
import rasterio
import os


def flooplain_access(filled_dem: str, valley: str, reaches: str, intermediates_path: str, outraster: str, road: str = None, rail: str = None, canal: str = None):
    """ Generate a foodplain accessiblity raster where 0 is inaccessible and 1 is accessible

    Arguments:
        filled_dem (str): Path to a pit-filled DEM
        valley (str): Path to a valley bottom feature class
        reaches (str): Path to a stream network feature class
        intermediates_path (str): Path to RCAT project intermediates folder
        outraster (str): Path to store the output raster
        road (str): Path to a road feature class
        rail (str): Path to a railroad feature class
        canal (str): Path to a canal feature class
    """

    log = Logger('Floodplain accessibility')

    NCORES = os.environ['TAUDEM_CORES'] if 'TAUDEM_CORES' in os.environ else '2'

    # create temp folder to store vector layers
    safe_makedirs(os.path.join(intermediates_path, 'temp'))
    temp_dir = os.path.join(intermediates_path, 'temp')

    # buffer vector layers by dem resolution so that flow direction paths can't cross them diagonally
    dataset = gdal.Open(filled_dem)
    geo_transform = dataset.GetGeoTransform()
    cell_res = abs(geo_transform[1])
    srs = osr.SpatialReference()
    srs.ImportFromWkt(dataset.GetProjectionRef())

    vec_lyrs = [[road, 'road.shp'], [rail, 'rail.shp'], [canal, 'canal.shp']]
    vec_src = ogr.Open(os.path.join(os.path.dirname(intermediates_path), 'inputs/inputs.gpkg'))
    for vec in vec_lyrs:
        inlyr = vec_src.GetLayer(os.path.basename(vec[0]))
        if inlyr is not None:

            shpdriver = ogr.GetDriverByName('ESRI Shapefile')
            if os.path.exists(os.path.join(temp_dir, vec[1])):
                shpdriver.DeleteDataSource(os.path.join(temp_dir, vec[1]))
            out_buffer_ds = shpdriver.CreateDataSource(os.path.join(temp_dir, vec[1]))
            bufferlyr = out_buffer_ds.CreateLayer(os.path.basename(vec[0]), srs, geom_type=ogr.wkbPolygon)
            ftr_defn = inlyr.GetLayerDefn()

            for feature in inlyr:
                ingeom = feature.GetGeometryRef()
                geom_buffer = ingeom.Buffer(cell_res)
                out_feature = ogr.Feature(ftr_defn)
                out_feature.SetGeometry(geom_buffer)
                bufferlyr.CreateFeature(out_feature)
            out_buffer_ds = None
            bufferlyr = None

    # rasterize layers
    log.info('Rasterizing vector layers')
    inputs_ds = gdal.OpenEx(os.path.join(os.path.dirname(intermediates_path), 'inputs/inputs.gpkg'))
    chan_lyr = inputs_ds.GetLayer(os.path.basename(reaches))
    chan_lyr.SetAttributeFilter('ReachCode != 33600')
    road_ds = gdal.OpenEx(os.path.join(temp_dir, 'road.shp'))
    road_lyr = road_ds.GetLayer()
    rail_ds = gdal.OpenEx(os.path.join(temp_dir, 'rail.shp'))
    rail_lyr = rail_ds.GetLayer()
    canal_ds = gdal.OpenEx(os.path.join(temp_dir, 'canal.shp'))
    canal_lyr = canal_ds.GetLayer()
    # vb_ds = gdal.OpenEx(valley)
    vb_lyr = inputs_ds.GetLayer(os.path.basename(valley))

    drv_tiff = gdal.GetDriverByName('GTiff')

    channel_raster = os.path.join(intermediates_path, 'channel.tif')
    chan_ras = drv_tiff.Create(channel_raster, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16, options=['COMPRESS=LZW'])
    chan_ras.SetProjection(srs.ExportToWkt())
    chan_ras.SetGeoTransform(geo_transform)
    road_raster = os.path.join(intermediates_path, 'road.tif')
    road_ras = drv_tiff.Create(road_raster, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16, options=['COMPRESS=LZW'])
    road_ras.SetProjection(srs.ExportToWkt())
    road_ras.SetGeoTransform(geo_transform)
    rail_raster = os.path.join(intermediates_path, 'rail.tif')
    rail_ras = drv_tiff.Create(rail_raster, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16, options=['COMPRESS=LZW'])
    rail_ras.SetProjection(srs.ExportToWkt())
    rail_ras.SetGeoTransform(geo_transform)
    canal_raster = os.path.join(intermediates_path, 'canal.tif')
    canal_ras = drv_tiff.Create(canal_raster, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16, options=['COMPRESS=LZW'])
    canal_ras.SetProjection(srs.ExportToWkt())
    canal_ras.SetGeoTransform(geo_transform)
    vb_raster = os.path.join(intermediates_path, 'valley.tif')
    vb_ras = drv_tiff.Create(vb_raster, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16, options=['COMPRESS=LZW'])
    vb_ras.SetProjection(srs.ExportToWkt())
    vb_ras.SetGeoTransform(geo_transform)

    gdal.RasterizeLayer(chan_ras, [1], chan_lyr)
    chan_ras.GetRasterBand(1).SetNoDataValue(0.0)
    chan_ras = None
    gdal.RasterizeLayer(road_ras, [1], road_lyr)
    road_ras.GetRasterBand(1).SetNoDataValue(0.0)
    road_ras = None
    gdal.RasterizeLayer(rail_ras, [1], rail_lyr)
    rail_ras.GetRasterBand(1).SetNoDataValue(0.0)
    rail_ras = None
    gdal.RasterizeLayer(canal_ras, [1], canal_lyr)
    canal_ras.GetRasterBand(1).SetNoDataValue(0.0)
    canal_ras = None
    gdal.RasterizeLayer(vb_ras, [1], vb_lyr)
    vb_ras.GetRasterBand(1).SetNoDataValue(0.0)
    vb_ras = None

    # get d8 flow directions
    log.info('Generating flow directions')
    fd_path = os.path.join(intermediates_path, 'd8_flow_dir.tif')
    slp = os.path.join(intermediates_path, 'd8_slp.tif')
    d8flowdir_status = run_subprocess(intermediates_path, ["mpiexec", "-n", NCORES, "d8flowdir", "-fel", filled_dem, "-p", fd_path, "-sd8", slp])
    if d8flowdir_status != 0 or not os.path.isfile(fd_path):
        raise Exception('TauDEM: d8flowdir failed')

    # accessibility algorithm
    log.info('Performing floodplain accessibility analysis')
    with rasterio.open(fd_path) as src, rasterio.open(channel_raster) as chan, rasterio.open(road_raster) as road, \
            rasterio.open(rail_raster) as rail, rasterio.open(canal_raster) as canal, rasterio.open(vb_raster) as vb:
        transform = src.transform
        array = np.asarray(src.read()[0, :, :], dtype=np.int32)
        src_nd = np.int32(src.nodata)
        meta = src.profile
        chan_a = np.asarray(chan.read()[0, :, :], dtype=np.int32)
        chan_nd = np.int32(chan.nodata)
        r_a = np.asarray(road.read()[0, :, :], dtype=np.int32)
        road_nd = np.int32(road.nodata)
        rr_a = np.asarray(rail.read()[0, :, :], dtype=np.int32)
        rail_nd = np.int32(rail.nodata)
        c_a = np.asarray(canal.read()[0, :, :], dtype=np.int32)
        canal_nd = np.int32(canal.nodata)
        vb_a = np.asarray(vb.read()[0, :, :], dtype=np.int32)
        vb_nd = np.int32(vb.nodata)

    st = datetime.datetime.now()
    out = access.access_algorithm(array, src_nd, chan_a, chan_nd, r_a, road_nd, rr_a, rail_nd, c_a, canal_nd, vb_a, vb_nd)

    end = datetime.datetime.now()
    print(f'ellapsed: {end-st}')

    with rasterio.open(outraster, 'w', **meta) as outfile:
        outfile.write(out, 1)

    safe_remove_dir(temp_dir)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('pit_filled', help='', type=str)
    parser.add_argument('valley', help='', type=str)
    parser.add_argument('reaches', help='', type=str)
    parser.add_argument('intermediates_path', help='', type=str)
    parser.add_argument('out_raster', help='', type=str)
    parser.add_argument('road', help='', type=str)
    parser.add_argument('rail', help='', type=str)
    parser.add_argument('canal', help='', type=str)

    args = dotenv.parse_args_env(parser)

    flooplain_access(args.pit_filled, args.valley, args.reaches, args.intermediates_path, args.out_raster,
                     args.road, args.rail, args.canal)


if __name__ == '__main__':
    main()
