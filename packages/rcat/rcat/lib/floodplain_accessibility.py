from .accessibility import access
import datetime
from rscommons.hand import run_subprocess
from rscommons.util import safe_makedirs, safe_remove_dir
from rscommons import Logger
from osgeo import gdal, ogr
import numpy as np
import rasterio
import os


def flooplain_access(filled_dem: str, valley: str, reaches: str, road: str, rail: str, canal: str, intermediates_path: str, outraster: str):

    log = Logger('Floodplain accessibility')

    NCORES = os.environ['TAUDEM_CORES'] if 'TAUDEM_CORES' in os.environ else '2'

    # create temp folder to store vector layers
    safe_makedirs(os.path.join(intermediates_path, 'temp'))
    temp_dir = os.path.join(intermediates_path, 'temp')

    # buffer vector layers by dem resolution so that flow direction paths can't cross them diagonally
    dataset = gdal.Open(filled_dem)
    geo_transform = dataset.GetGeoTransform()
    cell_res = abs(geo_transform[1])

    vec_lyrs = [[road, 'road.shp'], [rail, 'rail.shp'], [canal, 'canal.shp']]
    vec_src = ogr.Open(os.path.dirname(vec_lyrs[0][0]))
    for vec in vec_lyrs:
        inlyr = vec_src.GetLayer(os.path.basename(vec[0]))

        shpdriver = ogr.GetDriverByName('ESRI Shapefile')
        if os.path.exists(os.path.join(temp_dir, vec[1])):
            shpdriver.DeleteDataSource(os.path.join(temp_dir, vec[1]))
        out_buffer_ds = shpdriver.CreateDataSource(os.path.join(temp_dir, vec[1]))
        bufferlyr = out_buffer_ds.CreateLayer(os.path.basename(vec[0]), geom_type=ogr.wkbPolygon)
        ftr_defn = bufferlyr.GetLayerDefn()

        for feature in inlyr:
            ingeom = feature.GetGeometryRef()
            geom_buffer = ingeom.Buffer(cell_res)
            out_feature = ogr.Feature(ftr_defn)
            out_feature.SetGeometry(geom_buffer)
            bufferlyr.CreateFeature(out_feature)

    # rasterize layers
    log.info('Rasterizing vector layers')
    inputs_ds = gdal.OpenEx(os.path.join(os.path.dirname(intermediates_path), 'inputs/inputs.gpkg'))
    chan_lyr = inputs_ds.GetLayer(os.path.basename(reaches))
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
    chan_ras = drv_tiff.Create(channel_raster, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16)
    chan_ras.SetGeoTransform(geo_transform)
    road_raster = os.path.join(intermediates_path, 'road.tif')
    road_ras = drv_tiff.Create(road_raster, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16)
    road_ras.SetGeoTransform(geo_transform)
    rail_raster = os.path.join(intermediates_path, 'rail.tif')
    rail_ras = drv_tiff.Create(rail_raster, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16)
    rail_ras.SetGeoTransform(geo_transform)
    canal_raster = os.path.join(intermediates_path, 'canal.tif')
    canal_ras = drv_tiff.Create(canal_raster, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16)
    canal_ras.SetGeoTransform(geo_transform)
    vb_raster = os.path.join(intermediates_path, 'valley.tif')
    vb_ras = drv_tiff.Create(vb_raster, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16)
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


# filled = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/inputs/pitfill.tif'
# vb = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/inputs/inputs.gpkg/valley_bottom'
# stream = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/inputs/inputs.gpkg/reaches'
# rd = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/inputs/inputs.gpkg/roads'
# rr = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/inputs/inputs.gpkg/rails'
# can = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/inputs/inputs.gpkg/canals'
# intspath = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/intermediates'
# outpath = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/intermediates/fpaccess_cython.tif'
# flooplain_access(filled, vb, stream, rd, rr, can, intspath, outpath)
