import os
import rasterio
from osgeo import gdal
from rscommons import Logger, get_shp_or_gpkg
from rscommons.vector_ops import get_geometry_unary_union, copy_feature_class, VectorBase
from rscommons.util import safe_makedirs, safe_remove_dir
from rscommons.hand import run_subprocess
from shapely.geometry import MultiLineString
import numpy as np
import datetime


def flooplain_access(filled_dem: str, valley: str, reaches: str, road: str, rail: str, canal: str, intermediates_path: str, outraster: str):

    log = Logger('Floodplain accessibility')

    NCORES = os.environ['TAUDEM_CORES'] if 'TAUDEM_CORES' in os.environ else '2'

    # create temp folder to store vector layers
    safe_makedirs(os.path.join(intermediates_path, 'temp'))

    # buffer vector layers by dem resolution so that flowlines can't cross them
    dataset = gdal.Open(filled_dem)
    geo_transform = dataset.GetGeoTransform()
    cell_res = abs(geo_transform[1])

    vec_lyrs = [road, rail, canal]
    for vec in vec_lyrs:
        copy_feature_class(vec, os.path.join(intermediates_path, os.path.basename(vec)), buffer=cell_res * 0.5)

    # rasterize layers
    log.info('Rasterizing vector layers')
    chan_ds = gdal.OpenEx(reaches)
    chan_lyr = chan_ds.GetLayer()
    road_ds = gdal.OpenEx(road)
    road_lyr = road_ds.GetLayer()
    rail_ds = gdal.OpenEx(rail)
    rail_lyr = rail_ds.GetLayer()
    canal_ds = gdal.OpenEx(canal)
    canal_lyr = canal_ds.GetLayer()
    vb_ds = gdal.OpenEx(valley)
    vb_lyr = vb_ds.GetLayer()

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
        array = src.read()[0, :, :]
        src_nd = src.nodata
        meta = src.profile
        chan_a = chan.read()[0, :, :]
        chan_nd = chan.nodata
        r_a = road.read()[0, :, :]
        road_nd = road.nodata
        rr_a = rail.read()[0, :, :]
        rail_nd = rail.nodata
        c_a = canal.read()[0, :, :]
        canal_nd = canal.nodata
        vb_a = vb.read()[0, :, :]
        vb_nd = vb.nodata

    out_array = np.zeros(array.shape, dtype=np.int16)

    # keep track to not repeat cells
    processed = []

    movements = {0: -1, 1: 0, 2: 1}

    straight_dist = transform[0]  # these should actually be converted from degrees from transform
    diag_dist = (straight_dist**2 + straight_dist**2)**0.5

    st = datetime.datetime.now()
    for row in range(array.shape[0]):
        for col in range(array.shape[1]):
            if vb_a[row, col] == vb_nd:
                continue
            if [row, col] not in processed:

                subprocessed = [[row, col]]

                next_cell = array[row, col]
                rowa, cola = row, col
                while next_cell is not None:
                    if next_cell == src_nd:
                        for coord in subprocessed:
                            if coord not in processed:
                                processed.append(coord)
                        next_cell = None
                    if [rowa, cola] in processed:
                        for coord in subprocessed:
                            if coord not in processed:
                                out_array[coord[0], coord[1]] = out_array[rowa, cola]
                                processed.append(coord)
                        next_cell = None
                        print(f'{len(processed)} cells processed')
                    # if next_cell == 0:
                    #    for coord in subprocessed:
                    #        if coord not in processed:
                    #            out_array[coord[0], coord[1]] = 1
                    #            processed.append(coord)
                    #    next_cell = None
                    #    print(f'{len(processed)} cells processed')
                    if chan_a[rowa, cola] != chan_nd:
                        for coord in subprocessed:
                            if coord not in processed:
                                out_array[coord[0], coord[1]] = 1
                                processed.append(coord)
                        next_cell = None
                        print(f'{len(processed)} cells processed')
                    if r_a[rowa, cola] != road_nd:
                        for coord in subprocessed:
                            if coord not in processed:
                                processed.append(coord)
                        next_cell = None
                        print(f'{len(processed)} cells processed')
                    if rr_a[rowa, cola] != rail_nd:
                        for coord in subprocessed:
                            if coord not in processed:
                                processed.append(coord)
                        next_cell = None
                        print(f'{len(processed)} cells processed')
                    if c_a[rowa, cola] != canal_nd:
                        for coord in subprocessed:
                            if coord not in processed:
                                processed.append(coord)
                        next_cell = None
                        print(f'{len(processed)} cells processed')

                    if next_cell is not None:
                        if next_cell == 1:
                            rowa = rowa
                            cola = cola + 1
                        elif next_cell == 2:
                            rowa = rowa - 1
                            cola = cola + 1
                        elif next_cell == 3:
                            rowa = rowa - 1
                            cola = cola
                        elif next_cell == 4:
                            rowa = rowa - 1
                            cola = cola - 1
                        elif next_cell == 5:
                            rowa = rowa
                            cola = cola - 1
                        elif next_cell == 6:
                            rowa = rowa + 1
                            cola = cola - 1
                        elif next_cell == 7:
                            rowa = rowa + 1
                            cola = cola
                        elif next_cell == 8:
                            rowa = rowa + 1
                            cola = cola + 1
                        if [rowa, cola] in subprocessed:
                            print('circular flow path, could not resolve connectivity')
                            for coord in subprocessed:
                                if coord not in processed:
                                    out_array[coord[0], coord[1]] = 2
                                    processed.append(coord)
                            next_cell = None
                        else:
                            subprocessed.append([rowa, cola])
                            print(f'subprocessed {len(subprocessed)} cells')
                            next_cell = array[rowa, cola]

    end = datetime.datetime.now()
    print(f'ellapsed: {end-st}')

    with rasterio.open(outraster, 'w', **meta) as outfile:
        outfile.write(out_array, 1)
