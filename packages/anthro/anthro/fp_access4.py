import rasterio
from osgeo import gdal
from rscommons import get_shp_or_gpkg
from rscommons.vector_ops import get_geometry_unary_union, VectorBase
from shapely.geometry import MultiLineString
import numpy as np
import datetime

fd_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/flowdirection.tif'
vb_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/valley_bottom.shp'
channel_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/flowlines.shp'
road_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/roads_p.shp'
rail_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/rails_p.shp'
canal_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/canals_p.shp'
channel_raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/fl_rast.tif'
road_raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/road_rast.tif'
rail_raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/rail_rast.tif'
canal_raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/canal_rast.tif'
vb_raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/vb_rast.tif'
out_raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/fpaccess.tif'

raster_ds = gdal.Open(fd_path)
geot = raster_ds.GetGeoTransform()

# buffer datasets by 1/2 raster resolution and save to temp folder

chan_ds = gdal.OpenEx(channel_path)
chan_lyr = chan_ds.GetLayer()
road_ds = gdal.OpenEx(road_path)
road_lyr = road_ds.GetLayer()
rail_ds = gdal.OpenEx(rail_path)
rail_lyr = rail_ds.GetLayer()
canal_ds = gdal.OpenEx(canal_path)
canal_lyr = canal_ds.GetLayer()
vb_ds = gdal.OpenEx(vb_path)
vb_lyr = vb_ds.GetLayer()

drv_tiff = gdal.GetDriverByName('GTiff')

chan_ras = drv_tiff.Create(channel_raster, raster_ds.RasterXSize, raster_ds.RasterYSize, 1, gdal.GDT_Int16)
chan_ras.SetGeoTransform(geot)
road_ras = drv_tiff.Create(road_raster, raster_ds.RasterXSize, raster_ds.RasterYSize, 1, gdal.GDT_Int16)
road_ras.SetGeoTransform(geot)
rail_ras = drv_tiff.Create(rail_raster, raster_ds.RasterXSize, raster_ds.RasterYSize, 1, gdal.GDT_Int16)
rail_ras.SetGeoTransform(geot)
canal_ras = drv_tiff.Create(canal_raster, raster_ds.RasterXSize, raster_ds.RasterYSize, 1, gdal.GDT_Int16)
canal_ras.SetGeoTransform(geot)
vb_ras = drv_tiff.Create(vb_raster, raster_ds.RasterXSize, raster_ds.RasterYSize, 1, gdal.GDT_Int16)
vb_ras.SetGeoTransform(geot)

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

with rasterio.open(out_raster, 'w', **meta) as outfile:
    outfile.write(out_array, 1)
