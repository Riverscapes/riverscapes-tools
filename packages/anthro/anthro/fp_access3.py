import rasterio
from osgeo import gdal
from rscommons import get_shp_or_gpkg
from rscommons.vector_ops import get_geometry_unary_union, VectorBase
from shapely.geometry import MultiLineString
import numpy as np
import datetime

hand_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/dem.tif'
vb_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/valley_bottom.shp'
flowlines_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/flowlines.shp'
road_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/roads.shp'
rail_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/rails.shp'
canal_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/canals.shp'
flowline_raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/fl_rast.tif'
road_raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/road_rast.tif'
rail_raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/rail_rast.tif'
canal_raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/canal_rast.tif'
out_raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/fpaccess.tif'

# raster_ds = gdal.Open(hand_path)
# geot = raster_ds.GetGeoTransform()

# fl_ds = gdal.OpenEx(flowlines_path)
# fl_lyr = fl_ds.GetLayer()
# road_ds = gdal.OpenEx(road_path)
# road_lyr = road_ds.GetLayer()
# rail_ds = gdal.OpenEx(rail_path)
# rail_lyr = rail_ds.GetLayer()
# canal_ds = gdal.OpenEx(canal_path)
# canal_lyr = canal_ds.GetLayer()

# drv_tiff = gdal.GetDriverByName('GTiff')

# fl_ras = drv_tiff.Create(flowline_raster, raster_ds.RasterXSize, raster_ds.RasterYSize, 1, gdal.GDT_Int16)
# fl_ras.SetGeoTransform(geot)
# road_ras = drv_tiff.Create(road_raster, raster_ds.RasterXSize, raster_ds.RasterYSize, 1, gdal.GDT_Int16)
# road_ras.SetGeoTransform(geot)
# rail_ras = drv_tiff.Create(rail_raster, raster_ds.RasterXSize, raster_ds.RasterYSize, 1, gdal.GDT_Int16)
# rail_ras.SetGeoTransform(geot)
# canal_ras = drv_tiff.Create(canal_raster, raster_ds.RasterXSize, raster_ds.RasterYSize, 1, gdal.GDT_Int16)
# canal_ras.SetGeoTransform(geot)

# gdal.RasterizeLayer(fl_ras, [1], fl_lyr)
# fl_ras.GetRasterBand(1).SetNoDataValue(0.0)
# fl_ras = None
# gdal.RasterizeLayer(road_ras, [1], road_lyr)
# road_ras.GetRasterBand(1).SetNoDataValue(0.0)
# road_ras = None
# gdal.RasterizeLayer(rail_ras, [1], rail_lyr)
# rail_ras.GetRasterBand(1).SetNoDataValue(0.0)
# rail_ras = None
# gdal.RasterizeLayer(canal_ras, [1], canal_lyr)
# canal_ras.GetRasterBand(1).SetNoDataValue(0.0)
# canal_ras = None

with rasterio.open(hand_path) as src, rasterio.open(flowline_raster) as fl, rasterio.open(road_raster) as road, \
        rasterio.open(rail_raster) as rail, rasterio.open(canal_raster) as canal:
    transform = src.transform
    array = src.read()[0, :, :]
    src_nd = src.nodata
    meta = src.profile
    fl_a = fl.read()[0, :, :]
    fl_nd = fl.nodata
    r_a = road.read()[0, :, :]
    road_nd = road.nodata
    rr_a = rail.read()[0, :, :]
    rail_nd = rail.nodata
    c_a = canal.read()[0, :, :]
    canal_nd = canal.nodata

out_array = np.zeros(array.shape, dtype=np.int16)

# keep track to not repeat cells
processed = []

movements = {0: -1, 1: 0, 2: 1}

straight_dist = transform[0]  # these should actually be converted from degrees from transform
diag_dist = (straight_dist**2 + straight_dist**2)**0.5

st = datetime.datetime.now()
for row in range(1, array.shape[0] - 1):
    for col in range(1, array.shape[1] - 1):
        if array[row, col] == src_nd:
            continue
        if [row, col] not in processed:

            subprocessed = [[row, col]]

            next_cell = array[row, col]
            rowa, cola = row, col
            while next_cell is not None:
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
                if fl_a[rowa, cola] != fl_nd:
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
                    # need to fix so that when there's a no data value it defaults to a high gradient
                    gradients = np.array([[(array[rowa - 1, cola - 1] - array[rowa, cola]) / diag_dist, (array[rowa - 1, cola] - array[rowa, cola]) / straight_dist, (array[rowa - 1, cola + 1] - array[rowa, cola]) / diag_dist],
                                          [(array[rowa, cola - 1] - array[rowa, cola]) / straight_dist, 0, (array[rowa, cola + 1] - array[rowa, cola]) / straight_dist],
                                          [(array[rowa + 1, cola - 1] - array[rowa, cola]) / diag_dist, (array[rowa + 1, cola] - array[rowa, cola]) / straight_dist, (array[rowa + 1, cola + 1] - array[rowa, cola]) / diag_dist]])

                    # adjust gradient kernel for nodata
                    if array[rowa - 1, cola - 1] == src_nd:
                        gradients[0, 0] = 1000000
                    if array[rowa - 1, cola] == src_nd:
                        gradients[0, 1] = 1000000
                    if array[rowa - 1, cola + 1] == src_nd:
                        gradients[0, 2] = 1000000
                    if array[rowa, cola - 1] == src_nd:
                        gradients[1, 0] = 1000000
                    if array[rowa, cola + 1] == src_nd:
                        gradients[1, 2] = 1000000
                    if array[rowa + 1, cola - 1] == src_nd:
                        gradients[2, 0] = 1000000
                    if array[rowa + 1, cola] == src_nd:
                        gradients[2, 1] = 1000000
                    if array[rowa + 1, cola + 1] == src_nd:
                        gradients[2, 2] = 1000000

                    min = 1000000
                    move = None
                    for r in range(gradients.shape[0]):
                        for c in range(gradients.shape[1]):
                            if gradients[r, c] < min:
                                if [r, c] != [1, 1]:
                                    min = gradients[r, c]
                                    move = [r, c]
                    if move is None:
                        print('isolated cells')
                        for coord in subprocessed:
                            if coord not in processed:
                                processed.append(coord)
                        next_cell = None
                    else:
                        rowa = rowa + movements[move[0]]
                        cola = cola + movements[move[1]]
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

                    # if move is not None:
                    #     while [row + movements[move[0]], col + movements[move[1]]] in subprocessed:
                    #         print('rerouting circular flow path')
                    #         gradients[move[0], move[1]] = 1000000
                    #         min = 1000000
                    #         move = None
                    #         for r in range(gradients.shape[0]):
                    #             for c in range(gradients.shape[1]):
                    #                 if gradients[r, c] < min:
                    #                     if [r, c] != [1, 1]:
                    #                         min = gradients[r, c]
                    #                         move = [r, c]
                    #     row = row + movements[move[0]]
                    #     col = col + movements[move[1]]
                    #     subprocessed.append([row, col])
                    #     print(f'subprocessed {len(subprocessed)} cells')
                    #     next_cell = array[row, col]

                    # else:  # this is a case where circular flow was never resolved, set to 2?
                    #     for coord in subprocessed:
                    #         if coord not in processed:
                    #             out_array[coord[0], coord[1]] = 2
                    #             processed.append(coord)
                    #     next_cell = None
end = datetime.datetime.now()
print(f'ellapsed: {end-st}')

with rasterio.open(out_raster, 'w', **meta) as outfile:
    outfile.write(out_array, 1)
