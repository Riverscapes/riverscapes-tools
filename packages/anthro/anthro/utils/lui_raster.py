"""Generate a Land Use Intensity raster from Landfire existing vegetation

Jordan Gilbert

Dec 2022
"""
import rasterio
import sqlite3
import numpy as np

from rscommons import Logger


def lui_raster(existing_veg_raster, database, out_raster_path):

    log = Logger('Land Use')
    log.info('Generating Land Use Intensity raster')

    results = {}
    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute('SELECT VT.VegetationID, LU.Intensity'
                 ' FROM VegetationTypes VT'
                 ' INNER JOIN LandUses LU ON VT.Physiognomy = LU.Name')
    for row in curs.fetchall():
        results[row[0]] = row[1]

    with rasterio.open(existing_veg_raster) as src:
        meta = src.profile
        ndval = src.nodata
        veg_arr = src.read(1)

    out_array = np.full(veg_arr.shape, ndval)

    for j in range(veg_arr.shape[0]):
        for i in range(veg_arr.shape[1]):
            if veg_arr[j, i] != ndval and veg_arr[j, i] != -9999:
                out_array[j, i] = int(results[veg_arr[j, i]] * 100)

    with rasterio.open(out_raster_path, 'w', **meta) as dst:
        dst.write(out_array, 1)
