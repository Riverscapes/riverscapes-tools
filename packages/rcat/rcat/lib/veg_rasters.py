"""
"""
import rasterio
import sqlite3
import numpy as np
import json
import os

from rscommons import Logger


def rcat_rasters(existing_veg, historic_veg, database, out_folder):

    log = Logger('RCAT Vegetation Rasters')
    log.info('Generating RCAT vegetation rasters')

    exriparian_vals = {}
    exvegetated_vals = {}
    exconv_vals = {}
    hriparian_vals = {}
    hvegetated_vals = {}
    hconv_vals = {}

    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute('SELECT VT.VegetationID, C.Riparian, C.Vegetated, C.ConversionVal'
                 ' FROM VegetationTypes VT'
                 ' INNER JOIN VegClassification C ON VT.Physiognomy = C.Physiognomy'
                 ' WHERE VT.EpochID = 1')
    for row in curs.fetchall():
        exriparian_vals[row[0]] = row[1]
        exvegetated_vals[row[0]] = row[2]
        exconv_vals[row[0]] = row[3]
    curs.execute('SELECT VT.VegetationID, C.Riparian, C.Vegetated, C.ConversionVal'
                 ' FROM VegetationTypes VT'
                 ' INNER JOIN VegClassification C ON VT.Physiognomy = C.Physiognomy'
                 ' WHERE VT.EpochID = 2')
    for row in curs.fetchall():
        hriparian_vals[row[0]] = row[1]
        hvegetated_vals[row[0]] = row[2]
        hconv_vals[row[0]] = row[3]

    with rasterio.open(existing_veg) as ex, rasterio.open(historic_veg) as hist:
        meta = ex.profile
        ndval = ex.nodata
        exveg_arr = ex.read()[0, :, :]
        histveg_arr = hist.read()[0, :, :]

        ex_riparian_arr = np.full(exveg_arr.shape, ndval)
        hist_riparian_arr = np.full(histveg_arr.shape, ndval)
        ex_vegetated_arr = np.full(exveg_arr.shape, ndval)
        hist_vegetated_arr = np.full(histveg_arr.shape, ndval)
        conv_arr = np.full(exveg_arr.shape, ndval)

        for j in range(exveg_arr.shape[0]):
            for i in range(exveg_arr.shape[1]):
                if exveg_arr[j, i] != ndval:
                    ex_riparian_arr[j, i] = int(exriparian_vals[exveg_arr[j, i]])
                    hist_riparian_arr[j, i] = int(hriparian_vals[histveg_arr[j, i]])
                    ex_vegetated_arr[j, i] = int(exvegetated_vals[exveg_arr[j, i]])
                    hist_vegetated_arr[j, i] = int(hvegetated_vals[histveg_arr[j, i]])
                    conv_arr[j, i] = hconv_vals[histveg_arr[j, i]] - exconv_vals[exveg_arr[j, i]]

    out_arrays = [ex_riparian_arr, hist_riparian_arr, ex_vegetated_arr, hist_vegetated_arr, conv_arr]
    out_raster_paths = [os.path.join(out_folder, 'ex_riparian.tif'), os.path.join(out_folder, 'hist_riparian.tif'),
                        os.path.join(out_folder, 'ex_vegetated.tif'), os.path.join(out_folder, 'hist_vegetated.tif'),
                        os.path.join(out_folder, 'conversion.tif')]

    for i, array in enumerate(out_arrays):
        log.info(f'writing raster')
        with rasterio.open(out_raster_paths[i], 'w', **meta) as dst:
            dst.write(array, 1)
