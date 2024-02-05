"""Function to generate different vegetation rasters from landfire EVT and BPS

Jordan Gilbert

01/2023
"""

import rasterio
import argparse
import sqlite3
import numpy as np
import os

from rscommons import Logger, dotenv


def rcat_rasters(existing_veg: str, historic_veg: str, database: str, out_folder: str):
    """Generate intermediate vegetation rasters for RCAT analyses

    Arguments:
        existing_veg (str): Path to landfire EVT raster
        historic_veg (str): Path to landfire BpS raster
        database (str): Path to rcat output geopackage
        out_folder (str): Path to location to store output rasters
    """

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
                if exveg_arr[j, i] != ndval and exveg_arr[j, i] != -9999:
                    ex_riparian_arr[j, i] = int(exriparian_vals[exveg_arr[j, i]])
                    hist_riparian_arr[j, i] = int(hriparian_vals[histveg_arr[j, i]])
                    ex_vegetated_arr[j, i] = int(exvegetated_vals[exveg_arr[j, i]])
                    hist_vegetated_arr[j, i] = int(hvegetated_vals[histveg_arr[j, i]])
                    if hconv_vals[histveg_arr[j, i]] - exconv_vals[exveg_arr[j, i]] in (-80, -60, -50, 0, 50, 60, 80, 97, 98, 99, 100):
                        conv_arr[j, i] = hconv_vals[histveg_arr[j, i]] - exconv_vals[exveg_arr[j, i]]
                    else:
                        conv_arr[j, i] = 1000

    out_arrays = [ex_riparian_arr, hist_riparian_arr, ex_vegetated_arr, hist_vegetated_arr, conv_arr]
    out_raster_paths = [os.path.join(out_folder, 'ex_riparian.tif'), os.path.join(out_folder, 'hist_riparian.tif'),
                        os.path.join(out_folder, 'ex_vegetated.tif'), os.path.join(out_folder, 'hist_vegetated.tif'),
                        os.path.join(out_folder, 'conversion.tif')]

    for i, array in enumerate(out_arrays):
        log.info(f'writing raster')
        with rasterio.open(out_raster_paths[i], 'w', **meta) as dst:
            dst.write(array, 1)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('existing_veg', help='Path to landfire EVT raster', type=str)
    parser.add_argument('historic_veg', help='Path to landfire BpS raster', type=str)
    parser.add_argument('database', help='Path to rcat output geopackage', type=str)
    parser.add_argument('out_folder', help='Path to location to store output rasters', type=str)

    args = dotenv.parse_args_env(parser)

    rcat_rasters(args.existing_veg, args.historic_veg, args.database, args.out_folder)


if __name__ == '__main__':
    main()
