"""Functions to attribute IGO points with attributes related to land cover types and land use intensity within the riverscape.

Jordan Gilbert

Dec 2022
"""
import argparse
import sqlite3
import os
import rasterio
import numpy as np
from osgeo import gdal
from rasterio.mask import mask

from rscommons import Logger, VectorBase, dotenv
from rscommons.database import SQLiteCon


def igo_vegetation(windows: dict, raster: str, out_gpkg_path: str):  # , large_rivers: dict):
    """Summarizes vegetation raster datasets onto IGOs based on moving windows

    Arguments:
        windows (dict): dictionary with moving window features associated with each IGO
        raster (str): Path to raster dataset to summarize
        out_gpkg_path: Path to the geopackage containing the tables to fill out
    """

    log = Logger('IGO Vegetation')
    log.info(f'Summarizing vegetation raster {os.path.basename(raster)} for each IGO')

    dataset = gdal.Open(raster)
    geo_transform = dataset.GetGeoTransform()

    conversion_factor = VectorBase.rough_convert_metres_to_raster_units(raster, 1.0)
    cell_area = abs(geo_transform[1] * geo_transform[5]) / conversion_factor**2

    with rasterio.open(raster) as src:

        veg_counts = []
        for igoid, window in windows.items():
            try:
                raw_raster = mask(src, [window], crop=True)[0]
                mask_raster = np.ma.masked_values(raw_raster, src.nodata)

                for oldvalue in np.unique(mask_raster):
                    if oldvalue is not np.ma.masked:
                        cell_count = np.count_nonzero(mask_raster == oldvalue)
                        veg_counts.append([igoid, int(oldvalue), cell_count * cell_area, cell_count])
            except Exception as ex:
                log.warning(f'Error obtaining land cover raster values for igo ID {igoid}')
                log.warning(ex)

    with SQLiteCon(out_gpkg_path) as database:
        errs = 0
        batch_count = 0
        for veg_record in veg_counts:
            batch_count += 1
            if int(veg_record[1]) != -9999:
                try:
                    if os.path.basename(raster) in ['existing_veg.tif', 'historic_veg.tif']:
                        database.conn.execute('INSERT INTO IGOVegetation (IGOID, VegetationID, Area, CellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'ex_riparian.tif':
                        database.conn.execute('INSERT INTO IGOExRiparian (IGOID, ExRipVal, ExRipArea, ExRipCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'hist_riparian.tif':
                        database.conn.execute('INSERT INTO IGOHRiparian (IGOID, HRipVal, HRipArea, HRipCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'ex_vegetated.tif':
                        database.conn.execute('INSERT INTO IGOExVeg (IGOID, ExVegVal, ExVegArea, ExVegCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'hist_vegetated.tif':
                        database.conn.execute('INSERT INTO IGOHVeg (IGOID, HVegVal, HVegArea, HVegCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'conversion.tif':
                        database.conn.execute('INSERT INTO IGOConv (IGOID, ConvVal, ConvArea, ConvCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'fp_access.tif':
                        database.conn.execute('INSERT INTO IGOFPAccess (IGOID, AccessVal, CellArea, CellCount) VALUES (?, ?, ?, ?)', veg_record)
                except sqlite3.IntegrityError as err:
                    # THis is likely a constraint error.
                    errstr = "Integrity Error when inserting records: IGOID: {} VegetationID: {}".format(veg_record[0], veg_record[1])
                    log.error(errstr)
                    errs += 1
                except sqlite3.Error as err:
                    # This is any other kind of error
                    errstr = "SQL Error when inserting records: IGOID: {} VegetationID: {} ERROR: {}".format(veg_record[0], veg_record[1], str(err))
                    log.error(errstr)
                    errs += 1
        if errs > 0:
            raise Exception('Errors were found inserting records into the database. Cannot continue.')
        database.conn.commit()

    log.info('IGO vegetation summary complete')


def main():
    """IGO Vegetation 
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('windows', help='dictionary of moving windows for each IGO', type=dict)
    parser.add_argument('raster', help='Raster dataset to summarize within windows', type=str)
    parser.add_argument('out_gpkg_path', help='The database (geopackage) containing the tables to fill out', type=str)

    args = dotenv.parse_args_env(parser)

    igo_vegetation(args.windows, args.raster, args.out_gpgk_path)


if __name__ == '__main__':
    main()
