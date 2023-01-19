"""Functions to attribute IGO points with attributes related to land cover types and land use intensity within the riverscape.

Jordan Gilbert

Dec 2022
"""
import sqlite3
import os
import rasterio
import numpy as np
from osgeo import gdal
from rasterio.mask import mask

from rscommons import Logger, VectorBase
from rscommons.database import SQLiteCon


def igo_vegetation(windows: dict, raster: str, out_gpkg_path: str):  # , large_rivers: dict):

    log = Logger('IGO Vegetation')
    log.info('Summarizing vegetation rasters for each IGO')

    dataset = gdal.Open(raster)
    geo_transform = dataset.GetGeoTransform()

    conversion_factor = VectorBase.rough_convert_metres_to_raster_units(raster, 1.0)
    cell_area = abs(geo_transform[1] * geo_transform[5]) / conversion_factor**2

    with rasterio.open(raster) as src:

        veg_counts = []
        for igoid, window in windows.items():
            try:
                raw_raster = mask(src, window, crop=True)[0]
                mask_raster = np.ma.masked_values(raw_raster, src.nodata)

                for oldvalue in np.unique(mask_raster):
                    if oldvalue is not np.ma.masked:
                        cell_count = np.count_nonzero(mask_raster == oldvalue)
                        # adjust cell count if necessary for large rivers
                        # if os.path.basename(raster) == 'ex_riparian.tif':
                        #     if oldvalue == 1:
                        #         if igoid in large_rivers['ex'].keys():
                        #            cell_count = cell_count - large_rivers['ex'][igoid]
                        # if os.path.basename(raster) == 'hist_riparian.tif':
                        #     if oldvalue == 1:
                        #         if igoid in large_rivers['hist'].keys():
                        #             cell_count = cell_count - large_rivers['hist'][igoid]

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

    log.info('IGO vegetation summaries complete')
