"""Functions to attribute DGO polygons with attributes related to land cover types and land use intensity within the riverscape.

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

from rsxml import Logger, dotenv
from rscommons import VectorBase
from rscommons.database import SQLiteCon


def dgo_vegetation(raster: str, dgo: dict, out_gpkg_path: str):
    """Summarizes vegetation raster datasets onto IGOs based on moving windows

    Arguments:
        raster (str): Path to raster dataset to summarize
        dgo (dict): A dictionary where key = DGOID and val = shapely geometry of dgo with large rivers cut out
        out_gpkg_path: Path to the geopackage containing the tables to fill out
    """

    log = Logger('DGO Vegetation')
    log.info(
        f'Summarizing vegetation raster {os.path.basename(raster)} within each DGO')

    dataset = gdal.Open(raster)
    geo_transform = dataset.GetGeoTransform()

    conversion_factor = VectorBase.rough_convert_metres_to_raster_units(
        raster, 1.0)
    cell_area = abs(geo_transform[1] * geo_transform[5]) / conversion_factor**2

    with rasterio.open(raster) as src:

        veg_counts = []
        for dgoid, dgo_geom in dgo.items():
            try:
                raw_raster = mask(src, [dgo_geom], crop=True)[0]
                mask_raster = np.ma.masked_values(raw_raster, src.nodata)

                for oldvalue in np.unique(mask_raster):
                    if oldvalue is not np.ma.masked:
                        cell_count = np.count_nonzero(mask_raster == oldvalue)
                        veg_counts.append(
                            [dgoid, int(oldvalue), cell_count * cell_area, cell_count])
            except Exception as ex:
                log.warning(
                    f'Error obtaining land cover raster values for dgo ID {dgoid}')
                log.warning(ex)

    with SQLiteCon(out_gpkg_path) as database:
        errs = 0
        batch_count = 0
        for veg_record in veg_counts:
            batch_count += 1
            if int(veg_record[1]) != -9999:
                try:
                    if os.path.basename(raster) in ['existing_veg.tif', 'historic_veg.tif']:
                        database.conn.execute(
                            'INSERT INTO DGOVegetation (DGOID, VegetationID, Area, CellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'ex_riparian.tif':
                        database.conn.execute(
                            'INSERT INTO DGOExRiparian (DGOID, ExRipVal, ExRipArea, ExRipCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'hist_riparian.tif':
                        database.conn.execute(
                            'INSERT INTO DGOHRiparian (DGOID, HRipVal, HRipArea, HRipCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'ex_vegetated.tif':
                        database.conn.execute(
                            'INSERT INTO DGOExVeg (DGOID, ExVegVal, ExVegArea, ExVegCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'hist_vegetated.tif':
                        database.conn.execute(
                            'INSERT INTO DGOHVeg (DGOID, HVegVal, HVegArea, HVegCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'conversion.tif':
                        database.conn.execute(
                            'INSERT INTO DGOConv (DGOID, ConvVal, ConvArea, ConvCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(raster) == 'fp_access.tif':
                        database.conn.execute(
                            'INSERT INTO DGOFPAccess (DGOID, AccessVal, CellArea, CellCount) VALUES (?, ?, ?, ?)', veg_record)
                except sqlite3.IntegrityError as err:
                    log.debug(str(err))
                    # TThis is likely a constraint error.
                    errstr = f"Integrity Error when inserting records: DGOID: {veg_record[0]} VegetationID: {veg_record[1]}"
                    log.error(errstr)
                    errs += 1
                except sqlite3.Error as err:
                    # This is any other kind of error
                    errstr = f"SQL Error when inserting records: IGOID: {veg_record[0]} VegetationID: {veg_record[1]} ERROR: {str(err)}"
                    log.error(errstr)
                    errs += 1
        if errs > 0:
            raise Exception(
                'Errors were found inserting records into the database. Cannot continue.')
        database.conn.commit()

    log.info('DGO vegetation summary complete')


def main():
    """DGO Vegetation 
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'raster', help='Raster dataset to summarize within windows', type=str)
    parser.add_argument(
        'dgo', help='A dictionary where key = DGOID and val = shapely geometry of dgo with large rivers cut out', type=dict)
    parser.add_argument(
        'out_gpkg_path', help='The database (geopackage) containing the tables to fill out', type=str)

    args = dotenv.parse_args_env(parser)

    dgo_vegetation(args.raster, args.dgo, args.out_gpgk_path)


if __name__ == '__main__':
    main()
