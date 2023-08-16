"""Functions to attribute IGO points with attributes related to land cover types and land use intensity within the riverscape.

Jordan Gilbert

Dec 2022
"""
import sqlite3
import rasterio
import numpy as np
from osgeo import gdal
from rasterio.mask import mask

from rscommons import Logger, VectorBase, GeopackageLayer
from rscommons.database import SQLiteCon


def igo_vegetation(windows: dict, landuse_raster: str, out_gpkg_path: str):

    log = Logger('IGO Land Use')
    log.info('Summarizing land cover classes for each IGO')

    dataset = gdal.Open(landuse_raster)
    geo_transform = dataset.GetGeoTransform()

    conversion_factor = VectorBase.rough_convert_metres_to_raster_units(landuse_raster, 1.0)
    cell_area = abs(geo_transform[1] * geo_transform[5]) / conversion_factor**2

    with rasterio.open(landuse_raster) as src, \
            GeopackageLayer(out_gpkg_path, 'DGOGeometry') as dgo_lyr:

        veg_counts = []
        # for igoid, window in windows.items():
        #     try:
        #         raw_raster = mask(src, window[0], crop=True)[0]
        #         mask_raster = np.ma.masked_values(raw_raster, src.nodata)

        #         for oldvalue in np.unique(mask_raster):
        #             if oldvalue is not np.ma.masked:
        #                 cell_count = np.count_nonzero(mask_raster == oldvalue)
        #                 veg_counts.append([igoid, int(oldvalue), cell_count * cell_area, cell_count])
        #     except Exception as ex:
        #         log.warning(f'Error obtaining land cover raster values for igo ID {igoid}')
        #         log.warning(ex)
        for dgo_ftr, *_ in dgo_lyr.iterate_features():
            dgoid = dgo_ftr.GetFID()
            dgo_ogr = dgo_ftr.GetGeometryRef()
            dgo_geom = VectorBase.ogr2shapely(dgo_ogr)
            try:
                raw_raster = mask(src, [dgo_geom], crop=True)[0]
                mask_raster = np.ma.masked_values(raw_raster, src.nodata)

                for oldvalue in np.unique(mask_raster):
                    if oldvalue is not np.ma.masked:
                        cell_count = np.count_nonzero(mask_raster == oldvalue)
                        veg_counts.append([dgoid, int(oldvalue), cell_count * cell_area, cell_count])
            except Exception as ex:
                log.warning(f'Error obtaining land cover raster values for DGO ID {dgoid}')
                log.warning(ex)

    with SQLiteCon(out_gpkg_path) as database:
        errs = 0
        batch_count = 0
        for veg_record in veg_counts:
            batch_count += 1
            if int(veg_record[1]) != -9999:
                try:
                    database.conn.execute('INSERT INTO DGOVegetation (DGOID, VegetationID, Area, CellCount) VALUES (?, ?, ?, ?)', veg_record)
                except sqlite3.IntegrityError as err:
                    # THis is likely a constraint error.
                    errstr = "Integrity Error when inserting records: DGOID: {} VegetationID: {}".format(veg_record[0], veg_record[1])
                    log.error(errstr)
                    errs += 1
                except sqlite3.Error as err:
                    # This is any other kind of error
                    errstr = "SQL Error when inserting records: DGOID: {} VegetationID: {} ERROR: {}".format(veg_record[0], veg_record[1], str(err))
                    log.error(errstr)
                    errs += 1
        if errs > 0:
            raise Exception('Errors were found inserting records into the database. Cannot continue.')
        database.conn.commit()

    log.info('IGO land use summary complete')
