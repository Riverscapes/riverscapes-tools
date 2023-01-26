import os
import numpy as np
from osgeo import gdal
import rasterio
import sqlite3
from rasterio.mask import mask
from rscommons import Logger
from rscommons.database import SQLiteCon
from rscommons.classes.vector_base import VectorBase


def vegetation_summary(outputs_gpkg_path: str, reach_dgos: str, veg_raster: str, flowarea: str = None, waterbody: str = None):
    """ Loop through every reach in a BRAT database and
    retrieve the values from a vegetation raster within
    the specified buffer. Then store the tally of
    vegetation values in the BRAT database.
    Arguments:
        database {str} -- Path to BRAT database
        veg_raster {str} -- Path to vegetation raster
        buffer {float} -- Distance to buffer the reach polylines
    """

    log = Logger('Reach Vegetation')
    log.info('Summarizing vegetation rasters for each reach')

    # Retrieve the raster spatial reference and geotransformation
    dataset = gdal.Open(veg_raster)
    geo_transform = dataset.GetGeoTransform()
    # raster_buffer = VectorBase.rough_convert_metres_to_raster_units(veg_raster, 100)

    # Calculate the area of each raster cell in square metres
    conversion_factor = VectorBase.rough_convert_metres_to_raster_units(veg_raster, 1.0)
    cell_area = abs(geo_transform[1] * geo_transform[5]) / conversion_factor**2

    # Open the raster and then loop over all polyline features
    with rasterio.open(veg_raster) as src:
        veg_counts = []

        for reach_id, poly in reach_dgos.items():
            try:
                # retrieve an array for the cells under the polygon
                raw_raster = mask(src, [poly], crop=True)[0]
                mask_raster = np.ma.masked_values(raw_raster, src.nodata)
                # print(mask_raster)

                # Reclass the raster to dam suitability. Loop over present values for performance
                for oldvalue in np.unique(mask_raster):
                    if oldvalue is not np.ma.masked:
                        cell_count = np.count_nonzero(mask_raster == oldvalue)
                        veg_counts.append([reach_id, int(oldvalue), cell_count * cell_area, cell_count])
            except Exception as ex:
                log.warning('Error obtaining vegetation raster values for ReachID {}'.format(reach_id))
                log.warning(ex)

    with SQLiteCon(outputs_gpkg_path) as database:
        errs = 0
        batch_count = 0
        for veg_record in veg_counts:
            batch_count += 1
            if int(veg_record[1]) != -9999:
                try:
                    if os.path.basename(veg_raster) in ['existing_veg.tif', 'historic_veg.tif']:
                        database.conn.execute('INSERT INTO ReachVegetation (ReachID, VegetationID, Area, CellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(veg_raster) == 'ex_riparian.tif':
                        database.conn.execute('INSERT INTO ReachExRiparian (ReachID, ExRipVal, ExRipArea, ExRipCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(veg_raster) == 'hist_riparian.tif':
                        database.conn.execute('INSERT INTO ReachHRiparian (ReachID, HRipVal, HRipArea, HRipCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(veg_raster) == 'ex_vegetated.tif':
                        database.conn.execute('INSERT INTO ReachExVeg (ReachID, ExVegVal, ExVegArea, ExVegCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(veg_raster) == 'hist_vegetated.tif':
                        database.conn.execute('INSERT INTO ReachHVeg (ReachID, HVegVal, HVegArea, HVegCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(veg_raster) == 'conversion.tif':
                        database.conn.execute('INSERT INTO ReachConv (ReachID, ConvVal, ConvArea, ConvCellCount) VALUES (?, ?, ?, ?)', veg_record)
                    elif os.path.basename(veg_raster) == 'fp_access.tif':
                        database.conn.execute('INSERT INTO ReachFPAccess (ReachID, AccessVal, CellArea, CellCount) VALUES (?, ?, ?, ?)', veg_record)
                except sqlite3.IntegrityError as err:
                    # THis is likely a constraint error.
                    errstr = "Integrity Error when inserting records: ReachID: {} VegetationID: {}".format(veg_record[0], veg_record[1])
                    log.error(errstr)
                    errs += 1
                except sqlite3.Error as err:
                    # This is any other kind of error
                    errstr = "SQL Error when inserting records: ReachID: {} VegetationID: {} ERROR: {}".format(veg_record[0], veg_record[1], str(err))
                    log.error(errstr)
                    errs += 1
        if errs > 0:
            raise Exception('Errors were found inserting records into the database. Cannot continue.')
        database.conn.commit()

    log.info('Reach vegetation summary complete')
