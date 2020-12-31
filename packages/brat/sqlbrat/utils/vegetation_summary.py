# Name:     Vegation Summary
#
# Purpose:  Summarizes vegetation for each polyline feature within a buffer distance
#           on a raster. Inserts the area of each vegetation type into the BRAT database
#
# Author:   Philip Bailey
#
# Date:     28 Aug 2019
# -------------------------------------------------------------------------------
import os
import numpy as np
from osgeo import gdal
import rasterio
from rasterio.mask import mask
from rscommons import GeopackageLayer, Logger
from rscommons.database import SQLiteCon
from rscommons.classes.vector_base import VectorBase


def vegetation_summary(outputs_gpkg_path, label, veg_raster, buffer):
    """ Loop through every reach in a BRAT database and
    retrieve the values from a vegetation raster within
    the specified buffer. Then store the tally of
    vegetation values in the BRAT database.

    Arguments:
        database {str} -- Path to BRAT database
        veg_raster {str} -- Path to vegetation raster
        buffer {float} -- Distance to buffer the reach polylines
    """

    log = Logger('Vegetation')
    log.info('Summarizing {}m vegetation buffer from {}'.format(int(buffer), veg_raster))

    # Retrieve the raster spatial reference and geotransformation
    dataset = gdal.Open(veg_raster)
    gt = dataset.GetGeoTransform()
    raster_buffer = VectorBase.rough_convert_metres_to_raster_units(veg_raster, buffer)

    # Calculate the area of each raster cell in square metres
    conversion_factor = VectorBase.rough_convert_metres_to_raster_units(veg_raster, 1.0)
    cell_area = abs(gt[1] * gt[5]) / conversion_factor**2

    # Open the raster and then loop over all polyline features
    veg_counts = []
    with rasterio.open(veg_raster) as src:

        with GeopackageLayer(os.path.join(outputs_gpkg_path, 'ReachGeometry')) as lyr:

            _srs, transform = lyr.get_transform_from_raster(veg_raster)

            for feature, _counter, _progbar in lyr.iterate_features(label):
                reach_id = feature.GetFID()
                geom = feature.GetGeometryRef()
                if transform:
                    geom.Transform(transform)

                polygon = VectorBase.ogr2shapely(geom).buffer(raster_buffer)

                try:
                    # retrieve an array for the cells under the polygon
                    raw_raster = mask(src, [polygon], crop=True)[0]
                    mask_raster = np.ma.masked_values(raw_raster, src.nodata)
                    # print(mask_raster)

                    # Reclass the raster to dam suitability. Loop over present values for performance
                    for oldvalue in np.unique(mask_raster):
                        if oldvalue is not np.ma.masked:
                            cell_count = np.count_nonzero(mask_raster == oldvalue)
                            veg_counts.append([reach_id, int(oldvalue), buffer, cell_count * cell_area, cell_count])
                except Exception as ex:
                    log.warning('Error obtaining vegetation raster values for ReachID {}'.format(reach_id))
                    log.warning(ex)

    # Write the reach vegetation values to the database
    with SQLiteCon(outputs_gpkg_path) as database:
        database.conn.executemany('INSERT INTO ReachVegetation (ReachID, VegetationID, Buffer, Area, CellCount) VALUES (?, ?, ?, ?, ?)', veg_counts)
        database.conn.commit()

    log.info('Vegetation summary complete')
