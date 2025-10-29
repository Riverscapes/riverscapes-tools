""" Summarizes vegetation for each polyline feature within a buffer distance
    on a raster. Inserts the area of each vegetation type into the BRAT database

   Philip Bailey
   28 Aug 2019
"""
import os
import sqlite3
import numpy as np
from osgeo import gdal, ogr
import rasterio
from shapely.ops import unary_union
from rasterio.mask import mask
from rsxml import Logger
from rscommons import GeopackageLayer
from rscommons.database import SQLiteCon
from rscommons.classes.vector_base import VectorBase


def vegetation_summary(outputs_gpkg_path: str, label: str, veg_raster: str, buffer: float, channel_polygons: str, save_polygons_path: str):
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
    log.info(f'Summarizing {int(buffer)}m vegetation buffer from {veg_raster}')

    # Retrieve the raster spatial reference and geotransformation
    dataset = gdal.Open(veg_raster)
    geo_transform = dataset.GetGeoTransform()
    raster_buffer = VectorBase.rough_convert_metres_to_raster_units(veg_raster, buffer)

    # Calculate the area of each raster cell in square metres
    conversion_factor = VectorBase.rough_convert_metres_to_raster_units(veg_raster, 1.0)
    cell_area = abs(geo_transform[1] * geo_transform[5]) / conversion_factor**2

    # Open the raster and then loop over all polyline features
    veg_counts = []
    polygons = {}
    with rasterio.open(veg_raster) as src, GeopackageLayer(os.path.join(outputs_gpkg_path, 'ReachGeometry')) as lyr, \
            GeopackageLayer(channel_polygons) as channel_lyr:
        _srs, transform = VectorBase.get_transform_from_raster(lyr.spatial_ref, veg_raster)
        spatial_ref = lyr.spatial_ref

        for feature, _counter, _progbar in lyr.iterate_features(label):
            reach_id = feature.GetFID()
            geom = feature.GetGeometryRef()
            if transform:
                geom.Transform(transform)
            chan_polys = []
            for chan_feature, *_ in channel_lyr.iterate_features(clip_shape=geom):
                chan_polys.append(VectorBase.ogr2shapely(chan_feature.GetGeometryRef()))
            if len(chan_polys) > 0:
                geom = unary_union(chan_polys)
            else:
                geom = VectorBase.ogr2shapely(geom)

            polygon = geom.buffer(raster_buffer)
            if geom.geom_type not in ('LineString', 'MultiLineString'):
                polygon = polygon.difference(geom)

            polygons[reach_id] = polygon

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
                log.warning(f'Error obtaining vegetation raster values for ReachID {reach_id}')
                log.warning(ex)

    # Write the reach vegetation values to the database
    # Because sqlite3 doesn't give us any feedback we do this in batches so that we can figure out what values
    # Are causing constraint errors
    with SQLiteCon(outputs_gpkg_path) as database:
        errs = 0
        batch_count = 0
        for veg_record in veg_counts:
            batch_count += 1
            if int(veg_record[1]) != -9999:
                try:
                    database.conn.execute('INSERT INTO ReachVegetation (ReachID, VegetationID, Buffer, Area, CellCount) VALUES (?, ?, ?, ?, ?)', veg_record)
                # Sqlite can't report on SQL errors so we have to print good log messages to help intuit what the problem is
                except sqlite3.IntegrityError as err:
                    log.debug(str(err))
                    # THis is likely a constraint error.
                    errstr = f"Integrity Error when inserting records: ReachID: {veg_record[0]} VegetationID: {veg_record[1]}"
                    log.error(errstr)
                    errs += 1
                except sqlite3.Error as err:
                    # This is any other kind of error
                    errstr = f"SQL Error when inserting records: ReachID: {veg_record[0]} VegetationID: {veg_record[1]} ERROR: {str(err)}"
                    log.error(errstr)
                    errs += 1
        if errs > 0:
            raise Exception('Errors were found inserting records into the database. Cannot continue.')
        database.conn.commit()

    if save_polygons_path:
        log.info(f'Saving Buffer Polygons to {save_polygons_path}')

        with GeopackageLayer(save_polygons_path, write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbPolygon, spatial_ref=spatial_ref, fields={'ReachID': ogr.OFTInteger})
            out_lyr.ogr_layer.StartTransaction()
            for rid, polygon in polygons.items():
                out_lyr.create_feature(polygon, {'ReachID': rid})
            out_lyr.ogr_layer.CommitTransaction()
    log.info('Vegetation summary complete')
