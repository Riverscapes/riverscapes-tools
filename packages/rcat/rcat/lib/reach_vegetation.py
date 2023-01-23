import os
import numpy as np
from osgeo import gdal, ogr
import rasterio
import sqlite3
from rasterio.mask import mask
from rscommons import GeopackageLayer, Logger, get_shp_or_gpkg
from rscommons.database import SQLiteCon
from rscommons.classes.vector_base import VectorBase
from rscommons.vector_ops import get_geometry_unary_union
from shapely.geometry.base import GEOMETRY_TYPES
from shapely.geometry import MultiPolygon
from shapely.ops import unary_union


def vegetation_summary(outputs_gpkg_path: str, dgo: str, veg_raster: str, flowarea: str = None, waterbody: str = None):
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
    raster_buffer = VectorBase.rough_convert_metres_to_raster_units(veg_raster, 100)

    # Calculate the area of each raster cell in square metres
    conversion_factor = VectorBase.rough_convert_metres_to_raster_units(veg_raster, 1.0)
    cell_area = abs(geo_transform[1] * geo_transform[5]) / conversion_factor**2

    # Open the raster and then loop over all polyline features
    polygons = {}
    veg_counts = []
    with rasterio.open(veg_raster) as src, GeopackageLayer(os.path.join(outputs_gpkg_path, 'ReachGeometry')) as lyr:
        _srs, transform = VectorBase.get_transform_from_raster(lyr.spatial_ref, veg_raster)
        # spatial_ref = lyr.spatial_ref

        for feature, _counter, _progbar in lyr.iterate_features():
            reach_id = feature.GetFID()
            geom = feature.GetGeometryRef()
            if transform:
                geom.Transform(transform)

            with get_shp_or_gpkg(dgo) as dgolyr:
                dgolyr.ogr_layer.SetSpatialFilter(feature.GetGeometryRef())
                if dgolyr.ogr_layer.GetFeatureCount() == 0:
                    log.info(f'feature {reach_id} has no associated DGOs, using 100m buffer')
                    polygon = VectorBase.ogr2shapely(geom).buffer(raster_buffer)
                elif dgolyr.ogr_layer.GetFeatureCount() == 1:
                    ftrs = [ftr for ftr in dgolyr.ogr_layer]
                    polygon = VectorBase.ogr2shapely(ftrs[0].GetGeometryRef())
                else:
                    polys = [VectorBase.ogr2shapely(ftr.GetGeometryRef()) for ftr in dgolyr.ogr_layer]
                    polygon = unary_union(polys)

                if flowarea:
                    polygon = polygon.difference(flowarea)
                if waterbody:
                    polygon = polygon.difference(waterbody)

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
