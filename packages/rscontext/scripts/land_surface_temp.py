import os
import argparse
import glob
import datetime
import sqlite3
import json
import math
import csv
import sys
import traceback
import rasterio
from rasterio.mask import mask
from shapely.wkb import loads
from shapely.geometry import box, shape
import numpy as np
from osgeo import ogr
from rsxml import Logger, ProgressBar, dotenv
from rscommons.shapefile import export_geojson


def process_modis(out_sqlite, modis_folder, nhd_folder, verbose, debug_flag):
    """Generate land surface temperature sqlite db from NHD+ and MODIS data


    """

    log = Logger("Process LST")
    if os.path.isfile(out_sqlite):
        os.remove(out_sqlite)

    # Create sqlite database
    conn = sqlite3.connect(out_sqlite)
    cursor = conn.cursor()

    # test if table exists?
    cursor.execute("""SELECT COUNT(name) FROM sqlite_master WHERE type='table' AND name='MODIS_LST' """)
    log.info('Creating DB')
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
        CREATE TABLE MODIS_LST (
            NHDPlusID   INTEGER  NOT NULL,
            MODIS_Scene DATETIME NOT NULL,
            LST         REAL,
            PRIMARY KEY (
                NHDPlusID,
                MODIS_Scene
            )
        )
        WITHOUT ROWID;
        """)
    conn.commit()

    # populate list of modis files
    modis_files = glob.glob(os.path.join(modis_folder, "*.tif"))

    # Load NHD Layers
    log.info(f"Processing NHD Data: {nhd_folder}")
    in_driver = ogr.GetDriverByName("OpenFileGDB")
    in_datasource = in_driver.Open(nhd_folder, 0)
    layer_hucs = in_datasource.GetLayer(r"WBDHU8_reproject")

    # Process HUC
    huc_counter = 0
    total_hucs = layer_hucs.GetFeatureCount()
    for huc in layer_hucs:
        huc_counter += 1
        huc_id = huc.GetField(r"HUC8")

        log.info('Processing huc:{}  ({}/{})'.format(huc_id, huc_counter, total_hucs))
        log.info(f"HUC: {huc_id}")
        huc_geom = huc.GetGeometryRef()
        layer_catchments = None
        layer_catchments = in_datasource.GetLayer(r"NHDPlusCatchment_reproject")
        # layer_catchments.SetSpatialFilter(huc_geom) catchments not perfectly aligned with hucs
        layer_catchments.SetAttributeFilter(f"""HUC8 = {huc_id}""")
        huc_bounds = huc_geom.GetEnvelope()
        bbox = box(huc_bounds[0], huc_bounds[2], huc_bounds[1], huc_bounds[3])

        # open a single MODIS raster and load its projection and transform based on current huc
        with rasterio.open(f"{modis_files[0]}") as dataset:
            data, modis_transform = mask(dataset, [bbox], all_touched=True, crop=True)
            # Assuming there is only one band we can drop the first dimenson and get (36,78) instead of (1,36,78)
            modis_shape = data.shape[1:]

        # Read all MODIS Scences into array
        modis_array_raw = np.ma.array([load_cropped_raster(image, bbox) for image in modis_files])
        modis_array_sds = np.ma.masked_where(modis_array_raw == 0, modis_array_raw)

        # Make sure we mask out the invalid data
        modis_array_K = modis_array_sds * 0.02
        modis_array_C = modis_array_K - 273.15  # K to C

        # Generate list of MODIS scene dates
        modis_dates = np.array([os.path.basename(image).lstrip("A").rstrip(".tif") for image in modis_files])

        # Calcuate average LST per Catchemnt Layer
        progbar = ProgressBar(layer_catchments.GetFeatureCount(), 50, 'Processing HUC: {}'.format(huc_id))
        reach_counter = 0
        progbar.update(reach_counter)
        # loop_timer = LoopTimer("LoopTime", useMs=True)

        for reach in layer_catchments:
            reach_counter += 1
            progbar.update(reach_counter)

            # If debug flag is set then drop a CSV for every 5000 reaches
            debug_drop = debug_flag is True and reach_counter % 5000 == 1

            # For Debugging performance
            # loop_timer.tick()
            # loop_timer.progprint()
            nhd_id = int(reach.GetField("NHDPlusID"))

            # load_catchment_polygon and transform to raster SRS
            reach_geom = reach.GetGeometryRef()
            catch_poly = loads(reach_geom.ExportToWkb())

            # Catchment polygons are vectorized rasters and they can have invalid geometries
            if not catch_poly.is_valid:
                log.warning('Invalid catchment polygon detected. Trying the buffer technique: {}'.format(nhd_id))
                catch_poly = catch_poly.buffer(0)

            # Generate mask raster of catchment pixels
            reach_raster = np.ma.masked_invalid(
                rasterio.features.rasterize(
                    [catch_poly],
                    out_shape=modis_shape,
                    transform=modis_transform,
                    all_touched=True,
                    fill=np.nan
                )
            )
            # Now assign ascending integers to each cell. THis is so the rasterio.features.shapes gives us a unique shape for every cell
            reach_raster_idx = np.ma.masked_array(
                np.arange(modis_shape[0] * modis_shape[1], dtype=np.int32).reshape(modis_shape),
                # pylint: disable=E1101
                reach_raster.mask
            )

            # Generate a unique shape for each valid pixel
            geoms = [{
                'properties': {'name': 'modis_pixel', 'raster_val': int(v), 'valid': v > 0},
                'geometry': geom
            } for i, (geom, v) in enumerate(rasterio.features.shapes(reach_raster_idx, transform=modis_transform)) if test_pixel_geom(geom)]

            # Now create our weights array. Start with weights of 0 so we can rule out any weird points
            weights_raster_arr = np.ma.masked_array(
                np.full(modis_shape, 0, dtype=np.float32),
                # pylint: disable=E1101
                reach_raster.mask,
            )

            for geom in geoms:
                pxl = shape(geom['geometry'])
                poly_intersect = pxl.intersection(catch_poly)
                idx, idy = find_indeces(geom['properties']['raster_val'], modis_shape)
                weight = poly_intersect.area / catch_poly.area
                # For debugging
                if debug_drop:
                    geom['type'] = "Feature"
                    geom['properties']['weight'] = weight
                    geom['properties']['raster_coords'] = [idx, idy]
                    geom['properties']['world_coords'] = [pxl.centroid.coords[0][0], pxl.centroid.coords[0][1]]

                weights_raster_arr[idx][idy] = weight

            # Calculate average weighted modis
            ave = np.ma.average(modis_array_C, axis=(1, 2), weights=np.broadcast_to(weights_raster_arr, modis_array_C.shape))

            # Just some useful debugging stuff
            if debug_drop:
                progbar.erase()
                file_prefix = '{}-{}-debug'.format(huc_id, nhd_id)
                log.debug('Dropping files: {}'.format(file_prefix))
                # PrintArr(reach_raster_idx)
                # Dump some useful shapes to a geojson Object
                _debug_shape = DebugGeoJSON(os.path.join(os.path.dirname(out_sqlite), '{}.geojson'.format(file_prefix)))
                _debug_shape.add_shapely(bbox, {"name": "bbox"})
                _debug_shape.add_shapely(catch_poly, {"name": "catch_poly"})
                [_debug_shape.add_geojson(gj) for gj in geoms]
                _debug_shape.write()

                # Now dump an CSV array report for fun
                csv_file = os.path.join(os.path.dirname(out_sqlite), '{}.csv'.format(file_prefix))
                with open(csv_file, 'w') as csv_file:
                    csvw = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    csvw.writerow(['HUC', 'NHDPlusID', 'Area'])
                    csvw.writerow([huc_id, nhd_id, catch_poly.area])
                    csvw.writerow([])
                    debug_weights = []
                    # Summary of intersected pixels
                    for geom in geoms:
                        debug_weights.append((geom['properties']['weight'], geom['properties']['raster_coords']))
                    # Dump the weights Cell values so we can use excel to calculate them manually
                    # Write the average and the
                    csvw.writerow(['Intersecting Cells:'] + [' ' for g in geoms])
                    for key, name in {'raster_val': 'cell_id', 'raster_coords': '[row,col]', 'world_coords': '[x,y]', 'weight': 'weight'}.items():
                        csvw.writerow([name] + [g['properties'][key] for g in geoms])

                    csvw.writerow([])
                    csvw.writerow(['Date'] + [' ' for g in geoms] + ['np.ma.average'])
                    for didx, ave_val in enumerate(ave):
                        csvw.writerow([modis_dates[didx]] + [modis_array_sds[didx][w[1][0]][w[1][1]] for w in debug_weights] + [ave_val])

            # insert_lst_into_sqlite
            cursor.executemany("""INSERT INTO MODIS_LST VALUES(?,?,?)""", [(nhd_id, datetime.datetime.strptime(modis_date, "%Y%j").date(), float(v) if float(v) != 0 else None) for (modis_date, v) in zip(modis_dates, ave.data)])

            # Write data to sqlite after each reach
            conn.commit()

    # Close database connection
    conn.close()

    return


def load_cropped_raster(file, bbox):
    """load_cropped_raster 

    Args:
        file ([type]): Path to raster file
        bbox ([type]): shape to bound

    Returns:
        [type]: 2D numpy array representing the raster cropped to the bounding box
    """
    dataset = rasterio.open(file)
    out_raster, _out_transform = mask(dataset, [bbox], all_touched=True, crop=True)

    # We assume there's only one band so just return
    # a 2D raster to make numpy stuff easier later
    _raw_arr = np.array(out_raster[0]).astype("float")
    # Nodata values are set as 0.0 which is problematic so let's mask those
    return np.ma.masked_where(_raw_arr == 0, _raw_arr)


def find_indeces(rid, arr_shape):
    """[summary]

    Args:
        rid ([int]): unique incremental cell index
        arr_shape ([tuple]): 2D shape of the raster

    Returns:
        [tuple]: cell row and column tuple
    """
    xid = math.floor(rid / arr_shape[1])
    yid = math.floor(rid - (xid * arr_shape[1]))
    return (xid, yid)


def test_pixel_geom(geom):
    if len(geom['coordinates']) != 1:
        return False
    elif len(geom['coordinates'][0]) != 5:
        return False
    else:
        return True


class DebugGeoJSON:
    def __init__(self, filename):
        self.log = Logger(DebugGeoJSON)
        self._shape = export_geojson(None)
        self.filename = filename

    def add_shapely(self, new_shape, props=None):
        shp_shape = export_geojson(new_shape, props)
        self._shape["features"].append(shp_shape["features"][0])

    def add_geojson(self, new_shape):
        self._shape["features"].append(new_shape)

    def write(self):
        with open(self.filename, 'w') as f:
            json.dump(self._shape, f, indent=4)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('out_sqlite', help='output sqlite file', type=str)
    parser.add_argument('modis_folder', help='Top level data folder containing MODIS data', type=str)
    parser.add_argument('nhd_folder', help='Top level data folder containing nhd data', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) a little extra logging ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '.env'))

    # Initiate the log file
    log = Logger('Land Surface Temperature')
    log.setup(log_path=os.path.join(os.path.dirname(args.out_sqlite), os.path.splitext(args.out_sqlite)[0] + 'process_LST.log'), verbose=args.verbose)

    try:
        process_modis(args.out_sqlite, args.modis_folder, args.nhd_folder, args.verbose, args.debug)
        log.info('Process completed successfully')
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
