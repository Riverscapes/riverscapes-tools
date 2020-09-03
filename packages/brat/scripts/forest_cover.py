# -------------------------------------------------------------------------------
# Name:     Calculate Forest Cover
#
# Purpose:  Calculate the forest cover for every eight digit HUC in the continental
#           United States. Uses the national HUC8 watershed boundaries together with
#           a land cover raster. The results are stored in a SQLite database.
#
# Author:   Philip Bailey
#
# Date:     31 Oct 2019
#
# -------------------------------------------------------------------------------
import os
import json
import argparse
from osgeo import ogr
from osgeo import osr
import sqlite3
import gdal
import rasterio
import numpy as np
import csv
from shapely.geometry import shape, mapping
from hydro_parameters import forest_cover
from rasterio.mask import mask

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')

evt_csv = os.path.join(os.environ['SOME_PATH'], '/ExistingVegetation_AllEcoregions.csv')


def national_forest_cover(watersheds, land_cover, database):

    # Load CSV values
    forest_values = []
    with open(evt_csv, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['EVT_PHYS'] in ['Hardwood', 'Conifer', 'Conifer-Hardwood']:
                forest_values.append(int(row['Value']))

    print(len(forest_values), 'forest type values')

    # National HUC8 watershed boundary dataset
    driver = ogr.GetDriverByName("OpenFileGDB")
    datasource = driver.Open(watersheds, 0)
    layer = datasource.GetLayer('WBDHU8')
    feature_count = layer.GetFeatureCount()
    vector_srs = layer.GetSpatialRef()

    # National land cover raster
    raster_dataset = gdal.Open(land_cover)
    raster_srs = osr.SpatialReference(wkt=raster_dataset.GetProjection())

    # https://github.com/OSGeo/gdal/issues/1546
    raster_srs.SetAxisMappingStrategy(vector_srs.GetAxisMappingStrategy())

    transform = osr.CoordinateTransformation(vector_srs, raster_srs)
    raster_dataset = None

    # BRAT SQLite database
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    processing = 0
    with rasterio.open(land_cover) as src:
        for feature in layer:
            processing += 1
            huc = feature.GetField('HUC8')
            print('Processing {}, {:,} of {:,}'.format(huc, processing, feature_count))
            geom = feature.GetGeometryRef()
            geom.Transform(transform)
            polygon = shape(json.loads(geom.ExportToJson()))

            try:
                raw_raster = mask(src, [polygon], crop=True)[0]
                mask_raster = np.ma.masked_values(raw_raster, src.nodata)
                total_area = mask_raster.count()

                for oldvalue in np.unique(mask_raster):
                    if oldvalue not in forest_values:
                        np.ma.masked_where(mask_raster == oldvalue, mask_raster, copy=False)

                forest_area = mask_raster.count()
                forest_ratio = (float(forest_area) / float(total_area)) * 100.0 if forest_area > 0 else 0

                if forest_area > 0:
                    print('got one', forest_area)

                curs.execute('UPDATE HUCs SET ForestCover = ?, ForestCoverP1 = ? WHERE HUC = ?',
                             [forest_ratio, forest_ratio + 1, huc])
                conn.commit()
            except Exception as e:
                print('Error processing {}'.format(huc))

    print('Calculating forest cover complete.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('watersheds', help='National watershed boundary file geodatabase', type=str)
    parser.add_argument('land_cover', help='Path to national land cover raster', type=argparse.FileType('r'))
    parser.add_argument('database', help='Path to SQLite database', type=argparse.FileType('r'))
    args = parser.parse_args()

    national_forest_cover(args.watersheds, args.land_cover.name, args.database.name)


if __name__ == '__main__':
    main()
