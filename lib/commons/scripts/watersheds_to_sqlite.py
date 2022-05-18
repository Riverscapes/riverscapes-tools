"""
Philip Bailey - 17 May 2022
Script to export nationwide HUC8 watersheds, their names and geometries to a SQLite
database for use in cleaning warehouse XML."""
import os
import json
import sqlite3
from osgeo import ogr
from osgeo import osr


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


watersheds = '/Users/philip/GISData/watershed_boundaries/watershed_boundaries_dataset.gpkg'
output = '/Users/philip/GISData/watershed_boundaries/watersheds_for_riverscapes.sqlite'


create_table = os.path.isfile(output)
rs_conn = sqlite3.connect(output)
rs_conn.row_factory = dict_factory
rs_curs = rs_conn.cursor()

if create_table is False:
    rs_curs.execute("""CREATE TABLE watersheds (
        huc8 TEXT NOT NULL,
        name TEXT,
        states TEXT,
        bounds TEXT,
        polygon TEXT
    )""")

driver = ogr.GetDriverByName("GPKG")
dataSource = driver.Open(watersheds, 0)
layer = dataSource.GetLayer('WBDHU8')
# in_spatial_ref = layer.GetSpatialRef()

# outSpatialRef = osr.SpatialReference()
# outSpatialRef.ImportFromEPSG(4326)
# transform = osr.CoordinateTransformation(in_spatial_ref, outSpatialRef)

for feature in layer:
    huc8 = feature.GetField('HUC8')
    name = feature.GetField('NAME')
    states = feature.GetField('STATES')

    geom = feature.GetGeometryRef()
    # geom.Transform(transform)

    simple_geom = geom.SimplifyPreserveTopology(0.001)
    extent_poly = simple_geom.ExportToJson()
    extent_centroid = simple_geom.Centroid()
    bbox = simple_geom.GetEnvelope()

    bounds = {
        'centroid': {
            'lng': extent_centroid.GetX(),
            'lat': extent_centroid.GetY()
        },
        'boundingBox': {
            'MinLat': bbox[2],
            'MinLng': bbox[0],
            'MaxLat': bbox[3],
            'MaxLng': bbox[1]
        }
    }

    rs_curs.execute('INSERT INTO watersheds (huc8, name, states, bounds, polygon) VALUES (?, ?, ?, ?, ?)', [huc8, name, states, json.dumps(bounds), extent_poly])

rs_conn.commit()
