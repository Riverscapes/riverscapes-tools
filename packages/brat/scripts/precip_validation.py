import os
import csv
import sqlite3
import json
import shutil
from shapely.geometry import Point, shape
from statistics import mean
from sqlbrat.lib.plotting import validation_chart
from rscommons.download import download_unzip
from rscommons.national_map import get_nhdhr_url
from rsxml.util import safe_makedirs

from osgeo import ogr

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')

temp_folder = '/SOMEPATH/GISData/precipitation/download'
usu_csv = '/SOMEPATH/GNAR/NARDrive/Projects/BRAT/Idaho/ModelParameters/Idaho_BRAT_iHyd Parameters.csv'
# precip_csv = '/SOMEPATH/precipitation/ppt2010s.csv'
database = '/SOMEPATH/GISData/BRAT/brat5.sqlite'


# Insert the locations into the database
hucs = {}
conn = sqlite3.connect(database)
curs = conn.cursor()
curs.execute("SELECT HUC FROM HUCs WHERE States LIKE '%ID%'")
for row in curs.fetchall():

    if len(row[0]) != 8:
        continue

    huc4 = row[0][0:4]
    if huc4 not in hucs:
        hucs[huc4] = {}
    hucs[huc4][row[0]] = {}

locations = []
with open(usu_csv, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        huc8 = row['HUC8Dir'].split('_')[1]
        huc4 = huc8[0:4]
        if huc4 in hucs:
            if huc8 in hucs[huc4]:
                hucs[huc4][huc8]['USU'] = float(row['PRECIP_IN']) * 25.4

safe_makedirs(temp_folder)
driver = ogr.GetDriverByName("OpenFileGDB")

chart_values = []
for huc4, huc8s in hucs.items():
    print('Processing', huc4)
    unzip_folder = os.path.join(temp_folder, huc4)
    try:
        nhd_url = get_nhdhr_url(huc4)
        nhd_unzip_folder = download_unzip(nhd_url, temp_folder, unzip_folder, False)
    except Exception as e:
        print('WARNING: Failed to download', huc4)
        continue

    folder_name = os.path.basename(nhd_url).split('.')[0]

    gdbname = None
    for dirName, subdirList, fileList in os.walk(nhd_unzip_folder):
        if dirName.endswith('.gdb'):
            gdbname = dirName
            break

    data_source = driver.Open(gdbname, 0)

    # /SOMEPATH/precipitation/download/NHDPLUS_H_1601_HU4_GDB/NHDPLUS_H_1601_HU4_GDB.gdb
    # /SOMEPATH/precipitation/download/1601/NHDPLUS_H_1601_HU4_GDB/NHDPLUS_H_1601_HU4_GDB.gdb

    for huc8, values in huc8s.items():
        print('Processing', huc8)

        if 'USU' not in hucs[huc4][huc8]:
            continue

        layer = data_source.GetLayer('WBDHU8')
        layer.SetAttributeFilter("HUC8 = '{}'".format(huc8))
        for feature in layer:
            poly = feature.GetGeometryRef()

        coords = poly.GetEnvelope()
        curs.execute('SELECT Latitude, Longitude, MeanPrecip FROM Precipitation WHERE (Latitude >= ?) AND (Latitude <= ?) AND (Longitude >= ?) AND (Longitude <= ?)',
                     [coords[2], coords[3], coords[0], coords[1]])

        shapelypoly = shape(json.loads(poly.ExportToJson()))

        prisms = []
        for row in curs.fetchall():
            point = Point(row[1], row[0])
            if shapelypoly.contains(point):
                prisms.append(row[2])

        chart_values.append((hucs[huc4][huc8]['USU'], mean(prisms)))

    print('Cleaning up', os.path.dirname(gdbname))
    shutil.rmtree(os.path.dirname(gdbname))


# Retrieve the precipitation data from downloaded CSV
validation_chart(chart_values, 'Precipition')

print('Validation complete.')
