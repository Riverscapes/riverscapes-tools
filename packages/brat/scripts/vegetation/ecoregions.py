# -------------------------------------------------------------------------------
# Name:     Ecoregions
#
# Purpose:  Read ecoregion IDs and names from the national ShapeFile downloaded from
#           the EPA and insert them into SQLite database.
#
# Author:   Philip Bailey
#
# Date:     24 Oct 2019
# -------------------------------------------------------------------------------

import os
import sqlite3
from osgeo import ogr

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')

ecoregions_shp = '/SOMEPATH/NationalProject/ecoregions/us_eco_l3_state_boundaries.shp'
database = '/SOMEPATH/beaver/pyBRAT4/data/vegetation.sqlite'

ecoregions = {}
unique = []
driver = ogr.GetDriverByName("ESRI Shapefile")
data_source = driver.Open(ecoregions_shp, 0)
layer = data_source.GetLayer()
for feature in layer:
    id = int(feature.GetField('US_L3CODE'))
    name = feature.GetField('US_L3NAME')
    if id not in ecoregions:
        ecoregions[id] = name
        unique.append((id, name))

print(len(ecoregions), 'unique ecoregions found in ShapeFile')

conn = sqlite3.connect(database)
curs = conn.executemany('INSERT INTO Ecoregions (ID, Name) Values (?, ?)', unique)
conn.commit()

print('Process complete')
