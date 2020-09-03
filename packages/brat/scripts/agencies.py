from osgeo import ogr
from osgeo import osr
import sqlite3
import os

raise Exception('PATHS NEED TO BE RESET')

conn = sqlite3.connect(os.path.join(os.environ['SOME_PATH'], '/database/brat_template.sqlite'))
curs = conn.cursor()
curs.execute('SELECT Abbreviation FROM Agencies')
agencies = [row[0] for row in curs.fetchall()]

driver = ogr.GetDriverByName("ESRI Shapefile")
inDataSource = driver.Open('/SOMEPATH/ownership/surface_management_agency.shp', 0)
inLayer = inDataSource.GetLayer()
for feature in inLayer:
    abbr = feature.GetField('ADMIN_AGEN').strip()
    if abbr and abbr not in agencies:
        agencies.append(abbr)
        print(abbr)
