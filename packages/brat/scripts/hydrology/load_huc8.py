import sqlite3
from osgeo import ogr, osr
from shapely.geometry import shape, mapping
from rscommons.shapefile import get_transform_from_epsg

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')

watersheds = '/SOMEPATH/GISData/WatershedBoundaries/WBD_National_GDB/WBD_National_GDB.gdb'
brat = '/SOMEPATH/code/beaver/pyBRAT4/database/brat_template.sqlite'

# Load National Watershed boundary dataset
driver = ogr.GetDriverByName('OpenFileGDB')
ds = driver.Open(watersheds, 0)
layer = ds.GetLayer('WBDHU8')
in_spatial_ref = layer.GetSpatialRef()

out_spatial_ref, transform = get_transform_from_epsg(in_spatial_ref, 4326)

hucs = []
for f in layer:

    # geom = f.GetGeometryRef()
    # if transform:
    #     geom.Transform(transform)
    # geojson = geom.ExportToJson()

    hucs.append((
        f.GetField('HUC8'),
        f.GetField('NAME'),
        f.GetField('STATES'),
        round(f.GetField('AREASQKM'), 2) if f.GetField('AREASQKM') else None))
    # geojson))


# Update the BRAT
conn = sqlite3.connect(brat)
curs = conn.cursor()
curs.executemany('REPLACE INTO Watersheds (WatershedID, Name, States, AreaSqKm) VALUES (?, ?, ?, ?)', hucs)
conn.commit()

print(len(hucs), 'HUCs processed')
