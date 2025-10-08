from osgeo import ogr

wbd = ogr.Open('/workspaces/data/rs_context/1302010105/hydrology/nhdplushr.gpkg')
huc10lyr = wbd.GetLayer('WBDHU10')

aceq = ogr.Open('/workspaces/data/Acequias.shp')
canals = ogr.Open('/workspaces/data/rs_context/1302010105/transportation/canals.shp', 1)

wbd_feat = huc10lyr.GetFeature(1)
wbd_geom = wbd_feat.GetGeometryRef()

aceq_lyr = aceq.GetLayer()
aceq_lyr.SetSpatialFilter(wbd_geom)
for aceq_feat in aceq_lyr:
    aceq_geom = aceq_feat.GetGeometryRef()
    new_feat = ogr.Feature(canals.GetLayer().GetLayerDefn())
    new_feat.SetGeometry(aceq_geom)
    canals.GetLayer().CreateFeature(new_feat)
    new_feat = None
    print('added')
