import os
from rscommons import get_shp_or_gpkg, GeopackageLayer


def add_acequias(acequias_fc, rsc_path):

    canals_fc = os.path.join(rsc_path, 'transportation', 'canals.shp')
    wbd_fc = os.path.join(rsc_path, 'hydrology', 'nhdplushr.gpkg')
    acequias_layer = get_shp_or_gpkg(acequias_fc)
    canals_layer = get_shp_or_gpkg(canals_fc, write=True)
    wbd_layer = GeopackageLayer(wbd_fc, layer_name='WBDHU10')

    wbd_geom = wbd_layer.ogr_layer.GetFeature(1).GetGeometryRef()
    for *_, aceq_ftr in acequias_layer.iterate_features(clip_shape=wbd_geom):
        aceq_geom = aceq_ftr.GetGeometryRef()

        canals_layer.create_feature(aceq_geom)


afc = '/workspaces/data/Acequias.shp'
rsc_p = '/workspaces/data/rs_context/1302010105'

add_acequias(afc, rsc_p)
