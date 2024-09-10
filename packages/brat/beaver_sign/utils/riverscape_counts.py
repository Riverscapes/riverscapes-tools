import os
from osgeo import ogr

from rscommons import GeopackageLayer, Logger


def riverscapes_dam_counts(gpkg_path: str, windows: dict):

    log = Logger('Moving Window Beaver Activity')
    log.info('Attributing IGOs with dam count attributes')

    dgo = os.path.join(gpkg_path, 'dgos')
    igo = os.path.join(gpkg_path, 'igos')

    out_data = {}

    with GeopackageLayer(dgo) as dgo_lyr, GeopackageLayer(igo) as igo_lyr:
        for igo_ftr, *_ in igo_lyr.iterate_features('Getting info from DGOs'):
            dam_ct = 0
            cl_len = 0
            sql = f'WHERE fid in {str(tuple(windows[igo_ftr.GetFID()]))}'
            for dgo_ftr, *_ in dgo_lyr.iterate_features(attribute_filter=sql):
                dam_ct += dgo_ftr.GetField('dam_ct')
                cl_len += dgo_ftr.GetField('centerline_length')
            out_data[igo_ftr.GetFID()] = [dam_ct, cl_len]

    with GeopackageLayer(igo, write=True) as igo_lyr:
        igo_lyr.ogr_layer.CreateField(ogr.FieldDefn('dam_ct', ogr.OFTInteger))
        igo_lyr.ogr_layer.CreateField(ogr.FieldDefn('dam_density', ogr.OFTReal))
        for ftr, *_ in igo_lyr.iterate_features('Attributing IGOs'):
            dam_ct, cl_len = out_data[ftr.GetFID()]
            ftr.SetField('dam_ct', dam_ct)
            ftr.SetField('dam_density', dam_ct / (cl_len / 1000))
            igo_lyr.ogr_layer.SetFeature(ftr)

    log.info('Done')
