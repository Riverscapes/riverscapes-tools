import os
from osgeo import ogr

from rscommons import GeopackageLayer, Logger, ProgressBar


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
            if len(windows[igo_ftr.GetFID()]) == 1:
                sql = f'fid = {windows[igo_ftr.GetFID()][0]}'
            else:
                sql = f'fid in {str(tuple(windows[igo_ftr.GetFID()]))}'
            for dgo_ftr, *_ in dgo_lyr.iterate_features(attribute_filter=sql):
                if dgo_ftr.GetField('centerline_length') is None or dgo_ftr.GetField('centerline_length') == 0:
                    continue
                dam_ct += dgo_ftr.GetField('dam_ct')
                cl_len += dgo_ftr.GetField('centerline_length')
            if dam_ct > 0:
                out_data[igo_ftr.GetFID()] = [dam_ct, cl_len]

    with GeopackageLayer(igo, write=True) as igo_lyr:
        igo_lyr.ogr_layer.CreateField(ogr.FieldDefn('dam_ct', ogr.OFTInteger))
        igo_lyr.ogr_layer.CreateField(ogr.FieldDefn('dam_density', ogr.OFTReal))

    ct = 0
    progbar = ProgressBar(len(out_data), text='Adding dam counts to IGOs')
    conn = ogr.Open(gpkg_path, 1)
    for fid, dam_ct in out_data.items():
        ct += 1
        progbar.update(ct)
        conn.ExecuteSQL(f'UPDATE igos SET dam_ct = {dam_ct[0]}, dam_density = {dam_ct[0] / (dam_ct[1] / 1000)} WHERE fid = {fid}')

    conn.ExecuteSQL('UPDATE igos SET dam_ct = 0, dam_density = 0 WHERE dam_ct IS NULL')

    # for ftr, *_ in igo_lyr.iterate_features('Attributing IGOs'):
    #     dam_ct, cl_len = out_data[ftr.GetFID()]
    #     if cl_len == 0:
    #         continue
    #     ftr.SetField('dam_ct', dam_ct)
    #     ftr.SetField('dam_density', dam_ct / (cl_len / 1000))
    #     igo_lyr.ogr_layer.SetFeature(ftr)

    log.info('Done')
