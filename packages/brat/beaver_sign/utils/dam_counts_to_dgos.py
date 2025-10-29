import os

from osgeo import ogr

from rscommons import GeopackageLayer
from rsxml import ProgressBar
from rscommons.database import SQLiteCon
from rsxml import Logger


def dam_counts_to_dgos(dam_pts: str, dgos: str):
    """Adds a field with dam counts to DGO polygons

    Args:
        dam_pts (str): path to dam point feature class
        dgos (str): path to valley bottom DGO feature class
    """

    log = Logger('Dam Counts')
    log.info('Getting dam counts')

    dam_cts = {}

    with GeopackageLayer(dam_pts) as dams, GeopackageLayer(dgos) as dgo:
        for dgo_ftr, *_ in dgo.iterate_features('Counting dams in DGOs',  clip_shape=dams.ogr_layer.GetSpatialFilter()):
            if dgo_ftr.GetField('centerline_length') is None or dgo_ftr.GetField('centerline_length') == 0:
                continue
            cl_len = dgo_ftr.GetField('centerline_length')
            geom = dgo_ftr.GetGeometryRef()
            dam_ct = 0
            for dam_ftr, *_ in dams.iterate_features(clip_rect=geom.GetEnvelope()):
                if geom.Contains(dam_ftr.GetGeometryRef()):
                    dam_ct += 1

            if dam_ct > 0:
                dam_cts[dgo_ftr.GetFID()] = [dam_ct, cl_len]

    with GeopackageLayer(dgos, write=True) as dgo:
        dgo.ogr_layer.CreateField(ogr.FieldDefn('dam_ct', ogr.OFTInteger))
        dgo.ogr_layer.CreateField(ogr.FieldDefn('dam_density', ogr.OFTReal))
        # for ftr, *_ in dgo.iterate_features('Adding dam counts to DGOs'):
        #     if ftr.GetField('centerline_length') is None or ftr.GetField('centerline_length') == 0:
        #         continue
        #     if ftr.GetFID() in dam_cts:
        #         dam_ct = dam_cts[ftr.GetFID()]
        #         ftr.SetField('dam_ct', dam_ct)
        #         ftr.SetField('dam_density', dam_ct / (ftr.GetField('centerline_length') / 1000))
        #     dgo.ogr_layer.SetFeature(ftr)

    ct = 0
    progbar = ProgressBar(len(dam_cts), text='Adding dam counts to DGOs')
    # with SQLiteCon(os.path.dirname(dgos)) as db:
    # db.curs.execute('BEGIN TRANSACTION')
    # for fid, dam_ct in dam_cts.items():
    #     ct += 1
    #     progbar.update(ct)
    #     db.curs.execute('UPDATE dgos SET dam_ct = ?, dam_density = ? WHERE fid = ?', (dam_ct[0], dam_ct[0] / (dam_ct[1] / 1000), fid))
    # db.conn.commit()
    conn = ogr.Open(os.path.dirname(dgos), 1)
    for fid, dam_ct in dam_cts.items():
        ct += 1
        progbar.update(ct)
        conn.ExecuteSQL(f'UPDATE beaver_activity_dgos SET dam_ct = {dam_ct[0]}, dam_density = {dam_ct[0] / (dam_ct[1] / 1000)} WHERE fid = {fid}')

    conn.ExecuteSQL('UPDATE beaver_activity_dgos SET dam_ct = 0, dam_density = 0 WHERE dam_ct IS NULL')

    log.info('Dam counts added to DGOs')
