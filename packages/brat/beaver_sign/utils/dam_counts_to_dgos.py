from osgeo import ogr

from rscommons import GeopackageLayer, Logger


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
            geom = dgo_ftr.GetGeometryRef()
            dam_ct = 0
            for dam_ftr, *_ in dams.iterate_features(clip_rect=geom.GetEnvelope()):
                if geom.Contains(dam_ftr.GetGeometryRef()):
                    dam_ct += 1

            dam_cts[dgo_ftr.GetFID()] = dam_ct

    with GeopackageLayer(dgos, write=True) as dgo:
        dgo.ogr_layer.CreateField(ogr.FieldDefn('dam_ct', ogr.OFTInteger))
        dgo.ogr_layer.CreateField(ogr.FieldDefn('dam_density', ogr.OFTReal))
        for ftr, *_ in dgo.iterate_features('Adding dam counts to DGOs'):
            if ftr.GetField('centerline_length') is None or ftr.GetField('centerline_length') == 0:
                continue
            if ftr.GetFID() in dam_cts:
                dam_ct = dam_cts[ftr.GetFID()]
                ftr.SetField('dam_ct', dam_ct)
                ftr.SetField('dam_density', dam_ct / (ftr.GetField('centerline_length') / 1000))
            dgo.ogr_layer.SetFeature(ftr)

    log.info('Dam counts added to DGOs')
