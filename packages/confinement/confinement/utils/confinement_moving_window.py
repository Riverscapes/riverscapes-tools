from rsxml import Logger
from rscommons import GeopackageLayer


def igo_confinement(igo: str, dgo: str, windows: dict):

    logger = Logger("Moving Window Confinement")
    logger.info('Starting moving window confinement analysis')

    # conn = sqlite3.connect(database)
    # curs = conn.cursor()

    with GeopackageLayer(igo, write=True) as igo_lyr, \
            GeopackageLayer(dgo) as dgo_lyr:
        igo_lyr.ogr_layer.StartTransaction()
        for igo_ftr, *_ in igo_lyr.iterate_features():
            igoid = igo_ftr.GetFID()

            confinement_length = 0.0
            constricted_length = 0.0
            segment_length = 0.0

            if len(windows[igoid]) == 1:
                attrib_filt = f'fid = {windows[igoid][0]}'
            else:
                attrib_filt = f'fid IN {tuple(windows[igoid])}'

            for dgo_ftr, *_ in dgo_lyr.iterate_features(attribute_filter=attrib_filt):
                confinement_length += dgo_ftr.GetField('confin_leng')
                constricted_length += dgo_ftr.GetField('constr_leng')
                segment_length += dgo_ftr.GetField('approx_leng')

            confinement_ratio = min((confinement_length + constricted_length) /
                                    segment_length, 1.0) if segment_length > 0.0 else None
            constricted_ratio = constricted_length / \
                segment_length if segment_length > 0.0 else None

            igo_ftr.SetField(
                'confin_leng', confinement_length + constricted_length)
            igo_ftr.SetField('constr_leng', constricted_length)
            igo_ftr.SetField('approx_leng', segment_length)
            igo_ftr.SetField('confinement_ratio', confinement_ratio)
            igo_ftr.SetField('constriction_ratio', constricted_ratio)

            igo_lyr.ogr_layer.SetFeature(igo_ftr)

        igo_lyr.ogr_layer.CommitTransaction()
    return
