from rscommons import Logger, GeopackageLayer


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
                confinement_length += dgo_ftr.GetField('ConfinLeng')
                constricted_length += dgo_ftr.GetField('ConstrLeng')
                segment_length += dgo_ftr.GetField('ApproxLeng')

            confinement_ratio = min((confinement_length + constricted_length) / segment_length, 1.0) if segment_length > 0.0 else 0.0
            constricted_ratio = constricted_length / segment_length if segment_length > 0.0 else 0.0

            igo_ftr.SetField('ConfinLeng', confinement_length + constricted_length)
            igo_ftr.SetField('ConstrLeng', constricted_length)
            igo_ftr.SetField('ApproxLeng', segment_length)
            igo_ftr.SetField('Confinement_Ratio', confinement_ratio)
            igo_ftr.SetField('Constriction_Ratio', constricted_ratio)

            igo_lyr.ogr_layer.SetFeature(igo_ftr)

        igo_lyr.ogr_layer.CommitTransaction()
    return
