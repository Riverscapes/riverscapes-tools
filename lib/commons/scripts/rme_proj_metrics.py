import sqlite3

def get_rme_metrics(gpkg_path):

    out_metrics = {
        'channelArea': {'all': {},
                        'perennial': {},
                        'intermittent': {},
                        'ephemeral': {}}, 
        'lowlyingArea': {'all': {},
                        'perennial': {},
                        'intermittent': {},
                        'ephemeral': {}}, 
        'elevatedArea': {'all': {},
                        'perennial': {},
                        'intermittent': {},
                        'ephemeral': {}},
        'valleyWidth': {'all': {},
                        'perennial': {},
                        'intermittent': {},
                        'ephemeral': {}}
    }

    conn = sqlite3.connect(gpkg_path)
    curs = conn.cursor()

    # total riverscape length
    curs.execute("""SELECT SUM(centerline_length) FROM dgos""")
    out_metrics['riverscapeLength'] = curs.fetchone()[0]

    # channel area all
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_channel_area AS REAL))*0.000247105 FROM vw_dgo_metrics GROUP BY rme_dgo_ownership""")
    chan_area_all = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['channelArea']['all'] = chan_area_all
    
    # channel area perennial
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_channel_area AS REAL))*0.000247105 FROM vw_dgo_metrics WHERE (nhd_dgo_streamtype = 46006 or nhd_dgo_streamtype = 55800) GROUP BY rme_dgo_ownership""")
    chan_area_peren = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['channelArea']['perennial'] = chan_area_peren

    # channel area intermittent
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_channel_area AS REAL))*0.000247105 FROM vw_dgo_metrics WHERE nhd_dgo_streamtype = 46003 GROUP BY rme_dgo_ownership""")
    chan_area_inter = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['channelArea']['intermittent'] = chan_area_inter

    # channel area ephemeral
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_channel_area AS REAL))*0.000247105 FROM vw_dgo_metrics WHERE nhd_dgo_streamtype = 46007 GROUP BY rme_dgo_ownership""")
    chan_area_ephem = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['channelArea']['ephemeral'] = chan_area_ephem

    # lowlying area all
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_lowlying_area AS REAL))*0.000247105 FROM vw_dgo_metrics GROUP BY rme_dgo_ownership""")
    low_area_all = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['lowlyingArea']['all'] = low_area_all
    
    # lowlying area perennial
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_lowlying_area AS REAL))*0.000247105 FROM vw_dgo_metrics WHERE (nhd_dgo_streamtype = 46006 or nhd_dgo_streamtype = 55800) GROUP BY rme_dgo_ownership""")
    low_area_peren = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['lowlyingArea']['perennial'] = low_area_peren

    # lowlying area intermittent
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_lowlying_area AS REAL))*0.000247105 FROM vw_dgo_metrics WHERE nhd_dgo_streamtype = 46003 GROUP BY rme_dgo_ownership""")
    low_area_inter = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['lowlyingArea']['intermittent'] = low_area_inter

    # lowlying area ephemeral
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_lowlying_area AS REAL))*0.000247105 FROM vw_dgo_metrics WHERE nhd_dgo_streamtype = 46007 GROUP BY rme_dgo_ownership""")
    low_area_ephem = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['lowlyingArea']['ephemeral'] = low_area_ephem

    # elevated area all
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_elevated_area AS REAL))*0.000247105 FROM vw_dgo_metrics GROUP BY rme_dgo_ownership""")
    ele_area_all = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['elevatedArea']['all'] = ele_area_all
    
    # elevated area perennial
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_elevated_area AS REAL))*0.000247105 FROM vw_dgo_metrics WHERE (nhd_dgo_streamtype = 46006 or nhd_dgo_streamtype = 55800) GROUP BY rme_dgo_ownership""")
    ele_area_peren = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['elevatedArea']['perennial'] = ele_area_peren

    # elevated area intermittent
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_elevated_area AS REAL))*0.000247105 FROM vw_dgo_metrics WHERE nhd_dgo_streamtype = 46003 GROUP BY rme_dgo_ownership""")
    ele_area_inter = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['elevatedArea']['intermittent'] = ele_area_inter

    # elevated area ephemeral
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_dgo_elevated_area AS REAL))*0.000247105 FROM vw_dgo_metrics WHERE nhd_dgo_streamtype = 46007 GROUP BY rme_dgo_ownership""")
    ele_area_ephem = {row[0]: row[1] for row in curs.fetchall()}
    out_metrics['elevatedArea']['ephemeral'] = ele_area_ephem

    # valley width all
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_igo_integrated_width AS REAL)) FROM vw_dgo_metrics GROUP BY rme_dgo_ownership""")
    out_metrics['valleyWidth']['all'] = {row[0]: row[1] for row in curs.fetchall()}
    
    # valley width perennial
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_igo_integrated_width AS REAL)) FROM vw_dgo_metrics WHERE (nhd_dgo_streamtype = 46006 or nhd_dgo_streamtype = 55800) GROUP BY rme_dgo_ownership""")
    out_metrics['valleyWidth']['perennial'] = {row[0]: row[1] for row in curs.fetchall()}

    # valley width intermittent
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_igo_integrated_width AS REAL)) FROM vw_dgo_metrics WHERE nhd_dgo_streamtype = 46003 GROUP BY rme_dgo_ownership""")
    out_metrics['valleyWidth']['intermittent'] = {row[0]: row[1] for row in curs.fetchall()}

    # valley width ephemeral
    curs.execute("""SELECT rme_dgo_ownership, SUM(CAST(vbet_igo_integrated_width AS REAL)) FROM vw_dgo_metrics WHERE nhd_dgo_streamtype = 46007 GROUP BY rme_dgo_ownership""")
    out_metrics['valleyWidth']['ephemeral'] = {row[0]: row[1] for row in curs.fetchall()}

    return out_metrics


