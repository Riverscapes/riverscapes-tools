import sqlite3


def dam_reach_type(brat_gpkg):
    """Add one of three values for dam setting to the ReachAttributes table in the BRAT geopackage.
    """

    conn = sqlite3.connect(brat_gpkg)
    curs = conn.cursor()

    curs.execute('SELECT MaxDrainage from Watersheds')
    max_drainage = curs.fetchone()[0]

    if max_drainage is None or max_drainage == '':
        max_drainage = 1000000
    curs.execute(f"""UPDATE ReachAttributes SET Dam_Setting = 'Classic' WHERE iGeo_DA < {max_drainage} AND iGeo_Slope < 0.06 AND oCC_EX > 0 and ReachCode in (46000, 46006, 46003, 46007, 55800, 33400)""")
    curs.execute(f"""UPDATE ReachAttributes SET Dam_Setting = 'Steep' WHERE iGeo_DA < {max_drainage} AND iGeo_Slope >= 0.06 AND oCC_EX > 0 and ReachCode in (46000, 46006, 46003, 46007, 55800, 33400)""")
    curs.execute(f"""UPDATE ReachAttributes SET Dam_Setting = 'Floodplain' WHERE iGeo_DA >= {max_drainage} AND oVC_EX > 0 and ReachCode in (46000, 46006, 46003, 46007, 55800, 33400)""")
    conn.commit()

    return
