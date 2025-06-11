"""
"""
import sqlite3

from rscommons import Logger


def igo_attributes(database: str, windows: dict):

    log = Logger('RCAT IGO Attributes')

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    curs.execute('SELECT DGOID, segment_area FROM DGOAttributes')
    dgoareas = {row[0]: row[1] for row in curs.fetchall()}

    # fp accessibility
    curs.execute('SELECT DGOFPAccess.DGOID, CellCount, TotCells FROM DGOFPAccess'
                 ' INNER JOIN (SELECT DGOID, SUM(CellCount) AS TotCells FROM DGOFPAccess GROUP BY DGOID) AS CC ON DGOFPAccess.DGOID=CC.DGOID'
                 ' WHERE AccessVal = 1')
    igoaccess = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in igoaccess.items():
        curs.execute(f'UPDATE DGOAttributes SET FloodplainAccess = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET FloodplainAccess = 0 WHERE FloodplainAccess IS NULL')
    for igoid, dgoids in windows.items():
        accessvals = []
        area = []
        for dgoid in dgoids:
            try:
                accessvals.append(igoaccess[dgoid])
            except KeyError:
                accessvals.append(0)
            area.append(dgoareas[dgoid])
        accessval = sum([accessvals[i] * (area[i] / sum(area)) for i in range(len(accessvals))])
        curs.execute(f'UPDATE IGOAttributes SET FloodplainAccess = {accessval} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET FloodplainAccess = 0 WHERE FloodplainAccess IS NULL')

    # from conifer
    curs.execute('SELECT DGOConv.DGOID, ConvCellCount, TotCells FROM DGOConv'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS TotCells FROM DGOConv GROUP BY DGOID) AS CC ON DGOConv.DGOID=CC.DGOID'
                 ' WHERE ConvVal = -80')
    from_conifer = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in from_conifer.items():
        curs.execute(f'UPDATE DGOAttributes SET FromConifer = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET FromConifer = 0 WHERE FromConifer IS NULL')
    for igoid, dgoids in windows.items():
        fcvals = []
        area = []
        for dgoid in dgoids:
            try:
                fcvals.append(from_conifer[dgoid])
            except KeyError:
                fcvals.append(0)
            area.append(dgoareas[dgoid])
        fc = sum([fcvals[i] * (area[i] / sum(area)) for i in range(len(fcvals))])
        curs.execute(f'UPDATE IGOAttributes SET FromConifer = {fc} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET FromConifer = 0 WHERE FromConifer IS NULL')

    # from devegetated
    curs.execute('SELECT DGOConv.DGOID, ConvCellCount, TotCells FROM DGOConv'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS TotCells FROM DGOConv GROUP BY DGOID) AS CC ON DGOConv.DGOID=CC.DGOID'
                 ' WHERE ConvVal = -60')
    from_deveg = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in from_deveg.items():
        curs.execute(f'UPDATE DGOAttributes SET FromDevegetated = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET FromDevegetated = 0 WHERE FromDevegetated IS NULL')
    for igoid, dgoids in windows.items():
        devegvals = []
        area = []
        for dgoid in dgoids:
            try:
                devegvals.append(from_deveg[dgoid])
            except KeyError:
                devegvals.append(0)
            area.append(dgoareas[dgoid])
        deveg = sum([devegvals[i] * (area[i] / sum(area)) for i in range(len(devegvals))])
        curs.execute(f'UPDATE IGOAttributes SET FromDevegetated = {deveg} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET FromDevegetated = 0 WHERE FromDevegetated IS NULL')

    # from grass shrubland
    curs.execute('SELECT DGOConv.DGOID, ConvCellCount, TotCells FROM DGOConv'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS TotCells FROM DGOConv GROUP BY DGOID) AS CC ON DGOConv.DGOID=CC.DGOID'
                 ' WHERE ConvVal = -50')
    from_grassshrub = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in from_grassshrub.items():
        curs.execute(f'UPDATE DGOAttributes SET FromGrassShrubland = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET FromGrassShrubland = 0 WHERE FromGrassShrubland IS NULL')
    for igoid, dgoids in windows.items():
        gsvals = []
        area = []
        for dgoid in dgoids:
            try:
                gsvals.append(from_grassshrub[dgoid])
            except KeyError:
                gsvals.append(0)
            area.append(dgoareas[dgoid])
        gs = sum([gsvals[i] * (area[i] / sum(area)) for i in range(len(gsvals))])
        curs.execute(f'UPDATE IGOAttributes SET FromGrassShrubland = {gs} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET FromGrassShrubland = 0 WHERE FromGrassShrubland IS NULL')

    # no change
    curs.execute('SELECT DGOConv.DGOID, ConvCellCount, TotCells FROM DGOConv'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS TotCells FROM DGOConv GROUP BY DGOID) AS CC ON DGOConv.DGOID=CC.DGOID'
                 ' WHERE ConvVal = 0')
    no_change = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in no_change.items():
        curs.execute(f'UPDATE DGOAttributes SET NoChange = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET NoChange = 0 WHERE NoChange IS NULL')
    for igoid, dgoids in windows.items():
        ncvals = []
        area = []
        for dgoid in dgoids:
            try:
                ncvals.append(no_change[dgoid])
            except KeyError:
                ncvals.append(0)
            area.append(dgoareas[dgoid])
        nc = sum([ncvals[i] * (area[i] / sum(area)) for i in range(len(ncvals))])
        curs.execute(f'UPDATE IGOAttributes SET NoChange = {nc} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET NoChange = 0 WHERE NoChange IS NULL')

    # grass shrubland
    curs.execute('SELECT DGOConv.DGOID, ConvCellCount, TotCells FROM DGOConv'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS TotCells FROM DGOConv GROUP BY DGOID) AS CC ON DGOConv.DGOID=CC.DGOID'
                 ' WHERE ConvVal = 50')
    grassshrub = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in grassshrub.items():
        curs.execute(f'UPDATE DGOAttributes SET GrassShrubland = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET GrassShrubland = 0 WHERE GrassShrubland IS NULL')
    for igoid, dgoids in windows.items():
        gsvals = []
        area = []
        for dgoid in dgoids:
            try:
                gsvals.append(grassshrub[dgoid])
            except KeyError:
                gsvals.append(0)
            area.append(dgoareas[dgoid])
        gs = sum([gsvals[i] * (area[i] / sum(area)) for i in range(len(gsvals))])
        curs.execute(f'UPDATE IGOAttributes SET GrassShrubland = {gs} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET GrassShrubland = 0 WHERE GrassShrubland IS NULL')

    # devegetation
    curs.execute('SELECT DGOConv.DGOID, ConvCellCount, TotCells FROM DGOConv'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS TotCells FROM DGOConv GROUP BY DGOID) AS CC ON DGOConv.DGOID=CC.DGOID'
                 ' WHERE ConvVal = 60')
    devegetation = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in devegetation.items():
        curs.execute(f'UPDATE DGOAttributes SET Devegetation = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET Devegetation = 0 WHERE Devegetation IS NULL')
    for igoid, dgoids in windows.items():
        devegvals = []
        area = []
        for dgoid in dgoids:
            try:
                devegvals.append(devegetation[dgoid])
            except KeyError:
                devegvals.append(0)
            area.append(dgoareas[dgoid])
        deveg = sum([devegvals[i] * (area[i] / sum(area)) for i in range(len(devegvals))])
        curs.execute(f'UPDATE IGOAttributes SET Devegetation = {deveg} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET Devegetation = 0 WHERE Devegetation IS NULL')

    # Conifer
    curs.execute('SELECT DGOConv.DGOID, ConvCellCount, TotCells FROM DGOConv'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS TotCells FROM DGOConv GROUP BY DGOID) AS CC ON DGOConv.DGOID=CC.DGOID'
                 ' WHERE ConvVal = 80')
    conifer = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in conifer.items():
        curs.execute(f'UPDATE DGOAttributes SET Conifer = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET Conifer = 0 WHERE Conifer IS NULL')
    for igoid, dgoids in windows.items():
        convals = []
        area = []
        for dgoid in dgoids:
            try:
                convals.append(conifer[dgoid])
            except KeyError:
                convals.append(0)
            area.append(dgoareas[dgoid])
        con = sum([convals[i] * (area[i] / sum(area)) for i in range(len(convals))])
        curs.execute(f'UPDATE IGOAttributes SET Conifer = {con} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET Conifer = 0 WHERE Conifer IS NULL')

    # Invasive
    curs.execute('SELECT DGOConv.DGOID, ConvCellCount, TotCells FROM DGOConv'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS TotCells FROM DGOConv GROUP BY DGOID) AS CC ON DGOConv.DGOID=CC.DGOID'
                 ' WHERE ConvVal = 97')
    invasive = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in invasive.items():
        curs.execute(f'UPDATE DGOAttributes SET Invasive = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET Invasive = 0 WHERE Invasive IS NULL')
    for igoid, dgoids in windows.items():
        invvals = []
        area = []
        for dgoid in dgoids:
            try:
                invvals.append(invasive[dgoid])
            except KeyError:
                invvals.append(0)
            area.append(dgoareas[dgoid])
        inv = sum([invvals[i] * (area[i] / sum(area)) for i in range(len(invvals))])
        curs.execute(f'UPDATE IGOAttributes SET Invasive = {inv} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET Invasive = 0 WHERE Invasive IS NULL')

    # Development
    curs.execute('SELECT DGOConv.DGOID, ConvCellCount, TotCells FROM DGOConv'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS TotCells FROM DGOConv GROUP BY DGOID) AS CC ON DGOConv.DGOID=CC.DGOID'
                 ' WHERE ConvVal = 98')
    development = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in development.items():
        curs.execute(f'UPDATE DGOAttributes SET Development = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET Development = 0 WHERE Development IS NULL')
    for igoid, dgoids in windows.items():
        devvals = []
        area = []
        for dgoid in dgoids:
            try:
                devvals.append(development[dgoid])
            except KeyError:
                devvals.append(0)
            area.append(dgoareas[dgoid])
        dev = sum([devvals[i] * (area[i] / sum(area)) for i in range(len(devvals))])
        curs.execute(f'UPDATE IGOAttributes SET Development = {dev} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET Development = 0 WHERE Development IS NULL')

    # Agriculture
    curs.execute('SELECT DGOConv.DGOID, ConvCellCount, TotCells FROM DGOConv'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS TotCells FROM DGOConv GROUP BY DGOID) AS CC ON DGOConv.DGOID=CC.DGOID'
                 ' WHERE ConvVal = 99')
    agriculture = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in agriculture.items():
        curs.execute(f'UPDATE DGOAttributes SET Agriculture = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET Agriculture = 0 WHERE Agriculture IS NULL')
    for igoid, dgoids in windows.items():
        agvals = []
        area = []
        for dgoid in dgoids:
            try:
                agvals.append(agriculture[dgoid])
            except KeyError:
                agvals.append(0)
            area.append(dgoareas[dgoid])
        ag = sum([agvals[i] * (area[i] / sum(area)) for i in range(len(agvals))])
        curs.execute(f'UPDATE IGOAttributes SET Agriculture = {ag} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET Agriculture = 0 WHERE Agriculture IS NULL')

    # Non Riparian
    curs.execute('SELECT DGOConv.DGOID, NonRip, TotCells FROM DGOConv'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS NonRip FROM DGOConv WHERE ConvVal NOT IN (-80, -60, -50, 0, 50, 60, 80, 97, 98, 99) GROUP BY DGOID) AS CC ON DGOConv.DGOID=CC.DGOID'
                 ' INNER JOIN (SELECT DGOID, SUM(ConvCellCount) AS TotCells FROM DGOConv GROUP BY DGOID) AS CD ON DGOConv.DGOID=CD.DGOID')
    nonrip = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in nonrip.items():
        curs.execute(f'UPDATE DGOAttributes SET NonRiparian = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET NonRiparian = 0 WHERE NonRiparian IS NULL')
    for igoid, dgoids in windows.items():
        nonrvals = []
        area = []
        for dgoid in dgoids:
            try:
                nonrvals.append(nonrip[dgoid])
            except KeyError:
                nonrvals.append(0)
            area.append(dgoareas[dgoid])
        nonr = sum([nonrvals[i] * (area[i] / sum(area)) for i in range(len(nonrvals))])
        curs.execute(f'UPDATE IGOAttributes SET NonRiparian = {nonr} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET NonRiparian = 0 WHERE NonRiparian IS NULL')

    # Riparian
    # curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
    #              ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
    #              ' WHERE ConvVal = -100')
    # riparian = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    # for igoid, rip in riparian.items():
    #     conn.execute(f'UPDATE IGOAttributes SET FromConifer = {rip} WHERE IGOID = {igoid}')

    # dict for order in table: conversion type ID
    log.info('Finding riparian conversion types')
    conv_out = {}
    curs.execute('SELECT IGOID, FromConifer, FromDevegetated, FromGrassShrubland, NoChange, GrassShrubland, Devegetation, Conifer, Invasive, Development, Agriculture, NonRiparian FROM IGOAttributes')
    for row in curs.fetchall():
        id = 0
        val = 0
        for i, _ in enumerate(row):
            if i != 0 and row[i] is not None:
                if row[i] > val:
                    id = i
                    val = row[i]
                elif row[i] == val:
                    id = 12
        if val <= 0.01:
            level = 0
        elif 0.01 < val < 0.1:
            level = 1
        elif 0.1 <= val < 0.25:
            level = 2
        elif 0.25 <= val < 0.5:
            level = 3
        else:
            level = 4
        conv_out[row[0]] = [id, level]

    for igoid, convid in conv_out.items():
        curs.execute(f'UPDATE IGOAttributes SET ConversionID = {convid[0]}, LevelID = {convid[1]} WHERE IGOID = {igoid}')

    # existing riparian mean
    curs.execute('SELECT DGOExRiparian.DGOID, ExRipCellCount, TotalCells FROM DGOExRiparian'
                 ' INNER JOIN (SELECT DGOID, SUM(ExRipCellCount) AS TotalCells FROM DGOExRiparian GROUP BY DGOID) AS RS ON DGOExRiparian.DGOID = RS.DGOID'
                 ' WHERE ExRipVal = 1')

    ex_rip = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in ex_rip.items():
        curs.execute(f'UPDATE DGOAttributes SET ExistingRiparianMean = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET ExistingRiparianMean = 0 WHERE ExistingRiparianMean IS NULL')
    for igoid, dgoids in windows.items():
        ex_rip_vals = []
        area = []
        for dgoid in dgoids:
            try:
                ex_rip_vals.append(ex_rip[dgoid])
            except KeyError:
                ex_rip_vals.append(0)
            area.append(dgoareas[dgoid])
        ex_rip_mean = sum([ex_rip_vals[i] * (area[i] / sum(area)) for i in range(len(ex_rip_vals))])
        curs.execute(f'UPDATE IGOAttributes SET ExistingRiparianMean = {ex_rip_mean} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET ExistingRiparianMean = 0 WHERE ExistingRiparianMean IS NULL')

    # historic riparian mean
    curs.execute('SELECT DGOHRiparian.DGOID, HRipCellCount, TotalCells FROM DGOHRiparian'
                 ' INNER JOIN (SELECT DGOID, SUM(HRipCellCount) AS TotalCells FROM DGOHRiparian GROUP BY DGOID) AS RS ON DGOHRiparian.DGOID = RS.DGOID'
                 ' WHERE HRipVal = 1')

    h_rip = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for dgoid, val in h_rip.items():
        curs.execute(f'UPDATE DGOAttributes SET HistoricRiparianMean = {val} WHERE DGOID = {dgoid}')
    curs.execute('UPDATE DGOAttributes SET HistoricRiparianMean = 0 WHERE HistoricRiparianMean IS NULL')
    for igoid, dgoids in windows.items():
        h_rip_vals = []
        area = []
        for dgoid in dgoids:
            try:
                h_rip_vals.append(h_rip[dgoid])
            except KeyError:
                h_rip_vals.append(0)
            area.append(dgoareas[dgoid])
        h_rip_mean = sum([h_rip_vals[i] * (area[i] / sum(area)) for i in range(len(h_rip_vals))])
        curs.execute(f'UPDATE IGOAttributes SET HistoricRiparianMean = {h_rip_mean} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET HistoricRiparianMean = 0 WHERE HistoricRiparianMean IS NULL')

    conn.commit()

    # departure
    log.info('Finding riparian departure')
    curs.execute('SELECT IGOAttributes.IGOID, ExistingRiparianMean, HistoricRiparianMean FROM IGOAttributes')

    dep = {row[0]: [row[1], row[2]] for row in curs.fetchall()}
    for igoid, val in dep.items():
        # if val[0] is None:
        #     val[0] = 0
        # if val[1] is None:
        if val[1] == 0:
            curs.execute(f'UPDATE IGOAttributes SET RiparianDeparture = 1, RiparianDepartureID = 0 WHERE IGOID = {igoid}')
        else:
            if 1 > val[0] / val[1] >= 0.9:
                depid = 1
                curs.execute(f'UPDATE IGOAttributes SET RiparianDeparture = {val[0]/val[1]}, RiparianDepartureID = {depid} WHERE IGOID = {igoid}')
            elif 0.9 >= val[0] / val[1] > 0.66:
                depid = 2
                curs.execute(f'UPDATE IGOAttributes SET RiparianDeparture = {val[0]/val[1]}, RiparianDepartureID = {depid} WHERE IGOID = {igoid}')
            elif 0.66 >= val[0] / val[1] > 0.33:
                depid = 3
                curs.execute(f'UPDATE IGOAttributes SET RiparianDeparture = {val[0]/val[1]}, RiparianDepartureID = {depid} WHERE IGOID = {igoid}')
            elif val[0] / val[1] <= 0.33:
                depid = 4
                curs.execute(f'UPDATE IGOAttributes SET RiparianDeparture = {val[0]/val[1]}, RiparianDepartureID = {depid} WHERE IGOID = {igoid}')
            else:
                depid = 5
                curs.execute(f'UPDATE IGOAttributes SET RiparianDeparture = {val[0]/val[1]}, RiparianDepartureID = {depid} WHERE IGOID = {igoid}')

    curs.execute('SELECT DGOAttributes.DGOID, ExistingRiparianMean, HistoricRiparianMean FROM DGOAttributes')
    dep_dgo = {row[0]: [row[1], row[2]] for row in curs.fetchall()}
    for dgoid, val in dep_dgo.items():
        if val[1] == 0:
            curs.execute(f'UPDATE DGOAttributes SET RiparianDeparture = 1 WHERE DGOID = {dgoid}')
        else:
            curs.execute(f'UPDATE DGOAttributes SET RiparianDeparture = {val[0]/val[1]} WHERE DGOID = {dgoid}')

    # native riparian
    # curs.execute('SELECT IGOAttributes.IGOID, ExistingRiparianMean - (ExInv / TotCells), HistoricRiparianMean FROM IGOAttributes'
    #              ' INNER JOIN (SELECT IGOID, SUM(CellCount) AS TotCells FROM IGOVegetation GROUP BY IGOID) AS CC ON IGOAttributes.IGOID = CC.IGOID'
    #              ' INNER JOIN (SELECT IGOID, CellCount AS ExInv FROM IGOVegetation WHERE VegetationID = 9327 OR VegetationID = 9827 OR VegetationID = 9318 OR VegetationID = 9320 OR VegetationID = 9324 OR VegetationID = 9329 OR VegetationID = 9332) AS EXV ON IGOAttributes.IGOID = EXV.IGOID')
    # invdep = {row[0]: [row[1], row[2]] for row in curs.fetchall()}
    # for igoid, vals in invdep.items():
    #     if vals[0] is None:
    #         vals[0] = 0
    #     conn.execute(f'UPDATE IGOAttributes SET ExistingNativeRiparianMean = {vals[0]} WHERE IGOID = {igoid}')
    #     if vals[1] is None:
    #         conn.execute(f'UPDATE IGOAttributes SET NativeRiparianDeparture = 1 WHERE IGOID = {igoid}')
    #     else:
    #         conn.execute(f'UPDATE IGOAttributes SET HistoricNativeRiparianMean = {vals[1]} WHERE IGOID = {igoid}')
    #         conn.execute(f'UPDATE IGOAttributes SET NativeRiparianDeparture = {vals[0] / vals[1]} WHERE IGOID = {igoid}')

    conn.commit()
    log.info('Completed riparian departure and conversion calculations for IGOs')


def reach_attributes(database: str):

    log = Logger('RCAT Reach Attributes')

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    # fp accessibility
    curs.execute('SELECT ReachFPAccess.ReachID, CellCount, TotCells FROM ReachFPAccess'
                 ' INNER JOIN (SELECT ReachID, SUM(CellCount) AS TotCells FROM ReachFPAccess GROUP BY ReachID) AS CC ON ReachFPAccess.ReachID=CC.ReachID'
                 ' WHERE AccessVal = 1')
    igoaccess = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, accessval in igoaccess.items():
        curs.execute(f'UPDATE ReachAttributes SET FloodplainAccess = {accessval} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET FloodplainAccess = 0 WHERE FloodplainAccess IS NULL')

    # from conifer
    curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
                 ' WHERE ConvVal = -80')
    from_conifer = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, fc in from_conifer.items():
        curs.execute(f'UPDATE ReachAttributes SET FromConifer = {fc} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET FromConifer = 0 WHERE FromConifer IS NULL')

    # from devegetated
    curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
                 ' WHERE ConvVal = -60')
    from_deveg = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, deveg in from_deveg.items():
        curs.execute(f'UPDATE ReachAttributes SET FromDevegetated = {deveg} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET FromDevegetated = 0 WHERE FromDevegetated IS NULL')

    # from grass shrubland
    curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
                 ' WHERE ConvVal = -50')
    from_grassshrub = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, gs in from_grassshrub.items():
        curs.execute(f'UPDATE ReachAttributes SET FromGrassShrubland = {gs} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET FromGrassShrubland = 0 WHERE FromGrassShrubland IS NULL')

    # from deciduous
    # curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
    #              ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
    #              ' WHERE ConvVal = -35')
    # from_decid = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    # for rid, decid in from_decid.items():
    #     curs.execute(f'UPDATE ReachAttributes SET FromDeciduous = {decid} WHERE ReachID = {rid}')
    # curs.execute('UPDATE ReachAttributes SET FromDeciduous = 0 WHERE FromDeciduous IS NULL')

    # no change
    curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
                 ' WHERE ConvVal = 0')
    no_change = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, nc in no_change.items():
        curs.execute(f'UPDATE ReachAttributes SET NoChange = {nc} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET NoChange = 0 WHERE NoChange IS NULL')

    # deciduous
    # curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
    #              ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
    #              ' WHERE ConvVal = 35')
    # deciduous = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    # for rid, decid in deciduous.items():
    #     curs.execute(f'UPDATE ReachAttributes SET Deciduous = {decid} WHERE ReachID = {rid}')
    # curs.execute('UPDATE ReachAttributes SET Deciduous = 0 WHERE Deciduous IS NULL')

    # grass shrubland
    curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
                 ' WHERE ConvVal = 50')
    grassshrub = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, gs in grassshrub.items():
        curs.execute(f'UPDATE ReachAttributes SET GrassShrubland = {gs} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET GrassShrubland = 0 WHERE GrassShrubland IS NULL')

    # devegetation
    curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
                 ' WHERE ConvVal = 60')
    devegetation = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, deveg in devegetation.items():
        curs.execute(f'UPDATE ReachAttributes SET Devegetation = {deveg} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET Devegetation = 0 WHERE Devegetation IS NULL')

    # Conifer
    curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
                 ' WHERE ConvVal = 80')
    conifer = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, con in conifer.items():
        curs.execute(f'UPDATE ReachAttributes SET Conifer = {con} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET Conifer = 0 WHERE Conifer IS NULL')

    # Invasive
    curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
                 ' WHERE ConvVal = 97')
    invasive = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, inv in invasive.items():
        curs.execute(f'UPDATE ReachAttributes SET Invasive = {inv} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET Invasive = 0 WHERE Invasive IS NULL')

    # Development
    curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
                 ' WHERE ConvVal = 98')
    development = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, dev in development.items():
        curs.execute(f'UPDATE ReachAttributes SET Development = {dev} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET Development = 0 WHERE Development IS NULL')

    # Agriculture
    curs.execute('SELECT ReachConv.ReachID, ConvCellCount, TotCells FROM ReachConv'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
                 ' WHERE ConvVal = 99')
    agriculture = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, ag in agriculture.items():
        curs.execute(f'UPDATE ReachAttributes SET Agriculture = {ag} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET Agriculture = 0 WHERE Agriculture IS NULL')

    # Non-riparian
    curs.execute('SELECT ReachConv.ReachID, NonRip, TotCells FROM ReachConv'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS NonRip FROM ReachConv WHERE ConvVal NOT IN (-80, -60, -50, 0, 50, 60, 80, 97, 98, 99) GROUP BY ReachID) AS CC ON ReachConv.ReachID=CC.ReachID'
                 ' INNER JOIN (SELECT ReachID, SUM(ConvCellCount) AS TotCells FROM ReachConv GROUP BY ReachID) AS CD ON ReachConv.ReachID=CD.ReachID')
    nonrip = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, nonr in nonrip.items():
        curs.execute(f'UPDATE ReachAttributes SET NonRiparian = {nonr} WHERE ReachID = {igoid}')
    curs.execute('UPDATE ReachAttributes SET NonRiparian = 0 WHERE NonRiparian IS NULL')

    # Riparian
    # curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
    #              ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
    #              ' WHERE ConvVal = -100')
    # riparian = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    # for igoid, rip in riparian.items():
    #     conn.execute(f'UPDATE IGOAttributes SET FromConifer = {rip} WHERE IGOID = {igoid}')

    # dict for order in table: conversion type ID
    log.info('Finding riparian conversion types')
    conv_out = {}
    curs.execute('SELECT ReachID, FromConifer, FromDevegetated, FromGrassShrubland, NoChange, GrassShrubland, Devegetation, Conifer, Invasive, Development, Agriculture, NonRiparian FROM ReachAttributes')
    for row in curs.fetchall():
        id = 0
        val = 0
        for i, _ in enumerate(row):
            if i != 0 and row[i] is not None:
                if row[i] > val:
                    id = i
                    val = row[i]
                elif row[i] == val:
                    id = 12
        if val <= 0.01:
            level = 0
        elif 0.01 < val < 0.1:
            level = 1
        elif 0.1 <= val < 0.25:
            level = 2
        elif 0.25 <= val < 0.5:
            level = 3
        else:
            level = 4
        conv_out[row[0]] = [id, level]

    for rid, convid in conv_out.items():
        curs.execute(f'UPDATE ReachAttributes SET ConversionID = {convid[0]}, LevelID = {convid[1]} WHERE ReachID = {rid}')

    # existing riparian mean
    curs.execute('SELECT ReachExRiparian.ReachID, ExRipCellCount, TotalCells FROM ReachExRiparian'
                 ' INNER JOIN (SELECT ReachID, SUM(ExRipCellCount) AS TotalCells FROM ReachExRiparian GROUP BY ReachID) AS RS ON ReachExRiparian.ReachID = RS.ReachID'
                 ' WHERE ExRipVal = 1')

    ex_rip = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, ex_rip_mean in ex_rip.items():
        curs.execute(f'UPDATE ReachAttributes SET ExistingRiparianMean = {ex_rip_mean} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET ExistingRiparianMean = 0 WHERE ExistingRiparianMean IS NULL')

    # historic riparian mean
    curs.execute('SELECT ReachHRiparian.ReachID, HRipCellCount, TotalCells FROM ReachHRiparian'
                 ' INNER JOIN (SELECT ReachID, SUM(HRipCellCount) AS TotalCells FROM ReachHRiparian GROUP BY ReachID) AS RS ON ReachHRiparian.ReachID = RS.ReachID'
                 ' WHERE HRipVal = 1')

    h_rip = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for rid, h_rip_mean in h_rip.items():
        curs.execute(f'UPDATE ReachAttributes SET HistoricRiparianMean = {h_rip_mean} WHERE ReachID = {rid}')
    curs.execute('UPDATE ReachAttributes SET HistoricRiparianMean = 0 WHERE HistoricRiparianMean IS NULL')

    conn.commit()

    # departure
    log.info('Finding riparian departure')
    curs.execute('SELECT ReachAttributes.ReachID, ExistingRiparianMean, HistoricRiparianMean FROM ReachAttributes')

    dep = {row[0]: [row[1], row[2]] for row in curs.fetchall()}
    for rid, val in dep.items():
        if val[1] == 0:
            curs.execute(f'UPDATE ReachAttributes SET RiparianDeparture = 1, RiparianDepartureID = 0 WHERE ReachID = {rid}')
        else:
            if 1 > val[0] / val[1] >= 0.9:
                depid = 1
                curs.execute(f'UPDATE ReachAttributes SET RiparianDeparture = {val[0]/val[1]}, RiparianDepartureID = {depid} WHERE ReachID = {rid}')
            elif 0.9 >= val[0] / val[1] > 0.66:
                depid = 2
                curs.execute(f'UPDATE ReachAttributes SET RiparianDeparture = {val[0]/val[1]}, RiparianDepartureID = {depid} WHERE ReachID = {rid}')
            elif 0.66 >= val[0] / val[1] > 0.33:
                depid = 3
                curs.execute(f'UPDATE ReachAttributes SET RiparianDeparture = {val[0]/val[1]}, RiparianDepartureID = {depid} WHERE ReachID = {rid}')
            elif val[0] / val[1] <= 0.33:
                depid = 4
                curs.execute(f'UPDATE ReachAttributes SET RiparianDeparture = {val[0]/val[1]}, RiparianDepartureID = {depid} WHERE ReachID = {rid}')
            else:
                depid = 5
                curs.execute(f'UPDATE ReachAttributes SET RiparianDeparture = {val[0]/val[1]}, RiparianDepartureID = {depid} WHERE ReachID = {rid}')

    # native riparian
    # curs.execute('SELECT ReachAttributes.ReachID, ExistingRiparianMean - (ExInv / TotCells), HistoricRiparianMean FROM ReachAttributes'
    #              ' INNER JOIN (SELECT ReachID, SUM(CellCount) AS TotCells FROM ReachVegetation GROUP BY ReachID) AS CC ON ReachAttributes.ReachID = CC.ReachID'
    #              ' INNER JOIN (SELECT ReachID, CellCount AS ExInv FROM ReachVegetation WHERE VegetationID = 9327 OR VegetationID = 9827 OR VegetationID = 9318 OR VegetationID = 9320 OR VegetationID = 9324 OR VegetationID = 9329 OR VegetationID = 9332) AS EXV ON ReachAttributes.ReachID = EXV.ReachID')
    # invdep = {row[0]: [row[1], row[2]] for row in curs.fetchall()}
    # for rid, vals in invdep.items():
    #     if vals[0] is None:
    #         vals[0] = 0
    #     conn.execute(f'UPDATE ReachAttributes SET ExistingNativeRiparianMean = {vals[0]} WHERE ReachID = {rid}')
    #     if vals[1] is None:
    #         conn.execute(f'UPDATE ReachAttributes SET NativeRiparianDeparture = 1 WHERE ReachID = {rid}')
    #     else:
    #         conn.execute(f'UPDATE ReachAttributes SET HistoricNativeRiparianMean = {vals[1]} WHERE ReachID = {rid}')
    #         conn.execute(f'UPDATE ReachAttributes SET NativeRiparianDeparture = {vals[0] / vals[1]} WHERE ReachID = {rid}')

    conn.commit()
    log.info('Completed riparian departure and conversion calculations for reaches')
