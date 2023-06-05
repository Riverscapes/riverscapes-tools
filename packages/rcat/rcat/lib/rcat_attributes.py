"""
"""
import sqlite3

from rscommons import Logger


def igo_attributes(database: str):

    log = Logger('RCAT IGO Attributes')

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    # fp accessibility
    curs.execute('SELECT IGOFPAccess.IGOID, CellCount, TotCells FROM IGOFPAccess'
                 ' INNER JOIN (SELECT IGOID, SUM(CellCount) AS TotCells FROM IGOFPAccess GROUP BY IGOID) AS CC ON IGOFPAccess.IGOID=CC.IGOID'
                 ' WHERE AccessVal = 1')
    igoaccess = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, accessval in igoaccess.items():
        curs.execute(f'UPDATE IGOAttributes SET FloodplainAccess = {accessval} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET FloodplainAccess = 0 WHERE FloodplainAccess IS NULL')

    # from conifer
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = -80')
    from_conifer = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, fc in from_conifer.items():
        curs.execute(f'UPDATE IGOAttributes SET FromConifer = {fc} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET FromConifer = 0 WHERE FromConifer IS NULL')

    # from devegetated
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = -60')
    from_deveg = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, deveg in from_deveg.items():
        curs.execute(f'UPDATE IGOAttributes SET FromDevegetated = {deveg} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET FromDevegetated = 0 WHERE FromDevegetated IS NULL')

    # from grass shrubland
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = -50')
    from_grassshrub = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, gs in from_grassshrub.items():
        curs.execute(f'UPDATE IGOAttributes SET FromGrassShrubland = {gs} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET FromGrassShrubland = 0 WHERE FromGrassShrubland IS NULL')

    # from deciduous
    # curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
    #              ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
    #              ' WHERE ConvVal = -35')
    # from_decid = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    # for igoid, decid in from_decid.items():
    #     conn.execute(f'UPDATE IGOAttributes SET FromDeciduous = {decid} WHERE IGOID = {igoid}')
    # curs.execute('UPDATE IGOAttributes SET FromDeciduous = 0 WHERE FromDeciduous IS NULL')

    # no change
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 0')
    no_change = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, nc in no_change.items():
        curs.execute(f'UPDATE IGOAttributes SET NoChange = {nc} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET NoChange = 0 WHERE NoChange IS NULL')

    # deciduous
    # curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
    #              ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
    #              ' WHERE ConvVal = 35')
    # deciduous = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    # for igoid, decid in deciduous.items():
    #     conn.execute(f'UPDATE IGOAttributes SET Deciduous = {decid} WHERE IGOID = {igoid}')
    # curs.execute('UPDATE IGOAttributes SET Deciduous = 0 WHERE Deciduous IS NULL')

    # grass shrubland
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 50')
    grassshrub = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, gs in grassshrub.items():
        curs.execute(f'UPDATE IGOAttributes SET GrassShrubland = {gs} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET GrassShrubland = 0 WHERE GrassShrubland IS NULL')

    # devegetation
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 60')
    devegetation = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, deveg in devegetation.items():
        curs.execute(f'UPDATE IGOAttributes SET Devegetation = {deveg} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET Devegetation = 0 WHERE Devegetation IS NULL')

    # Conifer
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 80')
    conifer = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, con in conifer.items():
        curs.execute(f'UPDATE IGOAttributes SET Conifer = {con} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET Conifer = 0 WHERE Conifer IS NULL')

    # Invasive
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 97')
    invasive = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, inv in invasive.items():
        curs.execute(f'UPDATE IGOAttributes SET Invasive = {inv} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET Invasive = 0 WHERE Invasive IS NULL')

    # Development
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 98')
    development = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, dev in development.items():
        curs.execute(f'UPDATE IGOAttributes SET Development = {dev} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET Development = 0 WHERE Development IS NULL')

    # Agriculture
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 99')
    agriculture = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, ag in agriculture.items():
        curs.execute(f'UPDATE IGOAttributes SET Agriculture = {ag} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET Agriculture = 0 WHERE Agriculture IS NULL')

    # Non Riparian
    curs.execute('SELECT IGOConv.IGOID, NonRip, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS NonRip FROM IGOConv WHERE ConvVal NOT IN (-80, -60, -50, 0, 50, 60, 80, 97, 98, 99) GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CD ON IGOConv.IGOID=CD.IGOID')
    nonrip = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, nonr in nonrip.items():
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
    curs.execute('SELECT IGOExRiparian.IGOID, ExRipCellCount, TotalCells FROM IGOExRiparian'
                 ' INNER JOIN (SELECT IGOID, SUM(ExRipCellCount) AS TotalCells FROM IGOExRiparian GROUP BY IGOID) AS RS ON IGOExRiparian.IGOID = RS.IGOID'
                 ' WHERE ExRipVal = 1')

    ex_rip = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, ex_rip_mean in ex_rip.items():
        curs.execute(f'UPDATE IGOAttributes SET ExistingRiparianMean = {ex_rip_mean} WHERE IGOID = {igoid}')
    curs.execute('UPDATE IGOAttributes SET ExistingRiparianMean = 0 WHERE ExistingRiparianMean IS NULL')

    # historic riparian mean
    curs.execute('SELECT IGOHRiparian.IGOID, HRipCellCount, TotalCells FROM IGOHRiparian'
                 ' INNER JOIN (SELECT IGOID, SUM(HRipCellCount) AS TotalCells FROM IGOHRiparian GROUP BY IGOID) AS RS ON IGOHRiparian.IGOID = RS.IGOID'
                 ' WHERE HRipVal = 1')

    h_rip = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, h_rip_mean in h_rip.items():
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
            elif 0.9 >= val[0] / val[1] > 0.66:
                depid = 2
            elif 0.66 >= val[0] / val[1] > 0.33:
                depid = 3
            else:
                depid = 4
            curs.execute(f'UPDATE IGOAttributes SET RiparianDeparture = {val[0]/val[1]}, RiparianDepartureID = {depid} WHERE IGOID = {igoid}')

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
        # if val[0] is None:
        #     val[0] = 0
        # if val[1] is None:
        if val[1] == 0:
            curs.execute(f'UPDATE ReachAttributes SET RiparianDeparture = 1, RiparianDepartureID = 0 WHERE ReachID = {rid}')
        else:
            if 1 > val[0] / val[1] >= 0.9:
                depid = 1
            elif 0.9 >= val[0] / val[1] > 0.66:
                depid = 2
            elif 0.66 >= val[0] / val[1] > 0.33:
                depid = 3
            else:
                depid = 4
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


# db = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/outputs/rcat.gpkg'

# igo_attributes(db)
# reach_attributes(db)
