"""
"""
import sqlite3

from rscommons import Logger


def igo_attributes(database: str):

    # log = Logger('RCAT IGO Attributes')

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    # from conifer
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = -80')
    from_conifer = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, fc in from_conifer.items():
        conn.execute(f'UPDATE IGOAttributes SET FromConifer = {fc} WHERE IGOID = {igoid}')

    # from devegetated
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = -60')
    from_deveg = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, deveg in from_deveg.items():
        conn.execute(f'UPDATE IGOAttributes SET FromDevegetated = {deveg} WHERE IGOID = {igoid}')

    # from grass shrubland
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = -50')
    from_grassshrub = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, gs in from_grassshrub.items():
        conn.execute(f'UPDATE IGOAttributes SET FromGrassShrubland = {gs} WHERE IGOID = {igoid}')

    # from deciduous
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = -35')
    from_decid = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, decid in from_decid.items():
        conn.execute(f'UPDATE IGOAttributes SET FromDeciduous = {decid} WHERE IGOID = {igoid}')

    # no change
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 0')
    no_change = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, nc in no_change.items():
        conn.execute(f'UPDATE IGOAttributes SET NoChange = {nc} WHERE IGOID = {igoid}')

    # deciduous
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 35')
    deciduous = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, decid in deciduous.items():
        conn.execute(f'UPDATE IGOAttributes SET Deciduous = {decid} WHERE IGOID = {igoid}')

    # grass shrubland
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 50')
    grassshrub = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, gs in grassshrub.items():
        conn.execute(f'UPDATE IGOAttributes SET GrassShrubland = {gs} WHERE IGOID = {igoid}')

    # devegetation
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 60')
    devegetation = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, deveg in devegetation.items():
        conn.execute(f'UPDATE IGOAttributes SET Devegetation = {deveg} WHERE IGOID = {igoid}')

    # Conifer
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 80')
    conifer = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, con in conifer.items():
        conn.execute(f'UPDATE IGOAttributes SET Conifer = {con} WHERE IGOID = {igoid}')

    # Invasive
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 97')
    invasive = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, inv in invasive.items():
        conn.execute(f'UPDATE IGOAttributes SET Invasive = {inv} WHERE IGOID = {igoid}')

    # Development
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 98')
    development = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, dev in development.items():
        conn.execute(f'UPDATE IGOAttributes SET Development = {dev} WHERE IGOID = {igoid}')

    # Agriculture
    curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
                 ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
                 ' WHERE ConvVal = 99')
    agriculture = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, ag in agriculture.items():
        conn.execute(f'UPDATE IGOAttributes SET Agriculture = {ag} WHERE IGOID = {igoid}')

    # Multiple
    # curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
    #              ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
    #              ' WHERE ConvVal = 100')
    # multiple = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    # for igoid, mult in multiple.items():
    #     conn.execute(f'UPDATE IGOAttributes SET FromConifer = {mult} WHERE IGOID = {igoid}')

    # Riparian
    # curs.execute('SELECT IGOConv.IGOID, ConvCellCount, TotCells FROM IGOConv'
    #              ' INNER JOIN (SELECT IGOID, SUM(ConvCellCount) AS TotCells FROM IGOConv GROUP BY IGOID) AS CC ON IGOConv.IGOID=CC.IGOID'
    #              ' WHERE ConvVal = -100')
    # riparian = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    # for igoid, rip in riparian.items():
    #     conn.execute(f'UPDATE IGOAttributes SET FromConifer = {rip} WHERE IGOID = {igoid}')

    # dict for order in table: conversion type ID
    conv_out = {}
    curs.execute('SELECT IGOID, FromConifer, FromDevegetated, FromGrassShrubland, FromDeciduous, NoChange, Deciduous, GrassShrubland, Devegetation, Conifer, Invasive, Development, Agriculture FROM IGOAttributes')
    for row in curs.fetchall():
        id = 0
        for i, _ in enumerate(row):
            if row[i] is not None:
                if row[i] > id:
                    id = i
        conv_out[row[0]] = i

    for igoid, convid in conv_out.items():
        conn.execute(f'UPDATE IGOAttributes SET ConversionID = {convid} WHERE IGOID = {igoid}')

    # existing riparian mean
    curs.execute('SELECT IGOExRiparian.IGOID, ExRipCellCount, TotalCells FROM IGOExRiparian'
                 ' INNER JOIN (SELECT IGOID, SUM(ExRipCellCount) AS TotalCells FROM IGOExRiparian GROUP BY IGOID) AS RS ON IGOExRiparian.IGOID = RS.IGOID'
                 ' WHERE ExRipVal = 1')

    ex_rip = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, ex_rip_mean in ex_rip.items():
        conn.execute(f'UPDATE IGOAttributes SET ExistingRiparianMean = {ex_rip_mean} WHERE IGOID = {igoid}')

    # historic riparian mean
    curs.execute('SELECT IGOHRiparian.IGOID, HRipCellCount, TotalCells FROM IGOHRiparian'
                 ' INNER JOIN (SELECT IGOID, SUM(HRipCellCount) AS TotalCells FROM IGOHRiparian GROUP BY IGOID) AS RS ON IGOHRiparian.IGOID = RS.IGOID'
                 ' WHERE HRipVal = 1')

    h_rip = {row[0]: row[1] / row[2] for row in curs.fetchall()}
    for igoid, h_rip_mean in h_rip.items():
        conn.execute(f'UPDATE IGOAttributes SET HistoricRiparianMean = {h_rip_mean} WHERE IGOID = {igoid}')

    conn.commit()


db = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/outputs/rcat.gpkg'

igo_attributes(db)
