"""
"""
import sqlite3

from rscommons import Logger


def igo_attributes(database: str):

    log = Logger('RCAT IGO Attributes')

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    curs.execute('SELECT IGOV.IGOID, SUM(ExRipVal) / TotalCells'
                 ' FROM IGOVegetation IGOV'
                 ' INNER JOIN (SELECT IGOID, SUM(ExRipCellCount) AS TotalCells FROM IGOVegetation GROUP BY IGOID) AS RS ON IGOV.IGOID = RS.IGOID'
                 ' GROUP BY IGOV.IGOID')

    ex_rip = {row[0]: row[1] for row in curs.fetchall()}
    for igoid, ex_rip_mean in ex_rip.items():
        conn.execute(f'UPDATE IGOAttributes SET ExistingRiparianMean = {ex_rip_mean} WHERE IGOID = {igoid}')

    curs.execute('SELECT IGOV.IGOID, SUM(HRipVal) / TotalCells'
                 ' FROM IGOVegetation IGOV'
                 ' INNER JOIN (SELECT IGOID, SUM(HRipCellCount) AS TotalCells FROM IGOVegetation GROUP BY IGOID) AS RS ON IGOV.IGOID = RS.IGOID'
                 ' GROUP BY IGOV.IGOID')

    h_rip = {row[0]: row[1] for row in curs.fetchall()}
    for igoid, h_rip_mean in h_rip.items():
        conn.execute(f'UPDATE IGOAttributes SET ExistingRiparianMean = {h_rip_mean} WHERE IGOID = {igoid}')
