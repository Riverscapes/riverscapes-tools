""" Calculate the land use intensity within the window of each igo

    Jordan Gilbert

    Dec 2022
"""
import sqlite3
from rscommons import Logger
from rscommons.database import write_db_attributes


def land_use(database: str):

    reaches = calculate_land_use(database)
    write_db_attributes(database, reaches, ['LUI'])


def calculate_land_use(database: str):

    log = Logger('Land Use')
    log.info('')

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    curs.execute('SELECT IGOV.IGOID, 100.0 * SUM(Intensity * CAST(CellCount AS REAL)) / CAST(TotalCells AS REAL) AS Intensity'
                 ' FROM IGOVegetation IGOV'
                 ' INNER JOIN VegetationTypes VT ON IGOV.VegetationID = VT.VegetationID'
                 ' INNER JOIN LandUses L ON VT.LandUseID = l.LandUseID'
                 ' INNER JOIN (SELECT IGOID, SUM(CellCount) AS TotalCells FROM IGOVegetation GROUP BY IGOID) AS RS ON IGOV.IGOID = RS.IGOID'
                 ' GROUP BY RV.OGRID')

    results = {row[0]: {'LUI': row[1], 'Cumulative': 0.0} for row in curs.fetchall()}
