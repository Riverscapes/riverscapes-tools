""" Calculate the land use intensity within the window of each igo

    Jordan Gilbert

    Dec 2022
"""
import sqlite3
from rscommons import Logger
from rscommons.database import write_db_attributes


def calculate_land_use(database: str, windows: dict):

    log = Logger('Land Use')
    log.info('Summarizing land use intensity within moving windows for IGOs')

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    curs.execute('SELECT DGOV.DGOID, 100.0 * SUM(Intensity * CAST(CellCount AS REAL)) / CAST(TotalCells AS REAL) AS Intensity'
                 ' FROM DGOVegetation DGOV'
                 ' INNER JOIN VegetationTypes VT ON DGOV.VegetationID = VT.VegetationID'
                 ' INNER JOIN LandUses L ON VT.Physiognomy = L.Name'
                 ' INNER JOIN (SELECT DGOID, SUM(CellCount) AS TotalCells FROM DGOVegetation GROUP BY DGOID) AS RS ON DGOV.DGOID = RS.DGOID'
                 ' GROUP BY DGOV.DGOID')

    results = {row[0]: {'LUI': row[1], 'Cumulative': 0.0} for row in curs.fetchall()}

    for dgoid, lui in results.items():
        luival = lui['LUI']
        curs.execute(f'UPDATE DGOAttributes SET LUI = {luival} WHERE DGOID = {dgoid}')

    for igoid, dgoids in windows.items():
        lui_vals = []
        areas = []
        for dgoid in dgoids:
            curs.execute(f"SELECT LUI, segment_area FROM DGOAttributes WHERE DGOID = {dgoid}")
            res = curs.fetchone()
            lui_vals.append(res[0])
            areas.append(res[1])
        if len(lui_vals) == len(areas) and None not in lui_vals and None not in areas:
            igo_lui = sum([lui * (area / sum(areas)) for lui, area in zip(lui_vals, areas)])
        else:
            log.warning(f'Unable to calculate land use intensity for IGO ID {igoid}')
            igo_lui = -9999
        curs.execute(f'UPDATE IGOAttributes SET LUI = {igo_lui} WHERE IGOID = {igoid}')

    conn.commit()
    conn.close()
