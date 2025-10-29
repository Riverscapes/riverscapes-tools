""" Calculate the land use intensity required by the conflict attributes
    Philip Bailey
    17 Oct 2019
"""
import argparse
import sqlite3
from rsxml import Logger, dotenv
from rscommons.database import write_db_attributes


def land_use(database: str):
    """Calculate land use intensity for each reach in BRAT database
    Arguments:
        database {str} -- Path to BRAT SQLite database
        buffer {float} -- Distance (meters) to buffer reach to obtain land use
    """

    reaches = calculate_land_use(database)
    write_db_attributes(database, reaches, ['iPC_LU', 'iPC_VLowLU', 'iPC_LowLU', 'iPC_ModLU', 'iPC_HighLU'])


def calculate_land_use(database: str):
    """ Perform actual land use intensity calculation
    Args:
        database (str): [description]
        buffer (float): [description]
    Returns:
        [type]: [description]
    """

    log = Logger('Land Use')
    log.info('Calculating land use for each reach within associated DGOs')

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    # Calculate the average land use intensity and also retrieve the total number of cells for each reach
    curs.execute('SELECT RV.ReachID, 100.0 * SUM(Intensity * CAST(CellCount AS REAL)) / CAST(TotalCells AS REAL) AS Intensity'
                 ' FROM ReachVegetation RV'
                 ' INNER JOIN VegetationTypes VT ON RV.VegetationID = VT.VegetationID'
                 ' INNER JOIN LandUses L ON VT.Physiognomy = L.Name'
                 ' INNER JOIN (SELECT ReachID, SUM(CellCount) AS TotalCells FROM ReachVegetation GROUP BY ReachID) AS RS ON RV.ReachID = RS.ReachID'
                 ' GROUP BY RV.ReachID')

    results = {row[0]: {'iPC_LU': row[1], 'Cumulative': 0.0} for row in curs.fetchall()}

    # Get the land use intensity classes in ASCENDING ORDER OF INTENSITY
    curs.execute('SELECT Name, MaxIntensity, TargetCol FROM LandUseIntensities ORDER BY MaxIntensity ASC')
    luclasses = [(row[0], row[1], row[2]) for row in curs.fetchall()]

    for name, max_intensity, target_col in luclasses:
        log.info('Processing {} land use intensity class ({}) with max intensity of {}'.format(name, target_col, max_intensity))

        # Ensure all reaches initialized with zero proportion (because following code skips classes without values in DB)
        for values in results.values():
            values[target_col] = 0.0

        # Calculate the number
        curs.execute("""SELECT RV.ReachID, 100.0 * SUM(CAST(CellCount AS REAL) / CAST(TotalCells AS REAL)) AS Proportion
                      FROM ReachVegetation RV
                      INNER JOIN VegetationTypes VT ON RV.VegetationID = VT.VegetationID
                      INNER JOIN LandUses L ON VT.Physiognomy = L.Name
                      INNER JOIN (
                         SELECT ReachID, SUM(CellCount) AS TotalCells
                         FROM ReachVegetation RV
                             INNER JOIN VegetationTypes VT ON RV.VegetationID = VT.VegetationID GROUP BY ReachID
                      ) AS RS ON RV.ReachID = RS.ReachID
                      WHERE (Intensity <= ?)
                      GROUP BY RV.ReachID
                      """, [max_intensity])

        for row in curs.fetchall():
            results[row[0]][target_col] = row[1] - results[row[0]]['Cumulative']
            results[row[0]]['Cumulative'] = row[1]

    log.info('Land use intensity calculation complete.')
    return results


def main():
    """ Land use intensity
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='BRAT SQLite database', type=argparse.FileType('r'))
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    land_use(args.database.name)


if __name__ == '__main__':
    main()
