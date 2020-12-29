# Name:     Land Use
#
# Purpose:  Calculate the land use intensity required by the conflict attributes
#
# Author:   Philip Bailey
#
# Date:     17 Oct 2019
# -------------------------------------------------------------------------------
import argparse
import sqlite3
from rscommons import Logger, dotenv
from rscommons.database import write_attributes


def land_use(database, buffer):
    """Calculate land use intensity for each reach in BRAT database

    Arguments:
        database {str} -- Path to BRAT SQLite database
        buffer {float} -- Distance (meters) to buffer reach to obtain land use
    """

    reaches = calculate_land_use(database, buffer)
    write_attributes(database, reaches, ['iPC_LU', 'iPC_VLowLU', 'iPC_LowLU', 'iPC_ModLU', 'iPC_HighLU'])


def calculate_land_use(database, buffer):

    log = Logger('Land Use')
    log.info('Calculating land use using a buffer distance of {:,}m'.format(buffer))

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    # Calculate the average land use intensity and also retrieve the total number of cells for each reach
    curs.execute('SELECT RV.ReachID, 100.0 * SUM(Intensity * CAST(CellCount AS REAL)) / CAST(TotalCells AS REAL) AS Intensity'
                 ' FROM ReachVegetation RV'
                 ' INNER JOIN VegetationTypes VT ON RV.VegetationID = VT.VegetationID'
                 ' INNER JOIN LandUses L ON VT.LandUseID = L.LandUseID'
                 ' INNER JOIN Epochs EP ON VT.EpochID = EP.EpochID'
                 ' INNER JOIN (SELECT ReachID, SUM(CellCount) AS TotalCells FROM ReachVegetation WHERE Buffer = ? GROUP BY ReachID) AS RS ON RV.ReachID = RS.ReachID'
                 ' WHERE (Buffer = ?) AND (EP.Metadata = "EX")'
                 ' GROUP BY RV.ReachID', [buffer, buffer])

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
                      INNER JOIN LandUses L ON VT.LandUseID = L.LandUseID
                      INNER JOIN Epochs EP ON VT.EpochID = EP.EpochID
                      INNER JOIN (
                         SELECT ReachID, SUM(CellCount) AS TotalCells
                         FROM ReachVegetation RV
                             INNER JOIN VegetationTypes VT ON RV.VegetationID = VT.VegetationID
                             INNER JOIN Epochs E ON VT.EpochID = E.EpochID
                         WHERE (Buffer = ? AND E.Metadata = 'EX') GROUP BY ReachID
                      ) AS RS ON RV.ReachID = RS.ReachID
                      WHERE (Buffer = ?) AND (EP.Metadata = 'EX') AND (Intensity <= ?)
                      GROUP BY RV.ReachID
                      """, [buffer, buffer, max_intensity])

        for row in curs.fetchall():
            results[row[0]][target_col] = row[1] - results[row[0]]['Cumulative']
            results[row[0]]['Cumulative'] = row[1]

    log.info('Land use intensity calculation complete.')
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='BRAT SQLite database', type=argparse.FileType('r'))
    parser.add_argument('--buffer', help='Distance to buffer flow line fearures for sampling vegetation', default=100, type=int)
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    land_use(args.database.name, args.buffer)


if __name__ == '__main__':
    main()
