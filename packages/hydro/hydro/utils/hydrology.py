""" Calculate the low and high flow hydrology

    Philip Bailey
    30 May 2019
"""
import argparse
import os
import sys
import traceback
from rscommons import Logger, dotenv
from rscommons.database import write_db_attributes, SQLiteCon, load_attributes


# This is the reach drainage area variable in the regional curve equations
DRNAREA_PARAM = 'DRNAREA'


def hydrology(gpkg_path: str, prefix: str, huc: str):
    """Calculate low flow, peak flow discharges for each reach
    in a BRAT database

    Arguments:
        database {str} -- Path to BRAT SQLite database
        prefix {str} -- Q2 or QLow identifying which discharge to calculate
        huc {str} -- watershed identifier

    Raises:
        Exception: When the watershed is missing the regional discharge equation
    """

    hydrology_field = f'Q{prefix}'
    streampower_field = f'SP{prefix}'

    log = Logger('Hydrology')
    log.info(f'Calculating Q{prefix} hydrology for HUC {huc}')
    log.info(f'Discharge field: {hydrology_field}')
    log.info(f'Stream power field: {streampower_field}')

    # Load the hydrology equation for the HUC
    with SQLiteCon(gpkg_path) as database:
        database.curs.execute(f'SELECT Q{prefix} As Q FROM Watersheds WHERE WatershedID = ?', [huc])
        equation = database.curs.fetchone()['Q']
        equation = equation.replace('^', '**')

        if not equation:
            raise Exception(f'Missing {prefix} hydrology formula for HUC {huc}')

        log.info(f'Regional curve: {equation}')

        # Load the hydrology CONVERTED parameters for the HUC (the values will be in the same units as used in the regional equations)
        database.curs.execute('SELECT Parameter, ConvertedValue FROM vwHydroParams WHERE WatershedID = ?', [huc])
        params = {row['Parameter']: row['ConvertedValue'] for row in database.curs.fetchall()}
        [log.info(f'Param: {key} = {value:.2f}') for key, value in params.items()]

        # Load the conversion factor for converting reach attribute drainage areas to the values used in the regional equations
        database.curs.execute('SELECT Conversion FROM HydroParams WHERE Name = ?', [DRNAREA_PARAM])
        drainage_conversion_factor = database.curs.fetchone()['Conversion']
        log.info(f'Reach drainage area attribute conversion factor = {drainage_conversion_factor}')

    # Load the discharges for each reach
    reaches = load_attributes(gpkg_path, ['DrainArea'], '(DrainArea IS NOT NULL)')
    log.info(f'{len(reaches):,} reaches loaded with valid drainage area values')

    # Calculate the discharges for each reach
    results = calculate_hydrology(reaches, equation, params, drainage_conversion_factor, hydrology_field)
    log.info(f'{len(results):,} reach hydrology values calculated.')

    # Write the discharges to the database
    write_db_attributes(gpkg_path, results, [hydrology_field])

    # Convert discharges to stream power
    with SQLiteCon(gpkg_path) as database:
        database.curs.execute(f"""UPDATE ReachAttributes SET {streampower_field} = ROUND((1000 * 9.80665) * iGeo_Slope * ({hydrology_field} * 0.028316846592), 2)
                              WHERE ({hydrology_field} IS NOT NULL) AND (iGeo_Slope IS NOT NULL)""")
        database.conn.commit()

    log.info('Hydrology calculation complete')


def calculate_hydrology(reaches: dict, equation: str, params: dict, drainage_conversion_factor: float, field: str) -> dict:
    """ Perform the actual hydrology calculation

    Args:
        reaches ([type]): [description]
        equation ([type]): [description]
        params ([type]): [description]
        drainage_conversion_factor ([type]): [description]
        field ([type]): [description]

    Raises:
        ex: [description]

    Returns:
        [type]: [description]
    """

    results = {}

    log = Logger('Hydrology')

    try:
        # Loop over each reach
        for reachid, values in reaches.items():

            # Use the drainage area for the current reach and convert to the units used in the equation
            params[DRNAREA_PARAM] = values['DrainArea'] * drainage_conversion_factor

            # Execute the equation but restrict the use of all built-in functions
            eval_result = eval(equation, {'__builtins__': None}, params)
            results[reachid] = {field: eval_result}
    except Exception as ex:
        [log.warning(f'{param}: {value}') for param, value in params.items()]
        log.warning(f'Hydrology formula failed: {equation}')
        log.error('Error calculating hydrology')
        raise ex

    return results


def main():
    """ Main hydrology routine
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='BRAT SQLite database', type=str)
    parser.add_argument('prefix', help='Q2 or Low prefix', type=str)
    parser.add_argument('huc', help='HUC identification code', type=str)
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    logg = Logger("Hydrology")
    logfile = os.path.join(os.path.dirname(args.database), "hydrology.log")
    logg.setup(logPath=logfile, verbose=args.verbose)

    try:
        hydrology(args.database, args.prefix, args.huc)

    except Exception as ex:
        logg.error(ex)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
