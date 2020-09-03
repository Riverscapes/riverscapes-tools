import os
import csv
import argparse
import sqlite3
from sqlbrat.lib.plotting import validation_chart
from rscommons import dotenv


def hydrology_param_validation(usu_params, database):

    hucs = {}

    # Load the USU iHydrology model parameters from CSV file
    with open(usu_params, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            huc8 = row['HUC8Dir'].split('_')[1]

            if huc8.startswith('1705'):
                continue

            hucs[huc8] = {'USU': {
                'PRECIP': float(row['PRECIP_IN']) * 25.4,
                'RR': float(row['BASIN_RELIEF']) * 0.3048,
                'ELEV': float(row['ELEV_FT']) * 0.3048,
                'MINELEV': float(row['MIN_ELEV_FT']) * 0.3048,
                'MEANSLOPE': float(row['SLOPE_PCT']),
                'SLOP30_30M': float(row['BASIN_SLOPE']),
                'FOREST': float(row['FOREST_PCT']),
                'ForestCoverP1': float(row['FOREST_PLUS_PCT'])
            }}

    print(len(hucs), 'HUCs loaded from USU model parameters CSV file.')

    # Calculate the BRAT4 iHydrology mode parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()
    for huc, values in hucs.items():
        curs.execute('SELECT Name, Value FROM WatershedHydroParams WHP INNER JOIN HydroParams HP ON WHP.ParamID = HP.ParamID WHERE (WatershedID = ?)', [huc])
        values['pyBRAT4'] = {row[0]: row[1] for row in curs.fetchall()}

    # generate charts
    for param in ['PRECIP', 'RR', 'ELEV', 'MINELEV', 'MEANSLOPE', 'SLOP30_30M', 'FOREST']:
        results = []
        for huc, values in hucs.items():
            if param in values['USU'] and 'pyBRAT4' in values and param in values['pyBRAT4']:
                results.append((values['USU'][param], values['pyBRAT4'][param]))

        if len(results) > 0:
            validation_chart(results, 'Hydro Param {}'.format(param))

    print('Hydrology Parameter Validation Complete')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('usu_csv', help='USU hydrology parameter CSV', type=str)
    parser.add_argument('database', help='BRAT database', type=str)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '..', '..', '.env.validation'))

    hydrology_param_validation(args.usu_csv, args.database)


if __name__ == '__main__':
    main()
