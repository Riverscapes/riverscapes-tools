import csv
import os
import argparse
import psycopg2
from psycopg2.extras import RealDictCursor
from rscommons import dotenv
from rscommons import ProgressBar


relative_path = '../database/data'


def update_brat_parameters(host, port, database, user_name, password, csv_dir):
    """ Pull the BRAT parameters from PostGres database and save them to CSV
    files in the BRAT source code folder where they can be used by BRAT Build
    and also commited to git.

    Arguments:
        host {str} -- PostGres host URL or IP address
        port {str} -- PostGres port
        database {str} -- PostGres database name
        user_name {str} -- PostGres user name
        passord {str} -- PostGres password
        csv_dir {[type]} -- [description]

    """

    csv_dir = csv_dir if csv_dir else os.path.dirname(os.path.abspath(__file__))

    watershed_csv = os.path.join(csv_dir, relative_path, 'Watersheds.csv')
    hydro_params_csv = os.path.join(csv_dir, relative_path, 'HydroParams.csv')
    veg_type_csv = os.path.join(csv_dir, relative_path, 'VegetationTypes.csv')
    override_csv = os.path.join(csv_dir, relative_path, 'intersect', 'VegetationOverrides.csv')
    watershed_hydro_params_csv = os.path.join(csv_dir, relative_path, 'intersect', 'WatershedHydroParams.csv')

    conn = psycopg2.connect(host=host, port=port, database=database, user=user_name, password=password)
    curs = conn.cursor(cursor_factory=RealDictCursor)

    # Update watersheds first, because it will attempt to verify the hydrologic equations
    # and abort with errors and before any CSV files are changed.
    update_watersheds(curs, watershed_csv)
    update_vegetation_types(curs, veg_type_csv)
    update_vegetation_overrides(curs, override_csv)
    update_hydro_params(curs, hydro_params_csv)
    update_watershed_hydro_params(curs, watershed_hydro_params_csv)


def update_watersheds(curs, watershed_csv):

    # Load all the watersheds from the database in a PREDICTABLE ORDER (so git diff is useful for previewing changes)
    curs.execute("""SELECT * FROM watersheds ORDER BY watershed_id""")
    watersheds = {row['watershed_id']: {
        'WatershedID': row['watershed_id'],
        'Name': row['name'],
        'EcoregionID': row['ecoregion_id'],
        'MaxDrainage': row['max_drainage'],
        'QLow': row['qlow'],
        'Q2': row['q2'],
        'Notes': row['notes'],
        'MetaData': row['metadata'],
        'AreaSqKm': row['area_sqkm'],
        'States': row['states'].replace(',', '_') if row['states'] else None
    } for row in curs.fetchall()}

    # Validate the hydrologic equations. The following dictionary will be keyed by python exception concatenated to produce
    # a unique string for each type of error for each equation. These will get printed to the screen for easy cut and paste
    # into a GitHub issue for USU to resolve.
    unique_errors = {}
    for q in ['QLow', 'Q2']:
        progbar = ProgressBar(len(watersheds), 50, 'Verifying {} equations'.format(q))
        counter = 0
        for watershed, values in watersheds.items():
            counter += 1
            progbar.update(counter)

            # proceed if the watershed has a hydrologic formula defined
            if not values[q]:
                continue

            # Load the hydrologic parameters for this watershed and substitute a placeholder for drainage area
            curs.execute('SELECT * FROM vw_watershed_hydro_params WHERE watershed_id = %s', [watershed])
            params = {row['name']: row['value'] for row in curs.fetchall()}
            params['DRNAREA'] = 1.0

            try:
                equation = values[q]
                equation = equation.replace('^', '**')
                value = eval(equation, {'__builtins__': None}, params)
                _float_val = float(value)
            except Exception as ex:
                exception_id = repr(ex) + values[q]
                if exception_id in unique_errors:
                    unique_errors[exception_id]['watersheds'][watershed] = params
                else:
                    unique_errors[exception_id] = {
                        'watersheds': {watershed: params},
                        'exception': repr(ex),
                        'equation': values[q],
                    }

        progbar.finish()

    if len(unique_errors) > 0:
        for exception_id, values in unique_errors.items():
            print('\n## Hydrologic equation Error\n```')
            print('Equation:', values['equation'])
            print('Exception:', values['exception'])
            print('Watersheds:')
            for watershed, params in values['watersheds'].items():
                print('\t{}:'.format(watershed))
                [print('\t\t{}: {}'.format(key, val)) for key, val in params.items()]
            print('```')
        raise Exception('Aborting due to {} hydrology equation errors'.format(len(unique_errors)))

    write_values_to_csv(watershed_csv, watersheds)


def update_hydro_params(curs, hydro_params_csv):

    # Load all the hydro parameters from the database in a PREDICTABLE ORDER (so git diff is useful for previewing changes)
    curs.execute('SELECT param_id, name, description, aliases, data_units, equation_units, conversion, definition FROM hydro_params ORDER BY param_id')
    values = {row['param_id']: {
        'ParamID': row['param_id'],
        'Name': row['name'],
        'Description': row['description'],
        'Aliases': row['aliases'],
        'DataUnits': row['data_units'],
        'EquationUnits': row['equation_units'],
        'Conversion': row['conversion'],
        'Definition': row['definition']
    } for row in curs.fetchall()}

    write_values_to_csv(hydro_params_csv, values)


def update_watershed_hydro_params(curs, watershed_hydro_params_csv):

    # Load all the watershed hydro parameters from the database in a PREDICTABLE ORDER (so git diff is useful for previewing changes)
    curs.execute('SELECT watershed_id, param_id, value FROM watershed_hydro_params ORDER BY watershed_id, param_id')
    values = {(row['watershed_id'], row['param_id']): {
        'WatershedID': row['watershed_id'],
        'ParamID': row['param_id'],
        'Value': row['value']
    } for row in curs.fetchall()}

    write_values_to_csv(watershed_hydro_params_csv, values)


def update_vegetation_types(curs, veg_type_csv):

    # Update the CSV values with those from the Google Sheet
    updates = 0
    curs.execute('SELECT * FROM vegetation_types ORDER BY vegetation_id')
    values = {row['vegetation_id']: {
        'VegetationID': row['vegetation_id'],
        'EpochID': row['epoch_id'],
        'Name': row['name'],
        'DefaultSuitability': row['default_suitability'],
        'LandUseID': row['land_use_id'],
        'Physiognomy': row['physiognomy'],
        'Notes': row['notes']
    } for row in curs.fetchall()}

    write_values_to_csv(veg_type_csv, values)


def update_vegetation_overrides(curs, override_csv):

    curs.execute('SELECT vegetation_id, ecoregion_id, override_suitability, notes FROM vegetation_overrides ORDER BY vegetation_id, ecoregion_id')
    db_values = [{
        'VegetationID': row['vegetation_id'],
        'EcoregionID': row['ecoregion_id'],
        'OverrideSuitability': row['override_suitability'],
        'Notes': row['notes']
    }for row in curs.fetchall()]

    # Write to CSV
    cols = ['EcoregionID', 'VegetationID', 'OverrideSuitability', 'Notes']
    with open(override_csv, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(cols)
        for vals in db_values:
            writer.writerow([vals[col] for col in cols])

    print('{} vegetetation overrides CSV file written'.format(len(db_values)))
    return 0


def write_values_to_csv(csv_file, values):

    cols = list(values[next(iter(values))].keys())

    progBar = ProgressBar(len(values), 50, 'Writing to {}'.format(os.path.basename(csv_file)))
    with open(csv_file, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(cols)
        counter = 0
        for vals in values.values():
            counter += 1
            progBar.update(counter)
            writer.writerow([vals[col] for col in cols])

    progBar.finish()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('host', help='BRAT parameter DB host', type=str)
    parser.add_argument('port', help='BRAT parameter DB port', type=str)
    parser.add_argument('database', help='BRAT parameter database', type=str)
    parser.add_argument('user_name', help='BRAT parameter DB user name', type=str)
    parser.add_argument('password', help='BRAT parameter DB password', type=str)
    parser.add_argument('--csv_dir', help='directory where the csv lives', type=str)
    args = dotenv.parse_args_env(parser)

    try:
        update_brat_parameters(args.host, args.port, args.database, args.user_name, args.password, args.csv_dir)
        print('Processing completed successfully. Review changes using git.')
    except Exception as ex:
        print('Errors occurred:', str(ex))


if __name__ == "__main__":
    main()
