import csv
import os
import argparse
from rscommons import dotenv
import psycopg2
from psycopg2.extras import RealDictCursor


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
    ecoregion_csv = os.path.join(csv_dir, relative_path, 'Ecoregions.csv')
    hydro_params_csv = os.path.join(csv_dir, relative_path, 'HydroParams.csv')
    veg_type_csv = os.path.join(csv_dir, relative_path, 'VegetationTypes.csv')
    override_csv = os.path.join(csv_dir, relative_path, 'intersect', 'VegetationOverrides.csv')

    ecoregions = load_ecoregions(ecoregion_csv)
    hydro_params = load_hydro_params(hydro_params_csv)
    veg_types = load_veg_types(veg_type_csv)

    conn = psycopg2.connect(host=host, port=port, database=database, user=user_name, password=password)
    curs = conn.cursor(cursor_factory=RealDictCursor)

    value_updates = 0
    value_updates += update_watersheds(curs, ecoregions, hydro_params, watershed_csv)
    value_updates += update_vegetation_types(curs, veg_type_csv)
    value_updates += update_vegetation_overrides(curs, ecoregions, veg_types, override_csv)

    print('Process complete.')
    if value_updates > 0:
        print('Git commit and push is required.')
    else:
        print('Watersheds CSV file writing skipped. No git commit required.')


def update_watersheds(curs, ecoregions, hydro_params, watershed_csv):

    watersheds = load_watersheds(watershed_csv)

    curs.execute('SELECT watershed_id, name, ecoregion_id, max_drainage, qlow, q2, notes, metadata FROM watersheds')
    gsh_values = {row['watershed_id']: {
        'Name': row['name'],
        'EcoregionID': row['ecoregion_id'],
        'MaxDrainage': row['max_drainage'],
        'QLow': row['qlow'],
        'Q2': row['q2'],
        'Notes': row['notes'],
        'Metadata': row['metadata']
    } for row in curs.fetchall()}

    # Update the CSV values with those from the Google Sheet
    value_updates = 0
    watershed_updates = 0
    for watershed, values in gsh_values.items():

        # Google sheets stores HUC codes as strings. 7 character codes are not zero padded.
        if (len(watershed) < 8):
            watershed = '{:08d}'.format(int(watershed))

        initial = value_updates
        if watershed not in watersheds:
            raise Exception('{} watershed found in PostGres database but not in CSV file'.format(watershed))

        for col, value in values.items():
            if col not in watersheds[watershed]:
                raise Exception('{} value not found in CSV data for watershed {}'.format(col, watershed))

            if watersheds[watershed][col] != str(value):
                # Skip replacing empty string with None
                if not (watersheds[watershed][col] == '' and value is None):
                    print('Changing {} for watershed {} from {} to {}'.format(col, watershed, watersheds[watershed][col], value))
                    value_updates += 1
                    watersheds[watershed][col] = value

        if initial != value_updates:
            watershed_updates += 1

    # Quick and dirty validation of the regional curves
    equation_errors = []
    for watershed, values in watersheds.items():
        for q in ['QLow', 'Q2']:
            if values[q]:
                try:
                    equation = values[q]
                    equation = equation.replace('^', '**')
                    value = eval(equation, {'__builtins__': None}, hydro_params)
                except Exception as e:
                    print('{} HUC has error with {} discharge equation {}'.format(watershed, q, equation))
                    equation_errors.append((watershed, q, values[q]))

    if len(equation_errors) > 0:
        print('Aborting due to {} hydrology equation errors'.format(len(equation)))
        return

    if value_updates > 0:
        write_values_to_csv(watershed_csv, watersheds)

    print('{:,} CSV values changed across {:,} watersheds.'.format(value_updates, watershed_updates))

    return value_updates


def update_vegetation_types(curs, veg_type_csv):

    veg_types = load_veg_types(veg_type_csv)

    # Update the CSV values with those from the Google Sheet
    updates = 0
    curs.execute('SELECT vegetation_id, default_suitability AS DefaultSuitability, notes AS Notes FROM vegetation_types')
    for row in curs.fetchall():
        initial = updates
        vegetation_id = str(row['vegetation_id'])

        if vegetation_id not in veg_types:
            raise Exception('{} VegetationID found in PostGres database but not in CSV file'.format(vegetation_id))

        for col in ['DefaultSuitability', 'Notes']:
            if str(row[col.lower()]) != veg_types[vegetation_id][col]:
                veg_types[vegetation_id][col] = row[col.lower()]
                updates += 1

    if updates > 0:
        write_values_to_csv(veg_type_csv, veg_types)

    print('{:,} CSV values changed across {:,} vegetation types.'.format(updates, len(veg_types)))
    return updates


def update_vegetation_overrides(curs, ecoregions, veg_types, override_csv):

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


def load_watersheds(csv_file):

    infile = csv.DictReader(open(csv_file))
    values = {rows['WatershedID']: {col: rows[col] for col in infile.fieldnames} for rows in infile}
    print(len(values), 'watersheds retrieved from CSV file')
    return values


def load_veg_types(csv_file):

    infile = csv.DictReader(open(csv_file))
    values = {rows['VegetationID']: {col: rows[col] for col in infile.fieldnames} for rows in infile}
    print(len(values), 'vegetation types retrieved from CSV file')
    return values


def load_ecoregions(csv_file):

    infile = csv.DictReader(open(csv_file))
    values = {rows['Name']: rows['EcoregionID'] for rows in infile}
    print(len(values), 'ecoregions loaded from CSV file')
    return values


def load_hydro_params(csv_file):

    infile = csv.DictReader(open(csv_file))
    values = {rows['Name']: 1 for rows in infile}
    print(len(values), 'hydro parameters loaded from CSV file')
    return values


def write_values_to_csv(csv_file, values):

    cols = list(values[next(iter(values))].keys())

    with open(csv_file, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(cols)
        for vals in values.values():
            writer.writerow([vals[col] for col in cols])

    print('watersheds CSV file written')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('host', help='BRAT parameter DB host', type=str)
    parser.add_argument('port', help='BRAT parameter DB port', type=str)
    parser.add_argument('database', help='BRAT parameter database', type=str)
    parser.add_argument('user_name', help='BRAT parameter DB user name', type=str)
    parser.add_argument('password', help='BRAT parameter DB password', type=str)
    parser.add_argument('--csv_dir', help='directory where the csv lives', type=str)
    args = dotenv.parse_args_env(parser)

    update_brat_parameters(args.host, args.port, args.database, args.user_name, args.password, args.csv_dir)


if __name__ == "__main__":
    main()
