import csv
import os
import argparse
from calendar import monthrange
from datetime import datetime
from rscommons import ModelConfig, dotenv
from sqlbrat.lib.database import update_database
import sqlite3
import pickle
import requests

IDS = {
    "Watersheds": "0",  # sheet ID
    "VegetationTypes": "2001494214",
    "VegetationOverrides": "173331623",
    # "Ecoregions": "1619086859"
}

relative_path = '../database/data'
watershed_updateable_cols = ['Ecoregion', 'MaxDrainage', 'QLow', 'Q2', 'Notes']


def import_crb_parameters(sheet_guid, csv_dir):
    """Pull the CSV values from a known google sheet into local CSV files

    Arguments:
        sheet_guid {[type]} -- [description]
        db_path {[type]} -- [description]
        csv_dir {[type]} -- [description]

    Raises:
        Exception: [description]
        Exception: [description]
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

    gsheet_vals = {k: get_csv("https://docs.google.com/spreadsheets/d/e/{}/pub?gid={}&single=true&output=csv".format(sheet_guid, v)) for k, v in IDS.items()}

    value_updates = 0
    value_updates += update_watersheds(gsheet_vals['Watersheds'], ecoregions, hydro_params, watershed_csv)
    value_updates += update_vegetation_types(gsheet_vals['VegetationTypes'], veg_type_csv)
    value_updates += update_vegetation_overrides(gsheet_vals['VegetationOverrides'], ecoregions, veg_types, override_csv)

    print('Process complete.')
    if value_updates > 0:
        print('Git commit and push is required.')
    else:
        print('Watersheds CSV file writing skipped. No git commit required.')


def update_watersheds(gsheet_vals, ecoregions, hydro_params, watershed_csv):

    watersheds = load_watersheds(watershed_csv)

    # Reformat the CSV values from google sheets to be friendlier
    gsh_values = {}
    for row in gsheet_vals:
        new_row = {col: row[col] for col in watershed_updateable_cols}
        new_row['EcoregionID'] = ecoregions[row['Ecoregion']] if row['Ecoregion'] in ecoregions else None
        del new_row['Ecoregion']
        gsh_values[row['WatershedID']] = new_row

    # Update the CSV values with those from the Google Sheet
    value_updates = 0
    watershed_updates = 0
    for watershed, values in gsh_values.items():

        # Google sheets stores HUC codes as strings. 7 character codes are not zero padded.
        if (len(watershed) < 8):
            watershed = '{:08d}'.format(int(watershed))

        initial = value_updates
        if watershed not in watersheds:
            raise Exception('{} watershed found in Google Sheets but not in CSV file'.format(watershed))

        for col, value in values.items():
            if col not in watersheds[watershed]:
                raise Exception('{} value not found in CSV data for watershed {}'.format(col, watershed))

            if watersheds[watershed][col] != value:
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


def update_vegetation_types(gsheet_vals, veg_type_csv):

    veg_types = load_veg_types(veg_type_csv)

    # Update the CSV values with those from the Google Sheet
    updates = 0
    for row in gsheet_vals:
        initial = updates
        vegetation_id = row['VegetationID']

        if vegetation_id not in veg_types:
            raise Exception('{} VgetationID found in Google Sheets but not in CSV file'.format(vegetation_id))

        for col in ['DefaultSuitability', 'Notes']:
            if row[col] != veg_types[vegetation_id][col]:
                veg_types[vegetation_id][col] = row[col]
                updates += 1

    if updates > 0:
        write_values_to_csv(veg_type_csv, veg_types)

    print('{:,} CSV values changed across {:,} vegetation types.'.format(updates, len(veg_types)))
    return updates


def update_vegetation_overrides(gsheet_vals, ecoregions, veg_types, override_csv):

    csv_values = {}
    errors = 0
    for row in gsheet_vals:

        # Skip rows that don't have an ecoregion and vegetation type selected
        if len(row['Ecoregion']) < 1 or len(row['Vegetation Type']) < 1:
            continue

        # The Google Sheet vegetation type is formatted "VegetationID EpochName VegetationTypeName" with spaces between
        veg_type = row['Vegetation Type'].split(' ')[0]

        key = '{}_{}'.format(ecoregions[row['Ecoregion']], veg_types[veg_type]['VegetationID'])

        new_vals = {
            'EcoregionID': ecoregions[row['Ecoregion']],
            'VegetationID': veg_types[veg_type]['VegetationID'],
            'OverrideSuitability': row['OverrideSuitability'],
            'Notes': row['Notes']
        }

        if (key in csv_values):
            print('ERROR:: Duplicate vegetation override: {}  <--> {}'.format(csv_values[key], new_vals))
            errors += 1

        csv_values[key] = new_vals

    if errors > 0:
        raise Exception('Duplicate overrides were detected: {}'.format(errors))

    # Write to CSV
    cols = ['EcoregionID', 'VegetationID', 'OverrideSuitability', 'Notes']
    with open(override_csv, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(cols)
        for vals in csv_values.values():
            writer.writerow([vals[col] for col in cols])

    print('vegetetation overrides CSV file written')
    return 0


def get_csv(url):
    print('Getting: {}'.format(url))
    with requests.Session() as s:
        download = s.get(url)
        decoded_content = download.content.decode('utf-8')
        reader = csv.DictReader(decoded_content.splitlines(), delimiter=',')
        return list(reader)


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
    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('sheet_guid', help='Google Sheet Identifier', type=str)
    parser.add_argument('--csv_dir', help='directory where the csv lives', type=str)

    args = dotenv.parse_args_env(parser)

    import_crb_parameters(args.sheet_guid, args.csv_dir)


if __name__ == "__main__":
    main()
