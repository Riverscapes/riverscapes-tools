import pickle
import sqlite3
from datetime import datetime
from calendar import monthrange
import argparse
import os
import csv
from rscommons import ModelConfig, dotenv
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
relative_path = '../database/data'
watershed_updateable_cols = ['Ecoregion', 'MaxDrainage', 'QLow', 'Q2', 'Notes']


def import_crb_parameters(sheet_guid):
    """
    Import timesheet data from a Google Sheet into PipSuite financial database
    :param db_con: Absolute local path to SQLite database
    :param sheet_guid: Google Sheet GUID from URL
    :param month: integer month of year
    :return: None
    """

    watereshed_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_path, 'Watersheds.csv')
    ecoregion_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_path, 'Ecoregions.csv')
    hydro_params_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_path, 'HydroParams.csv')

    watersheds = load_watersheds(watereshed_csv)
    ecoregions = load_ecoregions(ecoregion_csv)
    hydro_params = load_hydro_params(hydro_params_csv)

    gsh_values = load_google_sheet_values(sheet_guid, ecoregions)

    # Update the CSV values with those from the Google Sheet
    value_updates = 0
    watershed_updates = 0
    for watershed, values in gsh_values.items():
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
        write_values_to_csv(watereshed_csv, watersheds)

    print('Process complete.')
    print('{:,} CSV values changed across {:,} watersheds.'.format(value_updates, watershed_updates))
    if value_updates > 0:
        print('Git commit and push is required.')
    else:
        print('Watersheds CSV file writing skipped. No git commit required.')


def load_google_sheet_values(sheet_guid, ecoregions):

    credentials = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            credentials = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(credentials, token)

    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()

    # Load the column names of the watersheds worksheet to determine their column indices
    result = sheet.values().get(spreadsheetId=sheet_guid, range='Watersheds!A1:G1').execute()
    col_names = result.get('values', [])[0]
    col_indices = {col: col_names.index(col) for col in watershed_updateable_cols}

    # sheetName = datetime.strptime('{} 1 2019'.format(month), '%m %d %Y').strftime('%b')

    result = sheet.values().get(spreadsheetId=sheet_guid, range='Watersheds!A2:G154').execute()

    values = {}
    for row in result.get('values', []):
        values[row[0]] = {}
        for col, index in col_indices.items():
            if index < len(row):
                value = row[index]

                # Get the database EcoregionID
                if col.lower() == 'ecoregion':
                    col = 'EcoregionID'
                    value = ecoregions[value]

                values[row[0]][col] = value

    print(len(values), 'watersheds retrieved from Google Sheets')
    return values


def load_watersheds(csv_file):

    infile = csv.DictReader(open(csv_file))
    values = {rows['WatershedID']: {col: rows[col] for col in infile.fieldnames} for rows in infile}
    print(len(values), 'watersheds retrieved from CSV file')
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
    parser.add_argument('sheet', help='Google Sheet Identifier', type=str)

    args = dotenv.parse_args_env(parser)

    import_crb_parameters(args.sheet)


if __name__ == "__main__":
    main()
