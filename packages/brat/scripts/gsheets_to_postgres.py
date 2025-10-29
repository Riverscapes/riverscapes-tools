# One time Script to transfer the latest BRAT parameters
# from Google Sheets to postgres.
# This is NOT part of BRAT or any official software release.
# Philip Bailey
# 9 Oct 2020

import csv
import os
import argparse
import requests
import pickle
from rsxml import dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

IDS = {
    "Watersheds": "0",  # sheet ID
    "VegetationTypes": "2001494214",
    "VegetationOverrides": "173331623",
    # "Ecoregions": "1619086859"
}

relative_path = '../database/data'
watershed_updateable_cols = ['Ecoregion', 'MaxDrainage', 'QLow', 'Q2', 'Notes']


def gsheets_to_postgres(sheet_guid, host, port, database, user_name, password):
    """Pull the CSV values from a known google sheet into local CSV files

    Arguments:
        sheet_guid {[type]} -- [description]
        db_path {[type]} -- [description]
        csv_dir {[type]} -- [description]

    Raises:
        Exception: [description]
        Exception: [description]
    """
    conn = psycopg2.connect(host=host, port=port, database=database, user=user_name, password=password)
    curs = conn.cursor(cursor_factory=RealDictCursor)

    curs.execute('SELECT ecoregion_id, name FROM ecoregions')
    ecoregions = {row['name']: row['ecoregion_id'] for row in curs.fetchall()}

    curs.execute('SELECT vegetation_id, name FROM vegetation_types')
    veg_types = {row['name']: row['vegetation_id'] for row in curs.fetchall()}

    gsheet_vals = {k: get_csv("https://docs.google.com/spreadsheets/d/e/{}/pub?gid={}&single=true&output=csv".format(sheet_guid, v)) for k, v in IDS.items()}

    value_updates = 0

    try:
        value_updates += update_watersheds(curs, gsheet_vals['Watersheds'], ecoregions)
        value_updates += update_vegetation_types(curs, gsheet_vals['VegetationTypes'])
        value_updates += update_vegetation_overrides(curs, gsheet_vals['VegetationOverrides'], ecoregions, veg_types)

        conn.commit()

    except Exception as ex:
        conn.rollback()
        print(ex)

    print('Process complete.')
    if value_updates > 0:
        print('Git commit and push is required.')
    else:
        print('Watersheds CSV file writing skipped. No git commit required.')


def update_watersheds(curs, gsheet_vals, ecoregions):

    print('Updating watersheds')
    # Reformat the CSV values from google sheets to be friendlier
    gsh_values = {}
    for row in gsheet_vals:
        new_row = {col: row[col] for col in watershed_updateable_cols}
        new_row['EcoregionID'] = ecoregions[row['Ecoregion']] if row['Ecoregion'] in ecoregions else None
        del new_row['Ecoregion']
        gsh_values[row['WatershedID']] = new_row

        # replace any empty strings with None
        for key, val in new_row.items():
            if isinstance(val, str):
                new_row[key] = val if len(val) > 0 else None

    for watershed_id, vals in gsh_values.items():
        curs.execute('UPDATE watersheds SET ecoregion_id = %s, qlow=%s, q2=%s, max_drainage=%s, notes=%s WHERE watershed_id =%s',
                     [vals['EcoregionID'], vals['QLow'], vals['Q2'], vals['MaxDrainage'], vals['Notes'], watershed_id])

    return len(gsh_values)


def update_vegetation_types(curs, gsheet_vals):

    print('Updating vegetation types')
    for row in gsheet_vals:
        curs.execute('UPDATE vegetation_types SET default_suitability = %s, notes=%s WHERE vegetation_id =%s',
                     [row['DefaultSuitability'], row['Notes'], row['VegetationID']])

    return 0


def update_vegetation_overrides(curs, gsheet_vals, ecoregions, veg_types):

    print('Updating vegetation overrides')
    curs.execute('DELETE FROM vegetation_overrides')

    for row in gsheet_vals:
        veg_id = int(row['Vegetation Type'].split(' ')[0])
        curs.execute('INSERT INTO vegetation_overrides (vegetation_id, ecoregion_id, override_suitability, notes) VALUES (%s, %s, %s, %s)',
                     [veg_id, ecoregions[row['Ecoregion']], int(row['OverrideSuitability']), row['Notes'] if row['Notes'] else None])

    return 0


def get_csv(url):
    print('Getting: {}'.format(url))
    with requests.Session() as s:
        download = s.get(url)
        decoded_content = download.content.decode('utf-8')
        reader = csv.DictReader(decoded_content.splitlines(), delimiter=',')
        return list(reader)


def main():
    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('sheet_guid', help='Google Sheet Identifier', type=str)
    parser.add_argument('host', help='BRAT parameter DB host', type=str)
    parser.add_argument('port', help='BRAT parameter DB port', type=str)
    parser.add_argument('database', help='BRAT parameter database', type=str)
    parser.add_argument('user_name', help='BRAT parameter DB user name', type=str)
    parser.add_argument('password', help='BRAT parameter DB password', type=str)

    args = dotenv.parse_args_env(parser)

    gsheets_to_postgres(args.sheet_guid, args.host, args.port, args.database, args.user_name, args.password)


if __name__ == "__main__":
    main()
