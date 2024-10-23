import requests
import csv
import json
import sqlite3
import os

from rscommons import ProgressBar


def get_gage_da(gage_id):

    url = f'https://waterdata.usgs.gov/nwis/inventory?site_no={gage_id}&format=rdb'
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            reader = csv.reader(response.text.splitlines(), delimiter='\t')
            for row in reader:
                if len(row) > 1 and 'agency_cd' in row[0]:
                    ix = row.index('contrib_drain_area_va')
                    ix2 = row.index('drain_area_va')
            reader2 = csv.reader(response.text.splitlines(), delimiter='\t')
            for row in reader2:
                if len(row) > 1 and 'USGS' in row[0]:
                    return row[ix], row[ix2], response.status_code
        else:
            return None, None, response.status_code
    except requests.exceptions.Timeout:
        return None, None, 408


def add_da_to_sites(db_path, err_filepath):
    errs = {'site_no': []}
    outputs = {}

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # c.execute('ALTER TABLE sites ADD COLUMN da REAL')
    c.execute('SELECT site_no FROM sites')
    sites = c.fetchall()
    progbar = ProgressBar(len(sites), 50, 'Getting gage drainage areas')
    counter = 0
    for site in sites:
        site_no = site[0]
        da = get_gage_da(site_no)
        if da is None:
            continue
        if da[0] is not None and da[0] != '':
            outputs[site_no] = da[0]
        elif (da[0] is None or da[1] == '') and da[1] is not None and da[1] != '':
            outputs[site_no] = da[1]
        else:
            errs['site_no'].append((site_no, da[2]))

        counter += 1
        progbar.update(counter)
    progbar.finish()

    with open(os.path.join(os.path.dirname(db_path), 'gage_da.json'), 'w') as f:
        json.dump(outputs, f)

    progbar = ProgressBar(len(outputs), 50, 'Updating database')
    counter = 0
    for site_no, da in outputs.items():
        if da is None:
            continue
        if da == '':
            continue
        c.execute(f"UPDATE sites SET da = {da} WHERE site_no = '{site_no}'")
        counter += 1
        progbar.update(counter)

    progbar.finish()

    conn.commit()
    conn.close()

    with open(err_filepath, 'w') as f:
        json.dump(errs, f)

    return

def add_da_from_json(db_path, json_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    with open(json_path, 'r') as f:
        da_dict = json.load(f)
    progbar = ProgressBar(len(da_dict), 50, 'Updating database')
    counter = 0
    for site_no, da in da_dict.items():
        if da is None:
            continue
        if da == '':
            continue
        c.execute(f"UPDATE sites SET da = {da} WHERE site_no = '{site_no}'")
        counter += 1
        progbar.update(counter)
    progbar.finish()

    conn.commit()
    conn.close()
    return

db_in = '/mnt/c/Users/jordang/Documents/StreamStats/riverscapes_production.gpkg'
err_file = '/mnt/c/Users/jordang/Documents/StreamStats/da_errors.json'
json_in = '/mnt/c/Users/jordang/Documents/StreamStats/gage_da.json'
add_da_to_sites(db_in, err_file)
# add_da_from_json(db_in, json_in)
