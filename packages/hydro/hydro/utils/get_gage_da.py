import requests
import csv
import json
import sqlite3

from rscommons import ProgressBar


def get_gage_da(gage_id, err_file):
    errs = {'site_no': []}

    url = f'https://waterdata.usgs.gov/nwis/inventory?site_no={gage_id}'
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
                return row[ix], row[ix2]
    else:
        errs['site_no'].append(gage_id)

    with open(err_file, 'w') as f:
        json.dump(errs, f)


def add_da_to_sites(db_path, err_filepath):

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('ALTER TABLE sites ADD COLUMN da REAL')
    c.execute('SELECT site_no FROM sites')
    sites = c.fetchall()
    progbar = ProgressBar(len(sites))
    counter = 0
    for site in sites:
        site_no = site[0]
        da = get_gage_da(site_no, err_filepath)
        if da[0] is not None:
            c.execute('UPDATE sites SET da = ? WHERE site_no = ?', (da[0], site_no))
        elif da[0] is None and da[1] is not None:
            c.execute('UPDATE sites SET da = ? WHERE site_no = ?', (da[1], site_no))
        else:
            c.execute('UPDATE sites SET da = ? WHERE site_no = ?', (None, site_no))
        c.execute('UPDATE sites SET da = ? WHERE site_no = ?', (da, site_no))
        counter += 1
        progbar.update(counter)
    conn.commit()
    conn.close()
    return
