import sqlite3
import requests
import json
import os
import csv

OUTPUT_PATH = '/Users/philipbailey/GISData/riverscapes/stream_gages.sqlite'

BASE_REQUEST = 'https://waterservices.usgs.gov/nwis/site/'

for i in range(1, 19):
    # print(i)

    # format = rdb
    # huc = 01
    # siteOutput = expanded
    # siteStatus = all
    # siteType = ST

    params = {
        'huc': '0' + str(i),
        'format': 'rdb',
        'siteStatus': 'all',  # 'active',
        # 'siteOutput': 'expanded',
        # 'hasDataTypeCd': 'dv',
        'siteType': 'ST',
        'seriesCatalogOutput': 'true'
    }
    response = requests.get(BASE_REQUEST, params=params, timeout=120)

    if response.status_code == 200:
        csv_raw = [line for line in response.text.split('\n') if not (line.startswith('#') or line.startswith('5s'))]
        sites = csv.DictReader(csv_raw, delimiter='\t')
    elif response.status_code == 404:
        raise Exception('error')
    else:
        raise Exception(response)

    conn = sqlite3.connect(OUTPUT_PATH)
    curs = conn.cursor()

    curs.execute('''
        CREATE TABLE IF NOT EXISTS sites
        (
            site_id INTEGER PRIMARY KEY,
            site_no TEXT,
            site_name TEXT,
            huc TEXT,
            lat REAL,
            lon REAL,
            county TEXT,
            state TEXT,
            site_type TEXT,
            agency_cd TEXT,
            status TEXT,
            series_catalog TEXT
        )''')

    for site in sites:
        curs.execute('''
            INSERT INTO sites
            (
                site_no,
                site_name,
                huc,
                lat,
                lon,
                site_type,
                agency_cd,
                status
            )
            VALUES
            (
                :site_no,
                :station_nm,
                :huc_cd,
                :dec_lat_va,
                :dec_long_va,
                :site_tp_cd,
                :agency_cd,
                :stat_cd
            )''', site)

    conn.commit()
