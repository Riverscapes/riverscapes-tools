import sqlite3
import json
import csv
from datetime import datetime
import requests
from dateutil.relativedelta import relativedelta

OUTPUT_PATH = '/Users/philipbailey/GISData/riverscapes/warehouse_report/riverscapes_production.gpkg'

BASE_REQUEST = 'https://waterservices.usgs.gov/nwis/site/'

DAILY_REQUEST = 'http://waterdata.usgs.gov/nwis/dv',
DAILY_FIELDS = {
    'datetime': 'datetime',
    'discharge': '84956_00060_00003',
    'discharge_code': '84956_00060_00003_cd',
    'gage_height': '84959_00065_00003',
    'gage_height_code': '84959_00065_00003_cd'
}


def get_gages(db_path: str, huc2: int, clearFirst: bool = False) -> None:
    """Retrieve gages from the USGS NWIS service for a given HUC"""

    conn = sqlite3.connect(db_path)
    curs = conn.cursor()

    # Ensure the sites table exists
    curs.execute('''
        CREATE TABLE IF NOT EXISTS sites
        (
            site_id INTEGER PRIMARY KEY,
            site_no TEXT UNIQUE NOT NULL,
            site_name TEXT,
            huc TEXT,
            lat REAL,
            lon REAL,
            county TEXT,
            state TEXT,
            site_type TEXT,
            agency_cd TEXT
        )''')

    if clearFirst is True:
        print(f'Deleting sites for HUC 0{huc2}')
        curs.execute('DELETE FROM sites WHERE huc Like ?', [f'{str(huc2).zfill(2)}%'])

    print(f'Retrieving sites for HUC {str(huc2).zfill(2)}')

    # format = rdb
    # huc = 01
    # siteOutput = expanded
    # siteStatus = all
    # siteType = ST

    params = {
        'huc': str(huc2).zfill(2),
        'format': 'rdb',
        'siteStatus': 'active',  # 'all',
        # 'siteOutput': 'expanded',
        # 'hasDataTypeCd': 'dv',
        'siteType': 'ST',
        # 'seriesCatalogOutput': 'true'
    }
    response = requests.get(BASE_REQUEST, params=params, timeout=120)

    if response.status_code == 200:
        csv_raw = [line for line in response.text.split('\n') if not (line.startswith('#') or line.startswith('5s'))]
        sites = csv.DictReader(csv_raw, delimiter='\t')
    elif response.status_code == 404:
        raise Exception('error')
    else:
        raise Exception(response)

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
                    agency_cd
                )
                VALUES
                (
                    :site_no,
                    :station_nm,
                    :huc_cd,
                    :dec_lat_va,
                    :dec_long_va,
                    :site_tp_cd,
                    :agency_cd
                ) ON CONFLICT DO NOTHING''', site)
    conn.commit()


def get_discharges(db_path: str, start_date, end_date, clearFirst: bool = True):

    conn = sqlite3.connect(db_path)
    curs = conn.cursor()

    curs.execute('SELECT site_no FROM sites')
    sites = curs.fetchall()

    for site in sites:

        params = {
            'format': 'rdb',
            'site_no': site['site_no'],
            'begin_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d')
        }

        response = requests.get(DAILY_REQUEST, params=params)


if __name__ == '__main__':
    for i in range(1, 19):
        get_gages(OUTPUT_PATH, i, True)

    current_datetime = datetime.now()
    two_years_prior = current_datetime - relativedelta(years=2)
    get_discharges(OUTPUT_PATH, current_datetime, two_years_prior)
