import sqlite3
import json
import csv
from datetime import datetime
import requests
from dateutil.relativedelta import relativedelta

OUTPUT_PATH = '/data/riverscapes_production.gpkg'

BASE_REQUEST = 'https://waterservices.usgs.gov/nwis/site/'

# DAILY_REQUEST = 'http://waterdata.usgs.gov/nwis/dv'
# DAILY_FIELDS = {
#     'datetime': 'datetime',
#     'discharge': '84956_00060_00003',
#     'discharge_code': '84956_00060_00003_cd',
#     'gage_height': '84959_00065_00003',
#     'gage_height_code': '84959_00065_00003_cd'
# }

DAILY_REQUEST = 'https://waterservices.usgs.gov/nwis/dv'
DAILY_STATISTICS_REQUEST = 'https://waterservices.usgs.gov/nwis/stat'

# FYI: https://help.waterdata.usgs.gov/codes-and-parameters

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

    with sqlite3.connect(db_path) as conn:
        # set the row_factory to return a dictionary
        conn.row_factory = sqlite3.Row
        curs = conn.cursor()

        # Ensure the discharge table exists
        # forign key site_no references sites(site_no)
        # curs.execute('''
        #     CREATE TABLE IF NOT EXISTS discharges
        #     (
        #         site_no TEXT UNIQUE NOT NULL REFERENCES sites(site_no) ON DELETE CASCADE,
        #         peak_discharge REAL
        #     )''')

        # # add a min_discharge_run column to discharges table
        # curs.execute('ALTER TABLE discharges ADD COLUMN peak_discharge_run INTEGER')

        # Find all sites without min_discharge_run data
        curs.execute('SELECT site_no FROM discharges WHERE peak_discharge_run IS NULL')
        sites = curs.fetchall()

        print(f'Found {len(sites)} sites without peak discharge data')

        # if clearFirst is True:
        #     print('Deleting all discharges')
        #     curs.execute('DELETE FROM discharges')

        for site in sites:

            # # check if we already have a peak discharge for this site
            # curs.execute('SELECT peak_discharge FROM discharges WHERE site_no = ?', [site['site_no']])
            # row = curs.fetchone()
            # if row is not None:
            #     continue

            print(f'Retrieving data for site {site["site_no"]}')
            params = {
                'format': 'rdb',
                'sites': site['site_no'],
                'endDT': start_date.strftime('%Y-%m-%d'),
                'startDT': end_date.strftime('%Y-%m-%d'),
                'parameterCd': '00060'
            }

            response = requests.get(DAILY_REQUEST, params=params)

            if response.status_code == 200:
                csv_raw = [line for line in response.text.split('\n') if not (line.startswith('#') or line.startswith('5s'))]

                headers = csv_raw[0].split('\t')
                if len(headers) < 4:
                    curs.execute('''UPDATE discharges SET peak_discharge_run = ? WHERE site_no = ?''', (1, site['site_no']))
                    continue
                discharge_name = headers[3]
                csv_data = csv.DictReader(csv_raw, delimiter='\t')
                # agency_cd,site_no,datetime,tz_cd,89062_00060,89062_00060_cd,89063_00065,89063_00065_cd
                discharge_values = []
                for row in csv_data:
                    try:
                        # get the third row
                        discharge_value = float(row.get(discharge_name, None))                      
                    except ValueError:
                        discharge_value = None

                    discharge_values.append(
                        discharge_value
                    )

                discharge_values = [value for value in discharge_values if value is not None]

                if len(discharge_values) == 0:
                    peak_discharge = None
                else:
                    # Find Peak Discharge
                    peak_discharge = max(discharge_values)    

                curs.execute('''UPDATE discharges SET peak_discharge = ?, peak_discharge_run = ? WHERE site_no = ?''', (peak_discharge, 1, site['site_no']))
                conn.commit()

                # curs.execute('''
                #     INSERT INTO discharges
                #     (
                #         site_no,
                #         peak_discharge
                #     )
                #     VALUES
                #     (
                #         ?, ?
                #     )''', (site['site_no'], peak_discharge))
                # conn.commit()


def get_min_flow(db_path: str, start_date, end_date, clearFirst: bool = True):

    with sqlite3.connect(db_path) as conn:
        # set the row_factory to return a dictionary
        conn.row_factory = sqlite3.Row
        curs = conn.cursor()

        # # Ensure the discharge table exists
        # # forign key site_no references sites(site_no)
        # curs.execute('''
        #     CREATE TABLE IF NOT EXISTS discharges
        #     (
        #         site_no TEXT UNIQUE NOT NULL REFERENCES sites(site_no) ON DELETE CASCADE,
        #         min_discharge REAL
        #     )''')

        # # add min_discharge to discharges table
        # curs.execute('ALTER TABLE discharges ADD COLUMN min_discharge REAL')

        # # add a min_discharge_run column to discharges table
        # curs.execute('ALTER TABLE discharges ADD COLUMN min_discharge_run INTEGER')

        # Find all sites without min_discharge_run data
        curs.execute('SELECT site_no FROM discharges WHERE min_discharge_run IS NULL')
        sites = curs.fetchall()

        print(f'Found {len(sites)} sites without min discharge data')

        # if clearFirst is True:
        #     print('Deleting all discharges')
        #     curs.execute('DELETE FROM discharges')

        for site in sites:

            print(f'Retrieving minimum flow data for site {site["site_no"]}')
            params = {
                'format': 'rdb',
                'sites': site['site_no'],
                'statTypeCd': 'min',
                'endDT': start_date.strftime('%Y-%m-%d'),
                'startDT': end_date.strftime('%Y-%m-%d'),
                'parameterCd': '00060'
            }

            response = requests.get(DAILY_STATISTICS_REQUEST, params=params)

            if response.status_code == 200:
                csv_raw = [line for line in response.text.split('\n') if not (line.startswith('#') or line.startswith('5s'))]

                # headers = csv_raw[0].split('\t')
                csv_data = csv.DictReader(csv_raw, delimiter='\t')
                min_discharges = []
                for row in csv_data:
                    try:
                        discharge_value = float(row.get('min_va', None))
                    except ValueError:
                        discharge_value = None

                    min_discharges.append(discharge_value)

                # Filter out the None values
                min_discharges = [value for value in min_discharges if value is not None]

                # Find Min Discharge
                if len(min_discharges) == 0:
                    min_discharge = None
                else:
                    min_discharge = min(min_discharges)    

                curs.execute('''UPDATE discharges SET min_discharge = ?, min_discharge_run = ? WHERE site_no = ?''', (min_discharge, 1, site['site_no']))
                conn.commit()


if __name__ == '__main__':
    # for i in range(1, 19):
    #     get_gages(OUTPUT_PATH, i, False)


    current_datetime = datetime.now()
    two_years_prior = current_datetime - relativedelta(years=2)
    get_discharges(OUTPUT_PATH, current_datetime, two_years_prior, False)

    get_min_flow(OUTPUT_PATH, current_datetime, two_years_prior, False)

