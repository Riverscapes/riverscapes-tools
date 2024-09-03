import sqlite3
import requests
import json
import os
import csv

BASE_REQUEST = 'https://waterservices.usgs.gov/nwis/site/'

for i in range(1, 19):
    # print(i)

    params = {
        'huc': '0' + str(i),
        'format': 'rdb',
        'siteStatus': 'all',  # 'active',
        # 'siteOutput': 'expanded',
        'hasDataTypeCd': 'dv',
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
