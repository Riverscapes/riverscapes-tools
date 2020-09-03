# Load prism locations into database

import csv
import sqlite3

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')

prism_csv = '/SOMEPATH/precipitation/prismid_ll.csv'
database = '/SOMEPATH/BRAT/brat5.sqlite'

# Load CSV values
locations = []
with open(prism_csv, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        locations.append((int(row['prismid']), float(row['lat']), float(row['lon'])))

# Insert the locations into the database
conn = sqlite3.connect(database)
conn.executemany('INSERT INTO PrecipLocations (PrismID, Latitude, Longitude) VALUES (?, ?, ?)', locations)
conn.commit()

print(len(locations), 'prism locations inserted into database')
