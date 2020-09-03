# Philip Bailey
# 12 Feb 2020
# Temporary script to load LandFire 2.0 vegetation types and land uses from CSV into the BRAT SQLite database template

import csv
import sqlite3

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')


csv_file = '/SOMEPATH/Landfire/landfire_2_0_0_evt_type.csv'
sql_file = '/SOMEPATH/beaver/pyBRAT4/database/brat_template.sqlite'

conn = sqlite3.connect(sql_file)
curs = conn.execute('SELECT LandUseID, Name FROM LandUses')
land_uses = {row[1]: row[0] for row in curs.fetchall()}
existing = len(land_uses)

veg = []
with open(csv_file, 'r') as f:
    reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)

    # print reader.fieldnames
    for row in reader:
        if row['EVT_GP_N'] not in land_uses:
            curs.execute('INSERT INTO LandUses (Name) VALUES (?)', [row['EVT_GP_N']])
            land_uses[row['EVT_GP_N']] = curs.lastrowid

        curs.execute('UPDATE VegetationTypes SET LandUseID = ? WHERE VegetationID = ?', [land_uses[row['EVT_GP_N']], row['VALUE']])

print(existing, 'existing land uses and', len(land_uses) - existing, 'added')
conn.commit()
