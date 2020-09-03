# -------------------------------------------------------------------------------
# Name:     HUC Ecoregions
#
# Purpose:  Load the CSV that describes which HUCs are assigned to which
#           ecoregions for Idaho BRAT into a SQLite database.
#
# Author:   Philip Bailey
#
# Date:     24 Oct 2019
# -------------------------------------------------------------------------------
import csv
import os
import sqlite3
from osgeo import ogr

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')

csv_path = '/SOMEPATH/BRAT/IdahoBRAT/Idaho_HUC8_Ecoregions.csv'
database = '/SOMEPATH/BRAT/brat5.sqlite'


def get_ecoregion(original, official):

    match = None
    for id, names in official.items():
        if original in names:
            if match:
                raise Exception('Already matched ecoregion')
            match = id

    if not match:
        raise Exception('No ecoregion found for {}'.format(original))

    return match


# Load the ecoregions from the database
ecoregions = {}
conn = sqlite3.connect(database)
curs = conn.cursor()
curs.execute('SELECT EcoregionID, Name FROM Ecoregions')
for row in curs.fetchall():
    no_spaces = row[1].replace(' ', '')
    no_spaces_no_ands = no_spaces.replace('and', '')
    no_spaces_no_ands_mtn = no_spaces_no_ands.replace('Mountains', 'Mtn')
    ecoregions[row[0]] = [
        row[1],
        no_spaces,
        no_spaces_no_ands,
        no_spaces_no_ands_mtn]

dbhucs = {}
curs.execute("SELECT HUC, Name, EcoregionID FROM HUCs WHERE States Like '%ID%' AND Length(HUC) = 8")
for row in curs.fetchall():
    dbhucs[row[0]] = {'Name': row[1], 'EcoregionID': row[2]}

# Load CSV file from USU
hucs = {}
with open(csv_path, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        raw_huc = row['HUC8FolderName'].split('_')[1]
        raw_Ecoregion = row['Ecoregion']

        if raw_Ecoregion == '-':
            continue

        if raw_huc in hucs:
            raise Exception('HUC already processed {}'.format(raw_huc))

        hucs[raw_huc] = get_ecoregion(raw_Ecoregion, ecoregions)

print(len(hucs), 'HUCS with ecoregion assignment')

matches = 0
misses = 0
for huc, values in dbhucs.items():

    if huc not in hucs:
        continue

    if hucs[huc] == values['EcoregionID']:
        matches += 1
    else:
        misses += 1
        print('HUC {0} was assigned {1} by USU but was assigned {2} by largest area'.format(huc, ecoregions[hucs[huc]][0], ecoregions[values['EcoregionID']][0]))

print(matches, 'Matches')
print(misses, 'Misses')

# Convert HUC assignment to a list for inserting into the database
# final = [(huc, ecoregionid) for huc, ecoregionid in hucs.items()]

# conn = sqlite3.connect(database)
# curs = conn.executemany('INSERT INTO HUCEcoregions (HUC, EcoregionID) Values (?, ?)', final)
# conn.commit()

print('Process complete')
