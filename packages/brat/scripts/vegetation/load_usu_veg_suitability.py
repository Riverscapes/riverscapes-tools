# -------------------------------------------------------------------------------
# Name:     Existing
#
# Purpose:  Read existing vegetation types from CSV file provided by USU and match
#           them up to official values.
#
# Author:   Philip Bailey
#
# Date:     24 Oct 2019
# -------------------------------------------------------------------------------

import csv
import sqlite3

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')

database = '/SOMEPATH/BRAT/brat5.sqlite'
csv_files = {
    # 'Existing': {
    #     'CSV': '/SOMEPATH/GISData/Landfire/ExistingVegetation_AllEcoregions.csv',
    #     'NameField', 'EVT_NAME',
    #     'AdditionalFields': True
    # },
    'Historic': {
        'CSV': '/SOMEPATH/GISData/BRAT/IdahoBRAT/HistoricVegetation_AllEcoregions.csv',
        'NameField': 'BPS_NAME',
        'AdditionalFields': False
    }
}


def get_ecoregion(original, official):

    match = None
    for id, names in official.items():
        if original in names:
            if match:
                raise Exception('Already matched ecoregion')
            match = id
            break

    if not match:
        raise Exception('No ecoregion found for {}'.format(original))

    return match


# Load the ecoregions
ecoregions = {}
ecoregions_rev = {}
conn = sqlite3.connect(database)
curs = conn.cursor()
curs = curs.execute('SELECT EcoregionID, Name FROM Ecoregions')
for row in curs.fetchall():
    ecoregions[row[0]] = [row[1], row[1].replace(' ', ''), row[1].replace('and', '').replace(' ', '')]
    ecoregions_rev[row[1]] = row[0]
print(len(ecoregions), 'ecoregions loaded from database')


for table, props in csv_files.items():
    print('Processing', table, 'vegetation')

    # Load Existing vegetation types
    veg_types = {}
    veg_types_rev = {}
    curs = curs.execute('SELECT ID, Name FROM {}Veg'.format(table))
    for row in curs.fetchall():
        veg_types[row[0]] = row[1]
        veg_types_rev[row[1]] = row[0]
    print(len(veg_types), '{} vegetation types loaded from database'.format(table.lower()))

    # Load CSV file from USU
    suitabilities = {}
    with open(props['CSV'], 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            raw_value = int(row['Value'])
            raw_name = row[props['NameField']]
            raw_VegCode = int(row['VEG_CODE'])
            raw_ecoregions = [get_ecoregion(ecoregion.strip(), ecoregions) for ecoregion in row['Regions'].split(',')]

            raw_LUCode = None
            raw_LUIClass = None
            if props['AdditionalFields'] is True:
                raw_LUCode = float(row['LU_CODE'])
                raw_LUIClass = row['LUI_Class']

            if raw_value not in suitabilities:
                suitabilities[raw_value] = {}

            for eco in raw_ecoregions:
                if eco not in suitabilities[raw_value]:
                    suitabilities[raw_value][eco] = (raw_VegCode, raw_LUCode, raw_LUIClass)
                else:
                    if suitabilities[raw_value][eco][0] != raw_VegCode:
                        raise Exception('Duplicate values with different veg codes')

    final = []
    for veg, eco in suitabilities.items():
        for ecoregion, suitability in eco.items():
            if props['AdditionalFields'] is True:
                final.append((veg, ecoregion, suitability[0], suitability[1], suitability[2]))
            else:
                final.append((veg, ecoregion, suitability[0]))

    print(len(final), table.lower(), 'suitability values found')

    fields = ['VegetationID', 'EcoregionID', 'Suitability']
    if props['AdditionalFields'] is True:
        fields.append('LU_CODE')
        fields.append('LUI_Class')

    curs = conn.executemany('INSERT INTO {0}VegSuitability ({1}) Values ({2})'.format(table, ','.join(fields), ','.join('?' * len(fields))), final)

# conn.rollback()
conn.commit()

print('Process complete')
