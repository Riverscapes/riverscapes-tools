import csv


csv_file = '/Users/philip/code/riverscapes/riverscapes-tools/packages/brat/database/data/Watersheds.csv'
sql_file = '/Users/philip/code/riverscapes/brat_parameters/docker/initdb/08_watersheds.sql'


input_file = csv.DictReader(open(csv_file))

with open(sql_file, "w") as f:
    for row in input_file:
        f.write("INSERT INTO watersheds (watershed_id, name, area_sqkm, states, geometry, qlow, q2, max_drainage, ecoregion_id, notes, metadata) VALUES ('{}', '{}', {},'{}', NULL, '{}', '{}', {}, {}, {}, {});\n".format(
            row['WatershedID'],
            row['Name'].replace("'", "''"),
            row['AreaSqKm'] if row['AreaSqKm'] else 'NULL',
            row['States'].replace(',', '_'),
            row['QLow'],
            row['Q2'],
            row['MaxDrainage'] if row['MaxDrainage'] else 'NULL',
            row['EcoregionID'] if row['EcoregionID'] else 'NULL',
            "'" + row['Notes'] + "'" if row['Notes'] else 'NULL',
            "'" + row['Metadata'] + "'" if row['Metadata'] else 'NULL'))
