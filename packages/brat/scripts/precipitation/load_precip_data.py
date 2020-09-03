# Load PRISM precip data into BRAT SQLite database
# Philip Bailey
# 25 Oct 2019
import csv
import sqlite3
import statistics

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')

precip_csv = '/SOMEPATH/precipitation/ppt2010s.csv'
database = '/SOMEPATH/BRAT/brat5.sqlite'

# Load Prism monthly mean precipitation values from downloaded CSV
# https://www.sciencebase.gov/catalog/item/59c28f66e4b091459a61d335
mean_precip = []
with open(precip_csv, 'r') as csvfile:
    next(csvfile)
    reader = csv.reader(csvfile)
    for row in reader:
        # Retrieve just the valid integer values
        valint = [int(val) for val in row[1:] if val.isdigit()]
        # Calculate how many years these data points represent
        years = float(len(valint)) / 12.0
        # Convert values into annual precip in mm
        tot = (float(sum(valint)) / 100.0) / years
        mean_precip.append((tot, int(row[0])))

# Reset all the precipitation values
conn = sqlite3.connect(database)
conn.execute('UPDATE Precipitation SET MeanPrecip = NULL')

# Insert the locations into the database
conn.executemany('UPDATE Precipitation SET MeanPrecip = ? WHERE PrismID = ?', mean_precip)
conn.commit()

print(len(mean_precip), 'precipitation data inserted into database')
