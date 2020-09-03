# -------------------------------------------------------------------------------
# Name:     Import Drainage Area Thresholds
#
# Purpose:  Import values from the Idaho BRAT CSV into the SQLite database
#
# Author:   Philip Bailey
#
# Date:     13 Aug 2019
#
# -------------------------------------------------------------------------------
import argparse
import sqlite3
import csv


def import_dathresh(dacsv, database):

    values = []
    with open(dacsv, newline='') as csvfile:
        csvfile.readline()
        data = csv.reader(csvfile, delimiter=',')
        for row in data:

            name, hucstr = str.split(row[0], '_')
            dathresh = float(row[1])
            values.append((dathresh, int(hucstr)))

    conn = sqlite3.connect(database)
    conn.executemany("UPDATE Watersheds SET MaxDrainage = ? WHERE WatershedID = ?", values)
    conn.commit()
    conn.close()

    print('Process completed successfully')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('csv', help='Path to CSV file being imported', type=argparse.FileType('r'))
    parser.add_argument('database', help='Path to SQLite database', type=argparse.FileType('r'))
    args = parser.parse_args()

    import_dathresh(args.csv.name, args.database.name)


if __name__ == '__main__':
    main()
