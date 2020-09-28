import os
import sqlite3
import argparse
from sqlbrat.lib.plotting import validation_chart
from sqlbrat.lib.plotting import xyscatter


def max_drainage(database):

    conn = sqlite3.connect(database)
    curs = conn.cursor()
    # curs.execute('SELECT TotDASqkm, Bankfull FROM Reaches')
    # values = [(row[0], row[1]) for row in curs.fetchall()]

    # conn.close()

    # file_path = os.path.join(os.path.dirname(database), 'max_drainage.png')
    # xyscatter(values, 'Drainage Area (sqkm)', 'Bankfull Width (m)', 'Drainage Area to Bankfull Width', file_path, one2one=False)

    curs.execute("""
    SELECT RS.HUC8, RSDA, MaxDrainage FROM
        (SELECT max(totDASqKm) RSDA, Count(totDASqKm), HUC8 FROM Reaches WHERE Bankfull >= 20 GROUP BY HUC8) RS
        INNER JOIN (SELECT huc8, MaxDrainage FROM Idaho) I ON RS.HUC8 = I.HUC8
        """)

    values = [(row[1], row[2]) for row in curs.fetchall()]
    file_path = os.path.join(os.path.dirname(database), 'idaho_compare.png')
    xyscatter(values, 'Min Drainage Area With BF > 20m (sqkm)', 'USU Max Drainage Idaho', 'Idaho Max Drainage Compared to Bankfull Calcs', file_path, one2one=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to SQLite database', type=str)
    args = parser.parse_args()

    max_drainage(args.database)


if __name__ == '__main__':
    main()
