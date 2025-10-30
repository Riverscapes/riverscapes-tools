import sqlite3
import os
from rsxml.util import safe_remove_dir
from cybercastor.lib.file_download import download_files

brat_dir = '/mnt/c/Users/jordang/Documents/Riverscapes/data/brat'
huc = 16010101

download_files('production', 'brat', huc, 'brat.gpkg')

lhf = 0
qr = 0
lti = 0
na = 0

for direc in os.listdir(brat_dir):
    if str(huc) in direc:
        brat_gpkg = os.path.join(brat_dir, direc, 'brat.gpkg')
        conn = sqlite3.connect(brat_gpkg)
        curs = conn.cursor()
        curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Opportunity FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Opportunity;""")
        for row in curs:
            if row[1] == 'Easiest - Low-Hanging Fruit':
                lhf += row[0]
            elif row[1] == 'Straight Forward - Quick Return':
                qr += row[0]
            elif row[1] == 'Strategic - Long-Term Investment':
                lti += row[0]
            else:
                na += row[0]

        safe_remove_dir(os.path.join(brat_dir, direc))

print(f'Low-Hanging Fruit: {lhf}')
print(f'Quick Return: {qr}')
print(f'Long-Term Investment: {lti}')
print(f'Not Applicable: {na}')
