import requests
import csv

# url = f'https://waterdata.usgs.gov/nwis/inventory?site_no=010642505&format=rdb'
# response = requests.get(url, timeout=10)
# if response.status_code == 200:
#     reader = csv.reader(response.text.splitlines(), delimiter='\t')
#     for row in reader:
#         if len(row) > 1 and 'agency_cd' in row[0]:
#             ix = row.index('contrib_drain_area_va')
#             ix2 = row.index('drain_area_va')
#     reader2 = csv.reader(response.text.splitlines(), delimiter='\t')
#     for row in reader2:
#         if len(row) > 1 and 'USGS' in row[0]:
#             print(row[ix], row[ix2], response.status_code)

print(9.43E-29 * 5000 ** 11.88)