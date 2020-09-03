import csv

landuses = {}
with open(csv_file, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        raw_value = int(row['Value'])
        raw_name = row['EVT_NAME']
        raw_VegCode = int(row['VEG_CODE'])
