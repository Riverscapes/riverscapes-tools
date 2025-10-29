import os
import argparse
from osgeo import ogr
from rsxml import dotenv
from rscommons.download import unzip
import shutil
import csv


def sum_lengths(huc_csv):

    # Load list of required HUCs
    hucs = {}
    with open(huc_csv) as csvfile:
        for row in csv.reader(csvfile, delimiter=','):
            hucs[row[0]] = 0.0

    # Load list of zip archives in the same folder as the CSV
    folder = os.path.dirname(huc_csv)
    zips = []
    for r, d, f in os.walk(folder):
        for file in f:
            if '.zip' in file:
                zips.append(os.path.join(r, file))

    driver = ogr.GetDriverByName('OpenFileGDB')

    missing_zips = []
    errors = []
    for huc in hucs:

        # Find the zip
        zipfile = None
        for azip in zips:
            if huc in azip:
                zipfile = azip
                break

        if not zipfile:
            missing_zips.append(huc)
            continue

        file_name = os.path.splitext(os.path.basename(zipfile))[0]
        unzip_path = os.path.join(folder, file_name)
        unzip(azip, unzip_path)

        try:
            gdb = os.path.join(unzip_path, file_name + '.gdb')
            dataset = driver.Open(gdb, 0)
            layer = dataset.GetLayer("NHDFlowline")
            # Limit to perennial streams
            layer.SetAttributeFilter("FCode = '46006'")

            for feature in layer:
                hucs[huc] += feature.GetField('LengthKM')

        except Exception as e:
            errors.append(huc)

        # Delete the unzipped file geodatabase to ensure disk doesn't fill up!
        shutil.rmtree(unzip_path)

    # Output lengths to CSV file. NOTE DELIMETER is tab (for easy import into spreadsheet)
    output_csv = os.path.join(folder, 'intermountain_lengths.csv')
    with open(output_csv, mode='w') as csvfile:
        writer = csv.writer(csvfile, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for huc, length in hucs.items():
            writer.writerow([huc, length])

    print('Lengths of flow lines for', len(hucs), 'HUCs written to', output_csv)
    write_hucs_to_csv(missing_zips, 'Missing NHD downloads logged to', folder, 'missing_nhd.csv')
    write_hucs_to_csv(errors, 'errors unzipping and summing lengths', folder, 'data_read_errors.csv')
    print('Process complete. Lengths written to', output_csv)


def write_hucs_to_csv(hucs, message, folder, file_name):

    file_path = os.path.join(folder, file_name)
    if os.path.isfile(file_path):
        os.remove(file_path)

    print(len(hucs), message, file_path)
    if len(hucs) > 0:
        with open(file_path, mode='w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            [writer.writerow([huc]) for huc in hucs]


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('csv', help='CSV file identifying four digit HUCs', type=argparse.FileType('r'))
    args = dotenv.parse_args_env(parser)

    sum_lengths(args.csv.name)


if __name__ == '__main__':
    main()
