import os
import csv
import argparse
from rsxml import dotenv
from rscommons.national_map import get_nhdhr_url
from rscommons.download import download_file

# Science base GUID for NHD Plus HR parent item
nhdhr_parent = '57645ff2e4b07657d19ba8e8'


def download_nhd(huc_csv):

    hucs = []
    with open(huc_csv) as csvfile:
        for row in csv.reader(csvfile, delimiter=','):
            hucs.append(row[0])

    download_folder = os.path.dirname(huc_csv)
    downloaded = []
    errors = []
    skipped = []
    for huc4 in hucs:
        try:
            url = get_nhdhr_url(huc4)
            local = os.path.join(download_folder, os.path.basename(url))

            if os.path.isfile(local):
                skipped.append(huc4)
            else:
                download_file(url, download_folder, force_download=False)
                downloaded.append(local)

        except Exception as e:
            errors.append(huc4)

    print(len(hucs), 'HUCs identified in CSV file.')
    print(len(downloaded), 'NHD archives successfully downloaded')

    if len(skipped) > 0:
        print(len(skipped), 'HUCs skipped because download already exists.')

    errors_path = os.path.join(download_folder, 'download_errors.csv')
    if os.path.isfile(errors_path):
        os.remove(errors_path)

    print(len(errors), 'errors encountered downloading 4 digit NHD Plus HR data')
    if len(errors) > 0:
        with open(errors_path, mode='w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            [writer.writerow([huc]) for huc in errors]
        print('Errors written to', errors_path)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('csv', help='CSV of 4 digit HUC IDs', type=argparse.FileType('r'))

    args = dotenv.parse_args_env(parser)

    download_nhd(args.csv.name)


if __name__ == '__main__':
    main()
