import os
import requests
import sys
import traceback
import argparse

base_url = 'https://web.corral.tacc.utexas.edu/nfiedata/HAND'

hucs = [
    170101,
    170102,
    170103,
    170200,
    170300,
    170401,
    170402,
    170501,
    170502,
    170601,
    170602,
    170603,
    170701,
    170702,
    170703,
    170800,
    170900,
]


def download_crb_hand(huc, output_dir):

    print('Downloading HAND for HUC {}'.format(huc))

    url = base_url + '/' + str(huc) + '/' + str(huc) + '/hand.tif'
    file_path = os.path.join(output_dir, os.path.basename(url))
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('download_folder', help='Download folder where original raster will be downloaded', type=str)
    parser.add_argument('--epsg', help='EPSG spatial reference of the final HAND raster', type=int, default=4326)
    args = parser.parse_args()

    for huc in hucs:
        try:
            download_crb_hand(huc, args.download_folder)

        except Exception as e:
            print(e)
            traceback.print_exc(file=sys.stdout)

    sys.exit(0)


if __name__ == '__main__':
    main()
