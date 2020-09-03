# -------------------------------------------------------------------------------
# Name:     Identify HUC 4s
#
# Purpose:  Identify unique 4 digit HUCs in a Shapefile of 12 digit HUCs
#
# Author:   Philip Bailey
#
# Date:     24 Sep 2019
#
# -------------------------------------------------------------------------------
import argparse
import os
import sys
import csv
from osgeo import ogr
from rscommons import dotenv


def identify_huc(input_path):
    """
    Create a CSV of unique 4 digit HUCs contained in the argument shapefile
    :param input_path: Path to Shapefile of 12 digit HUCs
    :return:
    """

    # Open the ShapeFile and loop over all features to get the four digit HUC
    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.Open(input_path, 0)
    layer = data_source.GetLayer()

    huc4s = []
    for feature in layer:
        huc4 = feature.GetField('HUC12')[:4]
        if huc4 not in huc4s:
            huc4s.append(huc4)

    data_source = None

    # Write the HUC codes to a CSV file
    csv_path = os.path.join(os.path.dirname(input_path), 'intermountain_huc4.csv')
    with open(csv_path, mode='w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        [writer.writerow([huc]) for huc in huc4s]

    print('Process complete. {} four-digit HUCs written to {}'.format(len(huc4s), csv_path))


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('shapefile', help='Shapefile with inter-mountain west HUC 12s', type=argparse.FileType('r'))

    args = dotenv.parse_args_env(parser)

    identify_huc(args.shapefile.name)

    sys.exit(0)


if __name__ == '__main__':
    main()
