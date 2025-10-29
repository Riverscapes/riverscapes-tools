import argparse
import os
import math
import sys
import traceback
import urllib.request
from osgeo import ogr

from rsxml import Logger


def global_surface_water(vector_path: str, destination_folder: str):

    log = Logger('Global Surface Water')

    datasets = ['occurrence', 'change', 'seasonality', 'recurrence', 'transitions', 'extent']
    urls = []

    # get top left coordinates of tiles to download based on overlap with vector (WBD HUC shapefile)
    corners = []
    vectorDataSource = ogr.GetDriverByName('ESRI Shapefile').Open(vector_path)
    vectorLayer = vectorDataSource.GetLayer()
    for feature in vectorLayer:
        geom = feature.GetGeometryRef()
        bounds = geom.GetEnvelope()  # needs to not be projected so that this returns lat long

        if bounds[0] < 0:
            long = str(math.ceil(abs(bounds[0]) / 10) * 10) + 'W'
        else:
            long = str(math.floor(abs(bounds[0]) / 10) * 10) + 'E'

        if bounds[3] < 0:
            lat = str(math.floor(abs(bounds[3]) / 10) * 10) + 'S'
        else:
            lat = str(math.ceil(abs(bounds[3]) / 10) * 10) + 'N'
        if [long, lat] not in corners:
            corners.append([long, lat])

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # download any tiles that intersect watershed
    for coords in corners:
        for dataset_name in datasets:
            if not os.path.isdir(os.path.join(destination_folder, dataset_name)):
                os.makedirs(os.path.join(destination_folder, dataset_name))
            filename = dataset_name + "_" + coords[0] + "_" + coords[1] + "v1_4_2021.tif"
            if os.path.exists(os.path.join(destination_folder, dataset_name, filename)):
                log.info(destination_folder + dataset_name + filename + ' already exists')
                urls.append("http://storage.googleapis.com/global-surface-water/downloads2021/" + dataset_name + "/" + filename)
            else:
                url = "http://storage.googleapis.com/global-surface-water/downloads2021/" + dataset_name + "/" + filename
                code = urllib.request.urlopen(url).getcode()
                if code != 404:
                    log.info("Downloading " + url)
                    urllib.request.urlretrieve(url, os.path.join(destination_folder, dataset_name, filename))  # maybe make it so each is downloaded into separate folder in case the need to be merged
                    urls.append(url)
                else:
                    log.info(url + ' not found')

    return urls


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('aoi_vector', help='Path to the vector feature class whose area tiles will be downloaded for', type=str)
    parser.add_argument('download_folder', help='Path to a folder where the tiles will be downloaded', type=str)
    args = parser.parse_args()

    log = Logger('Global Surface Water')

    # make sure the output folder exists
    if not os.path.isdir(args.download_folder):
        os.mkdir(args.download_folder)

    try:
        global_surface_water(args.aoi_vector, args.download_folder)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
