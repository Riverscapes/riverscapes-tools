import os
import math
import urllib.request
from osgeo import ogr, gdal


def global_surface_water(vector_path: str, destination_folder: str):

    datasets = ['occurrence', 'change', 'seasonality', 'recurrence', 'transitions', 'extent']

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
            filename = dataset_name + "_" + coords[0] + "_" + coords[1] + "v1_4_2021.tif"
            if os.path.exists(os.path.join(destination_folder, filename)):
                print(destination_folder + filename + ' already exists')
            else:
                url = "http://storage.googleapis.com/global-surface-water/downloads2021/" + dataset_name + "/" + filename
                code = urllib.request.urlopen(url).getcode()
                if code != 404:
                    print("Downloading " + url)
                    urllib.request.urlretrieve(url, os.path.join(destination_folder, filename))  # maybe make it so each is downloaded into separate folder in case the need to be merged
                else:
                    print(url + ' not found')


if __name__ == '__main__':
    vec_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rs_context/16010202/hydrology/WBDHU8.shp'
    dest_fold = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rs_context/16010202'
    global_surface_water(vec_path, dest_fold)
