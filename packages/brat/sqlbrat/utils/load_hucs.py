# -------------------------------------------------------------------------------
# Name:     Load HUCs
#
# Purpose:  Loop over a ShapeFile of HUC8 polygons and insert essential
#           information into SQLite database
#
# Author:   Philip Bailey
#
# Date:     12 Aug 2019
#
# -------------------------------------------------------------------------------
import argparse
import sqlite3
import os
import re
from osgeo import ogr


def load_hucs(polygons, database):

    # Get the input flow lines layer
    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.Open(polygons, 0)
    layer = data_source.GetLayer()
    print('{:,} features in polygon ShapeFile {}'.format(layer.GetFeatureCount(), polygons))

    values = []
    for inFeature in layer:
        values.append((inFeature.GetField('HUC8'), inFeature.GetField('NAME'), inFeature.GetField('AREASQKM')))

    # Open connection to SQLite database. This will create the file if it does not already exist.
    conn = sqlite3.connect(database)

    # Fill the table using bulk operation
    print('{:,} features about to be written to database {}'.format(len(values), database))
    conn.executemany("INSERT INTO HUCs (HUC8, Name, AreaSqKm) values (?, ?, ?)", values)
    conn.commit()
    conn.close()

    print('Process completed successfully')


def huc_info(database):

    conn = sqlite3.connect(database)
    curs = conn.cursor()
    conn.execute("SELECT HUC, Name FROM HUCs")

    hucs = {}
    for row in curs.execute("SELECT HUC, Name FROM HUCs").fetchall():
        hucs[row[0]] = row[1]

    conn.close()
    return hucs


def get_hucs_present(top_level_folder, database):

    all_hucs = huc_info(database)

    present_folders = {}
    for subdir, dirs, files in os.walk(top_level_folder):
        for dir in dirs:
            x = re.search('_([0-9]{8})\Z', dir)
            if x:
                present_folders[x[1]] = os.path.join(top_level_folder, subdir, dir)

    present_files = {}
    for huc, path in present_folders.items():
        present_files[int(huc)] = {}

        # TODO: Paths need to be reset
        raise Exception('PATHS NEED TO BE RESET')
        SOMEPATH = os.path.join(os.environ['SOME_PATH'], 'BearLake_16010201/Inputs/04_Anthropogenic/06_LandOwnership/Land_Ownership_01/NationalSurfaceManagementAgency.shp')

        search_items = {
            'Network': os.path.join(path, 'Outputs', 'Output_SHP', '01_Perennial_Network', '02_Combined_Capacity_Model'),
            'Roads': os.path.join(path, 'Inputs', '04_Anthropogenic', '02_Roads', 'Roads_01'),
            'Rail': os.path.join(path, 'Inputs', '04_Anthropogenic', '03_Railroads', 'Railroads_01'),
            'Canals': os.path.join(path, 'Inputs', '04_Anthropogenic', '04_Canals', 'Canals_01', 'NHDCanalsDitches.shp'),
            'LandOwnership': os.path.join(path, 'Inputs', '04_Anthropogenic', '06_LandOwnership', 'Land_Onership_01', SOMEPATH),
            'ExistingVeg': os.path.join(path, 'Inputs', '01_Vegetation', '01_ExistingVegetation', 'Ex_Veg_01')
        }

        for key, folder in search_items.items():
            for root, dirs, files in os.walk(folder):
                for file in files:
                    if file.endswith('.shp'):
                        present_files[int(huc)][key] = os.path.join(root, file)
                    elif file.endswith('.tif'):
                        present_files[int(huc)][key] = os.path.join(root, file)

    print(len(present_files), 'HUCs found in', top_level_folder)
    return present_files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('polygons', help='Path to ShapeFile of HUC8 polygons', type=argparse.FileType('r'))
    parser.add_argument('database', help='Path to SQLite database', type=argparse.FileType('r'))
    args = parser.parse_args()

    load_hucs(args.polygons.name, args.database.name)


if __name__ == '__main__':
    main()
