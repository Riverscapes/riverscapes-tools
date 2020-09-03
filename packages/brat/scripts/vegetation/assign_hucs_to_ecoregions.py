# -------------------------------------------------------------------------------
# Name:     Ecoregion HUC assignment
#
# Purpose:  Temporary preparation script for assigning HUCs with the ecoregion
#           in which they have the largest area.
#
# Author:   Philip Bailey
#
# Date:     29 Nov 2019
#
# -------------------------------------------------------------------------------
import csv
import os
import sqlite3
from osgeo import ogr, osr
import json
from rscommons import dotenv
from shapely.geometry import shape, mapping
import argparse


def assign_hucs_to_ecoregions(huc_path, database, ecoregion_path):
    """Assign each HUC to an ecoregion by calculating the ecoregion
    in which the largest area exists

    Arguments:
        hucs {string} -- path to file geoedatabase of the national eight digit HUCs
        database {string} -- Path to the BRAT SQLite database
        ecoregions {string} -- Path to the national Shapefile of level III ecoregions (not carved up by US states)
    """

    hucs = {}
    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute("SELECT WatershedID, Name FROM Watersheds")
    for row in curs.fetchall():
        hucs[row[0]] = {'Name': row[1]}

    ecoregions = {}
    curs.execute('SELECT EcoregionID, Name FROM Ecoregions')
    for row in curs.fetchall():
        ecoregions[row[1]] = row[0]

    # Open the national geodatabase layer of eight digit HUC boundaries
    driver = ogr.GetDriverByName("OpenFileGDB")
    huc_data_source = driver.Open(huc_path, 0)
    huc_layer = huc_data_source.GetLayer('WBDHU8')
    huc_srs = huc_layer.GetSpatialRef()

    # Open the national Shapefile layer of level 3 ecoregions
    driver = ogr.GetDriverByName('ESRI Shapefile')
    eco_data_source = driver.Open(ecoregion_path, 0)
    eco_layer = eco_data_source.GetLayer()
    ecoregion_srs = eco_layer.GetSpatialRef()

    # Get the transformation required to convert to the target spatial reference
    transform = osr.CoordinateTransformation(huc_srs, ecoregion_srs)

    # Load all the HUC geometries
    for feature in huc_layer:
        huc = feature.GetField('HUC8')
        states = feature.GetField('STATES')

        if huc not in hucs:
            # print('HUC {0} in national geodatabase is missing from BRAT database'.format(huc))
            continue

        geom = feature.GetGeometryRef()
        geom.Transform(transform)
        featobj = json.loads(geom.ExportToJson())
        hucs[huc]['Geometry'] = shape(featobj)
        hucs[huc]['States'] = states

    # Loop over all the HUCs again and find the ecoregions that have the largest area
    for huc, values in hucs.items():
        print('HUC', huc, 'in states', values['States'])

        try:
            # Filter the ecoregions to just those that intersect the current HUC
            eco_layer.SetSpatialFilter(ogr.CreateGeometryFromJson(json.dumps(mapping(values['Geometry']))))
            for feature in eco_layer:
                geom = feature.GetGeometryRef()
                featobj = json.loads(geom.ExportToJson())
                eco_poly = shape(featobj)

                if eco_poly.intersects(values['Geometry']):
                    area = eco_poly.intersection(values['Geometry']).area
                    if 'MaxArea' not in values or area > values['MaxArea']:
                        values['MaxArea'] = area
                        values['MaxEcoregion'] = feature.GetField('US_L3NAME')
                        values['MaxEcoregionID'] = ecoregions[feature.GetField('US_L3NAME')]

            if 'MaxEcoregionID' in values:
                curs.execute('UPDATE Watersheds SET EcoregionID = ? WHERE WatershedID = ?', [values['MaxEcoregionID'], huc])
        except Exception as ex:
            print('Error processing', huc, 'in states', values['States'])

    print('Process complete')
    conn.commit()


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('hucs', help='National geodatabase of eight digit HUCs', type=str)
    parser.add_argument('database', help='BRAT database', type=argparse.FileType('r'))
    parser.add_argument('ecoregions', help='Ecoregion Shapefile (not intersected with US states)', type=argparse.FileType('r'))
    args = dotenv.parse_args_env(parser)

    assign_hucs_to_ecoregions(args.hucs, args.database.name, args.ecoregions.name)


if __name__ == '__main__':
    main()
