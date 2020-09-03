import sqlite3
import json
import argparse
import os
from osgeo import ogr, osr
from shapely.geometry import shape, mapping
from rscommons import dotenv
# idaho_regions = '/SOMEPATH/StreamStats/Idaho_4269/idaho_4269.shp'
# streamstatsdb = '/SOMEPATH/NationalProject/streamstats/NSS_v6_2018-01-12.sqlite'
# streamstatsgdb = '/SOMEPATH/StreamStats/SS_regionPolys_20191017/SS_regionPolys.gdb'
# watersheds = '/SOMEPATH/WatershedBoundaries/WBD_National_GDB/WBD_National_GDB.gdb'
# brat = '/SOMEPATH/beaver/pyBRAT4/database/brat_template.sqlite'


def load_regional_equationS(streamstatsdb, streamstatsgdb, watersheds, brat):  # idaho_regions

    # Load Watersheds
    conBRAT = sqlite3.connect(brat)
    curBRAT = conBRAT.cursor()
    curBRAT.execute("SELECT WatershedID, Name, States FROM Watersheds WHERE (Metadata = 'CRB')")
    hucs = {row[0]: {'Name': '{} ({})'.format(row[1], row[2]), 'Q2': None, 'QLow': None}for row in curBRAT.fetchall()}
    print(len(hucs), 'HUCs loaded')

    # Load HUC geometries
    fgdbdriver = ogr.GetDriverByName('OpenFileGDB')
    dsWBD = fgdbdriver.Open(watersheds, 0)
    lrWBD = dsWBD.GetLayer('WBDHU8')
    huc_spatial_ref = lrWBD.GetSpatialRef()
    for huc, values in hucs.items():
        lrWBD.SetAttributeFilter("HUC8 = '{}'".format(huc))
        for feature in lrWBD:
            poly = shape(json.loads(feature.GetGeometryRef().ExportToJson()))
            poly = poly.buffer(0)
            values['Geometry'] = poly
            values['Area'] = feature.GetField('AREASQKM')
            values['States'] = feature.GetField('STATES')

    for equation_view, db_column in [('vwPeakFlows', 'Q2'), ('vwLowFlows', 'QLow')]:

        if equation_view == 'vwPeakFlows':
            continue

        regions = get_equations(huc_spatial_ref, streamstatsdb, streamstatsgdb, equation_view, db_column)

        missing = 0
        success = 0
        for huc, values in hucs.items():
            hgeom = values['Geometry']
            max_area = None
            for region, rvalues in regions.items():
                rgeom = rvalues['Geometry']

                if rgeom.intersects(hgeom):
                    if not max_area or max_area < rgeom.intersection(hgeom).area:
                        max_area = rgeom.intersection(hgeom).area
                        values[db_column] = rvalues[db_column]

            if values['Q2']:
                print('HUC', huc, 'Peak Flow is', values['Q2'])
                success += 1
            else:
                print(huc, 'does not intersect any regions')
                missing += 1

        print('Analysis complete.', success, 'success, and ', missing, 'missing')

        for huc, values in hucs.items():
            if values['Q2']:
                conBRAT.execute('UPDATE Watersheds SET Q2 = ?, Notes = NULL WHERE WatershedID = ?', [values['Q2'], huc])

        conBRAT.commit()

    print('Process completed successfully.')


def get_equations(huc_spatial_ref, streamstatsdb, streamstatsgdb, equation_view, db_column):
    # Load equations by region
    conSS = sqlite3.connect(streamstatsdb)
    curSS = conSS.cursor()
    curSS.execute("SELECT RegionName, Equation FROM vwPeakFlows")
    equations = {row[0]: row[1] for row in curSS.fetchall()}
    print(len(equations), 'regional equations loaded')

    driver = ogr.GetDriverByName('OpenFileGDB')
    dsSS = driver.Open(streamstatsgdb, 0)
    regions = {}
    for layer_name in ['regions_ID', 'regions_OR', 'regions_WA', 'regions_MT']:
        layerSS = dsSS.GetLayer(layer_name)
        # layerSS.SetAttributeFilter("Name Like 'Peak_Flow%'")

        # Get the spatial reference of the geodatabase layer
        gdb_spatial_ref = layerSS.GetSpatialRef()
        transform = osr.CoordinateTransformation(gdb_spatial_ref, huc_spatial_ref)

        for feature in layerSS:
            region_name = feature.GetField('Name')

            if region_name not in equations:
                print('WARNING: Region {0} not found in equations'.format(region_name))
                continue

            geom = feature.GetGeometryRef()
            geom.Transform(transform)
            poly = shape(json.loads(geom.ExportToJson()))
            if poly.is_empty:
                print('Empty region geomtry: {}'.format(region_name))
                continue
            elif poly.is_valid is False:
                clean = poly.buffer(0)
                if (clean.is_valid is False):
                    print('Invalid region geomtry: {}'.format(region_name))
                    continue
                else:
                    poly = clean

            if region_name in regions:
                print('WARNING: Region name already exists')

            regions[region_name] = {
                'Geometry': poly,
                db_column: equations[region_name]
            }

    return regions


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('stream_stats', help='Path to National Stream Stats database', type=argparse.FileType('r'))
    parser.add_argument('ssgdb', help='Stream stats file geodatabase path', type=str)
    parser.add_argument('watersheds', help='National watershed boundary file geodatabase', type=str)
    parser.add_argument('brat', help='BRAT SQLite database', type=argparse.FileType('r'))
    # parser.add_argument('idaho', help='Path to Idaho hydrological regions', type=argparse.FileType('r'))

    args = dotenv.parse_args_env(parser)

    load_regional_equationS(args.stream_stats.name, args.ssgdb, args.watersheds, args.brat.name)


if __name__ == '__main__':
    main()
