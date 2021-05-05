"""Call StreamStats for the most downstream reach in a watershed and retrieve
    the parameters used for discharge calculation.

   This script is incomlplete as on 5 May 2021. I am putting it on hold because the reliability
   of finding the correct point in a watershed to use when calling StreamStats is too error prone.

   Still adding this script to git in case it is useful in the future. 
    """
import sqlite3
from collections import namedtuple
import re
import os
import requests
import argparse
import json
from rscommons import dotenv
from rscommons.classes.vector_classes import get_shp_or_gpkg
from rscommons.database import SQLiteCon
from shapely.geometry import LineString
from shapely.wkb import loads


stream_stats_01_watershed = 'https://streamstats.usgs.gov/streamstatsservices/watershed.geojson?rcode={0}&xlocation={1}&ylocation={2}&crs=4326&includeparameters=false&includeflowtypes=false&includefeatures=true&simplify=true'
stream_stats_02_basin_chars = 'https://streamstats.usgs.gov/streamstatsservices/parameters.json?rcode={0}&workspaceID={1}&includeparameters=true'


def watershed_parameters(flowlines_path, watershed_id):

    # Find the reach with max drainage
    with SQLiteCon(os.path.dirname(flowlines_path)) as database:
        database.curs.execute('SELECT fid, geom FROM network order by TotDASqKm DESC limit 1')
        reach_id = database.curs.fetchone()['fid']

    # Load the geometry for this reach
    point = None
    with get_shp_or_gpkg(flowlines_path) as lyr:
        for feature, _counter, _progbar in lyr.iterate_features(attribute_filter='"fid" = {}'.format(reach_id)):
            line = loads(feature.GetGeometryRef().ExportToWkb())
            # point = line.interpolate(0.5, normalized=True)
            coord = list(line.coords)

            url = stream_stats_01_watershed.format('CA', coord[0][0], coord[0][1])
            response = requests.get(url)
            data = json.loads(response.content)

            url2 = stream_stats_02_basin_chars.format('CA', data['workspaceID'])
            response2 = requests.get(url2)
            data2 = json.loads(response.content)
            print(data)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('reach_network', help='Reach Network FeatureClass', type=str)
    parser.add_argument('watershed_id', help='8 digit watershed HUC code', type=str)
    args = dotenv.parse_args_env(parser)

    watershed_parameters(args.reach_network, args.watershed_id)


if __name__ == '__main__':
    main()
