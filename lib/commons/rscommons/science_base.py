# Name:     Science Base
#
# Purpose:  Discover, download and unzip items from the USGS Science Base catalog.
#

# Author:   Philip Bailey
#
# Date:     15 Jun 2019
# -------------------------------------------------------------------------------
import os
import csv
import re
import sciencebasepy
import zipfile
import requests
import json
import sys
import time
import shapely.geometry
from typing import Dict
from rscommons.download import download_unzip
from rscommons.shapefile import get_geometry_union
from rscommons import Logger

# The ID of the science base parent item for 10m DEMs
# https://www.sciencebase.gov/catalog/item/4f70aa71e4b058caae3f8de1
ned_parent = '4f70aa9fe4b058caae3f8de5'
nhd_parent = '5136012ce4b03b8ec4025bf7'
ntd_parent = '4f70b1f4e4b058caae3f8e16'
nhdhr_parent = '57645ff2e4b07657d19ba8e8'

us_states = {
    'AL': 'Alabama',
    'AK': 'Alaska',
    'AZ': 'Arizona',
    'AR': 'Arkansas',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'IA': 'Iowa',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'ME': 'Maine',
    'MD': 'Maryland',
    'MA': 'Massachusetts',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MS': 'Mississippi',
    'MO': 'Missouri',
    'MT': 'Montana',
    'NE': 'Nebraska',
    'NV': 'Nevada',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NY': 'New York',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VT': 'Vermont',
    'VA': 'Virginia',
    'WA': 'Washington',
    'WV': 'West Virginia',
    'WI': 'Wisconsin',
    'WY': 'Wyoming',
    'CN': 'Canada',
    'MX': 'Mexico'
}


def download_shapefile_collection(url, download_folder, unzip_folder, force_download=False):
    """
    Download the one and only item from Science base and unzip it.
    :param url: URL of the Science Base catalog item
    :param download_folder: Folder where the NHD zip will be downloaded
    :param unzip_folder: Folder where downloaded files will be unzipped
    :param force_download: The download will always be performed if this is true.
    Otherwise the download will be skipped if this is false and the file exists
    :return: Dictionary of all ShapeFiles contained in the NHD zip file.
    """

    log = Logger('Download Shapefile Collection')

    # download and unzip the archive. Note: leftover files are a possibility
    # so we allow one retry because unzip can clean things up
    final_unzip_folder = download_unzip(url, download_folder, unzip_folder, force_download)

    # Build a dictionary of all the ShapeFiles within the archive.
    # Keys will be the name of the ShapeFile without extension (e.g. WBDHU8)
    shapefiles = {}
    for root, _subFolder, files in os.walk(final_unzip_folder):
        for item in files:
            if item.endswith('.shp'):
                shapefiles[os.path.splitext(os.path.basename(item))[0]] = os.path.join(root, item)

    log.info('{} shapefiles identified.'.format(len(shapefiles)))
    return shapefiles


def get_dem_urls(vector_path, buffer_dist):
    """
    Retrieve a list of all DEM rasters within the polygons found in ShapeFile

    Note that this query used to include a browseType = citation but some
    NED DEM catalog items are not tracked as citations and they get omitted.
    :param vector_path: Polygon ShapeFile path
    :param buffer_dist: Distance in DEGREES to buffer the polygons found in ShapeFile
    :return: List of HTTPS download URLs for DEMs
    """

    # Get a union of all polygon features in the ShapeFile
    # Science Base API calls cannot handle very long lpolygon vertex lists.
    # Simplify the polygon so that there are few enough vertices to shorten the WKT
    # Resorting to using polygon envelope to guarantee successful API request
    polygon = get_geometry_union(vector_path, 4326)
    buffered = polygon
    if buffer_dist:
        buffered = polygon.buffer(buffer_dist)

    # simple = buffered.simplify(0.01)

    # Experimentation with using the bounding rectangle instead of the polygon.
    # simple = polygon.envelope
    # Cut and paste the dump text below into a json file and then drag it into QGIS
    # raw = shapely.geometry.mapping(simple)
    # dump = json.dumps(raw)
    # print(dump)

    # Spatial query containing the simplified polygon WKT
    spatial_query = "spatialQuery={}".format({
        "wkt": buffered.envelope.wkt,
        "relation": "intersects",
        "fields": "Bounds"
    })

    # TODO: Is the conjunction needed below?
    sbquery = {
        'ancestors': ned_parent,
        'filter1': 'tags=IMG',
        'conjunction': 'tags=OR',
        'filter3': spatial_query
    }

    urls = _get_url(sbquery)

    # IMGs might not be available. Fall back to Geotiff
    # NOTE: We can't just do an OR here in case both IMG and GeoTIFF are present because then
    # We'll have overlapping rasters and download too much stuff
    if len(urls) < 1:
        sbquery = {
            'ancestors': ned_parent,
            'filter1': 'tags=GeoTIFF',
            'conjunction': 'tags=OR',
            'filter2': spatial_query
        }
        urls = _get_url(sbquery)

    if len(urls) < 1:
        log = Logger('Science Base')
        log.error('Science Base Query: {}'.format(sbquery))
        raise Exception('No DEM rasters identified on Science Base')

    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'ned_urls.csv'), 'rt') as f:
        reader = csv.reader(f)
        data = [item[0] for item in list(reader)]

    clean_urls = []
    lat_long_used = []
    for url in urls:
        # Find Lat Long sequence in url
        lat_long = url.split('/')[8]
        if lat_long in lat_long_used:
            # Only one url per lat/long
            continue
        if url in data:
            # use url if its in the list
            clean_urls.append(url)
        else:
            # Find corresponding urls for lat long
            candidate_urls = [val for val in data if lat_long in val]
            if len(candidate_urls) == 0:
                log = Logger('Science Base')
                log.error(f'Unable to find valid download url for: {url}')
                raise Exception(f'Unable to find valid download url for: {url}')
            # Append the newest dem
            clean_urls.append(candidate_urls[-1])
        lat_long_used.append(lat_long)

    return clean_urls


def get_nhd_url(huc8):
    """
    Get the download URL for the specified HUC 8 original NHD
    :param huc8: HUC 8 code as either integer or string
    :return: List with a single download HTTPS URL for the HUC item
    """
    return _get_shapefile_urls(nhd_parent, 'Shapefile', 'HU8_{}'.format(huc8))


def get_nhdhr_url(huc4):
    """
    Get the download URL for the specified HUC 4 from NHD Plus HR
    :param huc4: HUC 4 code as either integer or string
    :return: List with a single download HTTPS URL for the HUC item
    """

    return _get_shapefile_urls(nhdhr_parent, 'FileGDB', 'HU4_{}'.format(huc4))


def get_ntd_urls(states):
    """
    Get the download URLs for all the states specified in the list
    :param states: List of state names for which you want transportation data
    :return: List with all the download HTTPS URL for the science base items
    """
    urls = {}
    for state in states:
        if state.lower() not in ['canada', 'mexico']:
            urls[state] = _get_shapefile_urls(ntd_parent, 'Shapefile', state)

    return urls


def _get_shapefile_urls(parent, file_format, tag):

    sbquery = {
        'ancestors': parent,
        'filter0': 'tags={}'.format(tag),
        'filter1': 'tags={}'.format(file_format)
    }

    # Query Science Base for the one and only item
    url = _get_url(sbquery)
    # filter the list to only include files ending in GDB.zip
    if file_format == 'FileGDB':
        url = [val for val in url if val.endswith('GDB.zip')]
    if len(url) == 0:
        raise Exception('Failed to identify Science Base item with tag "{}"'.format(tag))
    if len(url) > 1:
        #     raise Exception('More than one Science Base item identified with tag "{}"'.format(tag))
        url = [url[i] for i, val in enumerate(url) if tag in val]
        if len(url) == 0:
            raise Exception('Failed to identify Science Base item with tag "{}"'.format(tag))

        return url[0]
    else:
        return url[0]


def _get_url(params: Dict[str, str]):
    """
    Call Science Base API with the argument params and return list of download URLs
    :param params: Science Base params object
    :return: List of HTTPS download URLs for items on S3
    """

    log = Logger('Download')
    log.info('Science base query: {}'.format(params))

    sb = sciencebasepy.SbSession()
    items = sb.find_items(params)

    log.info('{} Science base item(s) identified.'.format(items['total']))

    urls = []
    while items and 'items' in items:
        for item in items['items']:
            result = sb.get_item(item['id'])
            for weblink in result['webLinks']:
                if weblink['type'] == 'download':
                    urls.append(weblink['uri'])

        # pylintrc is freaking out about sb.next being "not callable"
        # I don't know what that means but it's just an annoyance
        items = sb.next(items)

    return urls
