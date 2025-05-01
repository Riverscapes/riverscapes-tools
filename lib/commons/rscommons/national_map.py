# Name:     National Map
#
# Purpose:  Discover, download and unzip items from the National Map API.
#
# Date:     July 30 2024
# -------------------------------------------------------------------------------
import os
import csv
from typing import Dict


from rscommons.download import download_unzip
from rscommons.national_map_api import TNM
from rscommons.vector_ops import get_geometry_unary_union
from rscommons import Logger


us_states = {
    "AL": {"statefp": "01", "state_name": "Alabama"},
    "AK": {"statefp": "02", "state_name": "Alaska"},
    "AZ": {"statefp": "04", "state_name": "Arizona"},
    "AR": {"statefp": "05", "state_name": "Arkansas"},
    "CA": {"statefp": "06", "state_name": "California"},
    "CO": {"statefp": "08", "state_name": "Colorado"},
    "CT": {"statefp": "09", "state_name": "Connecticut"},
    "DE": {"statefp": "10", "state_name": "Delaware"},
    "DC": {"statefp": "11", "state_name": "District of Columbia"},
    "FL": {"statefp": "12", "state_name": "Florida"},
    "GA": {"statefp": "13", "state_name": "Georgia"},
    "HI": {"statefp": "15", "state_name": "Hawaii"},
    "ID": {"statefp": "16", "state_name": "Idaho"},
    "IL": {"statefp": "17", "state_name": "Illinois"},
    "IN": {"statefp": "18", "state_name": "Indiana"},
    "IA": {"statefp": "19", "state_name": "Iowa"},
    "KS": {"statefp": "20", "state_name": "Kansas"},
    "KY": {"statefp": "21", "state_name": "Kentucky"},
    "LA": {"statefp": "22", "state_name": "Louisiana"},
    "ME": {"statefp": "23", "state_name": "Maine"},
    "MD": {"statefp": "24", "state_name": "Maryland"},
    "MA": {"statefp": "25", "state_name": "Massachusetts"},
    "MI": {"statefp": "26", "state_name": "Michigan"},
    "MN": {"statefp": "27", "state_name": "Minnesota"},
    "MS": {"statefp": "28", "state_name": "Mississippi"},
    "MO": {"statefp": "29", "state_name": "Missouri"},
    "MT": {"statefp": "30", "state_name": "Montana"},
    "NE": {"statefp": "31", "state_name": "Nebraska"},
    "NV": {"statefp": "32", "state_name": "Nevada"},
    "NH": {"statefp": "33", "state_name": "New Hampshire"},
    "NJ": {"statefp": "34", "state_name": "New Jersey"},
    "NM": {"statefp": "35", "state_name": "New Mexico"},
    "NY": {"statefp": "36", "state_name": "New York"},
    "NC": {"statefp": "37", "state_name": "North Carolina"},
    "ND": {"statefp": "38", "state_name": "North Dakota"},
    "OH": {"statefp": "39", "state_name": "Ohio"},
    "OK": {"statefp": "40", "state_name": "Oklahoma"},
    "OR": {"statefp": "41", "state_name": "Oregon"},
    "PA": {"statefp": "42", "state_name": "Pennsylvania"},
    "RI": {"statefp": "44", "state_name": "Rhode Island"},
    "SC": {"statefp": "45", "state_name": "South Carolina"},
    "SD": {"statefp": "46", "state_name": "South Dakota"},
    "TN": {"statefp": "47", "state_name": "Tennessee"},
    "TX": {"statefp": "48", "state_name": "Texas"},
    "UT": {"statefp": "49", "state_name": "Utah"},
    "VT": {"statefp": "50", "state_name": "Vermont"},
    "VA": {"statefp": "51", "state_name": "Virginia"},
    "WA": {"statefp": "53", "state_name": "Washington"},
    "WV": {"statefp": "54", "state_name": "West Virginia"},
    "WI": {"statefp": "55", "state_name": "Wisconsin"},
    "WY": {"statefp": "56", "state_name": "Wyoming"},
    "MX": {"statefp": "-1", "state_name": "Mexico"},
    "CN": {"statefp": "-1", "state_name": "Canada"},
}


def download_shapefile_collection(url, download_folder, unzip_folder, force_download=False):
    """
    Download the one and only item from TNM and unzip it.
    :param url: URL of the TNM catalog item
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


def _get_urls(params: Dict[str, str]):
    """
    Call TNM API with the argument params and return list of download URLs
    :param params: TNM API params object
    :return: List of HTTPS download URLs for items on S3
    """

    log = Logger('Download')
    log.info('TNM query: {}'.format(params))

    items = TNM.get_items(params)

    log.info('{} item(s) identified.'.format(items['total']))

    urls = []
    for item in items["items"]:
        urls.extend(item["urls"].values())

    return urls


def _get_shapefile_urls(dataset, file_format, region_type, region):
    """
    Get the download URL for the specified dataset and file format
    :param dataset: Dataset name
    :param file_format: File format
    :param region_type: Region type (huc8, huc4, huc2, or state)
    :param region: Region code as either integer or string. State should be passed as FIPS code
    :return: List with a single download HTTPS URL for the item
    """

    # Small chance some HUC will be passed as integer
    region = str(region)

    # Not ideal to change region to statefp here, but couldn't find a better way
    if region_type == 'state':
        # state name is used later to filter excess urls
        state_name = us_states[region]["state_name"]
        region = us_states[region]["statefp"]

    params = {
        "datasets": dataset,
        "prodFormats": file_format,
        "polyType": region_type,
        "polyCode": region,
    }

    # Query TNM for the "one and only" item
    url = _get_urls(params)

    # filter the list to only include files ending in GDB.zip
    if file_format == 'FileGDB':
        url = [val for val in url if val.endswith('GDB.zip')]
    if len(url) == 0:
        raise Exception('Failed to identify National Map item for {} "{}"'.format(region_type, region))

    # TODO maybe address this section running almost every time. TNM API HUC uses bounding boxes so always multiple
    if len(url) > 1:
        # Keyword search for the tag in the URL
        if "huc" in region_type:
            tag = f"H_{region}"
        elif region_type == 'state':
            tag = "_".join(state_name.split())

        url = [val for val in url if tag in val]

        if len(url) == 0:
            raise Exception('Failed to identify National Map item with tag "{}"'.format(tag))

        return url[0]
    else:
        return url[0]


def get_1m_dem_urls(vector_path: str, buffer_dist: int | float) -> list[str]:
    """
    Retrieve a list of all DEM rasters within the polygons found in input layer 

    Note that this query used to include a browseType = citation but some
    NED DEM catalog items are not tracked as citations and they get omitted.
    :param vector_path: path to Shapefile or geopackage layer 
    :param buffer_dist: Distance in DEGREES to buffer the polygons 
    :return: List of HTTPS download URLs for DEMs
    """

    # Get a union of all polygon features in the input
    # Science Base API calls cannot handle very long lpolygon vertex lists.
    # Simplify the polygon so that there are few enough vertices to shorten the WKT
    # Resorting to using polygon envelope to guarantee successful API request

    polygon = get_geometry_unary_union(vector_path)
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

    # Pylint doesn't like this, but it works
    polygon_coords = list(buffered.envelope.exterior.coords)

    params = {
        "polygon": ",".join([f"{lat} {long}" for lat, long in polygon_coords]),
        "datasets": "Digital Elevation Model (DEM) 1 meter",
        "prodFormats": "GeoTIFF",
    }

    log.info(f'TNM API Query params: {params}')
    urls = _get_urls(params)

    if len(urls) < 1:
        log = Logger('The National Map')
        log.error('TNM API Query returned no results.')
        # lsg - think this will be fairly common and shouldn't trigger an exception that bubbles up that way
        raise Exception('No DEM rasters identified on The National Map')

    return urls


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

    # LSG: this didn't work, so replaced with unary version from different module
    # polygon = get_geometry_union(vector_path, 4326)
    polygon = get_geometry_unary_union(vector_path)
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

    # Pylint doesn't like this, but it works
    polygon_coords = list(buffered.envelope.exterior.coords)

    params = {
        "polygon": ",".join([f"{lat} {long}" for lat, long in polygon_coords]),
        "datasets": "Digital Elevation Model (DEM) 1 meter",
        "prodFormats": "GeoTIFF",
    }

    urls = _get_urls(params)

    # lsg we're starting with GeoTiff so this is not needed
    # IMGs might not be available. Fall back to Geotiff
    # NOTE: We can't just do an OR here in case both IMG and GeoTIFF are present because then
    # We'll have overlapping rasters and download too much stuff
    # if len(urls) < 1:
    #     params["prodFormats"] = "GeoTIFF"
    #     urls = _get_urls(params)

    if len(urls) < 1:
        log = Logger('The National Map')
        log.error('TNM API Query: {}'.format(params))
        # lsg - think this will be fairly common and shouldn't trigger an exception that bubbles up that way
        raise Exception('No DEM rasters identified on The National Map')

    return urls
    # lsg what are we doing with this csv list of URLs? do not think they are needed
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
                log = Logger('The National Map')
                log.error(f'Unable to find valid download url for: {url}')
                raise Exception(f'Unable to find valid download url for: {url}')
            # Append the newest dem
            clean_urls.append(candidate_urls[-1])
        lat_long_used.append(lat_long)

    return clean_urls


# NOTE This does not appear to be used anywhere
def get_nhd_url(huc8):
    """
    Get the download URL for the specified HUC 8 original NHD
    :param huc8: HUC 8 code as either integer or string
    :return: List with a single download HTTPS URL for the HUC item
    """
    # return _get_shapefile_urls(nhd_parent, 'Shapefile', 'HU8_{}'.format(huc8))
    return _get_shapefile_urls('National Hydrography Dataset (NHD) Best Resolution', 'Shapefile', 'huc8', huc8)


# NOTE This is also used in BRAT
def get_nhdhr_url(huc4):
    """
    Get the download URL for the specified HUC 4 from NHD Plus HR
    :param huc4: HUC 4 code as either integer or string
    :return: List with a single download HTTPS URL for the HUC item
    """
    return _get_shapefile_urls('National Hydrography Dataset Plus High Resolution (NHDPlus HR)', 'FileGDB', 'huc4', huc4)


def get_ntd_urls(states):
    """
    Get the download URLs for all the states specified in the list
    :param states: List of state names for which you want transportation data
    :return: List with all the download HTTPS URL for The National Map items
    """
    urls = {}
    for state in states:
        if state.upper() not in ["CN", "MX"]:
            # TODO determine if need state name as key or if abbreviation works
            # it is used for folder names in rscontext but maybe okay as long as it's consistent
            key = us_states[state]["state_name"]
            urls[key] = _get_shapefile_urls("National Transportation Dataset (NTD)", 'Shapefile', 'state', state)

    return urls
