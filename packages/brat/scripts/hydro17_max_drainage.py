import argparse
from osgeo import ogr
from osgeo import osr
from rscommons.raster_buffer_stats import raster_buffer_stats2
from rscommons.shapefile import get_transform_from_epsg
from shapely.wkb import loads as wkbload


def hydro17_max_drainage(huc_search, precip_raster, wbd, bankfull):
    """ Temporary script to calculate the BRAT maximum drainage area threshold
        Takes HUC8 watershed boundary polygons and finds the mean annual
        precipitation then uses the inverted Beechi and Imaki formula
        to derive drainage area at the specified constant bankfull width.

    Args:
        huc_search (str): feature layer attribute filter string (e.g. '17%' for hydro region 17)
        precip_raster (str): path to the national PRISM annual precipitation raster
        wbd (str): file path to the national watershed boundary dataset (WBD) file geodatabase
        bankfull (float): bankfull width at which drainage area threshold is calculated
    """

    # open watershed boundary file geodatabase
    driver = ogr.GetDriverByName('OpenFileGDB')
    data_source = driver.Open(wbd, 0)
    wbd_layer = data_source.GetLayer('WBDHU8')
    wbd_layer.SetAttributeFilter('HUC8 LIKE \'{}\''.format(huc_search))

    # Need to convert watersheds to the PESG:4269 used by the PRISM raster
    _srs, transform = get_transform_from_epsg(wbd_layer.GetSpatialRef(), 4269)

    watersheds = {}
    for feature in wbd_layer:
        huc = feature.GetField('HUC8')
        states = feature.GetField('states')
        if 'cn' not in states.lower():
            geom = feature.GetGeometryRef()
            geom.Transform(transform)
            watersheds[huc] = wkbload(geom.ExportToWkb())

    stats = raster_buffer_stats2(watersheds, precip_raster)

    for huc, stat in stats.items():
        mean_precip = stat['Mean']
        max_drain = pow(bankfull / (0.177) / (pow(mean_precip, 0.453)), 1 / 0.397)
        print("UPDATE watersheds SET max_drainage = {} WHERE watershed_id = '{}'".format(max_drain, huc))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('huc_search', help='Attribute filter string for which HUC8s to process in WBD', type=str)
    parser.add_argument('precip_raster', help='Path to national PRISM precipitation raster', type=str)
    parser.add_argument('wbd', help='Path to National Watershed Boundary Dataset File Geodatabase', type=str)
    parser.add_argument('--bankfull', help='Bankfull width at which drainage area threshold is calculated', default=30.0, type=float)
    args = parser.parse_args()

    hydro17_max_drainage(args.huc_search, args.precip_raster, args.wbd, args.bankfull)


if __name__ == '__main__':
    main()
