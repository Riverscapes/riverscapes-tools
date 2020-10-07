#!/usr/bin/env python3
# Name:     Confinement
#
# Purpose:
#
# Author:   Kelly Whitehead
#
# Date:     24 Sep 2020
# -------------------------------------------------------------------------------
import argparse
import sys
import os
import glob
import traceback
import uuid
import datetime
import json
from osgeo import ogr
from osgeo import gdal
from shapely.ops import unary_union
from shapely.wkb import loads as wkb_load
from shapely.geometry import Polygon, MultiPolygon, LineString, mapping

from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors
from rscommons.util import safe_makedirs, safe_remove_dir

from confinement.__version__ import __version__

initGDALOGRErrors()

cfg = (None, "0.0.0")  # ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/GNAT.xsd', __version__)

LayerTypes = {
    # key: (name, id, tag, relpath)
    'DEM': RSLayer('NED 10m DEM', 'DEM', 'DEM', 'topography/dem.tif'),
    'VBET_NETWORK': RSLayer('VBET Network', 'VBET_NETWORK', 'Vector', 'intermediates/vbet_network.shp'),
}


def confinement(huc, flowlines, active_channel_polygon, confining_polygon, output_folder):
    """[summary]

    Args:
        huc ([type]): [description]

    Raises:
        Exception: [description]
        Exception: [description]
        Exception: [description]
        Exception: [description]

    Returns:
        [type]: [description]
    """

    log = Logger("Confinement")
    log.info(f'Confinement v.{0.0}')  # .format(cfg.version))

    try:
        int(huc)
    except ValueError:
        raise Exception('Invalid HUC identifier "{}". Must be an integer'.format(huc))

    if not (len(huc) == 4 or len(huc) == 8):
        raise Exception('Invalid HUC identifier. Must be four digit integer')

    safe_makedirs(output_folder)
    out_gpkg = os.path.join(output_folder, "Confinement.gpkg")
    if os.path.exists(out_gpkg):
        os.remove(out_gpkg)
    log.info(f"Preparing output geopackage: {out_gpkg}")
    driver_gpkg = ogr.GetDriverByName("GPKG")
    driver_gpkg.CreateDataSource(out_gpkg)

    # Generate confining margins
    log.info('Generating Confined Margins')
    confining_margins, floodplain_pockets = generate_confining_margins(active_channel_polygon, confining_polygon, out_gpkg)

    return


def generate_confining_margins(active_channel_polygon, confining_polygon, output_gpkg, type="Unspecified"):
    """[summary]

    Args:
        active_channel_polygon (str): featureclass of active channel
        confining_margin_polygon (str): featureclass of confinig margins
        confinement_type (str): type of confinement

    Returns:
        geometry: confining margins polylines
        geometry: floodplain pockets polygons
    """

    # Load geoms
    driver = ogr.GetDriverByName("ESRI Shapefile")  # GPKG
    data_active_channel = driver.Open(active_channel_polygon, 0)
    lyr_active_channel = data_active_channel.GetLayer()
    data_confining_polygon = driver.Open(confining_polygon, 0)
    lyr_confining_polygon = data_confining_polygon.GetLayer()
    srs = lyr_active_channel.GetSpatialRef()
    geom_active_channel_polygon = unary_union([wkb_load(feat.GetGeometryRef().ExportToWkb()) for feat in lyr_active_channel])
    geom_confining_polygon = unary_union([wkb_load(feat.GetGeometryRef().ExportToWkb()) for feat in lyr_confining_polygon])

    geom_confined_area = geom_active_channel_polygon.difference(geom_confining_polygon)
    geom_confining_margins = geom_confined_area.boundary.intersection(geom_confining_polygon.boundary)
    geom_floodplain_pockets = geom_confining_polygon.difference(geom_active_channel_polygon)

    # TODO : clean/test outputs?

    # Save Outputs to Geopackage

    out_driver = ogr.GetDriverByName("GPKG")
    data_out = out_driver.Open(output_gpkg, 1)
    lyr_out_confining_margins = data_out.CreateLayer('ConfiningMargins', srs, geom_type=ogr.wkbLineString)
    feature_def = lyr_out_confining_margins.GetLayerDefn()
    feature = ogr.Feature(feature_def)
    feature.SetGeometry(ogr.CreateGeometryFromJson(json.dumps(mapping(geom_confining_margins))))
    lyr_out_confining_margins.CreateFeature(feature)
    feature = None

    lyr_out_floodplain_pockets = data_out.CreateLayer('FloodplainPockets', srs, geom_type=ogr.wkbPolygon)
    feature_def = lyr_out_confining_margins.GetLayerDefn()
    feature = ogr.Feature(feature_def)
    feature.SetGeometry(ogr.CreateGeometryFromJson(json.dumps(mapping(geom_floodplain_pockets))))
    lyr_out_floodplain_pockets.CreateFeature(feature)
    feature = None

    data_out = None
    data_active_channel = None
    data_confining_polygon = None

    return geom_confining_margins, geom_floodplain_pockets


def main():
    parser = argparse.ArgumentParser(
        description='Confinement',
        # epilog="This is an epilog"
    )
    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('flowlines', help="NHD Flowlines", type=str)
    parser.add_argument('active_channel_polygon', help='bankfull buffer or other polygon representing the active channel', type=str)
    parser.add_argument('confining_polygon', help='valley bottom or other polygon representing confining boundary', type=str)
    parser.add_argument('output_folder', help='Output folder', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("Confinement")
    log.setup(logPath=os.path.join(args.output_folder, "confinement.log"), verbose=args.verbose)
    log.title('Confinement For HUC: {}'.format(args.huc))

    try:
        confinement(args.huc, args.flowlines, args.active_channel_polygon, args.confining_polygon, args.output_folder)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
