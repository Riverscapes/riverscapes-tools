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
from osgeo import ogr
from osgeo import gdal
import shapely

from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors
from rscommons.util import safe_makedirs, safe_remove_dir

from gnat.__version__ import __version__

initGDALOGRErrors()

cfg = None  # ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/GNAT.xsd', __version__)

LayerTypes = {
    # key: (name, id, tag, relpath)
    'DEM': RSLayer('NED 10m DEM', 'DEM', 'DEM', 'topography/dem.tif'),
    'VBET_NETWORK': RSLayer('VBET Network', 'VBET_NETWORK', 'Vector', 'intermediates/vbet_network.shp'),
}


def confinement(huc, flowlines, active_channel_polygon, confining_margin_polygon, output_folder):
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
    log.info('Confinement v.{}'.format(cfg.version))

    try:
        int(huc)
    except ValueError:
        raise Exception('Invalid HUC identifier "{}". Must be an integer'.format(huc))

    if not (len(huc) == 4 or len(huc) == 8):
        raise Exception('Invalid HUC identifier. Must be four digit integer')

    safe_makedirs(output_folder)

    # Generate confining margins
    confining_margins, floodplain_pockets = generate_confining_margins(active_channel_polygon, confining_margin_polygon, output_folder)


def generate_confining_margins(active_channel_polygon, confining_margin_polygon, output_folder, type="Unspecified"):
    """[summary]

    Args:
        active_channel_polygon (str): featureclass of active channel
        confining_margin_polygon (str): featureclass of confinig margins
        confinement_type (str): type of confinement

    Returns:
        geometry: confining margins polylines
        geometry: floodplain pockets polygons
    """

    # TODO : Unary Union?
    # TODO : 0 buffer polygons?

    floodplain_pockets = active_channel_polygon.difference(confining_margin_polygon)
    confining_margins = confined_areas.boundary.intersection(active_channel_polygon.boundary)

    # TODO : clean/test outputs?

    # TODO : Save outputs

    return confining_margins, floodplain_pockets


def main():
    parser = argparse.ArgumentParser(
        description='Confinement',
        # epilog="This is an epilog"
    )
    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('flowlines', help="NHD Flowlines", type=str)
    parser.add_argument('active_channel_polygon', help='bankfull buffer or other polygon representing the active channel', type=str)
    parser.add_argument('confining_margins_polygon', help='valley bottom or other polygon representing confining margins', type=str)
    parser.add_argument('output_folder', help='Output folder', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("Confinement")
    log.setup(logPath=os.path.join(args.output, "confinement.log"), verbose=args.verbose)
    log.title('Confinement For HUC: {}'.format(args.huc))

    try:
        confinement(args.huc, args.flowlines, args.active_channel_polygon, args.confining_margins_polygon, args.output_folder)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
