import os
import json
import argparse
from osgeo import ogr
from osgeo import osr
from shapely.geometry import shape, mapping
from rscommons import Logger, dotenv
from rscommons.shapefile import get_geometry_unary_union
from rscommons.shapefile import _rough_convert_metres_to_shapefile_units
from rscommons.shapefile import copy_feature_class
from rscommons.shapefile import delete_shapefile


def clip_ownership(boundary, ownership, out_path, output_epsg, buffer_meters):

    log = Logger('Ownership')

    log.info('Clipping ownership feature class to {}m buffer around HUC boundary.'.format(buffer_meters))

    # Rough
    buff_dist = _rough_convert_metres_to_shapefile_units(boundary, buffer_meters)
    huc_boundary = get_geometry_unary_union(boundary, output_epsg)
    buffered = huc_boundary.buffer(buff_dist)
    copy_feature_class(ownership, output_epsg, out_path, buffered)
    log.info('Ownership clip complete.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('boundary', help='HUC boundary polygon shapefile', type=argparse.FileType('r'))
    parser.add_argument('ownership', help='National ownership polygon shapefile', type=argparse.FileType('r'))
    parser.add_argument('output', help='Output ownership ShapeFile path', type=str)
    parser.add_argument('--epsg', help='Output spatial reference EPSG', default=4326, type=int)
    parser.add_argument('--buffer', help='Buffer distance in meters', default=10000, type=int)

    args = dotenv.parse_args_env(parser)

    if os.path.isfile(args.output):
        print('Deleting existing output', args.output)
        delete_shapefile(args.output)

    clip_ownership(args.boundary.name, args.ownership.name, args.output, args.epsg, args.buffer)


if __name__ == '__main__':
    main()
