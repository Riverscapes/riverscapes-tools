import os
import argparse
from rscommons import Logger, dotenv
from rscommons.shapefile import get_geometry_unary_union
from rscommons.shapefile import _rough_convert_metres_to_shapefile_units
from rscommons.shapefile import copy_feature_class
from rscommons.shapefile import delete_shapefile


def clip_vector_layer(boundary, vector, out_path, output_epsg, buffer_meters):

    log = Logger('Vector Layer')

    log.info('Clipping {} feature class to {}m buffer around HUC boundary.'.format(os.path.basename(out_path), buffer_meters))

    # if dataset is not projected convert m to geo distance, else just use the meters
    # Rough
    buff_dist = _rough_convert_metres_to_shapefile_units(boundary, buffer_meters)
    huc_boundary = get_geometry_unary_union(boundary, output_epsg)
    buffered = huc_boundary.buffer(buff_dist)
    copy_feature_class(vector, output_epsg, out_path, buffered)
    log.info(f'{os.path.basename(vector)} clip complete.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('boundary', help='HUC boundary polygon shapefile', type=argparse.FileType('r'))
    parser.add_argument('vector', help='vector polygon shapefile', type=argparse.FileType('r'))
    parser.add_argument('output', help='Output vector ShapeFile path', type=str)
    parser.add_argument('--epsg', help='Output spatial reference EPSG', default=4326, type=int)
    parser.add_argument('--buffer', help='Buffer distance in meters', default=10000, type=int)

    args = dotenv.parse_args_env(parser)

    if os.path.isfile(args.output):
        print('Deleting existing output', args.output)
        delete_shapefile(args.output)

    clip_vector_layer(args.boundary.name, args.vector.name, args.output, args.epsg, args.buffer)


if __name__ == '__main__':
    main()
