import os
import argparse
from rsxml import Logger, dotenv
from rscommons.shapefile import get_geometry_unary_union
from rscommons.shapefile import _rough_convert_metres_to_shapefile_units
from rscommons.shapefile import copy_feature_class_shapefile
from rscommons.shapefile import delete_shapefile


def clip_vector_layer(boundary, vector, out_path, output_epsg, buffer_meters, clip=False):
    """ Clip a vector layers

    Args:
        boundary (_type_): _description_
        vector (_type_): _description_
        out_path (_type_): _description_
        output_epsg (_type_): _description_
        buffer_meters (_type_): _description_
        clip (bool, optional): _description_. Defaults to False.
    """

    log = Logger('Vector Layer')

    # if dataset is not projected convert m to geo distance, else just use the meters
    # Rough
    buff_dist = _rough_convert_metres_to_shapefile_units(boundary, buffer_meters)
    huc_boundary = get_geometry_unary_union(boundary, output_epsg)
    buffered = huc_boundary.buffer(buff_dist)
    if clip:
        log.info(f'Clipping {os.path.basename(out_path)} feature class to {buffer_meters}m buffer around HUC boundary.')
        copy_feature_class_shapefile(vector, output_epsg, out_path, clip_shape=buffered)
    else:
        log.info(f'Selecting features from {os.path.basename(out_path)} feature class that intersect HUC boundary.')
        copy_feature_class_shapefile(vector, output_epsg, out_path, intersect_shape=buffered)
    log.info(f'{os.path.basename(vector)} clip complete.')


def main():
    """ Main for this module
    """
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
