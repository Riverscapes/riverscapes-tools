# -------------------------------------------------------------------------------
# Name:     Reach Geometry Validation
#
# Purpose:  Compares the slope, feature length minimum and maximum elevation values
#           from pyBRAT 3 with those of pyBRAT 4.
#
# Author:   Philip Bailey
#
# Date:     12 Aug 2019
#
# -------------------------------------------------------------------------------
import argparse
import os
from rsxml import dotenv
from sqlbrat.validation.vegetation_fis_validation import vegetation_fis_validation
from sqlbrat.validation.combined_fis_validation import combined_fis_validation
from sqlbrat.validation.reach_geometry_validation import reach_geometry_validation
from sqlbrat.validation.segmentation_validation import segmentation_validation
from sqlbrat.validation.vegetation_summary_validation import vegetation_summary_validation
from sqlbrat.validation.conflict_attributes_validation import conflict_attributes_validation


def validate_all(top_level_folder, database, reach_id_field, buffer_distance, conflict_buffer_distance, distance_cell_size):

    segmentation_validation(top_level_folder, database, buffer_distance)
    vegetation_summary_validation(top_level_folder, database, buffer_distance)
    reach_geometry_validation(top_level_folder, database, buffer_distance)
    vegetation_fis_validation(top_level_folder, database)
    combined_fis_validation(top_level_folder, database)
    conflict_attributes_validation(top_level_folder, database, reach_id_field, conflict_buffer_distance, distance_cell_size)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', help='Top level folder where BRAT data are stored', type=str)
    parser.add_argument('database', help='Path to SQLite database', type=str)
    parser.add_argument('--reach_id_field', help='(Optional) field that uniquely identifies each reach', default='ReachID', type=str)
    parser.add_argument('--reach_geom_buffer', help='(Optional) reach geometry buffer distance (meters)', default=100, type=float)
    parser.add_argument('--conflict_buffer', help='(Optional) Conflict attribute buffer distance (meters).', default=30, type=float)
    parser.add_argument('--distance_cell_size', help='(Optional) Conflict attribute rasterization cell size (meters).', default=5, type=float)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '..', '..', '.env.validation'))

    validate_all(args.folder, args.database, args.reach_id_field, args.reach_geom_buffer, args.conflict_buffer, args.distance_cell_size)


if __name__ == '__main__':
    main()
