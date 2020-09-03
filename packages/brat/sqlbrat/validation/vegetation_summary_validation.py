# -------------------------------------------------------------------------------
# Name:     Vegetation Summary Validation
#
# Purpose:  Reads the inputs for vegetation buffer values from a pyBRAT 3 table
#           and then runs the pyBRAT 4 vegetation summary method and compares the
#           two results.
#
# Author:   Philip Bailey
#
# Date:     28 Aug 2019
#
# -------------------------------------------------------------------------------
import argparse
import gdal
import ogr
import osr
import os

from rscommons import dotenv
from rscommons.shapefile import load_geometries
from rscommons.shapefile import load_attributes
from rscommons.shapefile import _rough_convert_metres_to_shapefile_units

from sqlbrat.utils.vegetation_summary import calculate_vegetation_summary
from sqlbrat.lib.plotting import validation_chart
from sqlbrat.utils.load_hucs import get_hucs_present


def vegetation_summary_validation(database, veg_raster, top_level_folder):
    """
    Validate PyBRAT 4 vegetation FIS with that of pyBRAT 3
    :param top_level_folder: Top level folder containing pyBRAT 3 HUC 8 projects
    :param database: Path to the SQLite database containing pyBRAT configuration
    :return: None
    """

    hucs = get_hucs_present(top_level_folder, database)

    for table, prefix in {'Existing': 'EX', 'Historic': 'Hpe'}.items():
        for buffer in [30, 100]:
            plot_values = []

            for huc, paths in hucs.items():
                print('Validating', huc)

                # _rough_convert_metres_to_shapefile_units(paths['Network'], 300)

                veg_field = 'iVeg_{}{}'.format(buffer, prefix)
                if buffer == 100:
                    veg_field = veg_field.replace('_', '')

                # Load the input fields required as well as the pyBRAT3 output fields
                geometries = load_geometries(paths['Network'], 'ReachID', 5070)
                expected_output = load_attributes(paths['Network'], 'ReachID', [veg_field])

                results = calculate_vegetation_summary(database, geometries, veg_raster, buffer, table, prefix, huc)

                # Merge the results into a master list
                for reach, feature in results.items():
                    plot_values.append((expected_output[reach][veg_field], feature[veg_field]))

            validation_chart(plot_values, '{0}m {1} Vegetation Summary'.format(buffer, table))

    print('Validation complete')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to SQLite database', type=str)
    parser.add_argument('veg_raster', help='Path to vegetation raster', type=str)
    parser.add_argument('folder', help='Top level folder where BRAT data are stored', type=str)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '..', '..', '.env.validation'))

    vegetation_summary_validation(args.database, args.veg_raster, args.folder)


if __name__ == '__main__':
    main()
