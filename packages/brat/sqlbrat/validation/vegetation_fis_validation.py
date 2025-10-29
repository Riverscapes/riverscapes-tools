# -------------------------------------------------------------------------------
# Name:     Vegetation Combined FIS
#
# Purpose:  Reads the inputs for the vegetation FIS from a pyBRAT 3 table and
#           then runs them through the pyBRAT 4 combined FIS process. The
#           results are plotted.
#
# Author:   Philip Bailey
#
# Date:     12 Aug 2019
#
# -------------------------------------------------------------------------------
import argparse
import os
from rsxml import dotenv
from rscommons.shapefile import load_attributes
from rscommons.plotting import validation_chart
from sqlbrat.utils.load_hucs import get_hucs_present
from sqlbrat.utils.vegetation_fis import calculate_vegegtation_fis


def vegetation_fis_validation(top_level_folder, database):
    """
    Validate PyBRAT 4 vegetation FIS with that of pyBRAT 3
    :param top_level_folder: Top level folder containing pyBRAT 3 HUC 8 projects
    :param database: Path to the SQLite database containing pyBRAT configuration
    :return: None
    """

    hucs = get_hucs_present(top_level_folder, database)

    for _label, veg_type in {'Existing': 'EX', 'Historic': 'hpe'}.items():
        plot_values = []
        for _huc, paths in hucs.items():
            out_field = 'oVC_{}'.format(veg_type)
            streamside_field = 'iVeg_30{}'.format(veg_type)
            riparian_field = 'iVeg100{}'.format(veg_type)

            # Load the input fields required as well as the pyBRAT3 output fields
            feature_values = load_attributes(paths['Network'], 'ReachID', [streamside_field, riparian_field])
            expected_output = load_attributes(paths['Network'], 'ReachID', [out_field])

            calculate_vegegtation_fis(feature_values, streamside_field, riparian_field, out_field)

            # Merge the results into a master list
            for reach, feature in feature_values.items():
                plot_values.append((expected_output[reach][out_field], feature[out_field]))

        validation_chart(plot_values, '{} Vegetation FIS'.format(veg_type))

    print('Validation complete')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', help='Top level folder where BRAT data are stored', type=str)
    parser.add_argument('database', help='Path to SQLite database', type=str)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '..', '..', '.env.validation'))

    vegetation_fis_validation(args.folder, args.database)


if __name__ == '__main__':
    main()
