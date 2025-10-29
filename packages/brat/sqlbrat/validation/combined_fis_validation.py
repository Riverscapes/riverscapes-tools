# -------------------------------------------------------------------------------
# Name:     Test Combined FIS
#
# Purpose:  Reads the inputs for the combined FIS from a pyBRAT 3 table and
#           then runs them through the pyBRAT 4 combined FIS process. The
#           results comparing pyBRAT 3 and 4 are then plotted.
#
# Author:   Philip Bailey
#
# Date:     12 Aug 2019
#
# -------------------------------------------------------------------------------
import argparse
import os
import sqlite3
from rsxml import dotenv
from rscommons.shapefile import load_attributes
from rscommons.plotting import validation_chart
from sqlbrat.utils.combined_fis import calculate_combined_fis
from sqlbrat.utils.load_hucs import get_hucs_present


def combined_fis_validation(top_level_folder, database):
    """
    Validate PyBRAT 4 combined FIS with that of pyBRAT 3
    :param top_level_folder: Top level folder containing pyBRAT 3 HUC 8 projects
    :param database: Path to the SQLite database containing pyBRAT configuration
    :return: None
    """

    hucs = get_hucs_present(top_level_folder, database)
    dathresh = get_drainage_area_thresh(database)

    for label, veg_type in {'Existing': 'EX', 'Historic': 'HPE'}.items():
        capacity_values = []
        density_values = []

        veg_fis_field = 'oVC_{}'.format(veg_type)
        com_capacity_field = 'oCC_{}'.format(veg_type)
        com_density_field = 'oMC_{}'.format(veg_type)

        for huc, paths in hucs.items():

            max_drainage_area = dathresh[huc]

            # Load the input fields required as well as the pyBRAT3 output fields
            feature_values = load_attributes(paths['Network'], 'ReachID', [veg_fis_field, 'iGeo_Slope', 'iGeo_DA', 'iHyd_SP2', 'iHyd_SPLow', 'iGeo_Len'])
            expected_output = load_attributes(paths['Network'], 'ReachID', [com_capacity_field, com_density_field])

            # Do the combined FIS calculation
            calculate_combined_fis(feature_values, veg_fis_field, com_capacity_field, com_density_field, max_drainage_area)

            # Merge the results into a master list
            for reach, feature in feature_values.items():
                capacity_values.append((expected_output[reach][com_capacity_field], feature[com_capacity_field]))
                density_values.append((expected_output[reach][com_density_field], feature[com_density_field]))

        # Plot the master list
        validation_chart(capacity_values, '{} Combined FIS Capacity'.format(label))
        validation_chart(density_values, '{} Combined FIS Density'.format(label))

    print('Validation complete')


def get_drainage_area_thresh(database):
    """
    Retrieve the drainage area thresholds from the pyBRAT SQLite database
    :param database: Path to the SQLite database.
    :return: Dictionary where key is HUC8 (integer) keyed to drainage area threshold
    """

    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute('SELECT HUC8, DAThresh FROM HUCs')
    dathresh = {}
    for row in curs.fetchall():
        dathresh[row[0]] = row[1]

    conn.close()

    return dathresh


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', help='Top level folder where BRAT data are stored', type=str)
    parser.add_argument('database', help='Path to SQLite database', type=str)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '..', '..', '.env.validation'))

    combined_fis_validation(args.folder, args.database)


if __name__ == '__main__':
    main()
