# -------------------------------------------------------------------------------
# Name:     Stream Powewr Validation
#
# Purpose:  Compares the stream power attributes for pyBRAT 3 and pyBRAT 4
#
# Author:   Philip Bailey
#
# Date:     3 Dec 2019
#
# -------------------------------------------------------------------------------
import os
# import sys
import argparse
from rscommons import dotenv
from rscommons import shapefile
from rscommons import plotting
from sqlbrat.utils import load_hucs

calculate_stream_power = __import__('stream_power', fromlist=[''])


input_fields = ['iHyd_QLow', 'iHyd_Q2', 'iGeo_Slope']
output_fields = ['iHyd_SPLow', 'iHyd_SP2']


def stream_power_validation(top_level_folder, database, id_field):

    # Detect which Idaho BRAT HUCs are present on local disk
    hucs = load_hucs.get_hucs_present(top_level_folder, database)

    results = {}
    for huc, paths in hucs.items():

        if 'Network' not in paths:
            print('Skipping {} because no network shapefile'.format(huc))
            continue

        print('Validating stream power for HUC', huc)

        # Load the pyBRAT3 values
        inputs = shapefile.load_attributes(paths['Network'], id_field, input_fields)
        expected = shapefile.load_attributes(paths['Network'], id_field, output_fields)

        # Calculate the land use attributes
        calculated = calculate_stream_power.calculate_stream_power(inputs)

        # Synthesize the results for all the HUCs
        for field in output_fields:
            if field not in results:
                results[field] = []

            for reachid, values in calculated.items():
                if reachid in expected:
                    results[field].append((expected[reachid][field], values[field]))

    # Generate the validation plots
    [plotting.validation_chart(results[field], '{} Stream Power'.format(field)) for field in output_fields]

    print('Validation complete')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', help='Top level folder where BRAT data are stored', type=str)
    parser.add_argument('database', help='Path to SQLite database', type=str)
    parser.add_argument('--id_field', help='(Optional) Reach ID field', default='ReachID', type=str)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '..', '..', '.env.validation'))

    stream_power_validation(args.folder, args.database, args.id_field)


if __name__ == '__main__':
    main()
