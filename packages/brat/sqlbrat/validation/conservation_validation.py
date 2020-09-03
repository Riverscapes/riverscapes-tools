# -------------------------------------------------------------------------------
# Name:     Conservation Validation
#
# Purpose:  Compares the conservation and restoration attributes for pyBRAT 3 and pyBRAT 4
#
# Author:   Philip Bailey
#
# Date:     24 Oct 2019
#
# -------------------------------------------------------------------------------
import os
import sys
import argparse
from rscommons import dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
shapefile = __import__('lib.shapefile', fromlist=[''])
plotting = __import__('lib.plotting', fromlist=[''])
load_hucs = __import__('scripts.load_hucs', fromlist=[''])
conservation = __import__('conservation', fromlist=[''])

fields = ['oPBRC_UI', 'oPBRC_UD', 'oPBRC_CR']


def conservation_validation(top_level_folder, database, id_field):

    # Detect which Idaho BRAT HUCs are present on local disk
    hucs = load_hucs.get_hucs_present(top_level_folder, database)

    results = {}
    for huc, paths in hucs.items():

        if 'Network' not in paths:
            print('Skipping {} because no network shapefile'.format(huc))
            continue

        print('Validating Land Use for HUC', huc)

        # Load the pyBRAT3 output fields
        expected = shapefile.load_attributes(paths['Network'], id_field, fields)

        # Calculate the conservation attributes
        calculated = conservation.calculate_conservation(huc, paths['Network'], id_field)

        # Synthesize the results for all HUCs
        for field in fields:
            if field not in results:
                results[field] = []

            for reachid, values in calculated.items():
                if reachid in expected:
                    results[field].append((expected[reachid][field], values[field]))

    # Generate the validation plots
    [plotting.validation_chart(results[field], '{} Conservation Attribute'.format(field)) for field in fields]

    print('Validation complete')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', help='Top level folder where BRAT data are stored', type=str)
    parser.add_argument('database', help='Path to SQLite database', type=str)
    parser.add_argument('--id_field', help='(Optional) Reach ID field', default='ReachID', type=str)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '..', '..', '.env.validation'))

    conservation_validation(args.folder, args.database, args.id_field)


if __name__ == '__main__':
    main()
