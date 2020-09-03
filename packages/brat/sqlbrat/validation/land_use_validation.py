# -------------------------------------------------------------------------------
# Name:     Land Use Intensity Validation
#
# Purpose:  Compares the land use intensity attributes for pyBRAT 3 and pyBRAT 4
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
calculate_land_use = __import__('land_use', fromlist=[''])


fields = ['iPC_LU', 'iPC_VLowLU', 'iPC_LowLU', 'iPC_ModLU', 'iPC_HighLU']


def land_use_validation(top_level_folder, veg_raster, database, id_field, buffer_distance):

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

        # Calculate the land use attributes
        calculated = calculate_land_use.calculate_land_use(huc, paths['Network'], paths['ExistingVeg'], database, id_field, buffer_distance, None)

        # Synthesize the results for all the HUCs
        for field in fields:
            if field not in results:
                results[field] = []

            for reachid, values in calculated.items():
                if reachid in expected:
                    results[field].append((expected[reachid][field], values[field]))

    # Generate the validation plots
    [plotting.validation_chart(results[field], '{} Land Use Attribute'.format(field)) for field in fields]

    print('Validation complete')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', help='Top level folder where BRAT data are stored', type=str)
    parser.add_argument('veg_raster', help='Existing Vegetation Raster', type=str)
    parser.add_argument('database', help='Path to SQLite database', type=str)
    parser.add_argument('--id_field', help='(Optional) Reach ID field', default='ReachID', type=str)
    parser.add_argument('--buffer', help='Buffer distance for sampling vegetation raster', default=100, type=float)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '..', '..', '.env.validation'))

    land_use_validation(args.folder, args.veg_raster, args.database, args.id_field, args.buffer)


if __name__ == '__main__':
    main()
