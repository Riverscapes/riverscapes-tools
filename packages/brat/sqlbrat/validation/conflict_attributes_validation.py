# -------------------------------------------------------------------------------
# Name:     Reach Geometry Validation
#
# Purpose:  Compares the conflict attributes for pyBRAT 3 and pyBRAT 4
#
# Author:   Philip Bailey
#
# Date:     18 Oct 2019
#
# -------------------------------------------------------------------------------
import os
import sys
import argparse
from rsxml import dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
shapefile = __import__('lib.shapefile', fromlist=[''])
plotting = __import__('lib.plotting', fromlist=[''])
load_hucs = __import__('scripts.load_hucs', fromlist=[''])
conflict_attributes = __import__('conflict_attributes', fromlist=[''])


fields = ['iPC_RoadX', 'iPC_Road', 'iPC_Privat']  # , 'iPC_Rail']  # 'iPC_RoadVB', 'iPC_RailVB', 'iPC_Canal', 'iPC_LU']


def conflict_attributes_validation(top_level_folder, database, reach_id_field, buffer_distance, cell_size):

    hucs = load_hucs.get_hucs_present(top_level_folder, database)

    results = {}
    for huc, paths in hucs.items():

        if 'Network' not in paths:
            print('Skipping {} because no network shapefile'.format(huc))
            continue

        print('Validating HUC', huc)

        # Load the pyBRAT3 output fields
        expected = shapefile.load_attributes(paths['Network'], reach_id_field, fields)

        if 'Rail' not in paths:
            paths['Rail'] = None

        # Calculate the conflict attributes
        calculated = conflict_attributes.calc_conflict_attributes(paths['Network'],
                                                                  reach_id_field,
                                                                  paths['Roads'],
                                                                  paths['Rail'],
                                                                  paths['Canals'],
                                                                  paths['LandOwnership'],
                                                                  buffer_distance, cell_size)

        # Synthesize the results for all the HUCs
        for field in fields:
            if field not in results:
                results[field] = []

            for reachid, values in calculated.items():
                if reachid in expected:
                    results[field].append((expected[reachid][field], values[field]))

    # Generate the validation plots
    [plotting.validation_chart(results[field], '{} Conflict Attribute'.format(field)) for field in fields]

    print('Validation complete')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', help='Top level folder where BRAT data are stored', type=str)
    parser.add_argument('database', help='Path to SQLite database', type=str)
    parser.add_argument('--buffer', help='Buffer distance for sampling DEM raster', default=30, type=float)
    parser.add_argument('--cell_size', help='Cell size for rasterization', default=5, type=float)
    parser.add_argument('--reach_id_field', help='(Optional) Reach ID field', default='ReachID', type=str)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '..', '..', '.env.validation'))

    conflict_attributes_validation(args.folder, args.database, args.reach_id_field, args.buffer, args.cell_size)


if __name__ == '__main__':
    main()
