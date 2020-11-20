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
from rscommons import dotenv
from rscommons.shapefile import load_attributes, load_geometries
from rscommons.database import get_db_srs
from rscommons.plotting import validation_chart
from sqlbrat.utils.reach_geometry import calculate_reach_geometry
from sqlbrat.utils.load_hucs import get_hucs_present


def reach_geometry_validation(top_level_folder, database, buffer_distance):
    """
    Validate PyBRAT 4 vegetation FIS with that of pyBRAT 3
    :param top_level_folder: Top level folder containing pyBRAT 3 HUC 8 projects
    :param database: Path to the SQLite database containing pyBRAT configuration
    :param buffer_distance: PyBRAT 4 buffer distance for sampling DEM raster elevations
    :return: None
    """

    hucs = get_hucs_present(top_level_folder, database)
    fields = ['iGeo_Slope', 'iGeo_ElMin', 'iGeo_ElMax', 'iGeo_Len']

    results = {}
    for _huc, paths in hucs.items():
        polylines = load_geometries(paths['Network'], 'ReachID')
        db_srs = get_db_srs(database)
        expected = load_attributes(paths['Network'], 'ReachID', fields)
        results = calculate_reach_geometry(polylines, paths['DEM'], db_srs, buffer_distance)

        for field in fields:
            if field not in results:
                results[field] = []

            for reachid, values in results.items():
                if reachid in expected:
                    results[field].append((expected[reachid][field], values[field]))

    [validation_chart(results[field], '{} Reach Geometry'.format(field)) for field in fields]

    print('Validation complete')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', help='Top level folder where BRAT data are stored', type=str)
    parser.add_argument('database', help='Path to SQLite database', type=str)
    parser.add_argument('buffer', help='Buffer distance for sampling DEM raster', type=float)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '..', '..', '.env.validation'))

    reach_geometry_validation(args.folder, args.database, args.buffer)


if __name__ == '__main__':
    main()
