# Name:     Vegetation FIS
#
# Purpose:  Runs the vegetation FIS for the BRAT input table.
#           Adapted from Jordan Gilbert's original BRAT script.
#
# Author:   Jordan Gilbert
#           Philip Bailey
#
# Created:  30 May 2019
# -------------------------------------------------------------------------------
import os
import sys
import argparse
import traceback
import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl
from rscommons import Logger, ProgressBar, dotenv
from rscommons.database import load_attributes
from rscommons.database import write_attributes_NEW


def vegetation_fis(database, label, veg_type):
    """Calculate vegetation suitability for each reach in a BRAT
    SQLite database

    Arguments:
        database {str} -- Path to BRAT SQLite database
        label {str} -- Either 'historic' or 'existing'. Only used for lof messages.
        veg_type {str} -- Prefix either 'EX' for existing or 'HPE' for historic
    """

    log = Logger('Vegetation FIS')
    log.info('Processing {} vegetation'.format(label))

    streamside_field = 'iVeg_30{}'.format(veg_type)
    riparian_field = 'iVeg100{}'.format(veg_type)
    out_field = 'oVC_{}'.format(veg_type)

    feature_values = load_attributes(database, [streamside_field, riparian_field], '({} IS NOT NULL) AND ({} IS NOT NULL)'.format(streamside_field, riparian_field))
    calculate_vegegtation_fis(feature_values, streamside_field, riparian_field, out_field)
    write_attributes_NEW(database, feature_values, [out_field])

    log.info('Process completed successfully.')


def calculate_vegegtation_fis(feature_values, streamside_field, riparian_field, out_field):
    """
    Beaver dam capacity vegetation FIS
    :param feature_values: Dictionary of features keyed by ReachID and values are dictionaries of attributes
    :param streamside_field: Name of the feature streamside vegetation attribute
    :param riparian_field: Name of the riparian vegetation attribute
    :return: Inserts 'FIS' key into feature dictionaries with the vegetation FIS values
    """

    log = Logger('Vegetation FIS')

    feature_count = len(feature_values)
    reachid_array = np.zeros(feature_count, np.int64)
    riparian_array = np.zeros(feature_count, np.float64)
    streamside_array = np.zeros(feature_count, np.float64)

    counter = 0
    for reach_id, values in feature_values.items():
        reachid_array[counter] = reach_id
        riparian_array[counter] = values[riparian_field]
        streamside_array[counter] = values[streamside_field]
        counter += 1

    # Ensure vegetation inputs are within the 0-4 range
    riparian_array[riparian_array < 0] = 0
    riparian_array[riparian_array > 4] = 4
    streamside_array[streamside_array < 0] = 0
    streamside_array[streamside_array > 4] = 4

    # create antecedent (input) and consequent (output) objects to hold universe variables and membership functions
    riparian = ctrl.Antecedent(np.arange(0, 4, 0.01), 'input1')
    streamside = ctrl.Antecedent(np.arange(0, 4, 0.01), 'input2')
    density = ctrl.Consequent(np.arange(0, 45, 0.01), 'result')

    # build membership functions for each antecedent and consequent object
    riparian['unsuitable'] = fuzz.trapmf(riparian.universe, [0, 0, 0.1, 1])
    riparian['barely'] = fuzz.trimf(riparian.universe, [0.1, 1, 2])
    riparian['moderately'] = fuzz.trimf(riparian.universe, [1, 2, 3])
    riparian['suitable'] = fuzz.trimf(riparian.universe, [2, 3, 4])
    riparian['preferred'] = fuzz.trimf(riparian.universe, [3, 4, 4])

    streamside['unsuitable'] = fuzz.trapmf(streamside.universe, [0, 0, 0.1, 1])
    streamside['barely'] = fuzz.trimf(streamside.universe, [0.1, 1, 2])
    streamside['moderately'] = fuzz.trimf(streamside.universe, [1, 2, 3])
    streamside['suitable'] = fuzz.trimf(streamside.universe, [2, 3, 4])
    streamside['preferred'] = fuzz.trimf(streamside.universe, [3, 4, 4])

    density['none'] = fuzz.trimf(density.universe, [0, 0, 0.1])
    density['rare'] = fuzz.trapmf(density.universe, [0, 0.1, 0.5, 1.5])
    density['occasional'] = fuzz.trapmf(density.universe, [0.5, 1.5, 4, 8])
    density['frequent'] = fuzz.trapmf(density.universe, [4, 8, 12, 25])
    density['pervasive'] = fuzz.trapmf(density.universe, [12, 25, 45, 45])

    # build fis rule table
    veg_ctrl = ctrl.ControlSystem([
        ctrl.Rule(riparian['unsuitable'] & streamside['unsuitable'], density['none']),
        ctrl.Rule(riparian['barely'] & streamside['unsuitable'], density['rare']),
        ctrl.Rule(riparian['moderately'] & streamside['unsuitable'], density['rare']),
        ctrl.Rule(riparian['suitable'] & streamside['unsuitable'], density['occasional']),
        ctrl.Rule(riparian['preferred'] & streamside['unsuitable'], density['occasional']),
        ctrl.Rule(riparian['unsuitable'] & streamside['barely'], density['rare']),
        ctrl.Rule(riparian['barely'] & streamside['barely'], density['rare']),  # matBRAT has consequnt as 'occasional'
        ctrl.Rule(riparian['moderately'] & streamside['barely'], density['occasional']),
        ctrl.Rule(riparian['suitable'] & streamside['barely'], density['occasional']),
        ctrl.Rule(riparian['preferred'] & streamside['barely'], density['occasional']),
        ctrl.Rule(riparian['unsuitable'] & streamside['moderately'], density['rare']),
        ctrl.Rule(riparian['barely'] & streamside['moderately'], density['occasional']),
        ctrl.Rule(riparian['moderately'] & streamside['moderately'], density['occasional']),
        ctrl.Rule(riparian['suitable'] & streamside['moderately'], density['frequent']),
        ctrl.Rule(riparian['preferred'] & streamside['moderately'], density['frequent']),
        ctrl.Rule(riparian['unsuitable'] & streamside['suitable'], density['occasional']),
        ctrl.Rule(riparian['barely'] & streamside['suitable'], density['occasional']),
        ctrl.Rule(riparian['moderately'] & streamside['suitable'], density['frequent']),
        ctrl.Rule(riparian['suitable'] & streamside['suitable'], density['frequent']),
        ctrl.Rule(riparian['preferred'] & streamside['suitable'], density['pervasive']),
        ctrl.Rule(riparian['unsuitable'] & streamside['preferred'], density['occasional']),
        ctrl.Rule(riparian['barely'] & streamside['preferred'], density['frequent']),
        ctrl.Rule(riparian['moderately'] & streamside['preferred'], density['pervasive']),
        ctrl.Rule(riparian['suitable'] & streamside['preferred'], density['pervasive']),
        ctrl.Rule(riparian['preferred'] & streamside['preferred'], density['pervasive'])
    ])
    veg_fis = ctrl.ControlSystemSimulation(veg_ctrl)

    # calculate defuzzified centroid value for density 'none' MF group
    # this will be used to re-classify output values that fall in this group
    # important: will need to update the array (x) and MF values (mfx) if the
    #            density 'none' values are changed in the model
    x = np.arange(0, 45, 0.01)
    mfx = fuzz.trimf(x, [0, 0, 0.1])
    defuzz_centroid = round(fuzz.defuzz(x, mfx, 'centroid'), 6)

    mfx_pervasive = fuzz.trapmf(x, [12, 25, 45, 45])
    defuzz_pervasive = round(fuzz.defuzz(x, mfx_pervasive, 'centroid'))

    # run fuzzy inference system on inputs and defuzzify output
    progbar = ProgressBar(len(reachid_array), 50, "Vegetation FIS")
    counter = 0
    for i, reach_id in enumerate(reachid_array):
        veg_fis.input['input1'] = riparian_array[i]
        veg_fis.input['input2'] = streamside_array[i]
        veg_fis.compute()
        result = veg_fis.output['result']

        # set ovc_* to 0 if output falls fully in 'none' category and to 40 if falls fully in 'pervasive' category
        if round(result, 6) == defuzz_centroid:
            result = 0.0

        if round(result) >= defuzz_pervasive:
            result = 40.0

        feature_values[reach_id][out_field] = round(result, 2)

        counter += 1
        progbar.update(counter)

    progbar.finish()
    log.info('Done')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='BRAT SQLite database', type=argparse.FileType('r'))
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    logg = Logger("Vegetation FIS")
    logfile = os.path.join(os.path.dirname(args.network.name), "vegetation_fis.log")
    logg.setup(logPath=logfile, verbose=args.verbose)

    try:
        # vegetation_fis(args.network.name, 'historic', 'HPE')
        vegetation_fis(args.database.name, 'existing', 'EX')

    except Exception as ex:
        logg.error(ex)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
