"""RCAT Fuzzy Inference System

Jordan Gilbert

02/2023
"""

import argparse
import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl
from rscommons.database import load_attributes, load_igo_attributes, load_dgo_attributes, write_db_attributes, write_db_igo_attributes, write_db_dgo_attributes
from rscommons import Logger, ProgressBar, dotenv


def rcat_fis(database: str, igos: bool):
    """Run the RCAT FIS

    Arguments:
        database (str): Path to the RCAT database
        igos (bool): True if the FIS is being run for the IGO output 
    """

    log = Logger('RCAT FIS')
    log.info('Starting RCAT FIS')

    if igos is True:
        fields = ['RiparianDeparture', 'LUI', 'FloodplainAccess']
        features = load_igo_attributes(database, fields, ' AND '.join(['({} IS NOT NULL)'.format(f) for f in fields]))
        calculate_fis(features, igos)
        write_db_igo_attributes(database, features, ['Condition'], summarize=False)
        dgo_features = load_dgo_attributes(database, fields, ' AND '.join(['({} IS NOT NULL)'.format(f) for f in fields]))
        calculate_fis(dgo_features, igos)
        write_db_dgo_attributes(database, dgo_features, ['Condition'], summarize=False)
    else:
        fields = ['RiparianDeparture', 'iPC_LU', 'FloodplainAccess']
        features = load_attributes(database, fields, ' AND '.join(['({} IS NOT NULL)'.format(f) for f in fields]))
        calculate_fis(features, igos)
        write_db_attributes(database, features, ['Condition'], log)


def calculate_fis(feature_values: dict, igos: bool):
    """The fuzzy inference system

    Arguments:
        feature_values (dict): FIS input values associated with each reach
        igos (bool): True if the FIS is being run for the IGO output
    """

    log = Logger('RCAT FIS')

    if igos is True:
        lui_field = 'LUI'
    else:
        lui_field = 'iPC_LU'

    # get arrays for fields of interest
    feature_count = len(feature_values)
    id_array = np.zeros(feature_count, np.int64)
    rvd_array = np.zeros(feature_count, np.float64)
    lui_array = np.zeros(feature_count, np.float64)
    fpaccess_array = np.zeros(feature_count, np.float64)

    counter = 0
    for id, values in feature_values.items():
        id_array[counter] = id
        rvd_array[counter] = values['RiparianDeparture']
        lui_array[counter] = values[lui_field]
        fpaccess_array[counter] = values['FloodplainAccess']
        counter += 1

    # adjust inputs to be within FIS membership range
    rvd_array[rvd_array < 0] = 0
    # rvd_array[rvd_array > 1] = 1
    rvd_array = [r if r < 1 else 1 - (r - 1) for r in rvd_array]
    lui_array[lui_array < 0] = 0
    lui_array[lui_array > 100] = 100
    fpaccess_array[fpaccess_array < 0] = 0
    fpaccess_array[fpaccess_array > 1] = 1

    # set up FIS
    rvd = ctrl.Antecedent(np.arange(0, 1, 0.01), "input1")
    lui = ctrl.Antecedent(np.arange(0, 100, 0.01), "input2")
    fpaccess = ctrl.Antecedent(np.arange(0, 1, 0.01), "input3")
    condition = ctrl.Consequent(np.arange(0, 1, 0.01), "result")

    rvd["large"] = fuzz.trapmf(rvd.universe, [0, 0, 0.3, 0.5])
    rvd["significant"] = fuzz.trimf(rvd.universe, [0.3, 0.5, 0.85])
    rvd["minor"] = fuzz.trimf(rvd.universe, [0.5, 0.85, 0.95])
    rvd["negligible"] = fuzz.trapmf(rvd.universe, [0.85, 0.95, 1, 1])

    lui["low"] = fuzz.trapmf(lui.universe, [0, 0, 41.6, 58.3])
    lui["moderate"] = fuzz.trapmf(lui.universe, [41.6, 58.3, 83, 98.3])
    lui["high"] = fuzz.trapmf(lui.universe, [83, 98.3, 100, 100])

    fpaccess["low"] = fuzz.trapmf(fpaccess.universe, [0, 0, 0.5, 0.7])
    fpaccess["moderate"] = fuzz.trapmf(fpaccess.universe, [0.5, 0.7, 0.9, 0.95])
    fpaccess["high"] = fuzz.trapmf(fpaccess.universe, [0.9, 0.95, 1, 1])

    condition["very poor"] = fuzz.trimf(condition.universe, [0, 0, 0.1])
    condition["poor"] = fuzz.trapmf(condition.universe, [0, 0.1, 0.3, 0.5])
    condition["moderate"] = fuzz.trapmf(condition.universe, [0.3, 0.5, 0.6, 0.8])
    condition["good"] = fuzz.trapmf(condition.universe, [0.6, 0.8, 0.95, 1])
    condition["intact"] = fuzz.trimf(condition.universe, [0.95, 1, 1])

    rcat_ctrl = ctrl.ControlSystem([
        ctrl.Rule(rvd['large'] & lui['low'] & fpaccess['low'], condition['poor']),
        ctrl.Rule(rvd['large'] & lui['low'] & fpaccess['moderate'], condition['poor']),
        ctrl.Rule(rvd['large'] & lui['low'] & fpaccess['high'], condition['moderate']),
        ctrl.Rule(rvd['large'] & lui['moderate'] & fpaccess['low'], condition['poor']),
        ctrl.Rule(lui['moderate'] & fpaccess['moderate'], condition['moderate']),
        ctrl.Rule(rvd['large'] & lui['moderate'] & fpaccess['high'], condition['poor']),
        ctrl.Rule(rvd['large'] & lui['high'] & fpaccess['low'], condition['very poor']),
        ctrl.Rule((rvd['significant'] | rvd['minor'] | rvd['negligible']) & lui['high'] & fpaccess['low'], condition['poor']),
        ctrl.Rule(lui['high'] & fpaccess['moderate'], condition['poor']),
        ctrl.Rule(lui['high'] & fpaccess['high'], condition['moderate']),
        ctrl.Rule(rvd['significant'] & lui['low'] & fpaccess['low'], condition['moderate']),
        ctrl.Rule(rvd['significant'] & lui['low'] & fpaccess['moderate'], condition['moderate']),
        ctrl.Rule(rvd['significant'] & lui['low'] & fpaccess['high'], condition['good']),
        ctrl.Rule(rvd['significant'] & lui['moderate'] & fpaccess['low'], condition['poor']),
        ctrl.Rule(rvd['significant'] & lui['moderate'] & fpaccess['high'], condition['moderate']),
        ctrl.Rule(rvd['minor'] & lui['low'] & fpaccess['low'], condition['moderate']),
        ctrl.Rule(rvd['minor'] & lui['low'] & fpaccess['moderate'], condition['good']),
        ctrl.Rule(rvd['minor'] & lui['low'] & fpaccess['high'], condition['intact']),
        ctrl.Rule(rvd['minor'] & lui['moderate'] & fpaccess['low'], condition['moderate']),
        ctrl.Rule(rvd['minor'] & lui['moderate'] & fpaccess['high'], condition['moderate']),
        ctrl.Rule(rvd['negligible'] & lui['low'] & fpaccess['low'], condition['moderate']),
        ctrl.Rule(rvd['negligible'] & lui['low'] & fpaccess['moderate'], condition['good']),
        ctrl.Rule(rvd['negligible'] & lui['low'] & fpaccess['high'], condition['intact']),
        ctrl.Rule(rvd['negligible'] & lui['moderate'] & fpaccess['low'], condition['moderate']),
        ctrl.Rule(rvd['negligible'] & lui['moderate'] & fpaccess['high'], condition['good'])
    ])

    rcat_fis = ctrl.ControlSystemSimulation(rcat_ctrl)

    progbar = ProgressBar(len(id_array), 50, "RCAT FIS")
    counter = 0

    for i, id in enumerate(id_array):

        condition = 0

        rcat_fis.input['input1'] = rvd_array[i]
        rcat_fis.input['input2'] = lui_array[i]
        rcat_fis.input['input3'] = fpaccess_array[i]
        rcat_fis.compute()
        condition = rescale(rcat_fis.output['result'], 0.033, 0.9766)

        feature_values[id]['Condition'] = round(condition, 2)

        counter += 1
        progbar.update(counter)

    progbar.finish()
    log.info('Done')


def rescale(value, old_min, old_max):
    """Rescale a value from one range to another

    Arguments:
        value (float): The value to rescale
        old_min (float): The minimum of the old range
        old_max (float): The maximum of the old range
        new_min (float): The minimum of the new range
        new_max (float): The maximum of the new range

    Returns:
        float: The rescaled value
    """
    return (value - old_min) / (old_max - old_min)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='The RCAT database (output geopackage)', type=str)
    parser.add_argument('igos', help='True if the FIS is being run for the IGO output', type=bool)

    args = dotenv.parse_args_env(parser)

    rcat_fis(args.database, args.igos)


if __name__ == '__main__':
    main()
