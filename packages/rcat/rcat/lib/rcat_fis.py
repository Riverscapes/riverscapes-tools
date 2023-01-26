import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl
from rscommons.database import load_attributes, write_db_attributes
from rscommons import Logger, ProgressBar


def rcat_fis(database: str, igos: bool):

    log = Logger('RCAT FIS')
    log.info('Starting RCAT FIS')

    if igos is True:
        fields = ['RiparianDeparture', 'LUI', 'FloodplainAccess']
    else:
        fields = ['RiparianDeparture', 'iPC_LU', 'FloodplainAccess']

    reaches = load_attributes(database, )


# set up FIS
rvd = ctrl.Antecedent(np.arange(0, 1, 0.01), "input1")
lui = ctrl.Antecedent(np.arange(0, 1, 0.01), "input2")
connect = ctrl.Antecedent(np.arange(0, 1, 0.01), "input3")
condition = ctrl.Consequent(np.arange(0, 1, 0.01), "result")

rvd["large"] = fuzz.trapmf(rvd.universe, [0, 0, 0.3, 0.5])
rvd["significant"] = fuzz.trimf(rvd.universe, [0.3, 0.5, 0.85])
rvd["minor"] = fuzz.trimf(rvd.universe, [0.5, 0.85, 0.95])
rvd["negligible"] = fuzz.trapmf(rvd.universe, [0.85, 0.95, 1, 1])

lui["high"] = fuzz.trapmf(lui.universe, [0, 0, 0.416, 0.583])
lui["moderate"] = fuzz.trapmf(lui.universe, [0.416, 0.583, 0.83, 0.983])
lui["low"] = fuzz.trapmf(lui.universe, [0.83, 0.983, 1, 1])

connect["low"] = fuzz.trapmf(connect.universe, [0, 0, 0.5, 0.7])
connect["moderate"] = fuzz.trapmf(connect.universe, [0.5, 0.7, 0.9, 0.95])
connect["high"] = fuzz.trapmf(connect.universe, [0.9, 0.95, 1, 1])

condition["very poor"] = fuzz.trapmf(condition.universe, [0, 0, 0.1, 0.25])
condition["poor"] = fuzz.trapmf(condition.universe, [0.1, 0.25, 0.35, 0.5])
condition["moderate"] = fuzz.trimf(condition.universe, [0.35, 0.5, 0.8])
condition["good"] = fuzz.trimf(condition.universe, [0.5, 0.8, 0.95])
condition["intact"] = fuzz.trapmf(condition.universe, [0.8, 0.95, 1, 1])

rcat_ctrl = ctrl.ControlSystem([
    ctrl.Rule(rvd['large'] & lui['low'] & connect['low'], condition['poor']),
    ctrl.Rule(rvd['large'] & lui['low'] & connect['moderate'], condition['poor']),
    ctrl.Rule(rvd['large'] & lui['low'] & connect['high'], condition['moderate']),
    ctrl.Rule(rvd['large'] & lui['moderate'] & connect['low'], condition['poor']),
    ctrl.Rule(lui['moderate'] & connect['moderate'], condition['moderate']),
    ctrl.Rule(rvd['large'] & lui['moderate'] & connect['high'], condition['poor']),
    ctrl.Rule(rvd['large'] & lui['high'] & connect['low'], condition['very poor']),
    ctrl.Rule((rvd['significant'] | RVD['minor'] | RVD['negligible']) & lui['high'] & connect['low'], condition['poor']),
    ctrl.Rule(lui['high'] & connect['moderate'], condition['poor']),
    ctrl.Rule(lui['high'] & connect['high'], condition['moderate']),
    ctrl.Rule(rvd['significant'] & lui['low'] & connect['low'], condition['moderate']),
    ctrl.Rule(rvd['significant'] & lui['low'] & connect['moderate'], condition['moderate']),
    ctrl.Rule(rvd['significant'] & lui['low'] & connect['high'], condition['good']),
    ctrl.Rule(rvd['significant'] & lui['moderate'] & connect['low'], condition['poor']),
    ctrl.Rule(rvd['significant'] & lui['moderate'] & connect['high'], condition['moderate']),
    ctrl.Rule(rvd['minor'] & lui['low'] & connect['low'], condition['moderate']),
    ctrl.Rule(rvd['minor'] & lui['low'] & connect['moderate'], condition['good']),
    ctrl.Rule(rvd['minor'] & lui['low'] & connect['high'], condition['intact']),
    ctrl.Rule(rvd['minor'] & lui['moderate'] & connect['low'], condition['moderate']),
    ctrl.Rule(rvd['minor'] & lui['moderate'] & connect['high'], condition['moderate']),
    ctrl.Rule(rvd['negligible'] & lui['low'] & connect['low'], condition['moderate']),
    ctrl.Rule(rvd['negligible'] & lui['low'] & connect['moderate'], condition['good']),
    ctrl.Rule(rvd['negligible'] & lui['low'] & connect['high'], condition['intact']),
    ctrl.Rule(rvd['negligible'] & lui['moderate'] & connect['low'], condition['moderate']),
    ctrl.Rule(rvd['negligible'] & lui['moderate'] & connect['high'], condition['good'])
])

rcat_fis = ctrl.ControlSystemSimulation(rcat_ctrl)

# Defuzzify
out = np.zeros(len(RVDarray))
for i in range(len(out)):
    rca_fis.input["input1"] = RVDarray[i]
    rca_fis.input["input2"] = luiarray[i]
    rca_fis.input["input3"] = connectarray[i]
    rca_fis.compute()
    out[i] = rca_fis.output["result"]
