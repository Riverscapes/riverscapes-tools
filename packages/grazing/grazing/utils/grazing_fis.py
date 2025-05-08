import rasterio
import skfuzzy as fuzz
import skfuzzy.control as ctrl
import numpy as np

from rscommons import Logger


def calculate_grazing_fis(water_promixity: str, slope: str, vegetation_suitability: str, output_raster: str):

    log = Logger('Grazing Likelihood FIS')

    with rasterio.open(water_promixity) as water_src, \
        rasterio.open(slope) as slope_src, \
            rasterio.open(vegetation_suitability) as veg_src:
        water_array = water_src.read(1)
        slope_array = slope_src.read(1)
        slope_nd = slope_src.nodata
        meta = slope_src.meta
        veg_array = veg_src.read(1)

        # mask out nodata values
        mask = slope_array == slope_nd
        water_array = np.ma.masked_where(mask, water_array)
        slope_array = np.ma.masked_where(mask, slope_array)
        veg_array = np.ma.masked_where(mask, veg_array)

    if water_array.shape != slope_array.shape or water_array.shape != veg_array.shape:
        raise ValueError("All input rasters must have the same shape.")

    # create nodata array for output
    output_array = np.full(slope_array.shape, slope_nd, dtype=np.float32)
    output_array = np.ma.masked_where(mask, output_array)

    # limit input values to ranges
    water_array[water_array > 50000] = 50000
    slope_array[slope_array > 100] = 100
    veg_array[veg_array > 4] = 4

    # antecedents and consequent
    water_in = ctrl.Antecedent(np.arange(0, 50000, 1), 'water')
    slope_in = ctrl.Antecedent(np.arange(0, 100, 1), 'slope')
    veg_in = ctrl.Antecedent(np.arange(0, 4, 0.01), 'veg')
    grazing_out = ctrl.Consequent(np.arange(0, 1, 0.01), 'grazing')

    water_in['close'] = fuzz.trapmf(water_in.universe, [0, 0, 100, 500])
    water_in['moderately_far'] = fuzz.trapmf(water_in.universe, [100, 500, 800, 2000])
    water_in['too_far'] = fuzz.trapmf(water_in.universe, [800, 2000, 50000, 50000])

    slope_in['flat'] = fuzz.trapmf(slope_in.universe, [0, 0, 2, 3])
    slope_in['gentle'] = fuzz.trapmf(slope_in.universe, [2, 3, 11, 17])
    slope_in['too_steep'] = fuzz.trapmf(slope_in.universe, [11, 17, 100, 100])

    veg_in['unsuitable'] = fuzz.trapmf(veg_in.universe, [0, 0, 0.5, 1])
    veg_in['barely_suitable'] = fuzz.trapmf(veg_in.universe, [0.5, 1, 1.5, 2])
    veg_in['moderately_suitable'] = fuzz.trapmf(veg_in.universe, [1.5, 2, 2.5, 3])
    veg_in['suitable'] = fuzz.trapmf(veg_in.universe, [2.5, 3, 3.5, 4])
    veg_in['preferred'] = fuzz.trimf(veg_in.universe, [3.5, 4, 4])

    grazing_out['none'] = fuzz.trimf(grazing_out.universe, [0, 0, 0.0005])
    grazing_out['low_prob_use'] = fuzz.trapmf(grazing_out.universe, [0, 0.0005, 0.4, 0.5])
    grazing_out['moderate_prob_use'] = fuzz.trapmf(grazing_out.universe, [0.4, 0.5, 0.9, 0.95])
    grazing_out['high_prob_use'] = fuzz.trapmf(grazing_out.universe, [0.9, 0.95, 1, 1])

    log.info('Building Rules for FIS')
    grazing_ctrl = ctrl.ControlSystem([
        ctrl.Rule(veg_in['unsuitable'], grazing_out['none']),
        ctrl.Rule(veg_in['barely_suitable'] & slope_in['flat'] & water_in['close'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['barely_suitable'] & slope_in['gentle'] & water_in['close'], grazing_out['moderate_prob_use']),
        ctrl.Rule(veg_in['moderately_suitable'] & slope_in['flat'] & water_in['close'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['suitable'] & slope_in['flat'] & water_in['close'], grazing_out['moderate_prob_use']),
        ctrl.Rule(veg_in['preferred'] & slope_in['flat'] & water_in['close'], grazing_out['high_prob_use']),
        ctrl.Rule(veg_in['barely_suitable'] & slope_in['gentle'] & water_in['close'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['moderately_suitable'] & slope_in['gentle'] & water_in['close'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['suitable'] & slope_in['gentle'] & water_in['close'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['preferred'] & slope_in['gentle'] & water_in['close'], grazing_out['moderate_prob_use']),
        ctrl.Rule(veg_in['barely_suitable'] & slope_in['too_steep'] & water_in['close'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['moderately_suitable'] & slope_in['too_steep'] & water_in['close'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['suitable'] & slope_in['too_steep'] & water_in['close'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['preferred'] & slope_in['too_steep'] & water_in['close'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['barely_suitable'] & slope_in['flat'] & water_in['moderately_far'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['moderately_suitable'] & slope_in['flat'] & water_in['moderately_far'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['suitable'] & slope_in['flat'] & water_in['moderately_far'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['preferred'] & slope_in['flat'] & water_in['moderately_far'], grazing_out['moderate_prob_use']),
        ctrl.Rule(veg_in['barely_suitable'] & slope_in['gentle'] & water_in['moderately_far'], grazing_out['none']),
        ctrl.Rule(veg_in['moderately_suitable'] & slope_in['gentle'] & water_in['moderately_far'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['suitable'] & slope_in['gentle'] & water_in['moderately_far'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['preferred'] & slope_in['gentle'] & water_in['moderately_far'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['barely_suitable'] & slope_in['too_steep'] & water_in['moderately_far'], grazing_out['none']),
        ctrl.Rule(veg_in['moderately_suitable'] & slope_in['too_steep'] & water_in['moderately_far'], grazing_out['none']),
        ctrl.Rule(veg_in['suitable'] & slope_in['too_steep'] & water_in['moderately_far'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['preferred'] & slope_in['too_steep'] & water_in['moderately_far'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['barely_suitable'] & slope_in['flat'] & water_in['too_far'], grazing_out['none']),
        ctrl.Rule(veg_in['moderately_suitable'] & slope_in['flat'] & water_in['too_far'], grazing_out['none']),
        ctrl.Rule(veg_in['suitable'] & slope_in['flat'] & water_in['too_far'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['preferred'] & slope_in['flat'] & water_in['too_far'], grazing_out['moderate_prob_use']),
        ctrl.Rule(veg_in['barely_suitable'] & slope_in['gentle'] & water_in['too_far'], grazing_out['none']),
        ctrl.Rule(veg_in['moderately_suitable'] & slope_in['gentle'] & water_in['too_far'], grazing_out['none']),
        ctrl.Rule(veg_in['suitable'] & slope_in['gentle'] & water_in['too_far'], grazing_out['none']),
        ctrl.Rule(veg_in['preferred'] & slope_in['gentle'] & water_in['too_far'], grazing_out['low_prob_use']),
        ctrl.Rule(veg_in['barely_suitable'] & slope_in['too_steep'] & water_in['too_far'], grazing_out['none']),
        ctrl.Rule(veg_in['moderately_suitable'] & slope_in['too_steep'] & water_in['too_far'], grazing_out['none']),
        ctrl.Rule(veg_in['suitable'] & slope_in['too_steep'] & water_in['too_far'], grazing_out['none']),
        ctrl.Rule(veg_in['preferred'] & slope_in['too_steep'] & water_in['too_far'], grazing_out['low_prob_use'])
    ])

    grazing_fis = ctrl.ControlSystemSimulation(grazing_ctrl)

    x_vals = np.arange(0, 1, 0.001)
    mfx = fuzz.trimf(x_vals, [0, 0, 0.0005])
    defuzz_centroid = round(fuzz.defuzz(x_vals, mfx, 'centroid'), 6)

    def apply_fis(water, slope, veg):
        """
        Apply the FIS to the input values.
        """
        grazing_fis.input['water'] = water
        grazing_fis.input['slope'] = slope
        grazing_fis.input['veg'] = veg
        grazing_fis.compute()
        return grazing_fis.output['grazing']

    vectorized_fis = np.vectorize(apply_fis)

    log.info('Applying FIS to input data')
    output_array = vectorized_fis(water_array, slope_array, veg_array)
    output_array[output_array == defuzz_centroid] = 0

    with rasterio.open(output_raster, 'w', **meta) as dst:
        dst.write(output_array.astype(meta['dtype']), 1)
