import rasterio
import skfuzzy as fuzz
import skfuzzy.control as ctrl
import numpy as np


def calculate_grazing_fis(water_promixity: str, slope: str, vegetation_suitability: str):

    with rasterio.open(water_promixity) as water_src, \
        rasterio.open(slope) as slope_src, \
            rasterio.open(vegetation_suitability) as veg_src:
        water_array = water_src.read(1)
        slope_array = slope_src.read(1)
        veg_array = veg_src.read(1)

    if water_array.shape != slope_array.shape or water_array.shape != veg_array.shape:
        raise ValueError("All input rasters must have the same shape.")

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
