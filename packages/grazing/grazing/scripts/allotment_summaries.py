import rasterio
from rasterio.mask import mask
import numpy as np
import numpy.ma as ma
import os
import json

from rscommons import GeopackageLayer, VectorBase

M_TO_MILES = 0.000621371


def grazing_model_summaries(grazing_dir: str, allotments: str, out_path: str):

    outputs = {}

    slope = os.path.join(grazing_dir, 'inputs/dem_slope.tif')
    proximity = os.path.join(grazing_dir, 'intermediates/proximity.tif')
    prob = os.path.join(grazing_dir, 'outputs/likelihood.tif')
    suit = os.path.join(grazing_dir, 'intermediates/veg_suitability.tif')

    with GeopackageLayer(allotments) as allotment_lyr, \
            rasterio.open(slope) as slope_src, \
            rasterio.open(proximity) as proximity_src, \
            rasterio.open(prob) as probability_src, \
            rasterio.open(suit) as suitability_src:

        for allotment_ftr, _counter, _progbar in allotment_lyr.iterate_features("Processing allotment features"):

            allot_name = allotment_ftr.GetField('ALLOT_NAME')
            allot_acres = allotment_ftr.GetField('GIS_ACRES')
            allot_shapely = VectorBase.ogr2shapely(allotment_ftr.GetGeometryRef())

            try:
                raw_prox_raster = mask(proximity_src, [allot_shapely], crop=True)[0]
                mask_prox_raster = np.ma.masked_values(raw_prox_raster, proximity_src.nodata)
                raw_slope_raster = mask(slope_src, [allot_shapely], crop=True)[0]
                mask_slope_raster = np.ma.masked_values(raw_slope_raster, slope_src.nodata)
                raw_prob_raster = mask(probability_src, [allot_shapely], crop=True)[0]
                mask_prob_raster = np.ma.masked_values(raw_prob_raster, probability_src.nodata)
                raw_suit_raster = mask(suitability_src, [allot_shapely], crop=True)[0]
                mask_suit_raster = np.ma.masked_values(raw_suit_raster, suitability_src.nodata)

                # Proximity
                print('Getting proximity values')
                prox_vals = []
                for val in np.nditer(mask_prox_raster):
                    if val == proximity_src.nodata:
                        continue
                    prox_vals.append(val * M_TO_MILES)
                prox_vals = [v for v in prox_vals if not ma.is_masked(v)]

                if len(prox_vals) == 0:
                    print(f'No values found in allotment {allot_name} proximity raster')
                    continue

                # Slope
                print('Getting slope values')
                slope_vals = []
                for val in np.nditer(mask_slope_raster):
                    if val == slope_src.nodata:
                        continue
                    slope_vals.append(val)
                slope_vals = [v for v in slope_vals if not ma.is_masked(v)]
                if len(slope_vals) == 0:
                    print(f'No values found in allotment {allot_name} slope raster')
                    continue

                # Probability
                print('Getting likelihood values')
                prob_vals = []
                for val in np.nditer(mask_prob_raster):
                    if val == probability_src.nodata:
                        continue
                    prob_vals.append(val)
                prob_vals = [v for v in prob_vals if not ma.is_masked(v)]
                if len(prob_vals) == 0:
                    print(f'No values found in allotment {allot_name} probability raster')
                    continue

                # Suitability
                print('Getting suitability values')
                suit_vals = []
                for val in np.nditer(mask_suit_raster):
                    if val == suitability_src.nodata:
                        continue
                    suit_vals.append(val)
                suit_vals = [v for v in suit_vals if not ma.is_masked(v)]
                if len(suit_vals) == 0:
                    print(f'No values found in allotment {allot_name} suitability raster')
                    continue

                # Create lists of values for each category
                unsuitable = [x for x in suit_vals if x == 0]
                barely_suitable = [x for x in suit_vals if x == 1]
                moderately_suitable = [x for x in suit_vals if x == 2]
                suitable = [x for x in suit_vals if x == 3]
                preferred = [x for x in suit_vals if x == 4]
                probone = [v for v in prob_vals if 0 <= v < 0.2]
                probtwo = [v for v in prob_vals if 0.2 <= v < 0.4]
                probthree = [v for v in prob_vals if 0.4 <= v < 0.6]
                probfour = [v for v in prob_vals if 0.6 <= v < 0.8]
                probfive = [v for v in prob_vals if 0.8 <= v < 1]
                proxone = [x for x in prox_vals if x < 1]
                proxtwo = [x for x in prox_vals if 1 <= x < 2]
                proxthree = [x for x in prox_vals if 2 <= x]
                slopeone = [x for x in slope_vals if x < 10]
                slopetwo = [x for x in slope_vals if 10 <= x < 30]
                slopethree = [x for x in slope_vals if 30 <= x < 60]
                slopefour = [x for x in slope_vals if x >= 60]

                print('Calculating acres and writing to json')
                outputs[allot_name] = {
                    'proximityToWater': {
                        '< 1 mile': (len(proxone) / len(prox_vals)) * allot_acres,
                        '1 - 2 miles': (len(proxtwo) / len(prox_vals)) * allot_acres,
                        '> 2 miles': (len(proxthree) / len(prox_vals)) * allot_acres
                    },
                    'slope': {
                        '< 10': (len(slopeone) / len(slope_vals)) * allot_acres,
                        '10 - 30': (len(slopetwo) / len(slope_vals)) * allot_acres,
                        '31 - 60': (len(slopethree) / len(slope_vals)) * allot_acres,
                        '> 60': (len(slopefour) / len(slope_vals)) * allot_acres
                    },
                    'likelihoodOfGrazing': {
                        '< 0.2': (len(probone) / len(prob_vals)) * allot_acres,
                        '0.2 - 0.4': (len(probtwo) / len(prob_vals)) * allot_acres,
                        '0.4 - 0.6': (len(probthree) / len(prob_vals)) * allot_acres,
                        '0.6 - 0.8': (len(probfour) / len(prob_vals)) * allot_acres,
                        '> 0.8': (len(probfive) / len(prob_vals)) * allot_acres
                    },
                    'palatability': {
                        'unsuitable': (len(unsuitable) / len(suit_vals)) * allot_acres,
                        'barely suitable': (len(barely_suitable) / len(suit_vals)) * allot_acres,
                        'moderately suitable': (len(moderately_suitable) / len(suit_vals)) * allot_acres,
                        'suitable': (len(suitable) / len(suit_vals)) * allot_acres,
                        'preferred': (len(preferred) / len(suit_vals)) * allot_acres
                    }
                }
            except Exception as e:
                print(f'Error processing allotment {allot_name}: {e}')
                continue

    with open(out_path, 'w') as f:
        json.dump(outputs, f, indent=4)
    print(f'Grazing model summaries written to {out_path}')


grazing_model_summaries('/workspaces/data/grazing/Owyhee', '/workspaces/data/Owyhee_GA.gpkg/owyhee_ga', '/workspaces/data/grazing/Owyhee/outputs/grazing_model_summaries.json')
