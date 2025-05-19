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

    slope = os.path.join(grazing_dir, 'inputs/slope.tif')
    proximity = os.path.join(grazing_dir, 'intermediates/proximity.tif')

    with GeopackageLayer(allotments) as allotment_lyr, \
            rasterio.open(slope) as slope_src, \
            rasterio.open(proximity) as proximity_src:

        for allotment_ftr, _counter, _progbar in allotment_lyr.iterate_features("Processing allotment features"):

            allot_name = allotment_ftr.GetField('ALLOT_NAME')
            allot_acres = allotment_ftr.GetField('GIS_ACRES')
            allot_shapely = VectorBase.ogr2shapely(allotment_ftr.GetGeometryRef())

            try:
                raw_prox_raster = mask(proximity_src, [allot_shapely], crop=True)[0]
                mask_prox_raster = np.ma.masked_values(raw_prox_raster, proximity_src.nodata)
                raw_slope_raster = mask(slope_src, [allot_shapely], crop=True)[0]
                mask_slope_raster = np.ma.masked_values(raw_slope_raster, slope_src.nodata)

                # Proximity
                prox_vals = []
                for val in np.unique(mask_prox_raster):
                    if val == proximity_src.nodata:
                        continue
                    prox_vals.append(val * M_TO_MILES)
                prox_vals = [v for v in prox_vals if not ma.is_masked(v)]

                if len(prox_vals) == 0:
                    print(f'No values found in allotment {allot_name} proximity raster')
                    continue

                # Slope
                slope_vals = []
                for val in np.unique(mask_slope_raster):
                    if val == slope_src.nodata:
                        continue
                    slope_vals.append(val)
                slope_vals = [v for v in slope_vals if not ma.is_masked(v)]
                if len(slope_vals) == 0:
                    print(f'No values found in allotment {allot_name} slope raster')
                    continue

                proxone = prox_vals[prox_vals < 1]
                proxtwo = prox_vals[1 <= prox_vals < 2]
                proxthree = prox_vals[prox_vals >= 2]
                slopeone = slope_vals[slope_vals < 10]
                slopetwo = slope_vals[10 <= slope_vals < 30]
                slopethree = slope_vals[30 <= slope_vals < 60]
                slopefour = slope_vals[slope_vals >= 60]
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
                    }
                }
            except Exception as e:
                print(f'Error processing allotment {allot_name}: {e}')
                continue

    with open(out_path, 'w') as f:
        json.dump(outputs, f, indent=4)
    print(f'Grazing model summaries written to {out_path}')
