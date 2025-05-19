import os
import sqlite3
import rasterio
import numpy as np
import numpy.ma as ma
from rasterio.mask import mask

from rscommons import Logger, GeopackageLayer, VectorBase


def riverscape_grazing(likelihood: str, gpkg_path: str, windows: dict):

    log = Logger('Riverscapes Grazing')

    dgo = os.path.join(gpkg_path, 'grazing_dgos')
    dgo_vals = {}
    igo_vals = {}

    log.info('Calculating grazing probability in DGOs')
    with GeopackageLayer(dgo) as dgo_lyr, rasterio.open(likelihood) as src:

        for dgo_ftr, _counter, _progbar in dgo_lyr.iterate_features("Processing DGO features"):
            dgoid = dgo_ftr.GetFID()
            dgo_geom = dgo_ftr.GetGeometryRef()
            centerline_len = dgo_ftr.GetField('centerline_length')
            seg_dist = dgo_ftr.GetField('seg_distance')
            if seg_dist is None:
                continue

            try:
                dgo_shap = VectorBase.ogr2shapely(dgo_geom)
                raw_raster = mask(src, [dgo_shap], crop=True)[0]
                mask_raster = np.ma.masked_values(raw_raster, src.nodata)

                vals = []
                for val in np.unique(mask_raster):
                    if val == src.nodata:
                        continue
                    vals.append(val)

                vals = [v for v in vals if not ma.is_masked(v)]
                if len(vals) == 0:
                    log.warning(f'No values found in DGO {dgoid}')
                    continue

                dgo_vals[dgoid] = np.mean(vals)
            except Exception as e:
                log.error(f'Error processing DGO {dgoid}: {e}')
                continue

    with sqlite3.connect(gpkg_path) as conn:
        cursor = conn.cursor()
        for igo_id, dgo_ids in windows.items():
            dgovals = []
            for dgoid in dgo_ids:
                if dgoid in dgo_vals:
                    cursor.execute(f"SELECT segment_area FROM grazing_dgos WHERE DGOID = {dgoid}")
                    seg_area = cursor.fetchone()
                    dgovals.append([dgo_vals[dgoid], seg_area[0]])
                else:
                    log.warning(f'DGO {dgoid} not found in grazing probability calculation')
            if len(dgovals) == 0:
                log.warning(f'No values found for IGO {igo_id}')
                continue
            tot_area = sum(dgo[1] for dgo in dgovals)
            igo_vals[igo_id] = sum(
                dgo[0] * (dgo[1] / tot_area) for dgo in dgovals
            )

        log.info('Updating DGO Attributes')
        for dgoid, val in dgo_vals.items():
            cursor.execute(
                f"UPDATE DGOAttributes SET grazing_likelihood = {val} WHERE DGOID = {dgoid}"
            )
        conn.commit()

        log.info('Updating IGO Attributes')
        for igo_id, val in igo_vals.items():
            cursor.execute(
                f"UPDATE IGOAttributes SET grazing_likelihood = {val} WHERE IGOID = {igo_id}"
            )
        conn.commit()

    log.info('Grazing probability calculation complete')
