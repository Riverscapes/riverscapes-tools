import os
import sqlite3
import rasterio
import numpy as np
from rasterio.mask import mask

from rscommons import Logger, ProgressBar, GeopackageLayer, VectorBase
from rscommons.classes.vector_base import get_utm_zone_epsg


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
                raw_raster = mask(src, [dgo_geom], crop=True)[0]
                mask_raster = np.ma.masked_values(raw_raster, src.nodata)

                vals = []
                for val in np.unique(mask_raster):
                    if val == src.nodata:
                        continue
                    vals.append(val)

                if len(vals) == 0:
                    log.warning(f'No values found in DGO {dgoid}')
                    continue
                dgo_vals[dgoid] = np.mean(vals)
            except Exception as e:
                log.error(f'Error processing DGO {dgoid}: {e}')
                continue

    for igo_id, dgo_ids in windows.items():
        dgovals = []
        for dgoid in dgo_ids:
            if dgoid in dgo_vals:
                dgovals.append(dgo_vals[dgoid])
            else:
                log.warning(f'DGO {dgoid} not found in grazing probability calculation')
        if len(dgovals) == 0:
            log.warning(f'No values found for IGO {igo_id}')
            continue
        igo_vals[igo_id] = np.mean(dgovals)

    with sqlite3.connect(gpkg_path) as conn:
        with conn:
            cursor = conn.cursor()
            for dgoid, val in dgo_vals.items():
                cursor.execute(
                    f"UPDATE grazing_dgos SET grazing_probability = ? WHERE id = ?",
                    (val, dgoid)
                )
            conn.commit()

            for igo_id, val in igo_vals.items():
                cursor.execute(
                    f"UPDATE grazing_igos SET grazing_probability = ? WHERE id = ?",
                    (val, igo_id)
                )
            conn.commit()

    log.info('Grazing probability calculation complete')
