import numpy as np
import rasterio
from rsxml import Logger, ProgressBar
from rscommons.database import SQLiteCon


def vegetation_suitability(gpkg_path: str, input_raster: str, output_raster: str):

    log = Logger("Vegetation Suitability")

    with SQLiteCon(gpkg_path) as db:
        # Read the vegetation suitability data from the database
        db.curs.execute("SELECT VegetationID, EffectiveSuitability FROM vwVegetationSuitability")
        suitability = {row['VegetationID']: row['EffectiveSuitability'] for row in db.curs.fetchall()}

    def translate(in_val, in_nodata, out_nodata):
        """
        Translate the input value to the output value.
        """
        if in_val == in_nodata:
            return out_nodata
        elif in_val in suitability:
            return suitability[in_val]
        else:
            log.warning(f"Could not find suitability for VegetationID {in_val}.")
            return out_nodata

    vector = np.vectorize(translate)

    with rasterio.open(input_raster) as src:
        out_meta = src.meta
        out_meta['dtype'] = 'int16'
        out_meta['nodata'] = -9999
        out_meta['compress'] = 'deflate'

        with rasterio.open(output_raster, 'w', **out_meta) as dst:
            progbar = ProgressBar(len(list(src.block_windows(1))), 50, "Writing Vegetation Suitability Raster")
            counter = 0
            for ji, window in dst.block_windows(1):
                progbar.update(counter)
                counter += 1
                # Read the data from the source raster
                data = src.read(1, window=window, masked=True)

                # Apply the translation function to the data
                out_data = vector(data, src.nodata, out_meta['nodata'])

                # Write the output data to the destination raster
                dst.write(out_data.astype(out_meta['dtype']), window=window, indexes=1)

            progbar.finish()

    log.info("Vegetation suitability raster created successfully.")
