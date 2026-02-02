import os
import shutil
import rasterio
import numpy as np

from rscommons import Timer, TempRaster
from rscommons.classes.raster import deleteRaster
from rsxml import Logger, ProgressBar

from osgeo import gdal, gdal_array

Path = str


def mask_rasters_nodata(in_raster_path: Path, nodata_raster_path: Path, out_raster_path: Path):
    """Apply the nodata values of one raster to another of identical size

    Args:
        in_raster_path (Path): input raster with values
        nodata_raster_path (Path): raster with mask for no data values
        out_raster_path (Path): output raster
    """
    log = Logger('mask_rasters_nodata')

    with rasterio.open(nodata_raster_path) as nd_src, \
            rasterio.open(in_raster_path) as data_src:
        # All 3 rasters should have the same extent and properties. They differ only in dtype
        out_meta = data_src.meta
        out_meta['nodata'] = -9999
        out_meta['compress'] = 'deflate'

        progbar = ProgressBar(len(list(data_src.block_windows(1))), 50, "Applying nodata mask")
        with rasterio.open(out_raster_path, 'w', **out_meta) as out_src:
            counter = 0
            # Again, these rasters should be orthogonal so their windows should also line up
            for _ji, window in data_src.block_windows(1):
                progbar.update(counter)
                counter += 1
                # These rasterizations don't begin life with a mask.
                nd_arr = nd_src.read(1, window=window, masked=True)
                data = data_src.read(1, window=window, masked=True)
                # Combine the mask of the nd_src with that of the data. This is done in-place
                data.mask = np.logical_or(nd_arr.mask, data.mask)

                out_src.write(data, window=window, indexes=1)

        progbar.finish()


def proximity_raster(src_raster_path: Path, out_raster_path: Path, dist_units: str = "PIXEL", preserve_nodata: bool = True, dist_factor=None):
    """Create a proximity raster

    Args:
        src_raster_path ([type]): path of raster with pixels to use as proximity basis
        out_raster_path ([type]): path of output raster
        dist_units (str, optional): set to "GEO" for distance in length . Defaults to "PIXEL"
        preserve_nodata (bool, optional): Keep no data extent? Defaults to True
        dist_factor (float, optional): divide proximity by factor. Defaults to None
    """
    log = Logger('proximity_raster')
    tmr = Timer()
    src_ds = gdal.Open(src_raster_path)
    srcband = src_ds.GetRasterBand(1)

    drv = gdal.GetDriverByName('GTiff')
    with TempRaster('vbet_proximity_raster') as tempfile:  # , \
        # TempRaster('vbet_proximity_raster_distance') as tempfile_dist:

        temp_path = tempfile.filepath
        dst_ds = drv.Create(temp_path,
                            src_ds.RasterXSize, src_ds.RasterYSize, 1,
                            gdal.GetDataTypeByName('Float32'))

        dst_ds.SetGeoTransform(src_ds.GetGeoTransform())
        dst_ds.SetProjection(src_ds.GetProjectionRef())

        dstband = dst_ds.GetRasterBand(1)
        # Set COMPRESS to DEFLATE for this raster
        dstband.SetMetadataItem("COMPRESS", "DEFLATE")

        progbar = ProgressBar(100, 50, "ComputeProximity ")

        def poly_progress(progress, _msg, _data):
            progbar.update(int(progress * 100))

        log.info('Creating proximity raster')
        progbar.update(0)
        gdal.ComputeProximity(srcband, dstband, callback=poly_progress, options=["VALUES=1", f"DISTUNITS={dist_units}"])
        progbar.finish()

        srcband = None
        dstband = None
        src_ds = None
        dst_ds = None

        if dist_factor is not None:
            dist_file = os.path.join(os.path.dirname(tempfile.filepath), "raster.tif")

            ds = gdal.Open(temp_path)
            b1 = ds.GetRasterBand(1)
            arr = b1.ReadAsArray()

            # apply equation
            data = arr / dist_factor

            # save array, using ds as a prototype
            gdal_array.SaveArray(data.astype("float32"), dist_file, "GTIFF", ds)
            ds = None

            # scrips_dir = next(p for p in sys.path if p.endswith('.venv'))
            # script = os.path.join(scrips_dir, 'Scripts', 'gdal_calc.py')
            # run_subprocess(os.path.dirname(tempfile.filepath), ['python', script, '-A', temp_path, f'--outfile={dist_file}', f'--calc=A/{dist_factor}', '--co=COMPRESS=LZW'])

            if os.path.exists(temp_path):
                deleteRaster(temp_path)

            temp_path = dist_file

        # Preserve the nodata from the source
        if preserve_nodata:
            mask_rasters_nodata(temp_path, src_raster_path, out_raster_path)
        else:
            shutil.copyfile(temp_path, out_raster_path)

    log.info(f'completed in {tmr.toString()}')
