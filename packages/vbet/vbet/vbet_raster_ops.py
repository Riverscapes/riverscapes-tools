from tempfile import NamedTemporaryFile
from osgeo import gdal
import rasterio
import numpy as np
from rscommons import ProgressBar, Logger, VectorBase, Timer


def rasterize(in_lyr_path, out_raster_path, template_path):
    """Rasterizing an input 

    Args:
        in_lyr_path ([type]): [description]
        out_raster_ ([type]): [description]
        template_path ([type]): [description]
    """
    log = Logger('VBETRasterize')
    ds_path, lyr_path = VectorBase.path_sorter(in_lyr_path)

    progbar = ProgressBar(100, 50, "Rasterizing ")

    with rasterio.open(template_path) as raster:
        t = raster.transform
        raster_bounds = raster.bounds

    def poly_progress(progress, _msg, _data):
        progbar.update(int(progress * 100))

    # Rasterize the features (roads, rail etc) and calculate a raster of Euclidean distance from these features
    progbar.update(0)

    # Rasterize the polygon to a temporary file
    with NamedTemporaryFile(suffix='.tif', mode="w+", delete=True) as tempfile:
        log.debug('Temporary file: {}'.format(tempfile.name))
        gdal.Rasterize(
            tempfile.name,
            ds_path,
            layers=[lyr_path],
            xRes=t[0], yRes=t[4],
            burnValues=1, outputType=gdal.GDT_Int16,
            creationOptions=['COMPRESS=LZW'],
            # outputBounds --- assigned output bounds: [minx, miny, maxx, maxy]
            outputBounds=[raster_bounds.left, raster_bounds.bottom, raster_bounds.right, raster_bounds.top],
            callback=poly_progress
        )
        progbar.finish()

        # Now mask the output correctly
        mask_rasters_nodata(tempfile.name, template_path, out_raster_path)


def mask_rasters_nodata(in_raster_path, nodata_raster_path, out_raster_path):
    """Apply the nodata values of one raster to another of identical size

    Args:
        in_raster_path ([type]): [description]
        nodata_raster_path ([type]): [description]
        out_raster_path ([type]): [description]
    """
    log = Logger('mask_rasters_nodata')

    with rasterio.open(nodata_raster_path) as nd_src, rasterio.open(in_raster_path) as data_src:
        # All 3 rasters should have the same extent and properties. They differ only in dtype
        out_meta = data_src.meta
        out_meta['nodata'] = -9999
        out_meta['compress'] = 'deflate'

        with rasterio.open(out_raster_path, 'w', **out_meta) as out_src:
            progbar = ProgressBar(len(list(data_src.block_windows(1))), 50, "Applying nodata mask")
            counter = 0
            # Again, these rasters should be orthogonal so their windows should also line up
            for ji, window in data_src.block_windows(1):
                progbar.update(counter)
                counter += 1
                # These rasterizations don't begin life with a mask.
                mask = nd_src.read(1, window=window, masked=True).mask
                data = data_src.read(1, window=window)
                # Fill everywhere the mask reads true with a nodata value
                output = np.ma.masked_array(data, mask)
                out_src.write(output.filled(out_meta['nodata']), window=window, indexes=1)

            progbar.finish()
            log.info('Complete')


# Compute Proximity for channel rasters
def proximity_raster(src_raster_path: str, out_raster_path: str, dist_units="PIXEL"):
    """Create a proximity raster

    Args:
        src_raster_path ([type]): [description]
        out_raster_path ([type]): [description]
        dist_units (str, optional): set to "GEO" for distance in length . Defaults to "PIXEL".
    """
    log = Logger('proximity_raster')
    tmr = Timer()
    src_ds = gdal.Open(src_raster_path)
    srcband = src_ds.GetRasterBand(1)

    drv = gdal.GetDriverByName('GTiff')
    with NamedTemporaryFile(suffix=".tif", mode="w+", delete=True) as tempfile:
        dst_ds = drv.Create(tempfile.name,
                            src_ds.RasterXSize, src_ds.RasterYSize, 1,
                            gdal.GetDataTypeByName('Float32'))

        dst_ds.SetGeoTransform(src_ds.GetGeoTransform())
        dst_ds.SetProjection(src_ds.GetProjectionRef())

        dstband = dst_ds.GetRasterBand(1)

        log.info('Creating proximity raster')
        gdal.ComputeProximity(srcband, dstband, ["VALUES=1", f"DISTUNITS={dist_units}", "COMPRESS=DEFLATE"])

        srcband = None
        dstband = None
        src_ds = None
        dst_ds = None

        # Preserve the nodata from the source
        mask_rasters_nodata(tempfile.name, src_raster_path, out_raster_path)
        log.info('completed in {}'.format(tmr.toString()))


def translate(vrtpath_in: str, raster_out_path: str, band: int):
    """GDAL translate Operation from VRT 

    Args:
        vrtpath_in ([type]): [description]
        raster_out_path ([type]): [description]
        band ([int]): raster band
    """
    log = Logger('translate')
    tmr = Timer()
    progbar = ProgressBar(100, 50, "Translating ")

    def translate_progress(progress, _msg, _data):
        progbar.update(int(progress * 100))

    translateoptions = gdal.TranslateOptions(gdal.ParseCommandLine("-of Gtiff -b {} -co COMPRESS=DEFLATE".format(band)))
    gdal.Translate(raster_out_path, vrtpath_in, options=translateoptions, callback=translate_progress)

    log.info('completed in {}'.format(tmr.toString()))
