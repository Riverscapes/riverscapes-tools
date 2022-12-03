""" VBET Raster Operations

    Purpose:  Tools to support VBET raster operations
    Author:   North Arrow Research
    Date:     August 2022
"""

import os
import shutil

from osgeo import ogr, gdal, gdal_array, osr
import rasterio
from rasterio.windows import Window
# from rasterio.windows import bounds, from_bounds
import numpy as np

from rscommons import ProgressBar, Logger, VectorBase, Timer, TempRaster
from rscommons.classes.raster import PrintArr

Path = str


def rasterize(in_lyr_path: Path, out_raster_path: Path, template_path: Path, all_touched: bool = False):
    """Rasterize an input layer

    Args:
        in_lyr_path (Path): vector geometry path
        out_raster_path (Path): output raster path
        template_path (Path): path of existing raster for configuration and extent template
        all_touched (bool, optional): geos parameter of all touched (pixel if any part of geom touches cell). Defaults to False.
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
    with TempRaster('vbet_rasterize') as tempfile:
        log.debug('Temporary file: {}'.format(tempfile.filepath))
        gdal.Rasterize(
            tempfile.filepath,
            ds_path,
            layers=[lyr_path],
            xRes=t[0], yRes=t[4],
            allTouched=all_touched,
            burnValues=1, outputType=gdal.GDT_Int16,
            creationOptions=['COMPRESS=LZW'],
            # outputBounds --- assigned output bounds: [minx, miny, maxx, maxy]
            outputBounds=[raster_bounds.left, raster_bounds.bottom, raster_bounds.right, raster_bounds.top],
            callback=poly_progress
        )
        progbar.finish()

        # Now mask the output correctly
        mask_rasters_nodata(tempfile.filepath, template_path, out_raster_path)


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

        progbar = ProgressBar(100, 50, "ComputeProximity ")

        def poly_progress(progress, _msg, _data):
            progbar.update(int(progress * 100))

        log.info('Creating proximity raster')
        progbar.update(0)
        gdal.ComputeProximity(srcband, dstband, callback=poly_progress, options=["VALUES=1", f"DISTUNITS={dist_units}", "COMPRESS=DEFLATE"])
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
                os.remove(temp_path)

            temp_path = dist_file

        # Preserve the nodata from the source
        if preserve_nodata:
            mask_rasters_nodata(temp_path, src_raster_path, out_raster_path)
        else:
            shutil.copyfile(temp_path, out_raster_path)

        if os.path.exists(temp_path):
            os.remove(temp_path)

        log.info('completed in {}'.format(tmr.toString()))


def translate(vrtpath_in: Path, raster_out_path: Path, band: int):
    """GDAL translate Operation from VRT

    Args:
        vrtpath_in (Path): input vrt path
        raster_out_path (Path): output raster path
        band (int): raster band
    """
    log = Logger('translate')
    tmr = Timer()
    progbar = ProgressBar(100, 50, "Translating ")

    def translate_progress(progress, _msg, _data):
        progbar.update(int(progress * 100))

    translateoptions = gdal.TranslateOptions(gdal.ParseCommandLine("-of Gtiff -b {} -co COMPRESS=DEFLATE".format(band)))
    gdal.Translate(raster_out_path, vrtpath_in, options=translateoptions, callback=translate_progress)

    log.info('completed in {}'.format(tmr.toString()))


def inverse_mask(nodata_raster_path: Path, out_raster_path: Path):
    """ apply np logical_not on a raster

    Args:
        nodata_raster_path (Path): input raster
        out_raster_path (Path): output raster
    """
    log = Logger('mask_rasters_nodata')

    with rasterio.open(nodata_raster_path) as nd_src:
        # All 3 rasters should have the same extent and properties. They differ only in dtype
        out_meta = nd_src.meta
        if 'nodata' not in out_meta or out_meta['nodata'] is None:
            out_meta['nodata'] = -9999
        out_meta['compress'] = 'deflate'

        with rasterio.open(out_raster_path, 'w', **out_meta) as out_src:
            progbar = ProgressBar(len(list(nd_src.block_windows(1))), 50, "Applying inverse nodata mask")
            counter = 0
            # Again, these rasters should be orthogonal so their windows should also line up
            for _ji, window in nd_src.block_windows(1):
                progbar.update(counter)
                counter += 1
                # These rasterizations don't begin life with a mask.
                # TODO: Read mask is quicker?
                mask = nd_src.read(1, window=window, masked=True).mask

                # Fill everywhere the mask reads true with a nodata value
                mask_vals = np.ma.array(np.full(mask.shape, 1), mask=np.logical_not(mask))

                out_src.write(mask_vals.astype(out_meta['dtype']), window=window, indexes=1)

            progbar.finish()
            log.info('Complete')


def raster_clean(in_raster_path: Path, out_raster_path: Path, buffer_pixels: int = 1):
    """This method grows and shrinks the raster by n pixels

    Args:
        in_raster_path (Path): input raster
        out_raster_path (Path): output raster
        buffer_pixels (int, optional): how many pixels to grow and shrink. Defaults to 1.
    """

    log = Logger('raster_clean')

    with TempRaster('vbet_clean_prox_out') as tmp_prox_out, \
            TempRaster('vbet_clean_buff_out') as tmp_buff_out, \
            TempRaster('vbet_clean_prox_in') as tmp_prox_in, \
            TempRaster('vbet_clean_mask') as inv_mask:

        # 1. Find the proximity raster
        proximity_raster(in_raster_path, tmp_prox_out.filepath, preserve_nodata=False)

        # 2. Logical and the prox > 1 with the mask for the input raster
        with rasterio.open(tmp_prox_out.filepath) as prox_out_src, \
                rasterio.open(in_raster_path) as in_data_src:

            # All 3 rasters should have the same extent and properties. They differ only in dtype
            out_meta = in_data_src.meta
            # Rasterio can't write back to a VRT so rest the driver and number of bands for the output
            out_meta['driver'] = 'GTiff'
            out_meta['count'] = 1
            out_meta['compress'] = 'deflate'

            with rasterio.open(tmp_buff_out.filepath, 'w', **out_meta) as out_data:
                progbar = ProgressBar(len(list(out_data.block_windows(1))), 50, "Growing the raster by {} pixels".format(buffer_pixels))
                counter = 0
                # Again, these rasters should be orthogonal so their windows should also line up
                for _ji, window in out_data.block_windows(1):
                    progbar.update(counter)
                    counter += 1
                    prox_out_block = prox_out_src.read(1, window=window)
                    in_data_block = in_data_src.read(1, window=window, masked=True)

                    new_data = np.full(in_data_block.shape, 1)
                    new_mask = np.logical_and(in_data_block.mask, prox_out_block > buffer_pixels)

                    output = np.ma.masked_array(new_data, new_mask)

                    out_data.write(output.astype(out_meta['dtype']), window=window, indexes=1)

                progbar.finish()

        # 3. Invert the product of (2) and find the inwards proximity
        inverse_mask(tmp_buff_out.filepath, inv_mask.filepath)
        proximity_raster(inv_mask.filepath, tmp_prox_in.filepath, preserve_nodata=False)

        # 4. Now do the final logical and to shrink back a pixel
        with rasterio.open(tmp_prox_in.filepath) as prox_in_src:
            # Note: we reuse outmeta from before

            with rasterio.open(out_raster_path, 'w', **out_meta) as out_data_src:
                progbar = ProgressBar(len(list(out_data.block_windows(1))), 50, "Shrinking the raster by {} pixels".format(buffer_pixels))
                counter = 0
                # Again, these rasters should be orthogonal so their windows should also line up
                for _ji, window in out_data.block_windows(1):
                    progbar.update(counter)
                    counter += 1
                    prox_in_block = prox_in_src.read(1, window=window)

                    new_data = np.full(prox_in_block.shape, 1)
                    new_mask = np.logical_not(prox_in_block > buffer_pixels)

                    output = np.ma.masked_array(new_data, new_mask)
                    out_data_src.write(output.astype(out_meta['dtype']), window=window, indexes=1)

                progbar.finish()

        log.info('Cleaning finished')


def rasterize_attribute(in_lyr_path: Path, out_raster_path: Path, template_path: Path, attribute_field: str):
    """Rasterize an vector by attribue field

    Args:
        in_lyr_path (Path): vector file path
        out_raster_ (Path): output raster path
        template_path (Path): template raster path
        attribute_field(str): attribute field to rasterize
    """
    log = Logger('VBETRasterize')
    ds_path, lyr_path = VectorBase.path_sorter(in_lyr_path)

    progbar = ProgressBar(100, 50, "Rasterizing ")

    # with rasterio.open(template_path) as raster:
    #     t = raster.transform
    #     raster_bounds = raster.bounds
    #     ncol = raster.width
    #     nrow = raster.height

    raster_template = gdal.Open(template_path)
    # Fetch number of rows and columns
    ncol = raster_template.RasterXSize
    nrow = raster_template.RasterYSize
    # Fetch projection and extent
    proj = raster_template.GetProjectionRef()
    ext = raster_template.GetGeoTransform()
    raster_template = None

    # def poly_progress(progress, _msg, _data):
    #     progbar.update(int(progress * 100))

    # Rasterize the features (roads, rail etc) and calculate a raster of Euclidean distance from these features
    progbar.update(0)

    src_driver = ogr.GetDriverByName('GPKG')
    src_ds = src_driver.Open(ds_path)
    src_lyr = src_ds.GetLayer(lyr_path)

    # Rasterize the polygon to a temporary file
    with TempRaster('vbet_rasterize_attribute') as tempfile:
        log.debug('Temporary file: {}'.format(tempfile.filepath))

        driver = gdal.GetDriverByName('GTiff')
        out_raster = driver.Create(tempfile.filepath, ncol, nrow, 1, gdal.GDT_Int16)
        out_raster.SetProjection(proj)
        out_raster.SetGeoTransform(ext)
        options = ['ALL_TOUCHED=TRUE', f'ATTRIBUTE={attribute_field}']
        gdal.RasterizeLayer(out_raster, [1], src_lyr, options=options)

        # gdal.Rasterize(
        #     tempfile.filepath,
        #     ds_path,
        #     layers=[lyr_path],
        #     xRes=t[0], yRes=t[4],
        #     attribute=attribute_field, outputType=gdal.GDT_Int32,
        #     creationOptions=['COMPRESS=LZW'],
        #     # outputBounds --- assigned output bounds: [minx, miny, maxx, maxy]
        #     outputBounds=[raster_bounds.left, raster_bounds.bottom, raster_bounds.right, raster_bounds.top],
        #     callback=poly_progress
        # )
        out_raster = None
        progbar.finish()

        # Now mask the output correctly
        mask_rasters_nodata(tempfile.filepath, template_path, out_raster_path)


def raster2array(rasterfn: Path) -> np.array:
    """Open raster as np array"""
    log = Logger('raster2array')
    raster = gdal.Open(rasterfn)
    band = raster.GetRasterBand(1)
    array = band.ReadAsArray()
    if array.nbytes > 1e+8 * 200:
        nMBytes = array.nbytes / 1e+8 * 200
        log.warning(f'Reading large array: {nMBytes:,} Mb')
    return array


def create_empty_raster(raster_path: Path, template_raster: Path):
    """Create a raster with a shape identical to a template raster and fill it with nodata values

    Args:
        raster_path (Path): _description_
        template_raster (Path): _description_
    """
    log = Logger('create_empty_raster')
    log.debug('Creating empty raster')

    with rasterio.open(template_raster) as rio_template:
        out_meta = rio_template.meta
        out_meta['driver'] = 'GTiff'
        out_meta['count'] = 1
        out_meta['compress'] = 'deflate'

    use_big_tiff = os.path.getsize(template_raster) > 3800000000
    if use_big_tiff:
        out_meta['BIGTIFF'] = 'YES'

    with rasterio.open(raster_path, 'w', **out_meta) as rio_dest:
        for _ji, window in rio_dest.block_windows(1):
            # Writing a perfectly masked array should give us a raster where everything is nodata
            # numpy.full(shape, fill_value, dtype=None, order='C', *, like=None)
            out_arr = np.ma.array(np.full((window.height, window.width), 1), mask=np.full((window.width, window.height), True, bool))
            rio_dest.write(out_arr, window=window, indexes=1)
    return


def array2raster(newRasterfn: Path, rasterfn: Path, array: np.array, data_type: int = gdal.GDT_Byte, no_data=None):
    """write array to new raster file"""

    raster = gdal.Open(rasterfn)
    geotransform = raster.GetGeoTransform()
    originX = geotransform[0]
    originY = geotransform[3]
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    cols = array.shape[1]
    rows = array.shape[0]

    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(newRasterfn, cols, rows, 1, data_type, options=["COMPRESS=DEFLATE"])
    outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))
    outband = outRaster.GetRasterBand(1)
    outband.WriteArray(array)
    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromWkt(raster.GetProjectionRef())
    outRaster.SetProjection(outRasterSRS.ExportToWkt())
    if no_data:
        outband.SetNoDataValue(no_data)
    outband.FlushCache()


def new_raster(newRasterfn: Path, rasterfn: Path, data_type: int = gdal.GDT_Byte):
    """Create a new, empty raster from the template of another raster

    Args:
        newRasterfn (Path): _description_
        rasterfn (Path): _description_
        data_type (int, optional): _description_. Defaults to gdal.GDT_Byte.

    Returns:
        _type_: _description_
    """

    raster = gdal.Open(rasterfn)
    geotransform = raster.GetGeoTransform()
    originX = geotransform[0]
    originY = geotransform[3]
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    cols = raster.RasterXSize
    rows = raster.RasterYSize

    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(newRasterfn, cols, rows, 1, data_type, options=["COMPRESS=DEFLATE"])
    outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))
    outband = outRaster.GetRasterBand(1)

    return outRaster, outband


def raster_merge(in_raster: Path, out_raster: Path, template_raster: Path, logic_raster: Path):
    """insert pixels from one raster to new or existing raster based on binary raster

    Args:
        in_raster (Path): input raster with potential values to include
        out_raster (Path): new or existing raster
        template_raster (Path): existing template raster
        logic_raster (Path): binary raster where true results in value of input raster written to output raster
        temp_folder (Path): temporary folder for processing
    """
    log = Logger('raster_merge')

    if not os.path.isfile(out_raster):
        create_empty_raster(out_raster, template_raster)

    with rasterio.open(out_raster, 'r+') as rio_dest, \
            rasterio.open(in_raster) as rio_source, \
            rasterio.open(logic_raster) as rio_logic:

        # GT(0) x-coordinate of the upper-left corner of the upper-left pixel.
        # GT(1) w-e pixel resolution / pixel width.
        # GT(2) row rotation (typically zero).
        # GT(3) y-coordinate of the upper-left corner of the upper-left pixel.
        # GT(4) column rotation (typically zero).
        # GT(5) n-s pixel resolution / pixel height (negative value for a north-up image).
        in_transform = rio_source.get_transform()
        out_transform = rio_dest.get_transform()
        col_off_delta = round((in_transform[0] - out_transform[0]) / out_transform[1])
        row_off_delta = round((in_transform[3] - out_transform[3]) / out_transform[5])

        window_error = False
        for _ji, window in rio_source.block_windows(1):
            out_window = Window(window.col_off + col_off_delta, window.row_off + row_off_delta, window.width, window.height)
            array_dest = rio_dest.read(1, window=out_window, masked=True)
            array_logic_mask = rio_logic.read(1, window=window)
            array_source = rio_source.read(1, window=window, masked=True)

            if array_source.shape != array_dest.shape:
                window_error = True
                continue
            # make sure if the destination has a value already we don't touch it
            array_logic_mask[np.logical_not(array_dest.mask)] = 0

            array_out = np.ma.choose(array_logic_mask, [array_dest, array_source])

            rio_dest.write(array_out, window=out_window, indexes=1)

    if window_error:
        log.error(f'Different window shapes encounterd when processing {out_raster}')


def raster_update(raster, update_values_raster):
    """Update a bigger raster in-place with the contents of a smaller one

    Args:
        raster (_type_): _description_
        update_values_raster (_type_): _description_
    """
    _tmr = Timer()
    log = Logger('raster_update')
    with rasterio.open(raster, 'r+') as rio_dest, \
            rasterio.open(update_values_raster) as rio_updates:
        out_meta = rio_dest.meta
        out_meta['driver'] = 'GTiff'
        out_meta['count'] = 1
        out_meta['compress'] = 'deflate'

        # GT(0) x-coordinate of the upper-left corner of the upper-left pixel.
        # GT(1) w-e pixel resolution / pixel width.
        # GT(2) row rotation (typically zero).
        # GT(3) y-coordinate of the upper-left corner of the upper-left pixel.
        # GT(4) column rotation (typically zero).
        # GT(5) n-s pixel resolution / pixel height (negative value for a north-up image).
        in_transform = rio_updates.get_transform()
        out_transform = rio_dest.get_transform()
        col_off_delta = round((in_transform[0] - out_transform[0]) / out_transform[1])
        row_off_delta = round((in_transform[3] - out_transform[3]) / out_transform[5])

        for _ji, window in rio_updates.block_windows(1):
            out_window = Window(window.col_off + col_off_delta, window.row_off + row_off_delta, window.width, window.height)
            array_dest = rio_dest.read(1, window=out_window, masked=True)
            array_update = rio_updates.read(1, window=window, masked=True)

            # we're choosing from two values in an array. 0 = array_dest 1 = array_update
            chooser = array_dest.mask.astype(int)
            # Make sure that any nodata values in the array_update default back to array_dest
            chooser[array_update.mask] = 0

            array_out = np.choose(chooser, [array_dest, array_update])
            rio_dest.write(array_out, window=out_window, indexes=1)
    log.debug(f'Timer: {_tmr.ellapsed()}')


def raster_update_multiply(raster, update_values_raster, value=None):
    """_summary_

    Args:
        raster (_type_): _description_
        update_values_raster (_type_): _description_
        value (_type_, optional): _description_. Defaults to None.
    """
    with rasterio.open(raster, 'r+') as rio_dest, \
            rasterio.open(update_values_raster) as rio_updates:

        out_meta = rio_dest.meta
        out_meta['driver'] = 'GTiff'
        out_meta['count'] = 1
        out_meta['compress'] = 'deflate'

        # GT(0) x-coordinate of the upper-left corner of the upper-left pixel.
        # GT(1) w-e pixel resolution / pixel width.
        # GT(2) row rotation (typically zero).
        # GT(3) y-coordinate of the upper-left corner of the upper-left pixel.
        # GT(4) column rotation (typically zero).
        # GT(5) n-s pixel resolution / pixel height (negative value for a north-up image).
        in_transform = rio_updates.get_transform()
        out_transform = rio_dest.get_transform()
        col_off_delta = round((in_transform[0] - out_transform[0]) / out_transform[1])
        row_off_delta = round((in_transform[3] - out_transform[3]) / out_transform[5])

        for _ji, window in rio_updates.block_windows(1):
            out_window = Window(window.col_off + col_off_delta, window.row_off + row_off_delta, window.width, window.height)

            array_dest = rio_dest.read(1, window=out_window, masked=True)
            array_update = rio_updates.read(1, window=window, masked=True)

            if value is not None:
                array_update = np.multiply(array_update, value)

            # we're choosing from two values in an array. 0 = array_dest 1 = array_update
            chooser = array_dest.mask.astype(int)
            # Make sure that any nodata values in the array_update default back to array_dest
            chooser[array_update.mask | array_update == 0] = 0

            array_out = np.choose(chooser, [array_dest, array_update])
            array_out_format = array_out if out_meta['dtype'] == 'int32' else np.float32(array_out)
            rio_dest.write(array_out_format, window=out_window, indexes=1)
    return


def raster_remove_zone(raster, remove_raster, output_raster):
    with rasterio.open(raster, 'r') as rio_dest, \
            rasterio.open(remove_raster) as rio_remove:

        out_meta = rio_dest.meta
        out_meta['driver'] = 'GTiff'
        out_meta['count'] = 1
        out_meta['compress'] = 'deflate'

        with rasterio.open(output_raster, 'w', **out_meta) as rio_output:
            for _ji, window in rio_dest.block_windows(1):
                array_logic_mask = np.array(rio_remove.read(1, window=window) > 0).astype('int')  # mask of existing data in destination raster
                array_multiply = np.equal(array_logic_mask, 0).astype('int')
                array_dest = rio_dest.read(1, window=window, masked=True)
                array_out = np.multiply(array_multiply, array_dest)
                rio_output.write(array_out, window=window, indexes=1)
    return
