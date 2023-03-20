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

import numpy as np
from scipy.ndimage import label, generate_binary_structure, binary_closing
from scipy import ndimage

from rscommons import ProgressBar, Logger, VectorBase, Timer, TempRaster
from rscommons.classes.raster import deleteRaster

Path = str


def get_raster_meta(template_raster: str):
    """Extract the Rasterio meta we need to write a raster from a template raster

    This is a pretty common pattern when we want to match an output to an input
    We do explicitly set both the compression and the BIGTIFF property

    Args:
        template_raster (str): _description_

    Returns:
        _type_: _description_
    """
    with rasterio.open(template_raster) as rio_template:
        out_meta = rio_template.meta
        out_meta['driver'] = 'GTiff'
        out_meta['count'] = 1
        out_meta['compress'] = 'deflate'

    use_big_tiff = os.path.getsize(template_raster) > 3800000000
    if use_big_tiff:
        out_meta['BIGTIFF'] = 'YES'
    return out_meta


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
                deleteRaster(temp_path)

            temp_path = dist_file

        # Preserve the nodata from the source
        if preserve_nodata:
            mask_rasters_nodata(temp_path, src_raster_path, out_raster_path)
        else:
            shutil.copyfile(temp_path, out_raster_path)

        if os.path.exists(temp_path):
            deleteRaster(temp_path)

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


def create_empty_raster(raster_path: Path, template_raster: Path, compress: bool = True):
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

    if compress is True:
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
        newRasterfn (Path): new raster file name
        rasterfn (Path): template raster file name
        data_type (int, optional): gdal data type. Defaults to gdal.GDT_Byte.

    Returns:
        _type_: _description_
    """

    raster = gdal.Open(rasterfn)
    geotransform = raster.GetGeoTransform()
    origin_x = geotransform[0]
    origin_y = geotransform[3]
    pixel_width = geotransform[1]
    pixel_height = geotransform[5]
    cols = raster.RasterXSize
    rows = raster.RasterYSize

    driver = gdal.GetDriverByName('GTiff')
    out_raster = driver.Create(newRasterfn, cols, rows, 1, data_type, options=["COMPRESS=DEFLATE"])
    out_raster.SetGeoTransform((origin_x, pixel_width, 0, origin_y, 0, pixel_height))
    outband = out_raster.GetRasterBand(1)

    return out_raster, outband


def raster_logic_mask(in_raster: Path, out_raster: Path, logic_raster: Path):
    """insert pixels from one raster to new or existing raster based on binary raster

    Args:
        in_raster (Path): input raster with potential values to include
        out_raster (Path): output raster
        logic_raster (Path): binary raster where true results in value of input raster written to output raster
    """
    log = Logger('raster_merge')

    if os.path.exists(out_raster):
        log.debug('Output raster already exists. removing')
        deleteRaster(out_raster)

    # This will add compresssion parameters and make sure our output matches our input raster metadata
    meta = get_raster_meta(in_raster)

    with rasterio.open(out_raster, 'w', **meta) as rio_dest, \
            rasterio.open(in_raster, 'r') as rio_source, \
            rasterio.open(logic_raster, 'r') as rio_logic:

        for _ji, window in rio_source.block_windows(1):
            array_logic_mask = rio_logic.read(1, window=window)
            array_source = rio_source.read(1, window=window, masked=True)
            # Combine 0 values from the logic raster with the nodata values of the input raster
            new_mask = np.logical_or(np.logical_not(array_logic_mask), array_source.mask)

            array_out = np.ma.masked_array(array_source, mask=new_mask)
            rio_dest.write(array_out, window=window, indexes=1)

    return


def raster_recompress(raster_path: Path):
    """Working on a windowed raster can cause compression to fail

    Args:
        raster_path (Path): raster to compress
    """
    log = Logger('raster_recompress')
    _tmr = Timer()
    meta = get_raster_meta(raster_path)

    with TempRaster(prefix='raster_recompress') as tempfile:
        shutil.copy(raster_path, tempfile.filepath)
        with rasterio.open(tempfile.filepath, 'r') as src, rasterio.open(raster_path, 'w', **meta) as dst:
            for _ji, window in dst.block_windows(1):
                array_dest = src.read(1, window=window, masked=True)
                dst.write(array_dest, window=window, indexes=1)
    log.debug(f'raster_recompress: {_tmr.toString()}')


def raster_update_multiply(raster: Path, update_values_raster: Path, value=None):
    """use np.multiply to apply values from update_values_raster to raster

    Args:
        raster (Path): raster to update no data pixels on
        update_values_raster (Path): raster to update data pixels with
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


def raster_remove_zone(raster: Path, remove_raster: Path, output_raster: Path):
    """logic to remove raster areas

    Args:
        raster (Path): input raster
        remove_raster (Path): raster to remove areas
        output_raster (Path): output raster
    """
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


def get_endpoints_on_raster(raster: Path, geom_line: ogr.Geometry(), dist):
    """return a list of endpoints for a linestring or multilinestring

    Args:
        geom (ogr.Geometry): linestring or multilinestring geometry

    Returns:
        list: coords of points
    """

    line = VectorBase.ogr2shapely(geom_line)
    iterations = [dist, dist * 2, dist * 3]

    with rasterio.open(raster, 'r') as src:
        coords = []
        pnt_start = line.coords[0]
        for iteration in iterations:
            value = list(src.sample([(pnt_start[0], pnt_start[1])]))[0][0]
            if value is not None and value != 0.0:
                break
            pnt = line.interpolate(iteration)
            pnt_start = (pnt.x, pnt.y)

        coords.append(pnt_start)

        pnt_end = line.coords[-1]
        for iteration in iterations:
            value = list(src.sample([(pnt_end[0], pnt_end[1])]))[0][0]
            if value is not None and value != 0.0:
                break
            pnt = line.interpolate(-1 * iteration)
            pnt_end = (pnt.x, pnt.y)

        coords.append(pnt_end)

        return coords


def generate_vbet_polygon(vbet_evidence_raster: Path, rasterized_channel: Path, channel_hand: Path, out_valley_bottom: Path, temp_folder: Path, rasterized_flowline: Path = None, thresh_value: float = 0.68):
    """generate the vbet raster for a thresholded value

    Args:
        vbet_evidence_raster (Path): vbet evidience raster
        rasterized_channel (Path): raster of channel area (for filtering vbet regions)
        channel_hand (Path): hand for the local level path
        out_valley_bottom (Path): output thresholded vbet area raster
        temp_folder (Path): temporary folder for intermediate processing rasters
        thresh_value (float, optional): vbet threshold value from 0.0 to 1.0. Defaults to 0.68.
    """
    log = Logger('VBET Generate Polygon')
    _timer = Timer()
    # Mask to Hand area
    vbet_evidence_masked = os.path.join(temp_folder, f"vbet_evidence_masked_{thresh_value}.tif")
    mask_rasters_nodata(vbet_evidence_raster, channel_hand, vbet_evidence_masked)

    # Threshold Valley Bottom
    valley_bottom_raw = os.path.join(temp_folder, f"valley_bottom_raw_{thresh_value}.tif")
    threshold(vbet_evidence_masked, thresh_value, valley_bottom_raw)

    ds_valley_bottom = gdal.Open(valley_bottom_raw, gdal.GA_Update)
    band_valley_bottom = ds_valley_bottom.GetRasterBand(1)

    log.info('Sieve Filter vbet')
    # Sieve and Clean Raster
    gdal.SieveFilter(srcBand=band_valley_bottom, maskBand=None, dstBand=band_valley_bottom, threshold=10, connectedness=8, callback=gdal.TermProgress_nocb)
    band_valley_bottom.SetNoDataValue(0)
    band_valley_bottom.FlushCache()
    valley_bottom_sieved = band_valley_bottom.ReadAsArray()

    log.info('Generate regions')
    # Region Tool to find only connected areas
    struct = generate_binary_structure(2, 2)
    regions, _num = label(valley_bottom_sieved, structure=struct)
    valley_bottom_sieved = None

    chan = raster2array(rasterized_channel)
    selection = regions * chan
    chan = None

    values = np.unique(selection)
    non_zero_values = [v for v in values if v != 0]
    valley_bottom_region = np.isin(regions, non_zero_values)
    array2raster(os.path.join(temp_folder, f'regions_{thresh_value}.tif'), vbet_evidence_raster, regions, data_type=gdal.GDT_Int32)
    array2raster(os.path.join(temp_folder, f'valley_bottom_region_{thresh_value}.tif'), vbet_evidence_raster, valley_bottom_region.astype(int), data_type=gdal.GDT_Int32)

    # Clean Raster Edges
    log.info('Cleaning Raster edges')
    valley_bottom_clean = binary_closing(valley_bottom_region.astype(int), iterations=2)
    donuts = np.invert(valley_bottom_clean)
    donut_regions, num_donuts = label(donuts, struct)
    sizes = ndimage.sum(donuts, donut_regions, range(num_donuts + 1))
    donut_mask = sizes < 200
    final_valley_array = donut_mask[donut_regions]

    if rasterized_flowline:
        network_array = raster2array(rasterized_flowline)
        final_valley_array = np.maximum(final_valley_array, network_array)

    array2raster(out_valley_bottom, vbet_evidence_raster, final_valley_array, data_type=gdal.GDT_Int32)

    log.debug(f'Timer: {_timer.toString()}')


def generate_centerline_surface(vbet_raster: Path, out_cost_path: Path, temp_folder: Path):
    """generate the centerline cost path surface

    Args:
        vbet_raster (Path): binary int raster of vbet area
        out_cost_path (Path): output cost path raster
        temp_folder (Path): path of temp folder for intermediate rasters
    """

    log = Logger('Generate Centerline Surface')
    vbet = raster2array(vbet_raster)
    _timer = Timer()

    # Generate Inverse Raster for Proximity
    valley_bottom_inverse = (vbet != 1)
    inverse_mask_raster = os.path.join(temp_folder, 'inverse_mask.tif')
    array2raster(inverse_mask_raster, vbet_raster, valley_bottom_inverse)

    # Proximity Raster
    ds_valley_bottom_inverse = gdal.Open(inverse_mask_raster)
    band_valley_bottom_inverse = ds_valley_bottom_inverse.GetRasterBand(1)
    proximity_raster = os.path.join(temp_folder, 'proximity.tif')
    _ds_proximity, band_proximity = new_raster(proximity_raster, vbet_raster, data_type=gdal.GDT_Int32)
    gdal.ComputeProximity(band_valley_bottom_inverse, band_proximity, ['VALUES=1', "DISTUNITS=PIXEL", "COMPRESS=DEFLATE"])
    band_proximity.SetNoDataValue(0)
    band_proximity.FlushCache()
    proximity = band_proximity.ReadAsArray()

    # Rescale Raster
    rescaled = np.interp(proximity, (proximity.min(), proximity.max()), (0.0, 10.0))
    rescaled_raster = os.path.join(temp_folder, 'rescaled.tif')
    array2raster(rescaled_raster, vbet_raster, rescaled, data_type=gdal.GDT_Float32)

    # Centerline Cost Path
    cost_path = 10**((rescaled * -1) + 10) + (rescaled <= 0) * 1000000000000  # 10** (((A) * -1) + 10) + (A <= 0) * 1000000000000
    array2raster(out_cost_path, vbet_raster, cost_path, data_type=gdal.GDT_Float32)

    log.debug(f'Timer: {_timer.toString()}')


def threshold(evidence_raster_path: Path, thr_val: float, thresh_raster_path: Path):
    """Threshold a raster to greater than or equal to a threshold value

    Args:
        evidence_raster_path (Path): input evidience raster
        thr_val (float): value to threshold
        thresh_raster_path (Path): output threshold raster
    """
    log = Logger('threshold')
    _timer = Timer()
    with rasterio.open(evidence_raster_path) as fval_src:
        out_meta = fval_src.meta
        out_meta['count'] = 1
        out_meta['compress'] = 'deflate'
        out_meta['dtype'] = rasterio.uint8
        out_meta['nodata'] = 0

        log.info('Thresholding at {}'.format(thr_val))
        with rasterio.open(thresh_raster_path, "w", **out_meta) as dest:
            progbar = ProgressBar(len(list(fval_src.block_windows(1))), 50, "Thresholding at {}".format(thr_val))
            counter = 0
            for _ji, window in fval_src.block_windows(1):
                progbar.update(counter)
                counter += 1
                fval_data = fval_src.read(1, window=window, masked=True)
                # Fill an array with "1" values to give us a nice mask for polygonize
                fvals_mask = np.full(fval_data.shape, np.uint8(1))

                # Create a raster with 1.0 as a value everywhere in the same shape as fvals
                new_fval_mask = np.ma.mask_or(fval_data.mask, fval_data < thr_val)
                masked_arr = np.ma.array(fvals_mask, mask=[new_fval_mask])  # & ch_data.mask])
                dest.write(np.ma.filled(masked_arr, out_meta['nodata']), window=window, indexes=1)
            progbar.finish()
    log.debug(f'Timer: {_timer.toString()}')


def clean_raster_regions(raster: Path, target_value: int, out_raster: Path, out_regions: Path = None):
    """keep only the largest region of a raster by area

    Args:
        raster (Path): path of raster to clean
        target_value (int): value to keep
        out_raster (Path): output raster path 
        out_regions (Path, optional): path of output regions raster. Defaults to None.
    """
    log = Logger('Clean Raster Regions')

    array = raster2array(raster)
    if not np.any(array == target_value):
        log.info(f'Raster {raster} does not contain target value of {target_value}. No raster cleaning required.')
        return

    array[array != target_value] = 0  # all values not part of the target value
    array[array == target_value] = 1

    log.info('Generate regions')
    # Region Tool to find only connected areas
    struct = generate_binary_structure(2, 2)
    regions, _num_labels = label(array, structure=struct)
    array = None

    size = np.bincount(regions.ravel())
    biggest_label = size[1:].argmax() + 1
    regions[regions != biggest_label] = 0
    regions[regions == biggest_label] = 1

    array_non_target = raster2array(raster)
    array_non_target[array_non_target == target_value] = -9999
    out_array = np.choose(regions, [array_non_target, target_value])
    array_non_target = None

    array2raster(out_raster, raster, out_array, data_type=gdal.GDT_Int32, no_data=-9999)
    if out_regions:
        array2raster(out_regions, raster, regions, data_type=gdal.GDT_Int32)
