from os import path
import copy
from affine import Affine
import numpy as np
from osgeo import gdal, ogr, osr
from shapely.geometry import Polygon
from rscommons import Logger
from .exception import DataException, MissingException

# this allows GDAL to throw Python Exceptions
gdal.UseExceptions()


class Raster:

    def __init__(self, sfilename):
        self.filename = sfilename
        self.log = Logger("Raster")
        self.errs = ""
        try:
            if (path.isfile(self.filename)):
                src_ds = gdal.Open(self.filename)
            else:
                self.log.error(f"Missing file: {self.filename}")
                raise MissingException(f"Could not find raster file: {path.basename(self.filename)}")
        except RuntimeError as e:
            raise DataException(f"Raster file exists but has problems: {path.basename(self.filename)}") from e
        try:
            # Read Raster Properties
            srcband = src_ds.GetRasterBand(1)
            self.bands = src_ds.RasterCount
            self.driver = src_ds.GetDriver().LongName
            self.gt = src_ds.GetGeoTransform()
            self.nodata = srcband.GetNoDataValue()
            """ Turn a Raster with a single band into a 2D [x,y] = v array """
            self.array = srcband.ReadAsArray()

            # Now mask out any NAN or nodata values (we do both for consistency)
            if self.nodata is not None:
                # To get over the issue where self.nodata may be imprecisely set we may need to use the array's
                # true nodata, taken directly from the array
                workingNodata = self.nodata
                self.min = np.nanmin(self.array)
                if isclose(self.min, self.nodata, rel_tol=1e-03):
                    workingNodata = self.min
                self.array = np.ma.array(self.array, mask=(np.isnan(self.array) | (self.array == workingNodata)))

            self.dataType = srcband.DataType
            self.min = np.nanmin(self.array)
            self.max = np.nanmax(self.array)
            self.proj = src_ds.GetProjection()

            # Remember:
            # [0]/* top left x */
            # [1]/* w-e pixel resolution */
            # [2]/* rotation, 0 if image is "north up" */
            # [3]/* top left y */
            # [4]/* rotation, 0 if image is "north up" */
            # [5]/* n-s pixel resolution */
            self.left = self.gt[0]
            self.cellWidth = self.gt[1]
            self.top = self.gt[3]
            self.cellHeight = self.gt[5]
            self.cols = src_ds.RasterXSize
            self.rows = src_ds.RasterYSize
            # Important to throw away the srcband
            srcband.FlushCache()
            srcband = None

        except RuntimeError as e:
            print('Could not retrieve meta Data for %s' % self.filename, e)
            raise e

    def getBottom(self):
        return self.top + (self.cellHeight * self.rows)

    def getRight(self):
        return self.left + (self.cellWidth * self.cols)

    def getWidth(self):
        return self.getRight() - self.left

    def getHeight(self):
        return self.top - self.getBottom()

    def getBoundaryShape(self):
        return Polygon([
            (self.left, self.top),
            (self.getRight(), self.top),
            (self.getRight(), self.getBottom()),
            (self.left, self.getBottom()),
        ])

    def boundsContains(self, bounds, pt):
        return (bounds[0] < pt.coords[0][0]
                and bounds[1] < pt.coords[0][1]
                and bounds[2] > pt.coords[0][0]
                and bounds[3] > pt.coords[0][1])

    def rasterMaskLayer(self, shapefile, fieldname=None):
        """
        return a masked array that corresponds to the input polygon
        :param polygon:
        :return:
        """
        # Create a memory raster to rasterize into.
        target_ds = gdal.GetDriverByName('MEM').Create('', self.cols, self.rows, 1, gdal.GDT_Byte)
        target_ds.SetGeoTransform(self.gt)

        assert len(shapefile) > 0, "The ShapeFile path is empty"

        # Create a memory layer to rasterize from.
        driver = ogr.GetDriverByName("ESRI Shapefile")
        src_ds = driver.Open(shapefile, 0)
        src_lyr = src_ds.GetLayer()

        # Run the algorithm.
        options = ['ALL_TOUCHED=TRUE']
        if fieldname and len(fieldname) > 0:
            options.append('ATTRIBUTE=' + fieldname)

        err = gdal.RasterizeLayer(target_ds, [1], src_lyr, options=options)

        # Get the array:
        band = target_ds.GetRasterBand(1)
        return band.ReadAsArray()

    def getPixelVal(self, pt):
        # Convert from map to pixel coordinates.
        # Only works for geotransforms with no rotation.
        px = int((pt[0] - self.left) / self.cellWidth)  # x pixel
        py = int((pt[1] - self.top) / self.cellHeight)  # y pixel
        val = self.array[py, px]
        if isclose(val, self.nodata, rel_tol=1e-07) or val is np.ma.masked:
            return np.nan

        return val

    def lookupRasterValues(self, points):
        """
        Given an array of points with real-world coordinates, lookup values in raster
        then mask out any nan/nodata values
        :param points:
        :param raster:
        :return:
        """
        pointsdict = {"points": points, "values": []}

        for pt in pointsdict['points']:
            pointsdict['values'].append(self.getPixelVal(pt.coords[0]))

        # Mask out the np.nan values
        pointsdict['values'] = np.ma.masked_invalid(pointsdict['values'])

        return pointsdict

    def write(self, outputRaster):
        """
        Write this raster object to a file. The Raster is closed after this so keep that in mind
        You won't be able to access the raster data after you run this.
        :param outputRaster:
        :return:
        """
        if path.isfile(outputRaster):
            deleteRaster(outputRaster)

        driver = gdal.GetDriverByName('GTiff')
        outRaster = driver.Create(outputRaster, self.cols, self.rows, 1, self.dataType, ['COMPRESS=LZW'])

        # Remember:
        # [0]/* top left x */
        # [1]/* w-e pixel resolution */
        # [2]/* rotation, 0 if image is "north up" */
        # [3]/* top left y */
        # [4]/* rotation, 0 if image is "north up" */
        # [5]/* n-s pixel resolution */
        outRaster.SetGeoTransform([self.left, self.cellWidth, 0, self.top, 0, self.cellHeight])
        outband = outRaster.GetRasterBand(1)

        # Set nans to the original No Data Value
        outband.SetNoDataValue(self.nodata)
        self.array.data[np.isnan(self.array)] = self.nodata
        # Any mask that gets passed in here should have masked out elements set to
        # Nodata Value
        if isinstance(self.array, np.ma.MaskedArray):
            np.ma.set_fill_value(self.array, self.nodata)
            outband.WriteArray(self.array.filled())
        else:
            outband.WriteArray(self.array)

        spatialRef = osr.SpatialReference()
        spatialRef.ImportFromWkt(self.proj)

        outRaster.SetProjection(spatialRef.ExportToWkt())
        outband.FlushCache()
        # Important to throw away the srcband
        outband = None
        self.log.debug("Finished Writing Raster: {0}".format(outputRaster))

    def setArray(self, incomingArray, copy=False):
        """
        You can use the self.array directly but if you want to copy from one array
        into a raster we suggest you do it this way
        :param incomingArray:
        :return:
        """
        masked = isinstance(self.array, np.ma.MaskedArray)
        if copy:
            if masked:
                self.array = np.ma.copy(incomingArray)
            else:
                self.array = np.ma.masked_invalid(incomingArray, copy=True)
        else:
            if masked:
                self.array = incomingArray
            else:
                self.array = np.ma.masked_invalid(incomingArray)

        self.rows = self.array.shape[0]
        self.cols = self.array.shape[1]
        self.min = np.nanmin(self.array)
        self.max = np.nanmax(self.array)


def isclose(a, b, rel_tol=1e-09, abs_tol=0):
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def PrintArr(arr):
    """
    Print an ASCII representation of the array with an up-down flip if the
    the cell height is negative.

    Int this scenario:
        - '-' means masked
        - '_' means nodata
        - '#' means a number
        - '0' means 0
    :param arr:
    """
    print("\n")
    for row in range(arr.shape[0]):
        rowStr = ""
        for col in range(arr[row].shape[0]):
            rowStr += str(arr[row][col])
        print("{0}:: {1}".format(row, rowStr))
    print("\n")


def get_data_polygon(rasterfile):

    import rasterio
    from rasterio.features import shapes
    from shapely.geometry import shape

    r = Raster(rasterfile)
    array = np.array(r.array.mask * 1, dtype=np.int16)

#    with rasterio.drivers():
    with rasterio.open(rasterfile) as src:
        image = src.read(1)
        mask_array = image != src.nodata

        # https://github.com/mapbox/rasterio/issues/86
        if isinstance(src.transform, Affine):
            transform = src.transform
        else:
            transform = src.affine  # for compatibility with rasterio 0.36

        results = ({'properties': {'raster_val': v}, 'geometry': s}
                   for i, (s, v) in enumerate(shapes(array, mask=mask_array, transform=transform)))

    geoms = list(results)
    polygons = [shape(geom['geometry']) for geom in geoms]

    return polygons


def rasterCopy(rObj):
    return copy.copy(rObj)


def deleteRaster(sFullPath):
    """

    :param path:
    :return:
    """

    log = Logger("Delete Raster")

    if path.isfile(sFullPath):
        try:
            # Delete the raster properly
            driver = gdal.GetDriverByName('GTiff')
            gdal.Driver.Delete(driver, sFullPath)
            log.debug("Raster Successfully Deleted: {0}".format(sFullPath))
        except Exception as e:
            log.error("Failed to remove existing clipped raster at {0}".format(sFullPath))
            raise e
    else:
        log.debug("No raster file to delete at {0}".format(sFullPath))
