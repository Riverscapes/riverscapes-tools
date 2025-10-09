from .dataset import GIS_Dataset
from champmetrics.lib.raster import Raster
from validation_classes import ValidationResult

import logging
logging.getLogger("rasterio").setLevel(logging.ERROR)

class CHaMP_Raster(GIS_Dataset):

    targetCellsize = 0.1  # Meters
    maxRasterHeight = 1000  # Meters
    maxRasterWidth = 1000  # Meters

    def __init__(self, name, filepath):
        GIS_Dataset.__init__(self, name, filepath)

        self.surveyDEM_Polygon = None

        if self.exists():
            self.spatial_reference_wkt = self.get_crs_wkt()

    def get_raster(self):

        return Raster(self.filename)

    def get_crs_wkt(self):
        import rasterio
        with rasterio.open(self.filename) as src:
            crs = src.crs
        return crs.wkt

    def validate(self):

        results = super(CHaMP_Raster, self).validate()

        validate_cellsize = ValidationResult(self.__class__.__name__, "TargetCellSize")
        validate_rasterheight = ValidationResult(self.__class__.__name__, "MaxRasterHeight")
        validate_rasterwidth = ValidationResult(self.__class__.__name__, "MaxRasterWidth")
        validate_wholemeterextents = ValidationResult(self.__class__.__name__, "WholeMeterExtents")
        validate_concurrent_dem = ValidationResult(self.__class__.__name__, "ConcurrentWithDEM")

        if self.exists():
            gRaster = self.get_raster()

            if (abs(gRaster.cellHeight) == self.targetCellsize) and (abs(gRaster.cellWidth) == self.targetCellsize):
                validate_cellsize.pass_validation()
            else:
                validate_cellsize.error("Cell size (" + str(gRaster.cellHeight) + "," + str(gRaster.cellWidth) + \
                                            " not required size of " + str(self.targetCellsize))

            if gRaster.getHeight() < self.maxRasterHeight:
                validate_rasterheight.pass_validation()
            else:
                validate_rasterheight.error("Raster height " + str(gRaster.getHeight()) + " exceeds maximum of " + \
                                        str(self.maxRasterHeight))

            if gRaster.getWidth() < self.maxRasterWidth:
                validate_rasterwidth.pass_validation()
            else:
                validate_rasterwidth.error("Raster width " + str(gRaster.getWidth()) + " exceeds maximum of " + \
                                        str(self.maxRasterWidth))

            if float(gRaster.getHeight()).is_integer() and float(gRaster.getWidth()).is_integer():
                validate_wholemeterextents.pass_validation()
            else:
                validate_wholemeterextents.error("Raster extents not whole meters.")

            if self.surveyDEM_Polygon:
                if self.surveyDEM_Polygon.equals(self.get_raster().getBoundaryShape()):
                    validate_concurrent_dem.pass_validation()
                else:
                    validate_concurrent_dem.warning("Raster extent does not appear to be concurrent with DEM.")

        results.append(validate_concurrent_dem.get_dict())
        results.append(validate_wholemeterextents.get_dict())
        results.append(validate_rasterwidth.get_dict())
        results.append(validate_rasterheight.get_dict())
        results.append(validate_cellsize.get_dict())

        return results