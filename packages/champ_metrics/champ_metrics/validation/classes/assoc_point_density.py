from .raster import CHaMP_Raster
from .validation_classes import ValidationResult


class CHaMP_Associated_PointDensity(CHaMP_Raster):

    minCellValue = 0
    rangeCellValue = 10

    def __init__(self, name, filepath):
        CHaMP_Raster.__init__(self, name, filepath)
        self.required = False

    def validate(self):
        results = super(CHaMP_Associated_PointDensity, self).validate()
        validate_mincellvalue = ValidationResult(self.__class__.__name__, "MinCellValue")
        validate_rangecellvalue = ValidationResult(self.__class__.__name__, "RangeCellValues")
        if self.exists():
            gRaster = self.get_raster()
            if gRaster.min >= self.minCellValue:
                validate_mincellvalue.pass_validation()
            else:
                validate_mincellvalue.error("Minimum cell value (" + str(gRaster.min) + \
                                            ") is less than required value (" + str(self.minCellValue) + ")")
            if (gRaster.max - gRaster.min) < self.rangeCellValue:
                validate_rangecellvalue.pass_validation()
            else:
                validate_rangecellvalue.warning("Range of cell values (" + str(gRaster.max - gRaster.min) + \
                                            ") is greater than the allowed value (" + str(self.rangeCellValue) + ")")
        results.append(validate_mincellvalue.get_dict())
        results.append(validate_rangecellvalue.get_dict())
        return results