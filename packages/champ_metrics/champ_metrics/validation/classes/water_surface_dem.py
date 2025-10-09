from .raster import CHaMP_Raster
from .validation_classes import ValidationResult


class CHaMP_WaterSurfaceDEM(CHaMP_Raster):

    minCellValue = 0
    #rangeCellValue = 200

    def __init__(self, name, filepath):
        CHaMP_Raster.__init__(self, name, filepath)

    def validate(self):

        results = super(CHaMP_WaterSurfaceDEM, self).validate()
        validate_mincellvalue = ValidationResult(self.__class__.__name__, "MinCellValue")

        if self.exists():
            gRaster = self.get_raster()

            if gRaster.min >= self.minCellValue:
                validate_mincellvalue.pass_validation()
            else:
                validate_mincellvalue.error("Minimum cell value (" + str(gRaster.min) + \
                                            ") is less than required value (" + str(self.minCellValue) + ")")

        results.append(validate_mincellvalue.get_dict())

        return results