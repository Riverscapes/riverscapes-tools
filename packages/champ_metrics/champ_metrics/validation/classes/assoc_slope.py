from .raster import CHaMP_Raster
# from validation_classes import *


class CHaMP_Associated_Slope(CHaMP_Raster):

    # minCellValue = 0
    # rangeCellValue = 200

    def __init__(self, name, filepath):
        CHaMP_Raster.__init__(self, name, filepath)
        self.required = False

    def validate(self):
        results = super(CHaMP_Associated_Slope, self).validate()
        if self.exists():
            gRaster = self.get_raster()
            #results["DEM minCellValue"] = gRaster.min >= self.minCellValue
            #results["DEM rangeCellValue"] = (gRaster.max - gRaster.min) < self.rangeCellValue
        return results