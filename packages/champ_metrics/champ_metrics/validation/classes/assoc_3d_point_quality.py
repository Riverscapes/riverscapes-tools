from .raster import CHaMP_Raster
# from .validation_classes import *


class CHaMP_Associated_3DPointQuality(CHaMP_Raster):

    minCellValue = 0
    rangeCellValue = 200

    def __init__(self, name, filepath):
        CHaMP_Raster.__init__(self, name, filepath)
        self.required = False

    def validate(self):
        results = super(CHaMP_Associated_3DPointQuality, self).validate()
        return results