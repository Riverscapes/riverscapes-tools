from osgeo import ogr
from osgeo import gdal
import osgeo

CREATED = False


class _GdalErrorHandler(object):
    """Implement GDAL Error handling in Python

    Arguments:
        object {[type]} -- [description]
    """

    def __init__(self):
        self.err_level = gdal.CE_Failure
        self.err_no = 0
        self.err_msg = ''

    def handler(self, err_level, err_no, err_msg):
        self.err_level = err_level
        self.err_no = err_no
        self.err_msg = err_msg


def initGDALOGRErrors():
    """
    Call this function to make sure GDAL and OGR Exceptions get caught and handled properly
    """
    global CREATED
    if not CREATED:
        print("Initializing GDAL Exceptions")
        err = _GdalErrorHandler()
        handler = err.handler  # Note don't pass class method directly or python segfaults
        # due to a reference counting bug
        # http://trac.osgeo.org/gdal/ticket/5186#comment:4

        gdal.PushErrorHandler(handler)
        gdal.UseExceptions()  # Exceptions will get raised on anything >= gdal.CE_Failure
        ogr.UseExceptions()
        CREATED = True


if __name__ == '__main__':

    initGDALOGRErrors()

    try:
        gdal.Error(gdal.CE_Warning, 1, 'Test warning message')
    except Exception as e:
        print('Operation raised an exception')
        raise e
    # else:
    #     if err.err_level >= gdal.CE_Warning:
    #         print('Operation raised an warning')
    #         raise RuntimeError(err.err_level, err.err_no, err.err_msg)
    finally:
        gdal.PopErrorHandler()
