from __future__ import annotations
import os
from tempfile import mkstemp
from rscommons import Logger


class TempGISFileException(Exception):
    """Special exceptions

    Args:
        Exception ([type]): [description]
    """
    pass


class TempGISFile():
    """This is just a loose mapping class to allow us to use Python's 'with' keyword.

    Raises:
        VectorBaseException: Various
    """
    log = Logger('TempGISFile')

    def __init__(self, suffix: str, prefix: str = None):
        self.suffix = suffix
        self.prefix = 'rstools_{}'.format(prefix)
        self.filepath = None
        self.file = None

    def __enter__(self) -> TempGISFile:
        """Behaviour on open when using the "with VectorBase():" Syntax
        """
        self.file, self.filepath = mkstemp(suffix=self.suffix, text=True)
        return self

    def __exit__(self, _type, _value, _traceback):
        """Behaviour on close when using the "with VectorBase():" Syntax
        """
        try:
            os.close(self.file)
            os.remove(self.filepath)
        except Exception as e:
            self.log.warning('Error cleaning up file: {}'.format(self.filepath))


class TempRaster(TempGISFile):
    def __init__(self, prefix: str):
        super(TempRaster, self).__init__(suffix='.tiff', prefix=prefix)


class TempGeopackage(TempGISFile):
    def __init__(self, prefix: str):
        super(TempGeopackage, self).__init__(suffix='.gpkg', prefix=prefix)
