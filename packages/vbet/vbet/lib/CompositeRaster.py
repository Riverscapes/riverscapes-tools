""" VBET compiles lots of little rasters into a big one.

To do this efficiently we use VRTs 

That is just tedious enough that it deservers its own class
"""
import os
from typing import List
from osgeo import gdal
import rasterio
from rscommons.util import safe_makedirs
from rscommons import Logger, Timer, ProgressBar
from vbet.vbet_raster_ops import get_raster_meta


class CompositeRaster(object):
    """_summary_

    Args:
        _description_
    """

    def __init__(self, out_path: str, raster_paths: List[str], clean_inputs: bool = True):
        """_summary_

        Args:
            out_path (str): _description_
            raster_paths (List[str]): _description_
        """
        self.log = Logger('CompositeRaster')
        self.raster_paths = raster_paths
        self.out_path = out_path
        self.vrt_path = self.out_path + '.vrt'
        self.clean_inputs = clean_inputs

    def make_vrt(self, reverse: bool = True):
        """_summary_

        Args:
            _description_
        """
        _tmr = Timer()
        safe_makedirs(os.path.dirname(self.vrt_path))

        # Clear out any old VRTs for safety
        if os.path.exists(self.vrt_path):
            os.remove(self.vrt_path)

        # VRT is inverted (top layer is at the bottom of the file)
        if reverse is True:
            self.raster_paths.reverse()

        # Build our VRT and convert to raster
        gdal.BuildVRT(self.vrt_path, self.raster_paths)
        self.log.info(f'VRT "{self.vrt_path}" built in {_tmr.toString()}')

    def make_composite(self):
        """_summary_

        Args:
            _description_
        """
        _tmr = Timer()
        # This will add compresssion parameters
        meta = get_raster_meta(self.vrt_path)

        with rasterio.open(self.vrt_path, 'r') as src, rasterio.open(self.out_path, 'w', **meta) as dst:
            _prg = ProgressBar(len(list(src.block_windows(1))), 50, f"Transcribing VRT {self.vrt_path}")
            counter = 0
            for _ji, window in src.block_windows(1):
                _prg.update(counter)
                counter += 1
                array_dest = src.read(1, window=window, masked=True)
                dst.write(array_dest, window=window, indexes=1)
            _prg.finish()

        # Let's free up some space
        if self.clean_inputs is True:
            self.log.info('Cleaning VRT')
            os.remove(self.vrt_path)
            self.log.info(f'Cleaning up {len(self.raster_paths)} intermediate files')
            for raster_path in self.raster_paths:
                os.remove(raster_path)
        self.log.info(f'Composite built for "{self.out_path}" in: {_tmr.toString()}')
