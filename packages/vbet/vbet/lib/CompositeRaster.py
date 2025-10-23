""" VBET compiles lots of little rasters into a big one.

To do this efficiently we use VRTs 

That is just tedious enough that it deservers its own class
"""
import os
from typing import List
from osgeo import gdal
from rscommons.util import safe_makedirs
from rscommons import Logger, Timer


class CompositeRaster(object):
    """class to generate a vrt composite of several rasters and save to single raster
    """

    def __init__(self, out_path: str, raster_paths: List[str], vrt_path: str = None):
        """create a composte raster from a list of raster paths

        Args:
            out_path (str): directory where the vrt will be saved
            raster_paths (List[str]): list of rasters to include in composite raster
            vrt_path (str, optional): where to save the vrt
        """
        self.log = Logger('CompositeRaster')
        self.raster_paths = raster_paths
        self.out_path = out_path
        self.vrt_path = vrt_path if vrt_path else self.out_path + '.vrt'

    def make_vrt(self, reverse: bool = True):
        """create the composite raster vrt

        Args:
            reverse (bool, optional): reverse the order of rasters in list. Defaults to True
        """
        _tmr = Timer()
        safe_makedirs(os.path.dirname(self.vrt_path))

        # Clear out any old VRTs for safety
        if os.path.exists(self.vrt_path):
            os.remove(self.vrt_path)

        # VRT is inverted (top layer is at the bottom of the file)
        # Make sure not to do this in-place in case we want to re-use the raster later
        raster_arr = list(reversed(self.raster_paths)) if reverse else self.raster_paths

        # Build our VRT and convert to raster
        # Filter out missing rasters first; log any removed
        existing = []
        missing = []
        for rp in raster_arr:
            if os.path.isfile(rp):
                existing.append(rp)
            else:
                missing.append(rp)
        if missing:
            self.log.warning(f"Skipping {len(missing)} missing rasters in VRT build. First 5: {missing[:5]}")
        if not existing:
            raise FileNotFoundError("No existing rasters to build VRT.")
        gdal.BuildVRT(self.vrt_path, existing)
        self.log.info(f'VRT "{self.vrt_path}" built in {_tmr.toString()}')

    def _verify_vrt(self):
        """Open the VRT and verify we have non-zero dimensions and at least one source."""
        ds = gdal.Open(self.vrt_path)
        if ds is None:
            raise RuntimeError(f"Unable to open VRT: {self.vrt_path}")
        xsize = ds.RasterXSize
        ysize = ds.RasterYSize
        if xsize == 0 or ysize == 0:
            subdatasets = ds.GetSubDatasets()
            raise RuntimeError(f"VRT has zero dimensions (x={xsize}, y={ysize}). Subdatasets: {subdatasets}")
        band = ds.GetRasterBand(1)
        if band is None:
            raise RuntimeError("VRT has no band 1.")
        return xsize, ysize

    def make_composite(self):
        """output single raster fiile from composite raster

        """
        _tmr = Timer()
        # We previously used rasterio transcription; meta retained for possible future use
        # Early validation
        if not os.path.isfile(self.vrt_path):
            self.log.error(f"VRT path does not exist: {self.vrt_path}")
            raise FileNotFoundError(self.vrt_path)
        if not self.raster_paths:
            self.log.error("No raster paths provided for composite.")
            raise ValueError("Empty raster_paths list")

        # Verify VRT integrity
        try:
            xsize, ysize = self._verify_vrt()
            if xsize * ysize == 0:
                raise RuntimeError(f"VRT has invalid size ({xsize} x {ysize}).")
            self.log.debug(f"VRT dimensions verified: {xsize} x {ysize}")
        except Exception as verr:  # noqa: BLE001
            self.log.error(f"VRT verification failed: {verr}")
            raise

        try:
            # Proper usage: specify creation options via creationOptions
            translate_opts = gdal.TranslateOptions(creationOptions=['COMPRESS=DEFLATE'])

            def _progress(pct, _msg, _data):  # pct is 0..1
                if int(pct * 100) % 5 == 0:
                    self.log.debug(f"Composite progress: {int(pct*100)}%")

            gdal.Translate(self.out_path, self.vrt_path, options=translate_opts, callback=_progress)
        except Exception as gdal_err:  # noqa: BLE001
            self.log.error(f"GDAL Translate composite build failed: {gdal_err}")
            raise

        self.log.info(f'Composite built for "{self.out_path}" in: {_tmr.toString()}')
