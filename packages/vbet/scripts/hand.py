""" Name: Generate HAND

    Purpose: Generate network specific HAND raster for vbet using TauDEM

    Author: Kelly Whitehead

    Date: Feb 5 2021
    """

from __future__ import annotations
import os
from typing import List
import subprocess
from rscommons import Logger
import gdal


def hand(dem: str, flowlines_gpkg: str, flowlines_layer: str, working_dir: str, out_hand: str):
    """Generate HAND raster for a watershed

    Args:
        dem (Path): geotiff of dem raster
        flowlines_gpkg (GPKG): geopackage where flowline network is saved
        flowlines_layer (str): layer name in geopackage for flowline network (e.g. vbet_network)
        working_dir (Path): temporary directory to store intermedite files
        out_hand (file): path and name of geotiff hand output

    Returns:
        [type]: [description]
    """
    log = Logger("HAND")
    log.info(f"Generating HAND for {dem} and {os.path.join(flowlines_gpkg,flowlines_layer)} using {working_dir}")

    # Format Paths
    path_pitfill = os.path.join(working_dir, "pitfill.tif")
    path_ang = os.path.join(working_dir, "dinfflowdir_ang.tif")
    path_slp = os.path.join(working_dir, "dinfflowdir_slp.tif")
    path_rasterized_flowline = os.path.join(working_dir, "rasterized_flowline.tif")

    # PitRemove
    log.info("Filling DEM pits")
    pitfill_status = run_subprocess(working_dir, ["mpiexec", "-n", "8", "pitremove", "-z", dem, "-fel", path_pitfill])
    if pitfill_status != 0 or not os.path.isfile(path_pitfill):
        raise Exception('TauDEM: pitfill failed')

    # Flow Dir
    log.info("Finding flow direction")
    dinfflowdir_status = run_subprocess(working_dir, ["mpiexec", "-n", "8", "dinfflowdir", "-fel", path_pitfill, "-ang", path_ang, "-slp", path_slp])
    if dinfflowdir_status != 0 or not os.path.isfile(path_ang):
        raise Exception('TauDEM: dinfflowdir failed')

    # rasterize flowlines
    log.info("Rasterizing flowlines")

    g = gdal.Open(dem)
    geoT = g.GetGeoTransform()
    width, height = g.RasterXSize, g.RasterYSize
    xmin = min(geoT[0], geoT[0] + width * geoT[1])
    xmax = max(geoT[0], geoT[0] + width * geoT[1])
    ymin = min(geoT[3], geoT[3] + geoT[-1] * height)
    ymax = max(geoT[3], geoT[3] + geoT[-1] * height)
    # Close our dataset
    g = None

    gdal_rasterize_status = run_subprocess(working_dir, ["gdal_rasterize", "-te", str(xmin), str(ymin), str(xmax), str(ymax), "-ts", str(width), str(height), "-burn", "1", "-l", flowlines_layer, flowlines_gpkg, path_rasterized_flowline])
    # Reminder: -te xmin ymin xmax ymax, -ts width height

    # generate hand
    log.info("Generating HAND")
    dinfdistdown_status = run_subprocess(working_dir, ["mpiexec", "-n", "8", "dinfdistdown", "-ang", path_ang, "-fel", path_pitfill, "-src", path_rasterized_flowline, "-dd", out_hand, "-m", "ave", "v"])
    if dinfdistdown_status != 0 or not os.path.isfile(out_hand):
        raise Exception('TauDEM: dinfdistdown failed')

    # Fin
    log.info(f"Generated HAND Raster {out_hand}")
    log.info("HAND process complete")

    return out_hand


def run_subprocess(cwd: str, cmd: List[str]):

    log = Logger("Subprocess")
    log.info('Running command: {}'.format(' '.join(cmd)))

    # Realtime logging from subprocess
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
    # Here we print the lines in real time but we will also log them afterwords
    # replace '' with b'' for Python 3
    for output in iter(process.stdout.readline, b''):
        for line in output.decode('utf-8').split('\n'):
            if len(line) > 0:
                log.info(line)

    for errout in iter(process.stderr.readline, b''):
        for line in errout.decode('utf-8').split('\n'):
            if len(line) > 0:
                log.error(line)

    retcode = process.poll()
    if retcode is not None and retcode > 0:
        log.error('Process returned with code {}'.format(retcode))

    return retcode
