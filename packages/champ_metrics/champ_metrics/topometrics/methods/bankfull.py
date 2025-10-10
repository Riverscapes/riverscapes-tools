import argparse
import sys
import traceback
import os
from os import path
import xml.etree.ElementTree as ET

import geopandas
import rasterio
from shapely.geometry import Point
import numpy as np

from champ_metrics.lib import env
from champ_metrics.lib.sitkaAPI import downloadUnzipTopo
from champ_metrics.lib.loghelper import Logger
from champ_metrics.lib.exception import DataException, MissingException, NetworkException

__version__ = "0.0.2"


def BankfullMetrics(dem, detrended_dem, shp_points):
    """
    :param topoDataFolder:
    :param results_xmlfile:
    :param visitid:
    :return:
    """

    log = Logger("Bankfull Metrics")

    # 1.  find the average elevation of crew bankfull points in the detrended DEM.
    gdf_topo_points = geopandas.GeoDataFrame().from_file(shp_points)

    gdf_bf_points = None
    if 'Code' in gdf_topo_points:
        gdf_bf_points = gdf_topo_points[gdf_topo_points["Code"] == 'bf']
    else:
        gdf_bf_points = gdf_topo_points[gdf_topo_points["code"] == 'bf']

    log.info("Loaded BF points")

    with rasterio.open(detrended_dem) as rio_detrended:
        bf_elevations = [v[0] for v in rio_detrended.sample(zip([Point(p).x for p in gdf_bf_points.geometry],
                                                                [Point(p).y for p in gdf_bf_points.geometry]))
                         if v[0] != rio_detrended.nodata]  # Filter out points not within detrendedDEM data extent.
        detrended_band = rio_detrended.read(1)

    if len(bf_elevations) == 0:
        log.error("No valid bf elevation points found.")
    else:
        log.info("Sampled {} valid BF point elevations from the DetrendedDEM".format(str(len(bf_elevations))))

    with rasterio.open(dem) as rio_dem:
        dem_band = rio_dem.read(1)

    # enforce orthogonal rasters
    dem_pad_top = int((rio_detrended.bounds.top - rio_dem.bounds.top) / 0.1) if rio_detrended.bounds.top > rio_dem.bounds.top else 0
    dem_pad_bottom = int((rio_dem.bounds.bottom - rio_detrended.bounds.bottom) / 0.1) if rio_dem.bounds.bottom > rio_detrended.bounds.bottom else 0
    dem_pad_right = int((rio_detrended.bounds.right - rio_dem.bounds.right) / 0.1) if rio_detrended.bounds.right > rio_dem.bounds.right else 0
    dem_pad_left = int((rio_dem.bounds.left - rio_detrended.bounds.left) / 0.1) if rio_dem.bounds.left > rio_detrended.bounds.left else 0

    det_pad_top = int((rio_dem.bounds.top - rio_detrended.bounds.top) / 0.1) if rio_detrended.bounds.top < rio_dem.bounds.top else 0
    det_pad_bottom = int((rio_detrended.bounds.bottom - rio_dem.bounds.bottom) / 0.1) if rio_dem.bounds.bottom < rio_detrended.bounds.bottom else 0
    det_pad_right = int((rio_dem.bounds.right - rio_detrended.bounds.right) / 0.1) if rio_detrended.bounds.right < rio_dem.bounds.right else 0
    det_pad_left = int((rio_detrended.bounds.left - rio_dem.bounds.left) / 0.1) if rio_dem.bounds.left < rio_detrended.bounds.left else 0

    np_detrended_ortho = np.pad(detrended_band, ((det_pad_top, det_pad_bottom), (det_pad_left, det_pad_right)), mode="constant", constant_values=np.nan)
    np_dem_ortho = np.pad(dem_band, ((dem_pad_top, dem_pad_bottom), (dem_pad_left, dem_pad_right)), mode="constant", constant_values=np.nan)

    if all(v == 0 for v in [dem_pad_top, dem_pad_bottom, dem_pad_right, dem_pad_left, det_pad_top, det_pad_bottom, det_pad_right, det_pad_left]):
        log.info("DEM and DetrendedDEM have concurrent extents")
    else:
        log.warning("Non-Concurrent Rasters encountered. DEM and DetrendedDEM using padded extents")

    ma_detrended = np.ma.MaskedArray(np_detrended_ortho, np.equal(np_detrended_ortho, rio_detrended.nodata))
    ma_dem = np.ma.MaskedArray(np_dem_ortho, np.equal(np_dem_ortho, rio_dem.nodata))

    # Generate Trend Grid
    np_trendgrid = np.subtract(ma_dem, ma_detrended)
    log.info("Trend surface created")

    # Average BF elev to constant raster in detrended space
    ave_bf_det_elev = sum(bf_elevations)/float(len(bf_elevations))
    ma_bf_detrended = np.full_like(ma_detrended, ave_bf_det_elev, dtype=np.float64)
    log.info("Detrended BF surface created")

    # add trend grid to BF detrended surface
    np_bf_surface = np.add(ma_bf_detrended, np_trendgrid)
    log.info("BF elevation surface created")

    # Generate depth and volume
    np_bf_depth_raw = np.subtract(np_bf_surface, ma_dem)
    np_bf_depth = np.multiply(np.greater(np_bf_depth_raw, 0), np_bf_depth_raw)
    np_bf_volume = np.multiply(np_bf_depth, 0.1*0.1)
    log.info("BF Depth surface created")

    ma_bf_depth = np.ma.MaskedArray(np_bf_depth, np.equal(np_bf_depth, -0.0))  # -0.0 values were getting included in the mean calculation

    # Run ZonalStatisticsAsTable to get the metric values:
    # Sum the bankfull depth raster values and multiply by the area of one cell to produce BFVol.
    # Max the bankfull depth raster values is DepthBF_Max.
    # Average the bankfull depth raster values is DepthBF_Avg
    bf_volume = np.nansum(np_bf_volume)
    bf_depth_max = np.nanmax(ma_bf_depth)
    bf_depth_mean = np.nanmean(ma_bf_depth)
    log.info("BF metrics calculated")

    results = {"Volume": bf_volume,
               "Depth": {"Max": bf_depth_max, "Mean": bf_depth_mean}}

    return results


def write_bfmetrics_xml(dict_results, visit_id, out_xmlfile):
    """write the bankfull metric xml file"""
    import datetime

    root = ET.Element("BankfullMetrics")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    nodeMeta = ET.SubElement(root, "Meta")
    nodeGenerationDate = ET.SubElement(nodeMeta, "GenerationDate")
    nodeGenerationDate.text = str(datetime.datetime.now().isoformat())
    nodeModelVersion = ET.SubElement(nodeMeta, "ModelVersion")
    nodeModelVersion.text = str(__version__)
    nodeVisit = ET.SubElement(nodeMeta, "Visit")
    nodeVisit.text = str(visit_id)

    nodeMetrics = ET.SubElement(root, "Metrics")
    nodeBankfull = ET.SubElement(nodeMetrics, "Bankfull")
    nodeWaterExtent = ET.SubElement(nodeBankfull, "WaterExtent")
    nodeDepth = ET.SubElement(nodeWaterExtent, "Depth")
    nodeDepthMax = ET.SubElement(nodeDepth, "Max")
    if dict_results.has_key("DepthBF_Max"):
        nodeDepthMax.text = str(dict_results["DepthBF_Max"])
    nodeDepthMean = ET.SubElement(nodeDepth, "Mean")
    if dict_results.has_key("DepthBF_Avg"):
        nodeDepthMean.text = str(dict_results["DepthBF_Avg"])
    nodeVolume = ET.SubElement(nodeBankfull, "Volume")
    if dict_results.has_key("BFVol"):
        nodeVolume.text = str(dict_results["BFVol"])

    indent(root)
    tree = ET.ElementTree(root)
    tree.write(out_xmlfile, 'utf-8', True)

    return


def indent(elem, level=0, more_sibs=False):
    """ Pretty Print XML Element
    Source: http://stackoverflow.com/questions/749796/pretty-printing-xml-in-python
    """

    i = "\n"
    if level:
        i += (level - 1) * '  '
    num_kids = len(elem)
    if num_kids:
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
            if level:
                elem.text += '  '
        count = 0
        for kid in elem:
            indent(kid, level + 1, count < num_kids - 1)
            count += 1
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
            if more_sibs:
                elem.tail += '  '
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i
            if more_sibs:
                elem.tail += '  '


def main():
    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('visitID', help='Visit ID', type=int)
    parser.add_argument('outputfolder', help='Path to output folder', type=str)
    parser.add_argument('--datafolder', help='(optional) Top level folder containing TopoMetrics Riverscapes projects', type=str)
    parser.add_argument('--verbose', help='Get more information in your logs.', action='store_true', default=False)
    args = parser.parse_args()

    # Make sure the output folder exists
    resultsFolder = os.path.join(args.outputfolder, "outputs")

    # Initiate the log file
    logg = Logger("Program")
    logfile = os.path.join(resultsFolder, "bankfull_metrics.log")
    xmlfile = os.path.join(resultsFolder, "bankfull_metrics.xml")
    logg.setup(logPath=logfile, verbose=args.verbose)

    # Initiate the log file
    log = Logger("Program")
    log.setup(logPath=logfile, verbose=args.verbose)

    try:
        # Make some folders if we need to:
        if not os.path.isdir(args.outputfolder):
            os.makedirs(args.outputfolder)
        if not os.path.isdir(resultsFolder):
            os.makedirs(resultsFolder)

        # If we need to go get our own topodata.zip file and unzip it we do this
        if args.datafolder is None:
            topoDataFolder = os.path.join(args.outputfolder, "inputs")
            fileJSON, projectFolder = downloadUnzipTopo(args.visitID, topoDataFolder)
        # otherwise just pass in a path to existing data
        else:
            projectFolder = args.datafolder

        from champ_metrics.lib.topoproject import TopoProject
        topo_project = TopoProject(os.path.join(projectFolder, "project.rs.xml"))
        tree = ET.parse(os.path.join(projectFolder, "project.rs.xml"))
        root = tree.getroot()
        visitid = root.findtext("./MetaData/Meta[@name='Visit']") if root.findtext("./MetaData/Meta[@name='Visit']") is not None else root.findtext("./MetaData/Meta[@name='VisitID']")
        finalResult = BankfullMetrics(topo_project.getpath("DEM"),
                                      topo_project.getpath("DetrendedDEM"),
                                      topo_project.getpath("Topo_Points"))

        write_bfmetrics_xml(finalResult, visitid, xmlfile)
        sys.exit(0)

    except (DataException, MissingException, NetworkException) as e:
        # Exception class prints the relevant information
        traceback.print_exc(file=sys.stdout)
        sys.exit(e.returncode)
    except AssertionError as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


if __name__ == "__main__":
    main()
