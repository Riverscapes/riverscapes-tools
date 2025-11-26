import logging
import sys
import argparse
from os import path
from shapely.geometry import Point
import numpy as np
from rsxml import Logger
from champ_metrics.lib.shapefileloader import Shapefile
from champ_metrics.lib.exception import DataException, MissingException
from champ_metrics.lib.metrics import CHaMPMetric
from champ_metrics.lib.raster import Raster


class ThalwegMetrics(CHaMPMetric):

    TEMPLATE = {
        'Min': None,
        'Max': None,
        'Mean': None,
        'StDev': None,
        'Count': None,
        'Length': None,
        'WSGradientRatio': None,
        'WSGradientPC': None,
        'Sinuosity': None,
        'CV': None,
        'ThalwegToCenterlineRatio': None
    }

    def calc(self, sThalwegshp, sDepthRaster, sWaterSurfaceRaster, fDist, visitMetrics):
        """
        Calculate thalweg topometrics
        :param sThalwegshp: path to the thalweg shapefile
        :param sDepthRaster: path to the depth raster
        :param sWaterSurfaceRaster: path to the water surface raster
        :param fDist: distance interval for sampling the raster along the thalweg
        :param visitMetrics: dictionary of all the visit metrics calculated so far
        :return:
        """

        log = Logger("ThalwegMetrics")
        log.info("Calculating Thalweg Metrics")

        if not path.isfile(sThalwegshp):
            raise MissingException("Thalweg shapefile missing")
        if not path.isfile(sDepthRaster):
            raise MissingException("Depth raster missing")
        if not path.isfile(sWaterSurfaceRaster):
            raise MissingException("Surface raster missing")

        wettedMainstemLength = visitMetrics['Wetted']['Centerline']['MainstemLength']

        if wettedMainstemLength is None:
            raise MissingException("No wetted mainstem length found in visit metrics")

        sfile = Shapefile(sThalwegshp).featuresToShapely()

        if len(sfile) < 1:
            raise DataException("Thalweg shapefile has no features")

        thalweg = sfile[0]['geometry']
        depthRaster = Raster(sDepthRaster)
        waterSurfaceRaster = Raster(sWaterSurfaceRaster)
        samplepts = ThalwegMetrics.interpolateRasterAlongLine(thalweg, fDist)
        results = ThalwegMetrics.lookupRasterValues(samplepts, depthRaster)['values']

        # Get the elevation at the first (downstream) point on the Thalweg
        dsElev = waterSurfaceRaster.getPixelVal(thalweg.coords[0])
        usElev = waterSurfaceRaster.getPixelVal(thalweg.coords[-1])

        if (np.isnan(dsElev)):
            raise DataException('nodata detected in the raster for downstream point on the thalweg')
        elif np.isnan(usElev):
            raise DataException('nodata detected in the raster for upstream point on the thalweg')

        waterSurfaceGradientRatio = (usElev - dsElev) / thalweg.length
        waterSurfaceGradientPC = waterSurfaceGradientRatio * 100.0

        # Thalweg straight length and sinuosity
        firstPoint = Point(thalweg.coords[0])
        lastPoint = Point(thalweg.coords[-1])
        straightLength = firstPoint.distance(lastPoint)
        sinuosity = thalweg.length / straightLength

        self.metrics = {
            'Min': np.nanmin(results),
            'Max': np.nanmax(results),
            'Mean': np.mean(results),
            'StDev': np.std(results),
            'Count': np.count_nonzero(results),
            'Length': thalweg.length,
            'WSGradientRatio': waterSurfaceGradientRatio,
            'WSGradientPC': waterSurfaceGradientPC,
            'Sinuosity': sinuosity,
            'CV': 0.0,
            'ThalwegToCenterlineRatio': thalweg.length / wettedMainstemLength
            # , 'Values': results.data
        }
        if self.metrics['StDev'] != 0 and self.metrics['Mean'] != 0:
            self.metrics['CV'] = self.metrics['StDev'] / self.metrics['Mean']

        log.info("Thalweg Metrics Complete")

    @staticmethod
    def interpolateRasterAlongLine(line, fStationInterval):
        """
        Given a cross section (Linestring) and a spacing point return regularly spaced points
        along that line
        :param line:
        :param fStationInterval:
        :return:
        """
        points = [line.interpolate(currDist) for currDist in np.arange(0, line.length, fStationInterval)]
        # Add the endpoint if it doesn't already exist
        if points[-1] != line.coords[-1]:
            points.append(Point(line.coords[-1]))
        return points

    @staticmethod
    def lookupRasterValues(points, raster):
        """
        Given an array of points with real-world coordinates, lookup values in raster
        then mask out any nan/nodata values
        :param points:
        :param raster:
        :return:
        """
        pointsdict = {"points": points, "values": []}

        for pt in pointsdict['points']:
            pointsdict['values'].append(raster.getPixelVal(pt.coords[0]))

        # Mask out the np.nan values
        pointsdict['values'] = np.ma.masked_invalid(pointsdict['values'])

        return pointsdict


if __name__ == "__main__":

    logfmt = "[%(asctime)s - %(levelname)s] - %(message)s"
    dtfmt = "%Y-%m-%d %I:%M:%S"
    logging.basicConfig(filename='raster_metrics.log', level=logging.DEBUG, format=logfmt, datefmt=dtfmt)

    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('thalweg',
                        help='Path to the thalweg',
                        type=argparse.FileType('r'))
    parser.add_argument('depthraster',
                        help='Path to the depth raster',
                        type=argparse.FileType('r'))
    parser.add_argument('watersurfaceraster',
                        help='Path to the depth raster',
                        type=argparse.FileType('r'))
    parser.add_argument('dist',
                        help='interval spacing between raster measurements',
                        type=float)
    args = parser.parse_args()

    if not args.depthraster:
        print("ERROR: Missing arguments")
        parser.print_help()
        exit(0)

    if not args.watersurfaceraster:
        print("ERROR: Missing arguments")
        parser.print_help()
        exit(0)

    try:
        dMetrics = ThalwegMetrics(args.thalweg.name, args.depthraster.name, args.watersurfaceraster.name, args.dist)

    except AssertionError as e:
        sys.exit(0)
    except Exception as e:
        raise
        sys.exit(0)
