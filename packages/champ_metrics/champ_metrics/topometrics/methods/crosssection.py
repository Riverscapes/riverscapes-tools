import sys
import copy
import numpy as np
from shapely.geos import TopologicalError

from os import path
from champmetrics.lib.shapefileloader import Shapefile
from champmetrics.lib.raster import Raster
from champmetrics.lib.metrics import CHaMPMetric
from champmetrics.lib.loghelper import Logger
from champmetrics.lib.exception import DataException
from shapely.geometry import MultiLineString
from .thalweg import ThalwegMetrics

# There are various ways of summarizing the attributes of a dictionary:
# best - this is a way of picking one of the other three methods.
#       it's intended to be the version of the metric that other tools use.
#       For example, some topometrics will use the "crew" version as the best metric
#       while others will use the "auto" filtered as the best.
# auto - includes cross sections that have a width that is within 4 StDev
#       of the mean width
# none - includes all cross sections
# crew - uses the IsValid flag on the original dictionary

class CrossSectionMetrics(CHaMPMetric):

    dMetricTypes = {'WetWidth': 'Crew', 'W2MxDepth': 'Crew', 'W2AvDepth': 'Crew'}
    chTypes = ["Main", "Side", "Channel"]
    filterTypes = ["Best", "Auto", "None", "Crew"]

    TEMPLATE = {}

    def __init__(self, *args, **kwargs):
        # Build a dictionary of attributes that require topometrics.
        # The key is the cross section ShapeFile attribute field name.
        # The value is the type of filtering that will be used to report
        # the "best" topometrics (possible values are "crew", "auto", "none")
        if CrossSectionMetrics.TEMPLATE == {}:
            for chType in CrossSectionMetrics.chTypes:
                CrossSectionMetrics.TEMPLATE[chType] = {}
                for filterType in CrossSectionMetrics.filterTypes:
                    CrossSectionMetrics.TEMPLATE[chType][filterType] = {}
                    for mName in CrossSectionMetrics.dMetricTypes.keys():
                        CrossSectionMetrics.TEMPLATE[chType][filterType][mName] = {
                            "Count": None,
                            "CV": None,
                            "Mean": None,
                            "Min": None,
                            "Max": None,
                            "StdDev": None
                        }
        # Now that our template object is built call init
        super(CrossSectionMetrics, self).__init__(*args, **kwargs)

    def calc(self, crosssections, waterExtent, demPath, stationInterval):

        # Save space by only loading the desired fields from the ShapeFile.
        # We also need the 'Channel' and 'IsValid' fields if they exist.
        desiredFields = list(CrossSectionMetrics.dMetricTypes.keys())
        desiredFields.append('IsValid')

        # Open the cross section ShapeFile & build a list of all features with a dictionary of the desired fields
        clShp = Shapefile(crosssections)
        lChannels = ['Main']

        if not clShp.loaded:
            return

        if "Channel" in clShp.fields:
            desiredFields.append('Channel')
            lChannels.append('Side')

        # Older Cross Section Layers don't have the width and depth fields calculated.
        # So if all the necessary metric fields are present then continue to load
        # ShapeFile features. Otherwise we need to calculate the topometrics from scratch
        bMetricCalculationNeeded = False
        for aMetric in desiredFields:
            if not aMetric in clShp.fields:
                bMetricCalculationNeeded = True
                break

        allFeatures = []
        if bMetricCalculationNeeded:
            # Retrieve the water extent polygon exterior
            rivershp = Shapefile(waterExtent)
            polyRiverShapeFeats = rivershp.featuresToShapely()

            # Try and find a channel shape. Thers's a lot of variance here.
            if len(polyRiverShapeFeats) == 0 or 'geometry' not in polyRiverShapeFeats[0]:
                raise DataException("No features in crosssection shape file")

            # If there's only one shape then just use it
            elif len(polyRiverShapeFeats) == 1:
                polyRiverShape = polyRiverShapeFeats[0]['geometry']

            # If there's more than one shape then see if
            else:
                channelShapes = [feat['geometry'] for feat in polyRiverShapeFeats if feat['fields']['ExtentType'] == 'Channel']
                if len(channelShapes) == 0:
                    raise DataException("No features in crosssection shape file")
                polyRiverShape = channelShapes[0]


            # Calculate the topometrics from scratch for a single cross section
            shpXS = Shapefile(crosssections)
            demRaster = Raster(demPath)

            if shpXS.loaded:
                for aFeat in shpXS.featuresToShapely():
                    # Calculate the topometrics for this cross section. They will be stored on the aFeat dict under key 'topometrics'
                    calcXSMetrics(aFeat, polyRiverShape , demRaster, stationInterval)

                    # Build the all features dictionary that would be expect had the topometrics already
                    # existed in the XS shapefile and simply got loaded. This is a combination of the new topometrics
                    # and also the existing fields on the XS ShapeFile.
                    singleXSMetrics = copy.deepcopy(aFeat['topometrics'])
                    singleXSMetrics.update(aFeat['fields'])
                    allFeatures.append(singleXSMetrics)

            # Destroying the raster object appears to prevent warning messages on Windows
            demRaster = None
        else:
            allFeatures = clShp.attributesToList(desiredFields)

        # For simple ShapeFiles, make every feature part of the main channel, and
        # set every feature as valid. This helps keep code below generic
        for x in allFeatures:
            if 'Channel' not in x:
                x['Channel'] = 'Main'

            if 'IsValid' not in x:
                x['IsValid'] = 1

        for channelName in lChannels:

            # Filter the list of features to just those in this channel
            # PGB - 24 Apr 2017 - observed NULL values in 'Channel' ShapeFile field in Harold results.
            # Cast field contents to string to avoid crash here.
            channelFeatures = [x for x in allFeatures if str(x['Channel']).lower() == channelName.lower()]

            # Filter the list of features to just those that the crew considered valid
            validFeatures = [x for x in channelFeatures if x['IsValid'] != 0]

            # Filter the features to just those with a length that is within 4 standard deviations of mean wetted width
            channelStatistics = getStatistics(channelFeatures, 'WetWidth')

            autoFeatures = None
            if channelStatistics['StdDev'] is not None:
                wetWidthThreshold = channelStatistics['StdDev'] * 4
                autoFeatures = [x for x in channelFeatures if abs(x['WetWidth'] - channelStatistics['Mean']) < wetWidthThreshold]

            # Loop over each desired metric and calculate the statistics for each filtering type
            for metricName, bestFiltering in CrossSectionMetrics.dMetricTypes.items():
                populateChannelStatistics(self.metrics[channelName], 'None', metricName, channelFeatures)
                populateChannelStatistics(self.metrics[channelName], 'Crew', metricName, validFeatures)

                if channelStatistics['StdDev'] is not None:
                    populateChannelStatistics(self.metrics[channelName], 'Auto', metricName, autoFeatures)

                self.metrics[channelName]['Best'] = self.metrics[channelName][bestFiltering]

        # The topometrics for the whole channel are always the results for 'Main'.
        # For complex ShapeFiles this will be just the results for the main channel.
        # For simple, single threaded, ShapeFiles this will all cross sections.
        self.metrics['Channel'] = self.metrics['Main']


def calcXSMetrics(xs, rivershapeWithDonuts, demRaster, fStationInterval):
    """
    Calculate topometrics for a list of cross sections
    :param xs: The cross section to generate topometrics from
    :param rivershapeWithDonuts: The original rivershape file with donuts
    :param dem: Raster object
    :param fStationInterval: some interval (float)
    :return:
    """

    regularPoints = ThalwegMetrics.interpolateRasterAlongLine(xs['geometry'], fStationInterval)
    # Augment these points with values from the raster
    ptsdict = demRaster.lookupRasterValues(regularPoints)

    # Get the reference Elevation from the edges
    refElev = getRefElev(ptsdict['values'])

    xsmXSLength = xs['geometry'].length
    xsmWetWidth = dryWidth(xs['geometry'], rivershapeWithDonuts)
    xsmDryWidth = xsmXSLength - xsmWetWidth

    if refElev == 0:
        xs['fields']['isValid'] = False
        xsmMaxDepth = None
        xsmMeanDepth = None
        xsmW2MxDepth = None
        xsmW2AvDepth = None
    else:
        # The depth array must be calculated
        deptharr = refElev - ptsdict['values']

        xsmMaxDepth = maxDepth(deptharr)
        xsmMeanDepth = meanDepth(deptharr)

        xsmW2MxDepth = None
        xsmW2AvDepth = None

        if xsmWetWidth is not None and xsmMaxDepth is not None and xsmMaxDepth != 0:
            xsmW2MxDepth = xsmWetWidth / xsmMaxDepth

        if xsmWetWidth is not None and xsmMeanDepth is not None and xsmMeanDepth != 0:
            xsmW2AvDepth = xsmWetWidth / xsmMeanDepth

    # Make sure that everything has a value
    xs['topometrics'] = {
        "XSLength": metricSanitize(xsmXSLength),
        "WetWidth": metricSanitize(xsmWetWidth),
        "DryWidth": metricSanitize(xsmDryWidth),
        "MaxDepth": metricSanitize(xsmMaxDepth),
        "MeanDepth": metricSanitize(xsmMeanDepth),
        "W2MxDepth": metricSanitize(xsmW2MxDepth),
        "W2AvDepth": metricSanitize(xsmW2AvDepth),
        "BFElev": metricSanitize(refElev),
        "BFArea": None,
        "HRadius": None,
        "NumStat": None
    }

    return ptsdict

def maxDepth(arr):
    """
    Calculate the maximum   depth from a list of values
    :param arr:
    :return:
    """
    # Note we don't need to worry about negative depths because we're using max
    # Also don't worry about np.nan because the metricSanitize catches things
    return np.nanmax(arr)


def meanDepth(deptharr):
    """
    Calculate the mean depth from a list of depths
    :param deptharr:
    :return:
    """
    fValue = np.average([x for x in deptharr if x > 0])
    if np.isnan(fValue):
        return None

    return fValue

def dryWidth(xs, rivershapeWithDonuts):
    """

    :param xs: shapely cross section object
    :param rivershapeWithDonuts: Polygon with non-qualifying donuts retained
    :return:
    """
    # Get all intersects of this crosssection with the rivershape
    log = Logger("dryWidth")
    try:
        intersects = xs.intersection(rivershapeWithDonuts.buffer(0))  #KMW: buffer(0) clears up invalid geoms
    except TopologicalError as e:
        log.error(e)
        raise DataException("Could not perform intersection on `rivershapeWithDonuts`. Look for small, invalid islands as a possible cause.")

    # The intersect may be one object (LineString) or many. We have to handle both cases
    if intersects.type == "LineString":
        intersects = MultiLineString([intersects])
    elif intersects.type == "Point":
        return 0

    return sum([intersect.length for intersect in intersects])

def metricSanitize(metric):
    """
    This function does nothing more than prevent bad numbers
    :param metric:
    :return:
    """

    try:
        num = np.float(metric)
    except:
        num = None
    # We explicitly cast this to np.float (NOT np.float32 or np.float64 etc.) to keep ogr from breaking
    return num

def getRefElev(arr):
    """
    Take a masked array and return a reference depth
    :param arr: Masked array
    :return:
    """
    if isinstance(arr, np.ma.MaskedArray) and (arr.mask[0] or arr.mask[-1]):
        fValue = 0
    else:
        fValue = np.average(arr[0] + arr[-1]) / 2

    return fValue

def populateChannelStatistics(dChannelMetrics, filteringName, metricName, featureList):

    if not filteringName in dChannelMetrics:
        dChannelMetrics[filteringName] = {}

    dChannelMetrics[filteringName][metricName] = getStatistics(featureList, metricName)

def getStatistics(lFeatures, sAttribute):

    lValues = [x[sAttribute] for x in lFeatures]
    dStatistics = {}


    if len(lValues) > 0 and not all(b is None for b in lValues):
        # TODO: What do we mean here by np.mean? Do we mean any or all above?
        # This will make all None values 0 but that may skew the mean
        lValues = list(filter(lambda x: x is not None, lValues))

        dStatistics['Count'] = len(lValues)
        dStatistics['Mean'] = np.mean(lValues)
        dStatistics['Min'] = min(lValues)
        dStatistics['Max'] = max(lValues)
        dStatistics['StdDev'] = np.std(lValues)
        dStatistics['CV'] = None
        if dStatistics['StdDev'] != 0 and dStatistics['Mean'] != 0:
            dStatistics['CV'] = dStatistics['StdDev'] / dStatistics['Mean']

    else:
        dStatistics['Count'] = 0
        dStatistics['Mean'] = None
        dStatistics['Min'] = None
        dStatistics['Max'] = None
        dStatistics['StdDev'] = None
        dStatistics['CV'] = None

    return dStatistics

if __name__ == "__main__":
    import logging
    import argparse
    logfmt = "[%(asctime)s - %(levelname)s] - %(message)s"
    dtfmt = "%Y-%m-%d %I:%M:%S"
    logging.basicConfig(filename='crosssection_metrics.log', level=logging.DEBUG, format=logfmt, datefmt=dtfmt)

    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('crosssections',
                        help='Path to the cross section shapefile',
                        type=argparse.FileType('r'))

    parser.add_argument('waterextent',
                        help='Path to the water extent shapefile',
                        type=argparse.FileType('r'))

    parser.add_argument('dem',
                        help='Path to the DEM raster',
                        type=argparse.FileType('r'))

    parser.add_argument('stationinterval',
                        help='Station Interval',
                        type=float)

    args = parser.parse_args()

    if not args.crosssections:
        print("ERROR: Missing arguments")
        parser.print_help()
        exit(0)

    try:
        xsmetrics = CrossSectionMetrics(args.crosssections.name, args.waterextent.name, args.dem.name, args.stationinterval)

    except AssertionError as e:
        sys.exit(0)
    except Exception as e:
        raise
        sys.exit(0)
