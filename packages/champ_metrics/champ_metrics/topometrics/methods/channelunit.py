import sys
from os import path
import numpy as np
from shapely.geometry import Point
from champ_metrics.lib.raster import Raster
from champ_metrics.lib.shapefileloader import Shapefile
from champ_metrics.lib.exception import DataException
from champ_metrics.lib.channelunits import dUnitDefs
from champ_metrics.lib.channelunits import getCleanTierName
from champ_metrics.lib.metrics import CHaMPMetric
from champ_metrics.lib.exception import MissingException


class ChannelUnitMetrics(CHaMPMetric):

    TEMPLATE = {}

    def __init__(self, *args, **kwargs):

        # Default to the CHaMP channel unit definitions but
        # override and use provided dictionary if there is one
        localDefs = dUnitDefs
        if len(args) > 5:
            localDefs = args[5]

        if ChannelUnitMetrics.TEMPLATE == {}:
            ChannelUnitMetrics.TEMPLATE = ChannelUnitMetrics._templateMaker(None, localDefs)
        # Now that our template object is built call init
        super(ChannelUnitMetrics, self).__init__(*args, **kwargs)

    @staticmethod
    def _templateMaker(initValue, unitDefinitions):
        """
        This template object is super complex so we need a whole method just to build it
        :param initValue:
        :return:
        """
        base = {
            "resultsCU": [],
            "ResultsTier1": {},
            "ResultsTier2": {},
            "ResultsChannelSummary": {}
        }

        vallist = ['Area', 'Volume', 'Count', 'Spacing', 'Percent', 'Frequency', 'ThalwegIntersectCount']
        nonelist = ['ResidualDepth', 'DepthAtThalwegExit', 'MaxMaxDepth', 'AvgMaxDepth']

        if not unitDefinitions:
            unitDefinitions = dUnitDefs

        # Dictionaries for tier 1 and tier 2 results
        # These require area and volume and by creating them here there will always be values for all types.
        for tier1Name in unitDefinitions:

            base['ResultsTier1'][getCleanTierName(tier1Name)] = {}
            base['ResultsTier1'][getCleanTierName(tier1Name)]['Name'] = tier1Name

            for val in vallist:
                base['ResultsTier1'][getCleanTierName(tier1Name)][val] = initValue
            for val in nonelist:
                base['ResultsTier1'][getCleanTierName(tier1Name)][val] = None

            for tier2Name in unitDefinitions[tier1Name]:
                base['ResultsTier2'][getCleanTierName(tier2Name)] = {}
                base['ResultsTier2'][getCleanTierName(tier2Name)]['Name'] = tier2Name

                for val in vallist:
                    base['ResultsTier2'][getCleanTierName(tier2Name)][val] = initValue
                for val in nonelist:
                    base['ResultsTier2'][getCleanTierName(tier2Name)][val] = None

        # Dictionary for the side channel summary topometrics
        base['ResultsChannelSummary'] = {
            'Main': {'Area': initValue},
            'SmallSideChannel':  {'Area': initValue, 'Count': initValue, 'Percent': initValue, 'Volume': initValue},
            'LargeSideChannel':  {'Area': initValue, 'Count': initValue, 'Percent': initValue, 'Volume': initValue},
            'SideChannelSummary': {'Area': initValue, 'Count': initValue, 'Percent': initValue, 'Volume': initValue},
            'ChannelUnitBreakdown': {'SmallSideChannel': initValue, 'Other': initValue}
        }
        return base

    def calc(self, shpCUPath, shpThalweg, rasDepth, visitMetrics, dUnits, unitDefs):

        if not path.isfile(shpCUPath):
            raise MissingException("Channel units file not found")
        if not path.isfile(shpThalweg):
            raise MissingException("Thalweg shape file not found")
        if not path.isfile(rasDepth):
            raise MissingException("Depth raster file not found")

        siteLength = visitMetrics['Wetted']['Centerline']['MainstemLength']

        if siteLength is None:
            raise DataException("No valid site length found in visit metrics")

        # Give us a fresh template with 0's in the value positions
        self.metrics = self._templateMaker(0, unitDefs)
        dResultsChannelSummary = self.metrics['ResultsChannelSummary']
        dResultsTier1 = self.metrics['ResultsTier1']
        dResultsTier2 = self.metrics['ResultsTier2']
        resultsCU = self.metrics['resultsCU']

        # Load the Thalweg feature
        thalweg = Shapefile(shpThalweg).featuresToShapely()
        thalwegLine = thalweg[0]['geometry']

        # Load the depth raster
        depthRaster = Raster(rasDepth)

        # Load the channel unit polygons and calculate the total area
        # The channel units should be clipped to the wetted extent and so this
        # can be used as the site area wetted
        shpCU = Shapefile(shpCUPath)
        arrCU = depthRaster.rasterMaskLayer(shpCUPath, "UnitNumber")

        feats = shpCU.featuresToShapely()
        for aFeat in feats:
            dResultsChannelSummary['Main']['Area'] += aFeat['geometry'].area

        # Loop over each channel unit and calculate topometrics
        for aFeat in feats:
            nCUNumber = int(aFeat['fields']['UnitNumber'])

            if nCUNumber not in dUnits:
                self.log.error("Channel Unit: '{0}' not present in the aux data.".format(nCUNumber))
                # Keep it general for the exception so we can aggregate them
                raise DataException("The Channel Unit ShapeFile contains a unit number that is not present in the aux data.")

            tier1Name = dUnits[nCUNumber][0]
            tier2Name = dUnits[nCUNumber][1]
            nSegment = dUnits[nCUNumber][2]
            # print("Channel Unit Number {0}, Segment {1}, Tier 1 - {2}, Tier 2 - {3}").format(nCUNumber, nSegment, tier1Name, tier2Name)

            unitMetrics = {}
            resultsCU.append(unitMetrics)
            unitMetrics['ChannelUnitNumber'] = nCUNumber
            unitMetrics['Area'] = aFeat['geometry'].area
            unitMetrics['Tier1'] = tier1Name
            unitMetrics['Tier2'] = tier2Name
            unitMetrics['Length'] = None
            unitMetrics['ResidualDepth'] = None
            unitMetrics['DepthAtThalwegExit'] = None
            unitMetrics['ThalwegIntersect'] = 0

            # Get the depth raster for this unit as variable so we can check
            # whether it is entirely masked below.
            depArr = depthRaster.array[arrCU == nCUNumber]
            if depArr.count() == 0:
                unitMetrics['MaxDepth'] = 0
                unitMetrics['Volume'] = 0
            else:
                unitMetrics['MaxDepth'] = np.max(depArr)
                unitMetrics['Volume'] = np.sum(depthRaster.array[arrCU == nCUNumber]) * (depthRaster.cellWidth**2)

            if nSegment != 1:
                dSideChannelSummary = dResultsChannelSummary['SideChannelSummary']
                dMain = dResultsChannelSummary['Main']
                # Side channel summary captures both small and large side channels
                dSideChannelSummary['Area'] += aFeat['geometry'].area
                dSideChannelSummary['Count'] += 1
                dSideChannelSummary['Percent'] = 100 * dSideChannelSummary['Area'] / dMain['Area']
                dSideChannelSummary['Volume'] += unitMetrics['Volume']

                if 'side' in tier1Name.lower():
                    dSmallSideChannel = dResultsChannelSummary['SmallSideChannel']
                    dSmallSideChannel['Area'] += aFeat['geometry'].area
                    dSmallSideChannel['Count'] += 1
                    dSmallSideChannel['Percent'] = 100 * dSmallSideChannel['Area'] / dMain['Area']
                    dSmallSideChannel['Volume'] += unitMetrics['Volume']
                else:
                    dLargeSideChannel = dResultsChannelSummary['LargeSideChannel']
                    dLargeSideChannel['Area'] += aFeat['geometry'].area
                    dLargeSideChannel['Count'] += 1
                    dLargeSideChannel['Percent'] = 100 * dLargeSideChannel['Area'] / dMain['Area']
                    dLargeSideChannel['Volume'] += unitMetrics['Volume']

            if tier1Name is None:
                raise DataException("tier1Name cannot be 'None'")

            if 'side' in tier1Name.lower():
                dResultsChannelSummary['ChannelUnitBreakdown']['SmallSideChannel'] += 1
            else:
                dResultsChannelSummary['ChannelUnitBreakdown']['Other'] += 1

            if (thalwegLine.intersects(aFeat['geometry'])):
                cuThalwegLine = thalwegLine.intersection(aFeat['geometry'])

                exitPoint = None
                if cuThalwegLine.type == 'LineString':
                    exitPoint = cuThalwegLine.coords[0]
                else:
                    exitPoint = cuThalwegLine[0].coords[0]

                # Retrieve a list of points along the Thalweg in the channel unit
                thalwegPoints = ChannelUnitMetrics.interpolatePointsAlongLine(cuThalwegLine, 0.13)
                thalwegDepths = ChannelUnitMetrics.lookupRasterValuesAtPoints(thalwegPoints, depthRaster)
                unitMetrics['MaxDepth'] = np.nanmax(thalwegDepths['values'])
                unitMetrics['DepthAtThalwegExit'] = depthRaster.getPixelVal(exitPoint)
                unitMetrics['ResidualDepth'] = unitMetrics['MaxDepth'] - unitMetrics['DepthAtThalwegExit']
                unitMetrics['Length'] = cuThalwegLine.length
                unitMetrics['ThalwegIntersect'] = 1

            # Tier 1 and tier 2 topometrics. Note that metric dictionary keys are used for XML tags & require cleaning
            tier1NameClean = getCleanTierName(tier1Name)
            self._calcTierLevelMetrics(dResultsTier1[tier1NameClean], tier1Name, unitMetrics, siteLength, dResultsChannelSummary['Main']['Area'])

            tier2NameClean = getCleanTierName(tier2Name)
            self._calcTierLevelMetrics(dResultsTier2[tier2NameClean], tier2Name, unitMetrics, siteLength, dResultsChannelSummary['Main']['Area'])

        # Calculate the average of the channel unit max depths for each tier 1 and tier 2 type
        for tierKey, tierMetrics in {'Tier1': dResultsTier1, 'Tier2': dResultsTier2}.items():
            for tierName, metricDict in tierMetrics.items():
                maxDepthList = [aResult['MaxDepth'] for aResult in resultsCU if getCleanTierName(aResult[tierKey]) == tierName]
                if len(maxDepthList) > 0:
                    metricDict['AvgMaxDepth'] = np.average(maxDepthList)

        # Convert the sum of residual depth and depth at thalweg exist
        # to average residual depth for each tier 1 and tier 2 type
        for tierMetricDict in [dResultsTier1, dResultsTier2]:
            for tierName, tierMetrics in tierMetricDict.items():
                # channel unit types that don't occur should retain the value None for Residual Depth and Depth at Thalweg exit
                if tierMetrics['Count'] > 0 and tierMetrics['ThalwegIntersectCount'] > 0:
                    for metricName in ['ResidualDepth', 'DepthAtThalwegExit']:
                        if tierMetrics[metricName] is not None and tierMetrics[metricName] != 0:
                            tierMetrics[metricName] = tierMetrics[metricName] / tierMetrics['ThalwegIntersectCount']
                        else:
                            tierMetrics[metricName] = 0

    def _calcTierLevelMetrics(self, tierMetrics, rawTierName, unitMetrics, siteLength, siteArea):
        """
        Incorporates the metric values for a channel unit into a dictionary of tier level topometrics
        :param tierMetrics: Dictionary of either tier 1 or tier 2 topometrics that will be calculated in this method
        :param rawTierName: Raw, uncleaned name of Channel unit
        :param unitMetrics: the topometrics for an individual channel unit that have already been calculated
        :param siteLength: the total length of the site
        :param siteArea: the total area of all channel units
        :return: None
        """

        tierMetrics['Name'] = rawTierName
        tierMetrics['Area'] += unitMetrics['Area']
        tierMetrics['Volume'] += unitMetrics['Volume']
        tierMetrics['Count'] += 1
        tierMetrics['Spacing'] = siteLength / tierMetrics['Count']
        tierMetrics['Frequency'] = 100 * tierMetrics['Count'] / siteLength
        tierMetrics['Percent'] = 100 * tierMetrics['Area'] / siteArea
        tierMetrics['MaxMaxDepth'] = max(tierMetrics['MaxMaxDepth'], unitMetrics['MaxDepth']) if tierMetrics['MaxMaxDepth'] else unitMetrics['MaxDepth']
        tierMetrics['ThalwegIntersectCount'] += unitMetrics['ThalwegIntersect']

        if unitMetrics['ResidualDepth']:
            if tierMetrics['ResidualDepth']:
                tierMetrics['ResidualDepth'] += unitMetrics['ResidualDepth']
            else:
                tierMetrics['ResidualDepth'] = unitMetrics['ResidualDepth']

        if unitMetrics['DepthAtThalwegExit']:
            if tierMetrics['DepthAtThalwegExit']:
                tierMetrics['DepthAtThalwegExit'] += unitMetrics['DepthAtThalwegExit']
            else:
                tierMetrics['DepthAtThalwegExit'] = unitMetrics['DepthAtThalwegExit']

    @staticmethod
    def interpolatePointsAlongLine(line, fStationInterval):
        """
        Given a cross section (Linestring) and a spacing point return regularly spaced points
        along that line
        :param line:
        :param fStationInterval:
        :return:
        """
        try:
            points = [line.interpolate(currDist) for currDist in np.arange(0, line.length, fStationInterval)]
        except TypeError as e:
            raise DataException("Error interpolating thalweg in channel unit. Only linear types support this operation. Type of 'line' is '{}'".format(line.type))

        # The line can be a single line if the thalweg passes through the CU once
        # or multi part line if the thalweg passes through multiple times.
        # So unify the return type as a list of lines.
        if line.type == 'LineString':
            line = [line]

        # Add the endpoint if it doesn't already exist
        if points[-1] != line[-1].coords[-1]:
            points.append(Point(line[-1].coords[-1]))
        return points

    @staticmethod
    def lookupRasterValuesAtPoints(points, raster):
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
    import logging
    import argparse
    logfmt = "[%(asctime)s - %(levelname)s] - %(message)s"
    dtfmt = "%Y-%m-%d %I:%M:%S"
    logging.basicConfig(filename='channel_units.log', level=logging.DEBUG, format=logfmt, datefmt=dtfmt)

    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('channelunits',
                        help='Path to the channel units shapefile',
                        type=argparse.FileType('r'))

    parser.add_argument('thalweg',
                        help='Path to Thalweg shapefile',
                        type=argparse.FileType('r'))

    parser.add_argument('depth',
                        help='Path to depth raster',
                        type=argparse.FileType('r'))

    parser.add_argument('sitelength',
                        help='Site Length',
                        type=float)

    args = parser.parse_args()

    if not args.channelunits or not args.thalweg or not args.depth:
        print("ERROR: Missing arguments")
        parser.print_help()
        exit(0)

    dUnits = {}
    dUnits[1] = ('Slow/Pool', 'Scour Pool', 1)
    dUnits[2] = ('Fast - Turbulent', 'Riffle', 1)
    dUnits[3] = ('Fast - Turbulent', 'Riffle', 1)
    dUnits[4] = ('Small Side Channel', 'Small Side Channel', 1)
    dUnits[5] = ('Slow/Pool', 'Scour Pool', 1)
    dUnits[6] = ('Fast - Turbulent', 'Rapid', 1)
    dUnits[7] = ('Slow/Pool', 'Scour Pool', 1)
    dUnits[8] = ('Fast - Turbulent', 'Riffle', 1)
    dUnits[9] = ('Small Side Channel', 'Small Side Channel', 2)
    dUnits[10] = ('Slow/Pool', 'Scour Pool', 1)
    dUnits[11] = ('Fast - Turbulent', 'Rapid', 1)
    dUnits[12] = ('Slow/Pool', 'Plunge Pool', 1)
    dUnits[13] = ('Fast - Turbulent', 'Rapid', 1)
    dUnits[14] = ('Small Side Channel', 'Small Side Channel', 1)

    try:
        dMetrics = ChannelUnitMetrics(args.channelunits.name, args.thalweg.name, args.depth.name, args.sitelength, dUnits)

    except AssertionError as e:
        sys.exit(0)
    except Exception as e:
        raise
        sys.exit(0)
