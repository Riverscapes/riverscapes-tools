import sys
from os import path
from champ_metrics.lib.shapefileloader import *
from champ_metrics.lib.exception import DataException
from champ_metrics.lib.metrics import CHaMPMetric
from copy import copy
from shapely.geometry import MultiLineString, Point


class CenterlineMetrics(CHaMPMetric):

    TEMPLATE = {
        'ChannelCount': None,
        'MainstemCount': None,
        'MainstemLength': None,
        'MainstemSinuosity': None,
        'SideChannelCount': None,
        'SideChannelLength': None,
        'TotalChannelLength': None,
        'AverageSideChannelLength': None,
        'Braidedness': None,
        'ChannelType': None
    }

    def calc(self, centerline):
        dMetrics = self._CenterlinePartMetrics(centerline)
        self._CenterlineSummaryMetrics(dMetrics)

    def _CenterlinePartMetrics(self, centerline):
        """
        Centerline Part Metrics
        :param centerline:
        :return:
        """
        self.log.info("Loading centerline shapefile: {}".format(centerline))

        clShp = Shapefile(centerline)
        clList = clShp.featuresToShapely()

        dMetrics = {}

        lineIndex = 1
        for aLine in clList:

            if type(aLine['geometry']) is MultiLineString:
                raise DataException('Multipart features in centerline')

            curvedLength = aLine['geometry'].length
            firstPoint = Point(aLine['geometry'].coords[0])
            lastPoint = Point(aLine['geometry'].coords[-1])
            straightLength = firstPoint.distance(lastPoint)

            if straightLength == 0:
                raise DataException("Zero length centerline feature encountered")

            if 'Channel' not in aLine['fields']:
                raise DataException("Centerline 'Channel' field missing")

            if aLine['fields']['Channel'] is None:
                raise DataException("Centerline 'Channel' field contains no data")

            dMetrics[lineIndex] = {}
            dMetrics[lineIndex]['Type'] = aLine['fields']['Channel']
            dMetrics[lineIndex]['Length'] = curvedLength
            dMetrics[lineIndex]['Sinuosity'] = curvedLength / straightLength
            dMetrics[lineIndex]['StraightLength'] = straightLength

            lineIndex += 1

        return dMetrics

    def _CenterlineSummaryMetrics(self, dMetrics):
        """

        :param dMetrics:
        :return:
        """
        lMainParts = [x for x in dMetrics.values() if x['Type'] == 'Main']
        lSideParts = [x for x in dMetrics.values() if x['Type'] != 'Main']

        if len(lMainParts) < 1:
            raise DataException("Zero number of mainstem channel parts.")

        self.metrics['ChannelCount'] = len([x for x in dMetrics.values()])
        self.metrics['MainstemCount'] = len(lMainParts)
        self.metrics['SideChannelCount'] = len(lSideParts)
        self.metrics['MainstemLength'] = sum([fLen['Length'] for fLen in lMainParts])

        # Let's avoid some divisions by zero
        self.metrics['MainstemSinuosity'] = self.metrics['MainstemLength'] / sum([fLen['StraightLength'] for fLen in lMainParts])

        self.metrics['SideChannelLength'] = sum([fLen['Length'] for fLen in lSideParts])
        self.metrics['TotalChannelLength'] = self.metrics['MainstemLength'] + self.metrics['SideChannelLength']

        if self.metrics['ChannelCount'] > 0:
            self.metrics['AverageSideChannelLength'] = self.metrics['TotalChannelLength'] / self.metrics['ChannelCount']

        if self.metrics['MainstemLength'] > 0:
            self.metrics['Braidedness'] = (self.metrics['MainstemLength'] + self.metrics['SideChannelLength']) / self.metrics['MainstemLength']

        if len(dMetrics) > 1:
            self.metrics['ChannelType'] = 'complex'
        else:
            self.metrics['ChannelType'] = 'simple'


if __name__ == "__main__":
    import argparse
    import logging
    logfmt = "[%(asctime)s - %(levelname)s] - %(message)s"

    dtfmt = "%Y-%m-%d %I:%M:%S"
    logging.basicConfig(filename='centerline_metrics.log', level=logging.DEBUG, format=logfmt, datefmt=dtfmt)

    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('centerline',
                        help='Path to the centerline shapefile',
                        type=argparse.FileType('r'))
    args = parser.parse_args()

    if not args.centerline:
        logging.error("ERROR: Missing arguments")
        parser.print_help()
        exit(0)

    try:
        dMetricsObj = CenterlineMetrics(args.centerline.name)

    except AssertionError as e:
        sys.exit(0)
    except Exception as e:
        raise
        sys.exit(0)
