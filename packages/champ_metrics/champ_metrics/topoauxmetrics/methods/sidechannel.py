from champmetrics.lib.exception import MissingException
from champmetrics.lib.sitkaAPI import latestMetricInstances
from champmetrics.lib.metrics import CHaMPMetric
import numpy as np

class SidechannelMetrics(CHaMPMetric):

    TEMPLATE = {
        'VisitMetrics': {
            'Area': None,
            'AreaPercent': None,
        }
    }

    def calc(self, apiData):

        """
        Calculate side channel metrics
        :param apiData: dictionary of API data. Key is API call name. Value is API data
        :return: metrics dictionary
        """
        self.log.info("Running SideChannelMetrics")

        if 'ChannelSegments' not in apiData:
            raise MissingException("ChannelSegments missing from apiData")


        # Retrieve the channel segment measurements
        channelSegmentVals = [val['value'] for val in apiData['ChannelSegments']['values']]

        if 'ChannelUnitMetrics' not in apiData:
            raise MissingException('Missing channel metric instances')

        # Retrieve the channel unit metrics
        channelInstances = latestMetricInstances(apiData['ChannelUnitMetrics'])
        if channelInstances is None:
            raise MissingException('Missing channel unit metric instances')

        # calculate metrics
        self.metrics = self._calc(channelSegmentVals, channelInstances)

    @staticmethod
    def _calc(channelSegmentVals, channelInstances):
        """
        Calculate side channel metrics
        :param channelInstances: dictionary of channel unit topo metrics
        :return: metrics dictionary
        """

        # Total area of all channel units
        totalArea = np.sum([val['AreaTotal'] for val in channelInstances])

        dResults = {}

        # Filter channel segments to just small side channels with both length and width
        sscWithMeasurements = [val for val in channelSegmentVals
                               if val['SegmentType'] == 'Small Side Channel' and val['SideChannelLengthM'] and val['SideChannelWidthM']]

        # Sum the length and widths of filtered small side channels
        dResults['Area'] = np.sum([val['SideChannelLengthM'] * val['SideChannelWidthM'] for val in sscWithMeasurements])

        if dResults['Area'] == 0.0 and totalArea == 0.0:
            dResults['AreaPercent'] = 0.0
        else:
            dResults['AreaPercent'] = 100 * dResults['Area'] / totalArea

        visitMetrics = {"VisitMetrics": dResults}
        return visitMetrics
