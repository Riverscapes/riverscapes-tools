from champmetrics.lib.sitkaAPI import latestMetricInstances
from metricsbychannelunit import metricsByChannelUnit, emptiesByChannelUnit
from champmetrics.lib.exception import MissingException
from champmetrics.lib.metrics import CHaMPMetric


class SubstrateMetrics(CHaMPMetric):

    TEMPLATE = {}

    # Definitions of names keyed to the API measurements# that comprise each metric.
    # Note that the code that does the calculation expects the API measurement names
    # in a list because some metric types incorporate multiple API measurements.
    dSubstrateClasses = {
        'Bldr': (['Boulders'], True),
        'Cbl': (['Cobbles'], True),
        'Grvl': (['CourseGravel', 'FineGravel'], True),
        'SandFines': (['Fines', 'Sand'], True)
    }

    def __init__(self, apiData):
        if SubstrateMetrics.TEMPLATE == {}:
            SubstrateMetrics.TEMPLATE = emptiesByChannelUnit(SubstrateMetrics.dSubstrateClasses)
        super(SubstrateMetrics, self).__init__(apiData)

    def calc(self, apiData):

        """
        Calculate substrate metrics
        :param apiData: dictionary of API data. Key is API call name. Value is API data
        :return: metrics dictionary
        """

        self.log.info("Running Substrate Metrics")

        if 'SubstrateCover' not in apiData:
            raise MissingException("SubstrateCover missing from apiData")

        # Retrieve the undercut API data
        substrateCoverVals = [val['value'] for val in apiData['SubstrateCover']['values'] ]

        if 'ChannelUnitMetrics' not in apiData:
            raise MissingException('Missing channel metric instances')

        # Retrieve the channel unit metrics
        channelInstances = latestMetricInstances(apiData['ChannelUnitMetrics'])
        channelUnitMeasurements = apiData['ChannelUnitMeasurements']
        if channelInstances is None:
            raise MissingException('Missing channel metric instances')

        # calculate metrics
        self.metrics = metricsByChannelUnit(SubstrateMetrics.dSubstrateClasses, channelInstances, substrateCoverVals, channelUnitMeasurements)
