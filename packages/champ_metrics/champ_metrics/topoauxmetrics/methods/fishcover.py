from champ_metrics.lib.exception import MissingException
from champ_metrics.lib.sitkaAPI import latestMetricInstances
from champ_metrics.lib.metrics import CHaMPMetric
from champ_metrics.topoauxmetrics.methods.metricsbychannelunit import metricsByChannelUnit, emptiesByChannelUnit


class FishcoverMetrics(CHaMPMetric):

    TEMPLATE = {}

    # Definitions of names keyed to the API measurements# that comprise each metric.
    # Note that the code that does the calculation expects the API measurement names
    # in a list because some metric types incorporate multiple API measurements.
    fishCoverClasses = {
        'LW': (['LWDFC'], True),
        'TVeg': (['VegetationFC'], True),
        'Ucut': (['UndercutBanksFC'], True),
        'Art': (['ArtificialFC'], True),
        'AqVeg': (['AquaticVegetationFC'], True),
        'None': (['TotalNoFC'], False)
    }

    def __init__(self, apiData):
        if FishcoverMetrics.TEMPLATE == {}:
            FishcoverMetrics.TEMPLATE = emptiesByChannelUnit(FishcoverMetrics.fishCoverClasses)
        super(FishcoverMetrics, self).__init__(apiData)

    def calc(self, apiData):
        """
        Calculate fish cover metrics
        :param apiData: dictionary of API data. Key is API call name. Value is API data
        :return: metrics dictionary
        """

        self.log.info("Running Fish Cover Metrics")

        if 'FishCover' not in apiData:
            raise MissingException("FishCover missing from apiData")

        # Retrieve the undercut API data
        fishCoverVals = [val['value'] for val in apiData['FishCover']['value']]

        if 'ChannelUnits' not in apiData['TopoVisitMetrics']:
            raise MissingException('Missing channel unit instances')

        # Retrieve the channel unit metrics
        # channelInstances = latestMetricInstances(apiData['ChannelUnitMetrics'])
        channelInstances = apiData['TopoVisitMetrics']['ChannelUnits']
        channelUnitMeasurements = apiData['ChannelUnitMeasurements']
        if channelInstances is None:
            raise MissingException('Missing channel unit metric instances')

        # calculate metrics
        self.metrics = metricsByChannelUnit(FishcoverMetrics.fishCoverClasses, channelInstances, fishCoverVals, channelUnitMeasurements)
