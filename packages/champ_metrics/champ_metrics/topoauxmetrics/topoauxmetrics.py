from rsxml import Logger
from champ_metrics.lib.exception import MissingException
from champ_metrics.lib.metricxmloutput import writeMetricsToXML
from champ_metrics.lib.metricxmloutput import integrateMetricDictionary
from champ_metrics.lib.channelunits import dUnitDefs
from champ_metrics.lib.channelunits import getCleanTierName
from .methods.undercut import UndercutMetrics
from .methods.substrate import SubstrateMetrics
from .methods.sidechannel import SidechannelMetrics
from .methods.fishcover import FishcoverMetrics
from .methods.largewood import LargeWoodMetrics

__version__ = "0.0.4"

# List of API calls needed to support Topo+Aux metrics
apiCalls = {
    # 'Pebble' : 'measurements/Pebble',
    # 'PebbleXS' : 'measurements/Pebble Cross-Section',
    # 'RiparianStructure' : 'measurements/Riparian Structure',
    # 'TopoTier1Metrics' : 'metricschemas/QA - Topo Tier 1 Metrics/metrics',
    'LargeWoodyDebris': 'measurements/Large Woody Debris',
    'LargeWoodyPiece': 'measurements/Large Woody Pieces',
    'WoodyDebrisJam': 'measurements/Woody Debris Jams',
    'VisitDetails': '',
    'TopoVisitMetrics': 'metricschemas/QA - Topo Visit Metrics/metrics',
    'ChannelUnitMetrics': 'metricschemas/QA - Topo Channel Metrics/metrics',
    'ChannelUnitMeasurements': 'measurements/Channel Units',
    'ChannelSegments': 'measurements/Channel Segments',
    'FishCover': 'measurements/Fish Cover',
    'SubstrateCover': 'measurements/Substrate Cover',
    'UndercutBanks': 'measurements/Undercut Banks'
}


def visit_topo_aux_metrics(visit_id: int, topo_metrics: dict, aux_metrics: dict, metricXMLPath: str) -> dict:
    """
    Calculate TopoAux metrics for a given visit

    NOTE: the aux_metrics dictionary contains the aux measurements

    :param visit_id: Visit ID
    :param topo_metrics: Dictionary of topo metrics data
    :param aux_metrics: Dictionary of auxiliary metrics data
    :param metricXMLPath: Path to output metric XML file
    :return: Dictionary of calculated TopoAux metrics"""

    log = Logger('TopoAux Metrics')
    log.info(f'Topo aux metrics for visit {visit_id}')

    # Make all the API calls and return a dictionary of API call name keyed to data
    # apiData = downloadAPIData(visit_id)

    # Instead of calling API, retrieve the data from the passed dictionaries
    apiData = {
        'VisitYear': aux_metrics['VisitMetrics']['VisitYear'],
    }
    for key, api_path in apiCalls.items():

        if 'measurements' in api_path:
            actual_key = api_path.replace('measurements/', '').replace(' ', '')
            # lower case the first letter to match aux_metrics keys now they are from JSON files and not API
            actual_key = actual_key[0].lower() + actual_key[1:]

            if actual_key in aux_metrics['AuxMeasurements']:
                apiData[key] = aux_metrics['AuxMeasurements'][actual_key]
            else:
                raise MissingException(f"Aux measurement data for {actual_key} not found for visit {visit_id}")
        elif 'Topo Visit Metrics' in api_path:
            apiData[key] = topo_metrics
        elif 'Channel Unit Metrics' in api_path:
            apiData[key] = topo_metrics['ChannelUnits']
        # else:
        #     raise MissingException(f"API path for {key} not recognized for visit {visit_id}")

    # Dictionary to hold the metric values
    visit_metrics = {}

    metric_uc = UndercutMetrics(apiData)
    integrateMetricDictionaryWithTopLevelType(visit_metrics, 'Undercut', metric_uc.metrics)

    metrics_su = SubstrateMetrics(apiData)
    integrateMetricDictionaryWithTopLevelType(visit_metrics, 'Substrate', metrics_su.metrics)

    metrics_si = SidechannelMetrics(apiData)
    integrateMetricDictionaryWithTopLevelType(visit_metrics, 'SideChannel', metrics_si.metrics)

    metrics_fi = FishcoverMetrics(apiData)
    integrateMetricDictionaryWithTopLevelType(visit_metrics, 'FishCover', metrics_fi.metrics)

    metrics_wo = LargeWoodMetrics(apiData)
    integrateMetricDictionaryWithTopLevelType(visit_metrics, 'LargeWood', metrics_wo.metrics)

    # Metric calculation complete. Write the topometrics to the XML file
    writeMetricsToXML(visit_metrics, visit_id, '', metricXMLPath, 'TopoAuxMetrics', __version__)

    log.info(f"Topo aux metric calculation complete for visit {visit_id}")
    return visit_metrics


def integrateMetricDictionaryWithTopLevelType(topo_metrics, prefix, newCollection):
    """
    Integrate a dictionary of metrics into an existing dictionary.

    Note that this differs from the dictionary merging for other metric engines.
    This version looks for
    :param topo_metrics:
    :param prefix:
    :param newCollection:
    :return:

    """

    if 'VisitMetrics' in newCollection:
        if not 'VisitMetrics' in topo_metrics:
            topo_metrics['VisitMetrics'] = {}

        integrateMetricDictionary(topo_metrics['VisitMetrics'], prefix, newCollection['VisitMetrics'])

    if 'Tier1Metrics' in newCollection:
        if not 'Tier1Metrics' in topo_metrics:
            topo_metrics['Tier1Metrics'] = {}

        for t1Type in dUnitDefs:
            safet1Type = getCleanTierName(t1Type)

            if not safet1Type in topo_metrics['Tier1Metrics']:
                topo_metrics['Tier1Metrics'][safet1Type] = {'Name': t1Type}

            integrateMetricDictionary(topo_metrics['Tier1Metrics'][safet1Type], prefix, newCollection['Tier1Metrics'][safet1Type])
