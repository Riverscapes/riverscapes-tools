import argparse
import sys, traceback
import os
from champmetrics.lib.loghelper import Logger
from champmetrics.lib.exception import DataException, MissingException, NetworkException
from champmetrics.lib.metricxmloutput import writeMetricsToXML
from champmetrics.lib.metricxmloutput import integrateMetricDictionary
from champmetrics.lib.sitkaAPI import APIGet
import champmetrics.lib.env
from .methods.undercut import UndercutMetrics
from .methods.substrate import SubstrateMetrics
from .methods.sidechannel import SidechannelMetrics
from .methods.fishcover import FishcoverMetrics
from .methods.largewood import LargeWoodMetrics
from champmetrics.lib.channelunits import dUnitDefs
from champmetrics.lib.channelunits import getCleanTierName

__version__ = "0.0.4"

# List of API calls needed to support Topo+Aux metrics
apiCalls = {
    # 'Pebble' : 'measurements/Pebble',
    # 'PebbleXS' : 'measurements/Pebble Cross-Section',
    # 'RiparianStructure' : 'measurements/Riparian Structure',
    # 'TopoTier1Metrics' : 'metricschemas/QA - Topo Tier 1 Metrics/metrics',
    'LargeWoodyDebris': 'measurements/Large Woody Debris',
    'LargeWoodyPiece': 'measurements/Large Woody Piece',
    'WoodyDebrisJam' : 'measurements/Woody Debris Jam',
    'VisitDetails' : '',
    'TopoVisitMetrics' : 'metricschemas/QA - Topo Visit Metrics/metrics',
    'ChannelUnitMetrics' : 'metricschemas/QA - Topo Channel Metrics/metrics',
    'ChannelUnitMeasurements': 'measurements/Channel Unit',
    'ChannelSegments' : 'measurements/Channel Segment',
    'FishCover' : 'measurements/Fish Cover',
    'SubstrateCover' : 'measurements/Substrate Cover',
    'UndercutBanks' : 'measurements/Undercut Banks'
}

def visitTopoAuxMetrics(visitID, metricXMLPath):

    log = Logger('Metrics')
    log.info("Topo aux metrics for visit {0}".format(visitID))

    # Make all the API calls and return a dictionary of API call name keyed to data
    apiData = downloadAPIData(visitID)

    # Dictionary to hold the metric values
    visitMetrics = {}

    metric_uc = UndercutMetrics(apiData)
    integrateMetricDictionaryWithTopLevelType(visitMetrics, 'Undercut', metric_uc.metrics )

    metrics_su = SubstrateMetrics(apiData)
    integrateMetricDictionaryWithTopLevelType(visitMetrics, 'Substrate', metrics_su.metrics)

    metrics_si = SidechannelMetrics(apiData)
    integrateMetricDictionaryWithTopLevelType(visitMetrics, 'SideChannel', metrics_si.metrics)

    metrics_fi = FishcoverMetrics(apiData)
    integrateMetricDictionaryWithTopLevelType(visitMetrics, 'FishCover', metrics_fi.metrics)

    metrics_wo = LargeWoodMetrics(apiData)
    integrateMetricDictionaryWithTopLevelType(visitMetrics, 'LargeWood', metrics_wo.metrics)

    # Metric calculation complete. Write the topometrics to the XML file
    writeMetricsToXML(visitMetrics, visitID, '', metricXMLPath, 'TopoAuxMetrics', __version__)

    log.info("Metric calculation complete for visit {0}".format(visitID))
    return visitMetrics

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
                topo_metrics['Tier1Metrics'][safet1Type] = {'Name' : t1Type}

            integrateMetricDictionary(topo_metrics['Tier1Metrics'][safet1Type], prefix, newCollection['Tier1Metrics'][safet1Type])

def downloadAPIData(visitID):

    apiData = {}
    for name, URL in apiCalls.items():
        try:
            apiData[name] = APIGet('visits/{0}/{1}'.format(visitID, URL))
        except MissingException as e:
            pass
            # if not (name == 'LargeWoodyDebris' or name == 'LargeWoodyPiece' or name== 'WoodyDebrisJam'):
            #     raise MissingException("Missing API Data {}".format(URL))

    # if 'LargeWoodyDebris' not in apiData and 'LargeWoodyPiece' not in apiData:
    #     raise MissingException('Missing large wood API data')

    return apiData

def main():
    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('visitID', help='Visit ID', type=int)
    parser.add_argument('outputfolder', help='Path to output folder', type=str)
    parser.add_argument('--verbose', help='Get more information in your logs.', action='store_true', default=False )
    args = parser.parse_args()

    # Make sure the output folder exists
    resultsFolder = os.path.join(args.outputfolder, "outputs")

    # Initiate the log file
    logg = Logger("Program")
    logfile = os.path.join(resultsFolder, "topo_aux_metrics.log")
    xmlfile = os.path.join(resultsFolder, "topo_aux_metrics.xml")
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

        visitTopoAuxMetrics(args.visitID, xmlfile)

    except (DataException, MissingException, NetworkException) as e:
        # Exception class prints the relevant information
        traceback.print_exc(file=sys.stdout)
        sys.exit(e.returncode)
    except AssertionError as e:
        logg.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
    except Exception as e:
        logg.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()