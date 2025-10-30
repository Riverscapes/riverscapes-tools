import argparse
import sys
import traceback
import os
from champ_metrics.lib.loghelper import Logger
from champ_metrics.lib.exception import DataException, MissingException, NetworkException
from champ_metrics.lib.metricxmloutput import writeMetricsToXML
from champ_metrics.lib.metricxmloutput import integrateMetricDictionary
from champ_metrics.lib.sitkaAPI import APIGet
import champ_metrics.lib.env
from .methods.undercut import UndercutMetrics
from .methods.substrate import SubstrateMetrics
from .methods.sidechannel import SidechannelMetrics
from .methods.fishcover import FishcoverMetrics
from .methods.largewood import LargeWoodMetrics
from champ_metrics.lib.channelunits import dUnitDefs
from champ_metrics.lib.channelunits import getCleanTierName
from champ_metrics.topoauxmetrics.topoauxmetrics import visit_topo_aux_metrics

__version__ = "0.0.4"


def topo_aux_metrics(visitID, metricXMLPath):
    return visit_topo_aux_metrics(visitID, metricXMLPath)


def main():
    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('visitID', help='Visit ID', type=int)
    parser.add_argument('outputfolder', help='Path to output folder', type=str)
    parser.add_argument('--verbose', help='Get more information in your logs.', action='store_true', default=False)
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

        visit_topo_aux_metrics(args.visitID, xmlfile)

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
