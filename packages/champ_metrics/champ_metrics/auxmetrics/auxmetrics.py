import sys
import os

import champmetrics.lib.env
from champmetrics.lib.loghelper import Logger
from champmetrics.lib.sitkaAPI import downloadUnzipTopo, APIGet
import argparse
import traceback
from champmetrics.lib.exception import DataException, MissingException, NetworkException
from champmetrics.lib.metricxmloutput import writeMetricsToXML
import os
import argparse

from .metriclib.auxMetrics import calculateMetricsForVisit, calculateMetricsForChannelUnitSummary, calculateMetricsForTier1Summary, calculateMetricsForStructureSummary
# from .metriclib.fishMetrics import *


__version__ = "0.0.4"

def runAuxMetrics(xmlfile, outputDirectory, visit_id):
    log = Logger("Validation")

    # Make a big object we can pass around
    try:
        visit = APIGet("visits/{}".format(visit_id))
    except MissingException as e:
        raise MissingException("Visit Not Found in API")

    protocol = visit["protocol"]
    iteration = str(visit["iterationID"] + 2010)

    # {key: urlslug} dict
    measurekeys = {
        "snorkelFish": "Snorkel Fish",
        "snorkelFishBinned": "Snorkel Fish Count Binned",
        "snorkelFishSteelheadBinned": "Snorkel Fish Count Steelhead Binned",
        "channelUnits": "Channel Unit",
        "largeWoodyPieces": "Large Woody Piece",
        "largeWoodyDebris": "Large Woody Debris",
        "woodyDebrisJams": "Woody Debris Jam",
        "jamHasChannelUnits": "Jam Has Channel Unit",
        "riparianStructures": "Riparian Structure",
        "pebbles": "Pebble",
        "pebbleCrossSections": "Pebble Cross-Section",
        "channelConstraints": "Channel Constraints",
        "channelConstraintMeasurements": "Channel Constraint Measurements",
        "bankfullWidths": "Bankfull Width",
        "driftInverts": "Drift Invertebrate Sample",
        "driftInvertResults": "Drift Invertebrate Sample Results",
        "sampleBiomasses": "Sample Biomasses",
        "undercutBanks": "Undercut Banks",
        "solarInputMeasurements": "Daily Solar Access Meas",
        "discharge": "Discharge",
        "waterChemistry": "Water Chemistry",
        "poolTailFines": "Pool Tail Fines",
    }

    visitobj = {
        "visit_id": visit_id,
        "visit": APIGet("visits/{}".format(visit_id)),
        "iteration": iteration,
        "protocol": protocol,
    }

    log.info("Visit " + str(visit_id) + " - " + protocol + ": " + iteration)

    # Populate our measurements from the API
    for key,url in measurekeys.items():
        try:
            visitobj[key] = APIGet("visits/{0}/measurements/{1}".format(visit_id, url))
        except MissingException as e:
            visitobj[key] = None

    log.info("Writing Metrics for Visit {0} XML File".format(visit_id))

    # do metric calcs
    visitMetrics = calculateMetricsForVisit(visitobj)
    channelUnitMetrics = calculateMetricsForChannelUnitSummary(visitobj)
    tier1Metrics = calculateMetricsForTier1Summary(visitobj)
    structureMetrics = calculateMetricsForStructureSummary(visitobj)

    # write these files
    # dMetricsArg, visitID, sourceDir, xmlFilePath, modelEngineRootNode, modelVersion
    writeMetricsToXML({
        "VisitMetrics": visitMetrics,
        "ChannelUnitMetrics": channelUnitMetrics,
        "Tier1Metrics": tier1Metrics,
        "StructureMetrics": structureMetrics
    }, visit_id, "", xmlfile, "AuxMetrics", __version__)


def main():
    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('visitID', help='Visit ID', type=int)
    parser.add_argument('outputfolder', help='Path to output folder', type=str)
    parser.add_argument('--datafolder', help='(optional) Top level folder containing TopoMetrics Riverscapes projects', type=str)
    parser.add_argument('--verbose', help='Get more information in your logs.', action='store_true', default=False )
    args = parser.parse_args()

    # Make sure the output folder exists
    resultsFolder = os.path.join(args.outputfolder, "outputs")

    # Initiate the log file
    logg = Logger("Program")
    logfile = os.path.join(resultsFolder, "aux_metrics.log")
    xmlfile = os.path.join(resultsFolder, "aux_metrics.xml")
    logg.setup(logPath=logfile, verbose=args.verbose)

    # Initiate the log file
    log = Logger("Program")
    log.setup(logPath=logfile, verbose=args.verbose)

    try:
        if not os.path.isdir(resultsFolder):
            os.makedirs(resultsFolder)

        runAuxMetrics(xmlfile, resultsFolder, args.visitID)

    except (DataException, MissingException, NetworkException) as e:
        # Exception class prints the relevant information
        traceback.print_exc(file=sys.stdout)
        sys.exit(e.returncode)
    except AssertionError as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()
