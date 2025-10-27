import sys
import os
import argparse
import traceback

from rscommons import Logger
from rscommons.util import safe_makedirs
import champ_metrics.lib.env
from champ_metrics.lib.exception import DataException, MissingException, NetworkException
from champ_metrics.lib.metricxmloutput import writeMetricsToXML
from .metriclib.auxMetrics import calculateMetricsForVisit, calculateMetricsForChannelUnitSummary, calculateMetricsForTier1Summary, calculateMetricsForStructureSummary
# from .metriclib.fishMetrics import *


__version__ = "0.0.4"


def runAuxMetrics(xmlfile: str, visit_id: int, data_folder: str, results_folder: str) -> None:
    """Run the auxmetrics for a given visit and write them to an XML file."""

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

    # Populate our measurements from the Aux JSON files.
    # This used to call the Sitka API to get this information.
    for key, url in measurekeys.items():
        try:
            visitobj[key] = APIGet("visits/{0}/measurements/{1}".format(visit_id, url))
        except MissingException as e:
            visitobj[key] = None

    # Metric calculations
    visitMetrics = calculateMetricsForVisit(visitobj)
    channelUnitMetrics = calculateMetricsForChannelUnitSummary(visitobj)
    tier1Metrics = calculateMetricsForTier1Summary(visitobj)
    structureMetrics = calculateMetricsForStructureSummary(visitobj)

    # write these files
    # dMetricsArg, visitID, sourceDir, xmlFilePath, modelEngineRootNode, modelVersion
    log.info(f'Writing Metrics for Visit {visit_id} XML File')
    writeMetricsToXML({
        "VisitMetrics": visitMetrics,
        "ChannelUnitMetrics": channelUnitMetrics,
        "Tier1Metrics": tier1Metrics,
        "StructureMetrics": structureMetrics
    }, visit_id, "", xmlfile, "AuxMetrics", __version__)


def main():
    """Main function for running auxmetrics from the command line."""

    parser = argparse.ArgumentParser()
    parser.add_argument('visit_id', help='Visit ID', type=int)
    parser.add_argument('outputfolder', help='Path to output folder', type=str)
    parser.add_argument('data_folder', help='Top level folder containing topo riverscapes projects', type=str)
    parser.add_argument('--verbose', help='Get more information in your logs.', action='store_true', default=False)
    args = parser.parse_args()

    # Make sure the output folder exists
    resultsFolder = os.path.join(args.outputfolder, "outputs")
    safe_makedirs(resultsFolder)

    # Initiate the log file
    log = Logger("Aux Metrics")
    logfile = os.path.join(resultsFolder, "aux_metrics.log")
    xmlfile = os.path.join(resultsFolder, "aux_metrics.xml")
    log.setup(logPath=logfile, verbose=args.verbose)

    try:
        runAuxMetrics(xmlfile, args.visit_id, args.data_folder, resultsFolder)

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
