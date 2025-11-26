import sys
import os
import argparse
import traceback

from rsxml import Logger
from champ_metrics.lib.exception import DataException, MissingException, NetworkException
from .writetoxml import writeMetricsToXML
from .champ_survey import CHaMPSurvey

__version__ = "0.0.4"


class Status():
    PASS = "Pass"
    FAIL = "Fail"


def validate(topoPath, xmlfile, visitID):
    """
    Validate champ topo data in flat folder structure
    :param topoPath: Full Path to topo data (i.e. GISLayers)
    :return: 0 for success
             1 for all code failures or unhandled problems
             2 for data issues
    """

    returnValue = 0
    log = Logger("Validation")

    survey = CHaMPSurvey()
    survey.load_topo_project(topoPath, visitID)
    validationResults = survey.validate()

    stats = {
        "errors": 0,
        "warnings": 0,
        "nottested": 0,
        "status": Status.PASS,
        "layers": {
        }
    }
    for datasetName, datasetResults in validationResults.items():
        layerstatus = Status.PASS
        for result in datasetResults:
            log.info("[{0:{4}<{5}}] [{1}] [{2}] {3}".format(result["Status"], datasetName, result["TestName"], result["Message"], " ", 10))
            if result["Status"] == "Error":  # or result["Status"] == "NotTested":
                stats["errors"] += 1
                stats["status"] = Status.FAIL
                layerstatus = Status.FAIL
                returnValue = 2
            elif result["Status"] == "NotTested":
                stats["warnings"] += 1
            elif result["Status"] == "Warning":
                stats["nottested"] += 1

        stats['layers'][datasetName] = layerstatus

    if len(validationResults) == 0:
        log.error("No layers found to validate")
        stats["errors"] += 1
        stats["status"] = Status.FAIL
        returnValue = 2

    # The last message is what gets picked up so let's be clever:
    if returnValue == 2:
        log.error("Validation Failed")
    else:
        log.error("Validation Passed")

    writeMetricsToXML(validationResults, stats, xmlfile)

    return returnValue


def main():
    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('visitID', help='Visit ID', type=int)
    parser.add_argument('outputfolder', help='Path to output folder', type=str)
    parser.add_argument('--datafolder', help='(optional) Top level folder containing TopoMetrics Riverscapes projects', type=str)
    parser.add_argument('--verbose', help='Get more information in your logs.', action='store_true', default=False)
    args = parser.parse_args()

    # Make sure the output folder exists
    resultsFolder = os.path.join(args.outputfolder, "outputs")

    # Initiate the log file
    logg = Logger("Program")
    logfile = os.path.join(resultsFolder, "validation.log")
    xmlfile = os.path.join(resultsFolder, "validation.xml")
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

        # If we need to go get our own topodata.zip file and unzip it we do this
        if args.datafolder is None:
            topoDataFolder = os.path.join(args.outputfolder, "inputs")
            fileJSON, projectFolder = downloadUnzipTopo(args.visitID, topoDataFolder)
        # otherwise just pass in a path to existing data
        else:
            projectFolder = args.datafolder

        finalResult = validate(projectFolder, xmlfile, args.visitID)
        sys.exit(finalResult)

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
