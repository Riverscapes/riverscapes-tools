import sys
import os
import argparse
import traceback

from rscommons import Logger, dotenv
from rscommons.util import safe_makedirs

from champ_metrics.auxmetrics.auxmetrics import aux_metrics
from champ_metrics.lib.exception import DataException, MissingException, NetworkException


def main():
    """Main function for running auxmetrics from the command line."""

    args = argparse.ArgumentParser()
    args.add_argument('visit_id', help='Visit ID', type=int)
    args.add_argument('visit_year', help='Visit Year', type=int)
    args.add_argument('visit_data_folder', help='Parent folder containing topo riverscapes projects', type=str)
    args.add_argument('output_folder', help='Path to output XML metrics folder', type=str)
    args.add_argument('--verbose', help='Get more information in your logs.', action='store_true', default=False)
    args = dotenv.parse_args_env(args)

    aux_measurement_dir = os.path.join(args.visit_data_folder, "aux_measurements")

    # Make sure the output folder exists
    resultsFolder = os.path.join(args.output_folder, "outputs")
    safe_makedirs(resultsFolder)

    # Initiate the log file
    log = Logger("Aux Metrics")
    logfile = os.path.join(resultsFolder, "aux_metrics.log")
    log.setup(logPath=logfile, verbose=args.verbose)

    xmlfile = os.path.join(resultsFolder, "aux_metrics.xml")

    try:
        aux_metrics(xmlfile, args.visit_id, args.visit_year, aux_measurement_dir, args.output_folder)

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
