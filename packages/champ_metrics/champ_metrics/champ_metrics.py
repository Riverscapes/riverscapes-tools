""" CHaMP Metrics

Philip Bailey
9 Oct 2025
"""
import argparse
import os
import traceback
import sys

from rscommons import Logger, initGDALOGRErrors, ModelConfig, dotenv
from rscommons.util import safe_makedirs
from champ_metrics.topometrics.topometrics import visit_topo_metrics
from champ_metrics.__version__ import __version__


initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)


def champ_metrics(visit_id: int, workbench_db: str, topo_project_xml: str, output_folder: str):
    """Process each of the different kind of metrics."""

    log = Logger('CHaMP Metrics')
    log.info(f'Calculating CHaMP Metrics for Visit ID: {visit_id}')
    log.info(f'Using workbench DB: {workbench_db}')
    log.info(f'Using topo project XML: {topo_project_xml}')
    log.info(f'Output folder: {output_folder}')

    if not os.path.isfile(workbench_db):
        log.error(f'Workbench DB not found: {workbench_db}')
        sys.exit(1)

    if not os.path.isfile(topo_project_xml):
        log.error(f'Topo project XML not found: {topo_project_xml}')
        sys.exit(1)

    # If visit ID is not an integer, exittopo
    try:
        int(visit_id)
    except ValueError:
        log.error(f'Visit ID must be an integer: {visit_id}')
        sys.exit(1)

    safe_makedirs(output_folder)
    metric_xml_path = os.path.join(output_folder, f'CHaMP_Metrics_Visit_{visit_id}.xml')

    # Calculate the topo metrics and write them to XML file
    metrics = visit_topo_metrics(visit_id, topo_project_xml, output_folder, None, workbench_db, None, metric_xml_path)


def main():
    """Parse CHaMP Metrics command line arguments and call the main processing function."""

    args = argparse.ArgumentParser(description='CHaMP Metrics')
    args.add_argument('visit_id', help='Visit ID', type=int)
    args.add_argument('workbench_db', help='SQLite workbench db to load channel units from', type=str)
    args.add_argument('topo_project_xml', help='Local file path to existing topo project', type=str)
    args.add_argument('output_folder', help='Path to output folder', type=str)
    args.add_argument('--verbose', help='Get more information in your logs.', action='store_true', default=False)
    args.add_argument('--debug', help='(optional) more output about thigs like memory usage. There is a performance cost', action='store_true', default=False)

    args = dotenv.parse_args_env(args)

    log = Logger('CHaMP Metrics')
    log.setup(logPath=os.path.join(args.output_folder, "champ_metrics.log"), verbose=args.verbose)

    try:
        champ_metrics(int(args.visit_id), args.workbench_db, args.topo_project_xml, args.output_folder)
    except Exception as ex:
        log.error(ex)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
