from typing import Tuple
import argparse
import os
import traceback
import sys
import xml.etree.ElementTree as ET

from rscommons import Logger, initGDALOGRErrors, dotenv
from champ_metrics.__version__ import __version__
from champ_metrics.topometrics.topometrics import visit_topo_metrics
from champ_metrics.auxmetrics.auxmetrics import visit_aux_metrics
from champ_metrics.topoauxmetrics.topoauxmetrics import visit_topo_aux_metrics

initGDALOGRErrors()


def run_champ_metrics(topo_project_xml_path: str, output_dir: str) -> None:
    """
    Run CHaMP topo, aux and topo+aux metrics for a given topo project
    :param topo_project: Path to the topo project.rs.xml file
    :param output_folder: Top level folder where results will be written.
        The code will create visit folder inside this.
    """

    log = Logger('CHaMP Metrics')
    log.info(f'CHaMP Metrics Version: {__version__}')
    log.info(f'Loading topo project from: {topo_project_xml_path}')

    # Load visit info from the topo project XML
    watershed, site, visit_id, visit_year = load_visit_info(topo_project_xml_path)

    log.info(f'Watershed: {watershed}')
    log.info(f'Site: {site}')
    log.info(f'Visit ID: {visit_id}')
    log.info(f'Year: {visit_year}')

    # Zero pad the visit ID to four characters for folder naming

    visit_output_dir = os.path.join(output_dir, str(visit_id).zfill(4))
    topo_metric_xml = os.path.join(visit_output_dir, 'topo_metrics.xml')
    aux_metric_xml = os.path.join(visit_output_dir, 'aux_metrics.xml')
    topo_aux_metric_xml = os.path.join(visit_output_dir, 'topo_aux_metrics.xml')
    aux_data_folder = os.path.join(os.path.dirname(topo_project_xml_path), 'aux_measurements')

    topo_metrics = visit_topo_metrics(visit_id, topo_project_xml_path, topo_metric_xml)
    aux_metrics = visit_aux_metrics(visit_id, visit_year, aux_data_folder, aux_metric_xml)

    visit_topo_aux_metrics(visit_id, topo_metrics, aux_metrics, topo_aux_metric_xml)

    log.info('CHaMP Metrics processing complete.')


def load_visit_info(topo_project_xml_path: str) -> Tuple[str, str, int, int]:
    """
    Load the watershed, site, visit ID, and year from the topo project XML file.
    """

    if not os.path.isfile(topo_project_xml_path):
        raise FileNotFoundError(f'Topo project XML not found: {topo_project_xml_path}')

    tree = ET.parse(topo_project_xml_path)
    root = tree.getroot()

    return (
        get_metadata_item(root, 'Watershed', False),
        get_metadata_item(root, 'Site', False),
        int(get_metadata_item(root, 'Visit', True)),
        int(get_metadata_item(root, 'Year', True)),
    )


def get_metadata_item(nod_parent: ET.Element, item_name: str, is_mandatory: bool) -> str:
    """
    Extract a metadata item from the topo project.rs.xml project level Meta data.
    """

    nod_item = nod_parent.find(f'MetaData/Meta[@name="{item_name}"]')
    if nod_item is not None and nod_item.text is not None:
        return nod_item.text
    elif is_mandatory:
        raise ValueError(f'Mandatory metadata item not found: {item_name}')

    return None


def main():
    """
    Main function to run CHaMP topo, aux and topo+aux metrics from the command line.
    """
    args = argparse.ArgumentParser(description='CHaMP Metrics')
    args.add_argument('topo_project_xml', help='Local file path to existing topo project', type=str)
    args.add_argument('output_folder', help='Path to output folder', type=str)
    args.add_argument('--verbose', help='Get more information in your logs.', action='store_true', default=False)
    args.add_argument('--debug', help='(optional) more output about thigs like memory usage. There is a performance cost', action='store_true', default=False)
    args = dotenv.parse_args_env(args)

    log = Logger('CHaMP Metrics')
    log.setup(logPath=os.path.join(args.output_folder, "champ_metrics.log"), verbose=args.verbose)

    try:
        run_champ_metrics(args.topo_project_xml, args.output_folder)
    except Exception as ex:
        log.error(ex)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
