"""
This is the version of the CHaMP Metrics that is designed to be used with the Fargate script for
running this tool in Cybercastor. It calls the "run_champ_metrics" function for a single
topo project that must have aux measurements included.

The code runs all three types of CHaMP metrics (topo, aux, and topo+aux) and then adds the 
output metric XML files to the project XML as datasets in a new realization. 
The code also adds a meta tag to the project XML with the date and time of the metric calculation.

Philip Bailey
Feb 2026
"""

import os
import sys
import argparse
import traceback
from datetime import datetime
import argparse
import os
import traceback
import sys
from .__version__ import __version__

from rsxml import Logger, dotenv
from rsxml.project_xml import Project, Realization, Dataset

from champ_metrics.run_champ_metrics import run_champ_metrics

def champ_metrics(topo_project_xml: str) ->None:

    log = Logger('CHaMP Metrics')
    log.info(f'CHaMP Metrics Version: {__version__}')
    log.info(f'Loading topo project from: {topo_project_xml}')

    # Put the output metrics file in a subfolder of the same folder as the project XML
    output_folder = os.path.join(os.path.dirname(topo_project_xml), 'metrics')

    # Run the three types of CHaMP metrics and get the paths to the generated XML files
    topo_xml, aux_xml, topo_aux_xml = run_champ_metrics(topo_project_xml, output_folder)

    # Open the project and add to the project meta, the date and time of the metric calculation.
    project = Project.load_project(topo_project_xml)
    
    if project.meta_data.find_meta('CHaMP Metrics Calculation'):
        project.meta_data.remove_meta('CHaMP Metrics Calculation')
    project.meta_data.add_meta('CHaMP Metrics Calculation', datetime.now().isoformat())

    if project.meta_data.find_meta('CHaMP Metrics Version'):
        project.meta_data.remove_meta('CHaMP Metrics Version')
    project.meta_data.add_meta('CHaMP Metrics Version', __version__)
    
    # Delete any existing CHaMP Metrics realization if it exists
    existing_realization = next((r for r in project.realizations if r.xml_id == 'CHAMP_METRICS'), None)
    if existing_realization:
        project.realizations.remove(existing_realization)

    project.realizations.append(Realization(
        name='CHaMP Metrics',
        xml_id='CHAMP_METRICS',
        date_created=datetime.now(),
        product_version=__version__,
        datasets=[
            Dataset(name='Topo Metrics', xml_id='TOPO_METRICS', ds_type='File', path=os.path.relpath(topo_xml, start=os.path.dirname(topo_project_xml))),
            Dataset(name='Aux Metrics', xml_id='AUX_METRICS', ds_type='File', path=os.path.relpath(aux_xml, start=os.path.dirname(topo_project_xml))),
            Dataset(name='TopoAux Metrics', xml_id='TOPOAUX_METRICS', ds_type='File', path=os.path.relpath(topo_aux_xml, start=os.path.dirname(topo_project_xml)))
        ]
    ))
    
    project.write()
    log.info('CHaMP Metrics processing complete.')


def main():
    """
    Main function to run CHaMP topo, aux and topo+aux metrics from the command line.
    """
    args = argparse.ArgumentParser(description='CHaMP Metrics')
    args.add_argument('topo_project_xml', help='Local file path to existing topo project', type=str)
    args.add_argument('--verbose', help='Get more information in your logs.', action='store_true', default=False)
    args.add_argument('--debug', help='(optional) more output about thigs like memory usage. There is a performance cost', action='store_true', default=False)
    args = dotenv.parse_args_env(args)

    log = Logger('CHaMP Metrics')
    log.setup(log_path=os.path.join(os.path.dirname(args.topo_project_xml), "champ_metrics.log"), verbose=args.verbose)

    try:
        champ_metrics(args.topo_project_xml)
    except Exception as ex:
        log.error(ex)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
