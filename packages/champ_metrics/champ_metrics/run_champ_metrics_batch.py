""" CHaMP Metrics

Philip Bailey
9 Oct 2025
"""
import argparse
import os
import traceback
import sys
import xml.etree.ElementTree as ET
from rsxml import Logger, dotenv
from champ_metrics.run_champ_metrics import run_champ_metrics
from champ_metrics.scripts.load_project_guids_from_csv import load_project_guids_from_csv


def champ_metrics_batch(csv_dir: str, projects_dir: str, output_dir: str):
    """Process each of the different kind of metrics.

    Note that in both directry arguments are parent folders. The code will append
    the visit ID subfolder to find the project.rs.xml file in the projects_dir.

    :param csv_dir: Directory containing CSV files with project GUIDs to process
    :param projects_dir: Local parent folder where the topo projects are already downloaded
    :param output_folder: Parent folder where output metric XML files will be saved
    """

    log = Logger('CHaMP Metrics Batch')

    # Load the project GUIDs from the CSV file in the specified folder
    project_guids = load_project_guids_from_csv(csv_dir)
    log.info(f'{len(project_guids)} project GUIDs loaded from CSV files from directory: {csv_dir}')

    # Loop over the projects and find the project.rs.xml file in each subfolder of projects_dir
    project_xmls = []
    for guid in project_guids:
        project_xml = os.path.join(projects_dir, guid, 'project.rs.xml')
        if os.path.isfile(project_xml):
            project_xmls.append((guid, project_xml))
        else:
            log.warning(f'No project.rs.xml file found for Visit ID {guid} in folder {os.path.join(projects_dir, str(guid))}. Skipping this visit.')

    log.info(f'{len(project_xmls)} project.rs.xml files found for processing.')

    # Determine the visit IDs from the project XML files and process each one
    # Load the project XML into elementree and find the visit ID metadata item
    visit_ids = {}
    for guid, project_xml in project_xmls:
        tree = ET.parse(project_xml)
        root = tree.getroot()
        nod_visit = root.find('MetaData/Meta[@name="Visit"]')
        if nod_visit is not None and nod_visit.text is not None:
            visit_id = int(nod_visit.text)
            visit_ids[visit_id] = project_xml
        else:
            log.error(f'No Visit ID found in project XML: {project_xml}. Skipping this visit.')
            continue

    log.info(f'{len(visit_ids)} visit IDs retrieved from project.rs.xml files.')

    for visit_id, project_xml in visit_ids.items():
        log.info(f'Processing Visit ID {visit_id} with project file {project_xml}')
        try:
            run_champ_metrics(project_xml, output_dir)
        except Exception as e:
            log.error(f'Error processing Visit ID {visit_id}: {e}')
            continue

    log.info('Finished processing all visits.')


def main():
    """Parse CHaMP Metrics command line arguments and call the main processing function."""

    args = argparse.ArgumentParser(description='CHaMP Metrics')
    args.add_argument('csv_dir', help='Directory containing CSV files with project GUIDs to process', type=str)
    args.add_argument('topo_project_parent_dir', help='Local parent folder where the topo projects are already downloaded', type=str)
    args.add_argument('output_folder', help='Parent folder where output metric XML files will be saved', type=str)
    args.add_argument('--verbose', help='Get more information in your logs.', action='store_true', default=False)
    args.add_argument('--debug', help='(optional) more output about thigs like memory usage. There is a performance cost', action='store_true', default=False)
    args = dotenv.parse_args_env(args)

    log = Logger('CHaMP Metrics')
    log.setup(logPath=os.path.join(args.output_folder, "champ_metrics.log"), verbose=args.verbose)

    try:
        champ_metrics_batch(args.csv_dir, args.topo_project_parent_dir, args.output_folder)
    except Exception as ex:
        log.error(ex)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
