# input is huc list (huc8s)
# download brat projects for each huc
# download qris beaver census projects for each huc
# merge the brat network output
# merge the dams feature classes
# run the capacity validation function
# copy the validation folder from the merged project folder to each of the brat input hucs
# reupload the brat projects with the validation folder
# delete projects for that huc

import argparse
import os
import subprocess
import sys
import traceback
from riverscapes import RiverscapesAPI, RiverscapesSearchParams
from typing import List
import inquirer
from osgeo import ogr

from rscommons.util import safe_makedirs, safe_remove_dir
from rscommons import Logger, dotenv, RSProject, RSMeta, GeopackageLayer, ModelConfig
from rscommons.copy_features import copy_features_fields
from sqlbrat.utils.capacity_validation import validate_capacity
from sqlbrat.__version__ import __version__

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)


def run_validation(huc: int, working_dir: str = '/workspaces/data', upload_tags: str = None):
    """Download the HUC10s within a HUC8 and run the BRAT validation process

    Args:
        huc_list (List): A list of HUC8 watersheds for which beaver activity projects exist
        working_dir (str): The directory in which to store the downloaded files
        upload_tags (str, optional): tags to add to the BRAT projects that are reuploaded. Defaults to None.
    """

    log = Logger("BRAT Capacity Validation")

    download_dir = os.path.join(working_dir, "downloads")
    safe_makedirs(download_dir)
    brat_dir = os.path.join(working_dir, "brat")
    safe_makedirs(brat_dir)
    beav_dir = os.path.join(working_dir, "beaver_activity")
    safe_makedirs(beav_dir)

    riverscapes_api = RiverscapesAPI(stage='production')
    riverscapes_api.refresh_token()

    # download brat projects
    brat_params = RiverscapesSearchParams(
        {
            "projectTypeId": "riverscapes_brat",
            "meta": {
                "HUC": str(huc)
            }})

    to_download = {}
    projects = {}

    for project, _stats, search_total, _prg in riverscapes_api.search(brat_params):
        if project.huc not in projects.keys():
            projects[project.huc] = [project]
        else:
            projects[project.huc].append(project)

    for hucnum, project_list in projects.items():
        if len(project_list) > 1:
            project_list.sort(key=lambda x: x.created_date, reverse=True)
            questions = [
                inquirer.List('selected_project',
                              message=f"Select a project for HUC {hucnum}",
                              choices=[f"{proj.name} (Created: {proj.created_date})" for proj in project_list])
            ]
            answers = inquirer.prompt(questions)
            selected_project = next(proj for proj in project_list if f"{proj.name} (Created: {proj.created_date})" == answers['selected_project'])
            # Use selected_project for further processing
        else:
            selected_project = project_list[0]
            # Use selected_project for further processing
        to_download[hucnum] = selected_project

    log.info('Downloading BRAT projects')
    brat_gpkgs = []
    for hucnum, project in to_download.items():
        dl_dir = os.path.join(download_dir, 'brat', project.huc)
        brat_gpkgs.append(os.path.join(dl_dir, 'outputs', 'brat.gpkg'))
        riverscapes_api.download_files(project.id, dl_dir)

    # download qris beaver census projects
    beaver_params = RiverscapesSearchParams(
        {
            "projectTypeId": "beaver_activity",
            "meta": {
                "HUC": str(huc)
            }})
    to_download = {}
    projects = {}

    for project, _stats, search_total, _prg in riverscapes_api.search(beaver_params):
        if len(project.huc) < 10:
            continue
        if project.huc not in projects.keys():
            projects[project.huc] = [project]
        else:
            projects[project.huc].append(project)

    for hucnum, project_list in projects.items():
        if len(project_list) > 1:
            project_list.sort(key=lambda x: x.created_date, reverse=True)
            questions = [
                inquirer.List('selected_project',
                              message=f"Select a project for HUC {hucnum}",
                              choices=[f"{proj.name} (Created: {proj.created_date})" for proj in project_list])
            ]
            answers = inquirer.prompt(questions)
            selected_project = next(proj for proj in project_list if f"{proj.name} (Created: {proj.created_date})" == answers['selected_project'])
            # Use selected_project for further processing
        else:
            selected_project = project_list[0]
            # Use selected_project for further processing
        to_download[hucnum] = selected_project

    log.info('Downloading Beaver Activity projects')
    num_beaver_gpkgs = {}

    for hucnum, project in to_download.items():
        dl_dir = os.path.join(download_dir, 'beaver_activity', project.huc)
        riverscapes_api.download_files(project.id, dl_dir)
        num_beaver_gpkgs[hucnum] = [os.path.join(dl_dir, f) for f in os.listdir(dl_dir) if f.endswith('.gpkg')]

    # merge projects
    log.info('Merging projects')
    safe_makedirs(os.path.join(brat_dir, str(huc)))
    safe_makedirs(os.path.join(beav_dir, str(huc)))
    out_brat_gpkg = os.path.join(brat_dir, str(huc), 'brat.gpkg')
    # out_beaver_gpkg = os.path.join(qris_dir, huc, 'beaver_activity_1.gpkg')

    for g in brat_gpkgs:
        cmd = f"ogr2ogr -f GPKG -makevalid -append -nln 'vwReaches' {out_brat_gpkg} {g} 'vwReaches'"
        subprocess.run(cmd, shell=True)
        cmd2 = f"ogr2ogr -f GPKG -makevalid -append -nln 'ReachGeometry' {out_brat_gpkg} {g} 'ReachGeometry'"
        subprocess.run(cmd2, shell=True)

    beaver_gpkgs = []
    for hucnum, gpkgs in num_beaver_gpkgs.items():
        if len(gpkgs) > 1:
            beav_questions = [
                inquirer.List('selected gpkg',
                              message=f"Select a beaver activity gpkg for HUC {hucnum}",
                              choices=[f"{f}" for f in gpkgs])
            ]
            beav_answers = inquirer.prompt(beav_questions)
            selected_beaver_gpkg = beav_answers['selected gpkg']
            beaver_gpkgs.append(selected_beaver_gpkg)
        else:
            beaver_gpkgs.append(num_beaver_gpkgs[hucnum][0])

    out_beaver_gpkg = os.path.join(beav_dir, str(huc), 'beaver_activity.gpkg')
    census_dates = []
    for g in beaver_gpkgs:
        cmd = f"ogr2ogr -f GPKG -makevalid -append -nln 'dams' {out_beaver_gpkg} {g} 'dams'"
        subprocess.run(cmd, shell=True)
        proj = RSProject(None, os.path.join(os.path.dirname(g), 'project.rs.xml'))
        gpkg_node = proj.XMLBuilder.root.find('Realizations').find('Realization').find('Outputs').find('Geopackage')
        if gpkg_node.find('Path').text == os.path.basename(g):
            try:
                metas = gpkg_node.find('MetaData').findall('Meta')
                for meta in metas:
                    if meta.attrib['name'] == 'CensusDate':
                        if meta.text not in census_dates:
                            census_dates.append(meta.text)
            except:
                pass

    # run capacity validation
    log.info('Validating BRAT capacity outputs')
    validate_capacity(out_brat_gpkg, out_beaver_gpkg)
    valid_path = os.path.join(os.path.dirname(out_brat_gpkg), 'validation')
    for g in brat_gpkgs:
        # copy the validation folder to the brat projects
        cmd = f"cp -r {valid_path} {os.path.dirname(g)}"
        subprocess.run(cmd, shell=True)

        # copy the realized capacity feature class to the brat projects
        huc_id = None
        with GeopackageLayer(os.path.join(g, 'vwReaches')) as lyr:
            while huc_id is None:
                huc_id = lyr.ogr_layer.GetNextFeature().GetField('WatershedID')

        with GeopackageLayer(g, layer_name='vwCapacity', write=True) as cap_lyr:
            cap_lyr.create_layer(ogr.wkbMultiLineString, epsg=cfg.OUTPUT_EPSG, fields={
                'WatershedID': ogr.OFTString,
                'ReachCode': ogr.OFTInteger,
                'predicted_capacity': ogr.OFTReal,
                'dam_density': ogr.OFTReal,
                'percent_capacity': ogr.OFTReal
            })

        full_fc = os.path.join(out_brat_gpkg, 'vwCapacity')
        copy_fc = os.path.join(g, 'vwCapacity')
        copy_features_fields(full_fc, copy_fc, attribute_filter=f"WatershedID = '{huc_id}'")

        # update project xml
        log.info('Updating project xml file')
        project = RSProject(None, os.path.join(os.path.dirname(os.path.dirname(g)), 'project.rs.xml'))
        outputs_node = project.XMLBuilder.root.find('Realizations').find('Realization').find('Outputs')
        outgpkg_node = outputs_node.find('Geopackage').find('Layers')
        cap_node = project.XMLBuilder.add_sub_element(outgpkg_node, 'Vector', attribs={'lyrName': 'vwCapacity'})
        project.XMLBuilder.add_sub_element(cap_node, 'Name', text='Realized BRAT Capacity')
        newmeta_node = project.XMLBuilder.add_sub_element(cap_node, 'MetaData')
        project.XMLBuilder.add_sub_element(newmeta_node, 'Meta', attribs={'name': 'Description'}, text='Realized BRAT Capacity from Beaver Activity')
        image_node1 = project.XMLBuilder.add_sub_element(outputs_node, 'Image', attribs={'id': 'quantile'})
        project.XMLBuilder.add_sub_element(image_node1, 'Name', text='Quantile Regressions')
        project.XMLBuilder.add_sub_element(image_node1, 'Path', text='outputs/validation/regressions.png')
        image_node2 = project.XMLBuilder.add_sub_element(outputs_node, 'Image', attribs={'id': 'observed'})
        project.XMLBuilder.add_sub_element(image_node2, 'Name', text='Observed vs Predicted')
        project.XMLBuilder.add_sub_element(image_node2, 'Path', text='outputs/validation/obs_v_pred.png')
        csv_node = project.XMLBuilder.add_sub_element(outputs_node, 'CSV', attribs={'id': 'electivity_index'})
        project.XMLBuilder.add_sub_element(csv_node, 'Name', text='Electivity Index')
        project.XMLBuilder.add_sub_element(csv_node, 'Path', text='outputs/validation/electivity_index.csv')
        # meta_node = project.XMLBuilder.root.find('MetaData')
        project.add_metadata([RSMeta('CensusDate', ','.join(census_dates))])
        project.XMLBuilder.write()
        # reupload the brat projects with the validation folder
        if upload_tags:
            cmd2 = f"rscli upload {os.path.dirname(os.path.dirname(g))} --tags {upload_tags} --no-input --no-ui --verbose"
        else:
            cmd2 = f"rscli upload {os.path.dirname(os.path.dirname(g))} --no-input --no-ui --verbose"
        subprocess.run(cmd2, shell=True)

    safe_remove_dir(os.path.join(download_dir, 'brat'))
    safe_remove_dir(os.path.join(download_dir, 'beaver_activity'))
    safe_remove_dir(os.path.join(brat_dir, str(huc)))
    safe_remove_dir(os.path.join(beav_dir, str(huc)))

    log.info('Completed validation for HUC {}'.format(huc))

    return


def main():
    parser = argparse.ArgumentParser(description='Run BRAT capacity validation')
    parser.add_argument('huc', metavar='HUC8', type=int, help='HUC8 watersheds to validate')
    parser.add_argument('working_dir', type=str, default='/workspaces/data', help='Directory to store downloaded files')
    parser.add_argument('--upload_tags', type=str, default=None, help='Tags to add to the reuploaded BRAT projects')

    args = dotenv.parse_args_env(parser)

    log = Logger("BRAT Capacity Validation")
    try:
        run_validation(args.huc, args.working_dir, args.upload_tags)

    except Exception as e:
        log.error(f"Error running capacity validation: {e}")
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
