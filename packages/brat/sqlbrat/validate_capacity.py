# input is huc list (huc8s)
# download brat projects for each huc
# download qris beaver census projects for each huc
# merge the brat network output
# merge the dams feature classes
# run the capacity validation function
# copy the validation folder from the merged project folder to each of the brat input hucs
# reupload the brat projects with the validation folder
# delete projects for that huc

import os
import subprocess
import sys
from riverscapes import RiverscapesAPI, RiverscapesSearchParams
from typing import List
import inquirer

from rscommons.util import safe_makedirs
from sqlbrat.capacity_validation import validate_capacity


def run_validation(huc_list: List, working_dir: str, brat_model_version: str = '5.1.4'):

    download_dir = os.path.join(working_dir, "downloads")
    safe_makedirs(download_dir)
    brat_dir = os.path.join(working_dir, "brat")
    safe_makedirs(brat_dir)
    qris_dir = os.path.join(working_dir, "qris")
    safe_makedirs(qris_dir)

    riverscapes_api = RiverscapesAPI(stage='production')
    riverscapes_api.refresh_token()

    for huc in huc_list:

        # download brat projects
        brat_params = RiverscapesSearchParams(
            {
                "projectTypeId": "riverscapes_brat",
                "tags": ['2024CONUS'],
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

        for huc, project_list in projects.items():
            if len(project_list) > 1:
                project_list.sort(key=lambda x: x.created_date)
                questions = [
                    inquirer.List('selected_project',
                                  message=f"Select a project for HUC {huc}",
                                  choices=[f"{proj.name} (Created: {proj.created_date})" for proj in project_list])
                ]
                answers = inquirer.prompt(questions)
                selected_project = next(proj for proj in project_list if f"{proj.name} (Created: {proj.created_date})" == answers['selected_project'])
                # Use selected_project for further processing
            else:
                selected_project = project_list[0]
                # Use selected_project for further processing
            to_download[huc] = selected_project

        brat_gpkgs = []
        for huc, project in to_download.items():
            dl_dir = os.path.join(download_dir, 'brat', project.huc)
            brat_gpkgs.append(os.path.join(dl_dir, 'outputs', 'brat.gpkg'))
            riverscapes_api.download_files(project.id, dl_dir)

        # download qris beaver census projects
        beaver_params = RiverscapesSearchParams(
            {
                "projectTypeId": "riverscapesstudio",
                "meta": {
                    "HUC": str(huc)
                }})
        to_download = {}
        projects = {}

        for project, _stats, search_total, _prg in riverscapes_api.search(beaver_params):
            if project.huc not in projects.keys():
                projects[project.huc] = [project]
            else:
                projects[project.huc].append(project)
        for huc, project_list in projects.items():
            if len(project_list) > 1:
                project_list.sort(key=lambda x: x.created_date)
                questions = [
                    inquirer.List('selected_project',
                                  message=f"Select a project for HUC {huc}",
                                  choices=[f"{proj.name} (Created: {proj.created_date})" for proj in project_list])
                ]
                answers = inquirer.prompt(questions)
                selected_project = next(proj for proj in project_list if f"{proj.name} (Created: {proj.created_date})" == answers['selected_project'])
                # Use selected_project for further processing
            else:
                selected_project = project_list[0]
                # Use selected_project for further processing
            to_download[huc] = selected_project

        beaver_gpgks = []
        for huc, project in to_download.items():
            dl_dir = os.path.join(download_dir, 'beaver_activity', project.huc)
            beaver_gpgks.append(os.path.join(dl_dir, 'beaver_activity_1.gpkg'))
            riverscapes_api.download_files(project.id, dl_dir)

        # merge projects
        safe_makedirs(os.path.join(brat_dir, huc))
        safe_makedirs(os.path.join(qris_dir, huc))
        out_brat_gpkg = os.path.join(brat_dir, huc, 'brat.gpkg')
        out_beaver_gpkg = os.path.join(qris_dir, huc, 'beaver_activity_1.gpkg')

        for g in brat_gpkgs:
            cmd = f"ogr2ogr -f GPKG -makevalid -append -nln 'vwReaches' {out_brat_gpkg} {g} 'vwReachs'"
            subprocess.run(cmd, shell=True)


run_validation([10190007], '/workspaces/data/', brat_model_version=None)
