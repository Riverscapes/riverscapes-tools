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

from rscommons.util import safe_makedirs, safe_remove_dir
from sqlbrat.capacity_validation import validate_capacity


def run_validation(huc_list: List, working_dir: str, upload_tags: str = None):

    download_dir = os.path.join(working_dir, "downloads")
    safe_makedirs(download_dir)
    brat_dir = os.path.join(working_dir, "brat")
    safe_makedirs(brat_dir)
    beav_dir = os.path.join(working_dir, "beaver_activity")
    safe_makedirs(beav_dir)

    riverscapes_api = RiverscapesAPI(stage='production')
    riverscapes_api.refresh_token()

    for huc in huc_list:

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
                project_list.sort(key=lambda x: x.created_date)
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
                project_list.sort(key=lambda x: x.created_date)
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

        num_beaver_gpkgs = {}
        for hucnum, project in to_download.items():
            dl_dir = os.path.join(download_dir, 'beaver_activity', project.huc)
            riverscapes_api.download_files(project.id, dl_dir)
            num_beaver_gpkgs[hucnum] = [os.path.join(dl_dir, f) for f in os.listdir(dl_dir) if f.endswith('.gpkg')]

        # merge projects -- DO I COMBINE ALL REALIZATIONS OR JUST USE A SINGLE CHOSEN FOR EACH...?
        safe_makedirs(os.path.join(brat_dir, str(huc)))
        safe_makedirs(os.path.join(beav_dir, str(huc)))
        out_brat_gpkg = os.path.join(brat_dir, str(huc), 'brat.gpkg')
        # out_beaver_gpkg = os.path.join(qris_dir, huc, 'beaver_activity_1.gpkg')

        for g in brat_gpkgs:
            cmd = f"ogr2ogr -f GPKG -makevalid -append -nln 'vwReaches' {out_brat_gpkg} {g} 'vwReaches'"
            subprocess.run(cmd, shell=True)

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
        for g in beaver_gpkgs:
            cmd = f"ogr2ogr -f GPKG -makevalid -append -nln 'dams' {out_beaver_gpkg} {g} 'dams'"
            subprocess.run(cmd, shell=True)

        # run capacity validation
        validate_capacity(out_brat_gpkg, out_beaver_gpkg)
        valid_path = os.path.join(os.path.dirname(out_brat_gpkg), 'validation')
        for g in brat_gpkgs:
            cmd = f"cp -r {valid_path} {os.path.dirname(g)}"
            subprocess.run(cmd, shell=True)
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


run_validation([17010203], '/workspaces/data/', upload_tags='2024CONUS,validation_data')
