""" Query Script to Find and change visibility of projects on the server
    June 05, 2023
"""
import os
from typing import List
import json
import inquirer
from rsxml import Logger, safe_makedirs
from cybercastor import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject


def changeVis():
    """ Find and change visibility of projects on the server

    Args:
        stage (str): The stage to run the script on
        vis (str): The visibility to change to
    """
    log = Logger('ChangeVisibility')
    log.title('Change Visibility of Projects from the server')

    # First gather everything we need to make a search
    # ================================================================================================================

    search_params = RiverscapesSearchParams.load_from_json(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'add_tags_search.json'))

    default_dir = os.path.join(os.path.expanduser("~"), 'RSTagging')
    out_questions = [
        # Also get if this is production or staging (default production)
        inquirer.List('stage', message="Which Data Exchange stage?", choices=['production', 'staging'], default='production'),
        inquirer.Text('logdir', message="Where do you want to save the files?", default=default_dir),
        inquirer.List('vis', message="Which visibility do you want to change to?", choices=['PUBLIC', 'PRIVATE'], default='public'),
    ]
    out_answers = inquirer.prompt(out_questions)

    stage = out_answers['stage']
    new_visibility = out_answers['vis']
    logdir = out_answers['logdir']
    if not os.path.exists(logdir):
        safe_makedirs(logdir)

    # Make the search and collect all the data
    # ================================================================================================================

    riverscapes_api = RiverscapesAPI(stage=stage)
    riverscapes_api.refresh_token()

    changeable_projects: List[RiverscapesProject] = []
    total = 0
    for project, _stats, search_total in riverscapes_api.search(search_params, progress_bar=True):
        total = search_total
        if project.visibility != new_visibility:
            changeable_projects.append(project)

    # Now write all projects to a log file as json
    logpath = os.path.join(logdir, 'change_visibility.json')
    with open(logpath, 'w', encoding='utf8') as f:
        f.write(json.dumps([x.json for x in changeable_projects]))

    # Now ask if we're sure and then run mutations on all these projects
    # ================================================================================================================

    # Ask the user to confirm using inquirer
    log.info(f"Found {len(changeable_projects)} out of {total} projects to change visibility")
    questions = [
        inquirer.Confirm('confirm2', message=f"Do you want to change all {len(changeable_projects)} projects?"),
        inquirer.Confirm('confirm1', message="Are you sure?"),
    ]
    answers = inquirer.prompt(questions)
    if not answers['confirm1'] or not answers['confirm2']:
        log.info("Good choice. Aborting!")
        # Shut down the API since we don;t need it anymore
        riverscapes_api.shutdown()
        return

    # Now ChangeVisibility all projects
    mutation_script = riverscapes_api.load_mutation('updateProject')
    for project in changeable_projects:
        log.info(f"Changing project: {project.name} with id: {project.id}")
        riverscapes_api.run_query(mutation_script, {"projectId": project.id, "project": {"visibility": new_visibility}})

    # Shut down the API since we don;t need it anymore
    riverscapes_api.shutdown()

    log.info("Done!")


if __name__ == '__main__':
    changeVis()
