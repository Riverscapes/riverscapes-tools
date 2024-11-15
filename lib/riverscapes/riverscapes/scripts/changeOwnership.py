"""[summary]
"""
import os
from typing import List
import json
from rsxml import Logger, safe_makedirs
import inquirer
from riverscapes import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject


def change_owner(riverscapes_api: RiverscapesAPI):
    """ Change the ownership of projects based on a search of Riverscapes Data Exchange

    To run this file in VSCode choose "Python: Current File (Cybercastor)" from the command palette

    """
    log = Logger('ChangeOwner')
    log.title('Change Owner of Projects from the server')

    # First gather everything we need to make a search
    # ================================================================================================================

    search_params = RiverscapesSearchParams.load_from_json(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'change_owner_search.json'))

    default_dir = os.path.join(os.path.expanduser("~"), 'RSTagging')
    out_questions = [
        inquirer.Text('logdir', message="Where do you want to save the log files?", default=default_dir),
        inquirer.Text('orgGuid', message="Type the organization GUID?"),
    ]
    out_answers = inquirer.prompt(out_questions)

    new_org_id = out_answers['orgGuid']
    logdir = out_answers['logdir']
    safe_makedirs(logdir)

    # Make the search and collect all the data
    # ================================================================================================================

    changeable_projects: List[RiverscapesProject] = []
    total = 0
    for project, _stats, search_total, _prg in riverscapes_api.search(search_params, progress_bar=True):
        total = search_total
        if project.json['ownedBy']['id'] != new_org_id:
            changeable_projects.append(project)

    # Now write all projects to a log file as json
    logpath = os.path.join(logdir, f'change_owner_projects_{riverscapes_api.stage}.json')
    with open(logpath, 'w', encoding='utf8') as f:
        f.write(json.dumps([x.json for x in changeable_projects], indent=2))

    # Now ask if we're sure and then run mutations on all these projects
    # ================================================================================================================

    log.info(f"Found {len(changeable_projects)} out of {total} projects to change ownership")
    log.warning(f"Please review the summary of the affected projects in the log file at {logpath} before proceeding!")
    questions = [
        inquirer.Confirm('confirm1', message="Are you sure you want to change ownership on all these projects?"),
    ]
    answers = inquirer.prompt(questions)
    if not answers['confirm1']:  # or not answers['confirm2']:
        log.info("Good choice. Aborting!")
        riverscapes_api.shutdown()
        return

    # Now Change Owner of all projects
    mutation_script = riverscapes_api.load_mutation('changeProjectOwner')
    for project in changeable_projects:
        log.info(f"Change Owner of project: {project.name} with id: {project.id}")
        riverscapes_api.run_query(mutation_script, {
            "projectId": project.id,
            "owner": {
                "id": new_org_id,
                "type": "ORGANIZATION"
            }
        })

    # Shut down the API since we don;t need it anymore
    riverscapes_api.shutdown()

    log.info("Done!")


if __name__ == '__main__':
    with RiverscapesAPI() as api:
        change_owner(api)
