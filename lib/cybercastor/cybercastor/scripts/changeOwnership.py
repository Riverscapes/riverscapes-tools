"""[summary]
"""
import os
from typing import List
import json
from rsxml import Logger, safe_makedirs
import inquirer
from cybercastor import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject


def change_owner(stage: str, filedir: str, new_org_id: str):
    """ Change the ownership of projects based on a search of Riverscapes Data Exchange

    Args:
        output_folder ([type]): [description]
    """
    log = Logger('ChangeOwner')
    log.title('Change Owner of Projects from the server')

    riverscapes_api = RiverscapesAPI(stage=stage)
    riverscapes_api.refresh_token()

    searchParams = RiverscapesSearchParams({
        "meta": {
            "Runner": "Cybercastor",
        },
    })

    changeable_projects: List[RiverscapesProject] = []
    total = 0
    for project, _stats, search_total in riverscapes_api.search(searchParams, progress_bar=True):
        total = search_total
        if project.json['ownedBy']['id'] != new_org_id:
            changeable_projects.append(project)

    # Now write all projects to a log file as json
    logpath = os.path.join(filedir, 'change_owner_projects.json')
    with open(logpath, 'w', encoding='utf8') as f:
        f.write(json.dumps([x.json for x in changeable_projects]))

    # Ask the user to confirm using inquirer
    log.info(f"Found {len(changeable_projects)} out of {total} projects to change ownership")
    questions = [
        inquirer.Confirm('confirm1', message="Are you sure you want to change ownership on all these projects?"),
    ]
    answers = inquirer.prompt(questions)
    if not answers['confirm1']:  # or not answers['confirm2']:
        log.info("Good choice. Aborting!")
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
    default_dir = os.path.join(os.path.expanduser("~"), 'ChangeOwner')
    out_questions = [
        # Also get if this is production or staging (default production)
        inquirer.List('stage', message="Which stage?", choices=['production', 'staging'], default='production'),
        inquirer.Text('filedir', message="Where do you want to save the files?", default=default_dir),
        inquirer.Text('orgGuid', message="Type the organization GUID?"),
    ]
    out_answers = inquirer.prompt(out_questions)
    if not os.path.exists(out_answers['filedir']):
        safe_makedirs(out_answers['filedir'])

    change_owner(out_answers['stage'], out_answers['filedir'], out_answers['orgGuid'])
