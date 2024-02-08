""" Query Script to Find and change visibility of projects on the server
    June 05, 2023
"""

import json
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI
from rscommons import Logger
import inquirer


def changeVis(stage, vis: str):
    """ Find and change visibility of projects on the server

    Args:
        stage (str): The stage to run the script on
        vis (str): The visibility to change to
    """
    log = Logger('ChangeVisibility')
    log.title('Change Visibility of Projects from the server')

    riverscapes_api = RiverscapesAPI(stage=stage)
    search_query = riverscapes_api.load_query('searchProjects')
    # Only refresh the token if we need to
    if riverscapes_api.access_token is None:
        riverscapes_api.refresh_token()

    searchParams = {
        "meta": [{
            "key": "Runner",
            "value": "Cybercastor",
        }]
    }

    changeable_projects = []
    offset = 0
    total = 0
    # Create a timedelta object with a difference of 1 day
    while offset == 0 or offset < total:

        results = riverscapes_api.run_query(
            search_query, {"searchParams": searchParams, "limit": 500, "offset": offset})
        total = results['data']['searchProjects']['total']
        offset += 500

        projects = results['data']['searchProjects']['results']
        log.info(f"   Fetching projects {offset} to {offset + 500}")
        for search_result in projects:

            project = search_result['item']
            if project['id'] == 'b7ca4ed4-9dea-4e78-9fcb-f7d15ee8614b':
                changeable_projects.append(project)
            if project['visibility'] != vis:
                if project['id'] == 'b7ca4ed4-9dea-4e78-9fcb-f7d15ee8614b':
                    changeable_projects.append(project)

    # Now write all projects to a log file as json
    with open('changeable_projects.json', 'w') as f:
        f.write(json.dumps(changeable_projects))

    # Ask the user to confirm using inquirer
    log.info(f"Found {len(changeable_projects)} out of {total} projects to change visibility")
    questions = [
        inquirer.Confirm('confirm2',
                         message=f"Do you want to change all {len(changeable_projects)} projects?"),
        inquirer.Confirm('confirm1',
                         message="Are you sure?"),
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
        print(f"Changing project: {project['name']} with id: {project['id']}")
        riverscapes_api.run_query(mutation_script, {"projectId": project['id'], "project": {"visibility": vis}})

    # Shut down the API since we don;t need it anymore
    riverscapes_api.shutdown()

    log.info("Done!")


if __name__ == '__main__':
    changeVis('staging', 'PRIVATE')
