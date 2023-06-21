"""Query Script to Find and delete projects on the server
    June 06, 2023
"""
import json
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI
from rscommons import Logger
import inquirer


def deleteProjects(stage):
    """ Find and delete projects on the server

    Args:
        stage (str): The stage to run the script on
    """
    log = Logger('DeleteProjects')
    log.title('Delete Projects from the server')

    riverscapes_api = RiverscapesAPI(stage=stage)
    search_query = riverscapes_api.load_query('searchProjects')
    # Only refresh the token if we need to
    if riverscapes_api.accessToken is None:
        riverscapes_api.refresh_token()

    searchParams = {
        "meta": [{
            "key": "Runner",
            "value": "Cybercastor",
        }]
    }

    deletable_projects = []
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
            deletable_projects.append(project)

    # Now write all projects to a log file as json
    with open('deletable_projects.json', 'w') as f:
        f.write(json.dumps(deletable_projects))

    # Ask the user to confirm using inquirer
    log.info(f"Found {len(deletable_projects)} projects to delete")
    questions = [
        inquirer.Confirm('confirm1',
                         message="Are you sure you want to delete all projects?"),
        inquirer.Confirm('confirm2',
                         message="No, Seriously. You are DELETING PROJECTS!!! Are you really sure?"),
    ]
    answers = inquirer.prompt(questions)
    if not answers['confirm1'] or not answers['confirm2']:
        log.info("Good choice. Aborting!")
        # Shut down the API since we don;t need it anymore
        riverscapes_api.shutdown()
        return

    # Now delete all projects
    mutation_script = riverscapes_api.load_mutation('deleteProject')
    for project in deletable_projects:
        print(f"Deleting project: {project['name']} with id: {project['id']}")
        riverscapes_api.run_query(mutation_script, {"projectId": project['id'], 'options': {}})

    # Shut down the API since we don;t need it anymore
    riverscapes_api.shutdown()

    log.info("Done!")


if __name__ == '__main__':
    deleteProjects('staging')
