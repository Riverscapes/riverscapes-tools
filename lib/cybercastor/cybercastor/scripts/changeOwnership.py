"""[summary]
"""
import json
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI
from rscommons import Logger
import inquirer


def change_owner(stage, new_org_id: str):
    """ DUmp all projects to a DB

    Args:
        output_folder ([type]): [description]
    """
    log = Logger('ChangeOwner')
    log.title('Change Owner of Projects from the server')

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
            if project['ownedBy']['id'] != new_org_id:
                changeable_projects.append(project)

    # Now write all projects to a log file as json
    with open('change_owner_projects.txt', 'w') as f:
        f.write(json.dumps(changeable_projects))

    # Ask the user to confirm using inquirer
    log.info(f"Found {len(changeable_projects)} out of {total} projects to change ownership")
    questions = [
        inquirer.Confirm('confirm1',
                         message="Are you sure you want to change ownership on all these projects?"),
    ]
    answers = inquirer.prompt(questions)
    if not answers['confirm1']:  # or not answers['confirm2']:
        log.info("Good choice. Aborting!")
        return

    # Now Change Owner of all projects
    mutation_script = riverscapes_api.load_mutation('changeProjectOwner')
    for project in changeable_projects:
        print(f"Change Owner of project: {project['name']} with id: {project['id']}")
        riverscapes_api.run_query(mutation_script, {
            "projectId": project['id'],
            "owner": {
                "id": new_org_id,
                "type": "ORGANIZATION"
            }
        })

    # Shut down the API since we don;t need it anymore
    riverscapes_api.shutdown()

    log.info("Done!")


if __name__ == '__main__':
    change_owner('staging', 'org-guid-here')
