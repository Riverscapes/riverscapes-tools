""" Create a New Collection

This script is not in the launch. Use the "run the current file" command in VSCode to run this script.
"""
import os
import json
from termcolor import colored
from rsxml import Logger
import inquirer
from riverscapes import RiverscapesAPI


def create_collection(riverscapes_api: RiverscapesAPI, environment: str):
    """ Create a new collection

    To run this file in VSCode choose "Python: Current File (Riverscapes API)" from the command palette

    """
    log = Logger('CreateCol')
    log.title('Create New Collection')

    # First gather everything we need to to create a collection
    # ================================================================================================================

    default_dir = os.path.join(os.path.expanduser("~"), 'RSTagging')
    questions = [
        inquirer.Text('name', message="What is the name for the new collection?"),
        inquirer.Text('description', message="What is the description for the new collection?"),
        inquirer.Text('summary', message="What is the summary for the collection", ),
        inquirer.Text('owner', message="GUID of the owning organization or user?"),
        inquirer.List('visibility', message="What is the collection visibility", choices=['PUBLIC', 'SECRET'], default='SECRET'),
    ]

    answers = inquirer.prompt(questions)

    if answers['name'] is None or answers['name'] == '':
        log.error("Name is required")
        return

    log.info(f"Creating collection: {answers['name']}")
    log.info(f"Description: {answers['description']}")
    log.info(f"Summary: {answers['summary']}")
    log.info(f"Owner: {answers['owner']}")
    log.info(f"Visibility: {answers['visibility']}")

    # Confirm where to continue
    confirm = inquirer.prompt([
        inquirer.Confirm('continue', message="Continue with creating the collection?")
    ])

    if not confirm['continue']:
        log.info("Exiting")
        return

    params = {
        'name': answers['name'],
        'description': answers['description'] if answers['description'] else None,
        'summary': answers['summary'] if answers['summary'] else None,
        'visibility': answers['visibility'],
    }

    mutation_script = riverscapes_api.load_mutation('createCollection')
    result = riverscapes_api.run_query(mutation_script, {'collection': params, 'orgId': answers['owner']})

    collection_guid = result['data']['createCollection']['id']
    log.info(f"Collection created with GUID: {collection_guid}")
    log.info(f"Collection URL: https://{'staging.' if environment == 'staging' else ''}data.riverscapes.net/c/{collection_guid}")

    # Shut down the API since we don;t need it anymore
    riverscapes_api.shutdown()

    log.info("Done!")


if __name__ == '__main__':
    environment = 'production'
    with RiverscapesAPI(stage=environment) as api:
        create_collection(api, environment)
