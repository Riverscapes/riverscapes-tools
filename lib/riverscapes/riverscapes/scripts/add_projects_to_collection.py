""" Add projects to a collection

Works by either cutting and pasting a list of project GUIDs on the command line or loading a CSV file with the project GUIDs.
If using CSV then simply place each GUID on a new line in the file. No header line is needed.

This script is not in the launch. Use the "run the current file" command in VSCode to run this script.
"""
from rsxml import Logger
import inquirer
from riverscapes import RiverscapesAPI


def add_projects_to_collection(riverscapes_api: RiverscapesAPI, env: str):
    """ Add projects to a collection

    To run this file in VSCode choose "Python: Current File (Riverscapes API)" from the command palette

    """
    log = Logger('AddProjectstoColl')
    log.title('Add Projects to Collection')

    questions = [
        inquirer.Text('collection', message="What collection GUID do you want to add the project(s) to?"),
        inquirer.List('method', message="Do you want to paste project GUIDs or load from file", choices=['Cut/Paste comma separated list of GUIDs', 'CSV File of project GUIDS'], default='Cut/Paste'),
    ]
    answers = inquirer.prompt(questions)

    if answers['collection'] is None or answers['collection'] == '':
        log.error("Collection GUID is required")
        return

    project_ids = []
    if str.startswith(answers['method'], 'Cut'):
        answers.update(inquirer.prompt([inquirer.Text('projects', message="Paste the project GUIDs separated by commas")]))
    else:
        answers.update(inquirer.prompt([inquirer.Text('file', message="What is the path to the file with the project GUIDs?")]))

        with open(answers['file'], 'r', encoding='utf8') as f:
            guids = f.readlines()
            answers['projects'] = ','.join([s.replace('\n', '').replace(',', '') for s in guids])

    project_ids = answers['projects'].split(',')
    if len(project_ids) == 0:
        log.error("No projects provided")
        return

    log.info(f"Adding projects to collection: {answers['collection']}")
    log.info(f"Projects: {len(project_ids)}")

    # Confirm where to continue
    confirm = inquirer.prompt([
        inquirer.Confirm('continue', message=f"Continue adding {len(project_ids)} to collection: {answers['collection']}?")
    ])

    if not confirm['continue']:
        log.info("Exiting")
        return

    mutation_script = riverscapes_api.load_mutation('addProjectsToCollection')
    riverscapes_api.run_query(mutation_script, {'collectionId': answers['collection'], 'projectIds': project_ids})
    log.info(f"Collection URL: https://{'staging.' if env == 'staging' else ''}data.riverscapes.net/c/{answers['collection']}")
    riverscapes_api.shutdown()
    log.info("Done!")


if __name__ == '__main__':
    environment = 'production'
    with RiverscapesAPI(stage=environment) as api:
        add_projects_to_collection(api, environment)
