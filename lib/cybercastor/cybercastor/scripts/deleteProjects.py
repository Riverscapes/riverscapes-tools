"""[summary]
"""
import os
from typing import List
import json
from termcolor import colored
from rsxml import Logger, safe_makedirs
import inquirer
from cybercastor import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject


def delete_by_tags():
    """ Delete all projects with certain tag(s)

    Args:
        stage (str): The server to run the script on
        filedir (str): The directory to save the log file
        proj_type (str): The project type to search for
        tag (str): The tag to add to the projects
    """
    log = Logger('AddTag')
    log.title('Add tag to Projects from the server')

    # First gather everything we need to make a search
    # ================================================================================================================

    search_params = RiverscapesSearchParams.load_from_json(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'add_tags_search.json'))

    default_dir = os.path.join(os.path.expanduser("~"), 'RSTagging')
    out_questions = [
        # Also get if this is production or staging (default production)
        inquirer.List('stage', message="Which Data Exchange stage?", choices=['production', 'staging'], default='production'),
        inquirer.Text('logdir', message="Where do you want to save the log files?", default=default_dir),
        inquirer.Text('tags', message="Comma-separated tags", default='zzzz,abc')
    ]
    out_answers = inquirer.prompt(out_questions)
    logdir = out_answers['logdir']
    safe_makedirs(logdir)

    tags = [x.strip() for x in out_answers['tags'].split(',')]
    stage = out_answers['stage']
    filedir = out_answers['filedir']

    # Make the search and collect all the data
    # ================================================================================================================

    riverscapes_api = RiverscapesAPI(stage=stage)
    riverscapes_api.refresh_token()

    deletable_projects: List[RiverscapesProject] = []

    total = 0
    for project, _stats, _search_total in riverscapes_api.search(search_params, progress_bar=True):
        deletable_projects.append(project)

    # Now write all projects to a log file as json
    logpath = os.path.join(filedir, f'delete_by_tag_{stage}_{"-".join(tags)}')
    with open(logpath, 'w', encoding='utf8') as fobj:
        fobj.write(json.dumps([x.json for x in deletable_projects]))

    # Now ask if we're sure and then run mutations on all these projects
    # ================================================================================================================

    # Ask the user to confirm using inquirer
    log.info(f"Found {len(deletable_projects)} out of {total} projects to delete")
    questions = [
        inquirer.Confirm('confirm1', message="Are you sure you want to PERMANENTLY DELETE the projects?"),
    ]
    answers = inquirer.prompt(questions)
    if not answers['confirm1']:  # or not answers['confirm2']:
        log.info("Good choice. Aborting!")
        return
    questions2 = [
        inquirer.Confirm('confirm1', message=colored("NO, SERIOUSLY!!!!! TAKE A MOMENT HERE. ARE YOU ABSOLUTELY SURE?!?", 'red')),
    ]
    answers2 = inquirer.prompt(questions2)
    if not answers2['confirm1']:  # or not answers['confirm2']:
        log.info("Good choice. Aborting!")
        return

    # Now delete all projects
    mutation_script = riverscapes_api.load_mutation('deleteProject')
    for project in deletable_projects:
        print(f"Deleting project: {project['name']} with id: {project['id']}")
        raise Exception("TOO DANGEROUS!!! UNCOMMENT THE LINE BELOW TO DELETE PROJECTS!")
        # riverscapes_api.run_query(mutation_script, {"projectId": project['id'], 'options': {}})

    # Shut down the API since we don;t need it anymore
    riverscapes_api.shutdown()

    log.info("Done!")


if __name__ == '__main__':
    delete_by_tags()
