"""

    To run this file in VSCode choose "Python: Current File (Cybercastor)" from the command palette

"""
import os
import sys
from typing import List
import json
from rsxml import Logger
from rsxml.util import safe_makedirs
import inquirer
from riverscapes import RiverscapesAPI, RiverscapesProject, RiverscapesSearchParams


def confirm(msg: str = None):
    """ Just a little confirmation function
    """
    log = Logger('confirm')
    if msg:
        log.warning(msg)

    # Ask the user to confirm using inquirer
    questions = [inquirer.Confirm('confirm1', message="Are you sure you want to rebuild web tiles for all these projects?")]
    if not inquirer.prompt(questions)['confirm1']:
        log.info("Good choice. Aborting!")
        sys.exit()


def rebuild_web_tiles(riverscapes_api: RiverscapesAPI):
    """ Rebuild web tiles based on a series of choices

    Args:
        output_folder ([type]): [description]
    """

    log = Logger('Rebuilt Web times')
    log.title('Rebuilt web tiles for either a group of projects or just one')

    default_dir = os.path.join(os.path.expanduser("~"), 'RebuildTiles')

    # Ask if it's one project or many based on a search
    questions = [
        inquirer.Text('filedir', message="Where do you want to save the files?", default=default_dir),
        inquirer.List('oneOrSearch',
                      message="Would you like to rebuilt tiles for one project or many based on a search?",
                      default='one',
                      choices=['one', 'search'],
                      ),
        # Ask if we want the whole project or just specific xpaths
        inquirer.List('wholeOrXpaths',
                      message="Would you like to rebuilt tiles for the whole project or just specific xpaths?",
                      choices=['whole', 'xpaths'],
                      ),
        # Do you want to force the rebuild if there are already tiles there?
        inquirer.List('force',
                      message="Would you like to force the rebuild if there are already tiles there?",
                      default='no',
                      choices=['yes', 'no'],
                      )
    ]
    answers = inquirer.prompt(questions)

    logdir = answers['filedir']
    safe_makedirs(logdir)

    # Only refresh the token if we need to
    mutation_script = riverscapes_api.load_mutation('rebuildWebTiles')
    mutation_params = {
        "projectId": None,
        "rsXPaths": [],
        "force": False,
    }

    mutation_params['force'] = answers['force'] == 'yes'

    if answers['wholeOrXpaths'] == 'xpaths':
        # Ask for a comma-separated list of xpaths
        mutation_params['rsXPaths'] = inquirer.prompt([
            inquirer.Text('xpaths', message="What are the xpaths you want to rebuilt?"),
        ])['xpaths'].split(',')

    if answers['oneOrSearch'] == 'one':
        # Ask for the project id
        project_id = inquirer.prompt([
            inquirer.Text('projectId', message="What is the project id?"),
        ])['projectId']
        mutation_params['projectId'] = project_id

        # Ask the user to confirm using inquirer
        confirm("Ready to call rebuild tiles on a single project")

        # Now run the mutation
        riverscapes_api.run_query(mutation_script, mutation_params)

    else:
        search_params = RiverscapesSearchParams({"projectTypeId": "rcat"})
        confirm(f"Ready to search for projects using search params: \n {json.dumps(search_params, indent=2)}. \n\n IF THIS IS NOT WHAT YOU WANT, HIT CTRL-C NOW!")

        changeable_projects: List[RiverscapesProject] = []
        total = 0
        for project, _stats, search_total in riverscapes_api.search(search_params, progress_bar=True):
            total = search_total
            changeable_projects.append(project['item'])

        # Now write all projects to a log file as json
        logpath = os.path.join(logdir, 'rebuild_tiles.json')
        with open(logpath, 'w', encoding='utf8') as f:
            f.write(json.dumps([x.json for x in changeable_projects]))

        # Ask the user to confirm using inquirer
        confirm(f"Found {len(changeable_projects)} out of {total} projects to change rebuilt web tiles")

        # Now rebuilt web tiles all projects
        for project in changeable_projects:
            print(f"Rebuilding web tiles of project: {project['name']} with id: {project['id']}")
            mutation_params['projectId'] = project['id']
            riverscapes_api.run_query(mutation_script, mutation_params)

    log.info("Done!")


if __name__ == '__main__':
    if not os.environ.get('RS_CLIENT_ID') or not os.environ.get('RS_CLIENT_SECRET'):
        raise ValueError("You need to set the RS_CLIENT_ID and RS_CLIENT_SECRET environment variables")

    with RiverscapesAPI(machine_auth={
        "clientId": os.environ.get('RS_CLIENT_ID'),
        "secretId": os.environ.get('RS_CLIENT_SECRET'),
    }) as api:
        rebuild_web_tiles(api)
