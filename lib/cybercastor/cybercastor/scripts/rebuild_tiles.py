"""[summary]
"""
import os
from typing import List
import json
from rsxml import Logger, safe_makedirs
import inquirer
from cybercastor import RiverscapesAPI, RiverscapesProject, RiverscapesSearchParams


def confirm(riverscapes_api):
    """ Just a little confirmation function

    Args:
        riverscapes_api (_type_): _description_
    """
    log = Logger('confirm')
    # Ask the user to confirm using inquirer
    questions = [
        inquirer.Confirm('confirm1', message="Are you sure you want to rebuilt web tiles for all these projects?"),
    ]
    if not inquirer.prompt(questions)['confirm1']:
        log.info("Good choice. Aborting!")
        riverscapes_api.shutdown()
        exit()


def rebuildWebTiles():
    """ rebuild web tiles

    Args:
        output_folder ([type]): [description]
    """
    log = Logger('Rebuilt Web times')
    log.title('Rebuilt web tiles for either a group of projects or just one')

    default_dir = os.path.join(os.path.expanduser("~"), 'RebuildTiles')
    question1 = [
        # Use inquirer to ask "Staging" or "Production"
        inquirer.List('stage',
                      message="Which stage would you like to use?",
                      default='staging',
                      choices=['staging', 'production'],
                      ),
        inquirer.Text('filedir', message="Where do you want to save the files?", default=default_dir),
    ]
    answer1 = inquirer.prompt(question1)

    logdir = answer1['filedir']
    if not os.path.exists(logdir):
        safe_makedirs(logdir)

    if not os.environ.get('RS_CLIENT_ID') or not os.environ.get('RS_CLIENT_SECRET'):
        raise ValueError("You need to set the RS_CLIENT_ID and RS_CLIENT_SECRET environment variables")

    riverscapes_api = RiverscapesAPI(stage=answer1['stage'], machine_auth={
        "clientId": os.environ.get('RS_CLIENT_ID'),
        "secretId": os.environ.get('RS_CLIENT_SECRET'),
    })
    riverscapes_api.refresh_token()

    # Only refresh the token if we need to
    mutation_script = riverscapes_api.load_mutation('rebuildWebTiles')
    mutation_params = {
        "projectId": None,
        "rsXPaths": [],
        "force": False,
    }

    # Ask if it's one project or many based on a search
    question2 = [
        inquirer.List('oneOrSearch',
                      message="Would you like to rebuilt tiles for one project or many based on a search?",
                      default='one',
                      choices=['one', 'search'],
                      )
    ]
    answer2 = inquirer.prompt(question2)

    # Ask if we want the whole project or just specific xpaths
    question3 = [
        inquirer.List('wholeOrXpaths',
                      message="Would you like to rebuilt tiles for the whole project or just specific xpaths?",
                      choices=['whole', 'xpaths'],
                      )
    ]
    answer3 = inquirer.prompt(question3)

    # Do you want to force the rebuild if there are already tiles there?
    question4 = [
        inquirer.List('force',
                      message="Would you like to force the rebuild if there are already tiles there?",
                      default='no',
                      choices=['yes', 'no'],
                      )
    ]
    mutation_params['force'] = inquirer.prompt(question4)['force'] == 'yes'

    if answer3['wholeOrXpaths'] == 'xpaths':
        # Ask for a comma-separated list of xpaths
        rebuilt_xpaths = [
            inquirer.Text('xpaths', message="What are the xpaths you want to rebuilt?"),
        ]
        mutation_params['rsXPaths'] = inquirer.prompt(rebuilt_xpaths)['xpaths'].split(',')

    if answer2['oneOrSearch'] == 'one':
        # Ask for the project id
        question3 = [
            inquirer.Text('projectId', message="What is the project id?"),
        ]
        project_id = inquirer.prompt(question3)['projectId']
        mutation_params['projectId'] = project_id

        # Ask the user to confirm using inquirer
        log.info("Ready to call rebuild tiles on a single project")
        confirm(riverscapes_api)

        # Now run the mutation
        riverscapes_api.run_query(mutation_script, mutation_params)

    else:
        search_params = RiverscapesSearchParams({"projectTypeId": "rcat"})
        log.info(
            f"Ready to search for projects using search params: \n {json.dumps(search_params, indent=2)}. \n\n IF THIS IS NOT WHAT YOU WANT, HIT CTRL-C NOW!")
        confirm(riverscapes_api)

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
        log.info(f"Found {len(changeable_projects)} out of {total} projects to change rebuilt web tiles")
        confirm(riverscapes_api)

        # Now rebuilt web tiles all projects
        for project in changeable_projects:
            print(
                f"Rebuilding web tiles of project: {project['name']} with id: {project['id']}")
            mutation_params['projectId'] = project['id']
            riverscapes_api.run_query(mutation_script, mutation_params)

    # Shut down the API since we don;t need it anymore
    riverscapes_api.shutdown()

    log.info("Done!")


if __name__ == '__main__':
    rebuildWebTiles()
