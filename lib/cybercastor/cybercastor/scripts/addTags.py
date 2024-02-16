"""[summary]
"""
import os
from typing import List
import json
from rsxml import Logger, safe_makedirs
import inquirer
from cybercastor import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject

INPUT_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'inputs/tagging_huc_groups.json')
huc_groups = {}
if os.path.exists(INPUT_FILE):
    with open(INPUT_FILE, 'r', encoding='utf8') as f:
        huc_groups = json.load(f)
else:
    raise FileNotFoundError(f"Could not find the file {INPUT_FILE}")


def add_tag():
    """ Find and add tags to projects on the Riverscapes Data Exchange

    Args:
        stage (str): The server to run the script on
        tag (str): The tag to add to the projects
    """
    log = Logger('AddTag')
    log.title('Add tag to Projects from the server')

    # First gather everything we need to make a search
    # ================================================================================================================

    search_params = RiverscapesSearchParams.load_from_json(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'add_tags_search.json'))

    default_dir = os.path.join(os.path.expanduser("~"), 'RSTagging')
    questions = [
        # Also get if this is production or staging (default production)
        inquirer.List('stage', message="Which Data Exchange stage?", choices=['production', 'staging'], default='production'),
        inquirer.Text('logdir', message="Where do you want to save the log files?", default=default_dir),
        inquirer.Text('tags', message="Comma-separated tags", default='zzzz,abc')
    ]
    answers = inquirer.prompt(questions)

    tags = [x.strip() for x in answers['tags'].split(',')]
    stage = answers['stage']
    logdir = answers['logdir']
    safe_makedirs(logdir)

    # Make the search and collect all the data
    # ================================================================================================================

    riverscapes_api = RiverscapesAPI(stage=stage)
    riverscapes_api.refresh_token()

    changeable_projects: List[RiverscapesProject] = []

    huc_list = sum([huc for huc in huc_groups.values()], [])

    # Create a timedelta object with a difference of 1 day
    total = 0
    for project, _stats, search_total in riverscapes_api.search(search_params, progress_bar=True):
        total = search_total
        if 'HUC' in project.project_meta and project.project_meta['HUC'] in huc_list:
            if any(tag not in project.tags for tag in tags):
                changeable_projects.append(project)

    # Now write all projects to a log file as json
    logpath = os.path.join(logdir, f'add_tag_{stage}_{"-".join(tags)}')
    with open(logpath, 'w', encoding='utf8') as fobj:
        fobj.write(json.dumps([x.json for x in changeable_projects]))

    # Now ask if we're sure and then run mutations on all these projects
    # ================================================================================================================

    # Ask the user to confirm using inquirer
    log.info(f"Found {len(changeable_projects)} out of {total} projects to add tag")
    questions = [
        inquirer.Confirm('confirm1', message=f"Are you sure you want to add the tag {tags} to all these projects?"),
    ]
    answers = inquirer.prompt(questions)
    if not answers['confirm1']:  # or not answers['confirm2']:
        log.info("Good choice. Aborting!")
        riverscapes_api.shutdown()
        return

    # Now Change Owner of all projects
    mutation_script = riverscapes_api.load_mutation('updateProject')
    for project in changeable_projects:
        log.debug(f"Add Tag to project: {project.name} with id: {project.id}")
        huc_id = project.project_meta.get('HUC', None)
        huc_group = next(k for k, v in huc_groups.items() if huc_id in v)
        if huc_group not in project.tags:
            project.tags.append(huc_group)
        for tag in tags:
            if tag not in project.tags:
                project.tags.append(tag)
        # Now run the mutation
        riverscapes_api.run_query(mutation_script, {
            "projectId": project.id,
            "project": {"tags": project.tags}
        })

    # Shut down the API since we don;t need it anymore
    riverscapes_api.shutdown()

    log.info("Done!")


if __name__ == '__main__':
    add_tag()
