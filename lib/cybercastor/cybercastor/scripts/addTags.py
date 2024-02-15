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


def add_tag(stage, filedir: str, proj_type: str, tags: List[str]):
    """ Find and add tags to projects on the Riverscapes Data Exchange

    Args:
        stage (str): The server to run the script on
        tag (str): The tag to add to the projects
    """
    log = Logger('AddTag')
    log.title('Add tag to Projects from the server')

    riverscapes_api = RiverscapesAPI(stage=stage)
    riverscapes_api.refresh_token()

    searchParams = RiverscapesSearchParams({
        'meta': {
            'Runner': 'CyberCastor',
        },
        'projectTypeId': proj_type,
        # 'createdOn': {
            # "from": "2023-06-19",
            # "to": "2023-06-19"
        # }
    })

    changeable_projects: List[RiverscapesProject] = []

    huc_list = sum([huc for huc in huc_groups.values()], [])

    # Create a timedelta object with a difference of 1 day
    total = 0
    for project, _stats, search_total in riverscapes_api.search(searchParams, progress_bar=True):
        total = search_total
        if 'HUC' in project.project_meta and project.project_meta['HUC'] in huc_list:
            if any(tag not in project.tags for tag in tags):
                changeable_projects.append(project)

    # Now write all projects to a log file as json
    logpath = os.path.join(filedir, f'add_tag_{stage}_{"-".join(tags)}')
    with open(logpath, 'w', encoding='utf8') as fobj:
        fobj.write(json.dumps([x.json for x in changeable_projects]))

    # Ask the user to confirm using inquirer
    log.info(f"Found {len(changeable_projects)} out of {total} projects to add tag")
    questions = [
        inquirer.Confirm('confirm1', message=f"Are you sure you want to add the tag {tags} to all these projects?"),
    ]
    answers = inquirer.prompt(questions)
    if not answers['confirm1']:  # or not answers['confirm2']:
        log.info("Good choice. Aborting!")
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
    default_dir = os.path.join(os.path.expanduser("~"), 'RSTagging')
    out_questions = [
        # Also get if this is production or staging (default production)
        inquirer.List('stage', message="Which stage?", choices=['production', 'staging'], default='production'),
        inquirer.List('proj_type', message="Which project type?", choices=['brat', 'vbet', 'RSContext', 'anthro', '__ALL__'], default='brat'),
        inquirer.Text('filedir', message="Where do you want to save the files?", default=default_dir),
        inquirer.Text('tags', message="Comma-separated tags", default='zzzz,abc')
    ]
    out_answers = inquirer.prompt(out_questions)
    if not os.path.exists(out_answers['filedir']):
        safe_makedirs(out_answers['filedir'])

    tag_input = [x.strip() for x in out_answers['tags'].split(',')]

    proj_type_answer = out_answers['proj_type'] if out_answers['proj_type'] != '__ALL__' else None

    add_tag(out_answers['stage'], out_answers['filedir'], proj_type_answer, tag_input)
