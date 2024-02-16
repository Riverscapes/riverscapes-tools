"""[summary]
"""
import os
from typing import List
import json
from rsxml import Logger, safe_makedirs
import inquirer
from cybercastor import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject


def add_tag(riverscapes_api: RiverscapesAPI):
    """ Find and add tags to projects on the Riverscapes Data Exchange

    To run this file in VSCode choose "Python: Current File (Cybercastor)" from the command palette

    """
    log = Logger('AddTag')
    log.title('Add tag to Projects from the server')

    # First gather everything we need to make a search
    # ================================================================================================================

    # Load the search params from a JSON file so we don't have to hardcode them
    search_params = RiverscapesSearchParams.load_from_json(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'add_tags_search.json'))

    # Load the HUC groups from a JSON file so we don't have to hardcode them
    # tagging_hucs_file = os.path.join(os.path.dirname(__file__), '..', '..', 'inputs/tagging_huc_groups.json')
    # huc_groups = {}
    # if os.path.exists(tagging_hucs_file):
    #     with open(tagging_hucs_file, 'r', encoding='utf8') as f:
    #         huc_groups = json.load(f)
    # else:
    #     raise FileNotFoundError(f"Could not find the file {tagging_hucs_file}")

    # Instead of command-line arguments, we'll use inquirer to ask the user for the stage and tags
    default_dir = os.path.join(os.path.expanduser("~"), 'RSTagging')
    questions = [
        inquirer.Text('logdir', message="Where do you want to save the log files?", default=default_dir),
        inquirer.Text('tags', message="Comma-separated tags", default='zzzz,abc')
    ]
    answers = inquirer.prompt(questions)

    tags = [x.strip() for x in answers['tags'].split(',')]
    logdir = answers['logdir']
    safe_makedirs(logdir)

    # Make the search and collect all the data
    # ================================================================================================================

    changeable_projects: List[RiverscapesProject] = []

    # huc_list = sum([huc for huc in huc_groups.values()], [])

    # Create a timedelta object with a difference of 1 day
    total = 0
    for project, _stats, search_total in riverscapes_api.search(search_params, progress_bar=True):
        total = search_total
        if any(tag not in project.tags for tag in tags):
            changeable_projects.append(project)
        # if 'HUC' in project.project_meta and project.project_meta['HUC'] in huc_list:

    # Now write all projects to a log file as json
    logpath = os.path.join(logdir, f'add_tag_{riverscapes_api.stage}_{"-".join(tags)}.json')
    with open(logpath, 'w', encoding='utf8') as fobj:
        fobj.write(json.dumps([x.json for x in changeable_projects], indent=2))

    # Now ask if we're sure and then run mutations on all these projects one at a time
    # ================================================================================================================

    # Ask the user to confirm using inquirer
    log.info(f"Found {len(changeable_projects)} out of {total} projects to add tag")
    if len(changeable_projects) == 0:
        log.info("No projects to add tag to. Exiting.")
        return
    log.warning(f"Please review the summary of the affected projects in the log file at {logpath} before proceeding!")
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
        # huc_id = project.project_meta.get('HUC', None)
        # huc_group = next(k for k, v in huc_groups.items() if huc_id in v)
        # if huc_group not in project.tags:
        #     project.tags.append(huc_group)
        for tag in tags:
            if tag not in project.tags:
                project.tags.append(tag)
        # Now run the mutation
        riverscapes_api.run_query(mutation_script, {
            "projectId": project.id,
            "project": {"tags": project.tags}
        })

    log.info("Done!")


if __name__ == '__main__':
    with RiverscapesAPI() as api:
        add_tag(api)
