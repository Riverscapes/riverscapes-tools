"""
Reads a CSV file with project IDs and tags and updates the tags on the projects in Data Exchange
Philip Bailey
1 Feb 2025

NOT IN launch.json. USE "RUN CURRENT PYTHON FILE" INSTEAD
"""
from collections import Counter
import sys
import logging
import os
import questionary
from rsxml import Logger, ProgressBar
from riverscapes import RiverscapesAPI


def clean_tag_string(tags: str) -> list:
    return [tag.strip() for tag in tags.split(',')]


def tag_projects(rs_api: RiverscapesAPI):
    """ Update the tags on a CSV file of project GUIDs
    """

    log = Logger('TagProjectsCSV')
    log.title('Tagging Projects from CSV')

    new_tags = questionary.text("What tags do you want to apply?", default='abc,zzz').ask()
    method = questionary.select(
        "Do you want to paste project GUIDs or load from file",
        choices=[
            'Cut/Paste comma separated list of project GUIDs',
            'CSV File of project GUIDS'
        ],
        default='Cut/Paste'
    ).ask()
    replace = questionary.select(
        'Replace existing tags?',
        choices=['Yes', 'No'],
        default='No'
    ).ask()
    answers = {'new_tags': new_tags, 'method': method, 'replace': replace}

    if answers['new_tags'] is None or answers['new_tags'] == '':
        log.error("No new tags specified. Aborting")
        return

    project_ids = []
    if str.startswith(answers['method'], 'Cut'):
        projects = questionary.text("Paste the project GUIDs separated by commas").ask()
        answers['projects'] = projects
    else:
        file_path = questionary.text("What is the path to the file with the project GUIDs?").ask()
        with open(file_path, 'r', encoding='utf8') as f:
            guids = f.readlines()
            answers['projects'] = ','.join([s.replace('\n', '').replace(',', '') for s in guids])

    project_ids = answers['projects'].split(',')
    if len(project_ids) == 0:
        log.error("No projects provided")
        return

    clean_tags = clean_tag_string(answers['new_tags'])

    log.info(f"New Tags being applied: {answers['new_tags']}")
    log.info(f"Clean New Tags being applied: {clean_tags}")
    log.info(f"Projects: {len(project_ids)}")

    if clean_tags == [''] or len(clean_tags) == 0:
        print('No valid new tags provided')
        sys.exit(1)

    get_project_query = rs_api.load_query('getProjectTags')
    tags_mutation = rs_api.load_mutation('updateProject')
    _prg = ProgressBar(len(project_ids), 30, 'Tagging Projects')
    outer_counter = 0
    for project_id in project_ids:
        outer_counter += 1
        _prg.update(outer_counter)
        project = rs_api.run_query(get_project_query, {"id": project_id})
        existing_tags = project['data']['project']['tags']

        if Counter(existing_tags) == Counter(clean_tags):
            print(f'Project {project_id} already has tags {clean_tags}. No action for this project.')
            continue

        if answers['replace'] == 'Yes':
            new_tags = clean_tags
        else:
            new_tags = list(set(existing_tags + clean_tags))

        rs_api.run_query(tags_mutation, {"projectId": project_id, "project": {"tags": new_tags}})

    _prg.finish()

    log.info('Tagging complete')


def main():
    """
    Tag projects in Data Exchange with the user specified tag
    """

    stage = questionary.select(
        "What API stage?",
        choices=['production', 'staging'],
        default='production'
    ).ask()
    answers = {'stage': stage}

    log = Logger('Setup')
    log.setup(log_path=os.path.join(os.path.dirname(__file__), 'tag_projects_csv.log'), log_level=logging.DEBUG)
    log.info(f'Starting project tagger against {answers["stage"]} API')

    with RiverscapesAPI(stage=answers['stage']) as api:
        tag_projects(api)


if __name__ == '__main__':
    main()
