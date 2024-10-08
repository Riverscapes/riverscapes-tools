"""
Scrapes RME and RCAT outout GeoPackages from Data Exchange and extracts statistics for each HUC.
Produced for the BLM 2024 September analysis of 2024 CONUS RME projects.
Philip Bailey
"""
from typing import Tuple
from collections import Counter
import sys
import os
import sqlite3
import argparse
import inquirer
from rsxml import dotenv
from riverscapes import RiverscapesAPI

name_lookup = {'RSContext': "RS Context",
               'ChannelArea': "Channel Area",
               'TauDEM': "TauDEM",
               'VBET': "VBET",
               'BRAT': "BRAT",
               'anthro': "ANTHRO",
               'rcat': "RCAT",
               'rs_metric_engine': "Metric Engine"}


def clean_tag_string(tags: str) -> list:
    return [tag.strip() for tag in tags.split(',')]


def tag_projects(rs_api: RiverscapesAPI, engine: str, db_path: str) -> Tuple[bool, str]:
    """ Update the tags on a batch of projects
    """

    questions = [
        inquirer.List('engine', message='Cybercastor engine?', choices=name_lookup.keys(), default=engine),
        inquirer.Text("new-tags", message="New tags?", default="2024CONUS"),
        inquirer.List("method", message="Method?", choices=["Batch", 'HUC List']),
        inquirer.List('replace', message='Replace existing tags?', choices=['Yes', 'No'], default='No'),
    ]
    answers = inquirer.prompt(questions)

    clean_tags = clean_tag_string(answers['new-tags'])

    if clean_tags == [''] or len(clean_tags) == 0:
        print('No valid new tags provided')
        sys.exit(1)

    with sqlite3.connect(db_path) as conn:
        curs = conn.cursor()

        if answers['method'] == 'Batch':
            with sqlite3.connect(db_path) as conn:
                curs = conn.cursor()
                curs.execute("""
                    SELECT b.batch_id, b.name, count(bh.batch_id) hucs
                    FROM batches b
                        inner join batch_hucs bh on b.batch_id = bh.batch_id
                    GROUP BY b.name
                    ORDER BY b.name
                """)
            batches = {f'{row[1]} - ID{row[0]} ({row[2]} HUCs)': row[0] for row in curs.fetchall()}
            batch_answers = inquirer.prompt([inquirer.List("batch", message="Batch?", choices=batches.keys()),
                                             inquirer.Text("existing-tags", message="Existing tags?", default="2024CONUS")])
            batch_id = batches[batch_answers['batch']]
            existing_tags = clean_tag_string(batch_answers['existing-tags'])
            tag_clauses = 'AND' + " AND ".join([f" (tags LIKE ('%{tag}%')) " for tag in existing_tags])

            curs.execute(f'''
                SELECT project_id
                FROM batch_hucs bh
                    INNER JOIN rs_projects rp ON bh.huc10 = rp.huc10
                WHERE (project_type_id = ?) AND (batch_id = ?) {tag_clauses}''', [answers['engine'], batch_id])
            project_ids = [row[0] for row in curs.fetchall()]
        else:
            huc_answers = inquirer.prompt([inquirer.Text("huc_list", message="HUC list?")])
            huc_list = huc_answers['huc_list'].split(',')
            curs.execute(f"SELECT project_id FROM rs_projects WHERE project_type_id = ? and huc10 in ({','.join('?' * len(huc_list))})", [answers['engine']])
            project_ids = [row[0] for row in curs.fetchall()]

    if len(project_ids) == 0:
        print('No projects found for the specified engine and HUCs')
        return False, answers['engine']

    project_check = inquirer.prompt([inquirer.List("project_check", message=f'Continue and update {len(project_ids)} projects?', choices=['Yes', 'No'], default='No')])
    if project_check['project_check'] != 'Yes':
        return False, answers['engine']

    get_project_query = rs_api.load_query('getProjectTags')
    tags_mutation = rs_api.load_mutation('updateProject')
    for project_id in project_ids:
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

    continue_answers = inquirer.prompt([inquirer.List("continue", message="Run another batch?", choices=['Yes', 'No'], default='No')])
    return continue_answers['continue'] == 'Yes', answers['engine']


def main():
    """
    Tag projects in Data Exchange with the user specified tag
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('db_path', help='Path to the warehouse dump database', type=str)
    args = dotenv.parse_args_env(parser)

    if not os.path.isfile(args.db_path):
        print(f'Data Exchange project dump database file not found: {args.db_path}')
        sys.exit(1)

    more_projects = True
    current_engine = ''
    with RiverscapesAPI(stage=args.stage) as api:
        while more_projects is True:
            more_projects, current_engine = tag_projects(api, current_engine, args.db_path)


if __name__ == '__main__':
    main()
