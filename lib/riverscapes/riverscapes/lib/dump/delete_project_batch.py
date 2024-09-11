"""
Delete one or more projects from the Riverscapes Data Exchange.

This query can be used in Philip's data warehouse SQLite dump to identify the duplicate projects.

WITH DuplicateProjects AS (SELECT project_id,
                                  ROW_NUMBER() OVER (PARTITION BY project_type_id, huc10 ORDER BY created_on DESC) AS row_num
                           FROM rs_projects
                           WHERE tags = '2024CONUS')
SELECT *
FROM rs_projects
WHERE project_id IN (SELECT project_id
                     FROM DuplicateProjects
                     WHERE row_num > 1);

"""

from typing import List
import argparse
import sqlite3
import inquirer
from rsxml import dotenv
from riverscapes import RiverscapesAPI


def delete_project_batch(rs_api: RiverscapesAPI, stage: str, db_path: str, project_ids: List[str]) -> None:
    """Delete a batch of projects from the Riverscapes API"""

    questions = [
        inquirer.Confirm('continue', message=f'Delete {len(project_ids)} projects from {stage}?', default=False),
        inquirer.Text("confirm", message="type the word DELETE"),
        inquirer.Confirm("delete_local", message="Delete projects from local DB?", default=True),
        inquirer.Confirm("start_job", message="Start job?", default=False),
    ]

    answers = inquirer.prompt(questions)

    if not answers['continue'] and answers['confirm'] != 'DELETE':
        print('Aborting')
        return

    print(f'Deleting {len(project_ids)} projects from {stage}')

    if answers['delete_local'] is True:
        conn = sqlite3.connect(db_path) if answers['delete_local'] else None
        curs = conn.cursor()
        curs.execute("PRAGMA foreign_keys = ON;")

    not_found = 0
    deleted = 0
    delete_qry = rs_api.load_mutation('deleteProject')
    for project_id in project_ids:
        try:
            result = rs_api.run_query(delete_qry, {'projectId': project_id, 'options': {}})
            if result is None or result['data']['deleteProject']['error'] is not None:
                raise Exception(result['data']['deleteProject']['error'])
            else:
                deleted += 1
                if answers['delete_local']:
                    curs.execute('DELETE FROM rs_projects WHERE project_id = ?', [project_id])
        except Exception as e:
            if e is not None and 'not found' in str(e):
                not_found += 1
            else:
                raise e

    if conn is not None:
        conn.commit()

    print(f'Process complete. {deleted} projects deleted. {not_found} projects not found.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='URL to the cybercastor API', type=str, default='production')
    parser.add_argument('db_path', help='Path to local SQLite warehouse dump', type=str)
    parser.add_argument('project_ids', help='Comma separate list of project GUIDs to delete', type=str)
    args = dotenv.parse_args_env(parser)

    project_list = args.project_ids.split(',')

    if len(project_list) == 0:
        raise Exception('No project IDs provided to delete')

    with RiverscapesAPI(stage=args.stage) as api:
        delete_project_batch(api, args.stage, args.db_path, project_list)
