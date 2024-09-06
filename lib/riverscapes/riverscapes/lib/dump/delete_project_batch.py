from typing import List
import argparse
import inquirer
from riverscapes import RiverscapesAPI


def delete_project_batch(api: RiverscapesAPI, stage: str, project_ids: List[str]) -> None:
    """Delete a batch of projects from the Riverscapes API"""

    questions = [
        inquirer.Confirm('continue', message=f'Delete {len(project_ids)} projects from {stage}?', default=False),
        inquirer.Text("confirm", message="type the word DELETE"),
        inquirer.Confirm("start_job", message="Start job?", default=False),
    ]

    answers = inquirer.prompt(questions)

    if not answers['continue'] and answers['confirm'] != 'DELETE':
        print('Aborting')
        return

    print(f'Deleting {len(project_ids)} projects from {stage}')

    delete_qry = api.load_mutation('deleteProject')
    for project_id in project_ids:
        api.run_query(delete_qry, {'projectId': project_id, 'options': {}})

    print('Process complete')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='URL to the cybercastor API', type=str, default='production')
    parser.add_argument('project_ids', help='Comma separate list of project GUIDs to delete', type=str)
    args = parser.parse_args()

    project_list = args.project_ids.split(',')

    if len(project_list) == 0:
        raise Exception('No project IDs provided to delete')

    with RiverscapesAPI(stage=args.stage) as api:
        delete_project_batch(api, args.stage, project_list)
