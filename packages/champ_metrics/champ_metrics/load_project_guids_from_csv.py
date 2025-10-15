"""
Utility script to load project GUIDs from a CSV file in a specified folder.
The goal is to reuse this code in other scripts that need to load project GUIDs from CSV files.

Philip Bailey
15 Oct 2025

"""

import os
import inquirer


def load_project_guids_from_csv(csv_folder: str) -> list[str]:
    """Load project GUIDs from a CSV file in the specified folder"""

    if not os.path.exists(csv_folder):
        print(f'The folder {csv_folder} does not exist. Please provide a valid folder with CSV files.')
        return

    # Get a list of all CSV files in the specified folder. Do not walk to subfolders.
    csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    if not csv_files:
        print(f'No CSV files found in {csv_folder}. Please provide a valid folder with CSV files.')
        return

    answers = inquirer.prompt([inquirer.List("csv_path", message="Select a CSV file to use", choices=csv_files)])
    if not answers:
        print('Aborting')
        return

    try:
        csv_path = os.path.join(csv_folder, answers['csv_path'])
        project_ids = []
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            for line in csvfile:
                project_id = line.strip()
                if project_id:
                    project_ids.append(project_id)
    except Exception as e:
        print(f'Error reading CSV file: {e}')
        return

    return project_ids
