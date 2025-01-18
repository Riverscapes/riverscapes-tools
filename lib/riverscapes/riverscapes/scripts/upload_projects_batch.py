"""
Search recursively for project.rs.xml files under a given folder and upload them to Riverscapes
Data Exchange

Philip Bailey
16 Jan 2025
"""
from typing import List
import os
import subprocess
import inquirer


def find_project_files(base_folder: str) -> List[str]:
    """
    Recursively find riverscapes project files
    """
    project_files = []
    for root, _, files in os.walk(base_folder):
        for file_name in files:
            if file_name == 'project.rs.xml':
                project_files.append(os.path.join(root, file_name))

    return project_files


def main():
    """Ask user for the top level folder to process and the owner GUID to use for the projects"""

    questions = [
        inquirer.Text('folder', message="Top level folder under which to look for project.rs.xml files?"),
        inquirer.List('owner_type', message="Owner type for the projects?", choices=['User', 'Organization'], default='Organization'),
        inquirer.Text('owner', message="Owner GUID for the projects?"),
        inquirer.List('visibility', message="Visibility for the projects?", choices=['PUBLIC', 'PRIVATE', 'SECRET'], default='PRIVATE'),
        inquirer.Text('tags', message="Tags for the projects?"),
    ]
    answers = inquirer.prompt(questions)

    if answers['folder'] is None or answers['folder'] == '' or not os.path.isdir(answers['folder']):
        print("Valid folder is required")
        return

    project_files = find_project_files(answers['folder'])

    if len(project_files) == 0:
        print("No project files found")
        return

    if answers['owner'] is None or answers['owner'] == '':
        print("Owner GUID is required")
        return

    tags = [tag.strip() for tag in answers['tags'].split(',')]
    tags = [tag.replace(' ', '') for tag in tags if tag != '']
    tags = f'--tags {",".join(tags)}' if len(tags) > 0 else ''

    for project_file in project_files:
        cmd = f'rscli upload {project_file} --no-input --{'org' if answers['owner_type'] == 'Organization' else 'user'} {answers["owner"]} --visibility {answers["visibility"]} {tags}'
        try:
            print(cmd)
            # subprocess.run(cmd, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error processing {project_file}: {e}")


if __name__ == "__main__":
    main()
