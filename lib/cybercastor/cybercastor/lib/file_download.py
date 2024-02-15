import os
from typing import List
from rsxml import Logger
import inquirer
from cybercastor import RiverscapesAPI, RiverscapesSearchParams


def download_files(stage: str, filedir: str, proj_type: str, huc: str, re_filter: List[str]):
    """ Download files from riverscapes

    Args:
        stage (_type_): 'production' or 'staging'
        filedir (_type_): where to save the files
        proj_type (_type_): Machine code for the project type
        huc (_type_): HUC code
        dl_files (_type_): List of files to download. If blank, all files will be downloaded
    """
    log = Logger('Download Riverscapes Files')

    riverscapes_api = RiverscapesAPI(stage=stage)
    riverscapes_api.refresh_token()

    search_params = RiverscapesSearchParams({
        'projectTypeId': proj_type,
        'meta': {
            'HUC': huc
        }
    })

    for project, _stats in riverscapes_api.search(search_params):

        # Since we're searching for a huc we can pretty reliably assume that we're only going to get one project
        dlhuc = project.project_meta['HUC']
        if not dlhuc or len(dlhuc.strip()) < 1:
            log.warning(f'No HUC found for project: {project.id}')
            continue
        huc_dir = os.path.join(filedir, proj_type, f'{dlhuc}_{project.id}')
        # Note that the files will not be re-downloaded if they already exist.
        riverscapes_api.download_files(project.id, huc_dir, re_filter)

    # Remember to always shut down the API when you're done with it
    riverscapes_api.shutdown()


if __name__ == "__main__":
    default_dir = os.path.join(os.path.expanduser("~"), 'MY_PROJECTS')
    questions = [
        # Also get if this is production or staging (default production)
        inquirer.List('stage', message="Which stage?", choices=['production', 'staging'], default='production'),

        # Use inquirer to get a path to the folder we want
        inquirer.Text('filedir', message="Where do you want to save the files?", default=default_dir),
        inquirer.List('proj_type', message="Which project type?", choices=['brat', 'vbet', 'RSContext'], default='brat'),
        # HUC Code
        inquirer.Text('huc', message="What HUC do you want to download?", default='1604010107')
    ]
    answers = inquirer.prompt(questions)

    download_files(answers['stage'], answers['filedir'], answers['proj_type'], answers['huc'], [r'.*brat\.gpkg'])
