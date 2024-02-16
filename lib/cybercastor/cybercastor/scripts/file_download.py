import os
from rsxml import Logger, safe_makedirs
import inquirer
from cybercastor import RiverscapesAPI, RiverscapesSearchParams


def download_files():
    """ Download files from a riverscapes project search

    Args:
        stage (_type_): 'production' or 'staging'
        filedir (_type_): where to save the files
        proj_type (_type_): Machine code for the project type
        huc (_type_): HUC code
        re_filter (_type_): List of regex patterns to match in the file names
    """
    log = Logger('Download Riverscapes Files')
    log.title('Download Riverscapes Files')

    # First gather everything we need to make a search
    # ================================================================================================================

    search_params = RiverscapesSearchParams.load_from_json(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'download_files_search.json'))

    default_dir = os.path.join(os.path.expanduser("~"), 'DownloadedFiles')
    questions = [
        # Also get if this is production or staging (default production)
        inquirer.List('stage', message="Which Data Exchange stage?", choices=['production', 'staging'], default='production'),
        inquirer.Text('download_dir', message="Where do you want to save the downloaded files?", default=default_dir),
    ]
    answers = inquirer.prompt(questions)
    stage = answers['stage']
    download_dir = answers['download_dir']
    safe_makedirs(download_dir)

    file_filters = [r'.*brat\.gpkg']

    # Make the search and download all necessary files
    # ================================================================================================================

    riverscapes_api = RiverscapesAPI(stage=stage)
    riverscapes_api.refresh_token()

    for project, _stats, _total in riverscapes_api.search(search_params):

        # Since we're searching for a huc we can pretty reliably assume that we're only going to get one project
        dlhuc = project.project_meta['HUC']
        if not dlhuc or len(dlhuc.strip()) < 1:
            log.warning(f'No HUC found for project: {project.id}')
            continue
        huc_dir = os.path.join(download_dir, project.project_type, f'{dlhuc}_{project.id}')
        # Note that the files will not be re-downloaded if they already exist.
        riverscapes_api.download_files(project.id, huc_dir, file_filters)

    # Remember to always shut down the API when you're done with it
    riverscapes_api.shutdown()


if __name__ == "__main__":
    download_files()
