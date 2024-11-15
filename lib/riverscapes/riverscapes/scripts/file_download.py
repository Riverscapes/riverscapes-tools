import os
from rsxml import Logger, safe_makedirs
import inquirer
from riverscapes import RiverscapesAPI, RiverscapesSearchParams


def download_files(riverscapes_api: RiverscapesAPI):
    """ Download files from a riverscapes project search (not all files, just the ones that match our regex filters)

        To run this file in VSCode choose "Python: Current File (Cybercastor)" from the command palette

    """
    log = Logger('Download Riverscapes Files')
    log.title('Download Riverscapes Files')

    # First gather everything we need to make a search
    # ================================================================================================================

    # Load the search params from a JSON file so we don't have to hardcode them
    search_params = RiverscapesSearchParams.load_from_json(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'download_files_search.json'))

    default_dir = os.path.join(os.path.expanduser("~"), 'DownloadedFiles', riverscapes_api.stage)
    questions = [
        inquirer.Text('download_dir', message="Where do you want to save the downloaded files?", default=default_dir),
    ]
    answers = inquirer.prompt(questions)
    download_dir = answers['download_dir']
    safe_makedirs(download_dir)

    # NOTE: File filters is a list of regexes. If any one of the regexes matches any file in a project it will be downloaded
    file_filters = [r'.*brat\.gpkg']

    # Make the search and download all necessary files
    # ================================================================================================================

    for project, _stats, _total, _prg in riverscapes_api.search(search_params):

        # Since we're searching for a huc we can pretty reliably assume that we're only going to get one project
        dlhuc = project.project_meta['HUC']
        if not dlhuc or len(dlhuc.strip()) < 1:
            log.warning(f'No HUC found for project: {project.id}')
            continue
        huc_dir = os.path.join(download_dir, project.project_type, f'{dlhuc}_{project.id}')
        # Note that the files will not be re-downloaded if they already exist.
        riverscapes_api.download_files(project.id, huc_dir, file_filters)


if __name__ == "__main__":
    with RiverscapesAPI() as api:
        download_files(api)
