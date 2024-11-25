# from rsxml import Logger

from rscommons import Logger  # Logger from rsxml is missing log.success

import dateutil.parser
import inquirer
from riverscapes import RiverscapesAPI

# Key is JSON task script ID. Value is list of warehouse project types
# https://cybercastor.riverscapes.net/engines/manifest.json
engine_projecttype_map = {
    'rs_context': [],
    'rs_context_channel_taudem': [],
    'vbet': ['rscontext', 'channelarea', 'taudem'],
    'brat': ['rscontext', 'hydro_context', 'anthro', 'vbet'],
    'channel': ['rscontext'],
    'confinement': ['rscontext', 'vbet'],
    'anthro': ['rscontext', 'vbet'],
    'hydro_context': ['rscontext', 'vbet'],
    'rcat': ['rscontext', 'vbet', 'taudem', 'anthro'],
    'blm_context': ['rscontext', 'vbet'],
    'rs_metric_engine': ['rscontext', 'vbet', 'riverscapes_brat', 'anthro', 'rcat', 'confinement'],
    'rme_scraper': ['rs_metric_engine', 'rcat'],
}

# Key is warehouse project type. Value is Fargate environment variable
fargate_env_keys = {
    'rscontext': 'RSCONTEXT_ID',
    'channelarea': 'CHANNELAREA_ID',
    'taudem': 'TAUDEM_ID',
    'vbet': 'VBET_ID',
    'brat': 'BRAT_ID',
    'riverscapes_brat': 'BRAT_ID',
    'anthro': 'ANTHRO_ID',
    'hydro_context': 'HYDRO_ID',
    'rcat': 'RCAT_ID',
    'confinement': 'CONFINEMENT_ID',
    'rs_metric_engine': 'RME_ID',
}


def find_upstream_projects(job_data) -> bool:
    """ Find the upstream projects for a given task

    Args:
        job_data (_type_): _description_

    Raises:
        Exception: _description_

    Returns:
        bool: _description_
    """

    log = Logger('Upstream Project Finder')

    # global engine_projecttype_map
    # global fargate_env_keys

    # Verify that the task script is one that we know about
    task_script = job_data['taskScriptId']
    if task_script not in engine_projecttype_map:
        raise Exception(f'Unknown task script {task_script}')

    if 'lookups' not in job_data:
        job_data['lookups'] = {}

    # Initialize the warehouse API that will be used to search for available projects
    riverscapes_api = RiverscapesAPI(stage=job_data['server'])
    search_query = riverscapes_api.load_query('searchProjects')

    errors = []

    org_id = None

    # Loop over all the HUCs in the job
    for huc in job_data['hucs']:

        # Initialize the list of upstream project GUIDs for this HUC
        if huc not in job_data['lookups']:
            job_data['lookups'][huc] = {}

        # Loop over all the project types that we need to find upstream projects for
        for project_type in engine_projecttype_map[task_script]:
            # if
            if fargate_env_keys[project_type] in job_data['lookups'][huc]:
                lookup_val = job_data['lookups'][huc][fargate_env_keys[project_type]]
                log.info(f'Already found project for {huc} of type {project_type}: {lookup_val}. Skipping.')
                continue

            if org_id is not False:
                if org_id is None:
                    limit_by_org = inquirer.confirm('Limit upstream project search by job organization?')
                    if limit_by_org:
                        org_id = job_data['env']['ORG_ID']
                    else:
                        org_id = False

            selected_project = None
            log.info(f'Searching warehouse for project type {project_type} for HUC {huc}')

            # Search for projects of the given type that match the HUC
            searchParams = {
                "projectTypeId": project_type,
                "meta": [{
                    "key": "HUC",
                    "value": huc,
                }]
            }
            if org_id is not None and org_id is not False:
                org = {'id': org_id, "type": "ORGANIZATION"}
                searchParams['ownedBy'] = org

            # Only refresh the token if we need to
            if riverscapes_api.access_token is None:
                # Note: We might have to re-run this if the token expires but it shouldn't happen
                # within the context of a single call so for now leave this alone.
                riverscapes_api.refresh_token()

            results = riverscapes_api.run_query(search_query, {"searchParams": searchParams, "limit": 50, "offset": 0})
            available_projects = results['data']['searchProjects']['results']

            if len(available_projects) < 1:
                msg = f'Could not find project for {huc} of type {project_type}.'
                log.error(msg)
                errors.append(msg)
            elif len(available_projects) == 1:
                log.info(f'Found project for {huc} of type {project_type}: {available_projects[0]["item"]["id"]}')
                selected_project = available_projects[0]['item']['id']
            else:
                # Build a list of the projects that were found. Key is user-friendly string for CLI. Value is project GUID
                inquirer_projects = []
                # sort available projects by created date
                available_projects.sort(key=lambda x: x['item']['createdOn'], reverse=True)

                for available_project in available_projects:
                    project = available_project['item']

                    created_date = project.get('createdOn', None)
                    created_on_str = dateutil.parser.isoparse(project['createdOn']).strftime('%Y-%m-%d %H:%M') if created_date is not None else 'UNKNOWN'

                    version = ''
                    for meta in project['meta']:
                        if meta['key'].lower() == 'modelversion':
                            version = f" (v{meta['value']})"
                            break

                    # User fiendly string for CLI
                    # Example: RSCONTEXT (v1.0) on 2020-01-01 owned by Cybercastor
                    # add to list of projects as a tuple with the form: (label, value)
                    string_parts = [
                        project['name'],
                        version,
                        created_on_str,
                        'owned by',
                        f"{project['ownedBy']['__typename']}:{project['ownedBy'].get('name', 'UNKNOWN')}",
                        f"[{project['id']}]"
                    ]
                    inquirer_projects.append((" ".join(string_parts), project))

                # Prompt user which of the projects they want to use, or quit the process altogether
                choices = list(inquirer_projects)
                choices.append('skip')
                choices.append('quit')
                answers = inquirer.prompt([inquirer.List('Project', message=f'Choose which {project_type} for HUC {huc}?', choices=choices)])
                # Give the user a chance to skip this HUC/project type combo
                if answers['Project'] == 'skip':
                    errors.append(f'Could not find project for {huc} of type {project_type}')
                    continue

                if answers['Project'] == 'quit':
                    riverscapes_api.shutdown()
                    return False
                selected_project = answers['Project']['id']

            # Keep track of the project GUID for this HUC and project type
            if selected_project is not None:
                job_data['lookups'][huc][fargate_env_keys[project_type]] = selected_project

    # If we got to here them we found a project for each HUC and project type
    if riverscapes_api is not None:
        riverscapes_api.shutdown()

    if len(errors) > 0:
        log.title('ERRORS')
        log.error(f'Could not run this job because there were {len(errors)} errors:')
        log.error('You need to resolve these before you can run this job.')
        log.error('======================================================')
        counter = 0
        for error in errors:
            counter += 1
            log.error(f"{counter}. {error}")
        return False

    return True
