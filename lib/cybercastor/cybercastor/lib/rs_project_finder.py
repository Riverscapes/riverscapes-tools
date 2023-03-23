from .api_new import RiverscapesAPI
from rscommons import Logger
import dateutil.parser
import inquirer

# Key is JSON task script ID. Value is list of warehouse project types  
upstream_project_types = {
    'rs_context': [],
    'rs_context_channel_taudem': [],
    'vbet': ['rscontext', 'channelarea', 'taudem'],
    'brat': ['rscontext', 'vbet',],
    'channelarea': ['rscontext'],
    'confinement': ['rscontext', 'vbet'],
}

# Key is warehouse project type. Value is Fargate environment variable
fargate_env_keys = {
    'rscontext': 'RSCONTEXT_ID' ,
    'channelarea': 'CHANNEL_AREA_ID',
    'taudem': 'TAUDEM_ID',
    'vbet': 'VBET_ID',
    'brat': 'BRAT_ID'
}

query = """
  query searchProjects_query(
    $searchParams: ProjectSearchParamsInput!
    $sort: [SearchSortEnum!]
    ) {
      searchProjects(limit: 50, offset: 0, params: $searchParams, sort: $sort) {
    results {
      item {
        id
        name
        meta {
          key
          value
        }
        projectType {
          id
        }
        createdOn
        ownedBy {
          ... on Organization {
            name
          }
          ... on User {
            name
          }
          __typename
        }
        #
      }
    }
  }
}
"""

def find_upstream_projects(api_url: str, job_data) -> bool:
    
    log = Logger('Upstream Project Finder')
    
    global upstream_project_types
    global fargate_env_keys

    # Initialize the warehouse API that will be used to search for available projects
    riverscapes_api = RiverscapesAPI('https://api.warehouse.riverscapes.net/staging')
    riverscapes_api.refresh_token()

    # Verify that the task script is one that we know about
    task_script = job_data['taskScriptId']
    if task_script not in upstream_project_types:
        raise Exception(f'Unknown task script {task_script}')

    # Loop over all the HUCs in the job
    for huc in job_data['hucs']:
        
        # Initialize the list of upstream project GUIDs for this HUC
        upstream_project_ids = {}

        # Loop over all the project types that we need to find upstream projects for
        for project_type in upstream_project_types[task_script]:
            log.info(f'Searching warehouse for project type {project_type} for HUC {huc}')
            
            # Search for projects of the given type that match the HUC
            params = {
                "projectTypeId": project_type,
                "meta": [{
                    "key": "huc",
                    "value": huc,
                }]
            }
            results = riverscapes_api.run_query(query, {"searchParams": params})
            available_projects = results['data']['searchProjects']['results']

            if len(available_projects) <1 :
                log.error(f'Aborting. Could not find project for {huc} of type {project_type}.')
                return False
            elif len(available_projects) == 1:
                selected_project = available_projects[0]['item']['ID']
            else:  
                # Build a list of the projects that were found. Key is user-friendly string for CLI. Value is project GUID
                inquirer_projects = {}
                for available_project in available_projects:
                    project = available_project['item']
                    
                    created_date = project.get('createdOn', None)
                    created_on_str = dateutil.parser.isoparse(project['createdOn']).strftime('%Y-%m-%d') if created_date is not None else 'UNKNOWN'
                    
                    version = ''
                    for meta in project['meta']:
                        if meta['key'].lower() == 'modelversion':
                            version = f" (v{meta['value']})"
                            break
                    
                    # User fiendly string for CLI
                    # Example: RSCONTEXT (v1.0) on 2020-01-01 owned by Cybercastor
                    inquirer_projects[f"{project['name']}{version} on {created_on_str} owned by {project['ownedBy'].get('name', 'UNKNOWN')}"] = project['id']

                # Prompt user which of the projects they want to use, or quit the process altogether
                choices = list(inquirer_projects.keys())
                choices.append('quit')
                answers = inquirer.prompt([inquirer.List('Project', message=f'Which {project_type} for HUC {huc}?', choices=choices)])

                if answers['Project'] == 'quit':
                    return False
                
                selected_project = inquirer_projects[answers['Project']]

            # Keep track of the project GUID for this HUC and project type
            upstream_project_ids[fargate_env_keys[project_type]] = selected_project

            if project is None:
                raise Exception(f'Could not find project for {huc} and {project_type}')
            job_data['upstream_projects'] = upstream_project_ids

    # If we got to here them we found a project for each HUC and project type
    riverscapes_api.shutdown()
    return True
