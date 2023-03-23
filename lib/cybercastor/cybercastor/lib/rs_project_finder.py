from .api_new import RiverscapesAPI
from rscommons import Logger
import inquirer



def find_projects(api_url: str, data) -> bool:
    
    upstream_project_types = {
    'rs_context': [],
    'rs_context_channel_taudem': [],
    'vbet': ['RSCONTEXT', 'CHANNEL_AREA', 'TAUDEM'],
    'brat': ['RSCONTEXT', 'VBET',],
    'channel': ['RSCONTEXT'],
    'confinement': ['RSCONTEXT', 'VBET'],
    }
    
    log = Logger('Project Finder')
    
    riverscapes_api = RiverscapesAPI('https://api.warehouse.riverscapes.net/staging')
    riverscapes_api.refresh_token()

    task_script = data['taskScriptId']
    if task_script not in upstream_project_types:
        raise Exception(f'Unknown task script {task_script}')
    
    # These are the project types that we need for this task script
    upstream_project_types = upstream_project_types[task_script]

    for huc in data['hucs']:
        for project_type in upstream_project_types:
            log.info(f'Searching warehouse for project type {project_type} for HUC {huc}')
            
            search_params = {
            "projectTypeId": project_type
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
                    __typename
                  }
                  #
                }
              }
            }
          }
          """

            results = riverscapes_api.run_query(query, {"searchParams": {"projectTypeOd": "rscontext"}})

            available_projects = {
                 'My cool project (v1.1.0) on 12 Dec 2022': '6db44465-4bc2-46b5-9160-3a41d8178e45',
                'Another version (v0.86) on 19 Jan 2021': '85ea995d-dc16-436e-bd9e-cf2123985dfd'
            }

            choices = list(available_projects.keys())
            choices.append('quit')

            questions = [
            inquirer.List('Project', message=f'Which {project_type} for HUC {huc}?', choices=choices)]
            answers = inquirer.prompt(questions)

            if answers['Project'] == 'quit':
                return False
            




            if project is None:
                raise Exception(f'Could not find project for {huc} and {project_type}')
            data[project_type] = project


            riverscapes_api.shutdown()

    