import os
from rscommons.util import safe_makedirs
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI

def download_files(stage, proj_type, huc, dl_files):

    riverscapes_api = RiverscapesAPI(stage=stage)
    search_query = riverscapes_api.load_query('searchProjects')
    # Only refresh the token if we need to
    if riverscapes_api.accessToken is None:
        riverscapes_api.refresh_token()

    project_files_query = riverscapes_api.load_query('projectFiles')

    search_params = {
        'projectTypeId': proj_type,
        'meta': [
            {
            'key': 'HUC',
            'value': str(huc)
        }]
    }

    limit = 500
    offset = 0
    total = 0
    while offset == 0 or offset < total:
        results = riverscapes_api.run_query(search_query, {"searchParams": search_params, "limit": limit, "offset": offset})
        total = results['data']['searchProjects']['total']
        offset += limit

        projects = results['data']['searchProjects']['results']
            
        for search_result in projects:

            project = search_result['item']

            file_results = riverscapes_api.run_query(project_files_query, {"projectId": project['id']})
            if not file_results:
                continue
            project_files = file_results['data']['project']['files']

            for meta in project['meta']:
                if meta['key'] == 'HUC':
                    dlhuc = meta['value']
                    break

            for file in project_files:
                files_mess = file['downloadUrl'].split('?')[0]
                file_name = os.path.basename(files_mess)
                if file_name in dl_files:
                    local_path = f'/mnt/c/Users/jordang/Documents/Riverscapes/data/{proj_type}/{dlhuc}/{file_name}'
                    if os.path.exists(local_path):
                        continue
                    safe_makedirs(os.path.dirname(local_path))
                    riverscapes_api.download_file(file, local_path)

# download_files('production', 'brat', 1604010107, 'brat.gpkg')
