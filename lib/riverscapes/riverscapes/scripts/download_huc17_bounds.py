"""
Downloads all HUC 17 project bounds from Riverscapes API and saves them as GeoJSON files.
This was to support Dawn Urycki at NOAA for her work on the Columbia River Basin.
Philip Bailey
19 March 2025
"""
import os
import argparse
import requests
from rsxml import dotenv
from riverscapes import RiverscapesAPI, RiverscapesSearchParams


def main(api: RiverscapesAPI, output_folder: str):
    """Search for all projects in the Columbia River Basin and download their bounds as GeoJSON files"""

    params = RiverscapesSearchParams({
        "projectTypeId": "rscontext",
        "tags": ["2024CONUS"],
    })

    projects = {}
    for project, _stats, _total, _prg in api.search(params, progress_bar=True, page_size=100):
        if project.huc.startswith('17'):
            projects[project.huc] = project.id

    qry = api.load_query('getProjectBounds')
    for huc10, project_id in projects.items():
        result = api.run_query(qry, {'id': project_id})

        url = result['data']['project']['bounds']['polygonUrl']
        response = requests.get(url, timeout=90)
        response.raise_for_status()

        if not os.path.isdir(output_folder):
            os.makedirs(output_folder)

        save_path = os.path.join(output_folder, f'{huc10}_bounds.geojson')
        with open(save_path, 'w', encoding='utf-8') as file:
            file.write(response.text)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output_folder', help='Folder where downloaded project_bounds.geojson will be saved', type=str)
    args = dotenv.parse_args_env(parser)

    with RiverscapesAPI(stage='production') as rs_api:
        main(rs_api, args.output_folder)
