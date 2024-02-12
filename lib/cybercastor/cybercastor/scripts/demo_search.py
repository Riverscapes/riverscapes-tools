""" This script demonstrates how to search for projects on the server
"""
import json
import semver
from rsxml import Logger
from termcolor import colored
from dateutil.parser import parse as dateparse
from cybercastor.classes.RiverscapesAPI import RiverscapesAPI, RiverscapesProject


def simple_search(api: RiverscapesAPI):
    """ Simple search examples

    Args:
        api (RiverscapesAPI): _description_
    """
    log = Logger('Simple Search')
    # Set yp your search params
    search_params = {
        # keywords: "",          # [String]. Will search inside name, description, summary, meta data keys, metadata values, id and tags
        # name: "",              # [String]. Will search within the project name only
        # "editableOnly": True,  # [Boolean] Filter to Only items that I can edit as a user
        # "createdOn": {
        #     "from": "2024-01-01T00:00:00Z",
        # },                     # [SearchDate] Search by date {from, to}. If both from AND to are provided it will search inside a window.
        #                                       Otherwise it will search before, or after the date provided in the from or to field respecitvely
        # "updatedOn": {"from": "" },  [SearchDate] # search by date {from, to}
        # collection: "ID" # Filter to projects inside a collection
        # bbox: [minLng, minLat, maxLng, maxLat]
        # "projectTypeId": "vbet",
        "meta": [
            {
                "key": "Runner",
                "value": "Cybercastor",
            },
            # {
            #     "key": "ModelVersion",
            #     "value": "3.2.0",
            # }
        ],
        # "tags": ["tag1", "tag2"],  # AND query for tags
        # "ownedBy": {
        #     "id": "USER/ORGID",
        #     "type": "USER"  # or "ORGANIZATION"
        # }
    }

    # Here's a QUICK query to get a count without looping over everything
    # Really useful when you want summaries of the data. Average query time is < 100ms
    # ====================================================================================================
    search_count, stats = api.search_count(search_params)
    log.info(f"Found {search_count:,} projects")
    log.info(json.dumps(stats, indent=2))

    # Loop over each project and "DO" somewthing with each one
    # Note how the search function yields a project and a stats object. The query pagination is handled for you
    # ====================================================================================================
    search_params = {
        "projectTypeId": "vbet",
    }
    for project, stats in api.search(search_params, progress_bar=True):
        # Do a thing (like tag the project, delete it etc.)
        # INSERT THING DOING HERE
        pass

    # Collect all projects together first. This is useful if you want to do a lot of things with the projects
    # or query the metadata of each project to filter it down further.
    # ====================================================================================================
    search_params = {
        "projectTypeId": "vbet",
        "meta": [
            {
                "key": "Runner",
                "value": "Cybercastor",
            }
        ],
    }
    searched_projects = [p['id'] for p in api.search(search_params, progress_bar=True)]

    # Collect all the metadata for each project by id
    # ====================================================================================================
    log.info("Searching for projects by collecting them")
    search_params = {
        "projectTypeId": "vbet",
        # Used https://geojson.io to get a rough bbox to limit this query roughing to washington state (which makes the query cheaper)
        "bbox": [-125.40936477595693, 45.38966396117303, -116.21237724715607, 49.470853578429626]
    }
    searched_project_meta = {p['id']: p['meta'] for p, _stats in api.search(search_params, progress_bar=True)}


def retrieve_project(api: RiverscapesAPI):
    """ Simple retrieve project examples

    Args:
        api (RiverscapesAPI): _description_
    """
    # Get a full project record. This is a MUCH heavier query than what comes back from the search results
    # But it does include:
    #     - datasets
    #     - the project tree (sed to populate the web viewer (formerly WebRAVE))
    #     - the collections this project belongs to
    #     - all the project files
    #     - qaqc data
    # ====================================================================================================
    full_project = api.get_project_full("507916e1-b81d-4803-89d0-ccd65f6219e9")

    # Get Just the files corresponding to a project. This is a much cheaper query than the full query above
    # and it should give you everything including a download url, size, etag etc for each file
    # ====================================================================================================
    project_files = api.get_project_files("507916e1-b81d-4803-89d0-ccd65f6219e9")


def find_duplicates(api: RiverscapesAPI):
    """ Finding duplicate projects

    Args:
        api (RiverscapesAPI): _description_

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """
    log = Logger('Find Duplicates')

    # Here's a more complex search example: Search for projects that can be deleted because they have a newer version
    # ====================================================================================================
    # Get all the projects that match the search criteria
    search_params = {
        # "createdOn": {
        #     "from": "2024-01-01T00:00:00Z",
        # }
    }

    # Collect the metadata for each project into a dictionary with HUC as the key
    huc_lookup = {}
    for project, _stats in api.search(search_params, progress_bar=True):
        if project.project_type is None:
            raise Exception(f"Project {project.id} has no project type. This is likely a query error")

        if project.huc is not None and project.project_type is not None:
            if project.project_type not in huc_lookup:
                huc_lookup[project.project_type] = {}
            if project.huc not in huc_lookup[project.project_type]:
                huc_lookup[project.project_type][project.huc] = []
            huc_lookup[project.project_type][project.huc].append(project)
        else:
            log.warning(f"Project {project.id} has no HUC")

    def proj_print_str(proj: RiverscapesProject):
        return f"{proj.project_type} [{proj.huc}] <{proj.model_version}> ({proj.id})"

    deletable_projects = []
    for project_type, hucs in huc_lookup.items():
        for huc, projects in hucs.items():
            # Only consider projects with more than one version
            if len(projects) > 1:
                # Sort the projects by date and version descending order
                projects.sort(key=lambda x: (project.model_version, project.created_date), reverse=True)
                # Now delete the first list item, leaving only deletable projects behind
                log.info(f"Found {len(projects)} projects for {project_type} in HUC {huc}")
                log.info(colored(f"    KEEP: {proj_print_str(projects[0])} is the latest version", "green"))
                for proj in projects[1:]:
                    log.info(colored(f"  DELETE: {proj_print_str(proj)} is an older version", "red"))
                    deletable_projects.append(proj)


def demo_search(stage: str):
    """ Here is a demo script on how to search the API

    Args:
        stage (str): _description_
    """

    log = Logger('Search Projects')
    log.title('Demo script to search for projects on the serverand delete them.')

    # Instantiate your API
    riverscapes_api = RiverscapesAPI(stage=stage)
    # Only refresh the token if we need to
    riverscapes_api.refresh_token()

    # Simple search examples
    # simple_search(riverscapes_api)
    # retrieve_project(riverscapes_api)

    find_duplicates(riverscapes_api)

    # Remember to shut down the API to stop the polling process that refreshes the token
    riverscapes_api.shutdown()

    log.info("Done!")


if __name__ == '__main__':
    demo_search('production')
