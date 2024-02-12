""" This script demonstrates how to search for projects on the server
"""
import os
import json
import time
from rsxml import Logger
from termcolor import colored
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
        log.debug(f"Project {project.id} has {len(project.files)} files")

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
    searched_project_ids = [p.id for p, _stats in api.search(search_params, progress_bar=True)]
    log.debug(f"Found {len(searched_project_ids)} projects")

    # Collect all the metadata for each project by id
    # ====================================================================================================
    log.info("Searching for projects by collecting them")
    search_params = {
        "projectTypeId": "vbet",
        # Used https://geojson.io to get a rough bbox to limit this query roughing to washington state (which makes the query cheaper)
        "bbox": [-125.40936477595693, 45.38966396117303, -116.21237724715607, 49.470853578429626]
    }
    searched_project_meta = {p.id: p.project_meta for p, _stats in api.search(search_params, progress_bar=True)}

    log.info(f"Found {len(searched_project_meta)} projects")


def simple_search_with_cache(api: RiverscapesAPI):
    """Simple search with cache examples

    If you want to cache the search results to a file and use them later, here's how you can do it.
    This is useful if you're going to use the same data multiple times and you don't want to query the server
    or if you want to keep a record of the data you've queried.

    Args:
        api (RiverscapesAPI): _description_
    """
    log = Logger('Simple Search with Cache')

    search_params = {
        "projectTypeId": "vbet",
        # Used https://geojson.io to get a rough bbox to limit this query roughing to washington state (which makes the query cheaper)
        "bbox": [-125.40936477595693, 45.38966396117303, -116.21237724715607, 49.470853578429626]
    }

    # I the file creation date is younger than 6 hours then use it
    cache_filename = 'my_awesome_search_results.json'
    allowed_age = 6 * 3600  # 6 hours in seconds
    data = None
    if os.path.exists(cache_filename) and (os.path.getmtime(cache_filename) - time.time()) < allowed_age:
        # Load the data from the file
        with open(cache_filename, 'r', encoding='utf8') as f:
            data = json.load(f)
    else:
        data = {p.id: p.project_meta for p, _stats in api.search(search_params, progress_bar=True)}
        # Save the data to a file for later
        with open(cache_filename, 'w', encoding='utf8') as f:
            json.dump(data, f, indent=2)

    # Now use the data
    log.debug(f"Found {len(data)} projects")


def retrieve_project(api: RiverscapesAPI):
    """ Simple retrieve project examples

    Args:
        api (RiverscapesAPI): _description_
    """
    log = Logger('Retrieve Project')
    # Get a full project record. This is a MUCH heavier query than what comes back from the search results
    # But it does include:
    #     - datasets
    #     - the project tree (sed to populate the web viewer (formerly WebRAVE))
    #     - the collections this project belongs to
    #     - all the project files
    #     - qaqc data
    # ====================================================================================================
    full_project = api.get_project_full("507916e1-b81d-4803-89d0-ccd65f6219e9")
    log.debug(full_project)

    # Get Just the files corresponding to a project. This is a much cheaper query than the full query above
    # and it should give you everything including a download url, size, etag etc for each file
    # ====================================================================================================
    project_files = api.get_project_files("507916e1-b81d-4803-89d0-ccd65f6219e9")
    log.debug(project_files)


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

    # Collect the metadata for each project (of type RiverscapesProject) into a dictionary with HUC as the key This will look like:
    # {
    #     "vbet": {
    #         "17060304": [proj1, proj2, proj3],
    #         "17040302": [proj4, proj5, proj6],
    #         ...
    #     },
    #     ...
    # }
    huc_lookup = {}
    for project, _stats in api.search(search_params, progress_bar=True):
        if project.project_type is None:
            raise Exception(f"Project {project.id} has no project type. This is likely a query error")

        if project.huc is not None and project.project_type is not None:
            huc_lookup.setdefault(project.project_type, {}).setdefault(project.huc, []).append(project)
        else:
            log.warning(f"Project {project.id} has no HUC")

    # Just a little helper function for printing out the project
    def proj_print_str(proj: RiverscapesProject):
        return f"{proj.project_type} [{proj.huc}] <{proj.model_version}> ({proj.id})"

    deletable_projects = []
    # Now we go through the huc_lookup and find the projects that can be deleted
    # For every project type and huc, we sort the projects by date and version and then keep the latest
    # one and return everything else as "deletable"
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


if __name__ == '__main__':
    mainlog = Logger('Search Projects')
    mainlog.title('Demo script to search for projects on the serverand delete them.')

    # Instantiate your API once and then pass it around.
    riverscapes_api = RiverscapesAPI(stage='production')
    riverscapes_api.refresh_token()

    # Examples
    try:
        # simple_search(riverscapes_api)
        retrieve_project(riverscapes_api)
        simple_search_with_cache(riverscapes_api)
        # find_duplicates(riverscapes_api)
    except Exception as e:
        mainlog.error(e)
    finally:
        # Remember to shut down the API to stop the polling process that refreshes the token
        # If you put it inside a finally block it will always run (even if there's an error or a keyboard interrupt like ctrl+c)
        riverscapes_api.shutdown()

    mainlog.info("Done!")
