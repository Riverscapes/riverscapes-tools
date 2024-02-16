""" This script demonstrates how to search for projects on the server

NOTE: We set max_results=1234 on all these queries for demo purposes. You probably don't want to do that in production

    To run this file in VSCode choose "Python: Current File (Cybercastor)" from the command palette


"""
import os
import json
import time
from rsxml import Logger
from termcolor import colored
from cybercastor import RiverscapesAPI, RiverscapesProject, RiverscapesSearchParams

log = Logger('Search Projects')


def simple_search(api: RiverscapesAPI):
    """ Simple search examples

    Args:
        api (RiverscapesAPI): _description_
    """
    # Set yp your search params

    # EXAMPLE: Here's a QUICK query to get a count without looping over everything
    # Really useful when you want summaries of the data. Average query time is < 100ms
    # ====================================================================================================
    log.title("Quick search count")
    search_count, stats = api.search_count(RiverscapesSearchParams({
        "meta": {
            "Runner": "Cybercastor",
        }
    }))
    log.info(f"Found {search_count:,} projects")
    log.info(json.dumps(stats, indent=2))

    # EXAMPLE: Here's an example of all possible search parameters (this will return 0 results because it's too specific)
    # NOTE: RiverscapesSearchParams will throw errors for common mistakes and anything it doesn't recognize as valid
    # ====================================================================================================
    log.title("All possible search parameters")
    search_count = [x for x, _stats, _total in api.search(RiverscapesSearchParams({
        "projectTypeId": "vbet",  # Only return projects of this type
        "keywords": "my search terms",  # This will give a warning since keyword searches are not that useful in programmatic searches
        "name": "my project name",
        "editableOnly": True,  # Only return projects that I can edit
        "createdOn": {
            "from": "2024-01-02",  # Any datetime string parseable by python (Optional)
            "to": "2024-01-03"  # Any datetime string parseable by python (Optional)
        },
        "updatedOn": {
            "from": "2024-01-03T01:04:56Z",  # Any datetime string parseable by python (Optional)
            "to": "2024-01-04T03:04:56Z"  # Any datetime string parseable by python (Optional)
        },
        "collection": "00000000-0000-0000-0000-000000000000",  # Only return projects that are in this collection (by id)
        "ownedBy": {
            "id": "00000000-0000-0000-0000-000000000000",  # Only return projects that are owned by this user (by id)
            "type": "USER"  # "USER" or "ORGANIZATION"
        },
        "tags": ["tag1", "tag2"],  # Only return projects that have these tags
        "bbox": [-125.40936477595693, 45.38966396117303, -116.21237724715607, 49.470853578429626],  # A bounding box to limit the search
        "meta": {
            "Runner": "Cybercastor",
            "HUC": "17060304"
        }
    }))]

    # EXAMPLE: You can also load search parameters from a json file if you want to keep it out of .git or if it changes frequently
    # ====================================================================================================
    loaded_search_params = RiverscapesSearchParams.load_from_json(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'DEMO_search.json'))
    log.debug(json.dumps(loaded_search_params.to_gql(), indent=2))

    # EXAMPLE: Loop over each project and "DO" somewthing with each one
    # Note how the search function yields a project and a stats object. The query pagination is handled for you
    #
    # NB: We set max_results here for demo purposes so this doesn't take 20 minutes but you probably don't want to do that in production
    # ====================================================================================================
    log.title("Loop over each project and \"DO\" somewthing with each one")
    for project, stats, _total in api.search(RiverscapesSearchParams({"projectTypeId": "vbet"}), progress_bar=True, max_results=1234):
        # Do a thing (like tag the project, delete it etc.)
        # INSERT THING DOING HERE
        log.debug(f"Project {project.id} has {len(project.json['tags'])} tags")

    # EXAMPLE: Collect all projects together first. This is useful if you want to do a lot of things with the projects
    # or query the metadata of each project to filter it down further.
    # ====================================================================================================
    log.title("Collect all projects together first")
    search_params = RiverscapesSearchParams({
        "projectTypeId": "vbet",
        "meta": {
            "Runner": "Cybercastor",
        }
    })
    searched_project_ids = [p.id for p, _stats, _total in api.search(search_params, progress_bar=True, max_results=1234)]
    log.debug(f"Found {len(searched_project_ids)} projects")

    # EXAMPLE Collect all the metadata for each project by id
    # ====================================================================================================
    log.title("Collect all the metadata for each project by id")
    search_params = RiverscapesSearchParams({
        "projectTypeId": "vbet",
        # Used https://geojson.io to get a rough bbox to limit this query roughing to washington state (which makes the query cheaper)
        "bbox": [-125.40936477595693, 45.38966396117303, -116.21237724715607, 49.470853578429626]
    })
    searched_project_meta = {p.id: p.project_meta for p, _stats, _total in api.search(search_params, progress_bar=True, max_results=1234)}

    log.info(f"Found {len(searched_project_meta)} projects")


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
    log.title("Get a full project record")
    full_project = api.get_project_full("507916e1-b81d-4803-89d0-ccd65f6219e9")
    log.debug(full_project)

    # Get Just the files corresponding to a project. This is a much cheaper query than the full query above
    # and it should give you everything including a download url, size, etag etc for each file
    # ====================================================================================================
    log.title("Get Just the files corresponding to a project")
    project_files = api.get_project_files("507916e1-b81d-4803-89d0-ccd65f6219e9")
    log.debug(project_files)


def simple_search_with_cache(api: RiverscapesAPI):
    """Simple search with cache examples

    If you want to cache the search results to a file and use them later, here's how you can do it.
    This is useful if:

        - You're going to use the same data multiple times and you don't want to query the server
        - If you want to keep a record of the data you've queried.
        - If you don't always need the freshest data (e.g. you're doing a demo or a test)

    Args:
        api (RiverscapesAPI): _description_
    """
    log.title("Simple search with file-based cache")
    search_params = RiverscapesSearchParams({
        "projectTypeId": "vbet",
        # Used https://geojson.io to get a rough bbox to limit this query roughing to washington state (which makes the query cheaper)
        "bbox": [-125.40936477595693, 45.38966396117303, -116.21237724715607, 49.470853578429626]
    })

    # I the file creation date is younger than 6 hours then use it
    cache_filename = 'my_awesome_search_results.json'
    allowed_age = 6 * 3600  # 6 hours in seconds
    data = None
    if os.path.exists(cache_filename) and (os.path.getmtime(cache_filename) - time.time()) < allowed_age:
        # Load the data from the file
        log.info(f"Using cached data from {cache_filename}")
        with open(cache_filename, 'r', encoding='utf8') as f:
            data = json.load(f)
    else:
        log.info("Querying the server for fresh data")
        data = {p.id: p.project_meta for p, _stats, _total in api.search(search_params, progress_bar=True, max_results=1234)}
        # Save the data to a file for later
        with open(cache_filename, 'w', encoding='utf8') as f:
            json.dump(data, f, indent=2)

    # Now use the data
    log.debug(f"Found {len(data)} projects")


def stream_to_file(api: RiverscapesAPI):
    """ 

    Sometimes pulling the whole database and storing it in memory will crash your computer.
    In these cases you can use the search function to write the results to a file and then read from that file later.

    You can also combine this with the (simple_search_with_cache) method above for extra lean and mean searching.

    Args:
        api (RiverscapesAPI): _description_
    """
    log.title("File based search")
    # Set yp your search params
    search_params = RiverscapesSearchParams({
        # "createdOn": {
        #     "from": "2024-01-01T00:04:56Z",
        # }
    })

    with open('SEARCH_FULL_RECORDS.json', 'w', encoding='utf8') as f_json, \
            open('SEARCH_ONELINE_SUMMARY.csv', 'w', encoding='utf8') as f_csv:
        f_json.write('[\n')
        f_csv.write("id, project_type, huc, model_version, created_date\n")
        counter = 0
        for proj, _stats, _total in api.search(search_params, progress_bar=True, max_results=1234):
            if counter > 0:
                f_json.write(',\n')
            json.dump(proj.json, f_json, indent=2)
            f_csv.write(f"{proj.id}, {proj.project_type},{proj.huc},{proj.model_version},{proj.created_date}\n")
            counter += 1
        f_json.write(']\n')

    log.info("Done")


def find_duplicates(api: RiverscapesAPI):
    """ Finding duplicate projects

    Args:
        api (RiverscapesAPI): _description_

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """
    log.title("Finding duplicate projects")
    # Here's a more complex search example: Search for projects that can be deleted because they have a newer version
    # ====================================================================================================
    # Get all the projects that match the search criteria
    search_params = RiverscapesSearchParams({
        "createdOn": {
            "from": "2024-01-01T00:00:00Z",
        }
    })

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
    for project, _stats, _total in api.search(search_params, progress_bar=True):
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

    # Instantiating the API (Method 1)
    # ====================================================================================================

    # You can instantiate the API like this but you need to call refresh_token
    # Note how it will ask y ou interactively for the stage. Specify the stage like this:
    #   riverscapes_api = RiverscapesAPI(stage='dev') to not get the prompt
    # 
    # Also yout need to call riverscapes_api.shutdown() when you're done with it.
    riverscapes_api = RiverscapesAPI()
    riverscapes_api.refresh_token()

    # Here's how this looks:
    try:
        # Retrieve a list of valid project type objects. This is useful for filtering your searches
        project_types = riverscapes_api.get_project_types()
    except Exception as e:
        log.error(e)
    finally:
        # Remember to shut down the API to stop the polling process that refreshes the token
        # If you put it inside a finally block it will always run (even if there's an error or a keyboard interrupt like ctrl+c)
        riverscapes_api.shutdown()


    # But there's a better way! (Method 2)
    # ====================================================================================================
    # OR you can instantiate it with a "with" statement like this
    # This might be slightly preferred because it handles the refresh token AND automatically shuts down the 
    # polling process that refreshes the token when you're done with it
    with RiverscapesAPI() as riverscapes_api:
        simple_search(riverscapes_api)
        retrieve_project(riverscapes_api)
        simple_search_with_cache(riverscapes_api)
        find_duplicates(riverscapes_api)
        stream_to_file(riverscapes_api)

    log.info("Done!")
