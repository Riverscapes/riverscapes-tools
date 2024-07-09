""" This script demonstrates how to search for projects on the server

NOTE: We set max_results=1234 on all these queries for demo purposes. You probably don't want to do that in production

    To run this file in VSCode choose "Python: Current File (Cybercastor)" from the command palette


"""
import os
import json
from rsxml import Logger
from riverscapes import RiverscapesAPI, RiverscapesSearchParams

log = Logger('Search Projects')


def total_bytes_calc(api: RiverscapesAPI):
    """ Figure out the byteSize of all projects and some stats about them

    Args:
        api (RiverscapesAPI): _description_
    """

    loaded_search_params = RiverscapesSearchParams.load_from_json(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'DEMO_search.json'))
    log.debug(json.dumps(loaded_search_params.to_gql(), indent=2))

    total_bytes = 0
    biggest_project = 0
    smallest_project = 0
    total_projects = 0
    bytes_owner = {}

    log.title("Loop over each project and \"DO\" somewthing with each one")

    # Note how we keep the page size low here because byte size can be a little more expensive to calculate
    for project, _stats, _total in api.search(RiverscapesSearchParams({"projectTypeId": "vbet"}), progress_bar=True, page_size=100):

        size = project.json['totalSize']
        total_bytes += size
        owner = project.json['ownedBy']['name']
        total_projects += 1
        if size > biggest_project:
            biggest_project = size
        if size < smallest_project or smallest_project == 0:
            smallest_project = size

        bytes_owner[owner] = bytes_owner.get(owner, 0) + size

    log.info(json.dumps(bytes_owner, indent=2))

    log.info(f"Total bytes: {total_bytes:,}")
    log.info(f"Total projects: {total_projects}")
    log.info(f"Biggest project: {biggest_project:,}")
    log.info(f"Smallest project: {smallest_project:,}")
    log.info(f"Average project size: {total_bytes/total_projects:,}")
    for owner, bsize in bytes_owner.items():
        log.info(f"{owner}: {bsize:,}")

    return


if __name__ == '__main__':

    with RiverscapesAPI() as riverscapes_api:
        total_bytes_calc(riverscapes_api)

    log.info("Done!")
