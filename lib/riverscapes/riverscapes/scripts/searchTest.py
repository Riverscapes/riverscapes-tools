"""[summary]
"""
import os
from typing import List
import json
import inquirer
from riverscapes import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject


def change_owner(riverscapes_api: RiverscapesAPI):
    """
    """

    search_params = RiverscapesSearchParams({
        "projectTypeId": "vbet",
        "tags": ["2024CONUS"],
        "meta": {
            "HUC": "1604020108",
        }
    })

    # Make the search and collect all the data
    # ================================================================================================================

    for project, _stats, search_total, _prg in riverscapes_api.search(search_params, progress_bar=True):
        print(search_total)


if __name__ == '__main__':
    with RiverscapesAPI() as api:
        change_owner(api)
