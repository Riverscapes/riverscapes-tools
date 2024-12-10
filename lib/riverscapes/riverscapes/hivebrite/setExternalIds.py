'''Query script to set all the external ids in the system to the SSO_ID so we can look them up from different places
    June 06, 2023
'''
from typing import Dict, List
import json
import os
import time
import inquirer
from termcolor import colored
import requests
from rsxml import Logger, dotenv
from riverscapes.hivebrite.HivebriteAPI import HivebriteAPI

CHUNK_SIZE = 100


def make_chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def setExternalIds():
    ''' Find and delete projects on the server

    Args:
        stage (str): The stage to run the script on
    '''

    log = Logger("projectTypes")
    log.title("Hivebrite: Set External IDs")

    api = HivebriteAPI()

    page = 0
    total = -1
    per_page = 100
    users = []
    while total < 0 or len(users) < total:
        page += 1
        log.info(f"Getting page {page} of users")
        retVal = api.GET(f'/users?page={page}&per_page={per_page}&full_profile=true')
        if total < 0:
            total = int(retVal.headers['x-total'])
        retUsers = retVal.body['users']
        log.info(f"      -- Got {len(retUsers)} users")
        for user in retUsers:
            users.append(user)
        time.sleep(1)

    # Find users without an sso_identifier
    users_without_sso = [x for x in users if 'sso_identifier' not in x or x['sso_identifier'] is None]

    # Find users with an sso_identifier but no external_id
    users_with_sso = [x for x in users if 'sso_identifier' in x and x['sso_identifier'] is not None]
    users_with_correct_info = [x for x in users_with_sso if 'external_id' in x and x['external_id'] == x['sso_identifier']]

    users_with_missing_external_id = [x for x in users_with_sso if 'external_id' not in x or x['external_id'] is None]
    users_with_wrong_external_id = [x for x in users_with_sso if 'external_id' in x and x['external_id'] is not None and x['external_id'] != x['sso_identifier']]

    log.info(f"Total users: {len(users)}")
    if len(users_without_sso) > 0:
        log.info(f"Users without sso_identifier: {len(users_without_sso)}")
    if len(users_with_sso) > 0:
        log.info(f"Users with sso_identifier: {len(users_with_sso)}")
    if len(users_with_correct_info) > 0:
        log.info(f"Users with correct info: {len(users_with_correct_info)}")
    if len(users_with_missing_external_id) > 0:
        log.warning(f"Users with missing external_id: {len(users_with_missing_external_id)}")
    if len(users_with_wrong_external_id) > 0:
        log.error(f"Users with wrong external_id: {len(users_with_wrong_external_id)}")

    users_with_missing_external_id_CHUNKS = [x for x in make_chunks(users_with_missing_external_id, CHUNK_SIZE)]
    users_with_wrong_external_id_CHUNKS = [x for x in make_chunks(users_with_wrong_external_id, CHUNK_SIZE)]

    for chunk in users_with_missing_external_id_CHUNKS:
        api.PUT('/users/_bulk', {'users': [
            {'id': x['id'], 'external_id': x['sso_identifier']} for x in chunk
        ]})
        log.info(f"Set external_id for {len(chunk)} users")

    for chunk in users_with_wrong_external_id_CHUNKS:
        api.PUT('/users/_bulk', {'users': [
            {'id': x['id'], 'external_id': x['sso_identifier']} for x in chunk
        ]})
        log.info(f"Set external_id for {len(chunk)} users")

    pass


if __name__ == '__main__':
    setExternalIds()
