import os
from rsxml import Logger
import inquirer
import json
import time
import requests
from riverscapes import RiverscapesAPI, RiverscapesSearchParams


def update_hero_images(riverscapes_api: RiverscapesAPI):
    """ 
    """
    log = Logger('UpdateHero')
    log.title('Update Hero images')

    request_upload_image_qry = riverscapes_api.load_query('requestUploadImage')
    check_qry = riverscapes_api.load_query('checkUpload')

    # First gather everything we need to make a search
    # ================================================================================================================

    # Load all the image file names from the hero_images directory. Those names will be the guids of the projects we need to update
    allowed_suffixes = ['.jpg', '.jpeg', '.png']
    hero_images_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'inputs', 'projectHeroImages'))
    hero_images = [f for f in os.listdir(hero_images_dir) if os.path.isfile(os.path.join(hero_images_dir, f)) and os.path.splitext(f)[1].lower() in allowed_suffixes]

    for hero_image in hero_images:
        hero_image_path = os.path.join(hero_images_dir, hero_image)
        project_guid = os.path.splitext(hero_image)[0]

        # 1. Fetch the signed url we can use to upload the file
        url_data_resp = riverscapes_api.run_query(request_upload_image_qry, {'entityId': project_guid, 'entityType': 'PROJECT'})

        if not url_data_resp or 'data' not in url_data_resp or 'requestUploadImage' not in url_data_resp['data']:
            raise Exception(f'Failed to get signed url for project {project_guid}')

        data = url_data_resp['data']['requestUploadImage']
        token = data['token']
        fields = data['fields']
        signed_url = data['url']

        # fields: {'bucket': 'riverscapes-warehouse-staging-s3uploadd93486e3-44q50ta3hvxz', 'X-Amz-Algorithm': 'AWS4-HMAC-SHA256', 'X-Amz-Credential': 'ASIA3XVUILU52KMPHIHD/20241212/us-west-2/s3/aws4_request', 'X-Amz-Date': '20241212T170352Z', 'X-Amz-Security-Token': 'IQoJb3JpZ2luX2VjEAkaCXVzLXdlc3QtMiJIMEYCIQDafT4aibA1HTr3W7WoX0dSgX0CiZfwFBoUpSU3MsMNN...4zDpzpMZpboK+O61RAQwLAI085DYam4Mu5DTMKbHDw', 'key': 'images/hero/866da553-f376-40b2-9cc5-a6387cd16891/RAW', 'Policy': 'eyJleHBpcmF0aW9uIjoiMjAyNC0xMi0xMlQxODowMzo1MloiLCJjb25kaXRpb25zIjpbeyJidWNrZXQiOiJya...QwYjItOWNjNS1hNjM4N2NkMTY4OTEvUkFXIn1dfQ==', 'X-Amz-Signature': '75544f5440957b9ce034d6d87ad35d7bba16c966ab7b682132d26ea84b62ee43'}

        # 2. Upload the file using the request module and the signed url
        log.info(f'Uploading {hero_image} to project {project_guid} using signed url {signed_url}')
        # now create the request with the url and the fields
        response = requests.post(signed_url,
                                 data=fields,
                                 files={'file': open(hero_image_path, 'rb')})

        # 3.
        log.info('Waiting 5 seconds then Checking if image was uploaded successfully...')
        time.sleep(5)
        while True:
            check_data = riverscapes_api.run_query(check_qry, {'token': token})
            status = check_data['data']['checkUpload']['status']
            if status == 'PROCESSING':
                log.info('Waiting for image to be processed...')
            elif status == 'SUCCESS':
                log.info('Image uploaded successfully')
                break
            elif status == 'FAILED':
                log.error('Image upload failed')
                break
            else:
                log.error('Unknown status returned')
                log.error(json.dumps(check_data, indent=2))
                break

            log.info('Waiting 5 seconds before checking again...')
            time.sleep(5)


if __name__ == "__main__":
    with RiverscapesAPI() as api:
        update_hero_images(api)
