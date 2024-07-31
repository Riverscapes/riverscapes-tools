import json
import requests

from rscommons import Logger


class TNM:
    HEADERS = {"Accept": "application/json"}

    @staticmethod
    def get_items(params):
        """
        Call TNM API with the argument params and return list of items if successful.
        :param params: TNM API params object
        :return: List of items from TNM API
        """

        url = "https://tnmaccess.nationalmap.gov/api/v1/products"

        params["outputFormat"] = "JSON"

        log = Logger('TNM API Get Items')
        log.info('Get items from TNM API with query: {}'.format(params))

        response = requests.get(url, headers=TNM.HEADERS, params=params)

        if response.status_code == 200:
            try:
                return response.json()
            except json.JSONDecodeError as e:
                log.error('Failed to decode JSON response: {}'.format(e))
                log.info('Response text: {}'.format(response.text))

        else:
            log.error('Failed to get items from TNM API with status code: {}'.format(response.status_code))
            log.info('Response text: {}'.format(response.text))
