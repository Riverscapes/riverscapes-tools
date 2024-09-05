import json
import requests
import urllib.parse
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
        log.info('Get items from TNM API with query: {}'.format(json.dumps(params, indent=4)))

        def curl_str():
            """A little helper script to printout the curl command to replicate the request with all the params
            """
            encoded_params = urllib.parse.urlencode(params)
            full_url = f"{url}?{encoded_params}"
            cmd = 'curl --request GET --url "{}" --header "accept: application/json"'.format(full_url)
            return '\n\nCurl command: {}\n'.format(cmd)

        response = requests.get(url, headers=TNM.HEADERS, params=params, timeout=60)

        log.debug('Response code: {}'.format(response.status_code))

        if 'errorMessage' in response.text:
            log.error(curl_str())
            raise Exception('Failed to get items from TNM API with error message: {}'.format(response.text))

        if response.status_code == 200:
            try:
                response = response.json()
                log.debug(curl_str())
                return response
            except json.JSONDecodeError as e:
                log.error(curl_str())
                log.error('Failed to decode JSON response: {}'.format(e))
                log.info('Response text: {}'.format(response.text))
                raise Exception('Failed to get items from TNM API')

        else:
            log.error(curl_str())
            log.info('Response text: {}'.format(response.text))
            raise Exception('Failed to get items from TNM API with status code: {}'.format(response.status_code))
