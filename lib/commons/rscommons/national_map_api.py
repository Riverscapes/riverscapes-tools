import json
import random
import time
import urllib.parse

import requests

from rsxml import Logger


class TNM:
    """Client for The National Map products endpoint.

    Provides paginated item retrieval from the TNM API and applies retry
    logic with exponential backoff for transient network and service errors.
    """
    HEADERS = {"Accept": "application/json"}
    MAX_RETRIES = 7
    INITIAL_RETRY_DELAY_S = 2.5
    MAX_RETRY_DELAY_S = 65.0
    INITIAL_TIMEOUT_S = 60
    MAX_TIMEOUT_S = 150

    @staticmethod
    def _retry_delay(attempt: int) -> float:
        """Exponential backoff with jitter.

        attempt starts at 1.
        """
        base = min(TNM.INITIAL_RETRY_DELAY_S * (2 ** (attempt - 1)), TNM.MAX_RETRY_DELAY_S)
        return base + random.uniform(0, 0.5)

    @staticmethod
    def _is_transient_error(status_code: int | None, response_text: str, message: str = "") -> bool:
        if status_code in {408, 429, 500, 502, 503, 504}:
            return True

        haystack = f"{message} {response_text}".lower()
        transient_tokens = [
            "timed out",
            "timeout",
            "connection aborted",
            "remote end closed connection",
            "connection reset",
            "connection refused",
            "temporary failure",
            "temporarily unavailable",
            "max retries exceeded",
            "failed to establish a new connection",
            "expecting value: line 1 column 1",
        ]
        return any(token in haystack for token in transient_tokens)

    @staticmethod
    def get_items(params: dict[str, str]):
        """
        Call TNM API with the argument params and return list of items if successful.
        Will navigate pagination if number of items requires it.

        :param params: TNM API params object
        :return: List of items from TNM API
        """

        url = "https://tnmaccess.nationalmap.gov/api/v1/products"

        params["outputFormat"] = "JSON"

        log = Logger('TNM API Get Items')
        log.info(f'Get items from TNM API with query: {json.dumps(params, indent=4)}')

        def curl_str():
            """A little helper script to printout the curl command to replicate the request with all the params
            """
            encoded_params = urllib.parse.urlencode(params)
            full_url = f"{url}?{encoded_params}"
            cmd = f'curl --request GET --url "{full_url}" --header "accept: application/json"'
            return f'\n\nCurl command: {cmd}\n'

        # Pagination variables
        all_items = []
        offset = 0
        total = None

        while total is None or offset < total:
            params["offset"] = str(offset)
            response = None
            data = None

            for attempt in range(1, TNM.MAX_RETRIES + 1):
                try:
                    timeout_s = min(TNM.INITIAL_TIMEOUT_S + (attempt - 1) * 15, TNM.MAX_TIMEOUT_S)
                    log.debug(f'TNM request attempt {attempt}/{TNM.MAX_RETRIES} (offset={params["offset"]}, timeout={timeout_s}s)')
                    response = requests.get(url, headers=TNM.HEADERS, params=params, timeout=timeout_s)
                    log.debug(f'TNM response attempt {attempt}/{TNM.MAX_RETRIES}: status={response.status_code}')

                    if 'errorMessage' in response.text:
                        if TNM._is_transient_error(response.status_code, response.text):
                            raise RuntimeError(f'TNM transient error payload: {response.text}')
                        log.error(curl_str())
                        raise Exception('Failed to get items from TNM API with error message: {}'.format(response.text))

                    if response.status_code != 200:
                        if TNM._is_transient_error(response.status_code, response.text):
                            raise RuntimeError(f'TNM transient status {response.status_code}: {response.text}')
                        log.error(curl_str())
                        log.info('Response text: {}'.format(response.text))
                        raise Exception('Failed to get items from TNM API with status code: {}'.format(response.status_code))

                    try:
                        data = response.json()
                        log.debug(curl_str())
                    except json.JSONDecodeError as e:
                        if TNM._is_transient_error(response.status_code, response.text, str(e)):
                            raise RuntimeError(f'Failed to decode TNM JSON response: {e}') from e
                        log.error(curl_str())
                        log.error('Failed to decode JSON response: {}'.format(e))
                        log.info('Response text: {}'.format(response.text))
                        raise Exception('Failed to get items from TNM API') from e

                    if data.get('error') or data.get('errors'):
                        err_text = f"{data.get('error')}{data.get('errors')}"
                        if TNM._is_transient_error(response.status_code, response.text, err_text):
                            raise RuntimeError(f'TNM transient API error(s): {err_text}')
                        log.error(curl_str())
                        raise Exception(f"TNM API error(s): {err_text}")

                    log.info(f'TNM request succeeded on attempt {attempt}/{TNM.MAX_RETRIES} (offset={params["offset"]})')
                    break

                except (requests.RequestException, RuntimeError) as e:
                    if attempt >= TNM.MAX_RETRIES:
                        log.error(f'TNM request exhausted retries after {TNM.MAX_RETRIES} attempts (offset={params["offset"]})')
                        log.error(curl_str())
                        if response is not None:
                            log.info('Response text: {}'.format(response.text))
                        raise Exception(f'Failed to get items from TNM API after {TNM.MAX_RETRIES} attempts: {e}') from e

                    delay = TNM._retry_delay(attempt)
                    log.warning(f'TNM request failed (attempt {attempt}/{TNM.MAX_RETRIES}): {e}. Retrying in {delay:.1f}s')
                    time.sleep(delay)

            if data is None:
                raise Exception('Failed to get items from TNM API: no response payload received')

            items = data.get("items", [])
            all_items.extend(items)
            total = data.get("total", len(all_items))

            # Log messages array if present
            messages = data.get("messages", [])
            if messages:
                log.debug("Messages from TNM API response: " + " | ".join(messages))

            if len(all_items) >= total or not items:
                # all items fetched or no more items
                data["items"] = all_items
                return data

            log.debug('Loading next page of data')

            offset += len(items)
