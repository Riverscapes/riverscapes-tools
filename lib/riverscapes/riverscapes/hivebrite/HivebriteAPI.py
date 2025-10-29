'''Query script to set all the external ids in the system to the SSO_ID so we can look them up from different places
    June 06, 2023
'''
from typing import Dict, NamedTuple
import os
import requests
from rsxml import Logger, dotenv


class HivebriteAPI():
    """_summary_

    Args:
        RiverscapesAPI (_type_): _description_

    Returns:
        _type_: _description_
    """

    BASE_URL = "https://utah-state-university.us.hivebrite.com/api/admin/v1"
    token = None
    log = Logger("HivebriteAPI")

    def __init__(self):
        super().__init__()
        self.log.info("Hivebrite API initializing...")
        self.getAuth()

    class Response(NamedTuple):
        """_summary_

        Args:
            NamedTuple (_type_): _description_
        """
        headers: Dict
        body: Dict
        code: int
        raw: requests.Response

    def getUrl(self, endpoint: str) -> str:
        """_summary_

        Args:
            endpoint (str): _description_
            callVars (List[str], optional): _description_. Defaults to [].

        Returns:
            str: _description_
        """
        # Strip the leading slash
        if endpoint[0] == '/':
            endpoint = endpoint[1:]
        path_parts = [self.BASE_URL, endpoint]
        return "/".join(path_parts)

    def getAuth(self) -> str:
        """Get the hivebrite auth token

        Returns:
            str: _description_
        """
        self.log.info("Getting Hivebrite Auth")
        envPath = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
        env = dotenv.parse_dotenv(envPath)
        if not env:
            raise Exception(f"Could not find the .env file at {envPath}")

        if 'HIVEBRITE_EMAIL' not in env:
            raise Exception("HIVEBRITE_EMAIL not found in .env")
        elif 'HIVEBRITE_PASSWORD' not in env:
            raise Exception("HIVEBRITE_PASSWORD not found in .env")
        elif 'HIVEBRITE_CLIENT_ID' not in env:
            raise Exception("HIVEBRITE_CLIENT_ID not found in .env")
        elif 'HIVEBRITE_SECRET' not in env:
            raise Exception("HIVEBRITE_SECRET not found in .env")

        email = env['HIVEBRITE_EMAIL']
        password = env['HIVEBRITE_PASSWORD']
        client_id = env['HIVEBRITE_CLIENT_ID']
        client_secret = env['HIVEBRITE_SECRET']

        authUri = 'https://utah-state-university.us.hivebrite.com/api/oauth/token'
        response = requests.post(
            authUri,
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
            },
            data={
                'grant_type': 'password',
                'scope': 'admin',
                'admin_email': email,
                'password': password,
                'client_id': client_id,
                'client_secret': client_secret,
                'redirect_uri': 'https://localhost:3000/oauth/callback'
            },
            timeout=30
        )
        body = response.json()
        if response.status_code != 200 or 'access_token' not in body:
            raise Exception(f"Error getting auth: {response.status_code}")
        self.log.info("✅ Got Hivebrite Auth")
        self.token = body['access_token']

    def _error_gate(self, endpoint: str, response: requests.Response) -> None:
        """_summary_

        Args:
            response (requests.Response): _description_
        """
        if response.ok is False:
            self.log.error(f"Error getting {endpoint}: {response.status_code}")
            self.log.error(response.json())
            raise Exception(f"Error getting {endpoint}: {response.status_code}")

    def GET(self, endpoint: str) -> 'HivebriteAPI.Response':
        """Get a response from the server

        Args:
            endpoint (str): The endpoint to query
            callVars (List[str]): The variables to pass to the endpoint

        Returns:
            Dict: The response from the server
        """
        self.log.debug(f"GET: {endpoint}")
        uri = self.getUrl(endpoint)
        response = requests.get(
            uri,
            headers={
                'Authorization': f'Bearer {self.token}'
            },
            timeout=30
        )
        # I need a named tuple with the keys: headers and body
        self._error_gate(endpoint, response)
        return HivebriteAPI.Response(response.headers, response.json(), response.status_code, response)

    def PUT(self, endpoint: str, data: Dict = None) -> 'HivebriteAPI.Response':
        """Put a response to the server

        Args:
            endpoint (str): The endpoint to query
            callVars (List[str]): The variables to pass to the endpoint
            data (Dict): The data to send to the server

        Returns:
            Dict: The response from the server
        """

        uri = self.getUrl(endpoint)
        response = requests.put(
            uri,
            headers={
                'Authorization': f'Bearer {self.token}'
            },
            json=data,
            timeout=30
        )
        # I need a named tuple with the keys: headers and body
        self._error_gate(endpoint, response)
        return HivebriteAPI.Response(response.headers, response.json(), response.status_code, response)

    def POST(self, endpoint: str, data: Dict = None) -> 'HivebriteAPI.Response':
        """Post a response to the server

        Args:
            endpoint (str): The endpoint to query
            vars (List[str]): The variables to pass to the endpoint
            data (Dict): The data to send to the server

        Returns:
            Dict: The response from the server
        """
        uri = self.getUrl(endpoint)
        response = requests.post(
            uri,
            headers={
                'Authorization': f'Bearer {self.token}'
            },
            json=data,
            timeout=30
        )
        # I need a named tuple with the keys: headers and body
        self._error_gate(endpoint, response)
        return HivebriteAPI.Response(response.headers, response.json(), response.status_code, response)
