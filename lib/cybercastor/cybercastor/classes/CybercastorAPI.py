from typing import Dict
import os
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlencode, urlparse, urlunparse
import json
import threading
import hashlib
import base64
import logging
import requests
from rsxml import Logger

# Disable all the weird terminal noise from urllib3
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3").propagate = False

LOCAL_PORT = 4722
LOGIN_SCOPE = 'cybercastor:user'
AUTH_DETAILS = {
    "domain": "auth.riverscapes.net",
    "clientId": "Q5EwJSZ9ocY9roT7GAwfBv47Tj57BTET"
}


class CybercastorAPIException(Exception):
    """Exception raised for errors in the CybercastorAPI.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="CybercastorAPI encountered an error"):
        self.message = message
        super().__init__(self.message)


class CybercastorAPI:
    """This class is a wrapper around the Cybercastor API. It handles authentication and provides a 
    simple interface for making queries.

    If you specify a secretId and clientId then this class will use machine authentication. This is 
    appropriate for development and administration tasks. Otherwise it will use a browser-based 
    authentication workflow which is appropriate for end-users.
    """

    def __init__(self, stage: str, machineAuth: Dict[str, str] = None, devHeaders: Dict[str, str] = None):
        self.log = Logger('API')
        if not stage:
            raise Exception("You must specify a stage for the API: 'PRODUCTION' or 'STAGING'")
        self.machineAuth = machineAuth
        self.devHeaders = devHeaders
        self.accessToken = None
        self.tokenTimeout = None

        if stage.upper() == 'PRODUCTION':
            self.uri = 'https://api.cybercastor.riverscapes.net'
            self.stage = 'PRODUCTION'
        elif stage.upper() == 'STAGING':
            self.uri = 'https://api.cybercastor.riverscapes.net/staging'
            self.stage = 'STAGING'
        else:
            raise Exception(f'Unknown stage: {stage}')


    def __enter__(self) -> 'CybercastorAPI':
        """ Allows us to use this class as a context manager
        """
        self.refresh_token()
        return self

    def __exit__(self, _type, _value, _traceback):
        """Behaviour on close when using the "with RiverscapesAPI():" Syntax
        """
        # Make sure to shut down the token poll event so the process can exit normally
        self.shutdown()

    def _generate_challenge(self, code: str) -> str:
        return self._base64URL(hashlib.sha256(code.encode('utf-8')).digest())

    def _generate_state(self, length: int) -> str:
        result = ''
        i = length
        chars = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        while i > 0:
            result += chars[int(round(os.urandom(1)[0] * (len(chars) - 1)))]
            i -= 1
        return result

    def _base64URL(self, string: bytes) -> str:
        return base64.urlsafe_b64encode(string).decode('utf-8').replace('=', '').replace('+', '-').replace('/', '_')

    def _generate_random(self, size: int) -> str:
        CHARSET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~'
        buffer = os.urandom(size)
        state = []
        for b in buffer:
            index = b % len(CHARSET)
            state.append(CHARSET[index])
        return ''.join(state)

    def shutdown(self):
        """Shutdown the API and cancel any pending token refreshes. This is important to call when you're done with the API.
        """
        self.log.debug("Shutting down Riverscapes API")
        if self.tokenTimeout:
            self.tokenTimeout.cancel()


    def get_job_paginated(self, job_id):
        """Get the current job and all tasks associated with it (paginate through until you have them all)

        Args:
            job_id (_type_): _description_
        """
        get_job_query = self.load_query('GetJob')
        get_job_task_page_query = self.load_query('GetJobTaskPage')
        results = self.run_query(get_job_query, {"jobId": job_id})

        # If there are more tasks then paginate through them
        while (results['data']['getJob'] and results['data']['getJob']['tasks']['nextToken'] is not None):
            pageResults = self.run_query(get_job_task_page_query, {
                                         "jobId": job_id, "nextToken": results['data']['getJob']['tasks']['nextToken']})
            results['data']['getJob']['tasks']['items'] += pageResults['data']['getJob']['tasks']['items']
            results['data']['getJob']['tasks']['nextToken'] = pageResults['data']['getJob']['tasks']['nextToken']

        return results['data']['getJob'] if results['data'] and results['data']['getJob'] else None

    def get_active_jobs(self):
        """ Get all the active jobs.

        Returns:
            _type_: _description_
        """

        get_jobs_query = self.load_query('GetJobsByStatus')
        results = self.run_query(get_jobs_query, {"jobStatus": "ACTIVE"})

        # If there are more tasks then paginate through them
        while (results['data']['getJobs'] and results['data']['getJobs']['nextToken'] is not None):
            pageResults = self.run_query(get_jobs_query, {
                                         "jobStatus": "ACTIVE", "nextToken": results['data']['getJobs']['nextToken']})

            results['data']['getJobs']['items'] += pageResults['data']['getJobs']['items']
            results['data']['getJobs']['nextToken'] = pageResults['data']['getJobs']['nextToken']

        jobs = []
        for job in results['data']['getJobs']['items']:
            jobs.append(self.get_job_paginated(job['id']))

        return jobs

    def refresh_token(self):
        """_summary_

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        self.log.info(f"Authenticating on Cybercastor API: {self.uri}")
        if self.tokenTimeout:
            self.tokenTimeout.cancel()

        # On development there's no reason to actually go get a token
        if self.devHeaders and len(self.devHeaders) > 0:
            return self

        # Step 1: Determine if we're machine code or user auth
        # If it's machine then we can fetch tokens much easier:
        if self.machineAuth:
            raise Exception(
                "Machine authentication is not yet supported on Cybercastor")

            # tokenUri = self.uri if self.uri.endswith('/') else self.uri + '/'
            # tokenUri += 'token'

            # options = {
            #     'method': 'POST',
            #     'url': tokenUri,
            #     'headers': {'content-type': 'application/x-www-form-urlencoded'},
            #     'data': {
            #         'audience': 'https://api.riverscapes.net',
            #         'grant_type': 'client_credentials',
            #         'scope': LOGIN_SCOPE,
            #         'client_id': self.machineAuth['clientId'],
            #         'client_secret': self.machineAuth['secretId'],
            #     }
            # }

            # try:
            #     getTokenReturn = requests.request(**options).json()
            #     # NOTE: RETRY IS NOT NECESSARY HERE because we do our refresh on the API side of things
            #     # self.tokenTimeout = setTimeout(self.refreshToken, 1000 * getTokenReturn['expires_in'] - 20)
            #     self.accessToken = getTokenReturn['access_token']
            #     self.log.info("SUCCESSFUL Machine Authentication")
            # except Exception as error:
            #     self.log.info(f"Access Token error {error}")
            #     raise error

        # If this is a user workflow then we need to pop open a web browser
        else:
            code_verifier = self._generate_random(128)
            code_challenge = self._generate_challenge(code_verifier)
            state = self._generate_random(32)

            redirect_url = f"http://localhost:{LOCAL_PORT}/cc_cli/"
            login_url = urlparse(f"https://{AUTH_DETAILS['domain']}/authorize")
            query_params = {
                "client_id": AUTH_DETAILS["clientId"],
                "response_type": "code",
                "scope": LOGIN_SCOPE,
                "state": state,
                "audience": "https://api.cybercastor.riverscapes.net",
                "redirect_uri": redirect_url,
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            }
            login_url = login_url._replace(query=urlencode(query_params))
            webbrowser.open_new_tab(urlunparse(login_url))

            auth_code = self._wait_for_auth_code()
            authentication_url = f"https://{AUTH_DETAILS['domain']}/oauth/token"

            data = {
                "grant_type": "authorization_code",
                "client_id": AUTH_DETAILS["clientId"],
                "code_verifier": code_verifier,
                "code": auth_code,
                "redirect_uri": redirect_url,
            }

            response = requests.post(authentication_url, headers={
                                     "content-type": "application/x-www-form-urlencoded"},
                                     data=data,
                                     timeout=60)
            response.raise_for_status()
            res = response.json()
            self.tokenTimeout = threading.Timer(
                res["expires_in"] - 20, self.refresh_token)
            self.tokenTimeout.start()
            self.accessToken = res["access_token"]
            self.log.info("SUCCESSFUL Browser Authentication")

    def _wait_for_auth_code(self):
        """ Wait for the auth code to come back from the server using a simple HTTP server

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        class AuthHandler(BaseHTTPRequestHandler):
            """_summary_

            Args:
                BaseHTTPRequestHandler (_type_): _description_
            """

            def stop(self):
                """Stop the server
                """
                self.server.shutdown()

            def do_GET(self):
                """ Do all the server stuff here
                """
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><head><title>CYBERCASTOR API: Authentication successful</title></head>")
                self.wfile.write(
                    b"<body><p>CYBERCASTOR API: Authentication successful. You can now close this window.</p></body></html>")
                query = urlparse(self.path).query
                if "=" in query and "code" in query:
                    self.server.auth_code = dict(x.split("=")
                                                 for x in query.split("&"))["code"]
                    # Now shut down the server and return
                    self.stop()

        server = ThreadingHTTPServer(("localhost", LOCAL_PORT), AuthHandler)
        # Keep the server running until it is manually stopped
        try:
            print("Starting server to wait for auth, use <Ctrl-C> to stop")
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        if not hasattr(server, "auth_code"):
            raise CybercastorAPIException("Authentication failed")
        else:
            auth_code = server.auth_code if hasattr(server, "auth_code") else None
        return auth_code

    def load_query(self, queryName: str) -> str:
        """ Load a query file from the file system. 

        Args:
            queryName (str): _description_

        Returns:
            str: _description_
        """
        with open(os.path.join(os.path.dirname(__file__), '..', 'graphql', 'queries', f'{queryName}.graphql'), 'r', encoding='utf8') as queryFile:
            return queryFile.read()

    def load_mutation(self, mutationName: str) -> str:
        """ Load a mutation file from the file system.

        Args:
            mutationName (str): _description_

        Returns:
            str: _description_
        """
        with open(os.path.join(os.path.dirname(__file__), '..', 'graphql', 'mutations', f'{mutationName}.graphql'), 'r', encoding='utf8') as queryFile:
            return queryFile.read()

    # A simple function to use requests.post to make the API call. Note the json= section.
    def run_query(self, query, variables):
        """ A simple function to use requests.post to make the API call. Note the json= section.

        Args:
            query (_type_): _description_
            variables (_type_): _description_

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        headers = {"authorization": "Bearer " +
                   self.accessToken} if self.accessToken else {}
        request = requests.post(self.uri, json={
            'query': query,
            'variables': variables
        }, headers=headers, timeout=60)

        if request.status_code == 200:
            resp_json = request.json()
            if 'errors' in resp_json and len(resp_json['errors']) > 0:
                self.log.info(json.dumps(resp_json, indent=4, sort_keys=True))
                self.log.debug(json.dumps(query, indent=4, sort_keys=True))
                self.log.debug(json.dumps(variables, indent=4, sort_keys=True))
                # Authentication timeout: re-login and retry the query
                if len(list(filter(lambda err: 'You must be authenticated' in err['message'], resp_json['errors']))) > 0:
                    self.log.debug(
                        "Authentication timed out. Fetching new token...")
                    self.refresh_token()
                    self.log.debug("   done. Re-trying query...")
                    return self.run_query(query, variables)
            else:
                # self.last_pass = True
                # self.retry = 0
                return request.json()
        else:
            raise Exception(f"Query failed to run by returning code of {request.status_code}. {query}")


if __name__ == '__main__':
    log = Logger('API')
    gql = CybercastorAPI(os.environ.get('CC_API_URL'))
    gql.refresh_token()
    log.debug(gql.accessToken)
    gql.shutdown()  # remember to shutdown so the threaded timer doesn't keep the process alive

    # NOTE: There is no machine auth on CYbercastor yet

    # gql2 = CybercastorAPI(os.environ.get('CC_API_URL'), {
    #     'clientId': os.environ['CC_CLIENT_ID'],
    #     'secretId': os.environ['CC_CLIENT_SECRET']
    # })
    # gql2.refresh_token()
    # log.debug(gql2.accessToken)
    # gql2.shutdown()  # remember to shutdown so the threaded timer doesn't keep the process alive
