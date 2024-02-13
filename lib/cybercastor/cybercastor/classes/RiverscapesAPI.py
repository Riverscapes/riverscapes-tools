import os
import re
from typing import Dict, Generator, Tuple
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlencode, urlparse, urlunparse
import json
import threading
import hashlib
import base64
import logging
from datetime import datetime, timedelta
import requests
from dateutil.parser import parse as dateparse
import semver
from rsxml import Logger, ProgressBar
from cybercastor.lib.hashes import checkEtag

# Disable all the weird terminal noise from urllib3
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3").propagate = False

CHARSET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~'
LOCAL_PORT = 4721
LOGIN_SCOPE = 'openid'

AUTH_DETAILS = {
    "domain": "auth.riverscapes.net",
    "clientId": "pH1ADlGVi69rMozJS1cixkuL5DMVLhKC"
}


def sanitize_version(version: str) -> str:
    """trailing zeros in versions are not allowed
    """
    return re.sub(r'\b0+([0-9])', r'\1', version.strip())


def format_date(date: datetime) -> str:
    """_summary_

    Args:
        date (datetime): _description_

    Returns:
        str: _description_
    """
    return date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3]


class RiverscapesProject:
    """This is just a helper class to make some of the RiverscapesAPI calls easier to use

    Returns:
        _type_: A RiverscapesProject object
    """

    def __init__(self, proj_obj):
        """ Note that we do not check for the existence of the keys in the proj_obj. This is because the API is expected to return a 
        consistent structure. If it doesn't then we want to know about it. 

        For example, you don not NEED to return "id" from your graphql query (even though you always should):

        THIS IS ONLY A CONVENIENCE CLASS. IT DOES NOT VALIDATE THE INPUTS. IT ASSUMES THE INPUTS ARE VALID.
        """
        log = Logger('RiverscapesProject')

        try:
            self.json = proj_obj
            self.id = proj_obj['id'] if 'id' in proj_obj else None
            self.name = proj_obj['name'] if 'name' in proj_obj else None
            self.created_date = dateparse(proj_obj['createdOn']) if 'createdOn' in proj_obj else None
            self.updated_date = dateparse(proj_obj['updatedOn']) if 'updatedOn' in proj_obj else None
            # Turn the meta into a dictionary
            self.project_meta = {x['key']: x['value'] for x in proj_obj['meta']}
            # make a lowercase version of the meta and strip away spaces, dashes, and underscores
            self.project_meta_lwr = {x['key'].lower().replace(' ', '').replace('-', '').replace('_', ''): x['value'] for x in proj_obj['meta']}
            self.huc = self.project_meta_lwr['huc'] if 'huc' in self.project_meta_lwr else None
            try:
                if 'modelversion' in self.project_meta_lwr:
                    cleaned_version = sanitize_version(self.project_meta_lwr['modelversion'])
                    self.model_version = semver.VersionInfo.parse(sanitize_version(cleaned_version))
                else:
                    self.model_version = None
            except Exception as error:
                log.error(f"Error parsing model version: {error}")
                log.error(f"   Model version found: {self.project_meta_lwr['modelversion']}")
                self.model_version = None

            self.tags = proj_obj['tags'] if 'tags' in proj_obj else []
            self.project_type = proj_obj['projectType']['id'] if 'projectType' in proj_obj and 'id' in proj_obj['projectType'] else None

        except Exception as error:
            raise Exception(f"Error parsing project RiverscapesProject object: {error}") from error


class RiverscapesAPIException(Exception):
    """Exception raised for errors in the RiverscapesAPI.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="RiverscapesAPI encountered an error"):
        self.message = message
        super().__init__(self.message)


class RiverscapesAPI:
    """This class is a wrapper around the Riverscapes API. It handles authentication and provides a 
    simple interface for making queries.

    If you specify a secretId and clientId then this class will use machine authentication. This is 
    appropriate for development and administration tasks. Otherwise it will use a browser-based 
    authentication workflow which is appropriate for end-users.
    """

    def __init__(self, stage: str, machine_auth: Dict[str, str] = None, dev_headers: Dict[str, str] = None):
        self.log = Logger('API')
        self.machine_auth = machine_auth
        self.dev_headers = dev_headers
        self.access_token = None
        self.token_timeout = None

        if not stage or stage.upper() == 'PRODUCTION':
            self.uri = 'https://api.data.riverscapes.net'
        elif stage.upper() == 'STAGING':
            self.uri = 'https://api.data.riverscapes.net/staging'
        else:
            raise RiverscapesAPIException(f'Unknown stage: {stage}')

    def _generate_challenge(self, code: str) -> str:
        return self._base64_url(hashlib.sha256(code.encode('utf-8')).digest())

    def _generate_state(self, length: int) -> str:
        result = ''
        i = length
        chars = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        while i > 0:
            result += chars[int(round(os.urandom(1)[0] * (len(chars) - 1)))]
            i -= 1
        return result

    def _base64_url(self, string: bytes) -> str:
        """ Convert a string to a base64url string

        Args:
            string (bytes): this is the string to convert

        Returns:
            str: the base64url string
        """
        return base64.urlsafe_b64encode(string).decode('utf-8').replace('=', '').replace('+', '-').replace('/', '_')

    def _generate_random(self, size: int) -> str:
        """ Generate a random string of a given size

        Args:
            size (int): the size of the string to generate

        Returns:
            str: the random string
        """
        buffer = os.urandom(size)
        state = []
        for b in buffer:
            index = b % len(CHARSET)
            state.append(CHARSET[index])
        return ''.join(state)

    def shutdown(self):
        """_summary_
        """
        self.log.debug("Shutting down Riverscapes API")
        if self.token_timeout:
            self.token_timeout.cancel()

    def refresh_token(self):
        """_summary_

        Raises:
            error: _description_

        Returns:
            _type_: _description_
        """
        self.log.info(f"Authenticating on Riverscapes API: {self.uri}")
        if self.token_timeout:
            self.token_timeout.cancel()

        # On development there's no reason to actually go get a token
        if self.dev_headers and len(self.dev_headers) > 0:
            return self

        # Step 1: Determine if we're machine code or user auth
        # If it's machine then we can fetch tokens much easier:
        if self.machine_auth:
            token_uri = self.uri if self.uri.endswith('/') else self.uri + '/'
            token_uri += 'token'

            options = {
                'method': 'POST',
                'url': token_uri,
                'headers': {'content-type': 'application/x-www-form-urlencoded'},
                'data': {
                    'audience': 'https://api.riverscapes.net',
                    'grant_type': 'client_credentials',
                    'scope': 'machine:admin',
                    'client_id': self.machine_auth['clientId'],
                    'client_secret': self.machine_auth['secretId'],
                },
                'timeout': 30
            }

            try:
                get_token_return = requests.request(**options).json()
                # NOTE: RETRY IS NOT NECESSARY HERE because we do our refresh on the API side of things
                # self.tokenTimeout = setTimeout(self.refreshToken, 1000 * getTokenReturn['expires_in'] - 20)
                self.access_token = get_token_return['access_token']
                self.log.info("SUCCESSFUL Machine Authentication")
            except Exception as error:
                self.log.info(f"Access Token error {error}")
                raise RiverscapesAPIException(error) from error

        # If this is a user workflow then we need to pop open a web browser
        else:
            code_verifier = self._generate_random(128)
            code_challenge = self._generate_challenge(code_verifier)
            state = self._generate_random(32)

            redirect_url = f"http://localhost:{LOCAL_PORT}/rscli/"
            login_url = urlparse(f"https://{AUTH_DETAILS['domain']}/authorize")
            query_params = {
                "client_id": AUTH_DETAILS["clientId"],
                "response_type": "code",
                "scope": LOGIN_SCOPE,
                "state": state,
                "audience": "https://api.riverscapes.net",
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

            response = requests.post(authentication_url, headers={"content-type": "application/x-www-form-urlencoded"}, data=data, timeout=30)
            response.raise_for_status()
            res = response.json()
            self.token_timeout = threading.Timer(
                res["expires_in"] - 20, self.refresh_token)
            self.token_timeout.start()
            self.access_token = res["access_token"]
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
            raise RiverscapesAPIException("Authentication failed")
        else:
            auth_code = server.auth_code if hasattr(server, "auth_code") else None
        return auth_code

    def load_query(self, query_name: str) -> str:
        """ Load a query file from the file system. 

        Args:
            queryName (str): _description_

        Returns:
            str: _description_
        """
        with open(os.path.join(os.path.dirname(__file__), '..', 'graphql', 'riverscapes', 'queries', f'{query_name}.graphql'), 'r', encoding='utf-8') as queryFile:
            return queryFile.read()

    def load_mutation(self, mutation_name: str) -> str:
        """ Load a mutation file from the file system.

        Args:
            mutationName (str): _description_

        Returns:
            str: _description_
        """
        with open(os.path.join(os.path.dirname(__file__), '..', 'graphql', 'riverscapes', 'mutations', f'{mutation_name}.graphql'), 'r', encoding='utf-8') as queryFile:
            return queryFile.read()

    def get_project(self, project_id: str):
        """_summary_

        Args:
            project_id (str): _description_

        Returns:
            _type_: _description_
        """
        qry = self.load_query('getProject')
        results = self.run_query(qry, {"id": project_id})
        return results['data']['getProject']

    def search(self, search_params: Dict[str, str], progress_bar: bool = False) -> Generator[Tuple[RiverscapesProject, Dict], None, None]:
        """ A simple function to make a yielded search on the riverscapes API

        This search has two modes: If the total number of records is less than 10,000 then it will do a single paginated query. 
        If the total number of records is greater than 10,000 then it will do a date-partitioned search. 
        This is because ElasticSearch pagination breaks down at 10,000 items.

        The mode used is chosen automatically based on the total number of records returned by the search.

        Args:
            query (str): _description_
            variables (Dict[str, str]): _description_

        Returns:
            Dict[str, str]: _description_
        """
        qry = self.load_query('searchProjects')
        stats = {}
        page_size = 500
        # NOTE: DO NOT CHANGE THE SORT ORDER HERE. IT WILL BREAK THE PAGINATION.
        sort = ['DATE_CREATED_DESC']

        # First make a quick query to get the total number of records
        stats_results = self.run_query(qry, {"searchParams": search_params, "limit": 0, "offset": 0, "sort": sort})
        overall_total = stats_results['data']['searchProjects']['total']
        stats = stats_results['data']['searchProjects']['stats']
        _prg = ProgressBar(overall_total, 30, 'Search Progress')
        self.log.debug(f"Total records: {overall_total:,} .... starting retrieval...")

        # Set initial to and from dates so that we can paginate through more than 10,000 recirds
        now_date = datetime.now()
        createdOn = search_params.get('createdOn', {})
        search_to_date = dateparse(createdOn.get('to')) if createdOn.get('to') else now_date
        search_from_date = dateparse(createdOn.get('from')) if createdOn.get('from') else None

        num_results = 1  # Just to get the loop started
        outer_counter = 0
        while outer_counter < overall_total and num_results > 0:
            search_params['createdOn'] = {
                "to": format_date(search_to_date),
                "from": format_date(search_from_date) if search_from_date else None
            }
            if progress_bar:
                _prg.update(outer_counter)
            # self.log.debug(f"   Searching from {search_from_date} to {search_to_date}")
            results = self.run_query(qry, {"searchParams": search_params, "limit": page_size, "offset": 0, "sort": sort})
            projects = results['data']['searchProjects']['results']
            num_results = len(projects)
            inner_counter = 0
            project = None
            for search_result in projects:
                project_raw = search_result['item']
                if progress_bar:
                    _prg.update(outer_counter + inner_counter)
                project = RiverscapesProject(project_raw)
                # if inner_counter == 0:
                #     self.log.debug(f"      First created date {project.created_date} -- {project.id}")

                yield (project, stats)
                inner_counter += 1
                outer_counter += 1
            # Set the from date to the last project's created date
            if project is not None:
                # self.log.debug(f"      Last created date {project.created_date} -- {project.id}")
                search_to_date = project.created_date - timedelta(milliseconds=1)

        # Now loop over the actual pages of projects and yield them back one-by-one
        if progress_bar:
            _prg.erase()
            _prg.finish()
        self.log.debug(f"Search complete: retrieved {outer_counter:,} records")

    def get_project_full(self, project_id: str):
        """ This gets the full project record

        This is a MUCH heavier query than what comes back from the search function. If all you need is the project metadata this is 
        probably not the query for you

        Args:
            project_id (str): _description_

        Returns:
            _type_: _description_
        """
        qry = self.load_query('getProjectFull')
        results = self.run_query(qry, {"id": project_id})
        return RiverscapesProject(results['data']['project'])

    def get_project_files(self, project_id: str):
        """ This returns the file listing with everything you need to download project files


        Args:
            project_id (str): _description_

        Returns:
            _type_: _description_
        """
        qry = self.load_query('projectFiles')
        results = self.run_query(qry, {"projectId": project_id})
        return results['data']['project']['files']

    def search_count(self, search_params: Dict[str, str]):
        """ Return the number of records that match the search parameters
        Args:
            query (str): _description_
            variables (Dict[str, str]): _description_

        Returns:
            Tuple[total: int, Dict[str, any]]: the total results and the stats dictionary
        """
        qry = self.load_query('searchCount')
        results = self.run_query(qry, {"searchParams": search_params, "limit": 0, "offset": 0})
        total = results['data']['searchProjects']['total']
        stats = results['data']['searchProjects']['stats']
        return (total, stats)

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
        headers = {"authorization": "Bearer " + self.access_token} if self.access_token else {}
        request = requests.post(self.uri, json={
            'query': query,
            'variables': variables
        }, headers=headers, timeout=30)

        if request.status_code == 200:
            resp_json = request.json()
            if 'errors' in resp_json and len(resp_json['errors']) > 0:
                # Authentication timeout: re-login and retry the query
                if len(list(filter(lambda err: 'You must be authenticated' in err['message'], resp_json['errors']))) > 0:
                    self.log.debug("Authentication timed out. Fetching new token...")
                    self.refresh_token()
                    self.log.debug("   done. Re-trying query...")
                    return self.run_query(query, variables)
                raise RiverscapesAPIException(f"Query failed to run by returning code of {request.status_code}. ERRORS: {json.dumps(resp_json, indent=4, sort_keys=True)}")
            else:
                # self.last_pass = True
                # self.retry = 0
                return request.json()
        else:
            raise RiverscapesAPIException(f"Query failed to run by returning code of {request.status_code}. {query} {json.dumps(variables)}")

    def download_file(self, api_file_obj, local_path, force=False):
        """[summary]

        Arguments:
            api_file_obj {[type]} -- The dictionary that the API returns. should include the name, md5, size etc
            local_path {[type]} -- the file's local path

        Keyword Arguments:
            force {bool} -- if true we will download regardless
        """
        file_is_there = os.path.exists(local_path) and os.path.isfile(local_path)
        etag_match = file_is_there and checkEtag(local_path, api_file_obj['etag'])

        if force is True or not file_is_there or not etag_match:
            if not etag_match and file_is_there:
                self.log.info(f'        File etag mismatch. Re-downloading: {local_path}')
            elif not file_is_there:
                self.log.info(f'        Downloading: {local_path}')
            r = requests.get(api_file_obj['downloadUrl'], allow_redirects=True, stream=True, timeout=30)
            total_length = r.headers.get('content-length')

            dl = 0
            with open(local_path, 'wb') as f:
                if total_length is None:  # no content length header
                    f.write(r.content)
                else:
                    progbar = ProgressBar(int(total_length), 50, local_path, byte_format=True)
                    for data in r.iter_content(chunk_size=4096):
                        dl += len(data)
                        f.write(data)
                        progbar.update(dl)
                    progbar.erase()
            return True
        else:
            self.log.debug(f'        File already exists (skipping): {local_path}')
            return False


if __name__ == '__main__':
    log = Logger('API')
    gql = RiverscapesAPI(os.environ.get('RS_API_URL'))
    gql.refresh_token()
    log.debug(gql.access_token)
    gql.shutdown()  # remember to shutdown so the threaded timer doesn't keep the process alive

    gql2 = RiverscapesAPI(os.environ.get('RS_API_URL'), {
        'clientId': os.environ['RS_CLIENT_ID'],
        'secretId': os.environ['RS_CLIENT_SECRET']
    })
    gql2.refresh_token()
    log.debug(gql2.access_token)
    gql2.shutdown()  # remember to shutdown so the threaded timer doesn't keep the process alive
