import os
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict
from urllib.parse import urlencode, urlparse, urlunparse
import requests
import webbrowser
from typing import Dict
import os
import json
import threading
import hashlib
import base64
import os
from rscommons import ProgressBar, Logger
from cybercastor.lib.hashes import checkEtag


LOCAL_PORT = 4721
LOGIN_SCOPE = 'openid'


class RiverscapesAPI:
    """This class is a wrapper around the Riverscapes API. It handles authentication and provides a 
    simple interface for making queries.

    If you specify a secretId and clientId then this class will use machine authentication. This is 
    appropriate for development and administration tasks. Otherwise it will use a browser-based 
    authentication workflow which is appropriate for end-users.
    """
    def __init__(self, uri: str, machineAuth: Dict[str, str] = None, devHeaders: Dict[str, str] = None):
        self.log = Logger('API')
        self.uri = uri
        self.machineAuth = machineAuth
        self.devHeaders = devHeaders
        self.accessToken = None
        self.tokenTimeout = None

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

    def getAuth(self) -> Dict[str, str]:
        return {
            "domain": "dev-1redlpx2nwsh6a4j.us.auth0.com",
            "clientId": "pH1ADlGVi69rMozJS1cixkuL5DMVLhKC"
        }

    def shutdown(self):
        if self.tokenTimeout:
            self.tokenTimeout.cancel()

    def refresh_token(self) -> 'GQLApi':
        self.log.info("Authenticating...")
        if self.tokenTimeout:
            self.tokenTimeout.cancel()

        authDetails = self.getAuth()

        # On development there's no reason to actually go get a token
        if self.devHeaders and len(self.devHeaders) > 0:
            return self

        # Step 1: Determine if we're machine code or user auth
        # If it's machine then we can fetch tokens much easier:
        if self.machineAuth:
            tokenUri = self.uri if self.uri.endswith('/') else self.uri + '/'
            tokenUri += 'token'

            options = {
                'method': 'POST',
                'url': tokenUri,
                'headers': {'content-type': 'application/x-www-form-urlencoded'},
                'data': {
                    'audience': 'https://api.riverscapes.net',
                    'grant_type': 'client_credentials',
                    'scope': 'machine:admin',
                    'client_id': self.machineAuth['clientId'],
                    'client_secret': self.machineAuth['secretId'],
                }
            }

            try:
                getTokenReturn = requests.request(**options).json()
                # NOTE: RETRY IS NOT NECESSARY HERE because we do our refresh on the API side of things
                # self.tokenTimeout = setTimeout(self.refreshToken, 1000 * getTokenReturn['expires_in'] - 20)
                self.accessToken = getTokenReturn['access_token']
                self.log.info("SUCCESSFUL Machine Authentication")
            except Exception as error:
                self.log.info(f"Access Token error {error}")
                raise error

        # If this is a user workflow then we need to pop open a web browser
        else:
            code_verifier = self._generate_random(128)
            code_challenge = self._generate_challenge(code_verifier)
            state = self._generate_random(32)

            redirect_url = f"http://localhost:{LOCAL_PORT}/rscli/"
            login_url = urlparse(f"https://{authDetails['domain']}/authorize")
            query_params = {
                "client_id": authDetails["clientId"],
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
            authentication_url = f"https://{authDetails['domain']}/oauth/token"

            data = {
                "grant_type": "authorization_code",
                "client_id": authDetails["clientId"],
                "code_verifier": code_verifier,
                "code": auth_code,
                "redirect_uri": redirect_url,
            }

            response = requests.post(authentication_url, headers={
                                     "content-type": "application/x-www-form-urlencoded"}, data=data)
            response.raise_for_status()
            res = response.json()
            self.tokenTimeout = threading.Timer(
                res["expires_in"] - 20, self.refresh_token)
            self.tokenTimeout.start()
            self.accessToken = res["access_token"]
            self.log.info("SUCCESSFUL Browser Authentication")

    def _wait_for_auth_code(self):
        class AuthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><head><title>Authentication successful</title></head>")
                self.wfile.write(
                    b"<body><p>You can now close this window.</p></body></html>")
                query = urlparse(self.path).query
                self.server.auth_code = dict(x.split("=")
                                             for x in query.split("&"))["code"]

        server = HTTPServer(("localhost", LOCAL_PORT), AuthHandler)
        server.handle_request()
        auth_code = server.auth_code
        server.server_close()
        return auth_code

    def run_query(self, query, variables):  # A simple function to use requests.post to make the API call. Note the json= section.
      headers = {"authorization": "Bearer " + self.accessToken} if self.accessToken else {}
      request = requests.post(self.uri, json={
          'query': query,
          'variables': variables
      }, headers=headers)

      if request.status_code == 200:
          resp_json = request.json()
          if 'errors' in resp_json and len(resp_json['errors']) > 0:
              self.log.info(json.dumps(resp_json, indent=4, sort_keys=True))
              # Authentication timeout: re-login and retry the query
              if len(list(filter(lambda err: 'You must be authenticated' in err['message'], resp_json['errors']))) > 0:
                  self.log.debug("Authentication timed out. Fetching new token...")
                  self.cognito_login()
                  self.log.debug("   done. Re-trying query...")
                  return self.run_query(query, variables)
          else:
              # self.last_pass = True
              # self.retry = 0
              return request.json()
      else:
          raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))

    def download_file(self, api_file_obj, local_path, force=False):
        """[summary]

        Arguments:
            api_file_obj {[type]} -- The dictionary that the API returns. should include the name, md5, size etc
            local_path {[type]} -- the file's local path

        Keyword Arguments:
            force {bool} -- if true we will download regardless
        """
        file_is_there = os.path.exists(local_path) and os.path.isfile(local_path)
        etagMatch = file_is_there and checkEtag(local_path, api_file_obj['md5'])

        if force is True or not file_is_there or not etagMatch:
            if not etagMatch:
                self.log.info('        File etag mismatch. Re-downloading: {}'.format(local_path))
            elif not file_is_there:
                self.log.info('        Downloading: {}'.format(local_path))
            r = requests.get(api_file_obj['downloadUrl'], allow_redirects=True, stream=True)
            total_length = r.headers.get('content-length')

            dl = 0
            with open(local_path, 'wb') as f:
                if total_length is None:  # no content length header
                    f.write(r.content)
                else:
                    progbar = ProgressBar(int(total_length), 50, local_path, byteFormat=True)
                    for data in r.iter_content(chunk_size=4096):
                        dl += len(data)
                        f.write(data)
                        progbar.update(dl)
                    progbar.erase()
            return True
        return False


if __name__ == '__main__':
    log = Logger('API')
    gql = RiverscapesAPI(os.environ['RS_API_URL'])
    gql.refresh_token()
    log.debug(gql.accessToken)
    gql.shutdown() # remember to shutdown so the threaded timer doesn't keep the process alive

    gql2 = RiverscapesAPI(os.environ['RS_API_URL'], {
      'clientId': os.environ['RS_CLIENT_ID'],
      'secretId': os.environ['RS_CLIENT_SECRET']
    })
    gql2.refresh_token()
    log.debug(gql2.accessToken)
    gql2.shutdown()  # remember to shutdown so the threaded timer doesn't keep the process alive

