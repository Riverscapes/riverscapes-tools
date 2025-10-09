import os
from .loghelper import Logger
from .sslverify import verification
import requests
import time
from .exception import NetworkException
import json
import logging
logging.getLogger("requests").setLevel(logging.ERROR)


class TokenatorBorg(object):
    _shared_state = {}
    _initdone = False

    def __init__(self):
        self.__dict__ = self._shared_state

    @staticmethod
    def _kill():
        """
        This method should only be used in unit testing. Borg "Singleton" patterns are
        notoriously persistent so if you absolutely need to start from scratch do it this way
        :return:
        """
        TokenatorBorg._shared_state.clear()

class Tokenator(TokenatorBorg):
    """
    Read up on the Borg pattern if you don't already know it. Super useful
    """
    RETRIES_ALLOWED = 5
    RETRY_DELAY = 4

    def __init__(self):
        super(Tokenator, self).__init__()
        if not self._initdone:
            self.log = Logger("API:Tokenator")
            self.log.info("Init Token Settings")
            self.TOKEN = None
            self.tokenfile = os.environ.get('KEYSTONE_TOKENFILE')
            self.load()
            if self.TOKEN is None:
                self.getToken()
            else:
                self.log.debug("re-using security token")
            self._initdone = True
        else:
            self.log.debug("re-using security token")


    def getToken(self):

        self.log.info("Getting security token")
        if os.environ.get('KEYSTONE_URL') is None:
            raise Exception("Missing KEYSTONE_URL")
        if os.environ.get('KEYSTONE_USER') is None:
            raise Exception("Missing KEYSTONE_USER")
        if os.environ.get('KEYSTONE_PASS') is None:
            raise Exception("Missing KEYSTONE_PASS")
        if os.environ.get('KEYSTONE_CLIENT_ID') is None:
            raise Exception("Missing KEYSTONE_CLIENT_ID")
        if os.environ.get('KEYSTONE_CLIENT_SECRET') is None:
            raise Exception("Missing KEYSTONE_CLIENT_SECRET")

        retry = True
        retries = 0
        response = None
        while retry:
            retries += 1
            try:
                response = requests.post(os.environ.get('KEYSTONE_URL'), data={
                    "username": os.environ.get('KEYSTONE_USER'),
                    "password": os.environ.get('KEYSTONE_PASS'),
                    "grant_type": "password",
                    "client_id": os.environ.get('KEYSTONE_CLIENT_ID'),
                    "client_secret": os.environ.get('KEYSTONE_CLIENT_SECRET'),
                    "scope": 'keystone openid profile'
                }, verify=verification())
            except Exception as e:
                errmsg = "Connection Exception: Request exception caught and will be retried: `{}` Retry: {}/{}".format(e, retries, Tokenator.RETRIES_ALLOWED)
                if retry and retries >= Tokenator.RETRIES_ALLOWED:
                    raise NetworkException(errmsg)
                else:
                    self.log.error(errmsg)
                continue

            if response.status_code != 200:
                errMsg = "Could not retrieve SitkaAPI Access Token with error: {0}. Retry#: {1}/{2}".format(response.status_code, retries, Tokenator.RETRIES_ALLOWED)
                if retries >= Tokenator.RETRIES_ALLOWED:
                    raise NetworkException(errMsg)
                else:
                    self.log.error("{0} Retrying after {1} seconds. Retry#: {2}/{3}".format(errMsg, Tokenator.RETRY_DELAY, retries, Tokenator.RETRIES_ALLOWED))
                    time.sleep(Tokenator.RETRY_DELAY)
            else:
                retry = False

        respObj = json.loads(response.content)
        self.TOKEN = "bearer " + respObj['access_token']
        self.store()


    def load(self):
        """
        Load the token from temporary file
        :return:
        """
        if self.TOKEN is not None:
            return
        if self.tokenfile is None:
            self.log.debug("No tokenfile specified. Skipping token file write.")
            return
        try:
            if self.tokenfile is not None and os.path.isfile(self.tokenfile):
                with open(self.tokenfile, "r") as f:
                    newToken = f.read()
                    if len(newToken) > 100 and "bearer " in newToken:
                        self.TOKEN = newToken
                    self.log.debug("loaded token from file: '{}'".format(self.tokenfile))
        except Exception as e:
            self.log.error("error loading token file: '{}' (continuing)".format(self.tokenfile))
            pass

    def reset(self):
        """
        Resets the token in memory AND deletes the local file
        :return:
        """
        self.TOKEN = None
        self.log.warning("Getting a new security token.")

        if self.tokenfile is None:
            self.log.debug("No tokenfile specified. Skipping token file delete.")
            return

        if os.path.isfile(self.tokenfile):
            os.unlink(self.tokenfile)

    def store(self):
        """
        Save token to a file so the next process can potentially re-use it
        :return:
        """
        if self.TOKEN is None:
            self.log.warning("Tokenator TOKEN should not be none")
            return
        if self.tokenfile is None:
            self.log.debug("No tokenfile specified. Skipping token file store.")
            return

        try:
            if os.path.isfile(self.tokenfile):
                os.unlink(self.tokenfile)

            with open(self.tokenfile, "w") as f:
                f.write(self.TOKEN)
            self.log.debug("stored token in file: '{}'".format(self.tokenfile))
        except Exception as e:
            self.log.error("error storing token to file: '{}' (continuing)".format(self.tokenfile))
            pass