import dateutil.parser
from .metricxmloutput import DATECREATEDFIELD
import requests
import os
import json
import datetime
import time
from tempfile import NamedTemporaryFile
from champ_metrics.lib.tokenator import Tokenator
import zipfile
import logging
from champ_metrics.lib.loghelper import Logger
from champ_metrics.lib.sslverify import verification
from champ_metrics.lib.exception import MissingException, NetworkException
logging.getLogger("requests").setLevel(logging.ERROR)

RETRIES_ALLOWED = 5
RETRY_DELAY = 4


def APIGet(url, absolute=False):
    """
    GET Wrapper around APIGet
    :param url:
    :param absolute:
    :return:
    """
    log = Logger("APIGet")
    # log.info("Making Call: GET {}".format(url))
    return APICall(url, absolute, method=requests.get)


def APIDelete(url, absolute=False):
    """
    DELETE Wrapper around APICall
    :param url:
    :param absolute:
    :return:
    """
    log = Logger("APIDelete")
    log.info("Making Call: DELETE {}".format(url))
    return APICall(url, absolute, method=requests.delete)


def APICall(url, absolute=False, method=requests.get):
    """
    APICall is a catch-all function for any Sitka API call
    :param url:
    :param absolute:
    :return:
    """
    if os.environ.get('API_BASE_URL') is None:
        raise Exception("Missing API_BASE_URL")

    tokenator = Tokenator()
    log = Logger("API:Call")

    if absolute == False:
        url = "{0}/{1}".format(os.environ.get('API_BASE_URL'), url)

    retry = True
    retries = 0
    response = None

    while retry:
        retries += 1

        headers = {"Authorization": tokenator.TOKEN}

        # No more retries allowed
        try:
            response = method(url, headers=headers, verify=verification())
        except Exception as e:
            errmsg = "Connection Exception: Request exception caught and will be retried: `{}` Retry: {}/{}".format(e, retries, RETRIES_ALLOWED)
            if retry and retries >= RETRIES_ALLOWED:
                raise NetworkException(errmsg)
            else:
                log.error(errmsg)
            continue

        # 200 codes mean good
        code = response.status_code
        errmsg = ""
        if code >= 200 and code < 300:
            retry = False
        elif code == 401:
            # This means our token has expired and we should get a new one
            errmsg = "401 Authorization error: Problem with API Call. Getting new token Retry#: {1}/{2}".format(retries, RETRIES_ALLOWED)
            tokenator.reset()
            tokenator.getToken()
        elif code >= 400 and code < 500:
            raise MissingException("{} Error: {}".format(code, url))
        elif code >= 500 and code < 600:
            errmsg = "500 Error: Problem with API Call. Retrying after {0} seconds Retry#: {1}/{2}".format(RETRY_DELAY, retries, RETRIES_ALLOWED)
            time.sleep(RETRY_DELAY)
        else:
            errmsg = "UNKNOWN ERROR: Problem with API Call. Retrying after {0} seconds Retry#: {1}/{2}".format(RETRY_DELAY, retries, RETRIES_ALLOWED)
            time.sleep(RETRY_DELAY)

        if retry:
            if retries >= RETRIES_ALLOWED:
                raise NetworkException(errmsg)
            else:
                log.error(errmsg)

    # Simple JSON responses just return the parsed json. If this is binary data though we return the whole response object
    # and let the called deal with it.
    if 'json' in response.headers['content-type']:
        respObj = json.loads(response.content)
        return respObj
    else:
        return response


def downloadUnzipTopo(visitID, unzipPath):
    """
    Download a topo zip file to a local path using a visitID
    :param visitID:
    :param zipFilePath:
    :return:
    """
    tokenator = Tokenator()
    log = Logger("downloadTopoZip")

    # First find the appropriate download URL
    try:
        topoFieldFolders = APIGet('visits/{}/fieldFolders/Topo'.format(visitID))

        log.debug("Getting visit file data")
        file = next(file for file in topoFieldFolders['files'] if file['componentTypeID'] == 181)
        downloadUrl = file['downloadUrl']
    except Exception as e:
        raise MissingException("ERROR: No TopoData.zip file found for visit: {}".format(visitID))

    # Download the file to a temporary location
    with NamedTemporaryFile() as f:
        response = APIGet(downloadUrl, absolute=True)
        f.write(response.content)

        log.debug("Downloaded file: {} to: {}".format(downloadUrl, f.name))

        # Now we have it. Unzip
        with zipfile.ZipFile(f, 'r') as zip_ref:
            log.debug("Unzipping file: {} to: {}".format(f.name, unzipPath))
            zip_ref.extractall(unzipPath)

        # Return a folder where we can find a project.rs.xml (or not)
        projpath = None
        for root, subFolders, files in os.walk(unzipPath):
            if "project.rs.xml" in files:
                projpath = root

    return file, projpath


def downloadUnzipHydroResults(visitID, unzipPath):
    """
    Download a topo zip file to a local path using a visitID
    :param visitID: visit ID
    :param unzipPath: Location to save model results
    :returns: tuple (files, projpath)
        WHERE
        dict files is api dictionary of hydro model results
        list projpath is list of rs.xml project path for each model result.
    """
    tokenator = Tokenator()
    log = Logger("downloadHydroModelResults")

    # First find the appropriate download URL
    try:
        hydroFieldFolders = APIGet('visits/{}/fieldFolders/HydroModel'.format(visitID))

        log.debug("Getting visit file data")
        files = hydroFieldFolders['files']
    except Exception as e:
        raise MissingException("ERROR: No hydro results found for visit: {}".format(visitID))

    projpath = []
    for file in files:
        downloadUrl = file['downloadUrl']
        # Download the file to a temporary location
        with NamedTemporaryFile() as f:
            response = APIGet(downloadUrl, absolute=True)
            f.write(response.content)

            log.debug("Downloaded file: {} to: {}".format(downloadUrl, f.name))

            # Now we have it. Unzip
            with zipfile.ZipFile(f, 'r') as zip_ref:
                unzipPathModel = os.path.join(unzipPath, f.name.rstrip(".zip"))
                log.debug("Unzipping file: {} to: {}".format(f.name, unzipPathModel))
                zip_ref.extractall(unzipPathModel)

            # Return a folder where we can find a project.rs.xml (or not)
            for root, subFolders, files in os.walk(unzipPath):
                if "project.rs.xml" in files:
                    projpath.append(root)

    return files, projpath


def latestMetricInstance(metricInstanceList):
    """
    get the single latest metric instance
    :param metricInstanceList:
    :return:
    """
    result = latestMetricInstances(metricInstanceList, True)
    if result is not None and len(result) > 0:
        return result[0]
    else:
        return None


def latestMetricInstances(insts, single=False):
    """
    Take a list of instances from the API and return the latest one with a bit of fancy formatting
    :param instances: list of instances
    :return:
    """
    if len(insts) == 0:
        return None

    # Filter out the instances without dates

    # First create a "sortdate" helper. Setting the second and microsecond to zero is a hack to cover slight timing
    # differences
    for idx, inst in enumerate(insts):
        gendate = list(filter(lambda x: x['name'] == DATECREATEDFIELD, inst['value']))
        try:
            inst['date'] = dateutil.parser.parse(gendate[0]['value'])
            if not single:
                inst['date'] = inst['date'].replace(second=0, microsecond=0)
        except Exception as e:
            inst['date'] = None

    filteredinsts = filter(lambda x: x['date'] is not None, insts)

    if len(filteredinsts) == 0:
        return None

    # Sort by the field we just created
    latestDate = max(x['date'] for x in filteredinsts)

    # Now filter out the ones with the right date
    filteredInstances = filter(lambda x: x['date'] == latestDate, filteredinsts)

    # Only cast things to floating point where necessary
    def formatter(x): return float(x['value']) if x['type'] == "Numeric" and x['value'] is not None else x['value']

    # Now turn it inside out so it's a proper {name: value} pair
    results = []
    for inst in filteredInstances:
        result = {}
        for metric in inst['values']:
            result[metric['name']] = formatter(metric)
        results.append(result)

    return results
