from pathlib import Path
# from graphql import query
# from path import expanduser
import os
import json
import boto3
import requests
from rscommons import ProgressBar, Logger
from lib.hashes import checkEtag


class QueryMonster:
    """[summary]
    """

    def __init__(self, api_url, username, password):
        self.log = Logger('API')
        self.jwt = None
        self.api_url = None
        # 1). Load the .riverscapes file. This gets us the URL to the graphql endpoint
        self.username = username
        self.password = password
        self.api_url = api_url

        self.gql_auth_obj = self.auth_query()

        # 3) Get the JWT we will need to make queries
        self.cognito_login()

    def load_config(self):
        """load the .riverscapes config file
        """
        self.log.info('Loading Config file')
        with open(os.path.join(Path.home(), '.riverscapes')) as json_file:
            data = json.load(json_file)
        return data['programs'][data['default']]

    def cognito_login(self):
        """Log into cognito and retrieve the JWT token

        Returns:
            [type] -- [description]
        """
        client = boto3.client('cognito-idp')
        resp = client.admin_initiate_auth(
            UserPoolId=self.gql_auth_obj['userPool'],
            ClientId=self.gql_auth_obj['clientId'],
            AuthFlow='ADMIN_USER_PASSWORD_AUTH',
            AuthParameters={
                "USERNAME": self.username,
                "PASSWORD": self.password
            }
        )
        self.jwt = resp['AuthenticationResult']['AccessToken']

    def run_query(self, query, variables):  # A simple function to use requests.post to make the API call. Note the json= section.
        headers = {"Authorization": "Bearer " + self.jwt} if self.jwt else {}
        request = requests.post(self.api_url, json={
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

    def auth_query(self):
        result = self.run_query(QueryMonster._GQL_Auth, {})  # Execute the query
        return result['data']['auth']  # Drill down the dictionary

    def add_job(self, params):
        result = self.run_query(QueryMonster._GQL_AddJob, params)  # Execute the query
        return result['data']['addJob']  # Drill down the dictionary

    def get_jobs(self, jobStatus=None, taskStatus=None, limit=None, nextToken=None):
        # jobStatus: JobStatusEnum, $taskStatus: TaskStatusEnum, $limit: Int, $nextToken: String
        result = self.run_query(QueryMonster._GQL_GetJobs, {"jobStatus": jobStatus, "taskStatus": taskStatus, "limit": limit, "nextToken": nextToken})  # Execute the query
        return result['data']['getJobs']  # Drill down the dictionary

    def get_job(self, id):
        result = self.run_query(QueryMonster._GQL_GetJob, {"id": id})  # Execute the query
        return result['data']['getJob']  # Drill down the dictionary

    def jobs_query(self):
        result = self.run_query(QueryMonster._GQL_Jobs, {})  # Execute the query
        return result['data']['programs']  # Drill down the dictionary

    def start_task(self, id):
        result = self.run_query(QueryMonster._GQL_StartTask, {"id": id})  # Execute the query
        return result['data']['startTask']

    def stop_task(self, id):
        result = self.run_query(QueryMonster._GQL_StopTask, {"id": id})  # Execute the query
        return result['data']['stopTask']

    _GQL_Jobs = """
        getJobs(jobStatus: JobStatusEnum, taskStatus: TaskStatusEnum, limit: Int, nextToken: String): {
            jobs: {
                id
                createdBy
                createdOn
                updatedOn
                status
                meta
                name
                description
                taskDefId
                taskScriptId
                env
                tasks {
                    id
                    name
                    status
                    createdBy
                    createdOn
                    startedOn
                    endedOn
                    queriedOn
                    logStream
                    logUrl
                    meta
                    env
                    cpu
                    memory
                    taskDefProps {
                      cpu
                      memoryLimitMiB
                      ephemeralStorageGiB
                    }
                }
            }
            nextToken: String
        }
        """

    _GQL_Auth = """
        query Ping {
            auth{
                loggedIn
                userPool
                clientId
                region
                __typename
            }
        }
        """

    _GQL_GetJobs = """
        query GetJobs($jobStatus: JobStatusEnum, $taskStatus: TaskStatusEnum, $limit: Int, $nextToken: String) {
            getJobs(jobStatus: $jobStatus, taskStatus: $taskStatus, limit: $limit, nextToken: $nextToken){
                jobs {
                    id
                    createdBy
                    createdOn
                    updatedOn
                    status
                    meta
                    name
                    description
                    taskDefId
                    taskScriptId
                    env
                    tasks {
                        id
                        name
                        status
                        createdBy
                        createdOn
                        startedOn
                        endedOn
                        queriedOn
                        logStream
                        logUrl
                        meta
                        env
                        cpu
                        memory
                        taskDefProps {
                          cpu
                          memoryLimitMiB
                          ephemeralStorageGiB
                        }
                    }
                }
                nextToken
            }
        }
        """

    _GQL_GetJob = """
        query ($id: ID!) {
            getJob(id: $id) {
                id
                createdBy
                createdOn
                updatedOn
                status
                meta
                name
                description
                taskDefId
                taskScriptId
                env
                tasks {
                    id
                    name
                    status
                    createdBy
                    createdOn
                    startedOn
                    endedOn
                    queriedOn
                    logStream
                    logUrl
                    meta
                    env
                    cpu
                    memory
                    taskDefProps {
                      cpu
                      memoryLimitMiB
                      ephemeralStorageGiB
                    }
                }
            }
        }
    """

    _GQL_AddJob = """
        mutation ($job: JobInput!, $tasks: [TaskInput!]) {
            addJob(job: $job, tasks: $tasks) {
                id
                createdBy
                createdOn
                updatedOn
                status
                meta
                name
                description
                taskDefId
                taskScriptId
                env
                tasks {
                    id
                    name
                    createdBy
                    createdOn
                    logStream
                    logUrl
                    meta
                    env
                    cpu
                    memory
                    taskDefProps {
                      cpu
                      memoryLimitMiB
                      ephemeralStorageGiB
                    }
                }
            }
        }
    """

    _GQL_StartTask = """
        mutation ($id: ID!) {
            startTask(id: $id)
        }
    """

    _GQL_StopTask = """
        mutation ($id: ID!) {
            stopTask(id: $id)
        }
    """

    _GQL_GetEngines = """
        getEngines {
            id
            name
            description
            version
            props
            taskDefProps {
              cpu
              memoryLimitMiB
              ephemeralStorageGiB
            }
            taskScripts {
                id
                name
                filename
                description
                taskDefProps {
                  cpu
                  memoryLimitMiB
                  ephemeralStorageGiB
                }
                taskVars {
                    name
                    description
                    varType
                    regex
                }
            }
        }
        """
