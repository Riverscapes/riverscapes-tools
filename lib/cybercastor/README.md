# Cybercastor API

`.env.python` file

```
# To connect to the Cybercastor API you'll need the following:
API_URL=https://0qj6r9bs8i.execute-api.us-west-2.amazonaws.com/prod/api
# To be perfectly clear, these are COGNITO credentials
USERNAME=myemail@something.com
PASSWORD=XXXMyRSPAssword

# For the new Riverscapes API you'll need at least 
RS_API_URL=https://api.warehouse.riverscapes.net/staging
RS_CLIENT_ID=ASK_MATT_FOR_THIS
RS_CLIENT_SECRET=ASK_MATT_FOR_THIS
```

Job definition

```json
{
  "$schema": "../../job.schema.json",
  "name": "This is a job with a good name",
  "description": "Test job to run everything",
  "taskScriptId": "rs_tools_all",
  "meta": {
    "invoiceId": 342345234
  },
  "env": {
    "PROGRAM": "Anabranch",
    "TAGS": "TEST,SEP10"
  },
  "resources": {
    "cpu": 256,
    "memory": 512,
    "disk" : 25
  },
  "hucs": [
    "08080102",
    "08010100",
    "07080105"
  ]
}
```

All jobs should go in the `python/scripts/jobs` folder

* `Name`: Job name. Make it a good one. 
* `Description`: Some more contextual text about what this job is
* `taskScriptId`: This is the id of the script you want to run. Scripts are defined in the manifest json file: <https://cybercastor.northarrowresearch.com/engines/manifest.json>
* `meta`: this is a key-value pair that will be stored in the cybercastor db. Not mandatory but useful if you want to attach things like client id or store order numbers to a job.
* `hucs`: This is an array of Strings corresponding to the huc codes you want to run.
* `env`: These are the environment variables you need to specify corresponding to the script in the manifest json file: <https://cybercastor.northarrowresearch.com/engines/manifest.json>. There are a couple of special environment variables.
    * `RS_CONFIG` does not need to be provided in the job json. `rstools_add_job.py` adds that for you.
    * `NO_UI` does not need to be provided in the job json. `rstools_add_job.py` adds that for you.
    * `PROGRAM`: The program name corresponding to the key in your `.riverscapes` file. For example I might have the `BRAT` program listed as `BRAT_DEV` in the `.riverscapes` file so the value to use would be `BRAT_DEV`
    * `GIT_REF` is the branch, tag or commit hash of the commit you want to run. It is optional. The latest commit on the main branch is the default.
* `resources`: This is optional and in most cases you should probably not specify it. It is only when you absolutely NEED to change the default CPU and RAM allocations for a given job.
  * `cpu`: This is the number corresponding to the CPU units. ***NOTE: THIS IS SUBJECT TO CHANGE BY AWS***. Allowed values:
      * 256 (.25 vCPU) - Available memory values: 512 (0.5 GB), 1024 (1 GB), 2048 (2 GB)
      * 512 (.5 vCPU) - Available memory values: 1024 (1 GB), 2048 (2 GB), 3072 (3 GB), 4096 (4 GB)
      * 1024 (1 vCPU) - Available memory values: 2048 (2 GB), 3072 (3 GB), 4096 (4 GB), 5120 (5 GB), 6144 (6 GB), 7168 (7 GB), 8192 (8 GB)
      * 2048 (2 vCPU) - Available memory values: Between 4096 (4 GB) and 16384 (16 GB) in increments of 1024 (1 GB)
      * 4096 (4 vCPU) - Available memory values: Between 8192 (8 GB) and 30720 (30 GB) in increments of 1024 (1 GB)
  * `memory`: The amount of memory. Allowed values depend on what you chose for CPU and MUST be in increments of 1024.
  * `disk`: The ephemoral disk space assigned to each task. 20 is the default and this value must be between 20 and 200.


Here are some resources examples:

```json
    // Small box
    "resources": {
        "cpu": 512,
        "memory": 4096
    },
    // Medium box
    "resources": {
        "cpu": 1024,
        "memory": 8192
    },
    // Big box (this is what we need for TauDEM)
    "resources": {
        "cpu": 2048,
        "memory": 16384
    }
```

You can go bigger too but beware of price increases!