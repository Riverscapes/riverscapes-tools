# Cybercastor API

`.env.python` file

```
# To connect to the Cybercastor API you'll need the following:
CC_API_URL=https://XXXXXXXXXXXXXX.execute-api.us-west-2.amazonaws.com/prod/api

# To be perfectly clear, these are COGNITO credentials. Eventually they will go away.
USERNAME=myemail@something.com
PASSWORD=XXXMyRSPAssword

# Data root is only used by dump_sqlite.py
DATA_ROOT=/Users/whatever/data/
```

Job definition

```json
{
  "$schema": "../job.schema.json",
  "name": "Give your job a good name",
  "description": "A little more context for why we're running this job. Helps with auditing later",
  "taskScriptId": "rs_context",
  "meta": {},
  "server": "STAGING",
  "env": {
    "CHANNEL_TAGS": "Cybercastor,Testing",
    "TAUDEM_TAGS": "Cybercastor,Testing",
    "RSCONTEXT_TAGS": "Cybercastor,Testing",
    "VBET_TAGS": "JAN05",
    "GIT_REF": "VBET_GNAT"
  },
  "resources": {
    "cpu": 4096,
    "memory": 30720,
    "disk": 80
  },
  "hucs": [
    "1601020204",
    "1706030402"
  ],
  "lookups": {
    "1601020204": {
      "RSCONTEXT_ID": "d16a225e-55b0-41b2-9519-507db79cb7f9"
    },
    "1706030402": {
      "RSCONTEXT_ID": "796ca192-1f9f-4f6d-84b2-c0ec665dfad3"
    }
  }
}
```

All jobs should go in the `python/scripts/jobs` folder

* `$schema`: should always be "../job.schema.json" because VSCode will then tell you if there are problems with the file.
* `name`: Job name. Make it a good one. 
* `description`: Some more contextual text about what this job is
* `taskScriptId`: This is the id of the script you want to run. Scripts are defined in the manifest json file: <https://cybercastor.northarrowresearch.com/engines/manifest.json>
* `meta`: this is a key-value pair that will be stored in the cybercastor db. Not mandatory but useful if you want to attach things like client id or store order numbers to a job.
* `server`: This is optional and will default to PRODUCTION. The valid values for now are: `PRODUCTION`, `STAGING`
* `hucs`: This is an array of Strings corresponding to the huc codes you want to run.
* `env`: These are the environment variables you need to specify corresponding to the script in the manifest json file: <https://cybercastor.northarrowresearch.com/engines/manifest.json>. There are a couple of special environment variables.
    * `NO_UI` does not need to be provided in the job json. `rstools_add_job.py` adds that for you.
    * `GIT_REF` is the branch or tag of the commit you want to run. It is optional. The latest commit on the `new_project_xml` branch is the default.
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

  * `lookups`: After you run your job you may see lookups. This is populated automatically and consists of IDs from the warehouse to use as inputs when downloading projects. You can delete this field if you want to re-run the lookup process or change the ids to something else.
