---
title: Developing
---

## Branches

This repo may have lots of feature branches but there are 3 main branches that will persist and run parallel to each other:

1. `master`: This is the deployed branch and will be the current state of the repo corresponding to the latest release.
    - NEVER commit directly to master (we shouldn't be allowing it but best to not even try).
2. `dev`: development branch. This is the integration branch that will collect feature branches in preparation for deployment. When `dev` is fully tested we draw a line under it and merge to master.
    - You **should** use feature branches and pull requests to put new features into the repo.
    - Any new features should be rebased on top of `dev`
3. `docs`: this is what is deployed to GHPages. In general this will always stay ahead of `master` and should reflect the current version. We use this branch because documenting takes a lot of little commits that we may want to periodically squash.
    - You _can_ user pull requests for the `docs` branchg but you don't have to.
    - Anything pushed to `origin/docs` causes the website to rebuild immediately and goes live.
    - We may periodically flatten docs and move it back on top of the `master` branch to keep things tidy. 


## Getting set up

This repo makes extensive use of a single `virtualenv` and `pip install -e`

## Linux / OSX

To get things going run `./scripts/bootstrap.sh`

```
./scripts/bootstrap.sh
```

This will:

1. Create a `virtualenv` that all the packages in this repo can use
2. Install some trick prerequisites like GDAL, Cython, Scipy and rasterio etc.
2. Run `pip install -e` on each repo to link them up.


## Windows

1. Install python 3.8.3
2. Install C++ build tools: <https://visualstudio.microsoft.com/visual-cpp-build-tools/>
3. Install git bash and make sure VSCode has this set as its default terminal
4. Install `virtualenv` in the root using `pip install virtualenv`

Go and get the wheel files.
<https://www.lfd.uci.edu/~gohlke/pythonlibs/>

You'll need these. Actual file names may vary if you have a slightly different version of python `cp38 ==> python 3.8`

```
Cython-0.29.21-cp38-cp38-win_amd64.whl
GDAL-3.1.2-cp38-cp38-win_amd64.whl
rasterio-1.1.5-cp38-cp38-win_amd64.whl
Rtree-0.9.4-cp38-cp38-win_amd64.whl
Shapely‑1.7.1‑cp38‑cp38‑win_amd64.whl
```

once you've downloaded these make sure to pip install them OUTSIDE of any virtualenv (so just pip install in terminal)

```bash
pip install Cython-0.29.21-cp38-cp38-win_amd64.whl
```

Now run `./scripts/bootstrap-win.sh` from the root of this repo using git bash


## VSCode Settings

Minor note: for windows machines the pythonpath should be:

```
"python.pythonPath": "../../.venv/scripts/python.exe",
```

***CAVEAT:*** You will need to select a new interpreter for windows and this means that your `settings.json` files will constantly show up as having been changed. Please don't commit these files to git as they will change the environment for everyone else.

Microsoft is looking into platform-specific settings for VSCode but these haven't been implemented yet so we're ahead of the curve.

```json
{
  "python.pythonPath": "../../.venv/bin/python",
  "[python]": {
    "editor.tabSize": 4,
    "editor.formatOnSave": true,
  },
  "python.linting.enabled": true,
  "python.linting.lintOnSave": true,
  "python.linting.pycodestyleEnabled": true,
  "python.linting.pylintEnabled": true,
  "python.linting.pycodestyleArgs": [
    "--ignore=E501"
  ],
  "python.formatting.autopep8Args": [
    "--ignore=E501"
  ],
  "python.terminal.activateEnvironment": true,
  "python.autoComplete.showAdvancedMembers": false,
  "files.exclude": {
    "**/*.egg-info": true
  },
  "files.watcherExclude": {
    "**/*.egg-info/**": true
  },
  "search.exclude": {
    "**/*.egg-info": true
  },
}
```