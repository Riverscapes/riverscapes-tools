# Riverscapes Tools

This is a monorepo housing the python open-source GIS tools for Riverscapes, and the documentation site tools.riverscapes.net. Packages include:  

* [Riverscapes Context](./packages/rscontext)
* [BRAT](./packages/brat)
* [VBET](./packages/vbet)
* ...etc.

## `./lib` and `./packages`

Both `./lib` and `./packages` contain pep8-compliant python packages.

* `./lib` is for anything that can be shared between tools
* `./packages` is for the tools themselves. They cannot depend on one another.

If you find that your tools have code they need to share you can either pull it into the `./lib/commons` package or, if there's enough code, create a new `./lib/whatever` package.

## Repo guidelines

We're pretty much using the [Gitflow Workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow)

1. All work ***MUST*** happen on branches.
2. Make sure your pull requests merge onto `dev` or `docs` and not `master`.
3. Hotfixes may be applied directly to `master`... carefully and after lots of testing.
4. Add your issue and/or pull request to a milestone to make sure it will be included.
5. Attach your commits to issues and pull requests by [referencing them in commit messages](https://docs.github.com/en/enterprise/user/github/managing-your-work-on-github/closing-issues-using-keywords)

## Python Environment

Philip Bailey - 12 Jul 2023

The recommended Python setup involves a single `.venv` in created in the repo root folder. This venv is shared among all the packages (RSContext, VBET etc).

Each package has its own workspace. And these "package workspaces" do not include the root folder that contains the .venv. Therefore, when working in a package workspace Visual Studio Code can't find the root .venv.

We overcome this (on OSX) by creating a sym link to the root .venv inside each package workspace using the following command.

```sh
ln -s ../../.venv .
```

Then inside the package workspace you can do the following to point the workspace at this sym link (which links back to the one and only root .venv):

1. Open any package workspace (RSContext, VBET etc)
1. Open any Python code file.
1. click on the Python version in the status bar at the bottom right of Visual Studio Code. This will prompt to select a new interpreter.
1. Click "Set At Workspace Level...".
1. Visual Studio Code "should list" the sym link .venv folder as one of the options. If it doesn't then you can browse to it.

This workflow needs to be repeated for each riverscapes tools package.

## Working inside Github codespaces

If you are working inside a Github codespace there is no need for any kind of bootstrapping or `.venv` or even `.env` files.

### Data and files

The data dir cor codespaces is set to `/workspaces/data`. This folder is shared between all users of this codespace.

You can open a window to the data by typing `code /workspaces/data/`.

From there files can be dragged into the vscode window to upload them to the codespace. To download files or folders right click on them and select "Download".

You may wish to use projects downloaded from the Riverscapes Warehouse. This codespace comes complete with `rscli` installed.

To download projects you must know: The project id from the warehouse, the HUC and the project type.

```bash
rscli download --id XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX /workspaces/data/rs_context/17060304
```

A browser window should pop open to allow you to authenticate your Riverscapes Warehouse account. Once authenticated the project will be downloaded to the data folder.

Make sure you use the folder name that matches what this tool expects for the project type and huc number.

# Documentation Site

The documentation site is built with [Docusaurus](https://docusaurus.io/) and published  from the **`docs` branch** to [tools.riverscapes.net](https://tools.riverscapes.net).

## Editing & preview changes using VS Code

Use the [`Docs` VS code workspace](/Workspaces/Docs.code-workspace) for editing documentation in VS Code.

```sh
cd docs
yarn install
yarn start
```


-----------------------------------

## Using UV for Environment Management

This project uses [uv](https://github.com/astral-sh/uv) to manage Python virtual environments and dependencies. `uv` is an alternative to tools like `pipenv` and `poetry`.


## Environment Setup

### Prerequisites

1. Install `uv` by following the [installation instructions](https://github.com/astral-sh/uv#installation) for your operating system.
2. Ensure you have Python 3.12 or higher installed. See specific instructions below for tools like `pyenv` that can help you with that.

### OSX Setup (2 methods)

#### Method 1: Using pyenv (Recommended)

1. Install `pyenv` by following the [installation instructions](https://github.com/pyenv/pyenv#installation).
2. Install GDAL. You can do this using [homebrew](https://formulae.brew.sh/formula/gdal)
3. Install Python 3.12 or higher using `pyenv`:
4. Set the local Python version for this project. Once you do this the python version will be locked into the `.venv` and you won't need to set it again.

```bash
# If you don't have 3.12 installed yet then go get it
pyenv install 3.12
# Either set it globally ... 
pyenv global 3.12
# Or you can set it just for this project
pyenv shell 3.12
# Test that the correct version is active:
> python --version
Python 3.12.3
```

4. Now create the uv environment and add the gdal package afterwards:

```bash
cd /path/to/riverscapes-tools
uv sync
# GDAL is not included in the pyproject.toml file so we need to install it after every time we run uv sync. Running the command like this will install 
# the correct version of GDAL that matches the system installation. Hopefully it's GDAL > 3.8
> uv pip install GDAL==$(gdal-config --version)
Resolved 1 package in 112ms
Installed 1 package in 23ms
 + gdal==3.11.4
```

5. Now you simply need to set the python interpreter for the tool you will be running inside VSCode. You should set it to the uv environment python interpreter located at `/path/to/riverscapes-tools/.venv/bin/python`.

### Method 2: Using your QGIS Python Environment

If you don't want to go through the trouble of `pyenv` you can latch onto the Python environment that comes with QGIS. This can be a convenient option if you already have QGIS installed and configured.

1. Find the path to the QGIS Python interpreter. This is typically located at `/Applications/QGIS.app/Contents/MacOS/bin/python3` on OSX but you'll need to explore a bit. Your path may vary depending on your installation.
2. Run `uv sync` with that interpreter:

```bash
cd /path/to/riverscapes-tools
# Create a .venv using the QGIS python interpreter and make sure to use system site packages
/Applications/QGIS-3.42.1-Münster.app/Contents/MacOS/bin/python3 -m venv .venv --system-site-packages
# Now you can run uv sync as normal
uv sync
# Now find out which version of gdal is installed in that QGIS python environment
> /Applications/QGIS-3.42.1-Münster.app/Contents/MacOS/bin/python3 -m pip show gdal
Name: GDAL
Version: 3.3.2

# Now install that version of GDAL into the uv environment. --no-cache is important here to make sure uv doesn't try to re-resolve the package version
> uv pip install GDAL==3.3.2 --no-cache
```

5. Now you simply need to set the python interpreter for the tool you will be running inside VSCode. You should set it to the uv environment python interpreter located at `/path/to/riverscapes-tools/.venv/bin/python`.