# Open GIS

This is a monorepo housing the python open-source GIS tools for Riverscapes.

* [Riverscapes Context](./packages/rscontext)
* [BRAT](./packages/brat)
* [VBET](./packages/vbet)

### `./lib` and `./packages`

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