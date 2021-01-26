# Open GIS

This is a monorepo housing all of the python open-gis tools.

* [Riverscapes Context](./packages/rscontext)
* [BRAT](./packages/brat)
* [VBET](./packages/vbet)

### `./lib` and `./packages`

Both `./lib` and `./packages` contain pep8-compliant python packages. 

* `./lib` is for anything that can be shared between tools
* `./packages` is for the tools themselves. They cannot depend on one another. 

If you find that your tools have code they need to share you can either pull it into the `./lib/commons` package or, if there's enough code, create a new `./lib/whatever` package.

## Repo guidelines:

We're pretty much using the [Gitflow Workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow)

1. All work ***MUST*** happen on branches. 
2. Make sure your pull requests merge onto `dev` and not `master`.
3. Hotfixes may be applied directly to `master`... carefully and after lots of testing.
4. Add your issue and/or pull request to a milestone to make sure it will be included.
5. Attach your commits to issues and pull requests by [referencing them in commit messages](https://docs.github.com/en/enterprise/2.16/user/github/managing-your-work-on-github/closing-issues-using-keywords)