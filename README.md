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