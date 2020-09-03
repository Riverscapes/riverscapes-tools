# Open GIS

This is a monorepo housing all of the python open-gis tools.

* [Riverscapes Context](./packages/rscontext)
* [BRAT](./packages/brat)
* [VBET](./packages/vbet)

## Developing

This repo makes extensive use of a single `virtualenv` and `pip install -e`

To get things going run `./scripts/bootstrap.sh`

```
./scripts/bootstrap.sh
```

This will:

1. Create a `virtualenv` that all the packages in this repo can use
2. Install some trick prerequisites like GDAL, Cython, Scipy and rasterio etc.
2. Run `pip install -e` on each repo to link them up.


### `./lib` and `./packages`

Both `./lib` and `./packages` contain pep8-compliant python packages. 

* `./lib` is for anything that can be shared between tools
* `./packages` is for the tools themselves. They cannot depend on one another. 

If you find that your tools have code they need to share you can either pull it into the `./lib/commons` package or, if there's enough code, create a new `./lib/whatever` package.