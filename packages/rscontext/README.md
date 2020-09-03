# Riverscapes Context

Python 3 package to build the contextual layers used by our outher python riverscapes tools.


```
usage: rs-context [-h] [--force] [--temp_folder TEMP_FOLDER] [--verbose]
                  huc existing historic ownership ecoregions output download

Riverscapes Context Tool

positional arguments:
  huc                   HUC identifier
  existing              National existing vegetation raster
  historic              National historic vegetation raster
  ownership             National land ownership shapefile
  ecoregions            National EcoRegions shapefile
  output                Path to the output folder
  download              Temporary folder for downloading data. Different HUCs may
                        share this

optional arguments:
  -h, --help            show this help message and exit
  --force               (optional) download existing files
  --temp_folder TEMP_FOLDER
                        (optional) cache for downloading files
  --verbose             (optional) a little extra logging

```

## Developer notes

### Installing this locally

If you want to use rs-context as a command-line util on your system you can either 

```
pip install git+https://github.com/Riverscapes/rs-context@master
```

For developers it's best to symlink an editable version of this package to work on.

``` bash
# In the root of this repo
pip install -e .
```

## Upgrading `rs-commons`

`rs-commons` is a shared code repo. pip has no upgrade workflow so if you need to update your version of `rs-commons` to the latest just reinstall it: 

```
pip install git+https://github.com/Riverscapes/rs-commons-python@master
```


## `.env` File

Set up this env file to help VSCode's `launch.json` fill in the parameters for riverscapes context 

```
NATIONAL_PROJECT=/SOMEPATH/somefolder/NationalDatasets
DATA_ROOT=/SOMEPATH/somefolder
DOWNLOAD_FOLDER=/SOMEPATH/somefolder/download
```