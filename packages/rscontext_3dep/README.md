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


## `.env` File

Set up this env file to help VSCode's `launch.json` fill in the parameters for riverscapes context 

```
NATIONAL_PROJECT=/SOMEPATH/somefolder/NationalDatasets
DATA_ROOT=/SOMEPATH/somefolder
DOWNLOAD_FOLDER=/SOMEPATH/somefolder/download
```