# Riverscapes Context 3DEP

Python 3 package to build a single high-resolution DEM from USGS 3DEP program, which can be used as a contextual layers for other python riverscapes tools.

```
usage: rs-context [-h] [--force] [--temp_folder TEMP_FOLDER] [--verbose]
                  huc existing historic ownership ecoregions output download

Riverscapes Context Tool - 3DEP

positional arguments:
  boundary_layer        Path to shapefile or geopackage layer   containing the area for which the DEM will be built
  resolution            Target resolution in metres, between 1-10
  output                Path to the output folder
  target epsg           (Ignored)
  download              Temporary folder for downloading data. Different HUCs may share this

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