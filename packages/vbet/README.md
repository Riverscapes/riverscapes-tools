# vbet
Valley Bottom Extraction Tool

This is a placeholder repo for capturing issues and documentation. The VBET code will be migrated here soon.

```
usage: vbet [-h] [--max_slope MAX_SLOPE] [--max_hand MAX_HAND] [--min_hole_area MIN_HOLE_AREA]
            [--verbose] [--meta META]
            huc flowlines flowareas slope hand hillshade output_dir

Riverscapes VBET Tool

positional arguments:
  huc                   NHD flow line ShapeFile path
  flowlines             NHD flow line ShapeFile path
  flowareas             NHD flow areas ShapeFile path
  slope                 Slope raster path
  hand                  HAND raster path
  hillshade             Hillshade raster path
  output_dir            Folder where output VBET project will be created

optional arguments:
  -h, --help            show this help message and exit
  --max_slope MAX_SLOPE
                        Maximum slope to be considered
  --max_hand MAX_HAND   Maximum HAND to be considered
  --min_hole_area MIN_HOLE_AREA
                        Minimum hole retained in valley bottom (sq m)
  --verbose             (optional) a little extra logging
  --meta META           riverscapes project metadata as comma separated key=value pairs

```


## Developer notes

1. get your virtualenv set up

***NB: # On OSX you must have run `brew install gdal` so that the header files are findable***

```bash
./scripts/create_venv.py
```

### Installing this locally

If you want to use vbet as a command-line util on your system you can either 

```
pip install git+https://github.com/Riverscapes/vbet@master
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

Contents:

```
DATA_ROOT=/SOMEPATH/somefolder
```