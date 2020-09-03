# Valley Bottom Extraction Tool (VBET)

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

## `.env` File

Set up this env file to help VSCode's `launch.json` fill in the parameters for riverscapes context 

Contents:

```
DATA_ROOT=/SOMEPATH/somefolder
```