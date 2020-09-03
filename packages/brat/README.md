# Beaver Restoration Assessment Tool (BRAT)

This repo contains a new, experimental version of the [Beaver Restoration Assessment Tool](http://brat.riverscapes.xyz) (BRAT). The official version of this software is still [pyBRAT 3](https://github.com/Riverscapes/pyBRAT) that relies on ArcGIS geoprocessing. This new version uses only open source technologies and does not require ArcGIS to run.

It is strongly advised that you continue to use pyBRAT 3 until this new version of BRAT is verified and published as an official version of the BRAT model.

## Brat Build Tool

```
usage: bratbuild [-h] [--reach_codes REACH_CODES] [--canal_codes CANAL_CODES] [--flow_areas FLOW_AREAS] [--waterbodies WATERBODIES] [--max_waterbody MAX_WATERBODY] [--verbose]
                 huc max_length min_length dem slope hillshade flow_accum drainarea_sqkm flowlines existing_veg historical_veg valley_bottom roads rail canals ownership streamside_buffer riparian_buffer
                 max_drainage_area elevation_buffer output_folder

Build the inputs for an eventual brat_run:

positional arguments:
  huc                   huc input
  max_length            Maximum length of features when segmenting. Zero causes no segmentation.
  min_length            min_length input
  dem                   dem input
  slope                 slope input
  hillshade             hillshade input
  flow_accum            flow accumulation input
  drainarea_sqkm        drainage area input
  flowlines             flowlines input
  existing_veg          existing_veg input
  historical_veg        historical_veg input
  valley_bottom         Valley bottom shapeFile
  roads                 Roads shapeFile
  rail                  Railways shapefile
  canals                Canals shapefile
  ownership             Ownership shapefile
  streamside_buffer     streamside_buffer input
  riparian_buffer       riparian_buffer input
  max_drainage_area     max_drainage_area input
  elevation_buffer      elevation_buffer input
  output_folder         output_folder input

optional arguments:
  -h, --help            show this help message and exit
  --reach_codes REACH_CODES
                        Comma delimited reach codes (FCode) to retain when filtering features. Omitting this option retains all features.
  --canal_codes CANAL_CODES
                        Comma delimited reach codes (FCode) representing canals. Omitting this option retains all features.
  --flow_areas FLOW_AREAS
                        (optional) path to the flow area polygon feature class containing artificial paths
  --waterbodies WATERBODIES
                        (optional) waterbodies input
  --max_waterbody MAX_WATERBODY
                        (optional) maximum size of small waterbody artificial flows to be retained
  --verbose             (optional) a little extra logging
```

## Brat Run Tool

```
usage: bratrun [-h] [--csv_dir] [--verbose] project

Run brat against a pre-existing sqlite db:

positional arguments:
  project     Riverscapes project folder or project xml file

optional arguments:
  -h, --help  show this help message and exit
  --csv_dir   (optional) directory where we can find updated lookup tables
  --verbose   (optional) a little extra logging

```

## Questions

Questions about this repo can be directed to info@northarrowresearch.com

## License

[GNU general public license](https://github.com/NorthArrowResearch/pyBRAT4/blob/master/LICENSE)


## Environments

`.env`

```
NATIONAL_PROJECT=/SOMEPATH/bratDockerShare/NationalDatasets
DATA_ROOT=/SOMEPATH/bratDockerShare

DOWNLOAD_FOLDER=/SOMEPATH/bratDockerShare/download

FLOW_DEM=/SOMEPATH/bratDockerShare/brat_inputs_debug/tmp/dem.tif
FLOW_ACCUM=/SOMEPATH/bratDockerShare/brat_inputs_debug/tmp/flow_accum.tif
FLOW_DRAINAGE=/SOMEPATH/bratDockerShare/brat_inputs_debug/tmp/drainage.tif

SEGMENTED_IN=/SOMEPATH/Discharge/16010201/brat_input/nhd/NHDFlowline.shp
SEGMENTED_OUT=/SOMEPATH/Discharge/16010201/brat_input/nhd/NHDFlowline_SEGMNENTED.shp


WATERSHED_BOUNDARIES_GDB=/SOMEPATH/WBD_National_GDB.gdb
EXISTING_VEG_CSV=/SOMEPATH/ExistingVegetation_AllEcoregions.csv

GOOGLE_SHEET=XXXXXXXXXXXXXXXXXX
```

`.env.validation`

```
NATIONAL_PROJECT=/SOMEPATH/bratDockerShare/NationalDatasets
DATA_ROOT=/SOMEPATH/bratDockerShare
INPUTS_FOLDER=/SOMEPATH/bratDockerShare/inputs
```