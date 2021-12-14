---
title: Help
---

A dump of some helpful materials


## Shapely

The Shapely buffer operation wasn't working until I installed it using pip but omitting the binaries.
We were also getting an assertion failure during intersect opterations. Again, omitting binaries
solved this problem.

See this [post](https://github.com/Toblerity/Shapely/issues/260)

```bash
pip uninstall shapely
pip install --no-binary :all: shapely
```

## Convert ESRI GRID to GeoTIIF

Use this command:

```bash
gdalwarp -t_srs 'EPSG:26911' ./US_140EVT/us_140evt/ ./landfire.tif
```

## Clip National Landfire to a HUC8

```bash
gdalwarp us_140evt ./test.tif -cutline ../../17040209/Shape/WBDHU8.shp -crop_to_cutline -t_srs "EPSG:26911"
```

## PyGeoprocessing

Installed using terminal using the command:

```bash
pip install pygeoprocessing --no-deps --no-binary :all:
```

Also needed to install cython using PyCharm package manager and
spatial indexing package using as per [this post](https://github.com/gboeing/osmnx/issues/45):

```bash
brew install spatialindex
```

## Convert national landfire raster to compressed GeoTiff

```bash
gdal_translate -ot Int16 -of GTiff -co COMPRESS=DEFLATE ./US_140EVT_20180618/Grid/us_140evt ./test.tif
```

## Download Jordan's old VBET projects

aws s3 sync s3://riverscapesdata . --exclude="*" --include="*/VBET/*" --dryrun

## Ownership

[BLM Surface Management Agency] geodatabase was downloaded and then the "surface management layer exported to shapefile (and reprpojected) using the following command:

```bash
ogr2ogr -f "ESRI Shapefile" ~/GISData/NationalProject/ownership/surface_management_agency.shp ~/GISData/BLM_Owneership/BLM_National_Surface_Management_Agency/sma_wm.gdb SurfaceManagementAgency -t_srs 'EPSG:4269' -select 'ADMIN_AGENCY_CODE'
```

# Precipitation

[PrISM](http://www.prism.oregonstate.edu/recent)

[PRISM on Science Base](https://www.sciencebase.gov/catalog/item/59c28f66e4b091459a61d335)

# Canopy Cover

Downloaded the 2016 National Land Cover Database (NLCD) from [Multi-Resolution Land Characteristics](https://www.mrlc.gov/data) web site. Got there from the [USGS Land Cover Institute](https://archive.usgs.gov/archive/sites/landcover.usgs.gov/usgslandcover.html) (LCI).

# Running nested scripts

Use the following syntax to run scripts that are in subfolders, but run them from the parent folder in the context of that parent folder. This makes it possible for nested python scritps to refer to other nested libraries and still find these libraries when running the script directly.

Note the `lib` below is the folder name but it is separated by a period and not a directory separator. And there is no `.py` file suffix needed.

```bash
python -m lib.build_vrt arg1 arg2 arg3
```

# Landfire 

1. Downloaded the Landfire 2 REMAP dataset as multiple, overlapping tiles. Make sure to change the raster format to GTiff before commencing any of the downloads.
1. Used the pyBRAT4 tool to build a single VRT file that references each GTiff.
1. Performed `gdal_translate` to convert the VRT into a single compressed GTiff.
1. Performed `gdal_warp` to reproject the raster.
1. Re-ran `gdal_translate` to compress the reprojcted raster (because `gdal_warp` produces bloated rasters).

```bash
gdal_translate -ot Int16 -of GTiff -co COMPRESS=DEFLATE ./landfire.vrt ./landfire_vrt.tif
gdalwarp -t_srs 'EPSG:4326' ./landfire_vrt_comp.tif ./landfire_vrt_warp.tif
gdal_translate -ot Int16 -of GTiff -co COMPRESS=DEFLATE ./landfire_vrt_warp.if ./landfire_vrt_warp_trans.tif
```

## Warehouse RS Context download

```bash
rscli download ~/GISData/brat/rs_context/17060204 --type RSContext --meta huc8=17060204 --verbose
rscli download ~/GISData/BRAT/brat/17040201 --type BRAT --meta huc8=17040201 --verbose --file-filter "Outputs" --no-delete

```

## Update the CLI

```bash
npm install -g @riverscapes/cli
```


## Other options

`NO_UI` is an environment variable that, if set, will disable all colours and progress bars. This is mostly to create cleaner log files for headless runs. 
