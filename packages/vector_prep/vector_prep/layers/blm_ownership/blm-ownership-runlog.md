# Data preparation - Ownership

Also trying to build a more structured combination of code and json to capture what has been run, allowing for multiple re-runs of certain steps (iterations) without having to do everything from the beginning every time.

## Overview

1. download data DONE: ~/GISData/data.zip
2. unzip DONE: /workspaces/riverscapes-tools/SMA_WM.gdb
3. extract metadata
4. build layer definitions
5. run vector prep
6. review output
7. intersect with counties
8. review output
9. upload to athena

## 2B Review what we have


* Main layer is `SurfaceManagementAgency` , Multipoloygon
* Feature Count: 449531
* Extent: (-19942794.488800, 2801774.600100) - (20012451.780100, 11535890.138200)
* EPSG: 3857 
```txt
FID Column = OBJECTID
Geometry Column = SHAPE
SMA_ID: Integer(Int16) (0.0)
HOLD_ID: Integer(Int16) (0.0)
ADMIN_ST: String (2.0)
FAU_ID: Integer(Int16) (0.0)
ADMIN_UNIT_NAME: String (255.0)
ADMIN_UNIT_TYPE: String (255.0)
ADMIN_DEPT_CODE: String (6.0)
ADMIN_AGENCY_CODE: String (8.0)
HOLD_DEPT_CODE: String (255.0)
HOLD_AGENCY_CODE: String (255.0)
SHAPE_Length: Real (0.0) DEFAULT FILEGEODATABASE_SHAPE_LENGTH
SHAPE_Area: Real (0.0) DEFAULT FILEGEODATABASE_SHAPE_AREA
```

**Most important question: does this have the ownership?** No. But I think it's what we've used before. 

The Description/Abstract specifically says: 

> The ... dataset depicts Federal land for the United States and classifies this land by its active Federal surface managing agency.
> The SMA data do not illustrate land status ownership pattern boundaries or contain land ownership attribute details. 
> ... represents the polygon features that show the boundaries for Surface Management Agency and the surface extent of each Federal agency’s surface administrative jurisdiction.

And **Purpose**
> The purpose of this dataset is to fulfill the public and Government’s need to know what agency is managing Federal land in a given area, and for use by BLM Staff for use in analysis and reports. This dataset is useful as a tool to determine and illustrate the boundaries of a particular Federal agency’s “managing” area and to quantify these areas in terms of geographic acreage.

## 3/4 Metadata

From Copilot : 

FGDB has correct types (INTEGER for SMA_ID, HOLD_ID) but no descriptions (this GDB skips coded-value domains for these fields)
XML has rich descriptions + full domain enumerations (all agencies/departments) but dtype=STRING
The merged layer_definitions.json gets the best of both

## Vector Prep

Start with this config: 

```json
    "parameters": {
        "input": "~/GISData/SMA_WM.gdb",
        "layer": "SurfaceManagementAgency",
        "output": "~/GISData/SMA_WM_VP.gpkg",
        "garbage": "/GISData/SMA_WM_VP_GARBAGE.gpkg",
        "tolerance": 2,
        "min_size": 1,
        "min_size_drop": false,
        "layer_definitions": "./layer_definitions.json"
    }
```

Succeeded in about 2 hours. 
The garbage gpkg wasn't written due to error in the config path (wasn't a writable path) - fixed for next time

Open output in QGIS - select CONUS by dragging a rectangle around it
Save as new geopackage layer, dropping unnecessary fields

In QGIS, found I could not intersect. Got error "has invalid geometry".
There are 30 errors, but these cover huge areas of the country - can't ignore them. 
Errors are self-intersection and nested shells

#### Fix 
processing.run("native:fixgeometries", {'INPUT':'C:\\nardata\\temp\\SMA_WM_VP_conus.gpkg|layername=surfacemanagementagency_conus','METHOD':1,'OUTPUT':'ogr:dbname=\'C:/nardata/temp/SMA_WM_VP_conus.gpkg\' table="sma_conus_fixinq" (geom)'})
Execution completed in 177.86 seconds (2 minutes 58 seconds)
Results:
  OUTPUT: C:/nardata/temp/SMA_WM_VP_conus.gpkg|layername=sma_conus_fixinq

#### Intersect
processing.run("native:intersection", {'INPUT':'C:/nardata/temp/SMA_WM_VP_conus.gpkg|layername=sma_conus_fixinq','OVERLAY':'C:/nardata/datadownload/riverscapes_athena/us_cens_county.gpkg|layername=us_cens_county','INPUT_FIELDS':[],'OVERLAY_FIELDS':['geoidfq','namelsad','stusps'],'OVERLAY_FIELDS_PREFIX':'','OUTPUT':'ogr:dbname=\'C:/nardata/temp/SMA_WM_VP_conus.gpkg\' table="sma_cens_ixn" (geom)','GRID_SIZE':None})

Calculating intersection
Execution completed in 238.75 seconds (3 minutes 59 seconds)
Results:
  OUTPUT: C:/nardata/temp/SMA_WM_VP_conus.gpkg|layername=sma_cens_ixn

## Upload

upload to Athena