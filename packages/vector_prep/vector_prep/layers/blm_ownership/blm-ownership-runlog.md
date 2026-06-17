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

There were some 