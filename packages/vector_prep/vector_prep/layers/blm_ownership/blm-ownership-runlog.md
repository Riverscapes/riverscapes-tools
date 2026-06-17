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
## 3/4 Metadata

From Copilot : 

FGDB has correct types (INTEGER for SMA_ID, HOLD_ID) but no descriptions (this GDB skips coded-value domains for these fields)
XML has rich descriptions + full domain enumerations (all agencies/departments) but dtype=STRING
The merged layer_definitions.json gets the best of both

