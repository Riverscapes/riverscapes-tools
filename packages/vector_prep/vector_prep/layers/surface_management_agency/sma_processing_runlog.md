# 2026-10-20

Download latest from https://gbp-blm-egis.hub.arcgis.com/datasets/6bf2e737c59d4111be92420ee5ab0b46/about
It is only available as File Geodatabase 1.18 GB when unzipped.

```cmd
$inputpath="C:\nardata\datadownload\blm\blm_sma_polygons\2026-10-19\SMA_WM.gdb"
ogrinfo -so $inputpath
```

```txt
INFO: Open of `C:\nardata\datadownload\blm\blm_sma_polygons\2026-10-19\SMA_WM.gdb'
      using driver `OpenFileGDB' successful.
Layer: SurfaceMgtAgy_DOD (Multi Polygon)
Layer: SurfaceMgtAgy_BLM (Multi Polygon)
Layer: SurfaceMgtAgy_NPS (Multi Polygon)
Layer: SurfaceMgtAgy_USFS (Multi Polygon)
Layer: SurfaceMgtAgy_FWS (Multi Polygon)
Layer: SurfaceMgtAgy_BOR (Multi Polygon)
Layer: SurfaceMgtAgy_BIA (Multi Polygon)
Layer: SurfaceMgtAgy_OTHFED (Multi Polygon)
Layer: SurfaceMgtAgy_STATE (Multi Polygon)
Layer: SurfaceMgtAgy_LOCAL (Multi Polygon)
Layer: SurfaceMgtAgy_PRIUNK (Multi Polygon)
Layer: SurfaceMgtAgy_NTVALL (Multi Polygon)
Layer: SurfaceMgtAgy_NTVPIC (Multi Polygon)
Layer: SurfaceManagementAgency (Multi Polygon)
```

`ogrinfo -so $inputpath SurfaceManagementAgency`

```txt
INFO: Open of `C:\nardata\datadownload\blm\blm_sma_polygons\2026-10-19\SMA_WM.gdb'
      using driver `OpenFileGDB' successful.

Layer name: SurfaceManagementAgency
Geometry: Multi Polygon
Feature Count: 464680
Extent: (-19942794.488800, 2801774.600100) - (20012451.780100, 11535890.138200)
Layer SRS WKT:
PROJCRS["WGS 84 / Pseudo-Mercator",
    BASEGEOGCRS["WGS 84",
        ENSEMBLE["World Geodetic System 1984 ensemble",
            MEMBER["World Geodetic System 1984 (Transit)"],
            MEMBER["World Geodetic System 1984 (G730)"],
            MEMBER["World Geodetic System 1984 (G873)"],
            MEMBER["World Geodetic System 1984 (G1150)"],
            MEMBER["World Geodetic System 1984 (G1674)"],
            MEMBER["World Geodetic System 1984 (G1762)"],
            MEMBER["World Geodetic System 1984 (G2139)"],
            MEMBER["World Geodetic System 1984 (G2296)"],
            ELLIPSOID["WGS 84",6378137,298.257223563,
                LENGTHUNIT["metre",1]],
            ENSEMBLEACCURACY[2.0]],
        PRIMEM["Greenwich",0,
            ANGLEUNIT["degree",0.0174532925199433]],
        ID["EPSG",4326]],
    CONVERSION["Popular Visualisation Pseudo-Mercator",
        METHOD["Popular Visualisation Pseudo Mercator",
            ID["EPSG",1024]],
        PARAMETER["Latitude of natural origin",0,
            ANGLEUNIT["degree",0.0174532925199433],
            ID["EPSG",8801]],
        PARAMETER["Longitude of natural origin",0,
            ANGLEUNIT["degree",0.0174532925199433],
            ID["EPSG",8802]],
        PARAMETER["False easting",0,
            LENGTHUNIT["metre",1],
            ID["EPSG",8806]],
        PARAMETER["False northing",0,
            LENGTHUNIT["metre",1],
            ID["EPSG",8807]]],
    CS[Cartesian,2],
        AXIS["easting (X)",east,
            ORDER[1],
            LENGTHUNIT["metre",1]],
        AXIS["northing (Y)",north,
            ORDER[2],
            LENGTHUNIT["metre",1]],
    USAGE[
        SCOPE["Web mapping and visualisation."],
        AREA["World between 85.06┬░S and 85.06┬░N."],
        BBOX[-85.06,-180,85.06,180]],
    ID["EPSG",3857]]
Data axis to CRS axis mapping: 1,2
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

Extract CONUS by attribute to a GeoPackage for better performance
Intersect with US Counties so we don't have enormous polygons

Vector prep & Upload

## 2026-10-20 H3 split script

Added `split_sma_conus_h3.py` in this folder to replace county intersection with H3 level 3 splitting.

Example usage:

```cmd
uv run python packages\vector_prep\vector_prep\layers\surface_management_agency\split_sma_conus_h3.py ^
  --input-gdb C:\nardata\datadownload\blm\blm_sma_polygons\2026-10-19\SMA_WM.gdb ^
  --output-gpkg C:\nardata\temp\SMA_WM_conus_h3_l3.gpkg ^
  --output-layer sma_conus_h3_l3
```

Intersecting step takes a while. Total is about 15-20 minutes.
A couple of iterations of code to get this working but seems good now.

