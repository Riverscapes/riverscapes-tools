# US counties from Census - cartographic boundary 500k

This is an alternative data source to us_cens_county that is designed for small scale mapping and is clipped to coastline. It also happens to have much more user-friendly columns - e.g. state codes.

## Step overview

1. download
2. review. choose columns to keep 
3. filter for conus
4. fetch metadata / build layer_definitions
5. vector prep
6. upload
7. documentation

### Download

Source description:  https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html 

Source: https://www2.census.gov/geo/tiger/GENZ2025/gpkg/cb_2025_us_all_500k.zip 
Status: DONE
Operator: LSG
Output: "C:\nardata\datadownload\uscensus\cb_2025_us_all_500k.gpkg"

### Filter conus

STUSPS NOT IN ('AK', 'AS', 'MP', 'HI', 'PR', 'VI')

### Metadata

Started with the layer_definition for the us_cens_county (TIGER full res version) and modified manually based on the technical documentation

## Vector Prep

