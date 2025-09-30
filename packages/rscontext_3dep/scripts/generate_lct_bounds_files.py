"""
Generate individual GeoJSON files for each feature in a GeoPackage layer.
Each file is named based on a specified column value.

This was written to extract LCT occupied area boundaries from a GeoPackage
provided by Robert at USGS.

Philip Bailey
26 Sep 2025
"""

import apsw
import os
import json

# ---- Configuration ----
gpkg_path = "/Users/philipbailey/GISData/lct/lct_occupied_areas_4326.gpkg"
geojson_folder = '/Users/philipbailey/GISData/lct/geojson_bounds'
layer_name = "lct_occupied_areas"       # name of the layer in the GeoPackage
name_column = "huc12"    # column to use for naming files
spatialite_path = "/opt/homebrew/Cellar/libspatialite/5.1.0_1/lib/mod_spatialite.8.dylib"


# Make sure output folder exists
os.makedirs(geojson_folder, exist_ok=True)

# Connect to GeoPackage using APSW
connection = apsw.Connection(gpkg_path)
connection.enable_load_extension(True)
cursor = connection.cursor()

# Load SpatiaLite extension (needed for AsGeoJSON)
cursor.execute(f"select load_extension('{spatialite_path}')")

# Get all features with geometry as GeoJSON
query = f"""
    SELECT {name_column}, AsGeoJSON(CastAutomagic(geom)) AS geomjson
    FROM "{layer_name}"
"""
for row in cursor.execute(query):
    name_value, geomjson = row
    if geomjson is None:
        continue  # skip features without geometry

    # Build minimal GeoJSON Feature
    feature = {
        "type": "Feature",
        "properties": {name_column: name_value},
        "geometry": json.loads(geomjson)
    }

    # Write to file
    filename = f"{name_value}.geojson"
    filepath = os.path.join(geojson_folder, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(feature, f, ensure_ascii=False, indent=2)

    print(f"Wrote {filepath}")
