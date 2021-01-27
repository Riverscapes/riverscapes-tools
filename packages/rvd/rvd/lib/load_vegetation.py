import os
import xml.etree.ElementTree as ET
import sqlite3
import rasterio
import numpy as np
from rscommons.util import safe_makedirs


def load_vegetation_raster(rasterpath: str, gpkg: str, existing=False, output_folder=None):
    """[summary]

    Args:
        rasterpath ([type]): [description]
        existing (bool, optional): [description]. Defaults to False.
        output_folder ([type], optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """

    conversion_lookup = {
        "Open Water": 500,
        "Riparian": 100,
        "Hardwood": 100,
        "Grassland": 50,
        "Shrubland": 50,
        "Non-vegetated": 40,
        "Snow-Ice": 40,
        "Sparsely Vegetated": 40,
        "Barren": 40,
        "Hardwood-Conifer": 20,  # New for LANDFIRE 200
        "Conifer-Hardwood": 20,
        "Conifer": 20,
        "Developed": 2,
        "Developed-Low Intensity": 2,
        "Developed-Medium Intensity": 2,
        "Developed-High Intensity": 2,
        "Developed-Roads": 2,
        "Quarries-Strip Mines-Gravel Pits-Well and Wind Pads": 2,  # Updated for LANDFIRE 200
        "Exotic Tree-Shrub": 3,
        "Exotic Herbaceous": 3,
        "Ruderal Wet Meadow and Marsh": 3,  # New for LANDFIRE 200
        "Agricultural": 1
    } if existing else {
        "Open Water": 500,
        "Riparian": 100,
        "Hardwood": 100,
        "Shrubland": 50,
        "Grassland": 50,
        "Perennial Ice/Snow": 40,
        "Barren-Rock/Sand/Clay": 40,
        "Sparse": 40,
        "Conifer": 20,
        "Hardwood-Conifer": 20,
        "Conifer-Hardwood": 20}

    vegetated_classes = [
        "Riparian",
        "Hardwood",
        "Hardwood-Conifer",
        "Grassland",
        "Shrubland",
        "Sparsely Vegetated",
        "Conifer-Hardwood",
        "Conifer",
        "Ruderal Wet Meadow and Marsh",
        "Agricultural"
    ] if existing else [
        "Riparian",
        "Hardwood",
        "Conifer",
        "Shrubland",
        "Hardwood-Conifer",
        "Conifer-Hardwood",
        "Grassland"]

    lui_lookup = {  # TODO check for Landfire 200 updates
        "Agricultural-Aquaculture": 0.66,
        "Agricultural-Bush fruit and berries": 0.66,
        "Agricultural-Close Grown Crop": 0.66,
        "Agricultural-Fallow/Idle Cropland": 0.33,
        "Agricultural-Orchard": 0.66,
        "Agricultural-Pasture and Hayland": 0.33,
        "Agricultural-Row Crop": 0.66,
        "Agricultural-Row Crop-Close Grown Crop": 0.66,
        "Agricultural-Vineyard": 0.66,
        "Agricultural-Wheat": 0.66,
        "Developed-High Intensity": 1.0,
        "Developed-Medium Intensity": 1.0,
        "Developed-Low Intensity": 1.0,
        "Developed-Roads": 1.0,
        "Developed-Upland Deciduous Forest": 1.0,
        "Developed-Upland Evergreen Forest": 1.0,
        "Developed-Upland Herbaceous": 1.0,
        "Developed-Upland Mixed Forest": 1.0,
        "Developed-Upland Shrubland": 1.0,
        "Managed Tree Plantation - Northern and Central Hardwood and Conifer Plantation Group": 0.66,
        "Managed Tree Plantation - Southeast Conifer and Hardwood Plantation Group": 0.66,
        "Quarries-Strip Mines-Gravel Pits": 1.0}

    # Read xml for reclass - arcgis tends to overwrite this file. use csv instead and make sure to ship with rasters
    # root = ET.parse(f"{rasterpath}.aux.xml").getroot()
    # ifield_value = int(root.findall(".//FieldDefn/[Name='VALUE']")[0].attrib['index'])
    # ifield_conversion_source = int(root.findall(".//FieldDefn/[Name='EVT_PHYS']")[0].attrib['index']) if existing else int(root.findall(".//FieldDefn/[Name='GROUPVEG']")[0].attrib['index'])
    # ifield_group_name = int(root.findall(".//FieldDefn/[Name='EVT_GP_N']")[0].attrib['index']) if existing else int(root.findall(".//FieldDefn/[Name='GROUPNAME']")[0].attrib['index'])

    # conversion_values = {int(n[ifield_value].text): conversion_lookup.setdefault(n[ifield_conversion_source].text, 0) for n in root.findall(".//Row")}
    # riparian_values = {int(n[ifield_value].text): 1 if n[ifield_conversion_source].text == "Riparian" else 0 for n in root.findall(".//Row")}
    # native_riparian_values = {int(n[ifield_value].text): 1 if n[ifield_conversion_source].text == "Riparian" and not ("Introduced" in n[ifield_group_name].text) else 0 for n in root.findall(".//Row")}
    # vegetation_values = {int(n[ifield_value].text): 1 if n[ifield_conversion_source].text in vegetated_classes else 0 for n in root.findall(".//Row")}
    # lui_values = {int(n[ifield_value].text): lui_lookup.setdefault(n[ifield_group_name].text, 0) for n in root.findall(".//Row")} if existing is True else {}

    # Load reclass values
    with sqlite3.connect(gpkg) as conn:
        c = conn.cursor()
        valid_values = [v[0] for v in c.execute("SELECT VegetationID FROM VegetationTypes").fetchall()]
        conversion_values = {row[0]: conversion_lookup.setdefault(row[1], 0) for row in c.execute('SELECT VegetationID, Physiognomy FROM VegetationTypes').fetchall()}
        riparian_values = {row[0]: 1 if row[1] == "Riparian" else 0 for row in c.execute('SELECT VegetationID, Physiognomy FROM VegetationTypes').fetchall()}
        native_riparian_values = {row[0]: 1 if row[1] == "Riparian" and not("Introduced" in row[2]) else 0 for row in c.execute('SELECT VegetationID, Physiognomy, LandUseGroup FROM VegetationTypes').fetchall()}
        vegetation_values = {row[0]: 1 if row[1] in vegetated_classes else 0 for row in c.execute('SELECT VegetationID, Physiognomy FROM VegetationTypes').fetchall()}
        lui_values = {row[0]: lui_lookup.setdefault(row[1], 0) for row in c.execute('SELECT VegetationID, LandUseGroup FROM VegetationTypes').fetchall()} if existing else {}

    # Read array
    with rasterio.open(rasterpath) as raster:
        no_data = int(raster.nodatavals[0])
        conversion_values[no_data] = 0
        riparian_values[no_data] = 0
        native_riparian_values[no_data] = 0
        vegetation_values[no_data] = 0
        if existing:
            lui_values[no_data] = 0.0

        valid_values.append(no_data)
        raw_array = raster.read(1, masked=True)
        for value in np.unique(raw_array):
            if not isinstance(value, type(np.ma.masked)) and value not in valid_values:
                raise Exception(f"Vegetation raster value {value} not found in current data dictionary")

        # Reclass array https://stackoverflow.com/questions/16992713/translate-every-element-in-numpy-array-according-to-key
        riparian_array = np.vectorize(riparian_values.get)(raw_array)
        native_riparian_array = np.vectorize(native_riparian_values.get)(raw_array)
        vegetated_array = np.vectorize(vegetation_values.get)(raw_array)
        conversion_array = np.vectorize(conversion_values.get)(raw_array)
        lui_array = np.vectorize(lui_values.get)(raw_array) if existing else None

        output = {"RAW": raw_array,
                  "RIPARIAN": riparian_array,
                  "NATIVE_RIPARIAN": native_riparian_array,
                  "VEGETATED": vegetated_array,
                  "CONVERSION": conversion_array,
                  "LUI": lui_array}

        if output_folder:
            for raster_name, raster_array in output.items():
                if raster_array is not None:
                    safe_makedirs(output_folder)
                    with rasterio.open(os.path.join(output_folder, f"{'EXISTING' if existing else 'HISTORIC'}_{raster_name}.tif"),
                                       'w',
                                       driver='GTiff',
                                       height=raster.height,
                                       width=raster.width,
                                       count=1,
                                       dtype=np.int16,
                                       crs=raster.crs,
                                       transform=raster.transform) as dataset:
                        dataset.write(raster_array.astype(np.int16), 1)

    return output
