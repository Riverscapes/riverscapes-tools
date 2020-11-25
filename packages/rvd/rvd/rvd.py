#!/usr/bin/env python3
# Name:     RVD
#
# Purpose:  Build a Riparian Vegetation Departure project.
#
# Author:   Philip Bailey/Kelly Whitehead
#           Adapted from Jordan Gilbert
#
# Date:     23 Nov 2020
# -------------------------------------------------------------------------------
import argparse
import sys
import os
import glob
import traceback
import uuid
import datetime
import time
import xml.etree.ElementTree as ET
from osgeo import ogr
from osgeo import gdal
import rasterio
from rasterio import features
import numpy as np
import sqlite3
import csv
from collections import OrderedDict

from rscommons.util import safe_makedirs
from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors, ProgressBar
from rscommons.util import safe_makedirs, safe_remove_dir
from rscommons.build_network import build_network
from rscommons.segment_network import segment_network
from rscommons.database import create_database
from rscommons.database import populate_database
from rscommons.reach_attributes import write_attributes, write_reach_attributes
from rscommons.shapefile import get_geometry_unary_union_from_wkt, get_transform_from_epsg, get_transform_from_wkt
from rscommons.thiessen.vor import NARVoronoi
from rscommons.thiessen.shapes import centerline_points, clip_polygons, dissolve_by_points

# from rvd.report import report
from rvd.__version__ import __version__

from typing import List, Dict
Path = str

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/RVD.xsd', __version__)

LayerTypes = {
    # key: (name, id, tag, relpath)
    'EXVEG': RSLayer('Existing Vegetation', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HISTVEG': RSLayer('Historic Vegetation', 'HISTVEG', 'Raster', 'inputs/historic_veg.tif'),
    'FLOWLINES': RSLayer('NHD Flowlines', 'FLOWLINES', 'Vector', 'inputs/NHDFlowline.shp'),
    'FLOW_AREA': RSLayer('NHD Flow Area', 'FLOW_AREA', 'Vector', 'inputs/NHDArea.shp'),
    'WATERBODIES': RSLayer('NHD Waterbody', 'WATERBODIES', 'Vector', 'inputs/NHDWaterbody.shp'),
    'VALLEY_BOTTOM': RSLayer('Valley Bottom', 'VALLEY_BOTTOM', 'Vector', 'inputs/valley_bottom.shp'),
    'CLEANED': RSLayer('Cleaned Network', 'CLEANED', 'Vector', 'intermediates/intermediate_nhd_network.shp'),
    'NETWORK': RSLayer('Network', 'NETWORK', 'Vector', 'intermediates/network.shp'),
    'THIESSEN': RSLayer('Network', 'THIESSEN', 'Vector', 'intermediates/thiessen.shp'),
    'SEGMENTED': RSLayer('BRAT Network', 'SEGMENTED', 'Vector', 'outputs/rvd.shp'),
    'SQLITEDB': RSLayer('BRAT Database', 'BRATDB', 'SQLiteDB', 'outputs/rvd.sqlite'),  # TODO: change this to output geopackage
    'REPORT': RSLayer('RVD Report', 'RVD_REPORT', 'HTMLFile', 'outputs/rvd.html')
}  # TODO: Include intermediate rasters?

# Dictionary of fields that this process outputs, keyed by ShapeFile data type
output_fields = {
    ogr.OFTString: ['Risk', 'Limitation', 'Opportunity'],
    ogr.OFTInteger: ['RiskID', 'LimitationID', 'OpportunityID'],
    ogr.OFTReal: ['iVeg100EX', 'iVeg_30EX', 'iVeg100HPE', 'iVeg_30HPE', 'iPC_LU',
                  'iPC_VLowLU', 'iPC_LowLU', 'iPC_ModLU', 'iPC_HighLU', 'iHyd_QLow',
                  'iHyd_Q2', 'iHyd_SPLow', 'iHyd_SP2', 'oVC_HPE', 'oVC_EX', 'oCC_HPE',
                  'mCC_HPE_CT', 'oCC_EX', 'mCC_EX_CT', 'mCC_HisDep']
}

# This dictionary reassigns databae column names to 10 character limit for the ShapeFile
shapefile_field_aliases = {
    'WatershedID': 'HUC'
}

Epochs = [
    # (epoch, prefix, LayerType, OrigId)
    ('Existing', 'EX', 'EXVEG_SUIT', 'EXVEG'),
    ('Historic', 'HPE', 'HISTVEG_SUIT', 'HISTVEG')
]


def rvd(huc: int, max_length: float, min_length: float, flowlines: Path, existing_veg: Path, historic_veg: Path, valley_bottom: Path, output_folder: Path, reach_codes: List[int], flow_areas: Path, waterbodies: Path):
    """[Generate segmented reaches on flowline network and calculate RVD from historic and existing vegetation rasters

    Args:
        huc (integer): Watershed ID
        max_length (float): maximum length for reach segmentation
        min_length (float): minimum length for reach segmentation
        flowlines (Path): NHD flowlines feature layer
        existing_veg (Path): LANDFIRE version 2.00 evt raster, with adjacent xml metadata file
        historic_veg (Path): LANDFIRE version 2.00 bps raster, with adjacent xml metadata file
        valley_bottom (Path): Vbet polygon feature layer
        output_folder (Path): destination folder for project output
        reach_codes (List[int]): NHD reach codes for features to include in outputs
        flow_areas (Path): NHD flow area polygon feature layer
        waterbodies (Path): NHD waterbodies polygon feature layer
    """

    log = Logger("RVD")
    log.info('RVD v.{}'.format(cfg.version))

    try:
        int(huc)
    except ValueError:
        raise Exception('Invalid HUC identifier "{}". Must be an integer'.format(huc))

    if not (len(huc) == 4 or len(huc) == 8):
        raise Exception('Invalid HUC identifier. Must be four digit integer')

    safe_makedirs(output_folder)

    project, _realization, proj_nodes = create_project(huc, output_folder)

    log.info('Adding inputs to project')
    _prj_existing_path_node, prj_existing_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg)
    _prj_historic_path_node, prj_historic_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HISTVEG'], historic_veg)

    # Copy in the vectors we need
    _prj_flowlines_node, prj_flowlines = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['FLOWLINES'], flowlines, att_filter="\"ReachCode\" Like '{}%'".format(huc))
    _prj_flow_areas_node, prj_flow_areas = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['FLOW_AREA'], flow_areas) if flow_areas else None
    _prj_waterbodies_node, prj_waterbodies = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['WATERBODIES'], waterbodies) if waterbodies else None
    _prj_valley_bottom_node, prj_valley_bottom = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['VALLEY_BOTTOM'], valley_bottom) if valley_bottom else None

    # Other layers we need
    _cleaned_path_node, cleaned_path = project.add_project_vector(proj_nodes['Intermediates'], LayerTypes['CLEANED'], replace=True)
    _thiessen_path_node, thiessen_path = project.add_project_vector(proj_nodes['Intermediates'], LayerTypes['THIESSEN'], replace=True)
    _segmented_path_node, segmented_path = project.add_project_vector(proj_nodes['Outputs'], LayerTypes['SEGMENTED'], replace=True)
    _report_path_node, report_path = project.add_project_vector(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)

    # Generate GPKG for Intermediates
    driver_gpkg = ogr.GetDriverByName("GPKG")
    intermediate_gpkg = os.path.join(output_folder, "Intermediates", "rvd_intermediates.gpkg")
    driver_gpkg.CreateDataSource(intermediate_gpkg)
    output_gkpg = os.path.join(output_folder, "Outputs", "rvd.gpkg")
    driver_gpkg.CreateDataSource(output_gkpg)

    # Spatial Reference Setup
    spatialRef = ogr.osr.SpatialReference()
    spatialRef.ImportFromEPSG(cfg.OUTPUT_EPSG)
    spatialRef.SetAxisMappingStrategy(ogr.osr.OAMS_TRADITIONAL_GIS_ORDER)

    # Transform issues reading 102003 as espg id. Using sr wkt seems to work, however arcgis has problems loading feature classes with this method...
    srs = ogr.osr.SpatialReference()
    # raster_epsg = 102003
    # srs.ImportFromEPSG(raster_epsg)
    dataset = gdal.Open(prj_existing_path, 0)
    sr = dataset.GetProjection()
    srs.ImportFromWkt(sr)
    out_sr, transform_shp_to_raster = get_transform_from_wkt(spatialRef, sr)
    # _out_sr, transform_shp_to_raster = get_transform_from_epsg(spatialRef, raster_epsg)

    # Filter the flow lines to just the required features and then segment to desired length
    build_network(prj_flowlines, prj_flow_areas, prj_waterbodies, cleaned_path, cfg.OUTPUT_EPSG, reach_codes, None)
    segment_network(cleaned_path, segmented_path, max_length, min_length)

    metadata = {
        'RVD_DateTime': datetime.datetime.now().isoformat(),
        'Max_Length': max_length,
        'Min_Length': min_length,
        'Reach_Codes': reach_codes,
    }

    # TODO change to output geopackage
    db_path = os.path.join(output_folder, LayerTypes['SQLITEDB'].rel_path)
    watesrhed_name = create_database(huc, db_path, metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'rvd_schema.sql'))
    populate_database(db_path, segmented_path, huc)
    project.add_metadata({'Watershed': watesrhed_name})

    # Add this to the project file
    project.add_dataset(proj_nodes['Outputs'], db_path, LayerTypes['SQLITEDB'], 'SQLiteDB')

    # Generate Voroni polygons
    log.info("Calculating Voronoi Polygons...")

    # Add all the points (including islands) to the list
    flowline_thiessen_points_groups = centerline_points(segmented_path, 10.0, "ReachID", transform_shp_to_raster)
    flowline_thiessen_points = [pt for group in flowline_thiessen_points_groups.values() for pt in group]

    # Exterior is the shell and there is only ever 1
    myVorL = NARVoronoi(flowline_thiessen_points)

    # Generate Thiessen Polys
    myVorL.createshapes()

    # Dissolve by flowlines
    log.info("Dissolving Thiessen Polygons")
    dissolved_polys = dissolve_by_points(flowline_thiessen_points_groups, myVorL.polys)

    # Clip Thiessen Polys
    log.info("Clipping Thiessen Polygons to Valley Bottom")
    geom_vbottom = get_geometry_unary_union_from_wkt(prj_valley_bottom, sr)  # cfg.OUTPUT_EPSG)
    if waterbodies:
        geom_waterbodies = get_geometry_unary_union_from_wkt(prj_waterbodies, sr)
        geom_vbottom = geom_vbottom.difference(geom_waterbodies)
    if flow_areas:
        geom_flow_areas = get_geometry_unary_union_from_wkt(prj_flow_areas, sr)
        geom_vbottom = geom_vbottom.difference(geom_flow_areas)
    clipped_thiessen = clip_polygons(geom_vbottom, dissolved_polys)

    # Save Intermediates
    simple_save(clipped_thiessen.values(), ogr.wkbPolygon, out_sr, "Thiessen", intermediate_gpkg)
    simple_save(dissolved_polys.values(), ogr.wkbPolygon, out_sr, "ThiessenPolygonsDissolved", intermediate_gpkg)
    simple_save(myVorL.polys, ogr.wkbPolygon, out_sr, "ThiessenPolygonsRaw", intermediate_gpkg)
    simple_save([pt.point for pt in flowline_thiessen_points], ogr.wkbPoint, out_sr, "Thiessen_Points", intermediate_gpkg)

    # Load Vegetation Rasters
    log.info(f"Loading Existing and Historic Vegetation Rasters")
    vegetation = {}
    vegetation["EXISTING"] = load_vegetation_raster(prj_existing_path, True, output_folder=os.path.join(output_folder, 'Intermediates'))
    vegetation["HISTORIC"] = load_vegetation_raster(prj_historic_path, False, output_folder=os.path.join(output_folder, 'Intermediates'))

    # Vegetation zone calculations
    riparian_zone_arrays = {}
    riparian_zone_arrays["RIPARIAN_ZONES"] = ((vegetation["EXISTING"]["RIPARIAN"] + vegetation["HISTORIC"]["RIPARIAN"]) > 0) * 1
    riparian_zone_arrays["NATIVE_RIPARIAN_ZONES"] = ((vegetation["EXISTING"]["NATIVE_RIPARIAN"] + vegetation["HISTORIC"]["NATIVE_RIPARIAN"]) > 0) * 1
    riparian_zone_arrays["VEGETATION_ZONES"] = ((vegetation["EXISTING"]["VEGETATED"] + vegetation["HISTORIC"]["VEGETATED"]) > 0) * 1

    # Save Intermediate Rasters
    for name, raster in riparian_zone_arrays.items():
        save_numpy_to_geotiff(raster, os.path.join(output_folder, "Intermediates", f"{name}.tif"), prj_existing_path)

    # Calculate Riparian Departure per Reach
    riparian_arrays = {f"{epoch}_{name}_MEAN": array for epoch, arrays in vegetation.items() for name, array in arrays.items() if name in ["RIPARIAN", "NATIVE_RIPARIAN"]}
    reach_average_riparian = extract_mean_values_by_polygon(clipped_thiessen, riparian_arrays, prj_existing_path)
    riparian_departure_values = riparian_departure(reach_average_riparian)

    # Generate Vegetation Conversions
    vegetation_change = (vegetation["HISTORIC"]["CONVERSION"] - vegetation["EXISTING"]["CONVERSION"])
    save_numpy_to_geotiff(vegetation_change, os.path.join(output_folder, "Intermediates", "Conversion_Raster.tif"), prj_existing_path)
    conversion_classifications = [row for row in csv.DictReader(open(os.path.join(os.path.dirname(__file__), "conversion_proportions.csv"), "rt"))]

    # Split vegetation change classes into binary arrays
    vegetation_change_arrays = {c["ConversionType"]: (vegetation_change == int(c["ConversionValue"])) * 1 if int(c["ConversionValue"]) in np.unique(vegetation_change) else None for c in conversion_classifications}

    # Calcuate vegetation conversion per reach
    reach_values = extract_mean_values_by_polygon(clipped_thiessen, vegetation_change_arrays, prj_existing_path)

    # Add Conversion Code, Type to Vegetation Conversion
    reach_values_with_conversion_codes = classify_conversions(reach_values, conversion_classifications)

    # Write Output to GPKG table
    # TODO clean this up a bit?
    # TODO figure out why main.empty_table is getting generated
    with sqlite3.connect(output_gkpg) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE rvd_values (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ReachID INTEGER,
                EXISTING_RIPARIAN_MEAN REAL,
                HISTORIC_RIPARIAN_MEAN REAL,
                RIPARIAN_DEPARTURE REAL,
                EXISTING_NATIVE_RIPARIAN_MEAN REAL,
                HISTORIC_NATIVE_RIPARIAN_MEAN REAL,
                NATIVE_RIPARIAN_DEPARTURE REAL,
                FromConifer REAL,
                FromDevegetated REAL,
                FromGrassShrubland REAL,
                FromDeciduous REAL,
                NoChange REAL,
                Deciduous REAL,
                GrassShrubland REAL,
                Devegetation REAL,
                Conifer REAL,
                Invasive REAL,
                Development REAL,
                Agriculture REAL,
                ConversionCode INTEGER,
                ConversionType TEXT)''')
        conn.commit()
        cursor.execute('''INSERT INTO gpkg_contents (table_name, data_type) VALUES ('rvd_values', 'attributes')''')
        conn.commit()
        cursor.executemany('''INSERT INTO rvd_values (
                ReachID,
                FromConifer,
                FromDevegetated,
                FromGrassShrubland,
                FromDeciduous,
                NoChange,
                Deciduous,
                GrassShrubland,
                Devegetation,
                Conifer,
                Invasive,
                Development,
                Agriculture,
                ConversionCode,
                ConversionType)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', [
            (reach_id,
             value["FromConifer"],
             value["FromDevegetated"],
             value["FromGrassShrubland"],
             value["FromDeciduous"],
             value["NoChange"],
             value["Deciduous"],
             value["GrassShrubland"],
             value["Devegetation"],
             value["Conifer"],
             value["Invasive"],
             value["Development"],
             value["Agriculture"],
             value["ConversionCode"],
             value["ConversionType"]) for reach_id, value in reach_values_with_conversion_codes.items()])
        conn.commit()
        cursor.executemany('''UPDATE rvd_values SET
                EXISTING_RIPARIAN_MEAN=?,
                HISTORIC_RIPARIAN_MEAN=?,
                RIPARIAN_DEPARTURE=?,
                EXISTING_NATIVE_RIPARIAN_MEAN=?,
                HISTORIC_NATIVE_RIPARIAN_MEAN=?,
                NATIVE_RIPARIAN_DEPARTURE=?
                WHERE ReachID=?''', [(
            value["EXISTING_RIPARIAN_MEAN"],
            value["HISTORIC_RIPARIAN_MEAN"],
            value["RIPARIAN_DEPARTURE"],
            value["EXISTING_NATIVE_RIPARIAN_MEAN"],
            value["HISTORIC_NATIVE_RIPARIAN_MEAN"],
            value["NATIVE_RIPARIAN_DEPARTURE"],
            reachid) for reachid, value in riparian_departure_values.items()])
        conn.commit()

    # Calculate the proportion of vegetation for each vegetation Epoch
    for epoch, prefix, ltype, orig_id in Epochs:
        log.info('Processing {} epoch'.format(epoch))

        # TODO Copy BRAT build output fields from SQLite to ShapeFile in batches according to data type
    log.info('Copying values from SQLite to output ShapeFile')
    # write_reach_attributes(segmented_path, db_path, output_fields, shapefile_field_aliases)

    # report(db_path, report_path)

    log.info('RVD complete')


def classify_conversions(arrays: Dict[int, Dict[str, float]], conversion_classifications: List[dict]):
    """classify conversion by code and type using binned classes

    Args:
        arrays (dict): reach dictionaries with conversion type dictionaries to classify
        conversion_classifications (List(dict)): list of dicitionaries generated by conversion csv

    Returns:
        Dict: reach dictionary of conversion types and codes
    """

    bins = OrderedDict([("Very Minor", 0.1),
                        ("Minor", 0.25),
                        ("Moderate", 0.5),
                        ("Significant", 1.0)])  # value <= bin
    output = {}

    pos_classes = {value["ConversionType"]: value for value in conversion_classifications if int(value["ConversionValue"]) > 0}
    neg_classes = {value["ConversionType"]: value for value in conversion_classifications if int(value["ConversionValue"]) < 0}
    for reach_id, reach_values in arrays.items():
        pos_reach_values = {key: reach_values[key] for key in pos_classes.keys()}
        neg_reach_values = {key: reach_values[key] for key in neg_classes.keys()}
        sum_neg_classes = sum(list(neg_reach_values.values()))

        if reach_values["NoChange"] >= 0.85:  # no change is over .85
            reach_values["ConversionCode"] = 1
            reach_values["ConversionType"] = "No Change"
            output[reach_id] = reach_values
        elif all([value < sum_neg_classes for value in pos_reach_values.values()]):
            for text, b, code in zip(bins.keys(), bins.values(), [70, 71, 72, 73]):
                if sum_neg_classes <= b:
                    reach_values["ConversionCode"] = int(code)
                    reach_values["ConversionType"] = f"{text} {f'Change' if text in ['Very Minor', 'Minor'] else 'Riparian Expansion'}"
                    output[reach_id] = reach_values
                    break
        elif any([v > 0.0 for v in pos_reach_values.values()]):
            key = max(pos_reach_values, key=pos_reach_values.get)
            classification = pos_classes[key]
            for text, b in bins.items():
                if reach_values[key] <= b:  # check
                    reach_values["ConversionCode"] = int(classification[text.replace(" ", "")])
                    reach_values["ConversionType"] = f"{text} {f'Change' if text in ['Very Minor', 'Minor'] else f'Conversion to {key}'}"
                    output[reach_id] = reach_values
                    break
        else:
            reach_values["ConversionCode"] = 0
            reach_values["ConversionType"] = f"Unknown"
            output[reach_id] = reach_values
    return output


def extract_mean_values_by_polygon(polys, rasters, reference_raster):

    with rasterio.open(reference_raster) as dataset:

        output = {}
        for reachid, poly in polys.items():
            if poly.geom_type in ["Polygon", "MultiPolygon"]:
                values = {}
                reach_raster = np.ma.masked_invalid(
                    features.rasterize(
                        [poly],
                        out_shape=dataset.shape,
                        transform=dataset.transform,
                        all_touched=True,
                        fill=np.nan))

                for key, raster in rasters.items():
                    if raster is not None:
                        current_raster = np.ma.masked_array(raster, mask=reach_raster.mask)
                        values[key] = np.ma.mean(current_raster)
                    else:
                        values[key] = 0.0
                output[reachid] = values
                print(f"Reach: {reachid} | {sum([v for v in values.values() if v is not None]):.2f}")
            else:
                print(f"Reach: {reachid} | WARNING no geom")
    return output


def load_vegetation_raster(rasterpath, existing=False, output_folder=None):

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

    # rows = [row for row in csv.Reader(open(f"{rasterpath}.meta.csv"), "rt")]
    # field_names = rows.pop(0)

    # conversion_values = {int(row[0]): conversion_lookup.setdefault(row[field_names.index("" if exisitng else "")], 0) for row in rows}
    # riparian_values = {}
    # native_riparian_values = {}
    # vegetation_values = {}
    # lui_values = {} if existing else None
    # Read xml for reclass - arcgis tends to overwrite this file. use csv instead and make sure to ship with rasters
    root = ET.parse(f"{rasterpath}.aux.xml").getroot()
    ifield_value = int(root.findall(".//FieldDefn/[Name='VALUE']")[0].attrib['index'])
    ifield_conversion_source = int(root.findall(".//FieldDefn/[Name='EVT_PHYS']")[0].attrib['index']) if existing else int(root.findall(".//FieldDefn/[Name='GROUPVEG']")[0].attrib['index'])
    ifield_group_name = int(root.findall(".//FieldDefn/[Name='EVT_GP_N']")[0].attrib['index']) if existing else int(root.findall(".//FieldDefn/[Name='GROUPNAME']")[0].attrib['index'])

    # Load reclass values

    conversion_values = {int(n[ifield_value].text): conversion_lookup.setdefault(n[ifield_conversion_source].text, 0) for n in root.findall(".//Row")}
    riparian_values = {int(n[ifield_value].text): 1 if n[ifield_conversion_source].text == "Riparian" else 0 for n in root.findall(".//Row")}
    native_riparian_values = {int(n[ifield_value].text): 1 if n[ifield_conversion_source].text == "Riparian" and not ("Introduced" in n[ifield_group_name].text) else 0 for n in root.findall(".//Row")}
    vegetation_values = {int(n[ifield_value].text): 1 if n[ifield_conversion_source].text in vegetated_classes else 0 for n in root.findall(".//Row")}
    lui_values = {int(n[ifield_value].text): lui_lookup.setdefault(n[ifield_group_name].text, 0) for n in root.findall(".//Row")} if existing else None

    # Read array
    with rasterio.open(rasterpath) as raster:
        no_data = raster.nodatavals[0]
        conversion_values[no_data] = 0
        riparian_values[no_data] = 0
        native_riparian_values[no_data] = 0
        vegetation_values[no_data] = 0
        if existing:
            lui_values[no_data] = 0.0

        # Reclass array https://stackoverflow.com/questions/16992713/translate-every-element-in-numpy-array-according-to-key
        raw_array = raster.read(1)
        riparian_array = np.vectorize(riparian_values.get)(raw_array).astype(int)
        native_riparian_array = np.vectorize(native_riparian_values.get)(raw_array).astype(int)
        vegetated_array = np.vectorize(vegetation_values.get)(raw_array).astype(int)
        conversion_array = np.vectorize(conversion_values.get)(raw_array).astype(int)
        lui_array = np.vectorize(lui_values.get)(raw_array).astype(int) if existing else None

        output = {"RAW": raw_array,
                  "RIPARIAN": riparian_array,
                  "NATIVE_RIPARIAN": native_riparian_array,
                  "VEGETATED": vegetated_array,
                  "CONVERSION": conversion_array,
                  "LUI": lui_array}

        if output_folder:
            for raster_name, raster_array in output.items():
                if raster_array is not None:
                    with rasterio.open(os.path.join(output_folder, f"{'EXISTING' if existing else 'HISTORIC'}_{raster_name}.tiff"),
                                       'w',
                                       driver='GTiff',
                                       height=raster.height,
                                       width=raster.width,
                                       count=1,
                                       dtype=raster_array.dtype,
                                       crs=raster.crs,
                                       transform=raster.transform) as dataset:
                        dataset.write(raster_array, 1)

    return output


def riparian_departure(mean_riparian):
    output = {}
    for reachid, values in mean_riparian.items():
        for ripariantype in ["RIPARIAN", "NATIVE_RIPARIAN"]:
            values[f"{ripariantype}_DEPARTURE"] = values[f"EXISTING_{ripariantype}_MEAN"] / values[f"HISTORIC_{ripariantype}_MEAN"] if values[f"HISTORIC_{ripariantype}_MEAN"] != 0.0 else 0.0
        output[reachid] = values

    return output


def simple_save(list_geoms, ogr_type, srs, layer_name, out_folder):
    if out_folder[-4:] == 'gpkg':
        out_file = os.path.join(out_folder, f"{layer_name}")
        driver = ogr.GetDriverByName("GPKG")
        data_source = driver.Open(out_folder, 1)
    else:
        out_file = os.path.join(out_folder, f"{layer_name}.shp")
        driver = ogr.GetDriverByName("ESRI Shapefile")
        data_source = driver.CreateDataSource(out_file)
        # driver.Open(out_file, 1)

    lyr = data_source.CreateLayer(layer_name, srs, geom_type=ogr_type)
    featdef = lyr.GetLayerDefn()

    progbar = ProgressBar(len(list_geoms), 50, f"Saving {out_file}")
    counter = 0
    progbar.update(counter)
    lyr.StartTransaction()
    for geom in list_geoms:
        counter += 1
        progbar.update(counter)
        save_geom_to_feature(lyr, featdef, geom)
    lyr.CommitTransaction()
    lyr = None
    data_source = None
    return out_file


def save_numpy_to_geotiff(array, out_file, reference_raster):
    with rasterio.open(reference_raster) as reference:
        with rasterio.open(out_file,
                           "w",
                           driver="GTiff",
                           height=array.shape[0],
                           width=array.shape[1],
                           count=1,
                           dtype=array.dtype,
                           crs=reference.crs,
                           transform=reference.transform) as new:
            new.write(array, 1)


def save_geom_to_feature(out_layer, feature_def, geom, attributes=None):
    """save shapely geometry as a new feature

    Args:
        out_layer (ogr layer): output feature layer
        feature_def (ogr feature definition): feature definition of the output feature layer
        geom (geometry): geometry to save to feature
        attributes (dict, optional): dictionary of fieldname and attribute values. Defaults to None.
    """
    feature = ogr.Feature(feature_def)
    geom_ogr = ogr.CreateGeometryFromWkb(geom.wkb)
    feature.SetGeometry(geom_ogr)
    if attributes:
        for field, value in attributes.items():
            feature.SetField(field, value)
    out_layer.CreateFeature(feature)
    feature = None


def create_project(huc, output_dir):

    project_name = 'RVD for HUC {}'.format(huc)
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'RVD')

    project.add_metadata({
        'HUC{}'.format(len(huc)): str(huc),
        'RVDVersion': cfg.version,
        'RVDTimestamp': str(int(time.time()))
    })

    realizations = project.XMLBuilder.add_sub_element(project.XMLBuilder.root, 'Realizations')
    realization = project.XMLBuilder.add_sub_element(realizations, 'RVD', None, {
        'id': 'RVD1',
        'dateCreated': datetime.datetime.now().isoformat(),
        'guid': str(uuid.uuid1()),
        'productVersion': cfg.version
    })

    proj_nodes = {
        'Name': project.XMLBuilder.add_sub_element(realization, 'Name', project_name),
        'Inputs': project.XMLBuilder.add_sub_element(realization, 'Inputs'),
        'Intermediates': project.XMLBuilder.add_sub_element(realization, 'Intermediates'),
        'Outputs': project.XMLBuilder.add_sub_element(realization, 'Outputs')
    }

    project.XMLBuilder.write()
    return project, realization, proj_nodes


def main():
    parser = argparse.ArgumentParser(
        description='RVD',
        # epilog="This is an epilog"
    )

    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('max_length', help='Maximum length of features when segmenting. Zero causes no segmentation.', type=float)
    parser.add_argument('min_length', help='min_length input', type=float)
    parser.add_argument('flowlines', help='flowlines input', type=str)
    parser.add_argument('existing', help='National existing vegetation raster', type=str)
    parser.add_argument('historic', help='National historic vegetation raster', type=str)
    parser.add_argument('valley_bottom', help='Valley bottom shapeFile', type=str)
    parser.add_argument('output_folder', help='output_folder input', type=str)

    parser.add_argument('--reach_codes', help='Comma delimited reach codes (FCode) to retain when filtering features. Omitting this option retains all features.', type=str)
    parser.add_argument('--flow_areas', help='(optional) path to the flow area polygon feature class containing artificial paths', type=str)
    parser.add_argument('--waterbodies', help='(optional) waterbodies input', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help="(optional) save intermediate outputs for debugging", action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    reach_codes = args.reach_codes.split(',') if args.reach_codes else None

    # Initiate the log file
    log = Logger("RVD")
    log.setup(logPath=os.path.join(args.output_folder, "rvd.log"), verbose=args.verbose)
    log.title('RVD For HUC: {}'.format(args.huc))

    try:
        rvd(args.huc,
            args.max_length, args.min_length, args.flowlines,
            args.existing, args.historic, args.valley_bottom,
            args.output_folder,
            reach_codes,
            args.flow_areas, args.waterbodies)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
