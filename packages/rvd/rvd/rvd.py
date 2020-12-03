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
from osgeo import ogr, gdal, osr
import rasterio
from rasterio import features
import numpy as np
import sqlite3
import csv
from collections import OrderedDict
from typing import List, Dict
from rscommons.database import dict_factory

from rscommons.util import safe_makedirs
from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors, ProgressBar, Timer
from rscommons.util import safe_makedirs, safe_remove_dir
from rscommons import GeopackageLayer, ShapefileLayer, VectorBase, get_shp_or_gpkg
from rscommons.build_network import build_network_NEW
from rscommons.segment_network import segment_network_NEW
from rscommons.database import create_database_NEW
from rscommons.database import populate_database_NEW
from rscommons.reach_attributes import write_attributes, write_reach_attributes
from rscommons.vector_ops import get_geometry_unary_union, copy_feature_class
from rscommons.thiessen.vor import NARVoronoi
from rscommons.thiessen.shapes import centerline_points, clip_polygons, dissolve_by_points

# from rvd.report import report
from rvd.lib.load_vegetation import load_vegetation_raster
from rvd.lib.classify_conversions import classify_conversions
from rvd.__version__ import __version__


Path = str

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/RVD.xsd', __version__)

LayerTypes = {
    # key: (name, id, tag, relpath)
    'EXVEG': RSLayer('Existing Vegetation', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HISTVEG': RSLayer('Historic Vegetation', 'HISTVEG', 'Raster', 'inputs/historic_veg.tif'),
    'INPUTS': RSLayer('Confinement', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'FLOWLINES': RSLayer('NHD Flowlines', 'FLOWLINES', 'Vector', 'NHDFlowline'),
        'FLOW_AREA': RSLayer('NHD Flow Area', 'FLOW_AREA', 'Vector', 'NHDArea'),
        'WATERBODIES': RSLayer('NHD Waterbody', 'WATERBODIES', 'Vector', 'NHDWaterbody'),
        'VALLEY_BOTTOM': RSLayer('Valley Bottom', 'VALLEY_BOTTOM', 'Vector', 'valley_bottom'),
    }),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/intermediates.gpkg', {
        'CLEANED': RSLayer('Cleaned Network', 'CLEANED', 'Vector', 'intermediate_nhd_network'),
        'NETWORK': RSLayer('Network', 'NETWORK', 'Vector', 'network'),
        'THIESSEN': RSLayer('Network', 'THIESSEN', 'Vector', 'thiessen'),
    }),
    'OUTPUTS': RSLayer('RVD', 'OUTPUTS', 'Geopackage', 'outputs/outputs.gpkg', {
        'RVD': RSLayer('RVD', 'SEGMENTED', 'Vector', 'Reaches')
    }),
    'REPORT': RSLayer('RVD Report', 'RVD_REPORT', 'HTMLFile', 'outputs/rvd.html')
}


def rvd(huc: int, max_length: float, min_length: float, flowlines_orig: Path, existing_veg_orig: Path, historic_veg_orig: Path,
        valley_bottom_orig: Path, output_folder: Path, reach_codes: List[int], flow_areas_orig: Path, waterbodies_orig: Path):
    """[Generate segmented reaches on flowline network and calculate RVD from historic and existing vegetation rasters

    Args:
        huc (integer): Watershed ID
        max_length (float): maximum length for reach segmentation
        min_length (float): minimum length for reach segmentation
        flowlines_orig (Path): NHD flowlines feature layer
        existing_veg_orig (Path): LANDFIRE version 2.00 evt raster, with adjacent xml metadata file
        historic_veg_orig (Path): LANDFIRE version 2.00 bps raster, with adjacent xml metadata file
        valley_bottom_orig (Path): Vbet polygon feature layer
        output_folder (Path): destination folder for project output
        reach_codes (List[int]): NHD reach codes for features to include in outputs
        flow_areas_orig (Path): NHD flow area polygon feature layer
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
    _prj_existing_path_node, prj_existing_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg_orig)
    _prj_historic_path_node, prj_historic_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HISTVEG'], historic_veg_orig)

    # TODO: Don't forget the att_filter
    # _prj_flowlines_node, prj_flowlines = project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'], flowlines, att_filter="\"ReachCode\" Like '{}%'".format(huc))
    # Copy in the vectors we need
    inputs_gpkg_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg_path = os.path.join(output_folder, LayerTypes['INTERMEDIATES'].rel_path)
    outputs_gpkg_path = os.path.join(output_folder, LayerTypes['OUTPUTS'].rel_path)

    # Create 3 empty geopackages
    GeopackageLayer(inputs_gpkg_path, delete_dataset=True)
    GeopackageLayer(intermediates_gpkg_path, delete_dataset=True)

    # Copy our input layers and also find the difference in the geometry for the valley bottom
    flowlines_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    vbottom_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['VALLEY_BOTTOM'].rel_path)

    copy_feature_class(flowlines_orig, flowlines_path, epsg=cfg.OUTPUT_EPSG)
    copy_feature_class(valley_bottom_orig, vbottom_path, epsg=cfg.OUTPUT_EPSG)

    with GeopackageLayer(flowlines_path) as flow_lyr:
        # Set the output spatial ref as this for the whole project
        out_srs = flow_lyr.spatial_ref
        distance_buffer = flow_lyr.rough_convert_metres_to_vector_units(10)

    # Transform issues reading 102003 as espg id. Using sr wkt seems to work, however arcgis has problems loading feature classes with this method...
    raster_srs = ogr.osr.SpatialReference()
    ds = gdal.Open(prj_existing_path, 0)
    raster_srs.ImportFromWkt(ds.GetProjectionRef())
    raster_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    transform_shp_to_raster = VectorBase.get_transform(out_srs, raster_srs)

    # Create the output feature class fields
    with GeopackageLayer(outputs_gpkg_path, layer_name='Reaches', delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiLineString, spatial_ref=out_srs, fields={
            'GNIS_NAME': ogr.OFTString,
            'ReachCode': ogr.OFTString,
            'TotDASqKm': ogr.OFTReal,
            'NHDPlusID': ogr.OFTReal,
            'ReachID': ogr.OFTInteger,
            'WatershedID': ogr.OFTInteger,
        }, options=['FID=ReachID'])

    metadata = {
        'RVD_DateTime': datetime.datetime.now().isoformat(),
        'Max_Length': max_length,
        'Min_Length': min_length,
        'Reach_Codes': reach_codes,
    }

    # Execute the SQL to create the lookup tables in the RVD geopackage SQLite database
    watershed_name = create_database_NEW(huc, outputs_gpkg_path, metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'rvd_schema.sql'))
    project.add_metadata({'Watershed': watershed_name})

    geom_vbottom = get_geometry_unary_union(vbottom_path, spatial_ref=raster_srs)

    flowareas_path = None
    if flow_areas_orig:
        flowareas_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['FLOW_AREA'].rel_path)
        copy_feature_class(flow_areas_orig, flowareas_path, epsg=cfg.OUTPUT_EPSG)
        geom_flow_areas = get_geometry_unary_union(flowareas_path)
        # Difference with existing vbottom
        geom_vbottom = geom_vbottom.difference(geom_flow_areas)
    else:
        del LayerTypes['INPUTS'].sub_layers['FLOW_AREA']

    waterbodies_path = None
    if waterbodies_orig:
        waterbodies_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['WATERBODIES'].rel_path)
        copy_feature_class(waterbodies_orig, waterbodies_path, epsg=cfg.OUTPUT_EPSG)
        geom_waterbodies = get_geometry_unary_union(waterbodies_path)
        # Difference with existing vbottom
        geom_vbottom = geom_vbottom.difference(geom_waterbodies)
    else:
        del LayerTypes['INPUTS'].sub_layers['WATERBODIES']

    # Add the inputs to the XML
    _nd, _in_gpkg_path, _sublayers = project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])

    # Filter the flow lines to just the required features and then segment to desired length
    # TODO: These are brat methods that need to be refactored to use VectorBase layers
    cleaned_path = os.path.join(output_folder, LayerTypes['INTERMEDIATES'].rel_path, LayerTypes['INTERMEDIATES'].sub_layers['CLEANED'].rel_path)
    build_network_NEW(flowlines_path, flowareas_path, cleaned_path, waterbodies_path=waterbodies_path, epsg=cfg.OUTPUT_EPSG, reach_codes=reach_codes)

    segmented_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['RVD'].rel_path)
    segment_network_NEW(cleaned_path, segmented_path, max_length, min_length, huc)

    # Generate Voroni polygons
    log.info("Calculating Voronoi Polygons...")

    # Add all the points (including islands) to the list
    flowline_thiessen_points_groups = centerline_points(segmented_path, distance_buffer, transform_shp_to_raster)
    flowline_thiessen_points = [pt for group in flowline_thiessen_points_groups.values() for pt in group]
    simple_save([pt.point for pt in flowline_thiessen_points], ogr.wkbPoint, raster_srs, "Thiessen_Points", intermediates_gpkg_path)

    # Exterior is the shell and there is only ever 1
    myVorL = NARVoronoi(flowline_thiessen_points)

    # Generate Thiessen Polys
    myVorL.createshapes()

    # Dissolve by flowlines
    log.info("Dissolving Thiessen Polygons")
    dissolved_polys = myVorL.dissolve_by_property('fid')

    # Clip Thiessen Polys
    log.info("Clipping Thiessen Polygons to Valley Bottom")

    clipped_thiessen = clip_polygons(geom_vbottom, dissolved_polys)

    # Save Intermediates
    simple_save(clipped_thiessen.values(), ogr.wkbPolygon, raster_srs, "Thiessen", intermediates_gpkg_path)
    simple_save(dissolved_polys.values(), ogr.wkbPolygon, raster_srs, "ThiessenPolygonsDissolved", intermediates_gpkg_path)
    simple_save(myVorL.polys, ogr.wkbPolygon, raster_srs, "ThiessenPolygonsRaw", intermediates_gpkg_path)

    # OLD METHOD FOR AUDIT
    # dissolved_polys2 = dissolve_by_points(flowline_thiessen_points_groups, myVorL.polys)
    # simple_save(dissolved_polys2.values(), ogr.wkbPolygon, out_srs, "ThiessenPolygonsDissolved_OLD", intermediates_gpkg_path)

    # Load Vegetation Rasters
    log.info(f"Loading Existing and Historic Vegetation Rasters")
    vegetation = {}
    vegetation["EXISTING"] = load_vegetation_raster(prj_existing_path, True, output_folder=os.path.join(output_folder, 'Intermediates'))
    vegetation["HISTORIC"] = load_vegetation_raster(prj_historic_path, False, output_folder=os.path.join(output_folder, 'Intermediates'))

    if vegetation["EXISTING"]["RAW"].shape != vegetation["HISTORIC"]["RAW"].shape:
        raise Exception('Vegetation raster shapes are not equal Existing={} Historic={}. Cannot continue'.format(vegetation["EXISTING"]["RAW"].shape, vegetation["HISTORIC"]["RAW"].shape))

    # Vegetation zone calculations
    riparian_zone_arrays = {}
    riparian_zone_arrays["RIPARIAN_ZONES"] = ((vegetation["EXISTING"]["RIPARIAN"] + vegetation["HISTORIC"]["RIPARIAN"]) > 0) * 1
    riparian_zone_arrays["NATIVE_RIPARIAN_ZONES"] = ((vegetation["EXISTING"]["NATIVE_RIPARIAN"] + vegetation["HISTORIC"]["NATIVE_RIPARIAN"]) > 0) * 1
    riparian_zone_arrays["VEGETATION_ZONES"] = ((vegetation["EXISTING"]["VEGETATED"] + vegetation["HISTORIC"]["VEGETATED"]) > 0) * 1

    # Save Intermediate Rasters
    for name, raster in riparian_zone_arrays.items():
        save_intarr_to_geotiff(raster, os.path.join(output_folder, "Intermediates", f"{name}.tif"), prj_existing_path)

    # Calculate Riparian Departure per Reach
    riparian_arrays = {f"{epoch}_{name}_MEAN": array for epoch, arrays in vegetation.items() for name, array in arrays.items() if name in ["RIPARIAN", "NATIVE_RIPARIAN"]}
    reach_average_riparian = extract_mean_values_by_polygon(clipped_thiessen, riparian_arrays, prj_existing_path)
    riparian_departure_values = riparian_departure(reach_average_riparian)

    # Generate Vegetation Conversions
    vegetation_change = (vegetation["HISTORIC"]["CONVERSION"] - vegetation["EXISTING"]["CONVERSION"])
    save_intarr_to_geotiff(vegetation_change, os.path.join(output_folder, "Intermediates", "Conversion_Raster.tif"), prj_existing_path)

    # load conversion types dictionary from database
    conn = sqlite3.connect(outputs_gpkg_path)
    conn.row_factory = dict_factory
    curs = conn.cursor()
    curs.execute('SELECT * FROM ConversionProportions')
    conversion_classifications = curs.fetchall()

    # Split vegetation change classes into binary arrays
    vegetation_change_arrays = {
        c["ConversionType"]: (vegetation_change == int(c["ConversionCode"])) * 1 if int(c["ConversionCode"]) in np.unique(vegetation_change) else None
        for c in conversion_classifications
    }

    # Calcuate vegetation conversion per reach
    reach_values = extract_mean_values_by_polygon(clipped_thiessen, vegetation_change_arrays, prj_existing_path)

    # Add Conversion Code, Type to Vegetation Conversion
    reach_values_with_conversion_codes = classify_conversions(reach_values, conversion_classifications)

    # Write Output to GPKG table
    # TODO clean this up a bit?
    # TODO figure out why main.empty_table is getting generated
    with sqlite3.connect(outputs_gpkg_path) as conn:
        cursor = conn.cursor()

        # cursor.execute('''INSERT INTO gpkg_contents (table_name, data_type) VALUES ('rvd_values', 'attributes')''')
        # conn.commit()
        cursor.executemany('''INSERT INTO RVDValues (
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

        cursor.executemany('''UPDATE RVDValues SET
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

    log.info('Copying values from SQLite to output ShapeFile')

    # Add intermediates and the report to the XML
    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['OUTPUTS'])

    # Add the report to the XML
    project.add_project_vector(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)

    # report(db_path, report_path)

    log.info('RVD complete')


def extract_mean_values_by_polygon(polys, rasters, reference_raster):
    log = Logger('extract_mean_values_by_polygon')

    progbar = ProgressBar(len(polys), 50, "Extracting Mean values...")
    counter = 0

    with rasterio.open(reference_raster) as dataset:

        output = {}
        for reachid, poly in polys.items():
            counter += 1
            progbar.update(counter)
            if poly.geom_type in ["Polygon", "MultiPolygon"] and poly.area > 0:
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
                # log.debug(f"Reach: {reachid} | {sum([v for v in values.values() if v is not None]):.2f}")
            else:
                progbar.erase()
                log.warning(f"Reach: {reachid} | WARNING no geom")

    progbar.finish()
    return output


def riparian_departure(mean_riparian):
    output = {}
    for reachid, values in mean_riparian.items():
        for ripariantype in ["RIPARIAN", "NATIVE_RIPARIAN"]:
            values[f"{ripariantype}_DEPARTURE"] = values[f"EXISTING_{ripariantype}_MEAN"] / values[f"HISTORIC_{ripariantype}_MEAN"] if values[f"HISTORIC_{ripariantype}_MEAN"] != 0.0 else 0.0
        output[reachid] = values

    return output


def simple_save(list_geoms, ogr_type, srs, layer_name, gpkg_path):
    with GeopackageLayer(gpkg_path, layer_name, write=True) as lyr:
        lyr.create_layer(ogr_type, spatial_ref=srs)

        progbar = ProgressBar(len(list_geoms), 50, f"Saving {gpkg_path}/{layer_name}")
        counter = 0
        progbar.update(counter)
        lyr.ogr_layer.StartTransaction()
        for geom in list_geoms:
            counter += 1
            progbar.update(counter)

            feature = ogr.Feature(lyr.ogr_layer_def)
            geom_ogr = VectorBase.shapely2ogr(geom)
            feature.SetGeometry(geom_ogr)
            # if attributes:
            #     for field, value in attributes.items():
            #         feature.SetField(field, value)
            lyr.ogr_layer.CreateFeature(feature)
            feature = None

        progbar.finish()
        lyr.ogr_layer.CommitTransaction()


def save_intarr_to_geotiff(array, out_file, reference_raster):
    with rasterio.open(reference_raster) as reference:
        with rasterio.open(out_file,
                           "w",
                           driver="GTiff",
                           height=array.shape[0],
                           width=array.shape[1],
                           count=1,
                           dtype=np.int16,
                           crs=reference.crs,
                           transform=reference.transform) as new:
            new.write(array.astype(np.int16), 1)


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
    parser.add_argument('min_length', help='Minimum length input', type=float)
    parser.add_argument('flowlines', help='flowlines input', type=str)
    parser.add_argument('existing', help='National existing vegetation raster', type=str)
    parser.add_argument('historic', help='National historic vegetation raster', type=str)
    parser.add_argument('valley_bottom', help='Valley bottom (.shp, .gpkg/layer_name)', type=str)
    parser.add_argument('output_folder', help='Output folder input', type=str)
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
