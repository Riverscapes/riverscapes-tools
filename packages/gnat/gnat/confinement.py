#!/usr/bin/env python3
# Name:     Confinement
#
# Purpose:  Generate confining margins and calculate confinement on a stream
#           network
#
# Author:   Kelly Whitehead
#
# Date:     27 Oct 2020
# -------------------------------------------------------------------------------
import argparse
import sys
import os
import traceback
from typing import List, Dict

from osgeo import ogr
from osgeo import gdal
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union, split, nearest_points, linemerge, substring
from shapely.geometry import Point, Polygon, MultiPolygon, LineString, MultiLineString, mapping
from shapely.affinity import scale

from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors, ProgressBar
from rscommons import GeopackageLayer, ShapefileLayer, VectorBase, get_shp_or_gpkg
from rscommons.vector_ops import get_geometry_unary_union, copy_feature_class
from rscommons.util import safe_makedirs, safe_remove_dir, safe_remove_file, parse_metadata
from gnat.utils.confinement_report import ConfinementReport
from gnat.__version__ import __version__

Path = str

initGDALOGRErrors()
gdal.UseExceptions()

cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V1/Confinement.xsd', __version__)

LayerTypes = {
    # key: (name, id, tag, relpath)]
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'FLOWLINES': RSLayer('Flowlines', 'FLOWLINES', 'Vector', 'Flowlines'),
        'CHANNEL_AREA': RSLayer('Channel_Area', 'CHANNEL_AREA', 'Vector', 'channel_area'),
        'CONFINING_POLYGON': RSLayer('Confining Polygon', 'CONFINING_POLYGON', 'Vector', 'ConfiningPolygon'),
    }),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/confinement_intermediates.gpkg', {
        'SPLIT_POINTS': RSLayer('Split Points', 'SPLIT_POINTS', 'Vector', 'Split_Points'),
        'FLOWLINE_SEGMENTS': RSLayer('Flowline Segments', 'FLOWLINE_SEGMENTS', 'Vector', 'Flowline_Segments'),
        'ERROR_POLYLINES': RSLayer('Error Polylines', 'ERROR_POLYLINES', 'Vector', 'Error_Polylines'),
        'ERROR_POLYGONS': RSLayer('Error Polygons', 'ERROR_POLYGONS', 'Vector', 'Error_Polygons'),
        'CONFINEMENT_BUFFER_SPLIT': RSLayer('Active Channel Split Buffers', 'CONFINEMENT_BUFFER_SPLITS', 'Vector', 'Confinement_Buffers_Split')
    }),
    'CONFINEMENT_RUN_REPORT': RSLayer('Confinement Report', 'CONFINEMENT_RUN_REPORT', 'HTMLFile', 'outputs/confinement.html'),
    'CONFINEMENT': RSLayer('Confinement', 'CONFINEMENT', 'Geopackage', 'outputs/confinement.gpkg', {
        'CONFINEMENT_RAW': RSLayer('Confinement Raw', 'CONFINEMENT_RAW', 'Vector', 'Confinement_Raw'),
        'CONFINEMENT_MARGINS': RSLayer('Confinement Margins', 'CONFINEMENT_MARGINS', 'Vector', 'Confining_Margins'),
        'CONFINEMENT_RATIO': RSLayer('Confinement Ratio', 'CONFINEMENT_RATIO', 'Vector', 'Confinement_Ratio'),
        'CONFINEMENT_BUFFERS': RSLayer('Active Channel Buffer', 'CONFINEMENT_BUFFERS', 'Vector', 'Confinement_Buffers')
    }),
}


def confinement(huc: int, flowlines_orig: Path, channel_area_orig: Path, confining_polygon_orig: Path, output_folder: Path, buffer_field: str, confinement_type: str, reach_codes: List[str], min_buffer: float = 0.0, bankfull_expansion_factor: float = 1.0, debug: bool = False, meta=None):
    """Generate confinement attribute for a stream network

    Args:
        huc (integer): Huc identifier
        flowlines (path): input flowlines layer
        confining_polygon (path): valley bottom or other boundary defining confining margins
        output_folder (path): location to store confinement project and output geopackage
        buffer_field (string): name of float field with buffer values in meters (i.e. 'BFWidth')
        confinement_type (string): name of type of confinement generated
        reach_codes (List[int]): NHD reach codes for features to include in outputs
        min_buffer (float): minimum bankfull value to use in buffers e.g. raster cell resolution
        bankfull_expansion_factor (float): factor to expand bankfull on each side of bank
        debug (bool): run tool in debug mode (save intermediate outputs). Default = False
        meta (Dict[str,str]): dictionary of riverscapes metadata key: value pairs
    """

    log = Logger("Confinement")
    log.info(f'Confinement v.{cfg.version}')  # .format(cfg.version))

    try:
        int(huc)
    except ValueError:
        raise Exception('Invalid HUC identifier "{}". Must be an integer'.format(huc))

    if not (len(huc) == 4 or len(huc) == 8):
        raise Exception('Invalid HUC identifier. Must be four digit integer')

    # Make the projectXML
    project, _realization, proj_nodes, report_path = create_project(huc, output_folder, [
        RSMeta('HUC{}'.format(len(huc)), str(huc)),
        RSMeta('HUC', str(huc)),
        RSMeta('ConfinementType', confinement_type)
    ], meta)

    # Copy input shapes to a geopackage
    flowlines_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    confining_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['CONFINING_POLYGON'].rel_path)
    channel_area = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['CHANNEL_AREA'].rel_path)

    copy_feature_class(flowlines_orig, flowlines_path, epsg=cfg.OUTPUT_EPSG)
    copy_feature_class(channel_area_orig, channel_area)
    copy_feature_class(confining_polygon_orig, confining_path, epsg=cfg.OUTPUT_EPSG)

    _nd, _inputs_gpkg_path, inputs_gpkg_lyrs = project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])

    output_gpkg = os.path.join(output_folder, LayerTypes['CONFINEMENT'].rel_path)
    intermediates_gpkg = os.path.join(output_folder, LayerTypes['INTERMEDIATES'].rel_path)

    # Creates an empty geopackage and replaces the old one
    GeopackageLayer(output_gpkg, delete_dataset=True)
    GeopackageLayer(intermediates_gpkg, delete_dataset=True)

    # Add the flowlines file with some metadata
    project.add_metadata([RSMeta('BufferField', buffer_field)], inputs_gpkg_lyrs['FLOWLINES'][0])

    # Add the confinement polygon
    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    _nd, _inputs_gpkg_path, out_gpkg_lyrs = project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['CONFINEMENT'])

    # Additional Metadata
    project.add_metadata([
        RSMeta('Min Buffer', str(min_buffer), RSMetaTypes.FLOAT),
        RSMeta("Expansion Factor", str(bankfull_expansion_factor), RSMetaTypes.FLOAT)
    ], out_gpkg_lyrs['CONFINEMENT_BUFFERS'][0])

    # Generate confining margins
    log.info(f"Preparing output geopackage: {output_gpkg}")
    log.info(f"Generating Confinement from buffer field: {buffer_field}")

    # Load input datasets and set the global srs and a meter conversion factor
    with GeopackageLayer(flowlines_path) as flw_lyr:
        srs = flw_lyr.spatial_ref
        meter_conversion = flw_lyr.rough_convert_metres_to_vector_units(1)

    #geom_confining_polygon = get_geometry_unary_union(confining_path, cfg.OUTPUT_EPSG)

    # Calculate Spatial Constants
    # Get a very rough conversion factor for 1m to whatever units the shapefile uses
    offset = 0.1 * meter_conversion
    selection_buffer = 0.1 * meter_conversion

    # Standard Outputs
    field_lookup = {
        'side': ogr.FieldDefn("Side", ogr.OFTString),
        'flowlineID': ogr.FieldDefn("NHDPlusID", ogr.OFTString),  # ArcGIS cannot read Int64 and will show up as 0, however data is stored correctly in GPKG
        'confinement_type': ogr.FieldDefn("Confinement_Type", ogr.OFTString),
        'confinement_ratio': ogr.FieldDefn("Confinement_Ratio", ogr.OFTReal),
        'constriction_ratio': ogr.FieldDefn("Constriction_Ratio", ogr.OFTReal),
        'length': ogr.FieldDefn("ApproxLeng", ogr.OFTReal),
        'confined_length': ogr.FieldDefn("ConfinLeng", ogr.OFTReal),
        'constricted_length': ogr.FieldDefn("ConstrLeng", ogr.OFTReal),
        'bankfull_width': ogr.FieldDefn("Bankfull_Width", ogr.OFTReal),
        'buffer_width': ogr.FieldDefn("Buffer_Width", ogr.OFTReal),
        # Couple of Debug fields too
        'process': ogr.FieldDefn("ErrorProcess", ogr.OFTString),
        'message': ogr.FieldDefn("ErrorMessage", ogr.OFTString)
    }

    field_lookup['side'].SetWidth(5)
    field_lookup['confinement_type'].SetWidth(5)

    # Here we open all the necessary output layers and write the fields to them. There's no harm in quickly
    # Opening these layers to instantiate them

    # Standard Outputs
    with GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_MARGINS"].rel_path, write=True) as margins_lyr:
        margins_lyr.create(ogr.wkbLineString, spatial_ref=srs)
        margins_lyr.ogr_layer.CreateField(field_lookup['side'])
        margins_lyr.ogr_layer.CreateField(field_lookup['flowlineID'])
        margins_lyr.ogr_layer.CreateField(field_lookup['length'])

    with GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_RAW"].rel_path, write=True) as raw_lyr:
        raw_lyr.create(ogr.wkbLineString, spatial_ref=srs)
        raw_lyr.ogr_layer.CreateField(field_lookup['flowlineID'])
        raw_lyr.ogr_layer.CreateField(field_lookup['confinement_type'])
        raw_lyr.ogr_layer.CreateField(field_lookup['length'])

    with GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_RATIO"].rel_path, write=True) as ratio_lyr:
        ratio_lyr.create(ogr.wkbLineString, spatial_ref=srs)
        ratio_lyr.ogr_layer.CreateField(field_lookup['flowlineID'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['confinement_ratio'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['constriction_ratio'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['length'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['confined_length'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['constricted_length'])

    with GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["CONFINEMENT_BUFFER_SPLIT"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbPolygon, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['side'])
        lyr.ogr_layer.CreateField(field_lookup['flowlineID'])
        lyr.ogr_layer.CreateField(field_lookup['bankfull_width'])
        lyr.ogr_layer.CreateField(field_lookup['buffer_width'])

    with GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_BUFFERS"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbPolygon, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['flowlineID'])
        lyr.ogr_layer.CreateField(field_lookup['bankfull_width'])
        lyr.ogr_layer.CreateField(field_lookup['buffer_width'])

    with GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["SPLIT_POINTS"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbPoint, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['side'])
        lyr.ogr_layer.CreateField(field_lookup['flowlineID'])

    with GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["FLOWLINE_SEGMENTS"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbLineString, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['side'])
        lyr.ogr_layer.CreateField(field_lookup['flowlineID'])

    with GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["ERROR_POLYLINES"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbLineString, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['process'])
        lyr.ogr_layer.CreateField(field_lookup['message'])

    with GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["ERROR_POLYGONS"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbPolygon, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['process'])
        lyr.ogr_layer.CreateField(field_lookup['message'])

    # Generate confinement per Flowline
    with GeopackageLayer(flowlines_path) as flw_lyr, \
            GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_MARGINS"].rel_path, write=True) as margins_lyr, \
            GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_RAW"].rel_path, write=True) as raw_lyr, \
            GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_RATIO"].rel_path, write=True) as ratio_lyr, \
            GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["SPLIT_POINTS"].rel_path, write=True) as dbg_splitpts_lyr, \
            GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["FLOWLINE_SEGMENTS"].rel_path, write=True) as dbg_flwseg_lyr, \
            GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["CONFINEMENT_BUFFER_SPLIT"].rel_path, write=True) as conf_buff_split_lyr, \
            GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_BUFFERS"].rel_path, write=True) as buff_lyr, \
            GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["ERROR_POLYLINES"].rel_path, write=True) as dbg_err_lines_lyr, \
            GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["ERROR_POLYGONS"].rel_path, write=True) as dbg_err_polygons_lyr:

        err_count = 0

        ratio_lyr.ogr_layer.StartTransaction()
        buff_lyr.ogr_layer.StartTransaction()
        for flowline, _counter, progbar in flw_lyr.iterate_features("Generating confinement for flowlines",
                                                                    attribute_filter="FCode IN ({0})".format(','.join([key for key in reach_codes])),
                                                                    write_layers=[
                                                                        margins_lyr,
                                                                        raw_lyr,
                                                                        ratio_lyr,
                                                                        dbg_splitpts_lyr,
                                                                        dbg_flwseg_lyr,
                                                                        buff_lyr,
                                                                        conf_buff_split_lyr,
                                                                        dbg_err_lines_lyr,
                                                                        dbg_err_polygons_lyr
                                                                    ]):
            # Load Flowline
            flowlineID = int(flowline.GetFieldAsInteger64("NHDPlusID"))

            #bankfull_width = flowline.GetField(buffer_field)
            #buffer_value = max(bankfull_width, min_buffer)

            geom_flowline = GeopackageLayer.ogr2shapely(flowline)
            if not geom_flowline.is_valid or geom_flowline.is_empty or geom_flowline.length == 0:
                progbar.erase()
                log.warning("Invalid flowline with id: {}".format(flowlineID))
                continue

            # Generate buffer on each side of the flowline
            #geom_buffer = geom_flowline.buffer(((buffer_value * meter_conversion) / 2) * bankfull_expansion_factor, cap_style=2)

            # inital cleanup if geom is multipolygon
            if geom_buffer.geom_type == "MultiPolygon":
                log.warning(f"Cleaning multipolygon for id{flowlineID}")
                polys = [g for g in geom_buffer if g.intersects(geom_flowline)]
                if len(polys) == 1:
                    geom_buffer = polys[0]

            if not geom_buffer.is_valid or geom_buffer.is_empty or geom_buffer.area == 0 or geom_buffer.geom_type not in ["Polygon"]:
                progbar.erase()
                log.warning("Invalid flowline (after buffering) id: {}".format(flowlineID))
                dbg_err_lines_lyr.create_feature(geom_flowline, {"ErrorProcess": "Generate Buffer", "ErrorMessage": "Invalid Buffer"})
                err_count += 1
                continue

            buff_lyr.create_feature(geom_buffer, {"NHDPlusID": flowlineID, "Buffer_Width": buffer_value, "Bankfull_Width": bankfull_width})

            # Split the Buffer by the flowline
            geom_buffer_splits = split(geom_buffer, geom_flowline)  # snap(geom, geom_buffer)) <--shapely does not snap vertex to edge. need to make new function for this to ensure more buffers have 2 split polygons
            # Process only if 2 buffers exist
            if len(geom_buffer_splits) != 2:

                # Lets try to split this again by slightly extending the line
                geom_newline = scale(geom_flowline, 1.1, 1.1, origin='center')
                geom_buffer_splits = split(geom_buffer, geom_newline)

                if len(geom_buffer_splits) != 2:
                    # triage the polygon if still cannot split it
                    error_message = f"WARNING: Flowline FID {flowline.GetFID()} | Incorrect number of split buffer polygons: {len(geom_buffer_splits)}"
                    progbar.erase()
                    log.warning(error_message)
                    dbg_err_lines_lyr.create_feature(geom_flowline, {"ErrorProcess": "Buffer Split", "ErrorMessage": error_message})
                    err_count += 1
                    if len(geom_buffer_splits) > 1:
                        for geom in geom_buffer_splits:
                            dbg_err_polygons_lyr.create_feature(geom, {"ErrorProcess": "Buffer Split", "ErrorMessage": error_message})
                    else:
                        dbg_err_polygons_lyr.create_feature(geom_buffer_splits, {"ErrorProcess": "Buffer Split", "ErrorMessage": error_message})
                    continue

            # Generate point to test side of flowline
            geom_offset = geom_flowline.parallel_offset(offset, "left")
            if not geom_offset.is_valid or geom_offset.is_empty or geom_offset.length == 0:
                progbar.erase()
                log.warning("Invalid flowline (after offset) id: {}".format(flowlineID))
                err_count += 1
                dbg_err_lines_lyr.create_feature(geom_flowline, {"ErrorProcess": "Offset Error", "ErrorMessage": "Invalid flowline (after offset) id: {}".format(flowlineID)})
                continue

            geom_side_point = geom_offset.interpolate(0.5, True)

            # Store output segements
            lgeoms_right_confined_flowline_segments = []
            lgeoms_left_confined_flowline_segments = []

            for geom_side in geom_buffer_splits:

                # Identify side of flowline
                side = "LEFT" if geom_side.contains(geom_side_point) else "RIGHT"

                # Save the polygon
                conf_buff_split_lyr.create_feature(geom_side, {"Side": side, "NHDPlusID": flowlineID, "Buffer_Width": buffer_value, "Bankfull_Width": bankfull_width})

                # Generate Confining margins
                geom_confined_margins = geom_confining_polygon.boundary.intersection(geom_side)  # make sure intersection splits lines
                if geom_confined_margins.is_empty:
                    continue

                # Multilinestring to individual linestrings
                lines = [line for line in geom_confined_margins] if geom_confined_margins.geom_type == 'MultiLineString' else [geom_confined_margins]
                for line in lines:
                    margins_lyr.create_feature(line, {"Side": side, "NHDPlusID": flowlineID, "ApproxLeng": line.length / meter_conversion})

                    # Split flowline by Near Geometry
                    pt_start = nearest_points(Point(line.coords[0]), geom_flowline)[1]
                    pt_end = nearest_points(Point(line.coords[-1]), geom_flowline)[1]

                    for point in [pt_start, pt_end]:
                        dbg_splitpts_lyr.create_feature(point, {"Side": side, "NHDPlusID": flowlineID})

                    distance_sorted = sorted([geom_flowline.project(pt_start), geom_flowline.project(pt_end)])
                    segment = substring(geom_flowline, distance_sorted[0], distance_sorted[1])

                    # Store the segment by flowline side
                    if segment.is_valid and segment.geom_type in ["LineString", "MultiLineString"]:
                        if side == "LEFT":
                            lgeoms_left_confined_flowline_segments.append(segment)
                        else:
                            lgeoms_right_confined_flowline_segments.append(segment)

                        dbg_flwseg_lyr.create_feature(segment, {"Side": side, "NHDPlusID": flowlineID})

            # Raw Confinement Output
            # Prepare flowline splits
            splitpoints = [Point(x, y) for line in lgeoms_left_confined_flowline_segments + lgeoms_right_confined_flowline_segments for x, y in line.coords]
            cut_distances = sorted(list(set([geom_flowline.project(point) for point in splitpoints])))
            lgeoms_flowlines_split = []
            current_line = geom_flowline
            cumulative_distance = 0.0
            while len(cut_distances) > 0:
                distance = cut_distances.pop(0) - cumulative_distance
                if not distance == 0.0:
                    outline = cut(current_line, distance)
                    if len(outline) == 1:
                        current_line = outline[0]
                    else:
                        current_line = outline[1]
                        lgeoms_flowlines_split.append(outline[0])
                    cumulative_distance = cumulative_distance + distance
            lgeoms_flowlines_split.append(current_line)

            # Confined Segments
            lgeoms_confined_left_split = select_geoms_by_intersection(lgeoms_flowlines_split, lgeoms_left_confined_flowline_segments, buffer=selection_buffer)
            lgeoms_confined_right_split = select_geoms_by_intersection(lgeoms_flowlines_split, lgeoms_right_confined_flowline_segments, buffer=selection_buffer)

            lgeoms_confined_left = select_geoms_by_intersection(lgeoms_confined_left_split, lgeoms_confined_right_split, buffer=selection_buffer, inverse=True)
            lgeoms_confined_right = select_geoms_by_intersection(lgeoms_confined_right_split, lgeoms_confined_left_split, buffer=selection_buffer, inverse=True)

            geom_confined = unary_union(lgeoms_confined_left_split + lgeoms_confined_right_split)

            # Constricted Segments
            lgeoms_constricted_l = select_geoms_by_intersection(lgeoms_confined_left_split, lgeoms_confined_right_split, buffer=selection_buffer)
            lgeoms_constrcited_r = select_geoms_by_intersection(lgeoms_confined_right_split, lgeoms_confined_left_split, buffer=selection_buffer)
            lgeoms_constricted = []
            for geom in lgeoms_constricted_l + lgeoms_constrcited_r:
                if not any(g.equals(geom) for g in lgeoms_constricted):
                    lgeoms_constricted.append(geom)
            geom_constricted = MultiLineString(lgeoms_constricted)

            # Unconfined Segments
            lgeoms_unconfined = select_geoms_by_intersection(lgeoms_flowlines_split, lgeoms_confined_left_split + lgeoms_confined_right_split, buffer=selection_buffer, inverse=True)

            # Save Raw Confinement
            for con_type, geoms in zip(["Left", "Right", "Both", "None"], [lgeoms_confined_left, lgeoms_confined_right, lgeoms_constricted, lgeoms_unconfined]):
                for g in geoms:
                    if g.geom_type == "LineString":
                        raw_lyr.create_feature(g, {"NHDPlusID": flowlineID, "Confinement_Type": con_type, "ApproxLeng": g.length / meter_conversion})
                    elif geoms.geom_type in ["Point", "MultiPoint"]:
                        progbar.erase()
                        log.warning(f"Flowline FID: {flowline.GetFID()} | Point geometry identified generating outputs for Raw Confinement.")
                    else:
                        progbar.erase()
                        log.warning(f"Flowline FID: {flowline.GetFID()} | Unknown geometry identified generating outputs for Raw Confinement.")

            # Calculated Confinement per Flowline
            confinement_ratio = geom_confined.length / geom_flowline.length if geom_confined else 0.0
            constricted_ratio = geom_constricted.length / geom_flowline.length if geom_constricted else 0.0

            # Save Confinement Ratio
            attributes = {"NHDPlusID": flowlineID,
                          "Confinement_Ratio": confinement_ratio,
                          "Constriction_Ratio": constricted_ratio,
                          "ApproxLeng": geom_flowline.length / meter_conversion,
                          "ConfinLeng": geom_confined.length / meter_conversion if geom_confined else 0.0,
                          "ConstrLeng": geom_constricted.length / meter_conversion if geom_constricted else 0.0}

            ratio_lyr.create_feature(geom_flowline, attributes)
        ratio_lyr.ogr_layer.CommitTransaction()
        buff_lyr.ogr_layer.CommitTransaction()
    # Write a report

    report = ConfinementReport(output_gpkg, report_path, project)
    report.write()

    progbar.finish()
    log.info(f"Count of Flowline segments with errors: {err_count}")
    log.info('Confinement Finished')
    return


def create_project(huc, output_dir: str, meta: List[RSMeta], meta_dict: Dict[str, str]):

    project_name = 'Confinement for HUC {}'.format(huc)
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'Confinement', meta, meta_dict)

    realization = project.add_realization(project_name, 'Confinement', cfg.version)

    proj_nodes = {
        'Inputs': project.XMLBuilder.add_sub_element(realization, 'Inputs'),
        'Intermediates': project.XMLBuilder.add_sub_element(realization, 'Intermediates'),
        'Outputs': project.XMLBuilder.add_sub_element(realization, 'Outputs')
    }

    proj_dir = os.path.dirname(project.xml_path)
    safe_makedirs(os.path.join(proj_dir, 'inputs'))
    safe_makedirs(os.path.join(proj_dir, 'intermediates'))
    safe_makedirs(os.path.join(proj_dir, 'outputs'))

    report_path = os.path.join(project.project_dir, LayerTypes['CONFINEMENT_RUN_REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'], LayerTypes['CONFINEMENT_RUN_REPORT'], replace=True)

    project.XMLBuilder.write()

    return project, realization, proj_nodes, report_path


def cut(line, distance):
    """Cuts a line in two at a distance from its starting point

    Args:
        line (LineString): line to cut
        distance (float): distance to make the cut

    Returns:
        tuple: two linestrings on either side of cut
    """
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [
                LineString(coords[:i + 1]),
                LineString(coords[i:])]
        if pd > distance:
            cp = line.interpolate(distance)
            return [
                LineString(coords[:i] + [(cp.x, cp.y)]),
                LineString([(cp.x, cp.y)] + coords[i:])]


def cut_line_segment(line, distance_start, distance_stop):
    """Cuts a line in two at a distance from its starting point

    Args:
        line (LineString): linestring to cut
        distance_start (float): starting distance of segment
        distance_stop (float): ending distance of segment

    Returns:
        LineString: linestring segment between distances
    """
    if distance_start > distance_stop:
        raise ValueError(f"Cut Line Segment: Distance Start ({distance_start}) greater than Distance stop ({distance_stop})")
    if distance_start < 0.0 or distance_stop > line.length:
        return [LineString(line)]
    if distance_start == distance_stop:
        # raise ValueError(f"Cut Line Segment: Distance Start ({distance_start}) same as Distance stop ({distance_stop})")
        distance_start = distance_start - 0.01
        distance_stop = distance_stop + 0.01

    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance_start:
            segment = LineString(coords[i:])
            break

        if pd > distance_start:
            cp = line.interpolate(distance_start)
            segment = LineString([(cp.x, cp.y)] + coords[i:])
            break

    coords = list(segment.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance_stop:
            return LineString(coords[: i + 1])

        if pd > distance_stop:
            cp = line.interpolate(distance_stop)
            return LineString(coords[: i] + [(cp.x, cp.y)])


def select_geoms_by_intersection(in_geoms, select_geoms, buffer=None, inverse=False):
    """select geometires based on intersection (or non-intersection) of midpoint

    Args:
        in_geoms (list): list of LineString geometires to select from
        select_geoms ([type]): list of LineString geometries to make the selection
        buffer ([type], optional): apply a buffer on the midpoint selectors. Defaults to None.
        inverse (bool, optional): return list of LineStrings that Do Not intersect the selecion geometries. Defaults to False.

    Returns:
        list: List of LineString geometries that meet the selection criteria
    """
    out_geoms = []

    for geom in in_geoms:
        if inverse:
            if all([geom_select.disjoint(geom.interpolate(0.5, True).buffer(buffer) if buffer else geom.interpolate(0.5, True)) for geom_select in select_geoms]):
                out_geoms.append(geom)
        else:
            if any([geom_select.intersects(geom.interpolate(0.5, True).buffer(buffer) if buffer else geom.interpolate(0.5, True)) for geom_select in select_geoms]):
                out_geoms.append(geom)

    return out_geoms


def main():

    parser = argparse.ArgumentParser(description='Confinement Tool')

    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('flowlines', help="NHD Flowlines (.shp, .gpkg/layer_name)", type=str)
    parser.add_argument('channel_polygon')
    parser.add_argument('confining_polygon', help='valley bottom or other polygon representing confining boundary (.shp, .gpkg/layer_name)', type=str)
    parser.add_argument('output_folder', help='Output folder', type=str)
    parser.add_argument('buffer_field', help='(optional) float field in flowlines with buffer values', default=None)
    parser.add_argument('confinement_type', help='type of confinement', default="Unspecified")
    parser.add_argument('--reach_codes', help='Comma delimited reach codes (FCode) to retain when filtering features. Omitting this option retains all features.', type=str)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help="(optional) save intermediate outputs for debugging", action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("Confinement")
    log.setup(logPath=os.path.join(args.output_folder, "confinement.log"), verbose=args.verbose)
    log.title('Confinement For HUC: {}'.format(args.huc))

    meta = parse_metadata(args.meta)
    reach_codes = args.reach_codes.split(',') if args.reach_codes else None
    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_folder, 'confinement_mem.log')
            retcode, max_obj = ThreadRun(confinement, memfile,
                                         args.huc,
                                         args.flowlines,
                                         args.channel_area,
                                         args.confining_polygon,
                                         args.output_folder,
                                         args.buffer_field,
                                         args.confinement_type,
                                         reach_codes,
                                         min_buffer=10.0,
                                         bankfull_expansion_factor=2.5,
                                         debug=args.debug,
                                         meta=meta)
            log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))

        else:
            confinement(args.huc,
                        args.flowlines,
                        args.channel_area,
                        args.confining_polygon,
                        args.output_folder,
                        args.buffer_field,
                        args.confinement_type,
                        reach_codes,
                        min_buffer=10.0,
                        bankfull_expansion_factor=2.5,
                        debug=args.debug,
                        meta=meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
