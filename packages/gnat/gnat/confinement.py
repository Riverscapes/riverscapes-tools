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
import glob
import traceback
import uuid
import datetime
import json
from osgeo import ogr
from osgeo import gdal
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union, split, nearest_points, linemerge, substring
from shapely.wkb import loads as wkb_load
from shapely.geometry import Point, Polygon, MultiPolygon, LineString, MultiLineString, mapping

from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors, ProgressBar
from rscommons import GeopackageLayer, ShapefileLayer, VectorBase, get_shp_or_gpkg
from rscommons.vector_ops import get_geometry_unary_union
from rscommons.util import safe_makedirs, safe_remove_dir, safe_remove_file
from gnat.utils.confinement_report import ConfinementReport
from gnat.__version__ import __version__

initGDALOGRErrors()
gdal.UseExceptions()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/Confinement.xsd', __version__)

LayerTypes = {
    # key: (name, id, tag, relpath)
    'FLOWLINES': RSLayer('Flowlines', 'FLOWLINES', 'Vector', 'inputs/Flowlines.shp'),
    'CONFINING_POLYGON': RSLayer('Confining Polygon', 'CONFINING_POLYGON', 'Vector', 'inputs/confining.shp'),
    'CONFINEMENT_RUN_REPORT': RSLayer('Confinement Report', 'CONFINEMENT_RUN_REPORT', 'HTMLFile', 'outputs/confinement.html'),
    'CONFINEMENT': RSLayer('Confinement', 'CONFINEMENT', 'Geopackage', 'outputs/confinement.gpkg', {
        'CONFINEMENT_RAW': RSLayer('Confinement Raw', 'CONFINEMENT_RAW', 'Vector', 'main.Confinement_Raw'),
        'CONFINEMENT_MARGINS': RSLayer('Confinement Margins', 'CONFINEMENT_MARGINS', 'Vector', 'main.Confining_Margins'),
        'CONFINEMENT_RATIO': RSLayer('Confinement Ratio', 'CONFINEMENT_RATIO', 'Vector', 'main.Confinement_Ratio')
    }),
}


def confinement(huc, flowlines_orig, confining_polygon_orig, output_folder, buffer_field, confinement_type, debug=False):
    """Generate confinement attribute for a stream network

    Args:
        huc (integer): Huc identifier
        flowlines (path): input flowlines layer
        confining_polygon (path): valley bottom or other boundary defining confining margins
        output_folder (path): location to store confinement project and output geopackage
        buffer_field (string): name of float field with buffer values in meters (i.e. 'BFWidth')
        debug (bool): run tool in debug mode (save intermediate outputs). Default = False

    """

    log = Logger("Confinement")
    log.info(f'Confinement v.{cfg.version}')  # .format(cfg.version))

    try:
        int(huc)
    except ValueError:
        raise Exception('Invalid HUC identifier "{}". Must be an integer'.format(huc))

    if not (len(huc) == 4 or len(huc) == 8):
        raise Exception('Invalid HUC identifier. Must be four digit integer')

    output_gpkg = os.path.join(output_folder, LayerTypes['CONFINEMENT'].rel_path)

    # Creates an empty geopackage and replaces the old one
    GeopackageLayer(output_gpkg, delete_dataset=True)

    # Make the projectXML
    project, _realization, proj_nodes, report_path = create_project(huc, output_folder, {
        'ConfinementType': confinement_type
    })

    # Add the flowlines file with some metadata
    flowline_node, flowlines_shp = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['FLOWLINES'], flowlines_orig)
    project.add_metadata({'BufferField': buffer_field}, flowline_node)

    # Add the confinement polygon
    # TODO: Since we don't know if the input layer is a shp or gpkg it's hard to add
    # _vbet_node, confinement_shp = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['CONFINING_POLYGON'], confining_polygon_orig)
    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['CONFINEMENT'])

    # Generate confining margins
    log.info(f"Preparing output geopackage: {output_gpkg}")
    log.info(f"Generating Confinement from buffer field: {buffer_field}")

    # Load input datasets
    with get_shp_or_gpkg(flowlines_shp) as flw_lyr, get_shp_or_gpkg(confining_polygon_orig) as confine_lyr:
        srs = flw_lyr.spatial_ref
        meter_conversion = flw_lyr.rough_convert_metres_to_shapefile_units(1)
        geom_confining_polygon = get_geometry_unary_union(confine_lyr, cfg.OUTPUT_EPSG)

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
        # Couple of Debug fields too
        'process': ogr.FieldDefn("ErrorProcess", ogr.OFTString),
        'message': ogr.FieldDefn("ErrorMessage", ogr.OFTString)

    }
    field_lookup['side'].SetWidth(5)
    field_lookup['confinement_type'].SetWidth(5)

    # Here we open all the necessary output layers and write the fields to them. There's no harm in quickly
    # Opening these layers to instantiate them

    # Standard Outputs
    with GeopackageLayer(output_gpkg, layer_name="Confining_Margins", write=True) as margins_lyr:
        margins_lyr.create(ogr.wkbLineString, spatial_ref=srs)
        margins_lyr.ogr_layer.CreateField(field_lookup['side'])
        margins_lyr.ogr_layer.CreateField(field_lookup['flowlineID'])
        margins_lyr.ogr_layer.CreateField(field_lookup['length'])

    with GeopackageLayer(output_gpkg, layer_name="Confinement_Raw", write=True) as raw_lyr:
        raw_lyr.create(ogr.wkbLineString, spatial_ref=srs)
        raw_lyr.ogr_layer.CreateField(field_lookup['flowlineID'])
        raw_lyr.ogr_layer.CreateField(field_lookup['confinement_type'])
        raw_lyr.ogr_layer.CreateField(field_lookup['length'])

    # with GeopackageLayer(output_gpkg, layer_name="Floodplain_Polygons", write=True) as floodplain_lyr:
    #     floodplain_lyr.create(ogr.wkbLineString, spatial_ref=srs)
    #     floodplain_lyr.ogr_layer.CreateField(field_side)
    #     floodplain_lyr.ogr_layer.CreateField(field_flowlineID)

    with GeopackageLayer(output_gpkg, layer_name="Confinement_Ratio", write=True) as ratio_lyr:
        ratio_lyr.create(ogr.wkbLineString, spatial_ref=srs)
        ratio_lyr.ogr_layer.CreateField(field_lookup['flowlineID'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['confinement_ratio'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['constriction_ratio'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['length'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['confined_length'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['constricted_length'])

    # Debug Outputs
    if debug:
        with GeopackageLayer(output_gpkg, layer_name="DEBUG_Split_Points", write=True) as lyr:
            lyr.create(ogr.wkbPoint, spatial_ref=srs)
            lyr.ogr_layer.CreateField(field_lookup['side'])
            lyr.ogr_layer.CreateField(field_lookup['flowlineID'])

        with GeopackageLayer(output_gpkg, layer_name="DEBUG_Flowline_Segments", write=True) as lyr:
            lyr.create(ogr.wkbLineString, spatial_ref=srs)
            lyr.ogr_layer.CreateField(field_lookup['side'])
            lyr.ogr_layer.CreateField(field_lookup['flowlineID'])

        with GeopackageLayer(output_gpkg, layer_name="DEBUG_buffers", write=True) as lyr:
            lyr.create(ogr.wkbPolygon, spatial_ref=srs)
            lyr.ogr_layer.CreateField(field_lookup['side'])
            lyr.ogr_layer.CreateField(field_lookup['flowlineID'])

        with GeopackageLayer(output_gpkg, layer_name="DEBUG_Error_Polylines", write=True) as lyr:
            lyr.create(ogr.wkbLineString, spatial_ref=srs)
            lyr.ogr_layer.CreateField(field_lookup['process'])
            lyr.ogr_layer.CreateField(field_lookup['message'])

        with GeopackageLayer(output_gpkg, layer_name="DEBUG_Error_Polygons", write=True) as lyr:
            lyr.create(ogr.wkbPolygon, spatial_ref=srs)
            lyr.ogr_layer.CreateField(field_lookup['process'])
            lyr.ogr_layer.CreateField(field_lookup['message'])

    # Generate confinement per Flowline
    with get_shp_or_gpkg(flowlines_shp) as flw_lyr, \
            GeopackageLayer(output_gpkg, layer_name="Confining_Margins", write=True) as margins_lyr, \
            GeopackageLayer(output_gpkg, layer_name="Confinement_Raw", write=True) as raw_lyr, \
            GeopackageLayer(output_gpkg, layer_name="Confinement_Ratio", write=True) as ratio_lyr, \
            GeopackageLayer(output_gpkg, layer_name="DEBUG_Split_Points", write=True) as dbg_splitpts_lyr, \
            GeopackageLayer(output_gpkg, layer_name="DEBUG_Flowline_Segments", write=True) as dbg_flwseg_lyr, \
            GeopackageLayer(output_gpkg, layer_name="DEBUG_buffers", write=True) as dbg_buff_lyr, \
            GeopackageLayer(output_gpkg, layer_name="DEBUG_Error_Polylines", write=True) as dbg_err_lines_lyr, \
            GeopackageLayer(output_gpkg, layer_name="DEBUG_Error_Polygons", write=True) as dbg_err_polygons_lyr:

        for flowline, _counter, progbar in flw_lyr.iterate_features("Generating confinement for flowlines", write_layers=[
                margins_lyr,
                raw_lyr,
                ratio_lyr,
                dbg_splitpts_lyr,
                dbg_flwseg_lyr,
                dbg_buff_lyr,
                dbg_err_lines_lyr,
                dbg_err_polygons_lyr
        ]):
            # Load Flowline
            flowlineID = int(flowline.GetFieldAsInteger64("NHDPlusID"))
            buffer_value = flowline.GetField(buffer_field) * meter_conversion
            g = flowline.GetGeometryRef()
            g.FlattenTo2D()  # to avoid error if z value is present, even if 0.0
            geom_flowline = wkb_load(g.ExportToWkb())

            # Generate buffer on each side of the flowline
            geom_buffer = geom_flowline.buffer(buffer_value, cap_style=2)
            geom_buffer_splits = split(geom_buffer, geom_flowline)  # snap(geom, geom_buffer)) <--shapely does not snap vertex to edge. need to make new function for this to ensure more buffers have 2 split polygons

            # Generate point to test side of flowline
            geom_side_point = geom_flowline.parallel_offset(offset, "left").interpolate(0.5, True)

            # Store output segements
            lgeoms_right_confined_flowline_segments = []
            lgeoms_left_confined_flowline_segments = []

            # For each side of flowline, process only if 2 buffers exist
            if len(geom_buffer_splits) == 2:  # TODO fix the effectiveness of split buffer so more flowlines can be processed
                for geom_side in geom_buffer_splits:

                    # Identify side of flowline
                    side = "LEFT" if geom_side.contains(geom_side_point) else "RIGHT"

                    # Generate Confining margins
                    geom_confined_margins = geom_confining_polygon.boundary.intersection(geom_side)  # make sure intersection splits lines
                    if not geom_confined_margins.is_empty:

                        # Multilinestring to individual linestrings
                        lines = [line for line in geom_confined_margins] if geom_confined_margins.geom_type == 'MultiLineString' else [geom_confined_margins]
                        for line in lines:
                            margins_lyr.create_feature(line, {"Side": side, "NHDPlusID": flowlineID, "ApproxLeng": line.length / meter_conversion})

                            # Split flowline by Near Geometry
                            pt_start = nearest_points(Point(line.coords[0]), geom_flowline)[1]
                            pt_end = nearest_points(Point(line.coords[-1]), geom_flowline)[1]
                            distance_sorted = sorted([geom_flowline.project(pt_start), geom_flowline.project(pt_end)])
                            segment = substring(geom_flowline, distance_sorted[0], distance_sorted[1])
                            # segment = cut_line_segment(geom_flowline, distance_sorted[0], distance_sorted[1]) <-- Shapely substring seems to work here

                            # Store the segment by flowline side
                            if segment.geom_type in ["LineString", "MultiLineString"]:
                                if side == "LEFT":
                                    lgeoms_left_confined_flowline_segments.append(segment)
                                else:
                                    lgeoms_right_confined_flowline_segments.append(segment)

                            if debug:
                                dbg_flwseg_lyr.create_feature(segment, {"Side": side, "NHDPlusID": flowlineID})
                                for point in [pt_start, pt_end]:
                                    dbg_splitpts_lyr.create_feature(point, {"Side": side, "NHDPlusID": flowlineID})
                    if debug:
                        dbg_buff_lyr.create_feature(geom_side, {"Side": side, "NHDPlusID": flowlineID})

                    # TODO: genertate floodplain polygons, here, or at the end of processing
                    # geom_floodplain_polygons = geom_confining_polygon.difference(geom_side)

            else:
                error_message = f"WARNING: Flowline FID {flowline.GetFID()} | Incorrect number of split buffer polygons: {len(geom_buffer_splits)}"
                progbar.erase()
                log.warning(error_message)
                if debug:
                    dbg_err_lines_lyr.create_feature(geom_flowline, {"ErrorProcess": "Buffer Split", "ErrorMessage": error_message})
                    if len(geom_buffer_splits) > 0:
                        dbg_err_polygons_lyr.create_feature(geom_buffer_splits, {"ErrorProcess": "Buffer Split", "ErrorMessage": error_message})

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

    # Write a report
    report = ConfinementReport(output_gpkg, report_path, project)
    report.write()

    progbar.finish()
    log.info('Confinement Finished')
    return


def create_project(huc, output_dir, realization_meta):

    project_name = 'Confinement for HUC {}'.format(huc)
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'Confinement')

    project.add_metadata({'HUC{}'.format(len(huc)): str(huc)})
    project.add_metadata({'HUC': str(huc)})

    realizations = project.XMLBuilder.add_sub_element(project.XMLBuilder.root, 'Realizations')
    realization = project.XMLBuilder.add_sub_element(realizations, 'Confinement', None, {
        'id': 'Confinement1',
        'dateCreated': datetime.datetime.now().isoformat(),
        'guid': str(uuid.uuid4()),
        'productVersion': cfg.version
    })
    project.XMLBuilder.add_sub_element(realization, 'Name', project_name)

    project.add_metadata(realization_meta)

    proj_nodes = {
        'Inputs': project.XMLBuilder.add_sub_element(realization, 'Inputs'),
        'Outputs': project.XMLBuilder.add_sub_element(realization, 'Outputs')
    }

    proj_dir = os.path.dirname(project.xml_path)
    safe_makedirs(os.path.join(proj_dir, 'inputs'))
    safe_makedirs(os.path.join(proj_dir, 'outputs'))

    report_path = os.path.join(project.project_dir, LayerTypes['CONFINEMENT_RUN_REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'], LayerTypes['CONFINEMENT_RUN_REPORT'], replace=True)

    project.XMLBuilder.write()

    return project, realization, proj_nodes, report_path


# def generate_confining_margins(active_channel_polygon, confining_polygon, output_gpkg, type="Unspecified"):
#     """Old method for confinement margins based on single active channel polygon.

#     Args:
#         active_channel_polygon (str): featureclass of active channel
#         confining_margin_polygon (str): featureclass of confinig margins
#         confinement_type (str): type of confinement

#     Returns:
#         geometry: confining margins polylines
#         geometry: floodplain pockets polygons
#     """

#     # Load geoms
#     driver = ogr.GetDriverByName("ESRI Shapefile")  # GPKG
#     data_active_channel = driver.Open(active_channel_polygon, 0)
#     lyr_active_channel = data_active_channel.GetLayer()
#     data_confining_polygon = driver.Open(confining_polygon, 0)
#     lyr_confining_polygon = data_confining_polygon.GetLayer()
#     srs = lyr_active_channel.GetSpatialRef()

#     geom_active_channel_polygon = unary_union([wkb_load(feat.GetGeometryRef().ExportToWkb()) for feat in lyr_active_channel])
#     geom_confining_polygon = unary_union([wkb_load(feat.GetGeometryRef().ExportToWkb()) for feat in lyr_confining_polygon])

#     geom_confined_area = geom_active_channel_polygon.difference(geom_confining_polygon)
#     geom_confining_margins = geom_confined_area.boundary.intersection(geom_confining_polygon.boundary)
#     geom_floodplain_pockets = geom_confining_polygon.difference(geom_active_channel_polygon)

#     # Save Outputs to Geopackage

#     out_driver = ogr.GetDriverByName("GPKG")
#     data_out = out_driver.Open(output_gpkg, 1)
#     lyr_out_confining_margins = data_out.CreateLayer('ConfiningMargins', srs, geom_type=ogr.wkbLineString)
#     feature_def = lyr_out_confining_margins.GetLayerDefn()
#     feature = ogr.Feature(feature_def)
#     feature.SetGeometry(ogr.CreateGeometryFromWkb(geom_confining_margins.wkb))
#     lyr_out_confining_margins.CreateFeature(feature)
#     feature = None

#     lyr_out_floodplain_pockets = data_out.CreateLayer('FloodplainPockets', srs, geom_type=ogr.wkbPolygon)
#     feature_def = lyr_out_confining_margins.GetLayerDefn()
#     feature = ogr.Feature(feature_def)
#     feature.SetGeometry(ogr.CreateGeometryFromWkb(geom_floodplain_pockets.wkb))
#     lyr_out_floodplain_pockets.CreateFeature(feature)
#     feature = None

#     data_out = None
#     data_active_channel = None
#     data_confining_polygon = None

#     return geom_confining_margins, geom_floodplain_pockets


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
    parser.add_argument('confining_polygon', help='valley bottom or other polygon representing confining boundary (.shp, .gpkg/layer_name)', type=str)
    parser.add_argument('output_folder', help='Output folder', type=str)
    parser.add_argument('buffer_field', help='(optional) float field in flowlines with buffer values', default=None)
    parser.add_argument('confinement_type', help='type of confinement', default="Unspecified")  # TODO add this to project xml
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help="(optional) save intermediate outputs for debugging", action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("Confinement")
    log.setup(logPath=os.path.join(args.output_folder, "confinement.log"), verbose=args.verbose)
    log.title('Confinement For HUC: {}'.format(args.huc))

    try:
        confinement(args.huc, args.flowlines, args.confining_polygon, args.output_folder, args.buffer_field, args.confinement_type, debug=args.debug)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
