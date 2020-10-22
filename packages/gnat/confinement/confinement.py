#!/usr/bin/env python3
# Name:     Confinement
#
# Purpose:
#
# Author:   Kelly Whitehead
#
# Date:     24 Sep 2020
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
from shapely.ops import unary_union, split, nearest_points, linemerge, substring
from shapely.wkb import loads as wkb_load
from shapely.wkt import loads as wkt_load
from shapely.geometry import Point, Polygon, MultiPolygon, LineString, MultiLineString, mapping

from rscommons import shapefile
from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors
from rscommons.util import safe_makedirs, safe_remove_dir

from confinement.__version__ import __version__

initGDALOGRErrors()
gdal.UseExceptions()

cfg = (None, "0.0.0")  # ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/GNAT.xsd', __version__)

LayerTypes = {
    # key: (name, id, tag, relpath)
    'DEM': RSLayer('NED 10m DEM', 'DEM', 'DEM', 'topography/dem.tif'),
    'VBET_NETWORK': RSLayer('VBET Network', 'VBET_NETWORK', 'Vector', 'intermediates/vbet_network.shp'),
}


def confinement(huc, flowlines, active_channel_polygon, confining_polygon, output_folder, buffer_field=None, debug=False):
    """[summary]

    Args:
        huc ([type]): [description]

    Raises:
        Exception: [description]
        Exception: [description]
        Exception: [description]
        Exception: [description]

    Returns:
        [type]: [description]
    """

    log = Logger("Confinement")
    log.info(f'Confinement v.{0.0}')  # .format(cfg.version))

    try:
        int(huc)
    except ValueError:
        raise Exception('Invalid HUC identifier "{}". Must be an integer'.format(huc))

    if not (len(huc) == 4 or len(huc) == 8):
        raise Exception('Invalid HUC identifier. Must be four digit integer')

    # TODO Make sure input data is projected

    safe_makedirs(output_folder)
    output_gpkg = os.path.join(output_folder, "Confinement_Test.gpkg")
    if os.path.exists(output_gpkg):
        os.remove(output_gpkg)
    log.info(f"Preparing output geopackage: {output_gpkg}")
    driver_gpkg = ogr.GetDriverByName("GPKG")
    driver_gpkg.CreateDataSource(output_gpkg)

    # Generate confining margins
    if buffer_field:
        log.info("Generating Confinement from buffer field: {buffer_field}")
        # Load input datasets
        driver = ogr.GetDriverByName("ESRI Shapefile")  # will need to check source for GPKG
        data_flowlines = driver.Open(flowlines, 0)
        lyr_flowlines = data_flowlines.GetLayer()
        data_confining_polygon = driver.Open(confining_polygon, 0)
        lyr_confining_polygon = data_confining_polygon.GetLayer()
        srs = lyr_flowlines.GetSpatialRef()

        # Load confing polygon
        geom_confining_polygon = unary_union([wkt_load(feat.GetGeometryRef().ExportToWkt()) for feat in lyr_confining_polygon])

        # Standard Outputs
        out_driver = ogr.GetDriverByName("GPKG")
        data_out = out_driver.Open(output_gpkg, 1)
        field_side = ogr.FieldDefn("Side", ogr.OFTString)
        field_side.SetWidth(5)
        field_flowlineID = ogr.FieldDefn("NHDPlusID", ogr.OFTInteger64)
        field_confinement_type = ogr.FieldDefn("Confinement_Type", ogr.OFTString)
        field_confinement_type.SetWidth(5)
        field_confinement_ratio = ogr.FieldDefn("Confinement_Ratio", ogr.OFTReal)
        field_constriction_ratio = ogr.FieldDefn("Constriction_Ratio", ogr.OFTReal)

        # lyr_out_floodplain_polygons = data_out.CreateLayer('Floodplain_Polygons', srs, geom_type=ogr.wkbPolygon)
        # lyr_out_floodplain_polygons.CreateField(field_side)
        # lyr_out_floodplain_polygons.CreateField(field_flowlineID)
        # feat_def_floodplain_polygons = lyr_out_floodplain_polygons.GetLayerDefn()

        lyr_out_confining_margins = data_out.CreateLayer("Confining_Margins", srs, geom_type=ogr.wkbLineString)
        lyr_out_confining_margins.CreateField(field_side)
        lyr_out_confining_margins.CreateField(field_flowlineID)
        feature_def_confining_margins = lyr_out_confining_margins.GetLayerDefn()

        lyr_out_confinement_raw = data_out.CreateLayer("Confinement_Raw", srs, geom_type=ogr.wkbLineString)
        lyr_out_confinement_raw.CreateField(field_flowlineID)
        lyr_out_confinement_raw.CreateField(field_confinement_type)
        feature_def_confinement_raw = lyr_out_confinement_raw.GetLayerDefn()

        lyr_out_confinement_ratio = data_out.CreateLayer("Confinement_Ratio", srs, geom_type=ogr.wkbLineString)
        lyr_out_confinement_ratio.CreateField(field_flowlineID)
        lyr_out_confinement_ratio.CreateField(field_confinement_ratio)
        lyr_out_confinement_ratio.CreateField(field_constriction_ratio)
        feature_def_confinement_ratio = lyr_out_confinement_ratio.GetLayerDefn()

        # Debug Outputs
        if debug:
            lyr_debug_split_points = data_out.CreateLayer("DEBUG_Split_Points", srs, geom_type=ogr.wkbPoint)
            lyr_debug_split_points.CreateField(field_side)
            lyr_debug_split_points.CreateField(field_flowlineID)
            feature_def_split_points = lyr_debug_split_points.GetLayerDefn()

            lyr_debug_flowline_segments = data_out.CreateLayer("DEBUG_Flowline_Segments", srs, geom_type=ogr.wkbLineString)
            lyr_debug_flowline_segments.CreateField(field_side)
            lyr_debug_flowline_segments.CreateField(field_flowlineID)
            feature_def_flowline_segments = lyr_debug_flowline_segments.GetLayerDefn()

        # Generate confinement per Flowline
        for flowline in lyr_flowlines:

            # Load Flowline
            flowlineID = int(flowline.GetField("NHDPlusID"))
            buffer_value = flowline.GetField(buffer_field)
            g = flowline.GetGeometryRef()
            g.FlattenTo2D()  # to avoid error if z value is present, even if 0.0
            geom_flowline = wkb_load(g.ExportToWkb())

            # Generate buffer on each side of the flowline
            geom_buffer = geom_flowline.buffer(buffer_value, cap_style=2)
            geom_buffer_splits = split(geom_buffer, geom_flowline)  # snap(geom, geom_buffer)) <--shapely does not snap vertex to edge. need to make new function for this to ensure more buffers have 2 split polygons

            # Generate point to test side of flowline
            geom_side_point = geom_flowline.parallel_offset(0.1, "left").interpolate(0.5, True)

            # Store output segements
            geom_right_confined_flowline_segments = []
            geom_left_confined_flowline_segments = []

            # For each side of flowline, process only if 2 buffers exist
            if len(geom_buffer_splits) == 2:
                for geom_side in geom_buffer_splits:

                    # Identify side of flowline
                    side = "LEFT" if geom_side.contains(geom_side_point) else "RIGHT"

                    # Generate Confining margins
                    geom_confined_margins = geom_confining_polygon.boundary.intersection(geom_side)  # make sure intersection splits lines
                    if not geom_confined_margins.is_empty:

                        # Multilinestring to individual linestrings
                        lines = [line for line in geom_confined_margins] if geom_confined_margins.geom_type == 'MultiLineString' else [geom_confined_margins]
                        for line in lines:

                            save_geom_to_feature(lyr_out_confining_margins, feature_def_confining_margins, line, {"Side": side, "NHDPlusID": flowlineID})

                            # Split flowline by Near Geometry
                            pt_start = nearest_points(Point(line.coords[0]), geom_flowline)[1]
                            pt_end = nearest_points(Point(line.coords[-1]), geom_flowline)[1]
                            distance_sorted = sorted([geom_flowline.project(pt_start), geom_flowline.project(pt_end)])
                            segment = substring(geom_flowline, distance_sorted[0], distance_sorted[1])
                            # segment = cut_line_segment(geom_flowline, distance_sorted[0], distance_sorted[1])

                            # Store the segment by flowline side
                            if segment.geom_type in ["LineString", "MultiLineString"]:
                                if side == "LEFT":
                                    geom_left_confined_flowline_segments.append(segment)
                                else:

                                    geom_right_confined_flowline_segments.append(segment)

                            if debug:
                                save_geom_to_feature(lyr_debug_flowline_segments, feature_def_flowline_segments, segment, {"Side": side, "NHDPlusID": flowlineID})
                                for point in [pt_start, pt_end]:
                                    save_geom_to_feature(lyr_debug_split_points, feature_def_split_points, point, {"Side": side, "NHDPlusID": flowlineID})

                    # geom_floodplain_polygons = geom_confining_polygon.difference(geom_side)

                    # else:
                        # print(f"WARNING: Flowline FID {flowline.GetFID()} | No Confining Margins for {side} side.")

            else:
                log.warning(f"WARNING: Flowline FID {flowline.GetFID()} | Incorrect number of split buffer polygons: {len(geom_buffer_splits)}")
                # TODO: if debug, save the offending features!

            # Raw Confinement Output
            # Prepare flowline splits
            splitpoints = [Point(x, y) for line in geom_left_confined_flowline_segments + geom_right_confined_flowline_segments for x, y in line.coords]
            cut_distances = sorted(list(set([geom_flowline.project(point) for point in splitpoints])))
            current_line = geom_flowline
            cumulative_distance = 0.0
            split_flowlines = []
            # Change to substring here?
            while len(cut_distances) > 0:
                distance = cut_distances.pop(0) - cumulative_distance
                if not distance == 0.0:
                    outline = cut(current_line, distance)
                    if len(outline) == 1:
                        current_line = outline[0]
                    else:
                        current_line = outline[1]
                        split_flowlines.append(outline[0])
                    cumulative_distance = cumulative_distance + distance
            split_flowlines.append(current_line)
            geom_split_flowlines = MultiLineString(split_flowlines)

            geom_confined_left_all = MultiLineString(geom_left_confined_flowline_segments)
            geom_confined_right_all = MultiLineString(geom_right_confined_flowline_segments)

            geom_confined_left_split = geom_confined_left_all.intersection(geom_split_flowlines)
            geom_confined_right_split = geom_confined_right_all.intersection(geom_split_flowlines)

            geom_confined_left = geom_confined_left_split.difference(geom_confined_right_split)
            geom_confined_right = geom_confined_right_split.difference(geom_confined_left_split)

            geom_constricted = geom_confined_left_split.intersection(geom_confined_right_split)

            geom_confined = unary_union([geom_confined_right_split, geom_confined_left_split])
            geom_unconfined = geom_confined.symmetric_difference(geom_split_flowlines) if geom_confined else geom_split_flowlines

            # Save Raw Confinement
            for con_type, geoms in zip(["Left", "Right", "Both", "None"], [geom_confined_left, geom_confined_right, geom_constricted, geom_unconfined]):
                if geoms.geom_type == "LineString":
                    save_geom_to_feature(lyr_out_confinement_raw, feature_def_confinement_raw, geoms, {"NHDPlusID": flowlineID, "Confinement_Type": con_type})
                elif geoms.geom_type in ["Point", "MultiPoint"]:
                    log.warning(f"Flowline FID: {flowline.GetFID()} | Point geometry identified generating outputs for Raw Confinement.")
                else:
                    for g in geoms:
                        save_geom_to_feature(lyr_out_confinement_raw, feature_def_confinement_raw, g, {"NHDPlusID": flowlineID, "Confinement_Type": con_type})

            # Calculated Confinement per Flowline
            confinement_ratio = geom_confined.length / geom_flowline.length if geom_confined else 0.0
            constricted_ratio = geom_constricted.length / geom_flowline.length if geom_constricted else 0.0

            # Save Confinement Ratio
            save_geom_to_feature(lyr_out_confinement_ratio, feature_def_confinement_ratio, geom_flowline, {"NHDPlusID": flowlineID, "Confinement_Ratio": confinement_ratio, "Constriction_Ratio": constricted_ratio})

    else:
        log.info('Generating Confined Margins from buffered flowlines')
        dict_confining_margins = generate_confining_margins(active_channel_polygon, confining_polygon, output_gpkg)

        log.info('Converging confined flowlines')
        converge_line_attributes(flowlines, dict_confining_margins, output_gpkg)

    return


def generate_confining_margins(active_channel_polygon, confining_polygon, output_gpkg, type="Unspecified"):
    """[summary]

    Args:
        active_channel_polygon (str): featureclass of active channel
        confining_margin_polygon (str): featureclass of confinig margins
        confinement_type (str): type of confinement

    Returns:
        geometry: confining margins polylines
        geometry: floodplain pockets polygons
    """

    # Load geoms
    driver = ogr.GetDriverByName("ESRI Shapefile")  # GPKG
    data_active_channel = driver.Open(active_channel_polygon, 0)
    lyr_active_channel = data_active_channel.GetLayer()
    data_confining_polygon = driver.Open(confining_polygon, 0)
    lyr_confining_polygon = data_confining_polygon.GetLayer()
    srs = lyr_active_channel.GetSpatialRef()

    geom_active_channel_polygon = unary_union([wkb_load(feat.GetGeometryRef().ExportToWkb()) for feat in lyr_active_channel])
    geom_confining_polygon = unary_union([wkb_load(feat.GetGeometryRef().ExportToWkb()) for feat in lyr_confining_polygon])

    geom_confined_area = geom_active_channel_polygon.difference(geom_confining_polygon)
    geom_confining_margins = geom_confined_area.boundary.intersection(geom_confining_polygon.boundary)
    geom_floodplain_pockets = geom_confining_polygon.difference(geom_active_channel_polygon)

    # TODO : clean/test outputs?

    # Save Outputs to Geopackage

    out_driver = ogr.GetDriverByName("GPKG")
    data_out = out_driver.Open(output_gpkg, 1)
    lyr_out_confining_margins = data_out.CreateLayer('ConfiningMargins', srs, geom_type=ogr.wkbLineString)
    feature_def = lyr_out_confining_margins.GetLayerDefn()
    feature = ogr.Feature(feature_def)
    feature.SetGeometry(ogr.CreateGeometryFromWkb(geom_confining_margins.wkb))
    lyr_out_confining_margins.CreateFeature(feature)
    feature = None

    lyr_out_floodplain_pockets = data_out.CreateLayer('FloodplainPockets', srs, geom_type=ogr.wkbPolygon)
    feature_def = lyr_out_confining_margins.GetLayerDefn()
    feature = ogr.Feature(feature_def)
    feature.SetGeometry(ogr.CreateGeometryFromWkb(geom_floodplain_pockets.wkb))
    lyr_out_floodplain_pockets.CreateFeature(feature)
    feature = None

    data_out = None
    data_active_channel = None
    data_confining_polygon = None

    return geom_confining_margins, geom_floodplain_pockets


def converge_line_attributes(to_lines, from_lines, out_gpkg):

    return  # output_lines


def cut(line, distance):
    # Cuts a line in two at a distance from its starting point
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
    # Cuts a line in two at a distance from its starting point
    if distance_start > distance_stop:
        raise ValueError(f"Cut Line Segment: Distance Start ({distance_start}) greater than Distance stop ({distance_stop})")
    if distance_start < 0.0 or distance_stop > line.length:
        return [LineString(line)]
    if distance_start == distance_stop:
        # raise ValueError(f"Cut Line Segment: Distance Start ({distance_start}) same as Distance stop ({distance_stop})")
        distance_start = distance_start - 0.001
        distance_stop = distance_stop + 0.001

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
            return LineString(coords[:i + 1])

        if pd > distance_stop:
            cp = line.interpolate(distance_stop)
            return LineString(coords[:i] + [(cp.x, cp.y)])


def save_geom_to_feature(out_layer, feature_def, geom, attributes=None):
    feature = ogr.Feature(feature_def)
    geom_ogr = ogr.CreateGeometryFromWkb(geom.wkb)
    feature.SetGeometry(geom_ogr)
    if attributes:
        for field, value in attributes.items():
            feature.SetField(field, value)
    out_layer.CreateFeature(feature)
    feature = None


def main():

    parser = argparse.ArgumentParser(
        description='Confinement',
        # epilog="This is an epilog"
    )
    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('flowlines', help="NHD Flowlines", type=str)
    parser.add_argument('active_channel_polygon', help='bankfull buffer or other polygon representing the active channel', type=str)
    parser.add_argument('confining_polygon', help='valley bottom or other polygon representing confining boundary', type=str)
    parser.add_argument('output_folder', help='Output folder', type=str)
    parser.add_argument('buffer_field', help='(optional) float field in flowlines with buffer values', default=None)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help="(optional) save intermediate outputs for debugging", action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("Confinement")
    log.setup(logPath=os.path.join(args.output_folder, "confinement.log"), verbose=args.verbose)
    log.title('Confinement For HUC: {}'.format(args.huc))

    try:
        confinement(args.huc, args.flowlines, args.active_channel_polygon, args.confining_polygon, args.output_folder, args.buffer_field, debug=args.debug)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
