#!/usr/bin/env python3
# Name:     Confinement
#
# Purpose:  Generate confining margins and calculate confinement on a stream
#           network
#
# Author:   Kelly Whitehead
#
# Date:     27 Oct 2020
# Latest:   11 Oct 2021
# -------------------------------------------------------------------------------
import argparse
import sys
import os
import traceback
from typing import List, Dict
import sqlite3

from osgeo import ogr, osr
from osgeo import gdal
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from shapely.ops import split, nearest_points, linemerge, substring
from shapely.geometry import Point, MultiPoint, LineString, MultiLineString

from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors, ProgressBar
from rscommons import GeopackageLayer
from rscommons.vector_ops import collect_feature_class, get_geometry_unary_union, copy_feature_class
from rscommons.util import safe_makedirs, parse_metadata
from rscommons.copy_features import copy_features_fields
from rscommons.moving_window import moving_window_dgo_ids
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions
from rme.shapley_ops import line_segments, select_geoms_by_intersection, cut
from confinement.utils.calc_confinement import calculate_confinement, dgo_confinement
from confinement.utils.continuous_line import continuous_line
from confinement.utils.confinement_moving_window import igo_confinement
from confinement.confinement_report import ConfinementReport
from confinement.__version__ import __version__

Path = str

initGDALOGRErrors()
gdal.UseExceptions()

cfg = ModelConfig(
    'http://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(
    os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    # key: (name, id, tag, relpath)]
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'FLOWLINES': RSLayer('Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'CHANNEL_AREA': RSLayer('Channel_Area', 'CHANNEL_AREA', 'Vector', 'channel_area'),
        'CONFINING_POLYGON': RSLayer('Confining Polygon', 'CONFINING_POLYGON', 'Vector', 'confining_polygon'),
        'DGOS': RSLayer('DGOs', 'DGOS', 'Vector', 'dgos'),
        'IGOS': RSLayer('IGOs', 'IGOS', 'Vector', 'igos')
    }),
    'HILLSHADE': RSLayer('Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/confinement_intermediates.gpkg', {
        'SPLIT_POINTS': RSLayer('Split Points', 'SPLIT_POINTS', 'Vector', 'split_points'),
        'FLOWLINE_SEGMENTS': RSLayer('Flowline Segments', 'FLOWLINE_SEGMENTS', 'Vector', 'flowline_segments'),
        'ERROR_POLYLINES': RSLayer('Error Polylines', 'ERROR_POLYLINES', 'Vector', 'error_polylines'),
        'ERROR_POLYGONS': RSLayer('Error Polygons', 'ERROR_POLYGONS', 'Vector', 'error_polygons'),
        'CHANNEL_AREA_BUFFERED': RSLayer('Channel Area Buffered', 'CHANNEL_AREA_BUFFERED', 'Vector', 'channel_area_buffered'),
        'CONFINEMENT_BUFFER_SPLIT': RSLayer('Active Channel Split Buffers', 'CONFINEMENT_BUFFER_SPLITS', 'Vector', 'confinement_buffers_split'),
        'CONFINEMENT_ZONES': RSLayer('Zones of Confinement', 'CONFINEMENT_ZONES', 'Vector', 'confinement_zones'),
        'CONFINING_POLYGONS_UNION': RSLayer('Confinement Polygons (unioned)', 'CONFINING_POLYGONS_UNION', 'Vector', 'confining_polygons_union')
    }),
    'CONFINEMENT_RUN_REPORT': RSLayer('Confinement Report', 'CONFINEMENT_RUN_REPORT', 'HTMLFile', 'outputs/confinement.html'),
    'CONFINEMENT': RSLayer('Confinement', 'CONFINEMENT', 'Geopackage', 'outputs/confinement.gpkg', {
        'CONFINEMENT_RAW': RSLayer('Confinement Raw', 'CONFINEMENT_RAW', 'Vector', 'confinement_raw'),
        'CONFINEMENT_MARGINS': RSLayer('Confinement Margins', 'CONFINEMENT_MARGINS', 'Vector', 'confining_margins'),
        'CONFINEMENT_RATIO': RSLayer('Confinement Ratio', 'CONFINEMENT_RATIO', 'Vector', 'confinement_ratio'),
        'CONFINEMENT_BUFFERS': RSLayer('Active Channel Buffer', 'CONFINEMENT_BUFFERS', 'Vector', 'confinement_buffers'),
        'CONFINEMENT_RATIO_SEGMENTED': RSLayer('Confinement Ratio Segmented', 'CONFINEMENT_RATIO_SEGMENTED', 'Vector', 'confinement_ratio_segmented'),
        'CONFINEMENT_DGOS': RSLayer('Confinement DGOs', 'CONFINEMENT_DGOS', 'Vector', 'confinement_dgos'),
        'CONFINEMENT_IGOS': RSLayer('Confinement IGOS', 'CONFINEMENT_IGOS', 'Vector', 'confinement_igos')
    }),
}


def confinement(huc: int, flowlines_orig: Path, channel_area_orig: Path, confining_polygon_orig: Path, output_folder: Path, in_hillshade: str, vbet_summary_field: str,
                confinement_type: str, dgos: str, igos: str, buffer: float = 0.0, segmented_network=None, meta=None):
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
        raise Exception(
            'Invalid HUC identifier "{}". Must be an integer'.format(huc))

    if not (len(huc) == 4 or len(huc) == 8 or len(huc) == 10):
        raise Exception('Invalid HUC identifier. Must be four digit integer')

    # search windows for moving window analysis
    search_distance = {
        '0': 200,
        '1': 400,
        '2': 1200,
        '3': 2000,
        '4': 8000,
    }

    augment_layermeta('confinement', LYR_DESCRIPTIONS_JSON, LayerTypes)

    # Make the projectXML
    project, _realization, proj_nodes, report_path = create_project(huc, output_folder, [
        RSMeta('HUC{}'.format(len(huc)), str(huc)),
        RSMeta('HUC', str(huc)),
        RSMeta('ConfinementType', confinement_type),
        RSMeta('Small Search Window', str(
            search_distance['0']), RSMetaTypes.INT, locked=True),
        RSMeta('Medium Search Window', str(
            search_distance['1']), RSMetaTypes.INT, locked=True),
        RSMeta('Large Search Window', str(
            search_distance['2']), RSMetaTypes.INT, locked=True),
        RSMeta('Very Large Search Window', str(
            search_distance['3']), RSMetaTypes.INT, locked=True),
        RSMeta('Huge Search Window', str(
            search_distance['4']), RSMetaTypes.INT, locked=True)
    ], meta)

    # Copy input shapes to a geopackage
    flowlines_path = os.path.join(
        output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    confining_path = os.path.join(
        output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['CONFINING_POLYGON'].rel_path)
    channel_area = os.path.join(
        output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['CHANNEL_AREA'].rel_path)
    dgo_path = os.path.join(
        output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['DGOS'].rel_path)
    igo_path = os.path.join(
        output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['IGOS'].rel_path)

    copy_feature_class(flowlines_orig, flowlines_path, epsg=cfg.OUTPUT_EPSG)
    copy_feature_class(channel_area_orig, channel_area)
    copy_feature_class(confining_polygon_orig,
                       confining_path, epsg=cfg.OUTPUT_EPSG)
    copy_feature_class(dgos, dgo_path, epsg=cfg.OUTPUT_EPSG)
    copy_feature_class(igos, igo_path, epsg=cfg.OUTPUT_EPSG)

    if segmented_network:
        LayerTypes['INPUTS'].add_sub_layer("SEGMENTED_NETWORK", RSLayer(
            'Segmented Network', "SEGMENTED_NETWORK", 'Vector', 'segmented_network'))
        segmented_network_proj = os.path.join(
            output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['SEGMENTED_NETWORK'].rel_path)
        copy_feature_class(segmented_network, segmented_network_proj)

    _nd, _inputs_gpkg_path, inputs_gpkg_lyrs = project.add_project_geopackage(
        proj_nodes['Inputs'], LayerTypes['INPUTS'])

    _hs_node, hillshade = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], in_hillshade)

    output_gpkg = os.path.join(
        output_folder, LayerTypes['CONFINEMENT'].rel_path)
    intermediates_gpkg = os.path.join(
        output_folder, LayerTypes['INTERMEDIATES'].rel_path)

    # Creates an empty geopackage and replaces the old one
    GeopackageLayer(output_gpkg, delete_dataset=True)
    GeopackageLayer(intermediates_gpkg, delete_dataset=True)

    # Add the flowlines file with some metadata
    project.add_metadata(
        [RSMeta('VBET_Summary_Field', vbet_summary_field)], inputs_gpkg_lyrs['FLOWLINES'][0])

    # Add the confinement polygon
    project.add_project_geopackage(
        proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    _nd, _inputs_gpkg_path, out_gpkg_lyrs = project.add_project_geopackage(
        proj_nodes['Outputs'], LayerTypes['CONFINEMENT'])

    # Additional Metadata
    project.add_metadata([
        RSMeta('Buffer', str(buffer), RSMetaTypes.FLOAT)
    ], out_gpkg_lyrs['CONFINEMENT_BUFFERS'][0])

    # Generate confining margins
    log.info(f"Preparing output geopackage: {output_gpkg}")
    log.info(
        f"Generating Confinement from vbet summary field: {vbet_summary_field}")

    # Load input datasets and set the global srs and a meter conversion factor
    with GeopackageLayer(flowlines_path) as flw_lyr:
        empty = False
        # if there are no flowlines, skip the rest of the process
        if flw_lyr.ogr_layer.GetFeatureCount() == 0:
            log.warning("No flowlines found in input flowlines layer, creating empty outputs")
            ds = gdal.Open(hillshade)
            prj = ds.GetProjection()
            srs = osr.SpatialReference(wkt=prj)
            empty = True

        if not empty:
            srs = flw_lyr.spatial_ref
            meter_conversion = flw_lyr.rough_convert_metres_to_vector_units(1)
            offset = flw_lyr.rough_convert_metres_to_vector_units(0.1)
            selection_buffer = flw_lyr.rough_convert_metres_to_vector_units(0.1)

    # Calculate Spatial Constants
    # Get a very rough conversion factor for 1m to whatever units the shapefile uses
    # offset = 0.01 * meter_conversion
    # selection_buffer = 0.01 * meter_conversion

    # Standard Outputs
    field_lookup = {
        'side': ogr.FieldDefn("side", ogr.OFTString),
        # ArcGIS cannot read Int64 and will show up as 0, however data is stored correctly in GPKG
        'flowlineID': ogr.FieldDefn("NHDPlusID", ogr.OFTReal),
        'level_path': ogr.FieldDefn("level_path", ogr.OFTReal),
        'confinement_type': ogr.FieldDefn("confinement_type", ogr.OFTString),
        'confinement_ratio': ogr.FieldDefn("confinement_ratio", ogr.OFTReal),
        'constriction_ratio': ogr.FieldDefn("constriction_ratio", ogr.OFTReal),
        'length': ogr.FieldDefn("approx_leng", ogr.OFTReal),
        'confined_length': ogr.FieldDefn("confin_leng", ogr.OFTReal),
        'constricted_length': ogr.FieldDefn("constr_leng", ogr.OFTReal),
        # Couple of Debug fields too
        'process': ogr.FieldDefn("ErrorProcess", ogr.OFTString),
        'message': ogr.FieldDefn("ErrorMessage", ogr.OFTString)
    }

    field_lookup['side'].SetWidth(5)
    field_lookup['confinement_type'].SetWidth(5)

    # Here we open all the necessary output layers and write the fields to them. There's no harm in quickly
    # Opening these layers to instantiate them

    # Standard Outputs
    confining_margins_path = os.path.join(
        output_gpkg, LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_MARGINS"].rel_path)
    with GeopackageLayer(confining_margins_path, write=True) as margins_lyr:
        margins_lyr.create(ogr.wkbLineString, spatial_ref=srs)
        margins_lyr.ogr_layer.CreateField(field_lookup['side'])
        margins_lyr.ogr_layer.CreateField(field_lookup['level_path'])
        margins_lyr.ogr_layer.CreateField(field_lookup['length'])

    confinement_raw_path = os.path.join(
        output_gpkg, LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_RAW"].rel_path)
    with GeopackageLayer(confinement_raw_path, write=True) as raw_lyr:
        raw_lyr.create(ogr.wkbLineString, spatial_ref=srs)
        raw_lyr.ogr_layer.CreateField(field_lookup['level_path'])
        raw_lyr.ogr_layer.CreateField(field_lookup['confinement_type'])
        raw_lyr.ogr_layer.CreateField(field_lookup['length'])

    with GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_RATIO"].rel_path, write=True) as ratio_lyr:
        ratio_lyr.create(ogr.wkbLineString, spatial_ref=srs)
        ratio_lyr.ogr_layer.CreateField(field_lookup['level_path'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['confinement_ratio'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['constriction_ratio'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['length'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['confined_length'])
        ratio_lyr.ogr_layer.CreateField(field_lookup['constricted_length'])

    with GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_DGOS"].rel_path, write=True) as dgo_lyr:
        dgo_lyr.create(ogr.wkbMultiPolygon, spatial_ref=srs)
        dgo_lyr.ogr_layer.CreateField(ogr.FieldDefn('level_path', ogr.OFTReal))
        dgo_lyr.ogr_layer.CreateField(
            ogr.FieldDefn('seg_distance', ogr.OFTReal))
        dgo_lyr.ogr_layer.CreateField(
            ogr.FieldDefn('centerline_length', ogr.OFTReal))
        dgo_lyr.ogr_layer.CreateField(
            ogr.FieldDefn('segment_area', ogr.OFTReal))
        dgo_lyr.ogr_layer.CreateField(field_lookup['confinement_ratio'])
        dgo_lyr.ogr_layer.CreateField(field_lookup['constriction_ratio'])
        dgo_lyr.ogr_layer.CreateField(field_lookup['length'])
        dgo_lyr.ogr_layer.CreateField(field_lookup['confined_length'])
        dgo_lyr.ogr_layer.CreateField(field_lookup['constricted_length'])

    with GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_IGOS"].rel_path, write=True) as igo_lyr:
        igo_lyr.create(ogr.wkbMultiPoint, spatial_ref=srs)
        igo_lyr.ogr_layer.CreateField(ogr.FieldDefn('level_path', ogr.OFTReal))
        igo_lyr.ogr_layer.CreateField(
            ogr.FieldDefn('seg_distance', ogr.OFTReal))
        igo_lyr.ogr_layer.CreateField(
            ogr.FieldDefn('stream_size', ogr.OFTInteger))
        igo_lyr.ogr_layer.CreateField(field_lookup['confinement_ratio'])
        igo_lyr.ogr_layer.CreateField(field_lookup['constriction_ratio'])
        igo_lyr.ogr_layer.CreateField(field_lookup['length'])
        igo_lyr.ogr_layer.CreateField(field_lookup['confined_length'])
        igo_lyr.ogr_layer.CreateField(field_lookup['constricted_length'])

    dgo_out_path = os.path.join(
        output_gpkg, LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_DGOS"].rel_path)
    igo_out_path = os.path.join(
        output_gpkg, LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_IGOS"].rel_path)
    copy_features_fields(
        inputs_gpkg_lyrs['DGOS'][1], dgo_out_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(
        inputs_gpkg_lyrs['IGOS'][1], igo_out_path, epsg=cfg.OUTPUT_EPSG)

    if empty:
        return

    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()
        curs.execute('CREATE INDEX IF NOT EXISTS dgo_levelpath_seg_distance ON confinement_dgos (level_path, seg_distance)')
        curs.execute('CREATE INDEX IF NOT EXISTS igo_levelpath_seg_distance ON confinement_igos (level_path, seg_distance)')
        curs.execute('CREATE INDEX IF NOT EXISTS ratio_levelpath ON confinement_ratio (level_path)')
        curs.execute('CREATE INDEX IF NOT EXISTS raw_levelpath ON confinement_raw (level_path)')
        curs.execute('CREATE INDEX IF NOT EXISTS margins_levelpath ON confining_margins (level_path, side)')
        conn.commit()

    with GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["CONFINEMENT_BUFFER_SPLIT"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbPolygon, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['side'])
        lyr.ogr_layer.CreateField(field_lookup['level_path'])

    with GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_BUFFERS"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbPolygon, spatial_ref=srs)

    with GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["SPLIT_POINTS"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbPoint, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['side'])
        lyr.ogr_layer.CreateField(field_lookup['level_path'])

    with GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["FLOWLINE_SEGMENTS"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbLineString, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['side'])
        lyr.ogr_layer.CreateField(field_lookup['level_path'])

    with GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["ERROR_POLYLINES"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbLineString, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['level_path'])
        lyr.ogr_layer.CreateField(field_lookup['process'])
        lyr.ogr_layer.CreateField(field_lookup['message'])

    with GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["ERROR_POLYGONS"].rel_path, write=True) as lyr:
        lyr.create(ogr.wkbPolygon, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['level_path'])
        lyr.ogr_layer.CreateField(field_lookup['process'])
        lyr.ogr_layer.CreateField(field_lookup['message'])

    difference_path = os.path.join(
        intermediates_gpkg, LayerTypes['INTERMEDIATES'].sub_layers['CONFINEMENT_ZONES'].rel_path)
    with GeopackageLayer(difference_path, write=True) as lyr:
        lyr.create(ogr.wkbMultiPolygon, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['level_path'])
        lyr.ogr_layer.CreateField(field_lookup['side'])

    union_confining_path = os.path.join(
        intermediates_gpkg, LayerTypes['INTERMEDIATES'].sub_layers['CONFINING_POLYGONS_UNION'].rel_path)
    with GeopackageLayer(union_confining_path, write=True) as lyr:
        lyr.create(ogr.wkbPolygon, spatial_ref=srs)
        lyr.ogr_layer.CreateField(field_lookup['level_path'])

    level_paths = []
    with GeopackageLayer(confining_path) as confining_lyr:
        for confining_feat, _counter, progbar in confining_lyr.iterate_features("Generating list of level paths"):
            level_path = confining_feat.GetField('level_path')
            if level_path not in level_paths:
                level_paths.append(level_path)
    if None in level_paths:
        level_paths.remove(None)

    # Generate confinement per level_path
    with GeopackageLayer(confining_margins_path, write=True) as margins_lyr, \
            GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_RAW"].rel_path, write=True) as raw_lyr, \
            GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_RATIO"].rel_path, write=True) as ratio_lyr, \
            GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["SPLIT_POINTS"].rel_path, write=True) as dbg_splitpts_lyr, \
            GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["FLOWLINE_SEGMENTS"].rel_path, write=True) as dbg_flwseg_lyr, \
            GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["CONFINEMENT_BUFFER_SPLIT"].rel_path, write=True) as conf_buff_split_lyr, \
            GeopackageLayer(output_gpkg, layer_name=LayerTypes['CONFINEMENT'].sub_layers["CONFINEMENT_BUFFERS"].rel_path, write=True) as buff_lyr, \
            GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["ERROR_POLYLINES"].rel_path, write=True) as dbg_err_lines_lyr, \
            GeopackageLayer(intermediates_gpkg, layer_name=LayerTypes['INTERMEDIATES'].sub_layers["ERROR_POLYGONS"].rel_path, write=True) as dbg_err_polygons_lyr, \
            GeopackageLayer(difference_path, write=True) as difference_lyr, \
            GeopackageLayer(union_confining_path, write=True) as confining_polygon_lyr:

        err_count = 0
        raw_lyr.ogr_layer.StartTransaction()
        dbg_splitpts_lyr.ogr_layer.StartTransaction()
        progbar = ProgressBar(len(level_paths), 50,
                              "Calculating confinement by Level Path")
        counter = 0
        for level_path in level_paths:
            progbar.update(counter)
            counter += 1

            flowlines = collect_feature_class(
                flowlines_path, attribute_filter=f"level_path = {level_path} AND Divergence < 2")
            geom_flowlines = GeopackageLayer.ogr2shapely(flowlines)
            geom_flowlines_midpoints = MultiPoint(
                [line.interpolate(0.5, normalized=True) for line in geom_flowlines])

            geom_flowline = get_geometry_unary_union(
                flowlines_path, attribute_filter=f"level_path = {level_path}.0 AND Divergence < 2")
            if geom_flowline is None:
                log.warning(
                    "No flowlines found for level path: {}".format(level_path))
                continue

            if geom_flowline.geom_type == 'MultiLineString':
                log.warning(
                    "Attempting to merge MultiLineString flowline for level path: {}".format(level_path))
                geom_flowline = linemerge(geom_flowline)
                if geom_flowline.geom_type == 'MultiLineString':
                    log.warning(f'Gaps in flowline for level path: {level_path}, attempting to fill')
                    geom_flowline = continuous_line(geom_flowline)

            if not geom_flowline.is_valid or geom_flowline.is_empty or geom_flowline.length == 0 or geom_flowline.geom_type == 'MultiLineString':
                progbar.erase()
                log.warning(
                    "Invalid flowline with level path: {}".format(level_path))
                dbg_err_lines_lyr.create_feature(geom_flowline, {
                                                 "ErrorProcess": "Unary Union", 'level_path': level_path, "ErrorMessage": f"Invalid flowline level_path: {level_path}"})
                continue

            geom_confining_polygon = get_geometry_unary_union(
                confining_path, clip_shape=geom_flowlines)
            if geom_confining_polygon is None:
                progbar.erase()
                log.warning(
                    "Invalid confining polygon with level path: {}".format(level_path))
                continue
            confining_polygon_lyr.create_feature(
                geom_confining_polygon, {'level_path': level_path})

            geom_channel = get_geometry_unary_union(
                channel_area, clip_shape=geom_flowlines_midpoints)
            if geom_channel is None:
                log.warning(
                    "No channel polygons found with level path: {}".format(level_path))
                continue

            geom_intersected = geom_channel.intersection(
                geom_confining_polygon)
            geom_channel_buffer = geom_intersected.buffer(
                buffer * meter_conversion)

            if geom_channel_buffer.geom_type == "MultiPolygon":  # try just taking the largest polygon if its multipolygon
                areas = [g.area for g in geom_channel_buffer.geoms]
                for geom in geom_channel_buffer.geoms:
                    if geom.area == max(areas):
                        geom_channel_buffer = geom
                        break
            if geom_channel_buffer is None or geom_channel_buffer.area == 0.0 or geom_channel_buffer.geom_type == "MultiPolygon":
                progbar.erase()
                log.warning(
                    "Invalid buffer polygon with level path: {}".format(level_path))
                continue
            buff_lyr.create_feature(geom_channel_buffer)

            # Split the Buffer by the flowline
            start = nearest_points(
                Point(geom_flowline.coords[0]), geom_channel_buffer.exterior)[1]
            end = nearest_points(
                Point(geom_flowline.coords[-1]), geom_channel_buffer.exterior)[1]
            geom_flowline_extended = LineString(
                [start] + [pt for pt in geom_flowline.coords] + [end])
            geom_buffer_splits = split(
                geom_channel_buffer, geom_flowline_extended)

            # Process only if 2 buffers exist
            if len(geom_buffer_splits) != 2:
                log.warning(
                    "Buffer geom not split into exactly 2 parts with level path: {}".format(level_path))
                # Force the line extensions to a common coordinate
                geom_coords = MultiPoint(
                    [coord for coord in geom_channel_buffer.exterior.coords])
                start = nearest_points(
                    Point(geom_flowline.coords[0]), geom_coords)[1]
                end = nearest_points(
                    Point(geom_flowline.coords[-1]), geom_coords)[1]
                geom_newline = LineString(
                    [start] + [pt for pt in geom_flowline.coords] + [end])
                geom_buffer_splits = split(geom_channel_buffer, geom_newline)

                if len(geom_buffer_splits) != 2:
                    # triage the polygon if still cannot split it
                    error_message = f"WARNING: Flowline level_path {level_path} | Incorrect number of split buffer polygons: {len(geom_buffer_splits)}"
                    progbar.erase()
                    log.warning(error_message)
                    dbg_err_lines_lyr.create_feature(geom_newline, {
                                                     "ErrorProcess": "Buffer Split", 'level_path': level_path, "ErrorMessage": error_message})
                    dbg_err_lines_lyr.create_feature(geom_flowline_extended, {
                                                     "ErrorProcess": "Buffer Split", 'level_path': level_path, "ErrorMessage": error_message})
                    err_count += 1
                    if len(geom_buffer_splits) > 1:
                        for geom in geom_buffer_splits:
                            dbg_err_polygons_lyr.create_feature(
                                geom, {"ErrorProcess": "Buffer Split", 'level_path': level_path, "ErrorMessage": error_message})
                    else:
                        dbg_err_polygons_lyr.create_feature(geom_buffer_splits, {
                                                            "ErrorProcess": "Buffer Split", 'level_path': level_path, "ErrorMessage": error_message})
                        continue

            # Generate point to test side of flowline
            try:
                geom_offset = geom_flowline.parallel_offset(offset, "left")
            except Exception as e:
                progbar.erase()
                log.warning(
                    "Error offsetting flowline with level path: {}".format(level_path))
                log.warning(e)
                err_count += 1
                dbg_err_lines_lyr.create_feature(geom_flowline, {
                                                 "ErrorProcess": "Offset Error", 'level_path': level_path, "ErrorMessage": "Error offsetting flowline"})
                continue
            if not geom_offset.is_valid or geom_offset.is_empty or geom_offset.length == 0:
                progbar.erase()
                log.warning(
                    "Invalid flowline (after offset) id: {}".format(level_path))
                err_count += 1
                dbg_err_lines_lyr.create_feature(geom_flowline, {
                                                 "ErrorProcess": "Offset Error", 'level_path': level_path, "ErrorMessage": "Invalid flowline (after offset) id: {}".format(level_path)})
                continue

            geom_side_point = geom_offset.interpolate(0.5, True)

            # Store output segements
            lgeoms_right_confined_flowline_segments = []
            lgeoms_left_confined_flowline_segments = []

            for geom_side in geom_buffer_splits:

                # Identify side of flowline
                side = "LEFT" if geom_side.contains(
                    geom_side_point) else "RIGHT"

                # Save the polygon
                conf_buff_split_lyr.create_feature(
                    geom_side, {"side": side, "level_path": level_path})

                geom_difference = geom_side.difference(geom_confining_polygon)
                if not geom_difference.is_valid or geom_difference.is_empty or geom_difference.geom_type == 'GeometryCollection':
                    log.warning(
                        "No differenced polygons for level path: {}".format(level_path))
                    continue
                difference_lyr.create_feature(
                    geom_difference, {"level_path": level_path, 'side': side})

                # Generate Confining margins
                lines = []
                geom_difference = [
                    geom_difference] if geom_difference.geom_type == 'Polygon' else geom_difference
                for geom in geom_difference:
                    difference_segments = [
                        g for g in line_segments(geom.exterior)]
                    selected_lines = select_geoms_by_intersection(
                        difference_segments, [geom_side.exterior], buffer=selection_buffer)
                    line = linemerge(selected_lines)
                    line = line if line.geom_type == 'MultiLineString' else [
                        line]
                    for g in line:
                        lines.append(g)

                # Multilinestring to individual linestrings
                for line in lines:
                    if line.geom_type == 'GeometryCollection':
                        log.warning(
                            "GeometryCollection instead of polygon with level path: {}".format(level_path))
                        continue

                    margins_lyr.create_feature(
                        line, {"side": side, "level_path": level_path, "approx_leng": line.length / meter_conversion})

                    # Split flowline by Near Geometry
                    pt_start = nearest_points(
                        Point(line.coords[0]), geom_flowline)[1]
                    pt_end = nearest_points(
                        Point(line.coords[-1]), geom_flowline)[1]

                    for point in [pt_start, pt_end]:
                        dbg_splitpts_lyr.create_feature(
                            point, {"side": side, "level_path": level_path})

                    distance_sorted = sorted([geom_flowline.project(
                        pt_start), geom_flowline.project(pt_end)])
                    segment = substring(
                        geom_flowline, distance_sorted[0], distance_sorted[1])

                    # Store the segment by flowline side
                    if segment.is_valid and segment.geom_type in ["LineString", "MultiLineString"]:
                        if side == "LEFT":
                            lgeoms_left_confined_flowline_segments.append(
                                segment)
                        else:
                            lgeoms_right_confined_flowline_segments.append(
                                segment)

                        dbg_flwseg_lyr.create_feature(
                            segment, {"side": side, "level_path": level_path})

            # Raw Confinement Output
            # Prepare flowline splits
            splitpoints = [Point(x, y) for line in lgeoms_left_confined_flowline_segments +
                           lgeoms_right_confined_flowline_segments for x, y in line.coords]
            cut_distances = sorted(
                list(set([geom_flowline.project(point) for point in splitpoints])))
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
            lgeoms_confined_left_split = select_geoms_by_intersection(
                lgeoms_flowlines_split, lgeoms_left_confined_flowline_segments, buffer=selection_buffer)
            lgeoms_confined_right_split = select_geoms_by_intersection(
                lgeoms_flowlines_split, lgeoms_right_confined_flowline_segments, buffer=selection_buffer)

            lgeoms_confined_left = select_geoms_by_intersection(
                lgeoms_confined_left_split, lgeoms_confined_right_split, buffer=selection_buffer, inverse=True)
            lgeoms_confined_right = select_geoms_by_intersection(
                lgeoms_confined_right_split, lgeoms_confined_left_split, buffer=selection_buffer, inverse=True)

            geom_confined = sum(
                [geom.length for geom in lgeoms_confined_left_split + lgeoms_confined_right_split])
            # Constricted Segments
            lgeoms_constricted_l = select_geoms_by_intersection(
                lgeoms_confined_left_split, lgeoms_confined_right_split, buffer=selection_buffer)
            lgeoms_constrcited_r = select_geoms_by_intersection(
                lgeoms_confined_right_split, lgeoms_confined_left_split, buffer=selection_buffer)
            lgeoms_constricted = []
            for geom in lgeoms_constricted_l + lgeoms_constrcited_r:
                if not any(g.equals(geom) for g in lgeoms_constricted):
                    lgeoms_constricted.append(geom)
            geom_constricted = MultiLineString(lgeoms_constricted)

            # Unconfined Segments
            lgeoms_unconfined = select_geoms_by_intersection(
                lgeoms_flowlines_split, lgeoms_confined_left_split + lgeoms_confined_right_split, buffer=selection_buffer, inverse=True)

            # Save Raw Confinement
            for con_type, geoms in zip(["Left", "Right", "Both", "None"], [lgeoms_confined_left, lgeoms_confined_right, lgeoms_constricted, lgeoms_unconfined]):
                for g in geoms:
                    if g.geom_type == "LineString":
                        raw_lyr.create_feature(
                            g, {"level_path": level_path, "confinement_type": con_type, "approx_leng": g.length / meter_conversion})
                    elif geoms.geom_type in ["Point", "MultiPoint"]:
                        progbar.erase()
                        log.warning(
                            f"level path: {level_path} | Point geometry identified generating outputs for Raw Confinement.")
                    else:
                        progbar.erase()
                        log.warning(
                            f"leve path: {level_path} | Unknown geometry identified generating outputs for Raw Confinement.")

            # Calculated Confinement per Flowline
            confinement_ratio = min(geom_confined /
                                    geom_flowline.length, 1.0) if geom_confined else 0.0  # .length
            constricted_ratio = geom_constricted.length / \
                geom_flowline.length if geom_constricted else 0.0

            # Save Confinement Ratio
            attributes = {"level_path": level_path,
                          "confinement_ratio": confinement_ratio,
                          "constriction_ratio": constricted_ratio,
                          "approx_leng": geom_flowline.length / meter_conversion,
                          "confin_leng": geom_confined / meter_conversion if geom_confined else 0.0,  # .length
                          "constr_leng": geom_constricted.length / meter_conversion if geom_constricted else 0.0}

            ratio_lyr.create_feature(geom_flowline, attributes)

        raw_lyr.ogr_layer.CommitTransaction()
        dbg_splitpts_lyr.ogr_layer.CommitTransaction()

    if segmented_network is not None:
        segmented_confinement = os.path.join(
            output_gpkg, 'confinement_ratio_segmented')
        log.info("Calculating confinement on segmented network")
        calculate_confinement(confinement_raw_path,
                              segmented_network_proj, segmented_confinement)

    # summarize confinement at DGO level
    log.info('Summarizing confinement at DGO level')
    dgo_confinement(confinement_raw_path, dgo_out_path)

    # moving window analysis to attribute IGOs with confinement values
    log.info('Getting moving windows')
    windows = moving_window_dgo_ids(
        igo_out_path, dgo_out_path, level_paths, search_distance)
    log.info('Calculating confinement for IGOs using moving windows')
    igo_confinement(igo_out_path, dgo_out_path, windows)

    add_layer_descriptions(project, LYR_DESCRIPTIONS_JSON, LayerTypes)

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

    realization = project.add_realization(
        project_name, 'Confinement', cfg.version)

    proj_nodes = {
        'Inputs': project.XMLBuilder.add_sub_element(realization, 'Inputs'),
        'Intermediates': project.XMLBuilder.add_sub_element(realization, 'Intermediates'),
        'Outputs': project.XMLBuilder.add_sub_element(realization, 'Outputs')
    }

    proj_dir = os.path.dirname(project.xml_path)
    safe_makedirs(os.path.join(proj_dir, 'inputs'))
    safe_makedirs(os.path.join(proj_dir, 'intermediates'))
    safe_makedirs(os.path.join(proj_dir, 'outputs'))

    report_path = os.path.join(
        project.project_dir, LayerTypes['CONFINEMENT_RUN_REPORT'].rel_path)
    project.add_report(
        proj_nodes['Outputs'], LayerTypes['CONFINEMENT_RUN_REPORT'], replace=True)

    project.XMLBuilder.write()

    return project, realization, proj_nodes, report_path


def main():

    parser = argparse.ArgumentParser(description='Confinement Tool')

    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument(
        'flowlines', help="NHD Flowlines (.shp, .gpkg/layer_name)", type=str)
    parser.add_argument('channel_area')
    parser.add_argument(
        'confining_polygon', help='valley bottom or other polygon representing confining boundary (.shp, .gpkg/layer_name)', type=str)
    parser.add_argument('output_folder', help='Output folder', type=str)
    parser.add_argument('hillshade', help='Hillshade raster', type=str)
    parser.add_argument(
        'vbet_summary_field', help='(optional) float field in flowlines with vbet level_paths', default=None)
    parser.add_argument('confinement_type',
                        help='type of confinement', default="Unspecified")
    parser.add_argument('dgos', help='vbet dgo polygons', type=str)
    parser.add_argument('igos', help='vbet igo points', type=str)
    parser.add_argument(
        '--buffer', help='buffer to apply to channel area polygons (m)', type=float)
    parser.add_argument('--segmented_network',
                        help='segmented network to calculate confinement on (optional)', type=str)
    parser.add_argument('--calculate_existing',
                        action='store_true', default=False)
    parser.add_argument(
        '--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ',
                        action='store_true', default=False)
    parser.add_argument('--debug', help="(optional) save intermediate outputs for debugging",
                        action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    if args.calculate_existing:
        raw = os.path.join(args.output_folder, 'outputs',
                           'confinement.gpkg', 'confinement_raw')
        out = os.path.join(args.output_folder, 'outputs',
                           'confinement.gpkg', 'confinement_ratio_segmented')
        calculate_confinement(raw, args.segmented_network, out)

    else:
        # Initiate the log file
        log = Logger("Confinement")
        log.setup(logPath=os.path.join(args.output_folder,
                  "confinement.log"), verbose=args.verbose)
        log.title('Confinement For HUC: {}'.format(args.huc))

        meta = parse_metadata(args.meta)
        try:
            if args.debug is True:
                from rscommons.debug import ThreadRun
                memfile = os.path.join(
                    args.output_folder, 'confinement_mem.log')
                retcode, max_obj = ThreadRun(confinement, memfile,
                                             args.huc,
                                             args.flowlines,
                                             args.channel_area,
                                             args.confining_polygon,
                                             args.output_folder,
                                             args.hillshade,
                                             args.vbet_summary_field,
                                             args.confinement_type,
                                             args.dgos,
                                             args.igos,
                                             buffer=args.buffer,
                                             segmented_network=args.segmented_network,
                                             meta=meta)
                log.debug('Return code: {}, [Max process usage] {}'.format(
                    retcode, max_obj))

            else:
                confinement(args.huc,
                            args.flowlines,
                            args.channel_area,
                            args.confining_polygon,
                            args.output_folder,
                            args.hillshade,
                            args.vbet_summary_field,
                            args.confinement_type,
                            args.dgos,
                            args.igos,
                            buffer=args.buffer,
                            segmented_network=args.segmented_network,
                            meta=meta)

        except Exception as e:
            log.error(e)
            traceback.print_exc(file=sys.stdout)
            sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
