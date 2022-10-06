#!/usr/bin/env python3
"""
Name:     GNAT

Purpose:  Build a GNAT project by downloading and preparing
          commonly used data layers for several riverscapes tools.

Author:   Kelly Whitehead

Date:     29 Jul 2022
"""

import os
import sys
import sqlite3
import time
import argparse
import traceback
from collections import Counter
from typing import Dict, List

from osgeo import ogr
from osgeo import gdal
import numpy as np
import rasterio
from rasterio.mask import mask
from shapely.geometry import Point

from rscommons import GeopackageLayer, dotenv, Logger, initGDALOGRErrors, ModelConfig, RSLayer, RSMeta, RSMetaTypes, RSProject, VectorBase, ProgressBar
from rscommons.classes.vector_base import get_utm_zone_epsg
from rscommons.util import safe_makedirs, parse_metadata
from rscommons.database import load_lookup_data
from rscommons.geometry_ops import reduce_precision, get_endpoints
from rscommons.vector_ops import copy_feature_class, collect_linestring
from rscommons.vbet_network import copy_vaa_attributes, join_attributes

from gnat.__version__ import __version__
from gnat.gnat_window import GNATLine

Path = str

initGDALOGRErrors()
gdal.UseExceptions()

cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V1/GNAT.xsd', __version__)

LayerTypes = {
    # key: (name, id, tag, relpath)]
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'FLOWLINES': RSLayer('Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'VBET_SEGMENTS': RSLayer('Vbet Segments', 'VBET_SEGMENTS', 'Vector', 'vbet_segments'),
        'VBET_SEGMENT_POINTS': RSLayer('Vbet Segment Points', 'VBET_SEGMENT_POINTS', 'Vector', 'points'),
        'VBET_CENTERLINES': RSLayer('VBET Centerline', 'VBET_CENTERLINE', 'Vector', 'vbet_centerlines'),
        'ECO_REGIONS': RSLayer('Eco Regions', 'ECO_REGIONS', 'Vector', 'eco_regions'),
        'ROADS': RSLayer('Roads', 'Roads', 'Vector', 'roads'),
        'RAIL': RSLayer('Rail', 'Rail', 'Vector', 'rail')
    }),
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'inputs/dem.tif'),
    'PPT': RSLayer('Precipitation', 'Precip', 'Raster', 'inputs/precipitation.tif'),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/gnat_intermediates.gpkg', {
        'JUNCTION_POINTS': RSLayer('Junction Points', 'JUNCTION_POINTS', 'Vector', 'junction_points'),
        #
        # 'SPLIT_POINTS': RSLayer('Split Points', 'SPLIT_POINTS', 'Vector', 'Split_Points'),
        #     #     'FLOWLINE_SEGMENTS': RSLayer('Flowline Segments', 'FLOWLINE_SEGMENTS', 'Vector', 'Flowline_Segments'),
        #     #     'ERROR_POLYLINES': RSLayer('Error Polylines', 'ERROR_POLYLINES', 'Vector', 'Error_Polylines'),
        #     #     'ERROR_POLYGONS': RSLayer('Error Polygons', 'ERROR_POLYGONS', 'Vector', 'Error_Polygons'),
        #     #     'CHANNEL_AREA_BUFFERED': RSLayer('Channel Area Buffered', 'CHANNEL_AREA_BUFFERED', 'Vector', 'channel_area_buffered'),
        #     #     'CONFINEMENT_BUFFER_SPLIT': RSLayer('Active Channel Split Buffers', 'CONFINEMENT_BUFFER_SPLITS', 'Vector', 'Confinement_Buffers_Split'),
        #     #     'CONFINEMENT_ZONES': RSLayer('Zones of Confinement', 'CONFINEMENT_ZONES', 'Vector', 'confinement_zones'),
        #     #     'CONFINING_POLYGONS_UNION': RSLayer('Confinement Polygons (unioned)', 'CONFINING_POLYGONS_UNION', 'Vector', 'confining_polygons_union')
    }),
    'GNAT_OUTPUTS': RSLayer('GNAT', 'GNAT_OUTPUTS', 'Geopackage', 'outputs/gnat.gpkg', {
        'POINT_METRICS': RSLayer('Point Metrics', 'POINT_METRICS', 'Vector', 'vw_point_metrics')
    }),
}

stream_size_lookup = {0: 'small', 1: 'medium', 2: 'large'}
gradient_buffer_lookup = {'small': 25.0, 'medium': 50.0, 'large': 100.0}


def gnat(huc: int, in_flowlines: Path, in_vaa_table: Path, in_segments: Path, in_points: Path, in_vbet_centerline: Path, in_dem: Path, in_ppt: Path, in_roads: Path, in_rail: Path, in_ecoregions: Path, project_folder: Path, level_paths: list = None, meta: dict = None):
    """Generate GNAT project and calculate metrics

    Args:
        huc (int): NHD huc
        in_flowlines (Path): NHD flowlines
        in_vaa_table (Path): NHD vaa table
        in_segments (Path): vbet segmented polygons
        in_points (Path): vbet segmentation points
        in_vbet_centerline (Path): vbet centerlines
        in_dem (Path): input dem raster
        in_ppt (Path): input prism precpitation raster
        in_roads (Path): NTD roads line network
        in_rail (Path): NTD railroad line network
        in_ecoregions (Path): epa ecoregions polygon layer
        project_folder (Path): output folder for GNAT project
        level_paths (list, optional): level paths to process. Defaults to None.
        meta (dict, optional): key-value pairs of metadata. Defaults to None.
    """

    log = Logger('GNAT')
    log.info(f'Starting GNAT v.{cfg.version}')

    project, _realization, proj_nodes = create_project(huc, project_folder, [
        RSMeta(f'HUC{len(huc)}', str(huc)),
        RSMeta('HUC', str(huc)),
        RSMeta('GNATVersion', cfg.version),
        RSMeta('GNATTimestamp', str(int(time.time())), RSMetaTypes.TIMESTAMP),
    ], meta)

    inputs_gpkg = os.path.join(project_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg = os.path.join(project_folder, LayerTypes['INTERMEDIATES'].rel_path)
    gnat_gpkg = os.path.join(project_folder, LayerTypes['GNAT_OUTPUTS'].rel_path)
    GeopackageLayer.delete(inputs_gpkg)
    GeopackageLayer.delete(intermediates_gpkg)
    GeopackageLayer.delete(gnat_gpkg)

    flowlines = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    copy_feature_class(in_flowlines, flowlines)
    segments = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['VBET_SEGMENTS'].rel_path)
    copy_feature_class(in_segments, segments)
    points = os.path.join(gnat_gpkg, LayerTypes['INPUTS'].sub_layers['VBET_SEGMENT_POINTS'].rel_path)
    copy_feature_class(in_points, points)
    centerlines = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['VBET_CENTERLINES'].rel_path)
    copy_feature_class(in_vbet_centerline, centerlines)
    roads = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['ROADS'].rel_path)
    copy_feature_class(in_roads, roads)
    rail = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['RAIL'].rel_path)
    copy_feature_class(in_rail, rail)
    ecoregions = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['ECO_REGIONS'].rel_path)
    copy_feature_class(in_ecoregions, ecoregions)

    _dem_node, dem = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['DEM'], in_dem)
    _ppt_node, ppt = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['PPT'], in_ppt)
    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])

    vaa_table_name = copy_vaa_attributes(flowlines, in_vaa_table)
    line_network = join_attributes(inputs_gpkg, "vw_flowlines_vaa", os.path.basename(flowlines), vaa_table_name, 'NHDPlusID', ['LevelPathI', 'DnLevelPat', 'UpLevelPat', 'Divergence', 'StreamOrde', 'STARTFLAG', 'DnDrainCou', 'RtnDiv'], 4326)

    # Prepare Junctions
    junctions = os.path.join(intermediates_gpkg, LayerTypes['INTERMEDIATES'].sub_layers['JUNCTION_POINTS'].rel_path)
    with GeopackageLayer(junctions, write=True) as lyr_points, \
            GeopackageLayer(line_network) as lyr_lines:
        srs = lyr_lines.spatial_ref
        lyr_points.create_layer(ogr.wkbPoint, spatial_ref=srs, fields={'JunctionType': ogr.OFTString})
        lyr_points_defn = lyr_points.ogr_layer_def
        # Generate diffluence/confluence nodes
        for attribute, sql in [('Diffluence', '"DnDrainCou" > 1'), ('Confluence', '"RtnDiv" > 0')]:
            for feat, *_ in lyr_lines.iterate_features(attribute_filter=sql):
                geom = feat.GetGeometryRef()
                pnt = geom.GetPoint(0) if attribute == 'Confluence' else geom.GetPoint(geom.GetPointCount() - 1)
                geom_out = ogr.Geometry(ogr.wkbPoint)
                geom_out.AddPoint(*pnt)
                geom_out.FlattenTo2D()
                feat_out = ogr.Feature(lyr_points_defn)
                feat_out.SetGeometry(geom_out)
                feat_out.SetField('JunctionType', attribute)
                lyr_points.ogr_layer.CreateFeature(feat_out)
        # generate list of nodes for all potential trib junctions

        pts = []
        sql = '"DnDrainCou" <= 1 or "RtnDiv" = 0'
        for feat, *_ in lyr_lines.iterate_features(attribute_filter=sql):
            geom = feat.GetGeometryRef()
            pnt = geom.GetPoint(geom.GetPointCount() - 1)
            pts.append(pnt)

        counts = Counter(pts)
        trib_junctions = [pt for pt, count in counts.items() if count > 1]
        for pnt in trib_junctions:
            geom_out = ogr.Geometry(ogr.wkbPoint)
            geom_out.AddPoint(*pnt)
            geom_out.FlattenTo2D()
            feat_out = ogr.Feature(lyr_points_defn)
            feat_out.SetGeometry(geom_out)
            feat_out.SetField('JunctionType', 'Tributary')
            lyr_points.ogr_layer.CreateFeature(feat_out)

    database_folder = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'database')
    with sqlite3.connect(gnat_gpkg) as conn:
        cursor = conn.cursor()
        with open(os.path.join(database_folder, 'gnat_metrics.sql'), encoding='utf-8') as sqlfile:
            sql_commands = sqlfile.read()
            cursor.executescript(sql_commands)
            conn.commit()
    # Load tables
    load_lookup_data(gnat_gpkg, os.path.join(database_folder, 'data_metrics'))

    # Generate the list of level paths to run, sorted by ascending order and optional user filter
    level_paths_to_run = []
    with GeopackageLayer(line_network) as line_lyr:
        for feat, *_ in line_lyr.iterate_features():
            level_path = feat.GetField('LevelPathI')
            level_paths_to_run.append(str(int(level_path)))
    level_paths_to_run = list(set(level_paths_to_run))
    if level_paths:
        level_paths_to_run = [level_path for level_path in level_paths_to_run if level_path in level_paths]
    level_paths_to_run.sort(reverse=False)

    metrics = generate_metric_list(gnat_gpkg)
    measurements = generate_metric_list(gnat_gpkg, 'measurements')

    buffer_distance = {}
    for stream_size, distance in gradient_buffer_lookup.items():
        buffer = VectorBase.rough_convert_metres_to_raster_units(dem, distance)
        buffer_distance[stream_size] = buffer

    with GeopackageLayer(points) as lyr_points, \
            GeopackageLayer(segments) as lyr_segments,\
            sqlite3.connect(gnat_gpkg) as conn, \
            rasterio.open(dem) as src_dem:

        curs = conn.cursor()

        buffer_size_clip = lyr_points.rough_convert_metres_to_vector_units(0.25)

        for feat, *_ in lyr_points.iterate_features():
            geom = feat.GetGeometryRef()
            break
        utm_epsg = get_utm_zone_epsg(geom.GetPoint(0)[0])
        _transform_ref, transform = VectorBase.get_transform_from_epsg(lyr_points.spatial_ref, utm_epsg)
        GNATLine.transform = transform

        progbar = ProgressBar(len(level_paths_to_run), 50, "Calculating GNAT Metrics")
        counter = 0
        for level_path in level_paths_to_run:
            progbar.update(counter)
            counter += 1
            geom_flowline = collect_linestring(line_network, f'LevelPathI = {level_path}')
            if geom_flowline.IsEmpty():
                log.error(f'Flowline for level path {level_path} is empty geometry')
                continue

            geom_centerline = collect_linestring(centerlines, f'LevelPathI = {level_path}', precision=8)

            for feat_seg_pt, *_ in lyr_points.iterate_features(attribute_filter=f'LevelPathI = {level_path}'):
                # Gather common components for metric calcuations
                point_id = feat_seg_pt.GetFID()
                segment_distance = feat_seg_pt.GetField('seg_distance')
                stream_size_id = feat_seg_pt.GetField('stream_size')
                stream_size = stream_size_lookup[stream_size_id]
                window_geoms = {}  # Different metrics may require different windows. Store generated windows here for reuse.
                metrics_output = {}
                measurements_output = {}
                min_elev = None
                max_elev = None

                # Calculate each metric if it is active
                if 'STRMGRAD' in metrics:
                    metric = metrics['STRMGRAD']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance, buffer_size_clip)

                    stream_length, min_elev, max_elev = get_segment_measurements(geom_flowline, src_dem, window_geoms[window], buffer_distance[stream_size], transform)
                    measurements_output[measurements['STRMMINELEV']['measurement_id']] = min_elev
                    measurements_output[measurements['STRMMAXELEV']['measurement_id']] = max_elev
                    measurements_output[measurements['STRMLENG']['measurement_id']] = stream_length

                    gradient = None if any(value is None for value in [max_elev, min_elev]) else (max_elev - min_elev) / stream_length
                    metrics_output[metric['metric_id']] = gradient

                if 'VALGRAD' in metrics:
                    metric = metrics['VALGRAD']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance, buffer_size_clip)

                    centerline_length, *_ = get_segment_measurements(geom_centerline, src_dem, window_geoms[window], buffer_distance[stream_size], transform)
                    measurements_output[measurements['VALLENG']['measurement_id']] = centerline_length

                    if any(elev is None for elev in [min_elev, max_elev]):
                        _, min_elev, max_elev = get_segment_measurements(geom_flowline, dem, window_geoms[window], buffer_distance[stream_size], transform)
                        measurements_output[measurements['STRMMINELEV']['measurement_id']] = min_elev
                        measurements_output[measurements['STRMMAXELEV']['measurement_id']] = max_elev

                    gradient = None if any(value is None for value in [max_elev, min_elev]) else (max_elev - min_elev) / centerline_length
                    metrics_output[metric['metric_id']] = gradient

                if 'STRMORDR' in metrics:
                    metric = metrics['STRMORDR']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance, buffer_size_clip)

                    results = []
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=window_geoms[window]):
                            results.append(feat.GetField('StreamOrde'))
                        lyr_lines.ogr_layer.SetSpatialFilter(None)
                    if len(results) > 0:
                        stream_order = max(results)
                    else:
                        stream_order = None
                        log.warning(f'Unable to calculate Stream Order for pt {point_id} in level path {level_path}')
                    metrics_output[metric['metric_id']] = stream_order

                if 'HEDWTR' in metrics:
                    metric = metrics['HEDWTR']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance)

                    sum_attributes = {}
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=window_geoms[window]):
                            line_geom = feat.GetGeometryRef()
                            attribute = str(feat.GetField('STARTFLAG'))
                            if attribute not in ['1', '0']:
                                continue
                            geom_section = window_geoms[window].Intersection(line_geom)
                            length = geom_section.Length()
                            sum_attributes[attribute] = sum_attributes.get(attribute, 0) + length
                        lyr_lines.ogr_layer.SetSpatialFilter(None)
                        lyr_lines = None
                    if sum(sum_attributes.values()) == 0:
                        is_headwater = None
                    else:
                        is_headwater = 1 if sum_attributes.get('1', 0) / sum(sum_attributes.values()) > 0.5 else 0
                    metrics_output[metric['metric_id']] = is_headwater

                if 'STRMTYPE' in metrics:
                    metric = metrics['STRMTYPE']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance)

                    attributes = {}
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=window_geoms[window]):
                            line_geom = feat.GetGeometryRef()
                            attribute = str(feat.GetField('FCode'))
                            geom_section = window_geoms[window].Intersection(line_geom)
                            length = geom_section.Length()
                            attributes[attribute] = attributes.get(attribute, 0) + length
                        lyr_lines.ogr_layer.SetSpatialFilter(None)
                        lyr_lines = None
                    if len(attributes) == 0:
                        majority_fcode = None
                    else:
                        majority_fcode = max(attributes, key=attributes.get)
                    metrics_output[metric['metric_id']] = majority_fcode

                if 'ACTFLDAREA' in metrics:
                    metric = metrics['ACTFLDAREA']
                    window = metric[stream_size]

                    values = sum_window_attributes(lyr_segments, window, level_path, segment_distance, ['active_floodplain_area'])
                    afp_area = values.get('active_floodplain_area', 0.0)
                    metrics_output[metric['metric_id']] = afp_area

                if 'ACTCHANAREA' in metrics:
                    metric = metrics['ACTCHANAREA']
                    window = metric[stream_size]

                    values = sum_window_attributes(lyr_segments, window, level_path, segment_distance, ['active_channel_area'])
                    ac_area = values.get('active_channel_area', 0.0)
                    metrics_output[metric['metric_id']] = ac_area

                if 'INTGWDTH' in metrics:
                    metric = metrics['INTGWDTH']
                    window = metric[stream_size]

                    values = sum_window_attributes(lyr_segments, window, level_path, segment_distance, ['centerline_length', 'segment_area'])
                    ig_width = values.get('segment_area', 0.0) / values['centerline_length'] if 'centerline_length' in values else None
                    metrics_output[metric['metric_id']] = ig_width

                if 'CHANVBRAT' in metrics:
                    metric = metrics['CHANVBRAT']
                    window = metric[stream_size]

                    values = sum_window_attributes(lyr_segments, window, level_path, segment_distance, ['active_channel_area', 'segment_area'])
                    ac_area = values.get('active_channel_area', 0.0)
                    vbet_area = values.get('segment_area', 0.0)
                    ac_ratio = ac_area / vbet_area if vbet_area > 0.0 else None
                    metrics_output[metric['metric_id']] = ac_ratio

                if 'FLDVBRAT' in metrics:
                    metric = metrics['FLDVBRAT']
                    window = metric[stream_size]

                    values = sum_window_attributes(lyr_segments, window, level_path, segment_distance, ['floodplain_area', 'segment_area'])
                    fp_area = values.get('floodplain_area', 0.0)
                    vbet_area = values.get('segment_area', 0.0)
                    fp_ratio = fp_area / vbet_area if vbet_area > 0.0 else None
                    metrics_output[metric['metric_id']] = fp_ratio

                if 'RELFLWLNGTH' in metrics:
                    metric = metrics['RELFLWLNGTH']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance)

                    geom_flowline_full = collect_linestring(line_network, f'vbet_level_path = {level_path}')
                    stream_length_total, *_ = get_segment_measurements(geom_flowline_full, src_dem, window_geoms[window], buffer_distance[stream_size], transform)
                    centerline_length, *_ = get_segment_measurements(geom_centerline, src_dem, window_geoms[window], buffer_distance[stream_size], transform)

                    relative_flow_length = stream_length_total / centerline_length if centerline_length > 0.0 else None
                    metrics_output[metric['metric_id']] = relative_flow_length

                if 'STRMSIZE' in metrics:
                    metric = metrics['STRMSIZE']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance)

                    values = sum_window_attributes(lyr_segments, window, level_path, segment_distance, ['active_channel_area', 'active_floodplain_area'])
                    stream_length, *_ = get_segment_measurements(geom_flowline, src_dem, window_geoms[window], buffer_distance[stream_size], transform)
                    ac_area = values.get('active_channel_area', 0.0)

                    stream_size_metric = ac_area / stream_length if stream_length > 0.0 else None
                    metrics_output[metric['metric_id']] = stream_size_metric

                if 'ECORGIII' in metrics:
                    metric = metrics['ECORGIII']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance)

                    attributes = {}
                    with GeopackageLayer(ecoregions) as lyr_ecoregions:
                        for feat, *_ in lyr_ecoregions.iterate_features(clip_shape=window_geoms[window]):
                            geom_ecoregion = feat.GetGeometryRef()
                            attribute = str(feat.GetField('US_L3CODE'))
                            geom_section = window_geoms[window].Intersection(geom_ecoregion)
                            area = geom_section.GetArea()
                            attributes[attribute] = attributes.get(attribute, 0) + area
                        lyr_ecoregions = None
                    if len(attributes) == 0:
                        log.warning(f'Unable to find majority ecoregion III for pt {point_id} in level path {level_path}')
                        majority_attribute = None
                    else:
                        majority_attribute = max(attributes, key=attributes.get)
                    metrics_output[metric['metric_id']] = majority_attribute

                if 'ECORGIV' in metrics:
                    metric = metrics['ECORGIV']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance)

                    attributes = {}
                    with GeopackageLayer(ecoregions) as lyr_ecoregions:
                        for feat, *_ in lyr_ecoregions.iterate_features(clip_shape=window_geoms[window]):
                            geom_ecoregion = feat.GetGeometryRef()
                            attribute = str(feat.GetField('US_L4CODE'))
                            geom_section = window_geoms[window].Intersection(geom_ecoregion)
                            area = geom_section.GetArea()
                            attributes[attribute] = attributes.get(attribute, 0) + area
                        lyr_ecoregions = None
                    if len(attributes) == 0:
                        log.warning(f'Unable to find majority ecoregion III for pt {point_id} in level path {level_path}')
                        majority_attribute = None
                    else:
                        majority_attribute = max(attributes, key=attributes.get)
                    metrics_output[metric['metric_id']] = majority_attribute

                if 'CONF' in metrics:
                    metric = metrics['CONF']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance)

                    with GeopackageLayer(junctions) as lyr_pts:
                        count = 0
                        for feat, *_ in lyr_pts.iterate_features(clip_shape=window_geoms[window], attribute_filter=""""JunctionType" = 'Confluence'"""):
                            count += 1
                        metrics_output[metric['metric_id']] = count

                if 'DIFF' in metrics:
                    metric = metrics['DIFF']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance)

                    with GeopackageLayer(junctions) as lyr_pts:
                        count = 0
                        for feat, *_ in lyr_pts.iterate_features(clip_shape=window_geoms[window], attribute_filter=""""JunctionType" = 'Diffluence'"""):
                            count += 1
                        metrics_output[metric['metric_id']] = count

                if 'TRIBS' in metrics:
                    metric = metrics['TRIBS']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance)

                    with GeopackageLayer(junctions) as lyr_pts:
                        count = 0
                        for feat, *_ in lyr_pts.iterate_features(clip_shape=window_geoms[window], attribute_filter=""""JunctionType" = 'Tributary'"""):
                            count += 1
                        metrics_output[metric['metric_id']] = count

                if 'CHANSIN' in metrics:
                    metric = metrics['CHANSIN']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance)

                    line = GNATLine(geom_flowline, window_geoms[window])
                    measurements_output[measurements['STRMSTRLENG']['measurement_id']] = line.endpoint_distance
                    metrics_output[metric['metric_id']] = line.sinuosity()

                if 'DRAINAREA' in metrics:
                    metric = metrics['DRAINAREA']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance, buffer_size_clip)

                    results = []
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=window_geoms[window]):
                            results.append(feat.GetField('TotDASqKm'))
                    if len(results) > 0:
                        drainage_area = max(results)
                    else:
                        drainage_area = None
                        log.warning(f'Unable to calculate drainage area for pt {point_id} in level path {level_path}')
                    metrics_output[metric['metric_id']] = drainage_area

                if 'VALAZMTH' in metrics:
                    metric = metrics['VALAZMTH']
                    window = metric[stream_size]
                    if window not in window_geoms:
                        window_geoms[window] = generate_window(lyr_segments, window, level_path, segment_distance)

                    cline = GNATLine(geom_centerline, window_geoms[window])
                    metrics_output[metric['metric_id']] = cline.azimuth()

                # Write to Metrics
                if len(metrics_output) > 0:
                    curs.executemany("INSERT INTO metric_values (point_id, metric_id, metric_value) VALUES (?,?,?)", [(point_id, name, value) for name, value in metrics_output.items()])
                if len(measurements_output) > 0:
                    curs.executemany("INSERT INTO measurement_values (point_id, measurement_id, measurement_value) VALUES (?,?,?)", [(point_id, name, value) for name, value in measurements_output.items()])
            conn.commit()

    epsg = 4326
    with sqlite3.connect(gnat_gpkg) as conn:
        # Generate Pivot Table
        curs = conn.cursor()
        metrics_sql = ", ".join([f"{sql_name(metric['name'])} {metric['data_type']}" for metric in metrics.values()])
        sql = f'CREATE TABLE point_metrics_pivot (fid INTEGER PRIMARY KEY, {metrics_sql});'
        curs.execute(sql)
        conn.commit()

        # Insert Values into Pivot table
        metric_names_sql = ', '.join([sql_name(metric["name"]) for metric in metrics.values()])
        metric_values_sql = ", ".join([f"{sql_round(metric['data_type'], metric['metric_id'])} {sql_name(metric['name'])}" for metric in metrics.values()])
        sql = f'INSERT INTO point_metrics_pivot (fid, {metric_names_sql}) SELECT M.point_id, {metric_values_sql} FROM metric_values M GROUP BY M.point_id;'
        curs.execute(sql)
        conn.commit()

        # Create metric view
        metric_names_sql = ", ".join([f"M.{sql_name(metric['name'])} {sql_name(metric['name'])}" for metric in metrics.values()])
        sql = f'CREATE VIEW vw_point_metrics AS SELECT G.fid fid, G.geom geom, G.LevelPathI level_path, G.seg_distance seg_distance, G.stream_size stream_size, {metric_names_sql} FROM points G INNER JOIN point_metrics_pivot M ON M.fid = G.fid;'
        curs.execute(sql)

        # Add view to geopackage
        curs.execute("INSERT INTO gpkg_contents (table_name, identifier, data_type, srs_id) VALUES ('vw_point_metrics', 'vw_point_metrics', 'features', ?);", (epsg,))
        curs.execute("INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) values ('vw_point_metrics', 'geom', 'POINT', ?, 0, 0);", (epsg,))
        conn.commit()

    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['GNAT_OUTPUTS'])

    # Write a report
    # report_path = os.path.join(project.project_dir, LayerTypes['REPORT'].rel_path)
    # project.add_report(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)
    # report = GNATReport(output_gpkg, report_path, project)
    # report.write()

    progbar.finish()
    log.info('GNAT Finished')
    return


def sql_name(name: str) -> str:
    """return cleaned metric column name"""
    return name.lower().replace(' ', '_')


def sql_round(datatype: str, metric_id) -> str:
    """return round function"""
    return f"{'ROUND(' if datatype == 'REAL' else ''}SUM(M.metric_value) FILTER (WHERE M.metric_id == {metric_id}){', 4)' if datatype == 'REAL' else ''}"


def generate_metric_list(database: Path, source_table: str = 'metrics') -> dict:
    """_summary_

    Args:
        database (Path): path to gnat database
        source_table (str, optional): name of table ('metrics' or 'measurements'). Defaults to 'metrics'.

    Returns:
        dict: metric name: {metric attribute:value}
    """

    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        curs = conn.cursor()
        metric_data = curs.execute(f"""SELECT * from {source_table} WHERE is_active = 1""").fetchall()
        metrics = {metric['machine_code']: metric for metric in metric_data}
    return metrics


def generate_window(lyr: GeopackageLayer, window: float, level_path: str, segment_dist: float, buffer: float = 0) -> ogr.Geometry:
    """generate the window polygon geometry

    Args:
        lyr (GeopackageLayer): vbet segments polygon layer
        window (float): size of window
        level_path (str): level path of window
        segment_dist (float): vbet segment point of window (identifed by segment distance)
        buffer (float, optional): buffer the window polygon. Defaults to 0.

    Returns:
        ogr.Geometry: polygon of window
    """

    min_dist = segment_dist - 0.5 * window
    max_dist = segment_dist + 0.5 * window
    sql = f'LevelPathI = {level_path} AND seg_distance >= {min_dist} AND seg_distance <={max_dist}'
    geom_window_sections = ogr.Geometry(ogr.wkbMultiPolygon)
    for feat, *_ in lyr.iterate_features(attribute_filter=sql):
        geom = feat.GetGeometryRef()
        if geom.GetGeometryName() in ['MULTIPOLYGON', 'GEOMETRYCOLLECTION']:
            for i in range(0, geom.GetGeometryCount()):
                geo = geom.GetGeometryRef(i)
                if geo.GetGeometryName() == 'POLYGON':
                    geom_window_sections.AddGeometry(geo)
        else:
            geom_window_sections.AddGeometry(geom)
    geom_window = geom_window_sections.Buffer(buffer)  # ogr.ForceToPolygon(geom_window_sections)

    return geom_window


def get_segment_measurements(geom_line: ogr.Geometry, src_raster: rasterio.DatasetReader, geom_window: ogr.Geometry, buffer: float, transform) -> tuple:
    """ return length of segment and endpoint elevations of a line

    Args:
        geom_line (ogr.Geometry): unclipped line geometry
        raster (rasterio.DatasetReader): open dataset reader of elevation raster
        geom_window (ogr.Geometry): analysis window for clipping line
        buffer (float): buffer of endpoints to find min elevation
        transform(CoordinateTransform): transform used to obtain length
    Returns:
        float: stream length
        float: maximum elevation
        float: minimum elevation
    """

    geom_clipped = geom_window.Intersection(geom_line)
    if geom_clipped.GetGeometryName() == "MULTILINESTRING":
        geom_clipped = reduce_precision(geom_clipped, 6)
        geom_clipped = ogr.ForceToLineString(geom_clipped)
    endpoints = get_endpoints(geom_clipped)
    elevations = [None, None]
    if len(endpoints) == 2:
        elevations = []
        for pnt in endpoints:
            point = Point(pnt)
            polygon = point.buffer(buffer)  # BRAT uses 100m here for all stream sizes?
            raw_raster, _out_transform = mask(src_raster, [polygon], crop=True)
            mask_raster = np.ma.masked_values(raw_raster, src_raster.nodata)
            value = float(mask_raster.min())  # BRAT uses mean here
            elevations.append(value)
        elevations.sort()
    geom_clipped.Transform(transform)
    stream_length = geom_clipped.Length()

    return stream_length, elevations[0], elevations[1]


def sum_window_attributes(lyr: GeopackageLayer, window: float, level_path: str, segment_dist: float, fields: list) -> dict:
    """summerize window attributes from a list

    Args:
        lyr (GeopackageLayer): vbet segmented polygons layer
        window (float): size of window
        level_path (str): level path to summeize
        segment_dist (float): distance of segment
        fields (list): attribute fields to summerize

    Returns:
        dict: field name: attribute value
    """

    results = {}
    min_dist = segment_dist - 0.5 * window
    max_dist = segment_dist + 0.5 * window
    sql = f'LevelPathI = {level_path} AND seg_distance >= {min_dist} AND seg_distance <={max_dist}'
    for feat, *_ in lyr.iterate_features(attribute_filter=sql):
        for field in fields:
            result = feat.GetField(field)
            result = result if result is not None else 0.0
            results[field] = results.get(field, 0) + result

    return results


def create_project(huc, output_dir: str, meta: List[RSMeta], meta_dict: Dict[str, str]):
    """returns the gnat project, realization and project nodes"""

    project_name = f'GNAT for HUC {huc}'
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'GNAT', meta, meta_dict)

    realization = project.add_realization(project_name, 'GNAT', cfg.version)

    proj_nodes = {
        'Inputs': project.XMLBuilder.add_sub_element(realization, 'Inputs'),
        'Intermediates': project.XMLBuilder.add_sub_element(realization, 'Intermediates'),
        'Outputs': project.XMLBuilder.add_sub_element(realization, 'Outputs')
    }

    # Make sure we have these folders
    proj_dir = os.path.dirname(project.xml_path)
    safe_makedirs(os.path.join(proj_dir, 'inputs'))
    safe_makedirs(os.path.join(proj_dir, 'intermediates'))
    safe_makedirs(os.path.join(proj_dir, 'outputs'))

    project.XMLBuilder.write()
    return project, realization, proj_nodes


def main():
    """Run GNAT"""

    parser = argparse.ArgumentParser(description='GNAT Tool')

    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('flowlines', help="NHD Flowlines (.shp, .gpkg/layer_name)", type=str)
    parser.add_argument('vaa_table', help="NHD Plus vaa table")
    parser.add_argument('vbet_segments', help='vbet segment polygons')
    parser.add_argument('vbet_points', help='valley bottom or other polygon representing confining boundary (.shp, .gpkg/layer_name)', type=str)
    parser.add_argument('vbet_centerline', help='vbet centerline feature class')
    parser.add_argument('dem', help='dem')
    parser.add_argument('ppt', help='Precipitation Raster')
    parser.add_argument('roads', help='Roads shapefile')
    parser.add_argument('rail', help='Rail shapefile')
    parser.add_argument('ecoregions', help='Ecoregions shapefile')
    parser.add_argument('output_folder', help='Output folder', type=str)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help="(optional) save intermediate outputs for debugging", action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("GNAT")
    log.setup(logPath=os.path.join(args.output_folder, "gnat.log"), verbose=args.verbose)
    log.title(f'GNAT For HUC: {args.huc}')

    meta = parse_metadata(args.meta)
    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_folder, 'confinement_mem.log')
            retcode, max_obj = ThreadRun(gnat, memfile,
                                         args.huc,
                                         args.flowlines,
                                         args.vaa_table,
                                         args.vbet_segments,
                                         args.vbet_points,
                                         args.vbet_centerline,
                                         args.dem,
                                         args.ppt,
                                         args.roads,
                                         args.rail,
                                         args.ecoregions,
                                         args.output_folder,
                                         meta=meta)
            log.debug(f'Return code: {retcode}, [Max process usage] {max_obj}')

        else:
            gnat(args.huc,
                 args.flowlines,
                 args.vaa_table,
                 args.vbet_segments,
                 args.vbet_points,
                 args.vbet_centerline,
                 args.dem,
                 args.ppt,
                 args.roads,
                 args.rail,
                 args.ecoregions,
                 args.output_folder,
                 meta=meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
