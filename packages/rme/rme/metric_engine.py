#!/usr/bin/env python3
"""
Name:     Riverscapes Metric Engine

Purpose:  Build a Riverscapes Metric Engine project by downloading and preparing
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

from osgeo import ogr
from osgeo import gdal
import numpy as np
import rasterio
from rasterio.mask import mask
from shapely.geometry import Point

from rscommons import GeopackageLayer, dotenv, Logger, initGDALOGRErrors, ModelConfig, RSLayer, RSMeta, RSMetaTypes, RSProject, VectorBase, ProgressBar
from rscommons.classes.vector_base import get_utm_zone_epsg
from rscommons.util import safe_makedirs, parse_metadata
from rscommons.database import load_lookup_data, SQLiteCon
from rscommons.geometry_ops import reduce_precision, get_endpoints
from rscommons.vector_ops import copy_feature_class, collect_linestring
from rscommons.vbet_network import copy_vaa_attributes, join_attributes
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions
from rscommons.moving_window import moving_window_dgo_ids

from rme.__version__ import __version__
from rme.analysis_window import AnalysisLine
from rme.utils.rme_report import RMEReport
from rme.utils.check_vbet_inputs import vbet_inputs

Path = str

initGDALOGRErrors()
gdal.UseExceptions()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    # key: (name, id, tag, relpath)]
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'FLOWLINES': RSLayer('Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'VBET_SEGMENTS': RSLayer('Vbet Segments', 'VBET_SEGMENTS', 'Vector', 'vbet_segments'),
        'VBET_SEGMENT_POINTS': RSLayer('Vbet Segment Points', 'VBET_SEGMENT_POINTS', 'Vector', 'points'),
        'VBET_CENTERLINES': RSLayer('VBET Centerline', 'VBET_CENTERLINE', 'Vector', 'vbet_centerlines'),
        'ECO_REGIONS': RSLayer('Eco Regions', 'ECO_REGIONS', 'Vector', 'eco_regions'),
        'ROADS': RSLayer('Roads', 'Roads', 'Vector', 'roads'),
        'RAIL': RSLayer('Rail', 'Rail', 'Vector', 'rail'),
        'CONFINEMENT_DGO': RSLayer('Confinement DGO', 'CONFINEMENT_DGO', 'Vector', 'confinement_dgo'),
        'ANTHRO_DGO': RSLayer('Anthropogenic DGO', 'ANTHRO_DGO', 'Vector', 'anthro_dgo'),
        'RCAT_DGO': RSLayer('RCAT DGO', 'RCAT_DGO', 'Vector', 'rcat_dgo')
    }),
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'inputs/dem.tif'),
    'PPT': RSLayer('Precipitation', 'Precip', 'Raster', 'inputs/precipitation.tif'),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/rme_intermediates.gpkg', {
        'JUNCTION_POINTS': RSLayer('Junction Points', 'JUNCTION_POINTS', 'Vector', 'junction_points'),
    }),
    'RME_OUTPUTS': RSLayer('Riverscapes Metrics', 'RME_OUTPUTS', 'Geopackage', 'outputs/riverscapes_metrics.gpkg', {
        'DGO_METRICS': RSLayer('DGO Metrics', 'DGO_METRICS', 'Vector', 'vw_dgo_metrics'),
        'POINT_METRICS': RSLayer('Point Metrics', 'POINT_METRICS', 'Vector', 'vw_point_metrics'),
        'POINT_MEASUREMENTS': RSLayer('Point Measurements', 'POINT_MEASUREMENTS', 'Vector', 'vw_point_measurements')
    }),
    'REPORT': RSLayer('RME Report', 'REPORT', 'HTMLFile', 'outputs/rme.html')
}

stream_size_lookup = {0: 'small', 1: 'medium', 2: 'large', 3: 'very large', 4: 'huge'}
gradient_buffer_lookup = {'small': 25.0, 'medium': 50.0, 'large': 100.0, 'very large': 100.0, 'huge': 100.0}  # should this go as high as it does
window_distance = {'0': 200.0, '1': 400.0, '2': 1200.0, '3': 2000.0, '4': 8000.0}


def metric_engine(huc: int, in_flowlines: Path, in_vaa_table: Path, in_segments: Path, in_points: Path, in_vbet_centerline: Path,
                  in_dem: Path, in_ppt: Path, in_roads: Path, in_rail: Path, in_ecoregions: Path, project_folder: Path,
                  in_confinement_dgos: Path = None, in_anthro_dgos: Path = None, in_rcat_dgos: Path = None, level_paths: list = None, meta: dict = None):
    """Generate Riverscapes Metric Engine project and calculate metrics

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
        project_folder (Path): output folder for RME project
        level_paths (list, optional): level paths to process. Defaults to None.
        meta (dict, optional): key-value pairs of metadata. Defaults to None.
    """

    log = Logger('Riverscapes Metric Engine')
    log.info(f'Starting RME v.{cfg.version}')

    # Check that all inputs have the same VBET input
    project_dgos = []
    for p in [in_confinement_dgos, in_anthro_dgos, in_rcat_dgos]:
        if p is not None:
            project_dgos.append(os.path.dirname(os.path.dirname(os.path.dirname(p))))
    if len(project_dgos) > 0:
        vbin = vbet_inputs(os.path.dirname(os.path.dirname(os.path.dirname(in_segments))), project_dgos)

        if vbin is None or vbin is False:
            # check that all dgos have same num features
            ftr_count = []
            for p in [in_confinement_dgos, in_anthro_dgos, in_rcat_dgos]:
                if p is not None:
                    with GeopackageLayer(p) as lyr:
                        if lyr.ogr_layer.GetFeatureCount() not in ftr_count:
                            ftr_count.append(lyr.ogr_layer.GetFeatureCount())
            if len(ftr_count) > 1:
                log.error('DGO inputs do not have the same number of features')
                sys.exit(1)

    augment_layermeta('rs_metric_engine', LYR_DESCRIPTIONS_JSON, LayerTypes)

    project_name = f'Riverscapes Metrics for HUC {huc}'
    project = RSProject(cfg, project_folder)
    project.create(project_name, 'rs_metric_engine', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/rme', RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True),
        RSMeta('RME Version', cfg.version, locked=True),
        RSMeta('RME Timestamp', str(int(time.time())), RSMetaTypes.TIMESTAMP, locked=True)
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Inputs', 'Intermediates', 'Outputs'], create_folders=True)

    inputs_gpkg = os.path.join(project_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg = os.path.join(project_folder, LayerTypes['INTERMEDIATES'].rel_path)
    outputs_gpkg = os.path.join(project_folder, LayerTypes['RME_OUTPUTS'].rel_path)
    GeopackageLayer.delete(inputs_gpkg)
    GeopackageLayer.delete(intermediates_gpkg)
    GeopackageLayer.delete(outputs_gpkg)

    flowlines = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    copy_feature_class(in_flowlines, flowlines)
    segments = os.path.join(outputs_gpkg, LayerTypes['INPUTS'].sub_layers['VBET_SEGMENTS'].rel_path)
    copy_feature_class(in_segments, segments)
    points = os.path.join(outputs_gpkg, LayerTypes['INPUTS'].sub_layers['VBET_SEGMENT_POINTS'].rel_path)
    copy_feature_class(in_points, points)
    centerlines = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['VBET_CENTERLINES'].rel_path)
    copy_feature_class(in_vbet_centerline, centerlines)
    roads = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['ROADS'].rel_path)
    copy_feature_class(in_roads, roads)
    rail = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['RAIL'].rel_path)
    copy_feature_class(in_rail, rail)
    ecoregions = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['ECO_REGIONS'].rel_path)
    copy_feature_class(in_ecoregions, ecoregions)
    if in_confinement_dgos:
        confinement_dgos = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['CONFINEMENT_DGO'].rel_path)
        copy_feature_class(in_confinement_dgos, confinement_dgos)
    else:
        confinement_dgos = None
    if in_anthro_dgos:
        anthro_dgos = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['ANTHRO_DGO'].rel_path)
        copy_feature_class(in_anthro_dgos, anthro_dgos)
    else:
        anthro_dgos = None
    if in_rcat_dgos:
        rcat_dgos = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['RCAT_DGO'].rel_path)
        copy_feature_class(in_rcat_dgos, rcat_dgos)
    else:
        rcat_dgos = None

    # get utm
    with GeopackageLayer(points) as lyr_pts:
        feat = lyr_pts.ogr_layer.GetNextFeature()
        geom = feat.GetGeometryRef()
        utm_epsg = get_utm_zone_epsg(geom.GetPoint(0)[0])

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
    with sqlite3.connect(outputs_gpkg) as conn:
        cursor = conn.cursor()
        with open(os.path.join(database_folder, 'metrics_schema.sql'), encoding='utf-8') as sqlfile:
            sql_commands = sqlfile.read()
            cursor.executescript(sql_commands)
            conn.commit()
    # Load tables
    load_lookup_data(outputs_gpkg, os.path.join(database_folder, 'data_metrics'))

    # index level path and seg distance
    with sqlite3.connect(outputs_gpkg) as conn:
        curs = conn.cursor()
        curs.execute("CREATE INDEX idx_level_path_seg ON vbet_segments (LevelPathI)")
        curs.execute("CREATE INDEX idx_seg_distance_seg ON vbet_segments (seg_distance)")
        curs.execute("CREATE INDEX idx_size ON points (stream_size)")
        curs.execute("CREATE INDEX idx_level_path_pts ON points (LevelPathI)")
        curs.execute("CREATE INDEX idx_seg_distance_pts ON points (seg_distance)")
        conn.commit()

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

    # store moving windows of igos for later summarization
    windows = moving_window_dgo_ids(points, segments, level_paths_to_run, window_distance)

    # associate single DGOs with single IGOs for non moving window metrics
    log.info('Associating DGOs with IGOs')
    igo_dgo = {}
    with GeopackageLayer(segments) as lyr_segments, \
            GeopackageLayer(points) as lyr_points:
        for feat, *_ in lyr_points.iterate_features():
            igo_id = feat.GetFID()
            level_path = feat.GetField('LevelPathI')
            seg_distance = feat.GetField('seg_distance')
            sql = f'LevelPathI = {level_path} and seg_distance = {seg_distance}'
            for feat_seg, *_ in lyr_segments.iterate_features(attribute_filter=sql):
                igo_dgo[igo_id] = feat_seg.GetFID()
                break

    metrics = generate_metric_list(outputs_gpkg)
    measurements = generate_metric_list(outputs_gpkg, 'measurements')

    buffer_distance = {}
    for stream_size, distance in gradient_buffer_lookup.items():
        buffer = VectorBase.rough_convert_metres_to_raster_units(dem, distance)
        buffer_distance[stream_size] = buffer

    progbar = ProgressBar(len(level_paths_to_run), 50, "Calculating Riverscapes Metrics")
    counter = 0
    for level_path in level_paths_to_run:
        lp_metrics = {}
        lp_meas = {}
        progbar.update(counter)
        counter += 1

        with GeopackageLayer(segments) as lyr_segments, \
                rasterio.open(dem) as src_dem:

            # buffer_size_clip = lyr_points.rough_convert_metres_to_vector_units(0.25)
            _transform_ref, transform = VectorBase.get_transform_from_epsg(lyr_segments.spatial_ref, utm_epsg)
            AnalysisLine.transform = transform

            geom_flowline = collect_linestring(line_network, f'LevelPathI = {level_path}')
            if geom_flowline.IsEmpty():
                log.error(f'Flowline for level path {level_path} is empty geometry')
                continue

            geom_centerline = collect_linestring(centerlines, f'LevelPathI = {level_path}', precision=8)

            for feat_seg_dgo, *_ in lyr_segments.iterate_features(attribute_filter=f'LevelPathI = {level_path}'):
                # Gather common components for metric calcuations
                feat_geom = feat_seg_dgo.GetGeometryRef().Clone()
                dgo_id = feat_seg_dgo.GetFID()
                segment_distance = feat_seg_dgo.GetField('seg_distance')
                if segment_distance is None:
                    continue
                # stream_size_id = feat_seg_pt.GetField('stream_size')
                # curs.execute("SELECT stream_size from points WHERE seg_distance = ? and LevelPathI = ?", (segment_distance, level_path))
                # stream_size_id = curs.fetchone()[0]
                with GeopackageLayer(points) as lyr_points:
                    for pt_ftr, *_ in lyr_points.iterate_features(attribute_filter=f'LevelPathI = {level_path} and seg_distance = {segment_distance}'):
                        stream_size_id = pt_ftr.GetField('stream_size')
                        break

                stream_size = stream_size_lookup[stream_size_id]
                # window_geoms = {}  # Different metrics may require different windows. Store generated windows here for reuse.
                metrics_output = {}
                measurements_output = {}
                min_elev = None
                max_elev = None

                # Calculate each metric if it is active
                if 'STRMGRAD' in metrics:
                    metric = metrics['STRMGRAD']

                    stream_length, min_elev, max_elev = get_segment_measurements(geom_flowline, src_dem, feat_geom, buffer_distance[stream_size], transform)
                    measurements_output[measurements['STRMMINELEV']['measurement_id']] = min_elev
                    measurements_output[measurements['STRMMAXELEV']['measurement_id']] = max_elev
                    measurements_output[measurements['STRMLENG']['measurement_id']] = stream_length

                    gradient = None if any(value is None for value in [max_elev, min_elev]) else (max_elev - min_elev) / stream_length
                    metrics_output[metric['metric_id']] = gradient

                if 'VALGRAD' in metrics:
                    metric = metrics['VALGRAD']

                    centerline_length, *_ = get_segment_measurements(geom_centerline, src_dem, feat_geom, buffer_distance[stream_size], transform)
                    measurements_output[measurements['VALLENG']['measurement_id']] = centerline_length

                    if any(elev is None for elev in [min_elev, max_elev]):
                        _, min_elev, max_elev = get_segment_measurements(geom_flowline, dem, feat_geom, buffer_distance[stream_size], transform)
                        measurements_output[measurements['STRMMINELEV']['measurement_id']] = min_elev
                        measurements_output[measurements['STRMMAXELEV']['measurement_id']] = max_elev

                    gradient = None if any(value is None for value in [max_elev, min_elev]) else (max_elev - min_elev) / centerline_length
                    metrics_output[metric['metric_id']] = gradient

                if 'STRMORDR' in metrics:
                    metric = metrics['STRMORDR']

                    results = []
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                            results.append(feat.GetField('StreamOrde'))
                        lyr_lines.ogr_layer.SetSpatialFilter(None)
                    if len(results) > 0:
                        stream_order = max(results)
                    else:
                        stream_order = None
                        log.warning(f'Unable to calculate Stream Order for dgo {dgo_id} in level path {level_path}')
                    metrics_output[metric['metric_id']] = stream_order

                if 'HEDWTR' in metrics:
                    metric = metrics['HEDWTR']

                    sum_attributes = {}
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                            line_geom = feat.GetGeometryRef()
                            attribute = str(feat.GetField('STARTFLAG'))
                            if attribute not in ['1', '0']:
                                continue
                            geom_section = feat_geom.Intersection(line_geom)
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

                    attributes = {}
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                            line_geom = feat.GetGeometryRef()
                            attribute = str(feat.GetField('FCode'))
                            geom_section = feat_geom.Intersection(line_geom)
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

                    afp_area = feat_seg_dgo.GetField('active_floodplain_area') if feat_seg_dgo.GetField('active_floodplain_area') is not None else 0.0
                    metrics_output[metric['metric_id']] = afp_area

                if 'ACTCHANAREA' in metrics:
                    metric = metrics['ACTCHANAREA']

                    ac_area = feat_seg_dgo.GetField('active_channel_area') if feat_seg_dgo.GetField('active_channel_area') is not None else 0.0
                    metrics_output[metric['metric_id']] = ac_area

                if 'INTGWDTH' in metrics:
                    metric = metrics['INTGWDTH']

                    ig_width = feat_seg_dgo.GetField('segment_area') / feat_seg_dgo.GetField('centerline_length') if feat_seg_dgo.GetField('centerline_length') is not None else None
                    metrics_output[metric['metric_id']] = ig_width
                if 'CHANVBRAT' in metrics:
                    metric = metrics['CHANVBRAT']

                    ac_ratio = feat_seg_dgo.GetField('active_channel_area') / feat_seg_dgo.GetField('segment_area') if feat_seg_dgo.GetField('segment_area') > 0.0 else None
                    metrics_output[metric['metric_id']] = ac_ratio

                if 'FLDVBRAT' in metrics:
                    metric = metrics['FLDVBRAT']

                    fp_ratio = feat_seg_dgo.GetField('floodplain_area') / feat_seg_dgo.GetField('segment_area') if feat_seg_dgo.GetField('segment_area') > 0.0 else None
                    metrics_output[metric['metric_id']] = fp_ratio

                if 'RELFLWLNGTH' in metrics:
                    metric = metrics['RELFLWLNGTH']

                    geom_flowline_full = collect_linestring(line_network, f'vbet_level_path = {level_path}')
                    stream_length_total, *_ = get_segment_measurements(geom_flowline_full, src_dem, feat_geom, buffer_distance[stream_size], transform)
                    centerline_length, *_ = get_segment_measurements(geom_centerline, src_dem, feat_geom, buffer_distance[stream_size], transform)

                    relative_flow_length = stream_length_total / centerline_length if centerline_length > 0.0 else None
                    metrics_output[metric['metric_id']] = relative_flow_length

                if 'STRMSIZE' in metrics:
                    metric = metrics['STRMSIZE']

                    stream_length, *_ = get_segment_measurements(geom_flowline, src_dem, feat_geom, buffer_distance[stream_size], transform)

                    stream_size_metric = feat_seg_dgo.GetField('active_channel_area') / stream_length if stream_length > 0.0 else None
                    metrics_output[metric['metric_id']] = stream_size_metric

                if 'ECORGIII' in metrics:
                    metric = metrics['ECORGIII']

                    attributes = {}
                    with GeopackageLayer(ecoregions) as lyr_ecoregions:
                        for feat, *_ in lyr_ecoregions.iterate_features(clip_shape=feat_geom):
                            geom_ecoregion = feat.GetGeometryRef()
                            attribute = str(feat.GetField('US_L3CODE'))
                            geom_section = feat_geom.Intersection(geom_ecoregion)
                            area = geom_section.GetArea()
                            attributes[attribute] = attributes.get(attribute, 0) + area
                        lyr_ecoregions = None
                    if len(attributes) == 0:
                        log.warning(f'Unable to find majority ecoregion III for pt {dgo_id} in level path {level_path}')
                        majority_attribute = None
                    else:
                        majority_attribute = max(attributes, key=attributes.get)
                    metrics_output[metric['metric_id']] = majority_attribute

                if 'ECORGIV' in metrics:
                    metric = metrics['ECORGIV']

                    attributes = {}
                    with GeopackageLayer(ecoregions) as lyr_ecoregions:
                        for feat, *_ in lyr_ecoregions.iterate_features(clip_shape=feat_geom):
                            geom_ecoregion = feat.GetGeometryRef()
                            attribute = str(feat.GetField('US_L4CODE'))
                            geom_section = feat_geom.Intersection(geom_ecoregion)
                            area = geom_section.GetArea()
                            attributes[attribute] = attributes.get(attribute, 0) + area
                        lyr_ecoregions = None
                    if len(attributes) == 0:
                        log.warning(f'Unable to find majority ecoregion III for pt {dgo_id} in level path {level_path}')
                        majority_attribute = None
                    else:
                        majority_attribute = max(attributes, key=attributes.get)
                    metrics_output[metric['metric_id']] = majority_attribute

                if 'CONF' in metrics:
                    metric = metrics['CONF']

                    with GeopackageLayer(junctions) as lyr_pts:
                        count = 0
                        for feat, *_ in lyr_pts.iterate_features(clip_shape=feat_geom, attribute_filter=""""JunctionType" = 'Confluence'"""):
                            count += 1
                        metrics_output[metric['metric_id']] = count

                if 'DIFF' in metrics:
                    metric = metrics['DIFF']

                    with GeopackageLayer(junctions) as lyr_pts:
                        count = 0
                        for feat, *_ in lyr_pts.iterate_features(clip_shape=feat_geom, attribute_filter=""""JunctionType" = 'Diffluence'"""):
                            count += 1
                        metrics_output[metric['metric_id']] = count

                if 'TRIBS' in metrics:
                    metric = metrics['TRIBS']

                    with GeopackageLayer(junctions) as lyr_pts:
                        count = 0
                        for feat, *_ in lyr_pts.iterate_features(clip_shape=feat_geom, attribute_filter=""""JunctionType" = 'Tributary'"""):
                            count += 1
                        metrics_output[metric['metric_id']] = count

                if 'CHANSIN' in metrics:
                    metric = metrics['CHANSIN']

                    line = AnalysisLine(geom_flowline, feat_geom)
                    measurements_output[measurements['STRMSTRLENG']['measurement_id']] = line.endpoint_distance
                    metrics_output[metric['metric_id']] = line.sinuosity()

                if 'DRAINAREA' in metrics:
                    metric = metrics['DRAINAREA']

                    results = []
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                            results.append(feat.GetField('TotDASqKm'))
                    if len(results) > 0:
                        drainage_area = max(results)
                    else:
                        drainage_area = None
                        log.warning(f'Unable to calculate drainage area for pt {dgo_id} in level path {level_path}')
                    metrics_output[metric['metric_id']] = drainage_area

                if 'VALAZMTH' in metrics:
                    metric = metrics['VALAZMTH']

                    cline = AnalysisLine(geom_centerline, feat_geom)
                    metrics_output[metric['metric_id']] = cline.azimuth()

                if 'CNFMT' in metrics and confinement_dgos:
                    metric = metrics['CNFMT']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(f"SELECT Confinement_Ratio FROM confinement_dgo WHERE fid = {dgo_id}")
                        conf_ratio = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = conf_ratio

                if 'CONST' in metrics and confinement_dgos:
                    metric = metrics['CONST']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(f"SELECT Constriction_Ratio FROM confinement_dgo WHERE fid = {dgo_id}")
                        cons_ratio = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = cons_ratio

                if 'CONFMARG' in metrics and confinement_dgos:
                    metric = metrics['CONFMARG']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(f"SELECT ConfinLeng FROM confinement_dgo WHERE fid = {dgo_id}")
                        conf_margin = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = conf_margin

                if 'ROADDENS' in metrics and anthro_dgos:
                    metric = metrics['ROADDENS']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(f"SELECT Road_len, centerline_length FROM anthro_dgo WHERE fid = {dgo_id}")
                        roadd = curs.fetchone()
                        road_density = roadd[0] / roadd[1] if roadd[1] > 0.0 else None
                    metrics_output[metric['metric_id']] = road_density

                if 'RAILDENS' in metrics and anthro_dgos:
                    metric = metrics['RAILDENS']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(f"SELECT Rail_len, centerline_length FROM anthro_dgo WHERE fid = {dgo_id}")
                        raild = curs.fetchone()
                        rail_density = raild[0] / raild[1] if raild[1] > 0.0 else None
                    metrics_output[metric['metric_id']] = rail_density

                if 'LUI' in metrics and anthro_dgos:
                    metric = metrics['LUI']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(f"SELECT LUI FROM anthro_dgo WHERE fid = {dgo_id}")
                        lui = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = lui

                if 'FPACCESS' in metrics and rcat_dgos:
                    metric = metrics['FPACCESS']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(f"SELECT FloodplainAccess FROM rcat_dgo WHERE fid = {dgo_id}")
                        fp_access = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = fp_access

                # Write to Metrics
                if len(metrics_output) > 0:
                    lp_metrics[dgo_id] = metrics_output
                    # curs.executemany("INSERT INTO metric_values (dgo_id, metric_id, metric_value) VALUES (?,?,?)", [(dgo_id, name, value) for name, value in metrics_output.items()])
                if len(measurements_output) > 0:
                    lp_meas[dgo_id] = measurements_output
                    # curs.executemany("INSERT INTO measurement_values (dgo_id, measurement_id, measurement_value) VALUES (?,?,?)", [(dgo_id, name, value) for name, value in measurements_output.items()])

        with sqlite3.connect(outputs_gpkg) as conn:
            curs = conn.cursor()
            for dgo_id, vals in lp_metrics.items():
                curs.executemany("INSERT INTO metric_values (dgo_id, metric_id, metric_value) VALUES (?,?,?)", [(dgo_id, name, value) for name, value in vals.items()])
            for dgo_id, vals in lp_meas.items():
                curs.executemany("INSERT INTO measurement_values (dgo_id, measurement_id, measurement_value) VALUES (?,?,?)", [(dgo_id, name, value) for name, value in vals.items()])
            conn.commit()

    # fill out igo_metrics table using moving window analysis
    progbar = ProgressBar(len(windows), 50, "Calculating Moving Window Metrics")
    counter = 0
    for igo_id, dgo_ids in windows.items():
        counter += 1
        progbar.update(counter)
        with sqlite3.connect(outputs_gpkg) as conn:
            curs = conn.cursor()

            if 'STRMGRAD' in metrics:
                curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMMINELEV']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                elevs = [row[0] for row in curs.fetchall() if row[0] is not None]
                min_elev = min(elevs) if len(elevs) > 0 else None
                curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMMAXELEV']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                elevs = [row[0] for row in curs.fetchall() if row[0] is not None]
                max_elev = max(elevs) if len(elevs) > 0 else None
                curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                stream_length = sum([row[0] for row in curs.fetchall()])
                gradient = None if any(value is None for value in [max_elev, min_elev, stream_length]) else (max_elev - min_elev) / stream_length
                if gradient is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['STRMGRAD']['metric_id']}, {gradient})")

            if 'VALGRAD' in metrics:
                curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['VALLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                centerline_length = sum([row[0] for row in curs.fetchall()])
                if any(elev is None for elev in [min_elev, max_elev]):
                    curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMMINELEV']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    elevs = [row[0] for row in curs.fetchall() if row[0] is not None]
                    min_elev = min(elevs) if len(elevs) > 0 else None
                    curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMMAXELEV']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    elevs = [row[0] for row in curs.fetchall() if row[0] is not None]
                    max_elev = max(elevs) if len(elevs) > 0 else None
                gradient = None if any(value is None for value in [max_elev, min_elev, centerline_length]) else (max_elev - min_elev) / centerline_length
                if gradient is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['VALGRAD']['metric_id']}, {gradient})")

            if 'STRMORDR' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['STRMORDR']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                stream_order = curs.fetchone()[0]
                if stream_order is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['STRMORDR']['metric_id']}, {stream_order})")

            if 'HEDWTR' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['HEDWTR']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                hw = curs.fetchone()[0]
                if hw is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['HEDWTR']['metric_id']}, {hw})")

            if 'STRMTYPE' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['STRMTYPE']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                fcode = curs.fetchone()[0]
                if fcode is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['STRMTYPE']['metric_id']}, {majority_fcode})")

            if 'ACTFLDAREA' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['ACTFLDAREA']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                afp = [row[0] for row in curs.fetchall() if row[0] is not None]
                afp_area = sum(afp) if len(afp) > 0 else None
                if afp_area is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ACTFLDAREA']['metric_id']}, {afp_area})")

            if 'ACTCHANAREA' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['ACTCHANAREA']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                ac = [row[0] for row in curs.fetchall() if row[0] is not None]
                ac_area = sum(ac) if len(ac) > 0 else None
                if ac_area is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ACTCHANAREA']['metric_id']}, {ac_area})")

            if 'INTGWDTH' in metrics:
                if centerline_length is None:
                    curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['VALLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    cl = [row[0] for row in curs.fetchall() if row[0] is not None]
                    centerline_length = sum(cl) if len(cl) > 0 else None
                curs.execute(f"SELECT segment_area FROM vbet_segments WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                sa = [row[0] for row in curs.fetchall() if row[0] is not None]
                segment_area = sum(sa) if len(sa) > 0 else None
                ig_width = None if any(value is None for value in [segment_area, centerline_length]) else segment_area / centerline_length
                if ig_width is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['INTGWDTH']['metric_id']}, {ig_width})")

            if 'CHANVBRAT' in metrics:
                if ac_area is None:
                    curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['ACTCHANAREA']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    ac = [row[0] for row in curs.fetchall() if row[0] is not None]
                    ac_area = sum(ac) if len(ac) > 0 else None
                if segment_area is None:
                    curs.execute(f"SELECT segment_area FROM vbet_segments WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    sa = [row[0] for row in curs.fetchall() if row[0] is not None]
                    segment_area = sum(sa) if len(sa) > 0 else None
                ac_ratio = None if any(value is None for value in [ac_area, segment_area]) else ac_area / segment_area
                if ac_ratio is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CHANVBRAT']['metric_id']}, {ac_ratio})")

            if 'FLDVBRAT' in metrics:
                curs.execute(f"SELECT floodplain_area FROM vbet_segments WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                fp = [row[0] for row in curs.fetchall() if row[0] is not None]
                fp_area = sum(fp) if len(fp) > 0 else None
                if segment_area is None:
                    curs.execute(f"SELECT segment_area FROM vbet_segments WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    sa = [row[0] for row in curs.fetchall() if row[0] is not None]
                    segment_area = sum(sa) if len(sa) > 0 else None
                fp_ratio = None if any(value is None for value in [fp_area, segment_area]) else fp_area / segment_area
                if fp_ratio is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['FLDVBRAT']['metric_id']}, {fp_ratio})")

            if 'RELFLWLNGTH' in metrics:
                if centerline_length is None:
                    curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['VALLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    cl = [row[0] for row in curs.fetchall() if row[0] is not None]
                    centerline_length = sum(cl) if len(cl) > 0 else None
                if stream_length is None:
                    curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMSTRLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    sl = [row[0] for row in curs.fetchall() if row[0] is not None]
                    stream_length = sum(sl) if len(sl) > 0 else None
                relative_flow_length = None if any(value is None for value in [stream_length, centerline_length]) else stream_length / centerline_length
                if relative_flow_length is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['RELFLWLNGTH']['metric_id']}, {relative_flow_length})")

            if 'STRMSIZE' in metrics:
                curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMSTRLENG']['measurement_id']} AND dgo_id = {igo_dgo[igo_id]}")
                stream_length = curs.fetchone()[0]
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['ACTCHANAREA']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                ac = curs.fetchone()[0]

                stream_size_metric = None if any(value is None for value in [ac, stream_length]) else ac / stream_length
                if stream_size_metric is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['STRMSIZE']['metric_id']}, {stream_size_metric})")

            if 'ECORGIII' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['ECORGIII']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                ecor3 = curs.fetchone()[0]
                if ecor3 is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ECORGIII']['metric_id']}, {ecor3})")

            if 'ECORGIV' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['ECORGIV']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                ecor4 = curs.fetchone()[0]
                if ecor4 is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ECORGIV']['metric_id']}, {ecor4})")

            if 'CONF' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['CONF']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                count = curs.fetchone()[0]
                if count is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CONF']['metric_id']}, {count})")

            if 'DIFF' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['DIFF']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                count2 = curs.fetchone()[0]
                if count2 is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['DIFF']['metric_id']}, {count2})")

            if 'TRIBS' in metrics:
                if stream_length is None:
                    curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    stream_length = sum([row[0] for row in curs.fetchall()])
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['TRIBS']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                count3 = sum([row[0] for row in curs.fetchall()])
                trib_dens = None if any(value is None for value in [count3, stream_length]) else count3 / (stream_length / 1000.0)
                if trib_dens is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['TRIBS']['metric_id']}, {trib_dens})")

            if 'CHANSIN' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['CHANSIN']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                sinuos = [row[0] for row in curs.fetchall()]
                curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                lens = [row[0] for row in curs.fetchall()]
                if len(sinuos) != len(lens):
                    log.warning(f'Unable to calculate sinuosity for pt {dgo_id} using moving window, using DGO value')
                    curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['CHANSIN']['metric_id']} AND dgo_id = {dgo_id}")
                    sinuosity = curs.fetchone()[0]
                else:
                    sinuosity = sum([sinuos[i] * lens[i] for i in range(len(sinuos)) if sinuos[i] is not None and lens[i] is not None]) / sum(lens) if sum(lens) > 0.0 else None
                if sinuosity is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CHANSIN']['metric_id']}, {sinuosity})")

            if 'DRAINAREA' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['DRAINAREA']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                da = curs.fetchone()[0]
                if da is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['DRAINAREA']['metric_id']}, {da})")

            if 'VALAZMTH' in metrics:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['VALAZMTH']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                azs = [row[0] for row in curs.fetchall()]
                curs.execute(f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['VALLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                lens = [row[0] for row in curs.fetchall()]
                if len(azs) != len(lens):
                    log.warning(f'Unable to calculate azimuth for pt {dgo_id} using moving window, using DGO value')
                    curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['VALAZMTH']['metric_id']} AND dgo_id = {dgo_id}")
                    azimuth = curs.fetchone()[0]
                else:
                    azimuth = sum([azs[i] * lens[i] for i in range(len(azs)) if azs[i] is not None and lens[i] is not None]) / sum(lens) if sum(lens) > 0.0 else None
                if azimuth is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['VALAZMTH']['metric_id']}, {azimuth})")

            if 'CNFMT' in metrics and confinement_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(f"SELECT ConfinLeng, ApproxLeng FROM confinement_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    confs = curs2.fetchall()
                    conf_ratio = sum([c[0] for c in confs]) / sum([c[1] for c in confs]) if sum([c[1] for c in confs]) > 0 else None
                if conf_ratio is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CNFMT']['metric_id']}, {conf_ratio})")

            if 'CONST' in metrics and confinement_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(f"SELECT ConstrLeng, ApproxLeng FROM confinement_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    cons = curs2.fetchall()
                    cons_ratio = sum([c[0] for c in cons]) / sum([c[1] for c in cons]) if sum([c[1] for c in cons]) > 0 else None
                if cons_ratio is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CONST']['metric_id']}, {cons_ratio})")

            if 'CONFMARG' in metrics and confinement_dgos:
                curs.execute(f"SELECT metric_value FROM metric_values WHERE metric_id = {metrics['CONFMARG']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                conf_margin = curs.fetchone()[0]
                if conf_margin is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CONFMARG']['metric_id']}, {conf_margin})")

            if 'ROADDENS' in metrics and anthro_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(f"SELECT Road_len, centerline_length FROM anthro_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    roadd = curs2.fetchall()
                    road_density = sum([r[0] for r in roadd]) / sum([r[1] for r in roadd]) if sum([r[1] for r in roadd]) > 0.0 else None
                if road_density is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ROADDENS']['metric_id']}, {road_density})")

            if 'RAILDENS' in metrics and anthro_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(f"SELECT Rail_len, centerline_length FROM anthro_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    raild = curs2.fetchall()
                    rail_density = sum([r[0] for r in raild]) / sum([r[1] for r in raild]) if sum([r[1] for r in raild]) > 0.0 else None
                if rail_density is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['RAILDENS']['metric_id']}, {rail_density})")

            if 'LUI' in metrics and anthro_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(f"SELECT LUI, segment_area FROM anthro_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    luivals = curs2.fetchall()
                    lui = sum(luivals[i][0] * luivals[i][1] for i in range(len(luivals))) / sum([luivals[i][1] for i in range(len(luivals))]) if sum([luivals[i][1] for i in range(len(luivals))]) > 0.0 else None
                if lui is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['LUI']['metric_id']}, {lui})")

            if 'FPACCESS' in metrics and rcat_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(f"SELECT FloodplainAccess, segment_area FROM rcat_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    fpacc = curs2.fetchall()
                    fp_access = sum(fpacc[i][0] * fpacc[i][1] for i in range(len(fpacc))) / sum([fpacc[i][1] for i in range(len(fpacc))]) if sum([fpacc[i][1] for i in range(len(fpacc))]) > 0.0 else None
                if fp_access is not None:
                    curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['FPACCESS']['metric_id']}, {fp_access})")

            conn.commit()

    epsg = 4326
    with sqlite3.connect(outputs_gpkg) as conn:
        # Generate Pivot Table
        curs = conn.cursor()
        metrics_sql = ", ".join([f"{sql_name(metric['field_name'])} {metric['data_type']}" for metric in metrics.values()])
        sql = f'CREATE TABLE dgo_metrics_pivot (fid INTEGER PRIMARY KEY, {metrics_sql});'
        curs.execute(sql)
        sql2 = f'CREATE TABLE igo_metrics_pivot (fid INTEGER PRIMARY KEY, {metrics_sql});'
        curs.execute(sql2)
        conn.commit()

        # Insert Values into Pivot table
        metric_names_sql = ', '.join([sql_name(metric["field_name"]) for metric in metrics.values()])
        metric_values_sql = ", ".join([f"{sql_round(metric['data_type'], metric['metric_id'])} {sql_name(metric['field_name'])}" for metric in metrics.values()])
        sql = f'INSERT INTO dgo_metrics_pivot (fid, {metric_names_sql}) SELECT M.dgo_id, {metric_values_sql} FROM metric_values M GROUP BY M.dgo_id;'
        curs.execute(sql)
        sql2 = f'INSERT INTO igo_metrics_pivot (fid, {metric_names_sql}) SELECT M.igo_id, {metric_values_sql} FROM igo_metric_values M GROUP BY M.igo_id;'
        curs.execute(sql2)
        conn.commit()

        # Create metric view
        metric_names_sql = ", ".join([f"M.{sql_name(metric['field_name'])} {sql_name(metric['field_name'])}" for metric in metrics.values()])
        sql = f'CREATE VIEW vw_point_metrics AS SELECT G.fid fid, G.geom geom, G.LevelPathI level_path, G.seg_distance seg_distance, G.stream_size stream_size, {metric_names_sql} FROM points G INNER JOIN igo_metrics_pivot M ON M.fid = G.fid;'
        curs.execute(sql)
        sql2 = f'CREATE VIEW vw_dgo_metrics AS SELECT G.fid fid, G.geom geom, G.LevelPathI level_path, G.seg_distance seg_distance, {metric_names_sql} FROM vbet_segments G INNER JOIN dgo_metrics_pivot M ON M.fid = G.fid;'
        curs.execute(sql2)
        conn.commit()

        measure_sql = ", ".join([f"{sql_name(measurement['name'])} {measurement['name']}" for measurement in measurements.values()])
        sql = f'CREATE TABLE measurements_pivot (fid INTEGER PRIMARY KEY, {measure_sql});'
        curs.execute(sql)
        conn.commit()

        measure_names_sql = ', '.join([sql_name(measurement["name"]) for measurement in measurements.values()])
        measure_values_sql = ", ".join([f"{sql_round(measurement['data_type'], measurement['measurement_id'],'measurement')} {sql_name(measurement['name'])}" for measurement in measurements.values()])
        sql = f'INSERT INTO measurements_pivot (fid, {measure_names_sql}) SELECT M.dgo_id, {measure_values_sql} FROM measurement_values M GROUP BY M.dgo_id;'
        curs.execute(sql)
        conn.commit()

        # Create measure view
        measure_names_sql = ", ".join([f"M.{sql_name(measurement['name'])} {sql_name(measurement['name'])}" for measurement in measurements.values()])
        sql = f'CREATE VIEW vw_measurements AS SELECT G.fid fid, G.geom geom, G.LevelPathI level_path, G.seg_distance seg_distance, {measure_names_sql} FROM vbet_segments G INNER JOIN measurements_pivot M ON M.fid = G.fid;'
        curs.execute(sql)

        # Add view to geopackage
        curs.execute("INSERT INTO gpkg_contents (table_name, identifier, data_type, srs_id) VALUES ('vw_point_metrics', 'vw_point_metrics', 'features', ?);", (epsg,))
        curs.execute("INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) values ('vw_point_metrics', 'geom', 'POINT', ?, 0, 0);", (epsg,))
        curs.execute("INSERT INTO gpkg_contents (table_name, identifier, data_type, srs_id) VALUES ('vw_dgo_metrics', 'vw_dgo_metrics', 'features', ?);", (epsg,))
        curs.execute("INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) values ('vw_dgo_metrics', 'geom', 'POLYGON', ?, 0, 0);", (epsg,))
        curs.execute("INSERT INTO gpkg_contents (table_name, identifier, data_type, srs_id) VALUES ('vw_measurements', 'vw_measurements', 'features', ?);", (epsg,))
        curs.execute("INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) values ('vw_measurements', 'geom', 'POLYGON', ?, 0, 0);", (epsg,))
        conn.commit()

    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['RME_OUTPUTS'])

    # Write a report
    report_path = os.path.join(project.project_dir, LayerTypes['REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)
    report = RMEReport(outputs_gpkg, report_path, project)
    report.write()

    progbar.finish()
    log.info('Riverscapes Metric Engine Finished')
    return


def sql_name(name: str) -> str:
    """return cleaned metric column name"""
    return name.lower().replace(' ', '_')


def sql_round(datatype: str, metric_id, table='metric') -> str:
    """return round function"""
    return f"{'ROUND(' if datatype == 'REAL' else ''}SUM(M.{table}_value) FILTER (WHERE M.{table}_id == {metric_id}){', 4)' if datatype == 'REAL' else ''}"


def generate_metric_list(database: Path, source_table: str = 'metrics') -> dict:
    """_summary_

    Args:
        database (Path): path to output rme database
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


def main():
    """Run Riverscapes Metric Engine"""

    parser = argparse.ArgumentParser(description='Riverscapes Metric Engine')

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
    parser.add_argument('--confinement_dgos', help='confinement dgos', type=str)
    parser.add_argument('--anthro_dgos', help='anthro dgos', type=str)
    parser.add_argument('--rcat_dgos', help='rcat_dgos', type=str)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help="(optional) save intermediate outputs for debugging", action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("Riverscapes Metric Engine")
    log.setup(logPath=os.path.join(args.output_folder, "rme.log"), verbose=args.verbose)
    log.title(f'Riverscapes Metrics For HUC: {args.huc}')

    meta = parse_metadata(args.meta)
    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_folder, 'rme_mem.log')
            retcode, max_obj = ThreadRun(metric_engine, memfile,
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
                                         args.confinement_dgos,
                                         args.anthro_dgos,
                                         args.rcat_dgos,
                                         meta=meta)
            log.debug(f'Return code: {retcode}, [Max process usage] {max_obj}')

        else:
            metric_engine(args.huc,
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
                          args.confinement_dgos,
                          args.anthro_dgos,
                          args.rcat_dgos,
                          meta=meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
