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

from osgeo import ogr, osr
from osgeo import gdal
import numpy as np
import rasterio
from rasterio.mask import mask
from shapely.geometry import Point

from rscommons import GeopackageLayer, dotenv, Logger, initGDALOGRErrors, ModelConfig, RSLayer, RSMeta, RSMetaTypes, RSProject, VectorBase, ProgressBar
from rscommons.classes.vector_base import get_utm_zone_epsg
from rscommons.util import parse_metadata, pretty_duration
from rscommons.database import load_lookup_data
from rscommons.geometry_ops import reduce_precision, get_endpoints
from rscommons.vector_ops import copy_feature_class, collect_linestring
from rscommons.vbet_network import copy_vaa_attributes, join_attributes
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions
from rscommons.moving_window import moving_window_dgo_ids

from rme.__version__ import __version__
from rme.analysis_window import AnalysisLine
from rme.rme_report import RMEReport, FILTER_NAMES
from rme.utils.check_vbet_inputs import vbet_inputs

Path = str

initGDALOGRErrors()
gdal.UseExceptions()

cfg = ModelConfig(
    'https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(
    os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    # key: (name, id, tag, relpath)]
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'FLOWLINES': RSLayer('Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        # 'OWNERSHIP': RSLayer('Ownership', 'OWNERSHIP', 'Vector', 'ownership'),
        # 'STATES': RSLayer('States', 'STATES', 'Vector', 'states'),
        'COUNTIES': RSLayer('Counties', 'COUNTIES', 'Vector', 'counties'),
        'VBET_DGOS': RSLayer('Vbet DGOs', 'VBET_DGOS', 'Vector', 'vbet_dgos'),
        'VBET_IGOS': RSLayer('Vbet IGOs', 'VBET_IGOS', 'Vector', 'vbet_igos'),
        'VBET_CENTERLINES': RSLayer('VBET Centerline', 'VBET_CENTERLINE', 'Vector', 'valley_centerlines'),
        # 'ECOREGIONS': RSLayer('Ecoregions', 'ECOREGIONS', 'Vector', 'ecoregions'),
        # 'ROADS': RSLayer('Roads', 'Roads', 'Vector', 'roads'),
        # 'RAIL': RSLayer('Rail', 'Rail', 'Vector', 'rail'),
        # add these dynamically if they exist
        # 'CONFINEMENT_DGO': RSLayer('Confinement DGO', 'CONFINEMENT_DGO', 'Vector', 'confinement_dgo'),
        # 'ANTHRO_DGO': RSLayer('Anthropogenic DGO', 'ANTHRO_DGO', 'Vector', 'anthro_dgo'),
        # 'RCAT_DGO': RSLayer('RCAT DGO', 'RCAT_DGO', 'Vector', 'rcat_dgo')
    }),
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'inputs/dem.tif'),
    'HILLSHADE': RSLayer('Hillshade', 'HILLSHADE', 'Raster', 'inputs/hillshade.tif'),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/rme_intermediates.gpkg', {
        'JUNCTION_POINTS': RSLayer('Junction Points', 'JUNCTION_POINTS', 'Vector', 'junction_points'),
        'RME_DGO': RSLayer('RME DGO', 'RME_DGO', 'Vector', 'dgos'),
        'RME_IGO': RSLayer('RME IGO', 'RME_IGO', 'Vector', 'igos'),
        'DGO_METRIC_VIEW': RSLayer('DGO Metrics', 'DGO_METRICS', 'Vector', 'vw_dgo_metrics'),
        'IGO_METRIC_VIEW': RSLayer('IGO Metrics', 'IGO_METRICS', 'Vector', 'vw_igo_metrics'),
        'DGO_MEASUREMENTS': RSLayer('DGO Measurements', 'DGO_MEASUREMENTS', 'Vector', 'vw_measurements')
    }),
    'RME_OUTPUTS': RSLayer('Riverscapes Metrics', 'RME_OUTPUTS', 'Geopackage', 'outputs/riverscapes_metrics.gpkg', {
        'DGO_METRICS': RSLayer('RME DGO', 'RME_DGO', 'Vector', 'rme_dgos'),
        'IGO_METRICS': RSLayer('RME IGO', 'RME_IGO', 'Vector', 'rme_igos'),
    }),
    'REPORT': RSLayer('RME Report', 'REPORT', 'HTMLFile', 'outputs/rme.html'),
    'REPORT_PERENNIAL': RSLayer('RME Perennial Streams Report', 'REPORT_PERENNIAL', 'HTMLFile', 'outputs/rme_perennial.html'),
    'REPORT_PUBLIC_PERENNIAL': RSLayer('RME Public Perennial Streams Report', 'REPORT_PUBLIC_PERENNIAL', 'HTMLFile', 'outputs/rme_public_perennial.html'),
    'REPORT_BLM_LANDS': RSLayer('RME BLM Lands Report', 'REPORT_BLM_LANDS', 'HTMLFile', 'outputs/rme_blm_lands.html'),
    'REPORT_BLM_PERENNIAL': RSLayer('RME BLM Perennial Report', 'REPORT_BLM_PERENNIAL', 'HTMLFile', 'outputs/rme_blm_perennial.html'),
    'REPORT_USFS_PERENNIAL': RSLayer('RME USFS Perennial Report', 'REPORT_USFS_PERENNIAL', 'HTMLFile', 'outputs/rme_usfs_perennial.html'),
    'REPORT_NPS_PERENNIAL': RSLayer('RME NPS Perennial Report', 'REPORT_NPS_PERENNIAL', 'HTMLFile', 'outputs/rme_nps_perennial.html'),
    'REPORT_ST_PERENNIAL': RSLayer('RME ST Perennial Report', 'REPORT_ST_PERENNIAL', 'HTMLFile', 'outputs/rme_st_perennial.html'),
    'REPORT_FWS_PERENNIAL': RSLayer('RME FWS Perennial Report', 'REPORT_FWS_PERENNIAL', 'HTMLFile', 'outputs/rme_fws_perennial.html'),
}

stream_size_lookup = {0: 'small', 1: 'medium',
                      2: 'large', 3: 'very large', 4: 'huge'}
gradient_buffer_lookup = {'small': 25.0, 'medium': 50.0, 'large': 100.0,
                          'very large': 100.0, 'huge': 100.0}  # should this go as high as it does
window_distance = {'0': 200.0, '1': 400.0,
                   '2': 1200.0, '3': 2000.0, '4': 8000.0}


def metric_engine(huc: int, in_flowlines: Path, in_vaa_table: Path, in_counties: Path, in_segments: Path, in_points: Path,
                  in_vbet_centerline: Path, in_dem: Path, in_hillshade: Path, project_folder: Path,
                  in_confinement_dgos: Path = None, in_anthro_dgos: Path = None, in_rcat_dgos: Path = None, in_brat_dgos: Path = None,
                  level_paths: list = None, meta: dict = None):
    """Generate Riverscapes Metric Engine project and calculate metrics

    Args:
        huc (int): NHD huc
        in_flowlines (Path): NHD flowlines
        in_vaa_table (Path): NHD vaa table
        in_segments (Path): vbet segmented polygons
        in_points (Path): vbet segmentation points
        in_vbet_centerline (Path): vbet centerlines
        in_dem (Path): input dem raster
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
        vbin = vbet_inputs(os.path.dirname(os.path.dirname(
            os.path.dirname(in_segments))), project_dgos)

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

    augment_layermeta('rme', LYR_DESCRIPTIONS_JSON, LayerTypes)

    start_time = time.time()

    project_name = f'Riverscapes Metrics for HUC {huc}'
    project = RSProject(cfg, project_folder)
    project.create(
        project_name,
        'rs_metric_engine',
        [
            RSMeta('Model Documentation', 'https://tools.riverscapes.net/rme', RSMetaTypes.URL, locked=True),
            RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
            RSMeta('Hydrologic Unit Code', str(huc), locked=True),
            RSMeta('RME Version', cfg.version, locked=True),
            RSMeta('RME Timestamp', str(int(time.time())), RSMetaTypes.TIMESTAMP, locked=True)
        ],
        meta
    )

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=[
                                                       'Inputs', 'Intermediates', 'Outputs'], create_folders=True)

    inputs_gpkg = os.path.join(project_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg = os.path.join(project_folder, LayerTypes['INTERMEDIATES'].rel_path)
    outputs_gpkg = os.path.join(project_folder, LayerTypes['RME_OUTPUTS'].rel_path)
    GeopackageLayer.delete(inputs_gpkg)
    GeopackageLayer.delete(intermediates_gpkg)
    GeopackageLayer.delete(outputs_gpkg)

    flowlines = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    copy_feature_class(in_flowlines, flowlines)
    counties_f = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['COUNTIES'].rel_path)
    copy_feature_class(in_counties, counties_f)
    segments = os.path.join(intermediates_gpkg, LayerTypes['INTERMEDIATES'].sub_layers['RME_DGO'].rel_path)
    copy_feature_class(in_segments, os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['VBET_DGOS'].rel_path))
    copy_feature_class(in_segments, segments)
    points = os.path.join(intermediates_gpkg, LayerTypes['INTERMEDIATES'].sub_layers['RME_IGO'].rel_path)
    copy_feature_class(in_points, os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['VBET_IGOS'].rel_path))
    copy_feature_class(in_points, points)
    centerlines = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['VBET_CENTERLINES'].rel_path)
    copy_feature_class(in_vbet_centerline, centerlines)
    _dem_node, dem = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['DEM'], in_dem)
    _hs_node, hillshade = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], in_hillshade)

    in_gpkg_node, *_ = project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    if in_confinement_dgos:
        confinement_dgos = os.path.join(inputs_gpkg, 'confinement_dgo')
        copy_feature_class(in_confinement_dgos, confinement_dgos)
        project.add_dataset(in_gpkg_node.find('Layers'), 'confinement_dgo', RSLayer('Confinement DGO', 'CONFINEMENT_DGO', 'Vector', 'confinement_dgo'), 'Vector', rel_path=True, sublayer=True)
    else:
        confinement_dgos = None
    if in_anthro_dgos:
        anthro_dgos = os.path.join(inputs_gpkg, 'anthro_dgo')
        copy_feature_class(in_anthro_dgos, anthro_dgos)
        project.add_dataset(in_gpkg_node.find('Layers'), 'anthro_dgo', RSLayer('Anthropogenic DGO', 'ANTHRO_DGO', 'Vector', 'anthro_dgo'), 'Vector', rel_path=True, sublayer=True)
    else:
        anthro_dgos = None
    if in_rcat_dgos:
        rcat_dgos = os.path.join(inputs_gpkg, 'rcat_dgo')
        copy_feature_class(in_rcat_dgos, rcat_dgos)
        project.add_dataset(in_gpkg_node.find('Layers'), 'rcat_dgo', RSLayer('RCAT DGO', 'RCAT_DGO', 'Vector', 'rcat_dgo'), 'Vector', rel_path=True, sublayer=True)
    else:
        rcat_dgos = None
    if in_brat_dgos:
        brat_dgos = os.path.join(inputs_gpkg, 'brat_dgo')
        copy_feature_class(in_brat_dgos, brat_dgos)
        project.add_dataset(in_gpkg_node.find('Layers'), 'brat_dgo', RSLayer('BRAT DGO', 'BRAT_DGO', 'Vector', 'brat_network'), 'Vector', rel_path=True, sublayer=True)
    else:
        brat_dgos = None

    # get utm
    with GeopackageLayer(points) as lyr_pts:
        feat = lyr_pts.ogr_layer.GetNextFeature()
        geom = feat.GetGeometryRef()
        utm_epsg = get_utm_zone_epsg(geom.GetPoint(0)[0])

    vaa_table_name = copy_vaa_attributes(flowlines, in_vaa_table)
    line_network = join_attributes(inputs_gpkg, "vw_flowlines_vaa", os.path.basename(flowlines), vaa_table_name, 'NHDPlusID', [
                                   'STARTFLAG', 'DnDrainCou', 'RtnDiv'], 4326)

    # Prepare Junctions
    junctions = os.path.join(
        intermediates_gpkg, LayerTypes['INTERMEDIATES'].sub_layers['JUNCTION_POINTS'].rel_path)
    with GeopackageLayer(junctions, write=True) as lyr_points, \
            GeopackageLayer(line_network) as lyr_lines:
        srs = lyr_lines.spatial_ref
        lyr_points.create_layer(ogr.wkbPoint, spatial_ref=srs, fields={
                                'JunctionType': ogr.OFTString})
        lyr_points_defn = lyr_points.ogr_layer_def
        # Generate diffluence/confluence nodes
        for attribute, sql in [('Diffluence', '"DnDrainCou" > 1'), ('Confluence', '"RtnDiv" > 0')]:
            for feat, *_ in lyr_lines.iterate_features(attribute_filter=sql):
                geom = feat.GetGeometryRef()
                pnt = geom.GetPoint(0) if attribute == 'Confluence' else geom.GetPoint(
                    geom.GetPointCount() - 1)
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

    database_folder = os.path.join(os.path.abspath(
        os.path.dirname(__file__)), 'database')
    with sqlite3.connect(intermediates_gpkg) as conn:
        cursor = conn.cursor()
        with open(os.path.join(database_folder, 'metrics_schema.sql'), encoding='utf-8') as sqlfile:
            sql_commands = sqlfile.read()
            cursor.executescript(sql_commands)
            conn.commit()
    # Load tables
    load_lookup_data(intermediates_gpkg, os.path.join(
        database_folder, 'data_metrics'))

    # index level path and seg distance
    with sqlite3.connect(intermediates_gpkg) as conn:
        curs = conn.cursor()
        curs.execute("CREATE INDEX ix_dgos_level_path_seg_distance ON dgos (level_path, seg_distance)")
        curs.execute("CREATE INDEX idx_igos_size ON igos (stream_size)")
        curs.execute("CREATE INDEX ix_dgos_fcode ON dgos (FCode)")
        curs.execute("CREATE INDEX ix_igos_level_path_seg_distance ON igos (level_path, seg_distance)")
        curs.execute("CREATE INDEX idx_igos_fcode ON igos (FCode)")
        conn.commit()

    # Generate the list of level paths to run, sorted by ascending order and optional user filter
    level_paths_to_run = []
    with GeopackageLayer(line_network) as line_lyr:
        for feat, *_ in line_lyr.iterate_features():
            level_path = feat.GetField('level_path')
            level_paths_to_run.append(str(int(level_path)))
    level_paths_to_run = list(set(level_paths_to_run))
    if level_paths:
        level_paths_to_run = [
            level_path for level_path in level_paths_to_run if level_path in level_paths]
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
            level_path = feat.GetField('level_path')
            seg_distance = feat.GetField('seg_distance')
            sql = f'level_path = {level_path} and seg_distance = {seg_distance}'
            for feat_seg, *_ in lyr_segments.iterate_features(attribute_filter=sql):
                igo_dgo[igo_id] = feat_seg.GetFID()
                break

    metrics = generate_metric_list(intermediates_gpkg)
    measurements = generate_metric_list(intermediates_gpkg, 'measurements')

    buffer_distance = {}
    for stream_size, distance in gradient_buffer_lookup.items():
        buffer = VectorBase.rough_convert_metres_to_raster_units(dem, distance)
        buffer_distance[stream_size] = buffer

    progbar = ProgressBar(len(level_paths_to_run), 50,
                          "Calculating Riverscapes Metrics")
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

            geom_flowline = collect_linestring(line_network, f'level_path = {level_path}')
            if geom_flowline.IsEmpty():
                log.error(
                    f'Flowline for level path {level_path} is empty geometry')
                continue

            geom_centerline = collect_linestring(
                centerlines, f'level_path = {level_path}', precision=8)

            for feat_seg_dgo, *_ in lyr_segments.iterate_features(attribute_filter=f'level_path = {level_path}'):
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
                    for pt_ftr, *_ in lyr_points.iterate_features(attribute_filter=f'level_path = {level_path} and seg_distance = {segment_distance}'):
                        stream_size_id = pt_ftr.GetField('stream_size')
                        break
                if not 'stream_size_id' in locals():
                    log.warning(f'Unable to find stream size for dgo {dgo_id} in level path {level_path}')
                    stream_size_id = 0

                stream_size = stream_size_lookup[stream_size_id]
                # window_geoms = {}  # Different metrics may require different windows. Store generated windows here for reuse.
                metrics_output = {}
                measurements_output = {}
                min_elev = None
                max_elev = None

                # Calculate each metric if it is active
                if 'STRMGRAD' in metrics:
                    metric = metrics['STRMGRAD']

                    stream_length, min_elev, max_elev = get_segment_measurements(
                        geom_flowline, src_dem, feat_geom, buffer_distance[stream_size], transform)
                    measurements_output[measurements['STRMMINELEV']['measurement_id']] = min_elev
                    measurements_output[measurements['STRMMAXELEV']['measurement_id']] = max_elev
                    measurements_output[measurements['STRMLENG']['measurement_id']] = stream_length

                    gradient = None if any(value is None for value in [max_elev, min_elev]) else str(
                        (max_elev - min_elev) / stream_length)
                    metrics_output[metric['metric_id']] = gradient

                if 'VALGRAD' in metrics:
                    metric = metrics['VALGRAD']

                    centerline_length, *_ = get_segment_measurements(
                        geom_centerline, src_dem, feat_geom, buffer_distance[stream_size], transform)
                    measurements_output[measurements['VALLENG']['measurement_id']] = centerline_length

                    if any(elev is None for elev in [min_elev, max_elev]):
                        _, min_elev, max_elev = get_segment_measurements(
                            geom_flowline, dem, feat_geom, buffer_distance[stream_size], transform)
                        measurements_output[measurements['STRMMINELEV']['measurement_id']] = min_elev
                        measurements_output[measurements['STRMMAXELEV']['measurement_id']] = max_elev

                    gradient = None if any(value is None for value in [max_elev, min_elev]) else str(
                        (max_elev - min_elev) / centerline_length)
                    metrics_output[metric['metric_id']] = gradient

                if 'STRMORDR' in metrics:
                    metric = metrics['STRMORDR']

                    results = []
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                            results.append(feat.GetField('stream_order'))
                        lyr_lines.ogr_layer.SetSpatialFilter(None)
                    if len(results) > 0:
                        stream_order = str(max(results))
                    else:
                        stream_order = None
                        log.warning(
                            f'Unable to calculate Stream Order for dgo {dgo_id} in level path {level_path}')
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
                            sum_attributes[attribute] = sum_attributes.get(
                                attribute, 0) + length
                        lyr_lines.ogr_layer.SetSpatialFilter(None)
                        lyr_lines = None
                    if sum(sum_attributes.values()) == 0:
                        is_headwater = None
                    else:
                        is_headwater = str(1) if sum_attributes.get('1', 0) / sum(sum_attributes.values()) > 0.5 else str(0)
                    metrics_output[metric['metric_id']] = is_headwater

                if 'STRMTYPE' in metrics:
                    metric = metrics['STRMTYPE']

                    # attributes = {}
                    # with GeopackageLayer(line_network) as lyr_lines:
                    #     for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                    #         line_geom = feat.GetGeometryRef()
                    #         attribute = str(feat.GetField('FCode'))
                    #         geom_section = feat_geom.Intersection(line_geom)
                    #         length = geom_section.Length()
                    #         attributes[attribute] = attributes.get(attribute, 0) + length
                    #     lyr_lines.ogr_layer.SetSpatialFilter(None)
                    #     lyr_lines = None
                    # if len(attributes) == 0:
                    #     majority_fcode = None
                    # else:
                    #     majority_fcode = str(max(attributes, key=attributes.get))
                    fcode = feat_seg_dgo.GetField('FCode')
                    metrics_output[metric['metric_id']] = str(fcode)

                if 'STRMLENGTH' in metrics:
                    metric = metrics['STRMLENGTH']

                    with GeopackageLayer(line_network) as lyr_lines:
                        leng = 0
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                            geom_flowline_full = feat.GetGeometryRef()
                            feat_section = geom_flowline_full.Intersection(feat_geom)
                            section_proj = VectorBase.ogr2shapely(feat_section, transform=transform)
                            leng += section_proj.length
                        lyr_lines = None
                    metrics_output[metric['metric_id']] = str(leng)

                if 'ACTFLDAREA' in metrics:
                    metric = metrics['ACTFLDAREA']

                    afp_area = feat_seg_dgo.GetField('low_lying_floodplain_area') if feat_seg_dgo.GetField(
                        'low_lying_floodplain_area') is not None else 0.0
                    metrics_output[metric['metric_id']] = str(afp_area)

                if 'INACTFLDAREA' in metrics:
                    metric = metrics['INACTFLDAREA']

                    ifp_area = feat_seg_dgo.GetField('elevated_floodplain_area') if feat_seg_dgo.GetField(
                        'elevated_floodplain_area') is not None else 0.0
                    metrics_output[metric['metric_id']] = str(ifp_area)

                if 'ACTCHANAREA' in metrics:
                    metric = metrics['ACTCHANAREA']

                    ac_area = feat_seg_dgo.GetField('active_channel_area') if feat_seg_dgo.GetField(
                        'active_channel_area') is not None else 0.0
                    metrics_output[metric['metric_id']] = str(ac_area)

                if 'FLDPLNAREA' in metrics:
                    metric = metrics['FLDPLNAREA']

                    fp_area = feat_seg_dgo.GetField('floodplain_area') if feat_seg_dgo.GetField(
                        'floodplain_area') is not None else 0.0
                    metrics_output[metric['metric_id']] = str(fp_area)

                if 'INTGWDTH' in metrics:
                    metric = metrics['INTGWDTH']

                    # ig_width = str(feat_seg_dgo.GetField('segment_area') / feat_seg_dgo.GetField(
                    #     'centerline_length')) if feat_seg_dgo.GetField('centerline_length') is not None else None
                    ig_width = feat_seg_dgo.GetField('integrated_width')
                    metrics_output[metric['metric_id']] = str(ig_width)

                if 'CHANVBRAT' in metrics:
                    metric = metrics['CHANVBRAT']

                    ac_ratio = feat_seg_dgo.GetField('active_channel_prop') if feat_seg_dgo.GetField(
                        'active_channel_prop') is not None else 0.0
                    metrics_output[metric['metric_id']] = str(ac_ratio)

                if 'LOWLYVBRAT' in metrics:
                    metric = metrics['LOWLYVBRAT']

                    lowly_ratio = feat_seg_dgo.GetField(
                        'low_lying_floodplain_prop') if feat_seg_dgo.GetField('segment_area') > 0.0 else None
                    metrics_output[metric['metric_id']] = lowly_ratio

                if 'ELEVATEDVBRAT' in metrics:
                    metric = metrics['ELEVATEDVBRAT']

                    elevated_ratio = feat_seg_dgo.GetField('elevated_floodplain_prop') if feat_seg_dgo.GetField(
                        'elevated_floodplain_prop') is not None else 0.0
                    metrics_output[metric['metric_id']] = str(elevated_ratio)

                if 'FLDVBRAT' in metrics:
                    metric = metrics['FLDVBRAT']

                    fp_ratio = feat_seg_dgo.GetField('floodplain_prop') if feat_seg_dgo.GetField(
                        'floodplain_prop') is not None else 0.0
                    metrics_output[metric['metric_id']] = str(fp_ratio)

                if 'ACRESVBPM' in metrics:
                    metric = metrics['ACRESVBPM']

                    ac_mi = str((feat_seg_dgo.GetField('segment_area') * 0.000247105) / (feat_seg_dgo.GetField(
                        'centerline_length') * 0.000621371)) if feat_seg_dgo.GetField('centerline_length') is not None else None
                    metrics_output[metric['metric_id']] = ac_mi

                if 'HECTVBPKM' in metrics:
                    metric = metrics['HECTVBPKM']

                    ac_km = str((feat_seg_dgo.GetField('segment_area') * 0.0001) / (feat_seg_dgo.GetField(
                        'centerline_length') * 0.001)) if feat_seg_dgo.GetField('centerline_length') is not None else None
                    metrics_output[metric['metric_id']] = ac_km

                if 'RELFLWLNGTH' in metrics:
                    metric = metrics['RELFLWLNGTH']

                    geom_flowline_full = collect_linestring(line_network, f'level_path = {level_path}')
                    stream_length_total, *_ = get_segment_measurements(
                        geom_flowline_full, src_dem, feat_geom, buffer_distance[stream_size], transform)
                    centerline_length, *_ = get_segment_measurements(
                        geom_centerline, src_dem, feat_geom, buffer_distance[stream_size], transform)

                    relative_flow_length = str(
                        stream_length_total / centerline_length) if centerline_length > 0.0 else None
                    metrics_output[metric['metric_id']] = relative_flow_length

                if 'STRMSIZE' in metrics:
                    metric = metrics['STRMSIZE']

                    stream_length, *_ = get_segment_measurements(
                        geom_flowline, src_dem, feat_geom, buffer_distance[stream_size], transform)

                    stream_size_metric = str(feat_seg_dgo.GetField(
                        'active_channel_area') / stream_length) if stream_length > 0.0 else None
                    metrics_output[metric['metric_id']] = stream_size_metric

                if 'ECORGIII' in metrics:
                    metric = metrics['ECORGIII']

                    attributes = {}
                    # with GeopackageLayer(ecoregions) as lyr_ecoregions:
                    #     for feat, *_ in lyr_ecoregions.iterate_features(clip_shape=feat_geom):
                    #         geom_ecoregion = feat.GetGeometryRef()
                    #         attribute = str(feat.GetField('US_L3CODE'))
                    #         geom_section = feat_geom.Intersection(
                    #             geom_ecoregion)
                    #         area = geom_section.GetArea()
                    #         attributes[attribute] = attributes.get(
                    #             attribute, 0) + area
                    #     lyr_ecoregions = None
                    # if len(attributes) == 0:
                    #     log.warning(
                    #         f'Unable to find majority ecoregion III for pt {dgo_id} in level path {level_path}')
                    #     majority_attribute = None
                    # else:
                    #     majority_attribute = str(
                    #         max(attributes, key=attributes.get))
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                            geom_ecoregion = feat.GetGeometryRef()
                            attribute = feat.GetField('ecoregion_iii')
                            geom_section = feat_geom.Intersection(geom_ecoregion)
                            length = geom_section.Length()
                            attributes[attribute] = attributes.get(
                                attribute, 0) + length
                    if len(attributes) == 0:
                        log.warning(f'Unable to find majority ecoregion III for dgo {dgo_id} in level path {level_path}')
                        majority_attribute = None
                    else:
                        majority_attribute = str(max(attributes, key=attributes.get))
                    metrics_output[metric['metric_id']] = majority_attribute

                if 'ECORGIV' in metrics:
                    metric = metrics['ECORGIV']

                    attributes = {}
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                            geom_ecoregion = feat.GetGeometryRef()
                            attribute = feat.GetField('ecoregion_iv')
                            geom_section = feat_geom.Intersection(geom_ecoregion)
                            length = geom_section.Length()
                            attributes[attribute] = attributes.get(
                                attribute, 0) + length
                    if len(attributes) == 0:
                        log.warning(f'Unable to find majority ecoregion IV for dgo {dgo_id} in level path {level_path}')
                        majority_attribute = None
                    else:
                        majority_attribute = str(max(attributes, key=attributes.get))
                    metrics_output[metric['metric_id']] = majority_attribute

                if 'CONF' in metrics:
                    metric = metrics['CONF']

                    with GeopackageLayer(junctions) as lyr_pts:
                        count = 0
                        for feat, *_ in lyr_pts.iterate_features(clip_shape=feat_geom, attribute_filter=""""JunctionType" = 'Confluence'"""):
                            count += 1
                        metrics_output[metric['metric_id']] = str(count)

                if 'DIFF' in metrics:
                    metric = metrics['DIFF']

                    with GeopackageLayer(junctions) as lyr_pts:
                        count = 0
                        for feat, *_ in lyr_pts.iterate_features(clip_shape=feat_geom, attribute_filter=""""JunctionType" = 'Diffluence'"""):
                            count += 1
                        metrics_output[metric['metric_id']] = str(count)

                if 'TRIBS' in metrics:
                    metric = metrics['TRIBS']

                    with GeopackageLayer(junctions) as lyr_pts:
                        count = 0
                        for feat, *_ in lyr_pts.iterate_features(clip_shape=feat_geom, attribute_filter=""""JunctionType" = 'Tributary'"""):
                            count += 1
                        metrics_output[metric['metric_id']] = str(count)

                if 'CHANSIN' in metrics:
                    metric = metrics['CHANSIN']

                    line = AnalysisLine(geom_flowline, feat_geom)
                    measurements_output[measurements['STRMSTRLENG']['measurement_id']] = line.endpoint_distance
                    sin = str(line.sinuosity()) if line.sinuosity(
                    ) is not None else None
                    metrics_output[metric['metric_id']] = sin

                if 'DRAINAREA' in metrics:
                    metric = metrics['DRAINAREA']

                    results = []
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                            results.append(feat.GetField('DivDASqKm'))
                    if len(results) > 0:
                        drainage_area = str(max(results))
                    else:
                        drainage_area = None
                        log.warning(f'Unable to calculate drainage area for dgo {dgo_id} in level path {level_path}')
                    metrics_output[metric['metric_id']] = drainage_area

                if 'VALAZMTH' in metrics:
                    metric = metrics['VALAZMTH']

                    cline = AnalysisLine(geom_centerline, feat_geom)
                    az = str(cline.azimuth()) if cline.azimuth() is not None else None
                    metrics_output[metric['metric_id']] = az

                if 'CNFMT' in metrics and confinement_dgos:
                    metric = metrics['CNFMT']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT confinement_ratio FROM confinement_dgo WHERE fid = {dgo_id}")
                        conf_ratio = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(conf_ratio)

                if 'CONST' in metrics and confinement_dgos:
                    metric = metrics['CONST']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT constriction_ratio FROM confinement_dgo WHERE fid = {dgo_id}")
                        cons_ratio = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(cons_ratio)

                if 'CONFMARG' in metrics and confinement_dgos:
                    metric = metrics['CONFMARG']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT confin_leng FROM confinement_dgo WHERE fid = {dgo_id}")
                        conf_margin = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(conf_margin)

                if 'ROADDENS' in metrics and anthro_dgos:
                    metric = metrics['ROADDENS']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT Road_len, centerline_length FROM anthro_dgo WHERE fid = {dgo_id}")
                        roadd = curs.fetchone()
                        if roadd[0] is not None and roadd[1] is not None:
                            road_density = roadd[0] / \
                                roadd[1] if roadd[1] > 0.0 else None
                            metrics_output[metric['metric_id']] = str(road_density)
                        else:
                            road_density = None
                            metrics_output[metric['metric_id']] = None

                if 'RAILDENS' in metrics and anthro_dgos:
                    metric = metrics['RAILDENS']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT Rail_len, centerline_length FROM anthro_dgo WHERE fid = {dgo_id}")
                        raild = curs.fetchone()
                        if raild[0] is not None and raild[1] is not None:
                            rail_density = raild[0] / \
                                raild[1] if raild[1] > 0.0 else None
                            metrics_output[metric['metric_id']] = str(rail_density)
                        else:
                            rail_density = None
                            metrics_output[metric['metric_id']] = None

                if 'LUI' in metrics and anthro_dgos:
                    metric = metrics['LUI']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT LUI FROM anthro_dgo WHERE fid = {dgo_id}")
                        lui = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(lui)

                if 'FPACCESS' in metrics and rcat_dgos:
                    metric = metrics['FPACCESS']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT FloodplainAccess FROM rcat_dgo WHERE fid = {dgo_id}")
                        fp_access = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(fp_access)

                if 'AGENCY' in metrics:
                    metric = metrics['AGENCY']

                    agencies = {}
                    # with GeopackageLayer(ownership) as lyr:
                    #     for feat, *_ in lyr.iterate_features(clip_shape=feat_geom):
                    #         geom_agency = feat.GetGeometryRef()
                    #         attribute = feat.GetField('ADMIN_AGEN')
                    #         geom_section = feat_geom.Intersection(geom_agency)
                    #         area = geom_section.GetArea()
                    #         agencies[attribute] = agencies.get(
                    #             attribute, 0) + area
                    #     lyr = None
                    # if len(agencies) == 0:
                    #     log.warning(
                    #         f'Unable to find majority agency for pt {dgo_id} in level path {level_path}')
                    #     majority_agency = None
                    # else:
                    #     majority_agency = str(max(agencies, key=agencies.get))
                    # metrics_output[metric['metric_id']] = majority_agency
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                            geom_agency = feat.GetGeometryRef()
                            attribute = feat.GetField('ownership')
                            geom_section = feat_geom.Intersection(geom_agency)
                            length = geom_section.Length()
                            agencies[attribute] = agencies.get(attribute, 0) + length
                    if len(agencies) == 0:
                        log.warning(f'Unable to find majority agency for dgo {dgo_id} in level path {level_path}')
                        majority_agency = None
                    else:
                        majority_agency = str(max(agencies, key=agencies.get))
                    metrics_output[metric['metric_id']] = majority_agency

                if 'STATE' in metrics:
                    metric = metrics['STATE']

                    states = {}
                    # with GeopackageLayer(states_f) as lyr:
                    #     for feat, *_ in lyr.iterate_features(clip_shape=feat_geom):
                    #         geom_state = feat.GetGeometryRef()
                    #         attribute = feat.GetField('NAME')
                    #         geom_section = feat_geom.Intersection(geom_state)
                    #         area = geom_section.GetArea()
                    #         states[attribute] = states.get(attribute, 0) + area
                    #     lyr = None
                    # if len(states) == 0:
                    #     log.warning(
                    #         f'Unable to find majority state for pt {dgo_id} in level path {level_path}')
                    #     majority_state = None
                    # else:
                    #     majority_state = str(max(states, key=states.get))
                    # metrics_output[metric['metric_id']] = majority_state
                    with GeopackageLayer(line_network) as lyr_lines:
                        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
                            geom_state = feat.GetGeometryRef()
                            attribute = feat.GetField('us_state')
                            geom_section = feat_geom.Intersection(geom_state)
                            length = geom_section.Length()
                            states[attribute] = states.get(attribute, 0) + length
                    if len(states) == 0:
                        log.warning(f'Unable to find majority state for dgo {dgo_id} in level path {level_path}')
                        majority_state = None
                    else:
                        majority_state = str(max(states, key=states.get))
                    metrics_output[metric['metric_id']] = majority_state

                if 'COUNTY' in metrics:
                    metric = metrics['COUNTY']

                    counties = {}
                    with GeopackageLayer(counties_f) as lyr:
                        for feat, *_ in lyr.iterate_features(clip_shape=feat_geom):
                            geom_county = feat.GetGeometryRef()
                            attribute = feat.GetField('NAME')
                            geom_section = feat_geom.Intersection(geom_county)
                            area = geom_section.GetArea()
                            counties[attribute] = counties.get(
                                attribute, 0) + area
                        lyr = None
                    if len(counties) == 0:
                        log.warning(
                            f'Unable to find majority county for dgo {dgo_id} in level path {level_path}')
                        majority_county = None
                    else:
                        majority_county = str(max(counties, key=counties.get))
                    metrics_output[metric['metric_id']] = majority_county

                if 'PROP_RIP' in metrics:
                    metric = metrics['PROP_RIP']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT ExistingRiparianMean FROM rcat_dgo WHERE fid = {dgo_id}")
                        fp_access = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(fp_access)

                if 'RVD' in metrics:
                    metric = metrics['RVD']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT RiparianDeparture FROM rcat_dgo WHERE fid = {dgo_id}")
                        rvd = 1 - min(1, curs.fetchone()[0])
                    metrics_output[metric['metric_id']] = str(rvd)

                if 'AGCONV' in metrics:
                    metric = metrics['AGCONV']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT Agriculture FROM rcat_dgo WHERE fid = {dgo_id}")
                        ag_conv = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(ag_conv)

                if 'DEVEL' in metrics:
                    metric = metrics['DEVEL']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT Development FROM rcat_dgo WHERE fid = {dgo_id}")
                        devel = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(devel)

                if 'RIPCOND' in metrics:
                    metric = metrics['RIPCOND']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT Condition FROM rcat_dgo WHERE fid = {dgo_id}")
                        rip_cond = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(rip_cond)

                if 'BRATCAP' in metrics and brat_dgos:
                    metric = metrics['BRATCAP']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT oCC_EX FROM brat_dgo WHERE fid = {dgo_id}")
                        bratcap = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(bratcap)

                if 'BRATRISK' in metrics and brat_dgos:
                    metric = metrics['BRATRISK']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT Risk FROM brat_dgo WHERE fid = {dgo_id}")
                        bratrisk = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(bratrisk)

                if 'BRATOPP' in metrics and brat_dgos:
                    metric = metrics['BRATOPP']

                    with sqlite3.connect(inputs_gpkg) as conn:
                        curs = conn.cursor()
                        curs.execute(
                            f"SELECT Opportunity FROM brat_dgo WHERE fid = {dgo_id}")
                        bratopp = curs.fetchone()[0]
                    metrics_output[metric['metric_id']] = str(bratopp)

                # Write to Metrics
                if len(metrics_output) > 0:
                    lp_metrics[dgo_id] = metrics_output
                    # curs.executemany("INSERT INTO dgo_metric_values (dgo_id, metric_id, metric_value) VALUES (?,?,?)", [(dgo_id, name, value) for name, value in metrics_output.items()])
                if len(measurements_output) > 0:
                    lp_meas[dgo_id] = measurements_output
                    # curs.executemany("INSERT INTO measurement_values (dgo_id, measurement_id, measurement_value) VALUES (?,?,?)", [(dgo_id, name, value) for name, value in measurements_output.items()])

        with sqlite3.connect(intermediates_gpkg) as conn:
            curs = conn.cursor()
            for dgo_id, vals in lp_metrics.items():
                curs.executemany("INSERT INTO dgo_metric_values (dgo_id, metric_id, metric_value) VALUES (?,?,?)", [
                                 (dgo_id, name, value) for name, value in vals.items()])
            for dgo_id, vals in lp_meas.items():
                curs.executemany("INSERT INTO measurement_values (dgo_id, measurement_id, measurement_value) VALUES (?,?,?)", [
                                 (dgo_id, name, value) for name, value in vals.items()])
            conn.commit()

    # fill out igo_metrics table using moving window analysis
    progbar = ProgressBar(
        len(windows), 50, "Calculating Moving Window Metrics")
    counter = 0
    for igo_id, dgo_ids in windows.items():
        counter += 1
        progbar.update(counter)
        with sqlite3.connect(intermediates_gpkg) as conn:
            curs = conn.cursor()

            if igo_id in igo_dgo.keys():
                if 'STRMORDR' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['STRMORDR']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    stream_order = curs.fetchone()[0]
                    if stream_order is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['STRMORDR']['metric_id']}, {str(stream_order)})")

                if 'HEDWTR' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['HEDWTR']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    hw = curs.fetchone()[0]
                    if hw is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['HEDWTR']['metric_id']}, {str(hw)})")

                if 'STRMTYPE' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['STRMTYPE']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    fcode = curs.fetchone()[0]
                    if fcode is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['STRMTYPE']['metric_id']}, {str(fcode)})")

                if 'ACTFLDAREA' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['ACTFLDAREA']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    afp_area = curs.fetchone()[0]
                    if afp_area is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ACTFLDAREA']['metric_id']}, {str(afp_area)})")

                if 'INACTFLDAREA' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['INACTFLDAREA']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    ifp_area = curs.fetchone()[0]
                    if ifp_area is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['INACTFLDAREA']['metric_id']}, {str(ifp_area)})")

                if 'ACTCHANAREA' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['ACTCHANAREA']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    ac_area = curs.fetchone()[0]
                    if ac_area is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ACTCHANAREA']['metric_id']}, {str(ac_area)})")

                if 'FLDPLNAREA' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['FLDPLNAREA']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    fp_area = curs.fetchone()[0]
                    if fp_area is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['FLDPLNAREA']['metric_id']}, {str(fp_area)})")

                if 'STRMSIZE' in metrics:
                    curs.execute(
                        f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMSTRLENG']['measurement_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    stream_length = curs.fetchone()[0]
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['ACTCHANAREA']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    ac = curs.fetchone()[0]

                    stream_size_metric = None if any(value is None for value in [
                        ac, stream_length]) else float(ac) / float(stream_length)
                    if stream_size_metric is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['STRMSIZE']['metric_id']}, {str(stream_size_metric)})")

                if 'STRMLENGTH' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['STRMLENGTH']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    stream_length = curs.fetchone()[0]
                    if stream_length is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['STRMLENGTH']['metric_id']}, {str(stream_length)})")

                if 'ECORGIII' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['ECORGIII']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    ecor3 = curs.fetchone()[0]
                    if ecor3 is not None:
                        curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ECORGIII']['metric_id']}, '{str(ecor3)}')")

                if 'ECORGIV' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['ECORGIV']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    ecor4 = curs.fetchone()[0]
                    if ecor4 is not None:
                        curs.execute(f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ECORGIV']['metric_id']}, '{str(ecor4)}')")

                if 'CONF' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['CONF']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    count = curs.fetchone()[0]
                    if count is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CONF']['metric_id']}, {str(count)})")

                if 'DIFF' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['DIFF']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    count2 = curs.fetchone()[0]
                    if count2 is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['DIFF']['metric_id']}, {str(count2)})")

                if 'DRAINAREA' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['DRAINAREA']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    da = curs.fetchone()[0]
                    if da is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['DRAINAREA']['metric_id']}, {str(da)})")

                if 'CONFMARG' in metrics and confinement_dgos:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['CONFMARG']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    conf_margin = curs.fetchone()[0]
                    if conf_margin is not None:
                        curs.execute(
                            f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CONFMARG']['metric_id']}, {str(conf_margin)})")

                if 'STATE' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['STATE']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    state = curs.fetchone()[0]
                    if state is not None:
                        curs.execute("""INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES (?, ?, ?)""", (
                            igo_id, metrics['STATE']['metric_id'], str(state)))

                if 'COUNTY' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['COUNTY']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    county = curs.fetchone()[0]
                    if county is not None:
                        curs.execute("""INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES (?, ?, ?)""", (
                            igo_id, metrics['COUNTY']['metric_id'], str(county)))

                if 'AGENCY' in metrics:
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['AGENCY']['metric_id']} AND dgo_id = {igo_dgo[igo_id]}")
                    agency = curs.fetchone()[0]
                    if agency is not None:
                        curs.execute("""INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES (?, ?, ?)""", (
                            igo_id, metrics['AGENCY']['metric_id'], str(agency)))

            if 'STRMGRAD' in metrics:
                curs.execute(
                    f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMMINELEV']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                elevs = [float(row[0])
                         for row in curs.fetchall() if row[0] is not None]
                min_elev = min(elevs) if len(elevs) > 0 else None
                curs.execute(
                    f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMMAXELEV']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                elevs = [float(row[0])
                         for row in curs.fetchall() if row[0] is not None]
                max_elev = max(elevs) if len(elevs) > 0 else None
                curs.execute(
                    f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                stream_length = sum([float(row[0]) for row in curs.fetchall()])
                gradient = None if any(value is None for value in [
                                       max_elev, min_elev, stream_length]) else (max_elev - min_elev) / stream_length
                if gradient is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['STRMGRAD']['metric_id']}, {str(gradient)})")

            if 'VALGRAD' in metrics:
                curs.execute(
                    f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['VALLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                cl = [float(row[0])
                      for row in curs.fetchall() if row[0] is not None]
                centerline_length = sum(cl) if len(cl) > 0 else None
                if any(elev is None for elev in [min_elev, max_elev]):
                    curs.execute(
                        f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMMINELEV']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    elevs = [float(row[0])
                             for row in curs.fetchall() if row[0] is not None]
                    min_elev = min(elevs) if len(elevs) > 0 else None
                    curs.execute(
                        f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMMAXELEV']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    elevs = [float(row[0])
                             for row in curs.fetchall() if row[0] is not None]
                    max_elev = max(elevs) if len(elevs) > 0 else None
                gradient = None if any(value is None for value in [
                                       max_elev, min_elev, centerline_length]) else (max_elev - min_elev) / centerline_length
                if gradient is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['VALGRAD']['metric_id']}, {str(gradient)})")

            if 'INTGWDTH' in metrics:
                if centerline_length is None:
                    curs.execute(
                        f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['VALLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    cl = [float(row[0])
                          for row in curs.fetchall() if row[0] is not None]
                    centerline_length = sum(cl) if len(cl) > 0 else None
                curs.execute(
                    f"SELECT segment_area FROM dgos WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                sa = [float(row[0])
                      for row in curs.fetchall() if row[0] is not None]
                segment_area = sum(sa) if len(sa) > 0 else None
                ig_width = None if any(value is None for value in [
                                       segment_area, centerline_length]) else segment_area / centerline_length
                if ig_width is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['INTGWDTH']['metric_id']}, {str(ig_width)})")

            if 'CHANVBRAT' in metrics:
                curs.execute(
                    f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['ACTCHANAREA']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                ac = [float(row[0])
                      for row in curs.fetchall() if row[0] is not None]
                ac_area = sum(ac) if len(ac) > 0 else None
                if segment_area is None:
                    curs.execute(
                        f"SELECT segment_area FROM dgos WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    sa = [float(row[0])
                          for row in curs.fetchall() if row[0] is not None]
                    segment_area = sum(sa) if len(sa) > 0 else None
                ac_ratio = None if any(value is None for value in [
                                       ac_area, segment_area]) else ac_area / segment_area
                if ac_ratio is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CHANVBRAT']['metric_id']}, {str(ac_ratio)})")

            if 'LOWLYVBRAT' in metrics:
                curs.execute(
                    f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['ACTFLDAREA']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                afp = [float(row[0])
                       for row in curs.fetchall() if row[0] is not None]
                afp_area = sum(afp) if len(afp) > 0 else None
                if segment_area is None:
                    curs.execute(
                        f"SELECT segment_area FROM dgos WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    sa = [float(row[0])
                          for row in curs.fetchall() if row[0] is not None]
                    segment_area = sum(sa) if len(sa) > 0 else None
                afp_ratio = None if any(value is None for value in [
                                        afp_area, segment_area]) else afp_area / segment_area
                if afp_ratio is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['LOWLYVBRAT']['metric_id']}, {str(afp_ratio)})")

            if 'ELEVATEDVBRAT' in metrics:
                curs.execute(
                    f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['INACTFLDAREA']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                ifp = [float(row[0])
                       for row in curs.fetchall() if row[0] is not None]
                ifp_area = sum(ifp) if len(ifp) > 0 else None
                if segment_area is None:
                    curs.execute(
                        f"SELECT segment_area FROM dgos WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    sa = [float(row[0])
                          for row in curs.fetchall() if row[0] is not None]
                    segment_area = sum(sa) if len(sa) > 0 else None
                ifp_ratio = None if any(value is None for value in [
                                        ifp_area, segment_area]) else ifp_area / segment_area
                if ifp_ratio is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ELEVATEDVBRAT']['metric_id']}, {str(ifp_ratio)})")

            if 'FLDVBRAT' in metrics:
                curs.execute(
                    f"SELECT floodplain_area FROM dgos WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                fp = [float(row[0])
                      for row in curs.fetchall() if row[0] is not None]
                fp_area = sum(fp) if len(fp) > 0 else None
                if segment_area is None:
                    curs.execute(
                        f"SELECT segment_area FROM dgos WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    sa = [float(row[0])
                          for row in curs.fetchall() if row[0] is not None]
                    segment_area = sum(sa) if len(sa) > 0 else None
                fp_ratio = None if any(value is None for value in [
                                       fp_area, segment_area]) else fp_area / segment_area
                if fp_ratio is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['FLDVBRAT']['metric_id']}, {str(fp_ratio)})")

            if 'ACRESVBPM' in metrics:
                if segment_area is None:
                    curs.execute(
                        f"SELECT segment_area FROM dgos WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    sa = [float(row[0])
                          for row in curs.fetchall() if row[0] is not None]
                    segment_area = sum(sa) if len(sa) > 0 else None
                seg_area = segment_area * 0.000247105 if segment_area is not None else None
                if centerline_length is None:
                    curs.execute(
                        f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['VALLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    cl = [float(row[0])
                          for row in curs.fetchall() if row[0] is not None]
                    centerline_length = sum(cl) if len(cl) > 0 else None
                cl_length = centerline_length * \
                    0.000621371 if centerline_length is not None else None
                if seg_area is not None and cl_length is not None:
                    acres_vbpm = seg_area / cl_length
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ACRESVBPM']['metric_id']}, {str(acres_vbpm)})")

            if 'HECTVBPKM' in metrics:
                if segment_area is None:
                    curs.execute(
                        f"SELECT segment_area FROM dgos WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    sa = [float(row[0])
                          for row in curs.fetchall() if row[0] is not None]
                    segment_area = sum(sa) if len(sa) > 0 else None
                seg_area = segment_area * 0.0001 if segment_area is not None else None
                if centerline_length is None:
                    curs.execute(
                        f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['VALLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    cl = [float(row[0])
                          for row in curs.fetchall() if row[0] is not None]
                    centerline_length = sum(cl) if len(cl) > 0 else None
                cl_length = centerline_length / 1000 if centerline_length is not None else None
                if seg_area is not None and cl_length is not None:
                    hectares_vbpkm = seg_area / cl_length
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['HECTVBPKM']['metric_id']}, {str(hectares_vbpkm)})")

            if 'RELFLWLNGTH' in metrics:
                if centerline_length is None:
                    curs.execute(
                        f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['VALLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    cl = [float(row[0])
                          for row in curs.fetchall() if row[0] is not None]
                    centerline_length = sum(cl) if len(cl) > 0 else None
                if stream_length is None:
                    curs.execute(
                        f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMSTRLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    sl = [float(row[0])
                          for row in curs.fetchall() if row[0] is not None]
                    stream_length = sum(sl) if len(sl) > 0 else None
                relative_flow_length = None if any(value is None for value in [
                                                   stream_length, centerline_length]) else stream_length / centerline_length
                if relative_flow_length is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['RELFLWLNGTH']['metric_id']}, {str(relative_flow_length)})")

            if 'TRIBS' in metrics:
                if stream_length is None:
                    curs.execute(
                        f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    stream_length = sum([float(row[0])
                                        for row in curs.fetchall()])
                curs.execute(
                    f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['TRIBS']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                count3 = sum([float(row[0]) for row in curs.fetchall()])
                if stream_length <= 0.0:
                    trib_dens = None
                else:
                    trib_dens = None if any(value is None for value in [
                                            count3, stream_length]) else count3 / (stream_length / 1000.0)
                if trib_dens is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['TRIBS']['metric_id']}, {str(trib_dens)})")

            if 'CHANSIN' in metrics:
                curs.execute(
                    f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['CHANSIN']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                sinuos = [row[0] for row in curs.fetchall()]
                curs.execute(
                    f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['STRMLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                lens = [row[0] for row in curs.fetchall()]
                sin_f = [float(s) for i, s in enumerate(sinuos)
                         if s is not None and lens[i] is not None]
                len_f = [float(l) for i, l in enumerate(
                    lens) if l is not None and sinuos[i] is not None]
                if len(sinuos) != len(lens):
                    log.warning(
                        f'Unable to calculate sinuosity for pt {dgo_id} using moving window, using DGO value')
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['CHANSIN']['metric_id']} AND dgo_id = {dgo_id}")
                    sinuosity = curs.fetchone()[0]
                else:
                    sinuosity = sum([sin_f[i] * len_f[i] for i in range(len(sin_f))]
                                    ) / sum(len_f) if sum(len_f) > 0.0 else None
                if sinuosity is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CHANSIN']['metric_id']}, {str(sinuosity)})")

            if 'VALAZMTH' in metrics:
                curs.execute(
                    f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['VALAZMTH']['metric_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                azs = [float(row[0]) for row in curs.fetchall()]
                curs.execute(
                    f"SELECT measurement_value FROM measurement_values WHERE measurement_id = {measurements['VALLENG']['measurement_id']} AND dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                lens = [float(row[0]) for row in curs.fetchall()]
                azs_f = [float(a) for i, a in enumerate(
                    azs) if a is not None and lens[i] is not None]
                len_f = [float(l) for i, l in enumerate(
                    lens) if l is not None and azs[i] is not None]
                if len(azs) != len(lens):
                    log.warning(
                        f'Unable to calculate azimuth for pt {dgo_id} using moving window, using DGO value')
                    curs.execute(
                        f"SELECT metric_value FROM dgo_metric_values WHERE metric_id = {metrics['VALAZMTH']['metric_id']} AND dgo_id = {dgo_id}")
                    azimuth = curs.fetchone()[0]
                else:
                    azimuth = sum([azs_f[i] * len_f[i] for i in range(len(azs))]
                                  ) / sum(len_f) if sum(len_f) > 0.0 else None
                if azimuth is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['VALAZMTH']['metric_id']}, {str(azimuth)})")

            if 'CNFMT' in metrics and confinement_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT confin_leng, approx_leng FROM confinement_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    confs = curs2.fetchall()
                    conf_ratio = sum([c[0] for c in confs]) / sum([c[1]
                                                                   for c in confs]) if sum([c[1] for c in confs]) > 0 else None
                if conf_ratio is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CNFMT']['metric_id']}, {str(conf_ratio)})")

            if 'CONST' in metrics and confinement_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT constr_leng, approx_leng FROM confinement_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    cons = curs2.fetchall()
                    cons_ratio = sum([c[0] for c in cons]) / sum([c[1]
                                                                  for c in cons]) if sum([c[1] for c in cons]) > 0 else None
                if cons_ratio is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['CONST']['metric_id']}, {str(cons_ratio)})")

            if 'ROADDENS' in metrics and anthro_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT Road_len, centerline_length FROM anthro_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    roadd = curs2.fetchall()
                    rds = [r[0] for r in roadd if r[0] is not None]
                    cls = [r[1] for r in roadd if r[1] is not None]
                    road_density = sum(rds) / sum(cls) if sum(cls) > 0.0 else None
                if road_density is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['ROADDENS']['metric_id']}, {str(road_density)})")

            if 'RAILDENS' in metrics and anthro_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT Rail_len, centerline_length FROM anthro_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    raild = curs2.fetchall()
                    rls = [r[0] for r in raild if r[0] is not None]
                    cls = [r[1] for r in raild if r[1] is not None]
                    rail_density = sum(rls) / sum(cls) if sum(cls) > 0.0 else None

                if rail_density is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['RAILDENS']['metric_id']}, {str(rail_density)})")

            if 'LUI' in metrics and anthro_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT LUI, segment_area FROM anthro_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    luivals = curs2.fetchall()
                    lui = sum(luivals[i][0] * luivals[i][1] for i in range(len(luivals))) / sum([luivals[i][1]
                                                                                                 for i in range(len(luivals))]) if sum([luivals[i][1] for i in range(len(luivals))]) > 0.0 else None
                if lui is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['LUI']['metric_id']}, {str(lui)})")

            if 'FPACCESS' in metrics and rcat_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT FloodplainAccess, segment_area FROM rcat_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    fpacc = curs2.fetchall()
                    fp_access = sum(fpacc[i][0] * fpacc[i][1] for i in range(len(fpacc))) / sum([fpacc[i][1]
                                                                                                 for i in range(len(fpacc))]) if sum([fpacc[i][1] for i in range(len(fpacc))]) > 0.0 else None
                if fp_access is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['FPACCESS']['metric_id']}, {str(fp_access)})")

            if 'PROP_RIP' in metrics and rcat_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT ExistingRiparianMean, segment_area FROM rcat_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    rip = curs2.fetchall()
                    proprip = sum(rip[i][0] * rip[i][1] for i in range(len(rip))) / sum([rip[i][1]
                                                                                         for i in range(len(rip))]) if sum([rip[i][1] for i in range(len(rip))]) > 0.0 else None
                if proprip is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['PROP_RIP']['metric_id']}, {str(proprip)})")

            if 'RVD' in metrics and rcat_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT RiparianDeparture, segment_area FROM rcat_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    rvd = curs2.fetchall()
                    rvd_val = sum((1 - min(rvd[i][0], 1)) * rvd[i][1] for i in range(len(rvd))) / sum([rvd[i][1]
                                                                                                       for i in range(len(rvd))]) if sum([rvd[i][1] for i in range(len(rvd))]) > 0.0 else None
                if rvd_val is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['RVD']['metric_id']}, {str(rvd_val)})")

            if 'AGCONV' in metrics and rcat_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT Agriculture, segment_area FROM rcat_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    agconv = curs2.fetchall()
                    agconv_val = sum(agconv[i][0] * agconv[i][1] for i in range(len(agconv))) / sum([agconv[i][1]
                                                                                                     for i in range(len(agconv))]) if sum([agconv[i][1] for i in range(len(agconv))]) > 0.0 else None
                if agconv_val is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['AGCONV']['metric_id']}, {str(agconv_val)})")

            if 'DEVEL' in metrics and rcat_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT Development, segment_area FROM rcat_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    devel = curs2.fetchall()
                    devel_val = sum(devel[i][0] * devel[i][1] for i in range(len(devel))) / sum([devel[i][1]
                                                                                                 for i in range(len(devel))]) if sum([devel[i][1] for i in range(len(devel))]) > 0.0 else None
                if devel_val is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['DEVEL']['metric_id']}, {str(devel_val)})")

            if 'RIPCOND' in metrics and rcat_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT Condition, segment_area FROM rcat_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    ripcond = curs2.fetchall()
                    ripcond_val = sum(ripcond[i][0] * ripcond[i][1] for i in range(len(ripcond))) / sum([ripcond[i][1]
                                                                                                         for i in range(len(ripcond))]) if sum([ripcond[i][1] for i in range(len(ripcond))]) > 0.0 else None
                if ripcond_val is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['RIPCOND']['metric_id']}, {str(ripcond_val)})")

            if 'BRATCAP' in metrics and brat_dgos:
                with sqlite3.connect(inputs_gpkg) as conn:
                    curs2 = conn.cursor()
                    curs2.execute(
                        f"SELECT oCC_EX, centerline_length FROM brat_dgo WHERE fid IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])})")
                    caps = curs2.fetchall()
                    brat_cap = sum(caps[i][0] * (caps[i][1]/1000) for i in range(len(caps))) / (sum([caps[i][1] for i in range(len(caps))])/1000) if sum([caps[i][1] for i in range(len(caps))]) > 0.0 else None
                if brat_cap is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metrics['BRATCAP']['metric_id']}, {str(brat_cap)})")

            if 'BRATRISK' in metrics and brat_dgos:
                metric = metrics['BRATRISK']
                curs.execute(f"SELECT metric_value from dgo_metric_values WHERE dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])}) AND metric_id = {metric['metric_id']}")
                bratr = curs.fetchall()
                bratrisk = [row[0] for row in bratr if row[0] is not None]
                bratrisk_val = max(set(bratrisk), key=bratrisk.count) if len(bratrisk) > 0 else None
                if bratrisk_val is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metric['metric_id']}, '{str(bratrisk_val)}')")

            if 'BRATOPP' in metrics and brat_dgos:
                metric = metrics['BRATOPP']
                curs.execute(f"SELECT metric_value from dgo_metric_values WHERE dgo_id IN ({','.join([str(dgo_id) for dgo_id in dgo_ids])}) AND metric_id = {metric['metric_id']}")
                brato = curs.fetchall()
                bratopp = [row[0] for row in brato if row[0] is not None]
                bratopp_val = max(set(bratopp), key=bratopp.count) if len(bratopp) > 0 else None
                if bratopp_val is not None:
                    curs.execute(
                        f"INSERT INTO igo_metric_values (igo_id, metric_id, metric_value) VALUES ({igo_id}, {metric['metric_id']}, '{str(bratopp_val)}')")

            conn.commit()
    progbar.finish()

    with sqlite3.connect(intermediates_gpkg) as conn:
        curs = conn.cursor()

        # Insert Values into Pivot table
        number_metrics = {metric: val for metric, val in metrics.items(
        ) if val['data_type'] == 'INTEGER' or val['data_type'] == 'REAL'}
        text_metrics = {metric: val for metric,
                        val in metrics.items() if val['data_type'] == 'TEXT'}

        num_metric_names_sql = ", ".join([sql_name(metric["field_name"]) for metric in number_metrics.values()])
        text_metric_names_sql = ", ".join([sql_name(metric["field_name"]) for metric in text_metrics.values()])

        metric_values_sql = ", ".join(
            [f"{sql_round(metric['data_type'], metric['metric_id'])} {sql_name(metric['field_name'])}" for metric in number_metrics.values()])

        sql = f'CREATE VIEW dgo_num_metrics (fid, {num_metric_names_sql}) AS SELECT M.dgo_id, {metric_values_sql} FROM dgo_metric_values M GROUP BY M.dgo_id;'
        curs.execute(sql)
        sql2 = f"""CREATE VIEW igo_num_metrics (fid, {num_metric_names_sql}) AS SELECT M.igo_id, {metric_values_sql} FROM igo_metric_values M GROUP BY M.igo_id;"""
        curs.execute(sql2)

        curs.execute(f"""CREATE VIEW dgo_text_metrics(fid, {text_metric_names_sql}) AS SELECT dgo.fid, o.ownership, s.us_state, c.county, e.ecoregion3, f.ecoregion4, br.bratrisk, bo.bratopp FROM dgos dgo LEFT JOIN
                     (SELECT dgo_id, metric_value AS ownership FROM dgo_metric_values WHERE metric_id={metrics['AGENCY']['metric_id']}) o ON o.dgo_id=dgo.fid LEFT JOIN
                     (SELECT dgo_id, metric_value AS us_state FROM dgo_metric_values WHERE metric_id={metrics['STATE']['metric_id']}) s ON s.dgo_id=dgo.fid LEFT JOIN
                     (SELECT dgo_id, metric_value AS county FROM dgo_metric_values WHERE metric_id={metrics['COUNTY']['metric_id']}) c ON c.dgo_id=dgo.fid LEFT JOIN
                     (SELECT dgo_id, metric_value AS ecoregion3 FROM dgo_metric_values WHERE metric_id={metrics['ECORGIII']['metric_id']}) e ON e.dgo_id=dgo.fid LEFT JOIN
                     (SELECT dgo_id, metric_value AS ecoregion4 FROM dgo_metric_values WHERE metric_id={metrics['ECORGIV']['metric_id']}) f ON f.dgo_id=dgo.fid LEFT JOIN
                     (SELECT dgo_id, metric_value AS bratrisk FROM dgo_metric_values WHERE metric_id={metrics['BRATRISK']['metric_id']}) br ON br.dgo_id=dgo.fid LEFT JOIN
                     (SELECT dgo_id, metric_value AS bratopp FROM dgo_metric_values WHERE metric_id={metrics['BRATOPP']['metric_id']}) bo ON bo.dgo_id=dgo.fid
                     """)
        curs.execute(f"""CREATE VIEW igo_text_metrics(fid, {text_metric_names_sql}) AS SELECT igo.fid, o.ownership, s.us_state, c.county, e.ecoregion3, f.ecoregion4, br.bratrisk, bo.bratopp FROM igos igo LEFT JOIN
                     (SELECT igo_id, metric_value AS ownership FROM igo_metric_values WHERE metric_id={metrics['AGENCY']['metric_id']}) o ON o.igo_id=igo.fid LEFT JOIN
                     (SELECT igo_id, metric_value AS us_state FROM igo_metric_values WHERE metric_id={metrics['STATE']['metric_id']}) s ON s.igo_id=igo.fid LEFT JOIN
                     (SELECT igo_id, metric_value AS county FROM igo_metric_values WHERE metric_id={metrics['COUNTY']['metric_id']}) c ON c.igo_id=igo.fid LEFT JOIN
                     (SELECT igo_id, metric_value AS ecoregion3 FROM igo_metric_values WHERE metric_id={metrics['ECORGIII']['metric_id']}) e ON e.igo_id=igo.fid LEFT JOIN
                     (SELECT igo_id, metric_value AS ecoregion4 FROM igo_metric_values WHERE metric_id={metrics['ECORGIV']['metric_id']}) f ON f.igo_id=igo.fid LEFT JOIN
                     (SELECT igo_id, metric_value AS bratrisk FROM igo_metric_values WHERE metric_id={metrics['BRATRISK']['metric_id']}) br ON br.igo_id=igo.fid LEFT JOIN
                     (SELECT igo_id, metric_value AS bratopp FROM igo_metric_values WHERE metric_id={metrics['BRATOPP']['metric_id']}) bo ON bo.igo_id=igo.fid
                     """)
        curs.execute(
            "CREATE VIEW dgo_metrics_pivot AS SELECT * FROM dgo_num_metrics JOIN dgo_text_metrics USING (fid);")
        curs.execute(
            "CREATE VIEW igo_metrics_pivot AS SELECT * FROM igo_num_metrics JOIN igo_text_metrics USING (fid);")
        conn.commit()

        # Create metric view
        metric_names_sql = ", ".join(
            [f"M.{sql_name(metric['field_name'])} {sql_name(metric['field_name'])}" for metric in metrics.values()])
        sql = f'CREATE VIEW vw_igo_metrics AS SELECT G.fid, G.geom, G.level_path, G.seg_distance, G.stream_size, G.FCode, G.window_size, {metric_names_sql} FROM igos G INNER JOIN igo_metrics_pivot M ON M.fid = G.fid;'
        curs.execute(sql)
        sql2 = f'CREATE VIEW vw_dgo_metrics AS SELECT G.fid, G.geom, G.level_path, G.seg_distance, G.FCode, G.segment_area, G.centerline_length, {metric_names_sql} FROM dgos G INNER JOIN dgo_metrics_pivot M ON M.fid = G.fid;'
        curs.execute(sql2)
        conn.commit()

        measure_sql = ", ".join(
            [f"{sql_name(measurement['name'])} {measurement['name']}" for measurement in measurements.values()])
        sql = f'CREATE TABLE measurements_pivot (fid INTEGER PRIMARY KEY, {measure_sql});'
        curs.execute(sql)
        conn.commit()

        measure_names_sql = ', '.join(
            [sql_name(measurement["name"]) for measurement in measurements.values()])
        measure_values_sql = ", ".join(
            [f"{sql_round(measurement['data_type'], measurement['measurement_id'], 'measurement')} {sql_name(measurement['name'])}" for measurement in measurements.values()])
        sql = f'INSERT INTO measurements_pivot (fid, {measure_names_sql}) SELECT M.dgo_id, {measure_values_sql} FROM measurement_values M GROUP BY M.dgo_id;'
        curs.execute(sql)
        conn.commit()

        # Create measure view
        measure_names_sql = ", ".join(
            [f"M.{sql_name(measurement['name'])} {sql_name(measurement['name'])}" for measurement in measurements.values()])
        sql = f'CREATE VIEW vw_measurements AS SELECT G.fid, G.geom, G.level_path, G.seg_distance, {measure_names_sql} FROM dgos G INNER JOIN measurements_pivot M ON M.fid = G.fid;'
        curs.execute(sql)

        # Add view to geopackage
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_igo_metrics', data_type, 'igo_metrics', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_igo_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_dgo_metrics', data_type, 'dgo_metrics', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_dgo_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_measurements', data_type, 'measurements', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_measurements', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'dgos'""")
        conn.commit()

    # Flattnen outputs
    log.info('Preparing Final RME Outputs')

    field_types = {}
    with sqlite3.connect(intermediates_gpkg) as conn:
        rme_curs = conn.cursor()
        rme_curs.execute('''
            SELECT metric_id, field_name, data_type
            FROM metrics
            WHERE (is_active <> 0)
                AND (field_name is not null)
                AND (data_type is not null)
        ''')
        for row in rme_curs.fetchall():
            field_type = row[2]
            oft_type = ogr.OFTString
            if field_type.lower() == 'integer':
                oft_type = ogr.OFTInteger
            elif field_type.lower() == 'real':
                oft_type = ogr.OFTReal
            field_types[row[1].lower()] = oft_type

    rme_igos = LayerTypes['RME_OUTPUTS'].sub_layers['IGO_METRICS'].rel_path
    rme_dgos = LayerTypes['RME_OUTPUTS'].sub_layers['DGO_METRICS'].rel_path

    with GeopackageLayer(intermediates_gpkg, 'vw_igo_metrics') as igo_metrics_layer, \
            GeopackageLayer(outputs_gpkg, rme_igos, write=True) as igo_output_layer:

        fields = igo_metrics_layer.get_fields()
        igo_output_layer.create_layer_from_ref(igo_metrics_layer, create_fields=False)

        # reapply the field types
        for field, defn in fields.items():
            if field in field_types:
                igo_output_layer.create_field(field, field_types[field])
            else:
                igo_output_layer.create_field(field, field_def=defn)

        for feature, *_ in igo_metrics_layer.iterate_features('Copying IGO Metrics'):
            feature: ogr.Feature
            geometry: ogr.Geometry = feature.GetGeometryRef()
            attributes = {field: feature.GetField(field) for field in fields}
            igo_output_layer.create_feature(geometry, attributes)

    with GeopackageLayer(intermediates_gpkg, 'vw_dgo_metrics') as dgo_metrics_layer, \
            GeopackageLayer(outputs_gpkg, rme_dgos, write=True) as dgo_output_layer:

        fields = dgo_metrics_layer.get_fields()
        dgo_output_layer.create_layer_from_ref(dgo_metrics_layer, create_fields=False)

        # reapply the field types
        for field, defn in fields.items():
            if field in field_types:
                dgo_output_layer.create_field(field, field_types[field])
            else:
                dgo_output_layer.create_field(field, field_def=defn)

        for feature, *_ in dgo_metrics_layer.iterate_features('Copying DGO Metrics'):
            feature: ogr.Feature
            geometry: ogr.Geometry = feature.GetGeometryRef()
            attributes = {field: feature.GetField(field) for field in fields}
            dgo_output_layer.create_feature(geometry, attributes)

    #  index sg_distance, level_path and FCode in both the DGOs and IGOs tables
    with sqlite3.connect(outputs_gpkg) as conn:
        curs = conn.cursor()
        curs.execute('CREATE INDEX idx_dgo_fcode ON rme_dgos(FCode);')
        curs.execute('CREATE INDEX idx_igo_fcode ON rme_igos(FCode);')
        curs.execute('CREATE INDEX idx_dgos_level_path_seg_distance ON rme_dgos (level_path, seg_distance);')
        curs.execute('CREATE INDEX idx_igos_level_path_seg_distance ON rme_igos (level_path, seg_distance);')
        conn.commit()

    # Add nodes to the project
    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    project.add_project_geopackage(
        proj_nodes['Outputs'], LayerTypes['RME_OUTPUTS'])

    ellapsed = time.time() - start_time
    project.add_metadata([
        RSMeta("ProcTimeS", "{:.2f}".format(ellapsed),
               RSMetaTypes.HIDDEN, locked=True),
        RSMeta("Processing Time", pretty_duration(ellapsed), locked=True)
    ])

    add_layer_descriptions(project, LYR_DESCRIPTIONS_JSON, LayerTypes)

    # None will create the base report with no filter on the data
    filter_names = [None] + FILTER_NAMES

    for filter_name in filter_names:
        report_suffix = f"_{filter_name.upper()}" if filter_name is not None else ""

        # Write a report
        report_path = os.path.join(
            project.project_dir, LayerTypes[f'REPORT{report_suffix}'].rel_path
        )
        project.add_report(
            proj_nodes['Outputs'],
            LayerTypes[f'REPORT{report_suffix}'], replace=True
        )
        report = RMEReport(outputs_gpkg, report_path, project, filter_name, intermediates_gpkg)
        report.write()

    log.info('Riverscapes Metric Engine Finished')
    return


def sql_name(name: str) -> str:
    """return cleaned metric column name"""
    return name.lower().replace(' ', '_')


def sql_round(datatype: str, metric_id, table='metric') -> str:
    """return round function"""
    return f"CAST{'(ROUND(' if datatype == 'REAL' else '('}SUM(M.{table}_value) FILTER (WHERE M.{table}_id == {metric_id}){', 4) AS REAL)' if datatype == 'REAL' else 'AS INT)'}"


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
        metric_data = curs.execute(
            f"""SELECT * from {source_table} WHERE is_active = 1""").fetchall()
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
    sql = f'level_path = {level_path} AND seg_distance >= {min_dist} AND seg_distance <={max_dist}'
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
    # ogr.ForceToPolygon(geom_window_sections)
    geom_window = geom_window_sections.Buffer(buffer)

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
            # BRAT uses 100m here for all stream sizes?
            polygon = point.buffer(buffer)
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
    sql = f'level_path = {level_path} AND seg_distance >= {min_dist} AND seg_distance <={max_dist}'
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
    parser.add_argument(
        'flowlines', help="NHD Flowlines (.shp, .gpkg/layer_name)", type=str)
    parser.add_argument('vaa_table', help="NHD Plus vaa table")
    parser.add_argument('counties', help='Counties shapefile')
    parser.add_argument('dgos', help='vbet segment polygons')
    parser.add_argument(
        'vbet_points', help='valley bottom or other polygon representing confining boundary (.shp, .gpkg/layer_name)', type=str)
    parser.add_argument('valley_centerline',
                        help='vbet centerline feature class')
    parser.add_argument('dem', help='dem')
    parser.add_argument('hillshade', help='hillshade')
    parser.add_argument('output_folder', help='Output folder', type=str)
    parser.add_argument('--confinement_dgos',
                        help='confinement dgos', type=str)
    parser.add_argument('--anthro_dgos', help='anthro dgos', type=str)
    parser.add_argument('--rcat_dgos', help='rcat_dgos', type=str)
    parser.add_argument('--brat_dgos', help='brat dgos', type=str)
    parser.add_argument(
        '--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ',
                        action='store_true', default=False)
    parser.add_argument('--debug', help="(optional) save intermediate outputs for debugging",
                        action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("Riverscapes Metric Engine")
    log.setup(logPath=os.path.join(
        args.output_folder, "rme.log"), verbose=args.verbose)
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
                                         args.counties,
                                         args.dgos,
                                         args.vbet_points,
                                         args.valley_centerline,
                                         args.dem,
                                         args.hillshade,
                                         args.output_folder,
                                         args.confinement_dgos,
                                         args.anthro_dgos,
                                         args.rcat_dgos,
                                         args.brat_dgos,
                                         meta=meta)
            log.debug(f'Return code: {retcode}, [Max process usage] {max_obj}')

        else:
            metric_engine(args.huc,
                          args.flowlines,
                          args.vaa_table,
                          args.counties,
                          args.dgos,
                          args.vbet_points,
                          args.valley_centerline,
                          args.dem,
                          args.hillshade,
                          args.output_folder,
                          args.confinement_dgos,
                          args.anthro_dgos,
                          args.rcat_dgos,
                          args.brat_dgos,
                          meta=meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
