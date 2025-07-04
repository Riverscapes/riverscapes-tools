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
import time

from osgeo import ogr
from osgeo import gdal
import rasterio

from rscommons import GeopackageLayer, dotenv, Logger, initGDALOGRErrors, ModelConfig, RSLayer, RSMeta, RSMetaTypes, RSProject, VectorBase, ProgressBar
from rscommons.classes.vector_base import get_utm_zone_epsg
from rscommons.util import parse_metadata, pretty_duration
from rscommons.database import load_lookup_data
from rscommons.vector_ops import copy_feature_class, collect_linestring
from rscommons.copy_features import copy_features_fields
from rscommons.vbet_network import copy_vaa_attributes, join_attributes
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions
from rscommons.moving_window import moving_window_dgo_ids

from rme.__version__ import __version__
from rme.analysis_window import AnalysisLine
from rme.rme_report import RMEReport, FILTER_NAMES
from rme.utils.measurements import get_segment_measurements
from rme.utils.check_vbet_inputs import vbet_inputs
from rme.utils.summarize_functions import *
from rme.utils.bespoke_functions import *
from rme.utils.thematic_tables import create_thematic_table, create_measurement_table

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
        'WATERBODIES': RSLayer('Waterbodies', 'WATERBODIES', 'Vector', 'waterbodies'),
        'HUC12': RSLayer('HUC12 Watersheds', 'HUC12', 'Vector', 'huc12'),
        'COUNTIES': RSLayer('Counties', 'COUNTIES', 'Vector', 'counties'),
        'GEOLOGY': RSLayer('Geology', 'GEOLOGY', 'Vector', 'geology'),
        'VBET_DGOS': RSLayer('Vbet DGOs', 'VBET_DGOS', 'Vector', 'vbet_dgos'),
        'VBET_IGOS': RSLayer('Vbet IGOs', 'VBET_IGOS', 'Vector', 'vbet_igos'),
        'VBET_CENTERLINES': RSLayer('VBET Centerline', 'VBET_CENTERLINE', 'Vector', 'valley_centerlines')
    }),
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'inputs/dem.tif'),
    'HILLSHADE': RSLayer('Hillshade', 'HILLSHADE', 'Raster', 'inputs/hillshade.tif'),
    'EVT': RSLayer('Landfire EVT', 'EVT', 'Raster', 'inputs/evt.tif'),
    'BPS': RSLayer('Landfire BPS', 'BPS', 'Raster', 'inputs/bps.tif'),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/rme_intermediates.gpkg', {
        'JUNCTION_POINTS': RSLayer('Junction Points', 'JUNCTION_POINTS', 'Vector', 'junction_points')
    }),
    'OUTPUTS': RSLayer('Riverscapes Metrics', 'OUTPUTS', 'Geopackage', 'outputs/riverscapes_metrics.gpkg', {
        'GEOM_IGOS': RSLayer('RME IGO', 'GEOM_IGOS', 'Vector', 'igos'),
        'GEOM_DGOS': RSLayer('RME DGO', 'GEOM_DGOS', 'Vector', 'dgos'),
        'DGO_BEAVER': RSLayer('RME DGO Beaver', 'DGO_BEAVER', 'Vector', 'vw_dgo_beaver_metrics'),
        'DGO_DESC': RSLayer('RME DGO Descriptive', 'DGO_DESC', 'Vector', 'vw_dgo_desc_metrics'),
        'DGO_GEOMORPH': RSLayer('RME DGO Geomorph', 'DGO_GEOMORPH', 'Vector', 'vw_dgo_geomorph_metrics'),
        'DGO_HYDRO': RSLayer('RME DGO Hydro', 'DGO_HYDRO', 'Vector', 'vw_dgo_hydro_metrics'),
        'DGO_IMPACTS': RSLayer('RME DGO Impacts', 'DGO_IMPACTS', 'Vector', 'vw_dgo_impacts_metrics'),
        'DGO_VEG': RSLayer('RME DGO Vegetation', 'DGO_VEG', 'Vector', 'vw_dgo_veg_metrics'),
        'IGO_BEAVER': RSLayer('RME IGO Beaver', 'IGO_BEAVER', 'Vector', 'vw_igo_beaver_metrics'),
        'IGO_DESC': RSLayer('RME IGO Descriptive', 'IGO_DESC', 'Vector', 'vw_igo_desc_metrics'),
        'IGO_GEOMORPH': RSLayer('RME IGO Geomorph', 'IGO_GEOMORPH', 'Vector', 'vw_igo_geomorph_metrics'),
        'IGO_HYDRO': RSLayer('RME IGO Hydro', 'IGO_HYDRO', 'Vector', 'vw_igo_hydro_metrics'),
        'IGO_IMPACTS': RSLayer('RME IGO Impacts', 'IGO_IMPACTS', 'Vector', 'vw_igo_impacts_metrics'),
        'IGO_VEG': RSLayer('RME IGO Vegetation', 'IGO_VEG', 'Vector', 'vw_igo_veg_metrics'),
        'DGO_METRICS': RSLayer('RME DGO', 'DGO_METRICS', 'Vector', 'vw_dgo_metrics'),
        'IGO_METRICS': RSLayer('RME IGO', 'IGO_METRICS', 'Vector', 'vw_igo_metrics')
    }),
    'REPORT': RSLayer('RME Report', 'REPORT', 'HTMLFile', 'outputs/rme.html'),
    # 'REPORT_PERENNIAL': RSLayer('RME Perennial Streams Report', 'REPORT_PERENNIAL', 'HTMLFile', 'outputs/rme_perennial.html'),
    # 'REPORT_PUBLIC_PERENNIAL': RSLayer('RME Public Perennial Streams Report', 'REPORT_PUBLIC_PERENNIAL', 'HTMLFile', 'outputs/rme_public_perennial.html'),
    # 'REPORT_BLM_LANDS': RSLayer('RME BLM Lands Report', 'REPORT_BLM_LANDS', 'HTMLFile', 'outputs/rme_blm_lands.html'),
    # 'REPORT_BLM_PERENNIAL': RSLayer('RME BLM Perennial Report', 'REPORT_BLM_PERENNIAL', 'HTMLFile', 'outputs/rme_blm_perennial.html'),
    # 'REPORT_USFS_PERENNIAL': RSLayer('RME USFS Perennial Report', 'REPORT_USFS_PERENNIAL', 'HTMLFile', 'outputs/rme_usfs_perennial.html'),
    # 'REPORT_NPS_PERENNIAL': RSLayer('RME NPS Perennial Report', 'REPORT_NPS_PERENNIAL', 'HTMLFile', 'outputs/rme_nps_perennial.html'),
    # 'REPORT_ST_PERENNIAL': RSLayer('RME ST Perennial Report', 'REPORT_ST_PERENNIAL', 'HTMLFile', 'outputs/rme_st_perennial.html'),
    # 'REPORT_FWS_PERENNIAL': RSLayer('RME FWS Perennial Report', 'REPORT_FWS_PERENNIAL', 'HTMLFile', 'outputs/rme_fws_perennial.html'),
}

stream_size_lookup = {0: 'small', 1: 'medium',
                      2: 'large', 3: 'very large', 4: 'huge'}
gradient_buffer_lookup = {'small': 25.0, 'medium': 50.0, 'large': 100.0,
                          'very large': 100.0, 'huge': 100.0}  # should this go as high as it does
window_distance = {'0': 200.0, '1': 400.0,
                   '2': 1200.0, '3': 2000.0, '4': 8000.0}
# metric_functions = {metric_calculation_id: function (from summarize functions.py)}
metric_functions = {1: value_from_dgo, 2: value_density_from_dgo, 3: get_max_value, 4: value_from_max_length,
                    5: value_from_dataset_area, 6: value_by_count, 7: ex_veg_proportion, 8: hist_veg_proportion}
mw_metric_functions = {1: mw_copy_from_dgo, 2: mw_sum, 3: mw_sum_div_length, 4: mw_sum_div_chan_length, 5: mw_proportion, 6: mw_area_weighted_av}


def metric_engine(huc: int, in_flowlines: Path, in_waterbodies: Path, in_huc12: Path, in_vaa_table: Path, in_counties: Path, in_geology: Path,
                  in_segments: Path, in_points: Path, in_vbet_centerline: Path, in_dem: Path, in_hillshade: Path, project_folder: Path,
                  in_confinement_dgos: Path = None, in_hydro_dgos: Path = None, in_anthro_dgos: Path = None, in_anthro_lines: Path = None,
                  in_rcat_dgos: Path = None, in_rcat_dgo_table: Path = None, in_brat_dgos: Path = None, in_brat_lines: Path = None,
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
    for p in [in_confinement_dgos, in_hydro_dgos, in_anthro_dgos, in_rcat_dgos, in_brat_dgos]:
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
    outputs_gpkg = os.path.join(project_folder, LayerTypes['OUTPUTS'].rel_path)
    GeopackageLayer.delete(inputs_gpkg)
    GeopackageLayer.delete(intermediates_gpkg)
    GeopackageLayer.delete(outputs_gpkg)

    src_layers = {
        'FLOWLINES': in_flowlines,
        'WATERBODIES': in_waterbodies,
        'HUC12': in_huc12,
        'COUNTIES': in_counties,
        'GEOLOGY': in_geology,
        'VBET_DGOS': in_segments,
        'VBET_IGOS': in_points,
        'VBET_CENTERLINES': in_vbet_centerline
    }

    input_layers = {}
    for input_key, rslayer in LayerTypes['INPUTS'].sub_layers.items():
        input_layers[input_key] = os.path.join(inputs_gpkg, rslayer.rel_path)
        copy_feature_class(src_layers[input_key], input_layers[input_key], cfg.OUTPUT_EPSG)

    _dem_node, dem = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['DEM'], in_dem)
    _hs_node, hillshade = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], in_hillshade)

    in_gpkg_node, *_ = project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    if in_confinement_dgos:
        confinement_dgos = os.path.join(inputs_gpkg, 'confinement_dgo')
        copy_feature_class(in_confinement_dgos, confinement_dgos)
        project.add_dataset(in_gpkg_node.find('Layers'), 'confinement_dgo', RSLayer('Confinement DGO', 'CONFINEMENT_DGO', 'Vector', 'confinement_dgo'), 'Vector', rel_path=True, sublayer=True)
    else:
        confinement_dgos = None
    if in_hydro_dgos:
        hydro_dgos = os.path.join(inputs_gpkg, 'hydro_dgo')
        copy_feature_class(in_hydro_dgos, hydro_dgos)
        project.add_dataset(in_gpkg_node.find('Layers'), 'hydro_dgo', RSLayer('Hydrologic DGO', 'HYDRO_DGO', 'Vector', 'hydro_dgo'), 'Vector', rel_path=True, sublayer=True)
    else:
        hydro_dgos = None
    if in_anthro_dgos:
        anthro_dgos = os.path.join(inputs_gpkg, 'anthro_dgo')
        copy_feature_class(in_anthro_dgos, anthro_dgos)
        project.add_dataset(in_gpkg_node.find('Layers'), 'anthro_dgo', RSLayer('Anthropogenic DGO', 'ANTHRO_DGO', 'Vector', 'anthro_dgo'), 'Vector', rel_path=True, sublayer=True)
    else:
        anthro_dgos = None
    if in_anthro_lines:
        anthro_lines = os.path.join(inputs_gpkg, 'anthro_flowlines')
        copy_feature_class(in_anthro_lines, anthro_lines)
        project.add_dataset(in_gpkg_node.find('Layers'), 'anthro_flowlines', RSLayer('Anthropogenic Flowline', 'ANTHRO_LINE', 'Vector', 'anthro_flowlines'), 'Vector', rel_path=True, sublayer=True)
    else:
        anthro_lines = None
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
    if in_brat_lines:
        brat_lines = os.path.join(inputs_gpkg, 'brat_flowlines')
        copy_feature_class(in_brat_lines, brat_lines)
        project.add_dataset(in_gpkg_node.find('Layers'), 'brat_flowlines', RSLayer('BRAT Flowline', 'BRAT_LINE', 'Vector', 'brat_flowlines'), 'Vector', rel_path=True, sublayer=True)
    else:
        brat_lines = None

    # create output feature class fields. Only those listed here will get copied from the source
    with GeopackageLayer(outputs_gpkg, layer_name=LayerTypes['OUTPUTS'].sub_layers['GEOM_IGOS'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbPoint, epsg=cfg.OUTPUT_EPSG, options=['FID=igoid'], fields={
            'level_path': ogr.OFTString,
            'seg_distance': ogr.OFTInteger,
            'stream_size': ogr.OFTInteger,
            'FCode': ogr.OFTInteger
        })

    with GeopackageLayer(outputs_gpkg, layer_name=LayerTypes['OUTPUTS'].sub_layers['GEOM_DGOS'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPolygon, epsg=cfg.OUTPUT_EPSG, options=['FID=dgoid'], fields={
            'level_path': ogr.OFTString,
            'seg_distance': ogr.OFTInteger,
            'centerline_length': ogr.OFTReal,
            'segment_area': ogr.OFTReal,
            'FCode': ogr.OFTInteger
        })

    points = os.path.join(outputs_gpkg, LayerTypes['OUTPUTS'].sub_layers['GEOM_IGOS'].rel_path)
    segments = os.path.join(outputs_gpkg, LayerTypes['OUTPUTS'].sub_layers['GEOM_DGOS'].rel_path)
    copy_features_fields(input_layers['VBET_IGOS'], points, epsg=cfg.OUTPUT_EPSG)  # seg dist not null
    copy_features_fields(input_layers['VBET_DGOS'], segments, epsg=cfg.OUTPUT_EPSG)

    # copy DGOVegetation table from RCAT into outputs gpkg
    if in_rcat_dgo_table:
        copy_table(os.path.dirname(in_rcat_dgo_table), outputs_gpkg, 'DGOVegetation')

    # get utm
    with GeopackageLayer(input_layers['VBET_IGOS']) as lyr_pts:
        feat = lyr_pts.ogr_layer.GetNextFeature()
        geom = feat.GetGeometryRef()
        utm_epsg = get_utm_zone_epsg(geom.GetPoint(0)[0])

    vaa_table_name = copy_vaa_attributes(input_layers['FLOWLINES'], in_vaa_table)
    line_network = join_attributes(inputs_gpkg, "vw_flowlines_vaa", os.path.basename(input_layers['FLOWLINES']), vaa_table_name, 'NHDPlusID', [
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
    with sqlite3.connect(outputs_gpkg) as conn:
        cursor = conn.cursor()
        with open(os.path.join(database_folder, 'metrics_schema.sql'), encoding='utf-8') as sqlfile:
            sql_commands = sqlfile.read()
            cursor.executescript(sql_commands)
            conn.commit()
    # Load tables
    load_lookup_data(outputs_gpkg, os.path.join(
        database_folder, 'data'))

    # index level path and seg distance
    with sqlite3.connect(outputs_gpkg) as conn:
        curs = conn.cursor()
        curs.execute("CREATE INDEX ix_dgos_level_path_seg_distance ON dgos (level_path, seg_distance)")
        curs.execute("CREATE INDEX idx_igos_size ON igos (stream_size)")
        curs.execute("CREATE INDEX ix_dgos_fcode ON dgos (FCode)")
        curs.execute("CREATE INDEX ix_igos_level_path_seg_distance ON igos (level_path, seg_distance)")
        curs.execute("CREATE INDEX ix_veg_dgoid ON DGOVegetation (dgoid)")
        curs.execute("CREATE INDEX ix_veg_vegid ON DGOVegetation (VegetationID)")
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

    create_measurement_table(outputs_gpkg)
    # get measurements for DGOs
    buffer_distance = {}
    for stream_size, distance in gradient_buffer_lookup.items():
        buffer = VectorBase.rough_convert_metres_to_raster_units(dem, distance)
        buffer_distance[stream_size] = buffer
    dgo_meas = {}
    progbar = ProgressBar(len(level_paths_to_run), 50,
                          f"Calculating Measurements for DGOs")
    counter = 0
    for level_path in level_paths_to_run:
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
                input_layers['VBET_CENTERLINES'], f'level_path = {level_path}', precision=8)

            for feat_seg_dgo, *_ in lyr_segments.iterate_features(attribute_filter=f'level_path = {level_path}'):
                # Gather common components for metric calcuations
                feat_geom = feat_seg_dgo.GetGeometryRef().Clone()
                dgo_id = feat_seg_dgo.GetFID()
                segment_distance = feat_seg_dgo.GetField('seg_distance')
                if segment_distance is None:
                    continue

                with GeopackageLayer(points) as lyr_points:
                    for pt_ftr, *_ in lyr_points.iterate_features(attribute_filter=f'level_path = {level_path} and seg_distance = {segment_distance}'):
                        stream_size_id = pt_ftr.GetField('stream_size')
                        break
                if not 'stream_size_id' in locals():
                    log.warning(f'Unable to find stream size for dgo {dgo_id} in level path {level_path}')
                    stream_size_id = 0

                stream_size = stream_size_lookup[stream_size_id]
                stream_length, straight_length, min_elev, max_elev = get_segment_measurements(
                    geom_flowline, src_dem, feat_geom, buffer_distance[stream_size], transform)

                cl_length, cl_straight, cl_min_elev, cl_max_elev = get_segment_measurements(
                    geom_centerline, src_dem, feat_geom, buffer_distance[stream_size], transform)

                dgo_meas[dgo_id] = [stream_length, straight_length, cl_length, min_elev, max_elev, cl_min_elev, cl_max_elev]

    # add dgo measurements to the table and get metric groups
    with sqlite3.connect(outputs_gpkg) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        curs = conn.cursor()
        for dgo_id, meas in dgo_meas.items():
            if None not in meas:
                curs.execute(f"""INSERT INTO dgo_measurements (dgoid, strmleng, strmstrleng, valleng, strmminelev, strmmaxelev, clminelev, clmaxelev)
                             VALUES ({dgo_id}, {meas[0]}, {meas[1]}, {meas[2]}, {meas[3]}, {meas[4]}, {meas[5]}, {meas[6]})""")
        conn.commit()
        curs.execute("SELECT DISTINCT metric_group_id, metric_group_name FROM metric_groups")
        metric_groups = list(curs.fetchall())
        curs.execute("SELECT * FROM input_datasets")
        input_datasets = {row[0]: row[1:] for row in curs.fetchall()}
        curs.execute("SELECT * FROM input_datasets_mw")
        mw_input_datasets = {row[0]: row[1:] for row in curs.fetchall()}

    for metric_group in metric_groups:
        create_thematic_table(outputs_gpkg, metric_group[1], metric_group[0])
        metrics = generate_metric_list(outputs_gpkg, metric_group[0])

        with GeopackageLayer(segments) as lyr_segments, sqlite3.connect(outputs_gpkg) as conn:
            curs = conn.cursor()
            # log.info(f'Calculating {metric_group[1]} metrics for DGOs')
            for feat_seg_dgo, *_ in lyr_segments.iterate_features(f'Calculating {metric_group[1]} metrics for DGOs'):
                bespoke_method_metrics = []
                metrics_output = {}
                feat_geom = feat_seg_dgo.GetGeometryRef().Clone()
                if not feat_geom.IsValid():
                    feat_geom = feat_geom.MakeValid()
                dgo_id = feat_seg_dgo.GetFID()
                segment_distance = feat_seg_dgo.GetField('seg_distance')
                if segment_distance is None:
                    continue

                for metric in metrics:
                    if metrics[metric]['metric_calculation_id'] not in metric_functions.keys():
                        bespoke_method_metrics.append(metric)
                        continue

                    input_args = input_datasets[metrics[metric]['metric_id']]
                    input_args = [item for item in input_args if item]
                    input_args.insert(0, feat_seg_dgo)
                    input_args[1] = os.path.join(project_folder, input_args[1])
                    if not os.path.exists(input_args[1]) and not os.path.exists(os.path.dirname(input_args[1])):
                        log.error(f"Input file {input_args[1]} does not exist, unable to calucalate {metrics[metric]['metric_name']}")
                        continue
                    curs.execute(f"SELECT metric_calculation_id FROM metrics WHERE metric_id = {metrics[metric]['metric_id']}")
                    metric_calculation_id = int(curs.fetchone()[0])
                    if metric_calculation_id not in metric_functions.keys():
                        continue
                    val = call_function(metric_functions[metric_calculation_id], *input_args)
                    if val is None:
                        metrics_output[metrics[metric]['field_name']] = None
                    else:
                        if metrics[metric]['data_type'] == 'REAL':
                            metrics_output[metrics[metric]['field_name']] = float(val)
                        elif metrics[metric]['data_type'] == 'INTEGER':
                            metrics_output[metrics[metric]['field_name']] = int(val)
                        else:
                            metrics_output[metrics[metric]['field_name']] = str(val)

                if len(metrics_output) > 0:
                    field_names = ', '.join(metrics_output.keys())
                    placeholders = ', '.join(['?'] * len(metrics_output))
                    sql = f"""INSERT INTO dgo_{metric_group[1]} (dgoid, {field_names}) VALUES ({dgo_id}, {placeholders})"""
                    curs.execute(sql, list(metrics_output.values()))
                else:
                    log.warning(f"No {metric_group[1]} metrics calculated for DGO {dgo_id}")

                if len(bespoke_method_metrics) > 0:
                    besp_output = {}
                    for metric in bespoke_method_metrics:
                        # these are the metrics that need a specific, bespoke method (metric_calculation_id = 99),
                        if metric == 'WATERSHED':
                            watsid = watershed(huc)
                            besp_output[metrics[metric]['field_name']] = watsid

                        if metric == 'SUBWATS':
                            subwatsid = subwatershed(feat_seg_dgo, input_layers['HUC12'])
                            besp_output[metrics[metric]['field_name']] = subwatsid

                        if metric == 'HEDWTR':
                            is_headwater = headwater(feat_seg_dgo, line_network)
                            besp_output[metrics[metric]['field_name']] = is_headwater

                        if metric == 'STRMLENGTH':
                            leng = total_stream_length(feat_geom, line_network, transform)
                            besp_output[metrics[metric]['field_name']] = leng

                        if metric == 'WATEREXT':
                            area = waterbody_extent(feat_geom, input_layers['WATERBODIES'], transform)
                            besp_output[metrics[metric]['field_name']] = area

                        if metric == 'STRMGRAD':
                            grad = calculate_gradient(curs, dgo_id)
                            besp_output[metrics[metric]['field_name']] = grad

                        if metric == 'VALGRAD':
                            grad = calculate_gradient(curs, dgo_id, channel=False)
                            besp_output[metrics[metric]['field_name']] = grad

                        if metric == 'RELFLWLNGTH':
                            rel_len = rel_flow_length(feat_seg_dgo, line_network, transform)
                            besp_output[metrics[metric]['field_name']] = rel_len

                        if metric == 'LFEVT':
                            evt = landfire_classes(feat_seg_dgo, curs)
                            classes = ','.join([str(c) for c in evt])
                            besp_output[metrics[metric]['field_name']] = classes

                        if metric == 'LFBPS':
                            bps = landfire_classes(feat_seg_dgo, curs, epoch=2)
                            classes = ','.join([str(c) for c in bps])
                            besp_output[metrics[metric]['field_name']] = classes

                        if metric == 'ELEV':
                            elev = get_elevation(feat_geom, dem)
                            besp_output[metrics[metric]['field_name']] = elev

                    set_clause = ', '.join([f"{k} = ?" for k in besp_output.keys()])
                    sql = f"""UPDATE dgo_{metric_group[1]} SET {set_clause} WHERE dgoid = {dgo_id}"""
                    curs.execute(sql, list(besp_output.values()))
                if dgo_id % 100 == 0:
                    conn.commit()

        conn.commit()

    # calculate secondary metrics for dgo table
    secondary_metrics = generate_metric_list(outputs_gpkg, primary=0)
    with sqlite3.connect(outputs_gpkg) as conn:
        curs = conn.cursor()
        curs.execute("SELECT machine_code, field_name from metrics")
        lf_field_names = {row[0]: row[1] for row in curs.fetchall() if row[0][0] == 'L' and row[0][1] == 'F'}
        for metric in secondary_metrics:
            if metric == 'TRIBDENS':
                curs.execute("SELECT dgo_geomorph.dgoid, tributaries, valleng FROM dgo_geomorph LEFT JOIN dgo_measurements ON dgo_geomorph.dgoid = dgo_measurements.dgoid")
                data = curs.fetchall()
                for dgo_id, tribs, length in data:
                    if tribs is not None and length is not None:
                        trib_density = float(tribs) / float(length)
                        curs.execute(f"""UPDATE dgo_geomorph SET tribs_per_km = {trib_density} WHERE dgoid = {dgo_id}""")
            if metric == 'CHANSIN':
                curs.execute("SELECT dgoid, strmleng, strmstrleng FROM dgo_measurements")
                data = curs.fetchall()
                for dgo_id, stream_length, straight_length in data:
                    if stream_length is not None and straight_length is not None:
                        sin = float(stream_length) / float(straight_length)
                        curs.execute(f"UPDATE dgo_geomorph SET planform_sinuosity = {sin} WHERE dgoid = {dgo_id}")
            if metric in ('LFAG', 'LFCON', 'LFCONHW', 'LFDEV', 'LFEXOTH', 'LFEXTSH', 'LFGRASS', 'LFHW', 'LFRIP', 'LFSHRUB', 'LFSPARSE', 'LFCONBPS', 'LFCONHWBPS', 'LFGRASSBPS', 'LFHWBPS', 'LFHWCONBPS', 'LFPEATBPS', 'LFPEATNONBPS', 'LFRIPBPS', 'LFSAVBPS', 'LFSHRUBBPS', 'LFSPARSEBPS'):
                curs.execute(f"SELECT dgo_veg.dgoid, {lf_field_names[metric]}_prop, segment_area from dgo_veg LEFT JOIN dgos on dgos.dgoid = dgo_veg.dgoid")
                data = curs.fetchall()
                for dgo_id, veg_class, area in data:
                    if veg_class is not None and area is not None:
                        veg_area = float(veg_class) * float(area)
                        curs.execute(f"UPDATE dgo_veg SET {lf_field_names[metric]} = {veg_area} WHERE dgoid = {dgo_id}")
            if metric == 'ACRESVBPM':
                curs.execute("SELECT dgoid, segment_area, centerline_length FROM dgos")
                data = curs.fetchall()
                for dgo_id, area, length in data:
                    if area is not None and length is not None and length > 0:
                        acres = (float(area) * 0.000247105) / (float(length) * 0.000621371)
                        curs.execute(f"UPDATE dgo_geomorph SET acres_vb_per_mile = {acres} WHERE dgoid = {dgo_id}")
            if metric == 'HECTVBPKM':
                curs.execute("SELECT dgoid, segment_area, centerline_length FROM dgos")
                data = curs.fetchall()
                for dgo_id, area, length in data:
                    if area is not None and length is not None and length > 0:
                        hectares = (float(area) * 0.0001) / (float(length) * 0.001)
                        curs.execute(f"UPDATE dgo_geomorph SET hect_vb_per_km = {hectares} WHERE dgoid = {dgo_id}")
            if metric == 'STRMSIZE':
                curs.execute("SELECT dgo_geomorph.dgoid, channel_area, strmleng FROM dgo_geomorph LEFT JOIN dgo_measurements ON dgo_geomorph.dgoid = dgo_measurements.dgoid")
                data = curs.fetchall()
                for dgo_id, area, length in data:
                    if area is not None and length is not None and length > 0:
                        size = float(area) / float(length)
                        curs.execute(f"UPDATE dgo_geomorph SET channel_width = {size} WHERE dgoid = {dgo_id}")
            if metric == 'ROADDENS':
                curs.execute("SELECT dgo_impacts.dgoid, road_len, centerline_length FROM dgo_impacts LEFT JOIN dgos on dgos.dgoid = dgo_impacts.dgoid")
                data = curs.fetchall()
                for dgo_id, road_len, length in data:
                    if road_len is not None and length is not None and length > 0:
                        road_density = float(road_len) / float(length)
                        curs.execute(f"UPDATE dgo_impacts SET road_dens = {road_density} WHERE dgoid = {dgo_id}")
            if metric == 'RAILDENS':
                curs.execute("SELECT dgo_impacts.dgoid, rail_len, centerline_length FROM dgo_impacts LEFT JOIN dgos on dgos.dgoid = dgo_impacts.dgoid")
                data = curs.fetchall()
                for dgo_id, rail_len, length in data:
                    if rail_len is not None and length is not None and length > 0:
                        rail_density = float(rail_len) / float(length)
                        curs.execute(f"UPDATE dgo_impacts SET rail_dens = {rail_density} WHERE dgoid = {dgo_id}")
            if metric == 'ACFPEXT':
                curs.execute("SELECT dgo_impacts.dgoid, fldpln_access, segment_area FROM dgo_impacts LEFT JOIN dgos on dgos.dgoid = dgo_impacts.dgoid")
                data = curs.fetchall()
                for dgo_id, prop, area in data:
                    if prop is not None and area is not None and area > 0:
                        acpf = float(prop) * float(area)
                        curs.execute(f"UPDATE dgo_impacts SET access_fldpln_extent = {acpf} WHERE dgoid = {dgo_id}")
            if metric == 'EXRIP':
                curs.execute("SELECT dgo_veg.dgoid, prop_riparian, segment_area FROM dgo_veg LEFT JOIN dgos on dgos.dgoid = dgo_veg.dgoid")
                data = curs.fetchall()
                for dgo_id, prop, area in data:
                    if prop is not None and area is not None and area > 0:
                        exrip = float(prop) * float(area)
                        curs.execute(f"UPDATE dgo_veg SET ex_riparian = {exrip} WHERE dgoid = {dgo_id}")
            if metric == 'HISTRIP':
                curs.execute("SELECT dgo_veg.dgoid, hist_prop_riparian, segment_area FROM dgo_veg LEFT JOIN dgos on dgos.dgoid = dgo_veg.dgoid")
                data = curs.fetchall()
                for dgo_id, prop, area in data:
                    if prop is not None and area is not None and area > 0:
                        hisrip = float(prop) * float(area)
                        curs.execute(f"UPDATE dgo_veg SET hist_riparian = {hisrip} WHERE dgoid = {dgo_id}")
        conn.commit()

    # fill out igo_metrics table using moving window analysis
    for metric_group in metric_groups:
        log.info(f'Calculating {metric_group[1]} metrics for IGOs')

        metrics = generate_metric_list(outputs_gpkg, metric_group[0], primary=None)
        with sqlite3.connect(outputs_gpkg) as conn:
            curs = conn.cursor()

            progbar = ProgressBar(len(windows), 50, f'Caclulating {metric_group[1]} metrics for IGOs')
            counter = 0
            for igo_id, dgo_ids in windows.items():
                counter += 1
                progbar.update(counter)
                bespoke_method_metrics = []
                metrics_output = {}
                for metric in metrics:
                    if metrics[metric]['window_calc_id'] not in mw_metric_functions.keys():
                        bespoke_method_metrics.append(metric)
                        continue

                    if metrics[metric]['window_calc_id'] == 1:
                        if igo_id not in igo_dgo.keys():
                            continue
                        input_args = mw_input_datasets[metrics[metric]['metric_id']]
                        input_args = [item for item in input_args if item]
                        input_args.insert(0, igo_dgo[igo_id])
                        input_args[1] = os.path.join(project_folder, input_args[1])
                        if not os.path.exists(input_args[1]) and not os.path.exists(os.path.dirname(input_args[1])):
                            log.error(f"Input file {input_args[1]} does not exist, unable to calucalate {metrics[metric]['metric_name']}")
                            continue
                    else:
                        input_args = mw_input_datasets[metrics[metric]['metric_id']]
                        input_args = [item for item in input_args if item]
                        input_args.insert(0, dgo_ids)
                        input_args[1] = os.path.join(project_folder, input_args[1])
                        if not os.path.exists(input_args[1]) and not os.path.exists(os.path.dirname(input_args[1])):
                            log.error(f"Input file {input_args[1]} does not exist, unable to calucalate {metrics[metric]['metric_name']}")
                            continue

                    window_calc_id = metrics[metric]['window_calc_id']
                    if window_calc_id not in mw_metric_functions.keys():
                        continue
                    input_args.insert(0, curs)
                    val = call_function(mw_metric_functions[window_calc_id], *input_args)
                    if val is None:
                        metrics_output[metrics[metric]['field_name']] = None
                    else:
                        if metrics[metric]['data_type'] == 'REAL':
                            metrics_output[metrics[metric]['field_name']] = float(val)
                        elif metrics[metric]['data_type'] == 'INTEGER':
                            metrics_output[metrics[metric]['field_name']] = int(val)
                        else:
                            metrics_output[metrics[metric]['field_name']] = str(val)

                if len(metrics_output) > 0:
                    field_names = ', '.join(metrics_output.keys())
                    placeholders = ', '.join(['?'] * len(metrics_output))
                    sql = f"""INSERT INTO igo_{metric_group[1]} (igoid, {field_names}) VALUES ({igo_id}, {placeholders})"""
                    curs.execute(sql, list(metrics_output.values()))
                else:
                    log.warning(f"No {metric_group[1]} metrics calculated for IGO {igo_id}")

                if len(bespoke_method_metrics) > 0:
                    besp_output = {}
                    for metric in bespoke_method_metrics:
                        if metric == 'STRMGRAD':
                            grad = mw_calculate_gradient(curs, dgo_ids)
                            besp_output[metrics[metric]['field_name']] = grad
                        if metric == 'VALGRAD':
                            grad = mw_calculate_gradient(curs, dgo_ids, channel=False)
                            besp_output[metrics[metric]['field_name']] = grad
                        if metric == 'CHANSIN':
                            sin = mw_calculate_sinuosity(curs, dgo_ids)
                            besp_output[metrics[metric]['field_name']] = sin
                        if metric == 'ACRESVBPM':
                            acres = mw_acres_per_mi(curs, dgo_ids)
                            besp_output[metrics[metric]['field_name']] = acres
                        if metric == 'HECTVBPKM':
                            hectares = mw_hect_per_km(curs, dgo_ids)
                            besp_output[metrics[metric]['field_name']] = hectares
                        if metric == 'LOWFLOWSP':
                            sp_low = mw_stream_power(curs, dgo_ids)
                            besp_output[metrics[metric]['field_name']] = sp_low
                        if metric == 'HIGHFLOWSP':
                            sp_high = mw_stream_power(curs, dgo_ids, q='Q2')
                            besp_output[metrics[metric]['field_name']] = sp_high
                        if metric == 'RVD':
                            rvd = mw_rvd(curs, dgo_ids)
                            besp_output[metrics[metric]['field_name']] = rvd

                    set_clause = ', '.join([f"{k} = ?" for k in besp_output.keys()])
                    sql = f"""UPDATE igo_{metric_group[1]} SET {set_clause} WHERE igoid = {igo_id}"""
                    curs.execute(sql, list(besp_output.values()))

            conn.commit()

    with sqlite3.connect(outputs_gpkg) as conn:
        curs = conn.cursor()
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_dgo_beaver_metrics', data_type, 'beaver_dgos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_dgo_beaver_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_dgo_desc_metrics', data_type, 'desc_dgos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_dgo_desc_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_dgo_geomorph_metrics', data_type, 'geomorph_dgos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_dgo_geomorph_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_dgo_hydro_metrics', data_type, 'hydro_dgos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_dgo_hydro_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_dgo_impacts_metrics', data_type, 'impacts_dgos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_dgo_impacts_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_dgo_veg_metrics', data_type, 'veg_dgos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_dgo_veg_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_dgo_metrics', data_type, 'dgo_metrics', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'dgos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_dgo_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'dgos'""")

        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_igo_beaver_metrics', data_type, 'beaver_igos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_igo_beaver_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_igo_desc_metrics', data_type, 'desc_igos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_igo_desc_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_igo_geomorph_metrics', data_type, 'geomorph_igos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_igo_geomorph_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_igo_hydro_metrics', data_type, 'hydro_igos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_igo_hydro_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_igo_impacts_metrics', data_type, 'impacts_igos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_igo_impacts_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_igo_veg_metrics', data_type, 'veg_igos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_igo_veg_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) 
                     SELECT 'vw_igo_metrics', data_type, 'igo_metrics', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'igos'""")
        curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) 
                     SELECT 'vw_igo_metrics', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'igos'""")
        conn.commit()

    # Add nodes to the project
    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    project.add_project_geopackage(
        proj_nodes['Outputs'], LayerTypes['OUTPUTS'])

    ellapsed = time.time() - start_time
    project.add_metadata([
        RSMeta("ProcTimeS", "{:.2f}".format(ellapsed),
               RSMetaTypes.HIDDEN, locked=True),
        RSMeta("Processing Time", pretty_duration(ellapsed), locked=True)
    ])

    add_layer_descriptions(project, LYR_DESCRIPTIONS_JSON, LayerTypes)

    # None will create the base report with no filter on the data
    # filter_names = [None] + FILTER_NAMES

    # for filter_name in filter_names:
    #     report_suffix = f"_{filter_name.upper()}" if filter_name is not None else ""

    #     # Write a report
    #     report_path = os.path.join(
    #         project.project_dir, LayerTypes[f'REPORT{report_suffix}'].rel_path
    #     )
    #     project.add_report(
    #         proj_nodes['Outputs'],
    #         LayerTypes[f'REPORT{report_suffix}'], replace=True
    #     )
    #     report = RMEReport(outputs_gpkg, report_path, project, filter_name, intermediates_gpkg)
    #     report.write()

    log.info('Riverscapes Metric Engine Finished')
    return


def generate_metric_list(database: Path, group_id: int = None, source_table: str = 'metrics', primary: int = 1) -> dict:
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
        if group_id:
            if primary:
                metric_data = curs.execute(
                    f"""SELECT * from {source_table} WHERE is_active = 1 AND metric_group_id = {group_id} and primary_metric = {primary}""").fetchall()
            else:
                metric_data = curs.execute(
                    f"""SELECT * from {source_table} WHERE is_active = 1 and metric_group_id = {group_id}""").fetchall()
        else:
            if primary:
                metric_data = curs.execute(
                    f"""SELECT * from {source_table} WHERE is_active = 1 AND primary_metric = {primary}""").fetchall()
            else:
                metric_data = curs.execute(
                    f"""SELECT * from {source_table} WHERE is_active = 1""").fetchall()
        metrics = {metric['machine_code']: metric for metric in metric_data}
    return metrics


def copy_table(source_db_path, dest_db_path, table_name):
    # Connect to the source database
    source_conn = sqlite3.connect(source_db_path)
    source_cursor = source_conn.cursor()

    # Connect to the destination database
    dest_conn = sqlite3.connect(dest_db_path)
    dest_cursor = dest_conn.cursor()

    # Create the same table structure in the destination database (if it doesn't exist)
    source_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    create_table_sql = source_cursor.fetchone()[0]
    dest_cursor.execute(create_table_sql)

    # Copy the data from the source table to the destination table
    source_cursor.execute(f"SELECT * FROM {table_name};")
    rows = source_cursor.fetchall()

    # Insert rows into the destination table
    placeholders = ', '.join(['?'] * len(rows[0]))  # Create placeholders for each column
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders});"
    dest_cursor.executemany(insert_sql, rows)
    dest_cursor.execute(f"INSERT INTO gpkg_contents (table_name, data_type) VALUES ('{table_name}', 'attributes')")

    # Commit changes and close connections
    dest_conn.commit()
    source_conn.close()
    dest_conn.close()


def main():
    """Run Riverscapes Metric Engine"""

    parser = argparse.ArgumentParser(description='Riverscapes Metric Engine')

    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('flowlines', help='NHD Flowlines (.shp, .gpkg/layer_name)', type=str)
    parser.add_argument('waterbodies', help='NHD Waterbodies', type=str)
    parser.add_argument('huc12', help='HUC12 feature class', type=str)
    parser.add_argument('vaa_table', help='NHD Plus vaa table')
    parser.add_argument('counties', help='Counties shapefile')
    parser.add_argument('geology', help='Geology feature class')
    parser.add_argument('vbet_dgos', help='vbet segment polygons')
    parser.add_argument('vbet_igos', help='valley bottom or other polygon representing confining boundary (.shp, .gpkg/layer_name)', type=str)
    parser.add_argument('valley_centerline', help='vbet centerline feature class')
    parser.add_argument('dem', help='dem')
    parser.add_argument('hillshade', help='hillshade')
    parser.add_argument('output_folder', help='Output folder', type=str)
    parser.add_argument('--confinement_dgos', help='confinement dgos', type=str)
    parser.add_argument('--hydro_dgos', help='hydro dgos', type=str)
    parser.add_argument('--anthro_dgos', help='anthro dgos', type=str)
    parser.add_argument('--anthro_lines', help='anthro lines', type=str)
    parser.add_argument('--rcat_dgos', help='rcat_dgos', type=str)
    parser.add_argument('--rcat_dgo_table', help='The DGOVegetation table from rcat output gpkg', type=str)
    parser.add_argument('--brat_dgos', help='brat dgos', type=str)
    parser.add_argument('--brat_lines', help='brat lines', type=str)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
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
                                         args.waterbodies,
                                         args.huc12,
                                         args.vaa_table,
                                         args.counties,
                                         args.geology,
                                         args.vbet_dgos,
                                         args.vbet_igos,
                                         args.valley_centerline,
                                         args.dem,
                                         args.hillshade,
                                         args.output_folder,
                                         args.confinement_dgos,
                                         args.hydro_dgos,
                                         args.anthro_dgos,
                                         args.anthro_lines,
                                         args.rcat_dgos,
                                         args.rcat_dgo_table,
                                         args.brat_dgos,
                                         args.brat_lines,
                                         meta=meta)
            log.debug(f'Return code: {retcode}, [Max process usage] {max_obj}')

        else:
            metric_engine(args.huc,
                          args.flowlines,
                          args.waterbodies,
                          args.huc12,
                          args.vaa_table,
                          args.counties,
                          args.geology,
                          args.vbet_dgos,
                          args.vbet_igos,
                          args.valley_centerline,
                          args.dem,
                          args.hillshade,
                          args.output_folder,
                          args.confinement_dgos,
                          args.hydro_dgos,
                          args.anthro_dgos,
                          args.anthro_lines,
                          args.rcat_dgos,
                          args.rcat_dgo_table,
                          args.brat_dgos,
                          args.brat_lines,
                          meta=meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
