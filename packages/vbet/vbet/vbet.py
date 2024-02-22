""" Name:     Valley Bottom Extraction Tool (VBET)

    Purpose:  Generate Valley Bottom Polygons with centerline per level path. This
              version uses a raster-based approach for centerlines. Segmented
              polygons from points along the centerline are also generated.

    Author:   Kelly Whitehead

    Date:     Apr 11, 2022
"""

import os
import sys
import argparse
import traceback
import sqlite3
import shutil
from copy import deepcopy
import warnings
from math import ceil

from osgeo import ogr
import rasterio
from rasterio.windows import Window
from shapely.geometry import box
import numpy as np

from rscommons import RSProject, RSLayer, ModelConfig, ProgressBar, Logger, GeopackageLayer, dotenv, VectorBase, initGDALOGRErrors
from rscommons.vector_ops import copy_feature_class, polygonize, difference, collect_linestring, collect_feature_class
from rscommons.geometry_ops import get_extent_as_geom, get_rectangle_as_geom
from rscommons.util import safe_makedirs, parse_metadata, pretty_duration, safe_remove_dir
from rscommons.hand import run_subprocess
from rscommons.vbet_network import get_channel_level_path, get_distance_lookup, vbet_network
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons.raster_warp import raster_warp
from rscommons import TimerBuckets, TimerWaypoints
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions, raster_resolution_meta

from vbet.vbet_database import build_vbet_database, load_configuration
from vbet.vbet_raster_ops import rasterize, raster_logic_mask, raster_update_multiply, raster_remove_zone, get_endpoints_on_raster, generate_vbet_polygon, generate_centerline_surface, clean_raster_regions, proximity_raster
from vbet.vbet_outputs import clean_up_centerlines
from vbet.vbet_report import VBETReport
from vbet.vbet_segmentation import calculate_dgo_metrics, generate_igo_points, split_vbet_polygons, calculate_vbet_window_metrics, add_fcodes
from vbet.lib.CompositeRaster import CompositeRaster
from vbet.__version__ import __version__

from .lib.cost_path import least_cost_path
from .lib.raster2line import raster2line_geom

Path = str

# "8"
NCORES = os.environ['TAUDEM_CORES'] if 'TAUDEM_CORES' in os.environ else '2'
BIG_TIFF_THRESH = 3800000000
initGDALOGRErrors()

cfg = ModelConfig(
    'https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(
    os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'inputs/dem.tif'),
    'SLOPE_RASTER': RSLayer('Slope Raster', 'SLOPE_RASTER', 'Raster', 'inputs/slope.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/vbet_inputs.gpkg', {
        'NETWORK_INTERSECTION': RSLayer('NHD Flowlines intersected with road, rail and ownership', 'NETWORK_INTERSECTION', 'Vector', 'network_intersected'),
        # 'FLOWLINES': RSLayer('NHD Flowlines intersected with road, rail and ownership', 'FLOWLINES', 'Vector', 'flowlines'),
        'CHANNEL_AREA_POLYGONS': RSLayer('Channel Area Polygons', 'CHANNEL_AREA_POLYGONS', 'Vector', 'channel_area_polygons')
    }),
    # Taudem intermediate rasters can be provided as inputs, or generated in vbet
    'PITFILL': RSLayer('TauDEM Pitfill', 'PITFILL', 'Raster', 'intermediates/pitfill.tif'),
    'DINFFLOWDIR_ANG': RSLayer('TauDEM D-Inf Flow Directions', 'DINFFLOWDIR_ANG', 'Raster', 'intermediates/dinfflowdir_ang.tif'),
    'DINFFLOWDIR_SLP': RSLayer('TauDEM D-Inf Flow Directions Slope', 'DINFFLOWDIR_SLP', 'Raster', 'intermediates/dinfflowdir_slp.tif'),
    'INTERMEDIATES': RSLayer('Intermediates', 'Intermediates', 'Geopackage', 'intermediates/vbet_intermediates.gpkg', {
        'VBET_DGO_POLYGONS': RSLayer('VBET DGO Polygons', 'VBET_DGO_POLYGONS', 'Vector', 'vbet_dgos')
        # We also add all tht raw thresholded shapes here but they get added dynamically later
    }),
    # Same here. Sub layers are added dynamically later.
    'COMPOSITE_VBET_EVIDENCE': RSLayer('VBET Evidence Raster', 'VBET_EVIDENCE', 'Raster', 'outputs/vbet_evidence.tif'),
    'COMPOSITE_VBET_EVIDENCE_INTERIOR': RSLayer('Topo Evidence (Interior)', 'EVIDENCE_TOPO_INTERIOR', 'Raster', 'intermediates/topographic_evidence_interior.tif'),

    'COMPOSITE_HAND': RSLayer('Hand Raster', 'HAND_RASTER', 'Raster', 'intermediates/hand_composite.tif'),
    'COMPOSITE_HAND_INTERIOR': RSLayer('Hand Raster (Interior)', 'HAND_RASTER_INTERIOR', 'Raster', 'intermediates/hand_composite_interior.tif'),

    'TRANSFORMED_HAND': RSLayer('Transformed HAND Evidence', 'TRANSFORMED_HAND', 'Raster', 'intermediates/hand_transformed.tif'),
    'TRANSFORMED_HAND_INTERIOR': RSLayer('Transformed HAND Evidence (Interior)', 'TRANSFORMED_HAND_INTERIOR', 'Raster', 'intermediates/hand_transformed_interior.tif'),

    'EVIDENCE_TOPO': RSLayer('Topo Evidence', 'EVIDENCE_TOPO', 'Raster', 'intermediates/topographic_evidence.tif'),

    'TRANSFORMED_SLOPE': RSLayer('Transformed Slope', 'TRANSFORMED_SLOPE', 'Raster', 'intermediates/slope_transformed.tif'),
    'TRANSFORMED_SLOPE_INTERIOR': RSLayer('Transformed Slope (Interior)', 'TRANSFORMED_SLOPE_INTERIOR', 'Raster', 'intermediates/slope_transformed_interior.tif'),

    'VBET_ZONES': RSLayer('VBET LevelPath Zones', 'VBET_ZONES', 'Raster', 'intermediates/vbet_level_path_zones.tif'),
    'LOWLYING_FP_ZONES': RSLayer('Active Floodplain LevelPath Zones', 'LOWLYING_FP_ZONES', 'Raster', 'intermediates/lowlying_fp_level_path_zones.tif'),
    'ELEVATED_FP_ZONES': RSLayer('Inactive Floodplain LevelPath Zones', 'ELEVATED_FP_ZONES', 'Raster', 'intermediates/elevated_fp_level_path_zones.tif'),

    'VBET_OUTPUTS': RSLayer('VBET', 'VBET_OUTPUTS', 'Geopackage', 'outputs/vbet.gpkg', {
        'VBET_FULL': RSLayer('VBET Full Extent', 'VBET_FULL', 'Vector', 'vbet_full'),
        'VBET_IA': RSLayer('VBET Low lying/Elevated Boundary', 'VBET_IA', 'Vector', 'low_lying_valley_bottom'),
        'LOW_LYING_FLOODPLAIN': RSLayer('Low Lying Floodplain', 'LOW_LYING_FLOODPLAIN', 'Vector', 'low_lying_floodplain'),
        'ELEVATED_FLOODPLAIN': RSLayer('Elevated Floodplain', 'ELEVATED_FLOODPLAIN', 'Vector', 'elevated_floodplain'),
        'FLOODPLAIN': RSLayer('Floodplain', 'FLOODPLAIN', 'Vector', 'floodplain'),
        'VBET_CENTERLINES': RSLayer('VBET Centerline', 'VBET_CENTERLINES', 'Vector', 'vbet_centerlines'),
        'SEGMENTATION_POINTS': RSLayer('Segmentation Points', 'SEGMENTATION_POINTS', 'Vector', 'vbet_igos')
    }),
    'REPORT': RSLayer('RSContext Report', 'REPORT', 'HTMLFile', 'outputs/vbet.html')
}


def vbet(in_line_network, in_dem, in_slope, in_hillshade, in_channel_area, project_folder, huc, flowline_type='NHD',
         unique_stream_field='level_path', unique_reach_field='NHDPlusID', drain_area_field='DivDASqKm',
         level_paths=None, in_pitfill_dem=None, in_dinfflowdir_ang=None, in_dinfflowdir_slp=None, meta=None, debug=False,
         reach_codes=None, mask=None, temp_folder=None):
    """Run VBET"""

    thresh_vals = {'VBET_IA': 0.85, 'VBET_FULL': 0.65}
    _tmr_waypt = TimerWaypoints()
    log = Logger('VBET')
    log.info(f'Starting VBET v.{cfg.version}')

    # This could be a re-run and we need to clear out the tmp folders
    if os.path.isdir(temp_folder):
        safe_remove_dir(temp_folder)
    intermediates_dir = os.path.dirname(os.path.join(
        project_folder, LayerTypes['INTERMEDIATES'].rel_path))
    if os.path.isdir(intermediates_dir):
        safe_remove_dir(intermediates_dir)
    safe_makedirs(intermediates_dir)
    # Wipe the outputs directory so we don't get any bleed over from the last run
    outputs_dir = os.path.dirname(os.path.join(
        project_folder, LayerTypes['VBET_OUTPUTS'].rel_path))
    if os.path.isdir(outputs_dir):
        safe_remove_dir(outputs_dir)
    safe_makedirs(outputs_dir)

    with rasterio.open(in_dem) as dem_src:
        if dem_src.crs != cfg.OUTPUT_EPSG:
            cfg.OUTPUT_EPSG = dem_src.crs

    project_name = f'VBET for HUC {huc}'
    project = RSProject(cfg, project_folder)
    project.create(project_name, 'VBET', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/vbet',
               RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True),
        RSMeta("Flowline Type", flowline_type),
        RSMeta("Low Lying Valley Threshold",
               f"{int(thresh_vals['VBET_IA'] * 100)}", RSMetaTypes.INT, locked=True),
        RSMeta("Elevated Valley Threshold",
               f"{int(thresh_vals['VBET_FULL'] * 100)}", RSMetaTypes.INT, locked=True)
    ], meta)

    augment_layermeta('vbet', LYR_DESCRIPTIONS_JSON, LayerTypes)

    # save raster nodes and paths for adding resolution metadata later
    proj_rasters = []

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=[
                                                       'Inputs', 'Intermediates', 'Outputs'], create_folders=True)

    log.info('Preparing inputs:')
    inputs_gpkg = os.path.join(project_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg = os.path.join(
        project_folder, LayerTypes['INTERMEDIATES'].rel_path)
    vbet_gpkg = os.path.join(
        project_folder, LayerTypes['VBET_OUTPUTS'].rel_path)
    GeopackageLayer.delete(inputs_gpkg)
    GeopackageLayer.delete(intermediates_gpkg)
    GeopackageLayer.delete(vbet_gpkg)

    tmp_line_network = os.path.join(project_folder, 'tmp.gpkg', 'flowlines_tmp')
    line_network = os.path.join(
        inputs_gpkg, LayerTypes['INPUTS'].sub_layers['NETWORK_INTERSECTION'].rel_path)

    clip_mask = None
    if mask is not None:
        clip_mask = collect_feature_class(mask)
        clip_mask = clip_mask.MakeValid()

    log.info('Preparing network:')
    copy_feature_class(in_line_network, tmp_line_network,
                       epsg=cfg.OUTPUT_EPSG, clip_shape=clip_mask)

    lp_fcodes = {}  # if its only canal and art path, the level path will get removed from the run
    lps_to_remove = []
    with GeopackageLayer(tmp_line_network) as lyr:
        for feat, *_ in lyr.iterate_features():
            if feat.GetField(unique_stream_field) not in lp_fcodes:
                lp_fcodes[feat.GetField(unique_stream_field)] = [feat.GetField('FCode')]
            else:
                if feat.GetField('FCode') not in lp_fcodes[feat.GetField(unique_stream_field)]:
                    lp_fcodes[feat.GetField(unique_stream_field)].append(feat.GetField('FCode'))
    for lp, fcs in lp_fcodes.items():
        if fcs == [33600, 55800] or fcs == [55800, 33600]:
            if lp is not None:
                lps_to_remove.append(str(int(lp)))

    vbet_network(tmp_line_network, None, line_network, epsg=cfg.OUTPUT_EPSG,
                 fcodes=reach_codes, hard_clip_shape=clip_mask)

    GeopackageLayer.delete(os.path.dirname(tmp_line_network))

    channel_area = os.path.join(
        inputs_gpkg, LayerTypes['INPUTS'].sub_layers['CHANNEL_AREA_POLYGONS'].rel_path)
    copy_feature_class(in_channel_area, channel_area, epsg=cfg.OUTPUT_EPSG)

    log.info('Building VBET Database')
    build_vbet_database(inputs_gpkg)
    vbet_run = load_configuration(inputs_gpkg)

    get_channel_level_path(channel_area, line_network,
                           unique_stream_field, unique_reach_field)

    in_rasters = {}

    # check that rasters have same crs
    with rasterio.open(in_dem) as dem_src, rasterio.open(in_slope) as slope_src, rasterio.open(in_hillshade) as hillshade_src:
        dem_crs = dem_src.crs
        if dem_src.crs != slope_src.crs or dem_src.crs != hillshade_src.crs:
            raise Exception(
                'DEM, slope, and hillshade rasters must have the same projection.')

    _proj_hillshade_node, _hillshade = project.add_project_raster(
        proj_nodes['Inputs'], LayerTypes['HILLSHADE'], in_hillshade, replace=True)
    _proj_dem_node, dem = project.add_project_raster(
        proj_nodes['Inputs'], LayerTypes['DEM'], in_dem, replace=True)
    _proj_slope_node, in_rasters['Slope'] = project.add_project_raster(
        proj_nodes['Inputs'], LayerTypes['SLOPE_RASTER'], in_slope, replace=True)

    # Use the size of the DEM to guess if we need the bigTIFF flag for files at or near 4Gb
    use_big_tiff = os.path.getsize(dem) > BIG_TIFF_THRESH

    # generate top level taudem products if they do not exist
    if in_pitfill_dem is None:
        pitfill_dem = os.path.join(
            project_folder, LayerTypes['PITFILL'].rel_path)
        pitfill_status = run_subprocess(project_folder, [
                                        "mpiexec", "-n", NCORES, "pitremove", "-z", dem, "-fel", pitfill_dem])
        if pitfill_status != 0 or not os.path.isfile(pitfill_dem):
            raise Exception('TauDEM: pitfill failed')
        _proj_pitfill_node, pitfill_dem = project.add_project_raster(
            proj_nodes['Intermediates'], LayerTypes['PITFILL'])
        proj_rasters.append([_proj_pitfill_node, pitfill_dem])
    else:
        with rasterio.open(in_pitfill_dem) as pitfill_src:
            if pitfill_src.crs != dem_crs:
                raise Exception(
                    'Input Pitfill raster must have the same projection as the DEM')
        _proj_pitfill_node, pitfill_dem = project.add_project_raster(
            proj_nodes['Intermediates'], LayerTypes['PITFILL'], in_pitfill_dem, replace=True)

    if not all([in_dinfflowdir_ang, in_dinfflowdir_slp]):
        dinfflowdir_slp = os.path.join(
            project_folder, LayerTypes['DINFFLOWDIR_SLP'].rel_path)
        dinfflowdir_ang = os.path.join(
            project_folder, LayerTypes['DINFFLOWDIR_ANG'].rel_path)
        dinfflowdir_status = run_subprocess(project_folder, [
                                            "mpiexec", "-n", NCORES, "dinfflowdir", "-fel", pitfill_dem, "-ang", dinfflowdir_ang, "-slp", dinfflowdir_slp])
        if dinfflowdir_status != 0 or not os.path.isfile(dinfflowdir_ang):
            raise Exception('TauDEM: dinfflowdir failed')
        _proj_dinfflowdir_ang_node, dinfflowdir_ang = project.add_project_raster(
            proj_nodes['Intermediates'], LayerTypes['DINFFLOWDIR_ANG'])
        _proj_dinfflowdir_slp_node, dinfflowdir_slp = project.add_project_raster(
            proj_nodes['Intermediates'], LayerTypes['DINFFLOWDIR_SLP'])
        proj_rasters.extend([[_proj_dinfflowdir_ang_node, dinfflowdir_ang], [
                            _proj_dinfflowdir_slp_node, dinfflowdir_slp]])
    else:
        with rasterio.open(in_dinfflowdir_ang) as fdir_src:
            if fdir_src.crs != dem_crs:
                raise Exception(
                    'Input Dinf Flow Direction raster must have the same projection as the DEM')
        _proj_dinfflowdir_ang_node, dinfflowdir_ang = project.add_project_raster(
            proj_nodes['Intermediates'], LayerTypes['DINFFLOWDIR_ANG'], in_dinfflowdir_ang, replace=True)
        _proj_dinfflowdir_slp_node, dinfflowdir_slp = project.add_project_raster(
            proj_nodes['Intermediates'], LayerTypes['DINFFLOWDIR_SLP'], in_dinfflowdir_slp, replace=True)

    log.info('Writing Topo Evidence raster for project')
    read_rasters = {name: rasterio.open(raster)
                    for name, raster in in_rasters.items()}
    out_meta = read_rasters['Slope'].meta
    out_meta['driver'] = 'GTiff'
    out_meta['count'] = 1
    out_meta['compress'] = 'deflate'
    size_x = read_rasters['Slope'].width
    size_y = read_rasters['Slope'].height
    pixel_x, _pixel_y = read_rasters['Slope'].res
    srs = read_rasters['Slope'].crs
    espg = srs.to_epsg()

    empty_array = np.empty((size_x, size_y), dtype=np.int32)
    empty_array.fill(out_meta['nodata'])
    if use_big_tiff:
        out_meta['BIGTIFF'] = 'YES'

    int_meta = deepcopy(out_meta)
    int_meta['dtype'] = 'int32'

    # Initialize empty zone rasters
    vbet_zone_raster = os.path.join(
        project_folder, LayerTypes['VBET_ZONES'].rel_path)
    active_zone_raster = os.path.join(
        project_folder, LayerTypes['LOWLYING_FP_ZONES'].rel_path)
    # Difference raster created later on...
    inactive_zone_raster = os.path.join(
        project_folder, LayerTypes['ELEVATED_FP_ZONES'].rel_path)
    for raster in [vbet_zone_raster, active_zone_raster]:
        with rasterio.open(raster, 'w', **int_meta) as rio:
            rio.write(empty_array, 1)
    _vbet_zones_node, vbet_zone_ras = project.add_project_raster(
        proj_nodes['Intermediates'], LayerTypes['VBET_ZONES'])
    _active_zones_node, active_zones_ras = project.add_project_raster(
        proj_nodes['Intermediates'], LayerTypes['LOWLYING_FP_ZONES'])
    _inactive_zones_node, inactive_zones_ras = project.add_project_raster(
        proj_nodes['Intermediates'], LayerTypes['ELEVATED_FP_ZONES'])
    proj_rasters.extend([[_vbet_zones_node, vbet_zone_ras], [
                        _active_zones_node, active_zones_ras], [_inactive_zones_node, inactive_zones_ras]])

    # Allow us to specify a temp folder outside our project folder
    temp_rasters_folder = os.path.join(temp_folder, 'rasters')
    safe_makedirs(temp_rasters_folder)
    level_path_keys = {}

    # Initialize Outputs
    output_centerlines = os.path.join(
        vbet_gpkg, LayerTypes['VBET_OUTPUTS'].sub_layers['VBET_CENTERLINES'].rel_path)
    temp_centerlines = os.path.join(
        temp_folder, 'raw_centerlines.gpkg', 'centerlines')
    output_vbet = os.path.join(
        vbet_gpkg, LayerTypes["VBET_OUTPUTS"].sub_layers['VBET_FULL'].rel_path)
    output_vbet_ia = os.path.join(
        vbet_gpkg, LayerTypes['VBET_OUTPUTS'].sub_layers['VBET_IA'].rel_path)
    with GeopackageLayer(temp_centerlines, write=True) as lyr_temp_cl_init, \
        GeopackageLayer(output_vbet, write=True) as lyr_vbet_init, \
        GeopackageLayer(output_vbet_ia, write=True) as lyr_active_vbet_init, \
            GeopackageLayer(line_network) as lyr_ref:
        fields = {f'{unique_stream_field}': ogr.OFTString}
        lyr_temp_cl_init.create_layer(
            ogr.wkbMultiLineString, spatial_ref=lyr_ref.spatial_ref, fields=fields)

        lyr_vbet_init.create_layer(
            ogr.wkbMultiPolygon, spatial_ref=lyr_ref.spatial_ref, fields=fields)
        lyr_active_vbet_init.create_layer(
            ogr.wkbMultiPolygon, spatial_ref=lyr_ref.spatial_ref, fields=fields)

    # Generate the list of level paths to run, sorted by ascending order and optional user filter
    level_paths_to_run = []
    with sqlite3.connect(inputs_gpkg) as conn:
        curs = conn.cursor()
        level_paths_raw = curs.execute(
            f"SELECT {unique_stream_field}, MAX({drain_area_field}) AS drainage FROM {os.path.basename(line_network)} GROUP BY {unique_stream_field} ORDER BY drainage DESC").fetchall()
        all_level_paths = list(str(int(lp[0])) for lp in level_paths_raw if lp[0] is not None)
        level_paths_drainage = {str(int(lp[0])): lp[1] for lp in level_paths_raw if lp[0] is not None}
        level_paths_drainage[None] = 10
        log.info(f'Found {len(all_level_paths)} potential level paths to run.')

        # If the user specified a set of level paths then we filter to those, ignoring any that aren't found with a warning
        if level_paths:
            for level_path in level_paths:
                if level_path in all_level_paths:
                    level_paths_to_run.append(level_path)
                else:
                    log.warning(
                        'Specified level path {level_path} not found. Skipping.')
        else:
            level_paths_to_run = all_level_paths
        all_level_paths = None

    for lp in level_paths_to_run:
        if lp in lps_to_remove:
            level_paths_to_run.remove(lp)

        # level_path_stream_order = dict([(str(int(row[0])), row[1]) for row in curs.execute("SELECT LevelPathI, MAX(StreamOrde) FROM NHDPlusFlowlineVAA GROUP BY LevelPathI ").fetchall()])
        # level_path_stream_order[None] = 1

    # process all polygons that aren't assigned a level path: ponds, waterbodies etc.
    if flowline_type == 'NHD':
        level_paths_to_run.append(None)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])

    # Generate max extent based on dem size
    with rasterio.open(dem) as raster:
        raster_bounds = raster.bounds
    bbox = box(*raster_bounds)
    raster_envelope_geom = VectorBase.shapely2ogr(bbox)
    vbet_clip_buffer_size = VectorBase.rough_convert_metres_to_raster_units(
        dem, 0.25)

    _tmr_waypt.timer_break('InputPrep')  # this is where input prep ends

    # Debug function for collecting information on level paths
    _tmtbuckets = TimerBuckets(
        table_name='level_path_debug',
        csv_path=os.path.join(project_folder, 'level_path_debug.csv'),
        active=log.isverbose()
    )

    # Convenience function for errors and collecting metadata
    def _tmterr(err_code: str, err_msg: str):
        _tmtbuckets.meta['code'] = err_code
        _tmtbuckets.meta['msg'] = err_msg
        _tmtbuckets.meta['has_centerline'] = False

    def _tmtfinish():
        if _tmtbuckets.meta['has_centerline'] is not False:
            _tmtbuckets.meta['has_centerline'] = True

    raster_lookup = {
        'hand_raster': [],
        'hand_raster_interior': [],
        'transformed_hand': [],
        'transformed_hand_interior': [],
        'transformed_slope': [],
        'transformed_slope_interior': [],
        'evidence_raster': [],
        'evidence_raster_interior': []
    }

    ####################################################################################
    # Level path Loop
    ####################################################################################
    for level_path_key, level_path in enumerate(level_paths_to_run, 1):
        level_path_keys[level_path_key] = level_path

        _tmtbuckets.tick({
            "level_path": level_path,
            "drainage": level_paths_drainage[level_path] if level_path in level_paths_drainage else 0,
            "code": None,
            "msg": None,
            "has_centerline": None,
        })

        log.title(
            f'Processing Level Path: {level_path} {level_path_key}/{len(level_paths_to_run)}')
        temp_folder_lpath = os.path.join(
            temp_folder, f'levelpath_{level_path}')
        safe_makedirs(temp_folder_lpath)

        # Gather the channel area polygon for the level path
        sql = f"{unique_stream_field} = {level_path}" if level_path is not None else f"{unique_stream_field} is NULL"
        level_path_polygons = os.path.join(
            temp_folder_lpath, 'channel_polygons.gpkg', f'level_path_{level_path}')
        with TimerBuckets('ogr'):
            copy_feature_class(
                channel_area, level_path_polygons, attribute_filter=sql)

        # Generate the buffered channel area extent to minimize raster processing area
        if level_path is not None:
            with TimerBuckets('ogr'):
                with GeopackageLayer(level_path_polygons) as lyr_polygons:
                    if lyr_polygons.ogr_layer.GetFeatureCount() == 0:
                        err_msg = f"No channel area features found for Level Path {level_path}."
                        log.warning(err_msg)
                        _tmterr("NO_CHANNEL_AREA", err_msg)
                        continue
                    # Hack to check if any channel geoms are empty
                    check_empty = False
                    feat: ogr.Feature = None
                    for feat, *_ in lyr_polygons.iterate_features():
                        geom_test: ogr.Geometry = feat.GetGeometryRef()
                        if geom_test.IsEmpty():
                            check_empty = True
                    if check_empty is True:
                        err_msg = f"Empty channel area geometry found for Level Path {level_path}."
                        log.warning(err_msg)
                        _tmterr("EMPTY_CHANNEL_AREA", err_msg)
                        continue
                    channel_bbox = lyr_polygons.ogr_layer.GetExtent()
                    channel_buffer_size = lyr_polygons.rough_convert_metres_to_vector_units(
                        400)

                channel_envelope_geom: ogr.Geometry = get_rectangle_as_geom(
                    channel_bbox)
                log.debug(
                    f'channel_envelope_geom area: {channel_envelope_geom.Area}')

                if not raster_envelope_geom.Intersects(channel_envelope_geom):
                    log.warning(
                        f'Channel Area Envelope does not intersect DEM Extent for level path {level_path}')
                    continue

                with GeopackageLayer(line_network) as lyr_lines:
                    geom_envelope = channel_envelope_geom.Clone()
                    for feat_line, *_ in lyr_lines.iterate_features(clip_shape=channel_envelope_geom):
                        geom_line = feat_line.GetGeometryRef()
                        geom_line_envelope = get_extent_as_geom(geom_line)
                        geom_envelope = geom_envelope.Union(geom_line_envelope)
                        geom_envelope = get_extent_as_geom(geom_envelope)

            with TimerBuckets('ogr'):
                geom_channel_buffer = geom_envelope.Buffer(channel_buffer_size)
                envelope_geom: ogr.Geometry = raster_envelope_geom.Intersection(
                    geom_channel_buffer)
                if envelope_geom.IsEmpty():
                    err_msg = f'Empty processing envelope for level path {level_path}'
                    log.error(err_msg)
                    _tmterr("EMPTY_ENVELOPE", err_msg)
                    continue
        else:
            envelope_geom = raster_envelope_geom

        envelope = os.path.join(
            temp_folder_lpath, 'envelope_polygon.gpkg', f'level_path_{level_path}')
        with TimerBuckets('ogr'):
            with GeopackageLayer(envelope, write=True) as lyr_envelope:
                lyr_envelope.create_layer(ogr.wkbPolygon, cfg.OUTPUT_EPSG)
                lyr_envelope_dfn = lyr_envelope.ogr_layer_def
                feat = ogr.Feature(lyr_envelope_dfn)
                feat.SetGeometry(envelope_geom)
                lyr_envelope.ogr_layer.CreateFeature(feat)

        # use the channel extent to mask all hand input raster and channel area extents
        local_dinfflowdir_ang = os.path.join(
            temp_folder_lpath, f'dinfflowdir_ang_{level_path}.tif')
        local_pitfill_dem = os.path.join(
            temp_folder_lpath, f'pitfill_dem_{level_path}.tif')
        with TimerBuckets('gdal'):
            with GeopackageLayer(envelope) as envelope_lyr, rasterio.open(pitfill_dem) as pitfill_src:
                envelope_extent = envelope_lyr.ogr_layer.GetExtent()

                if ceil(abs(envelope_extent[0] - envelope_extent[1]) / pitfill_src.res[0]) >= pitfill_src.width or ceil(abs(envelope_extent[2] - envelope_extent[3]) / pitfill_src.res[1]) >= pitfill_src.height:
                    raster_warp(pitfill_dem, local_pitfill_dem, cfg.OUTPUT_EPSG)
                    raster_warp(dinfflowdir_ang, local_dinfflowdir_ang, cfg.OUTPUT_EPSG)
                else:
                    raster_warp(dinfflowdir_ang, local_dinfflowdir_ang,
                                cfg.OUTPUT_EPSG, clip=envelope)
                    raster_warp(pitfill_dem, local_pitfill_dem,
                                cfg.OUTPUT_EPSG, clip=envelope)

        rasterized_channel = os.path.join(
            temp_folder_lpath, f'rasterized_channel_{level_path}.tif')
        prox_raster_path = os.path.join(
            temp_folder_lpath, f'chan_proximity_{level_path}.tif')
        with TimerBuckets('rasterize'):
            rasterize(level_path_polygons, rasterized_channel,
                      local_pitfill_dem, all_touched=True)
            in_rasters['Channel'] = rasterized_channel
            # distance weighting for Slope evidence
            proximity_raster(rasterized_channel, prox_raster_path)
            in_rasters['Proximity'] = prox_raster_path
            with rasterio.open(prox_raster_path) as prox:
                prox_arr = prox.read(1)
                max_prox = np.max(prox_arr)

        with TimerBuckets('flowline'):
            if level_path is not None:
                # Generate and add rasterized version of level path flowline to make sure endpoint coords are on the raster.
                level_path_flowlines = os.path.join(
                    temp_folder_lpath, 'flowlines.gpkg', f'level_path_{level_path}')
                copy_feature_class(line_network, level_path_flowlines,
                                   attribute_filter=f'{unique_stream_field} = {level_path}')
                rasterized_level_path = os.path.join(
                    temp_folder_lpath, f'rasterized_flowline_{level_path}.tif')
                rasterize(level_path_flowlines, rasterized_level_path,
                          rasterized_channel, all_touched=True)
            else:
                rasterized_level_path = None

        with TimerBuckets('HAND'):
            hand_raster = os.path.join(
                temp_rasters_folder, f'local_hand_{level_path}.tif')
            hand_raster_interior = os.path.join(
                temp_rasters_folder, f'local_hand_interior_{level_path}.tif')
            dinfdistdown_status = run_subprocess(project_folder, ["mpiexec", "-n", NCORES, "dinfdistdown",
                                                                  "-ang", local_dinfflowdir_ang,
                                                                  "-fel", local_pitfill_dem,
                                                                  "-src", rasterized_channel,
                                                                  "-dd", hand_raster, "-m", "ave", "v"])
            if dinfdistdown_status != 0 or not os.path.isfile(hand_raster):
                err_msg = f'Error generating HAND for level path {level_path}'
                log.error(err_msg)
                _tmterr("HAND_ERROR", err_msg)
                continue
            in_rasters['HAND'] = hand_raster

        with TimerBuckets('rasterio'):
            # Open evidence rasters concurrently. We're looping over windows so this shouldn't affect
            # memory consumption too much
            read_rasters = {name: rasterio.open(
                raster) for name, raster in in_rasters.items()}
            out_meta = read_rasters['HAND'].meta
            out_meta['driver'] = 'GTiff'
            out_meta['count'] = 1
            out_meta['compress'] = 'deflate'

            use_big_tiff_interior = os.path.getsize(
                in_rasters['HAND']) > BIG_TIFF_THRESH
            if use_big_tiff_interior:
                out_meta['BIGTIFF'] = 'YES'

            evidence_raster = os.path.join(
                temp_rasters_folder, f'vbet_evidence_{level_path}.tif')
            evidence_raster_interior = os.path.join(
                temp_rasters_folder, f'vbet_evidence_interior_{level_path}.tif')
            transformed_hand = os.path.join(
                temp_rasters_folder, f'transformed_hand_{level_path}.tif')
            transformed_hand_interior = os.path.join(
                temp_rasters_folder, f'transformed_hand_interior_{level_path}.tif')
            transformed_slope = os.path.join(
                temp_rasters_folder, f'transformed_slope_{level_path}.tif')
            transformed_slope_interior = os.path.join(
                temp_rasters_folder, f'transformed_slope_interior_{level_path}.tif')

            # {name: rasterio.open(raster, 'w', **out_meta) for name, raster in out_rasters.items()}
            write_rasters = {}
            write_rasters['VBET_EVIDENCE'] = rasterio.open(
                evidence_raster, 'w', **out_meta)
            write_rasters['TRANSFORMED_HAND'] = rasterio.open(
                transformed_hand, 'w', **out_meta)
            write_rasters['TRANSFORMED_SLOPE'] = rasterio.open(
                transformed_slope, 'w', **out_meta)
            write_rasters['topo_evidence'] = rasterio.open(os.path.join(
                temp_folder_lpath, f'topo_evidence_{level_path}.tif'), 'w', **out_meta)

            progbar = ProgressBar(len(
                list(read_rasters['Slope'].block_windows(1))), 50, "Calculating evidence layer")
            counter = 0
            # Again, these rasters should be orthogonal so their windows should also line up
            in_transform = read_rasters['HAND'].get_transform()
            out_transform = read_rasters['Slope'].get_transform()
            col_off_delta = round(
                (in_transform[0] - out_transform[0]) / out_transform[1])
            row_off_delta = round(
                (in_transform[3] - out_transform[3]) / out_transform[5])

            for _ji, window in read_rasters['HAND'].block_windows(1):
                progbar.update(counter)
                counter += 1
                modified_window = Window(
                    window.col_off + col_off_delta, window.row_off + row_off_delta, window.width, window.height)
                block = {}
                for block_name, raster in read_rasters.items():
                    out_window = window if block_name in [
                        'HAND', 'Channel', 'TRANSFORM_ZONE_HAND', 'Proximity'] else modified_window
                    block[block_name] = raster.read(
                        1, window=out_window, masked=True)

                transformed = {}
                for name in vbet_run['Inputs']:
                    if name in vbet_run['Zones']:
                        zone = get_zone(
                            vbet_run, name, level_paths_drainage[level_path])
                        transform = vbet_run['Transforms'][name][zone]
                        if isinstance(transform, str):
                            locs = {'a': block[name].data}
                            with warnings.catch_warnings(record=True) as w:
                                warnings.simplefilter("always")
                                trans_ds = eval(
                                    transform, {'__builtins__': None}, locs)
                                if w and issubclass(w[-1].category, RuntimeWarning):
                                    pass
                            transformed[name] = np.ma.MaskedArray(
                                trans_ds, mask=block['HAND'].mask)

                        else:
                            transformed[name] = np.ma.MaskedArray(
                                transform(block[name].data), mask=block['HAND'].mask)
                    else:
                        transformed[name] = np.ma.MaskedArray(
                            vbet_run['Transforms'][name][0](block[name].data), mask=block['HAND'].mask)

                    masked_prox = np.ma.MaskedArray(
                        block['Proximity'].data, mask=block['HAND'].mask)
                    if name == 'Slope' and zone < 3:
                        # transformed[name] = transformed[name] - ((np.log(masked_prox + 0.1) + 2.303) / np.log(max_prox + 2.303))
                        transformed[name] = transformed[name] - \
                            (np.sqrt(masked_prox) / np.sqrt(max_prox))

                fvals_topo = vbet_run['Inputs']['HAND']['weight'] * transformed['HAND'] + \
                    vbet_run['Inputs']['Slope']['weight'] * \
                    transformed['Slope']
                fvals_channel = 0.995 * block['Channel']
                fvals_evidence = np.maximum(fvals_topo, fvals_channel)

                write_rasters['topo_evidence'].write(np.ma.filled(np.float32(
                    fvals_topo), out_meta['nodata']), window=window, indexes=1)
                write_rasters['VBET_EVIDENCE'].write(np.ma.filled(np.float32(
                    fvals_evidence), out_meta['nodata']), window=window, indexes=1)
                write_rasters['TRANSFORMED_HAND'].write(np.ma.filled(np.float32(
                    transformed['HAND']), out_meta['nodata']), window=window, indexes=1)
                write_rasters['TRANSFORMED_SLOPE'].write(np.ma.filled(np.float32(
                    transformed['Slope']), out_meta['nodata']), window=window, indexes=1)
            write_rasters['VBET_EVIDENCE'].close()
            write_rasters['TRANSFORMED_HAND'].close()
            write_rasters['TRANSFORMED_SLOPE'].close()
            write_rasters['topo_evidence'].close()

        # Generate VBET Polygon
        with TimerBuckets('gdal'):
            valley_bottom_raster = os.path.join(
                temp_folder_lpath, f'valley_bottom_{level_path}.tif')
            generate_vbet_polygon(evidence_raster, rasterized_channel, hand_raster,
                                  valley_bottom_raster, temp_folder_lpath, rasterized_level_path, thresh_value=0.65)

        log.info('Add VBET Raster to Output')
        with TimerBuckets('rasterio'):
            raster_update_multiply(
                vbet_zone_raster, valley_bottom_raster, value=level_path_key)

        if level_path is not None:
            with TimerBuckets('scipy'):
                region_raster = os.path.join(
                    temp_folder_lpath, f'region_cleaning_{level_path}.tif')
                clean_raster_regions(
                    vbet_zone_raster, level_path_key, vbet_zone_raster, region_raster)

        # Generate the Active Floodplain Polygon
        with TimerBuckets('gdal'):
            low_lying_valley_bottom_raster = os.path.join(
                temp_folder_lpath, f'low_lying_valley_bottom_{level_path}.tif')
            generate_vbet_polygon(evidence_raster, rasterized_channel, hand_raster,
                                  low_lying_valley_bottom_raster, temp_folder_lpath, rasterized_level_path, thresh_value=0.85)

        with TimerBuckets('rasterio'):
            raster_update_multiply(
                active_zone_raster, low_lying_valley_bottom_raster, value=level_path_key)

        # Generate centerline for level paths only
        with TimerBuckets('centerline'):
            if level_path is not None:
                # Generate and add rasterized version of level path flowline to make sure endpoint coords are on the raster.
                valley_bottom_flowline_raster = os.path.join(
                    temp_folder_lpath, f'valley_bottom_and_flowline_{level_path}.tif')
                with rasterio.open(valley_bottom_raster, 'r') as rio_vbet, \
                        rasterio.open(rasterized_level_path, 'r') as rio_flowline:
                    out_meta = rio_vbet.meta

                    use_big_tiff_cline = os.path.getsize(
                        valley_bottom_raster) > BIG_TIFF_THRESH
                    if use_big_tiff_cline:
                        out_meta['BIGTIFF'] = 'YES'

                    out_meta['compress'] = 'deflate'
                    with rasterio.open(valley_bottom_flowline_raster, 'w', **out_meta) as rio_out:
                        for _ji, window in rio_vbet.block_windows(1):
                            array_vbet = np.ma.MaskedArray(
                                rio_vbet.read(1, window=window).data)
                            array_flowline = np.ma.MaskedArray(
                                rio_flowline.read(1, window=window).data)
                            array_logic = array_vbet + array_flowline
                            array_out = np.greater_equal(array_logic, 1)
                            array_out_format = array_out if out_meta['dtype'] == 'int32' else np.float32(
                                array_out)
                            rio_out.write(np.ma.filled(
                                array_out_format, out_meta['nodata']), window=window, indexes=1)

                # Generate Centerline from Cost Path
                log.info('Generating Centerline from cost path')
                cost_path_raster = os.path.join(
                    temp_folder_lpath, f'cost_path_{level_path}.tif')
                generate_centerline_surface(
                    valley_bottom_flowline_raster, cost_path_raster, temp_folder_lpath)
                geom_flowline = collect_linestring(level_path_flowlines)

                geom_flowline = ogr.ForceToMultiLineString(geom_flowline)
                with GeopackageLayer(temp_centerlines, write=True) as lyr_cl:
                    g_flowline = [g for g in geom_flowline]
                    coords = get_endpoints_on_raster(
                        cost_path_raster, g_flowline, pixel_x)
                    if len(coords) != 2:
                        err_msg = f'Unable to generate centerline for level path {level_path}: found {len(coords)} target coordinates instead of expected 2.'
                        log.error(err_msg)
                        _tmterr("CENTERLINE_ERROR", err_msg)
                        continue
                    log.info('Find least cost path for centerline')
                    try:
                        centerline_raster = os.path.join(
                            temp_folder_lpath, f'centerline_{level_path}.tif')
                        least_cost_path(
                            cost_path_raster, centerline_raster, coords[0], coords[1])
                    except Exception as err:
                        # print(err)
                        err_msg = f'Unable to generate centerline for level path {level_path}: end points must all be within the costs array.'
                        log.error(err_msg)
                        log.debug(err)
                        _tmterr("CENTERLINE_COST_ERROR", err_msg)

                    log.info('Vectorize centerline from Raster')
                    geom_centerline = raster2line_geom(
                        centerline_raster, 1)
                    geom_centerline = ogr.ForceToLineString(
                        geom_centerline)

                    geom_centerline = ogr.ForceToMultiLineString(
                        geom_centerline)
                    out_feature = ogr.Feature(lyr_cl.ogr_layer_def)
                    out_feature.SetGeometry(geom_centerline)
                    out_feature.SetField(
                        f'{unique_stream_field}', str(level_path))

                    lyr_cl.ogr_layer.CreateFeature(out_feature)
                    out_feature = None

        # Mask the raster and create the inner versions of itself
        raster_logic_mask(hand_raster, hand_raster_interior,
                          valley_bottom_raster)
        raster_logic_mask(transformed_hand,
                          transformed_hand_interior, valley_bottom_raster)
        raster_logic_mask(
            evidence_raster, evidence_raster_interior, valley_bottom_raster)
        raster_logic_mask(transformed_slope,
                          transformed_slope_interior, valley_bottom_raster)

        # Add these to arrays so that we can use them later
        if os.path.isfile(hand_raster):
            if level_path is None:
                raster_lookup['hand_raster'] = [
                    hand_raster] + raster_lookup['hand_raster']
            else:
                raster_lookup['hand_raster'].append(hand_raster)

        if os.path.isfile(evidence_raster):
            if level_path is None:
                raster_lookup['evidence_raster'] = [
                    evidence_raster] + raster_lookup['evidence_raster']
            else:
                raster_lookup['evidence_raster'].append(evidence_raster)

        if os.path.isfile(transformed_hand):
            if level_path is None:
                raster_lookup['transformed_hand'] = [
                    transformed_hand] + raster_lookup['transformed_hand']
            else:
                raster_lookup['transformed_hand'].append(transformed_hand)

        if os.path.isfile(transformed_slope):
            if level_path is None:
                raster_lookup['transformed_slope'] = [
                    transformed_slope] + raster_lookup['transformed_slope']
            else:
                raster_lookup['transformed_slope'].append(transformed_slope)

        # Add these to arrays so that we can use them later
        if os.path.isfile(hand_raster_interior):
            raster_lookup['hand_raster_interior'].append(hand_raster_interior)

        if os.path.isfile(evidence_raster_interior):
            raster_lookup['evidence_raster_interior'].append(
                evidence_raster_interior)

        if os.path.isfile(transformed_hand_interior):
            raster_lookup['transformed_hand_interior'].append(
                transformed_hand_interior)

        if os.path.isfile(transformed_slope_interior):
            raster_lookup['transformed_slope_interior'].append(
                transformed_slope_interior)

        # End of level path for loop

    _tmtfinish()
    # Final tick to trigger writing the last row
    _tmtbuckets.tick()
    _tmtbuckets.write_csv()

    # this is where level path for loop ends
    _tmr_waypt.timer_break('LevelPaths for loop')

    # The interior rasters are a stitched-together composite of the local, levelpath rasters with the valley bottom logic applied
    out_hand_interior = os.path.join(
        project_folder, LayerTypes['COMPOSITE_HAND_INTERIOR'].rel_path)
    out_transformed_hand_interior = os.path.join(
        project_folder, LayerTypes['TRANSFORMED_HAND_INTERIOR'].rel_path)
    out_vbet_evidence_interior = os.path.join(
        project_folder, LayerTypes['COMPOSITE_VBET_EVIDENCE_INTERIOR'].rel_path)
    out_transformed_slope_interior = os.path.join(
        project_folder, LayerTypes['TRANSFORMED_SLOPE_INTERIOR'].rel_path)

    # Make VRTs for our composite rasters
    out_hand_interior_cmp = CompositeRaster(
        out_hand_interior, raster_lookup['hand_raster_interior'], vrt_path=os.path.join(temp_folder, 'hand_interior.vrt'))
    out_hand_interior_cmp.make_vrt()
    out_hand_interior_cmp.make_composite()

    out_transformed_hand_interior_cmp = CompositeRaster(
        out_transformed_hand_interior, raster_lookup['transformed_hand_interior'], vrt_path=os.path.join(temp_folder, 'transformed_hand_interior.vrt'))
    out_transformed_hand_interior_cmp.make_vrt()
    out_transformed_hand_interior_cmp.make_composite()

    out_transformed_slope_interior_cmp = CompositeRaster(
        out_transformed_slope_interior, raster_lookup['transformed_slope_interior'], vrt_path=os.path.join(temp_folder, 'transformed_slope_interior.vrt'))
    out_transformed_slope_interior_cmp.make_vrt()
    out_transformed_slope_interior_cmp.make_composite()

    out_vbet_evidence_interior_cmp = CompositeRaster(
        out_vbet_evidence_interior, raster_lookup['evidence_raster_interior'], vrt_path=os.path.join(temp_folder, 'vbet_evidence_interior.vrt'))
    out_vbet_evidence_interior_cmp.make_vrt()
    out_vbet_evidence_interior_cmp.make_composite()

    # this is where level path for loop ends
    _tmr_waypt.timer_break('LevelPaths VRTs')

    with sqlite3.connect(inputs_gpkg) as conn:
        _tmtbuckets.write_sqlite(conn)

    # Difference Raster
    log.info("Differencing inactive floodplain raster")
    raster_remove_zone(vbet_zone_raster, active_zone_raster,
                       inactive_zone_raster)

    # Geomorphic layers
    output_floodplain = os.path.join(
        vbet_gpkg, LayerTypes['VBET_OUTPUTS'].sub_layers['FLOODPLAIN'].rel_path)
    output_active_fp = os.path.join(
        vbet_gpkg, LayerTypes['VBET_OUTPUTS'].sub_layers['LOW_LYING_FLOODPLAIN'].rel_path)
    output_inactive_fp = os.path.join(
        vbet_gpkg, LayerTypes['VBET_OUTPUTS'].sub_layers['ELEVATED_FLOODPLAIN'].rel_path)

    # Polygonize Rasters
    log.info("Polygonizing VBET area")
    polygonize(vbet_zone_raster, 1, output_vbet, espg)
    log.info("Polygonizing Low-lying VBET area")
    polygonize(active_zone_raster, 1, output_vbet_ia, espg)
    log.info("Polygonizing Elevated VBET area")
    polygonize(inactive_zone_raster, 1, output_inactive_fp, espg)
    _tmr_waypt.timer_break('PolygonizeRasters')

    log.info('Set Level Path ID for output polygons')
    for layer in [output_vbet, output_vbet_ia, output_inactive_fp]:
        with GeopackageLayer(layer, write=True) as lyr, GeopackageLayer(channel_area) as wblyr:
            lyr.create_field(f'{unique_stream_field}', ogr.OFTString)
            for feat, *_ in lyr.iterate_features(write_layers=[lyr]):
                key = feat.GetField('id')
                if level_path_keys[key] is None:
                    ft_env = feat.GetGeometryRef().GetEnvelope()
                    intersects = False
                    for feat2, *_ in wblyr.iterate_features(clip_rect=ft_env):
                        if feat2.GetGeometryRef().Intersects(feat.GetGeometryRef()):
                            intersects = True
                            break
                    if intersects is False:
                        lyr.ogr_layer.DeleteFeature(feat.GetFID())
                    feat2 = None
                    continue
                feat.SetField(f'{unique_stream_field}', level_path_keys[key])
                lyr.ogr_layer.SetFeature(feat)
                feat = None
    _tmr_waypt.timer_break('set_level_path_id')

    # Clean Up Centerlines
    clean_up_centerlines(temp_centerlines, output_vbet,
                         output_centerlines, vbet_clip_buffer_size, unique_stream_field)
    _tmr_waypt.timer_break('clean_up_centerlines')

    log.info('Generating geomorphic layers')
    difference(channel_area, output_vbet, output_floodplain)
    difference(channel_area, output_vbet_ia, output_active_fp)
    # difference(output_vbet_ia, output_vbet, output_inactive_fp)
    _tmr_waypt.timer_break('GenGeomorhicLyrs')

    # Calculate VBET Metrics
    log.info('Generating VBET Segmentation Points')
    segmentation_points = os.path.join(
        vbet_gpkg, LayerTypes['VBET_OUTPUTS'].sub_layers['SEGMENTATION_POINTS'].rel_path)
    stream_size_lookup = get_distance_lookup(
        inputs_gpkg, level_paths_to_run, level_paths_drainage, vbet_run)
    generate_igo_points(output_centerlines, segmentation_points, unique_stream_field,
                        stream_size_lookup, distance={0: 100, 1: 200, 2: 300, 3: 500, 4: 2000})
    _tmr_waypt.timer_break('GenerateVBETSegmentPts')

    log.info('Generating VBET Segment Polygons')
    segmentation_polygons = os.path.join(
        intermediates_gpkg, LayerTypes['INTERMEDIATES'].sub_layers['VBET_DGO_POLYGONS'].rel_path)
    split_vbet_polygons(output_vbet, segmentation_points,
                        segmentation_polygons, unique_stream_field)
    _tmr_waypt.timer_break('GenerateVBETSegmentPolys')
    if flowline_type == 'NHD':
        add_fcodes(segmentation_polygons, segmentation_points, line_network)

    log.info('Calculating Segment Metrics')
    metric_layers = {'low_lying_floodplain': output_active_fp, 'active_channel': channel_area,
                     'elevated_floodplain': output_inactive_fp, 'floodplain': output_floodplain}
    for level_path in level_paths_to_run:
        if level_path is None:
            continue
        calculate_dgo_metrics(segmentation_polygons, output_centerlines,
                              metric_layers, f"{unique_stream_field} = {level_path}")
    _tmr_waypt.timer_break('CalcSegmentMetrics')

    log.info('Summerizing VBET Metrics')
    window_size = {0: 200.0, 1: 400.0, 2: 1200.0, 3: 2000.0, 4: 8000.0}
    distance_lookup = get_distance_lookup(
        inputs_gpkg, level_paths_to_run, level_paths_drainage, vbet_run, window_size)
    project.add_metadata([RSMeta("Small Search Window", str(window_size[0]), RSMetaTypes.INT, locked=True),
                          RSMeta("Medium Search Window", str(
                              window_size[1]), RSMetaTypes.INT, locked=True),
                          RSMeta("Large Search Window", str(
                              window_size[2]), RSMetaTypes.INT, locked=True),
                          RSMeta("Very Large Search Window", str(
                              window_size[3]), RSMetaTypes.INT, locked=True),
                          RSMeta("Huge Search Window", str(window_size[4]), RSMetaTypes.INT, locked=True)])
    metric_fields = list(metric_layers.keys())
    calculate_vbet_window_metrics(segmentation_points, segmentation_polygons,
                                  level_paths_to_run, unique_stream_field, distance_lookup, metric_fields)
    _tmr_waypt.timer_break('SummerizeMetrics')

    log.info('Apply values to No Data areas of HAND and Evidence rasters')

    # Now let's assemble the rasters using a VRT then bake that back to a real raster so we can clean up the temp folder later if we want.
    out_hand = os.path.join(
        project_folder, LayerTypes['COMPOSITE_HAND'].rel_path)
    out_transformed_hand = os.path.join(
        project_folder, LayerTypes['TRANSFORMED_HAND'].rel_path)
    out_transformed_slope = os.path.join(
        project_folder, LayerTypes['TRANSFORMED_SLOPE'].rel_path)
    out_vbet_evidence = os.path.join(
        project_folder, LayerTypes['COMPOSITE_VBET_EVIDENCE'].rel_path)

    out_hand_composite = CompositeRaster(out_hand, raster_paths=[
        out_hand_interior,
        *reversed(raster_lookup['hand_raster'])
    ], vrt_path=os.path.join(temp_folder, 'hand_composite.vrt'))
    out_hand_composite.make_vrt()
    out_hand_composite.make_composite()

    out_transformed_hand_composite = CompositeRaster(out_transformed_hand, raster_paths=[
        out_transformed_hand_interior,
        *reversed(raster_lookup['transformed_hand'])
    ], vrt_path=os.path.join(temp_folder, 'transformed_hand_composite.vrt'))
    out_transformed_hand_composite.make_vrt()
    out_transformed_hand_composite.make_composite()

    out_transformed_slope_composite = CompositeRaster(out_transformed_slope, raster_paths=[
        out_transformed_slope_interior,
        *reversed(raster_lookup['transformed_slope'])
    ], vrt_path=os.path.join(temp_folder, 'transformed_slope_composite.vrt'))
    out_transformed_slope_composite.make_vrt()
    out_transformed_slope_composite.make_composite()

    out_vbet_evidence_composite = CompositeRaster(out_vbet_evidence, raster_paths=[
        out_vbet_evidence_interior,
        *reversed(raster_lookup['evidence_raster'])
    ], vrt_path=os.path.join(temp_folder, 'vbet_evidence_composite.vrt'))
    out_vbet_evidence_composite.make_vrt()
    out_vbet_evidence_composite.make_composite()

    _tmr_waypt.timer_break('make_composites')
    # These VRTs are absolute paths so they need to be cleaned up.
    if debug is False:
        os.remove(out_hand_composite.vrt_path)
        os.remove(out_transformed_hand_composite.vrt_path)
        os.remove(out_vbet_evidence_composite.vrt_path)
        os.remove(out_transformed_slope_composite.vrt_path)

    # Now add our Geopackages to the project XML
    _composite_evidence_node, composit_evidence_ras = project.add_project_raster(
        proj_nodes['Outputs'], LayerTypes['COMPOSITE_VBET_EVIDENCE'])
    _composite_hand_node, composite_hand_ras = project.add_project_raster(
        proj_nodes['Intermediates'], LayerTypes['COMPOSITE_HAND'])
    _transformed_hand_node, transformed_hand_ras = project.add_project_raster(
        proj_nodes['Intermediates'], LayerTypes['TRANSFORMED_HAND'])
    _transformed_slope_node, transformed_slope_ras = project.add_project_raster(
        proj_nodes['Intermediates'], LayerTypes['TRANSFORMED_SLOPE'])

    _evidence_interior_node, evidence_interior_ras = project.add_project_raster(
        proj_nodes['Intermediates'], LayerTypes['COMPOSITE_VBET_EVIDENCE_INTERIOR'])
    _hand_interior_node, hand_interior_ras = project.add_project_raster(
        proj_nodes['Intermediates'], LayerTypes['COMPOSITE_HAND_INTERIOR'])
    _trans_hand_interior_node, trans_hand_interior_ras = project.add_project_raster(
        proj_nodes['Intermediates'], LayerTypes['TRANSFORMED_HAND_INTERIOR'])
    _trans_slp_interior_node, trans_slp_interior_ras = project.add_project_raster(
        proj_nodes['Intermediates'], LayerTypes['TRANSFORMED_SLOPE_INTERIOR'])
    proj_rasters.extend([[_composite_evidence_node, composit_evidence_ras], [_composite_hand_node, composite_hand_ras],
                         [_transformed_hand_node, transformed_hand_ras], [
                             _transformed_slope_node, transformed_slope_ras],
                         [_evidence_interior_node, evidence_interior_ras], [_hand_interior_node, hand_interior_ras], [
                             _trans_hand_interior_node, trans_hand_interior_ras],
                         [_trans_slp_interior_node, trans_slp_interior_ras]])

    project.add_project_geopackage(
        proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    project.add_project_geopackage(
        proj_nodes['Outputs'], LayerTypes['VBET_OUTPUTS'])

    # Processing time in hours
    _tmr_waypt.timer_break('END')
    ellapsed_time = _tmr_waypt.total_time

    log.debug(_tmr_waypt.toString())

    project.add_metadata([
        RSMeta("ProcTimeS", f"{ellapsed_time:.2f}",
               RSMetaTypes.HIDDEN, locked=True),
        RSMeta("Processing Time", pretty_duration(ellapsed_time), locked=True)
    ])

    for ras in proj_rasters:
        raster_resolution_meta(project, ras[1], ras[0])

    add_layer_descriptions(project, LYR_DESCRIPTIONS_JSON, LayerTypes)

    # Report
    report_path = os.path.join(
        project.project_dir, LayerTypes['REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'],
                       LayerTypes['REPORT'], replace=True)

    report = VBETReport(report_path, project)
    report.write()

    log.info('VBET Completed Successfully')


def get_zone(run, zone_type, drain_area):
    """get the max zone of the drainage area

    Args:
        run (dict): vbet configuration dict
        zone_type (str): name of zone to find
        drain_area (int): drainage area of zone to find

    Returns:
        int: zone
    """

    for zone, max_value in run['Zones'][zone_type].items():

        if max_value is None or max_value == '':
            return zone

        if drain_area < max_value:
            return zone


def main():
    """_summary_
    """
    parser = argparse.ArgumentParser(
        description='Riverscapes VBET Tool',
        # epilog="This is an epilog"
    )
    parser.add_argument('huc', help='nhd watershed id', type=str)
    parser.add_argument('flowline_network',
                        help='full nhd line network', type=str)
    parser.add_argument('dem', help='dem', type=str)
    parser.add_argument('slope', help='slope', type=str)
    parser.add_argument('hillshade', type=str)
    parser.add_argument('channel_area', type=str)
    parser.add_argument(
        'output_dir', help='Folder where output VBET project will be created', type=str)
    parser.add_argument('flowline_type', type=str, default='NHD')
    parser.add_argument('unique_stream_field', type=str, default='level_path')
    parser.add_argument('unique_reach_field', type=str, default='NHDPlusID')
    parser.add_argument('drain_area_field', type=str, default='DivDASqKm')
    parser.add_argument(
        '--level_paths', help='csv list of level paths', type=str, default="")
    parser.add_argument(
        '--pitfill', help='riverscapes project metadata as comma separated key=value pairs', default=None)
    parser.add_argument('--dinfflowdir_ang',
                        help='(optional) a little extra logging ', default=None)
    parser.add_argument(
        '--dinfflowdir_slp', help='Add debug tools for tracing things like memory usage at a performance cost.', default=None)
    parser.add_argument(
        '--reach_codes', help='Comma delimited reach codes (FCode) to retain when filtering features. Omitting this option retains all features.', type=str)
    parser.add_argument(
        '--temp_folder', help='(optional) cache folder for downloading files ', type=str)
    parser.add_argument('--mask', type=str, default=None)
    parser.add_argument(
        '--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ',
                        action='store_true', default=False)
    parser.add_argument('--debug', help='Add debug tools for tracing things like memory usage at a performance cost.',
                        action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # make sure the output folder exists
    safe_makedirs(args.output_dir)

    # Initiate the log file
    log = Logger('VBET')
    log.setup(logPath=os.path.join(args.output_dir,
              'vbet.log'), verbose=args.verbose)
    log.title(f'Riverscapes VBET For HUC: {args.huc}')

    meta = parse_metadata(args.meta)
    reach_codes = args.reach_codes.split(',') if args.reach_codes else None
    level_paths = args.level_paths.split(',')
    level_paths = level_paths if level_paths != ['.'] else None

    # Allow us to specify a temp folder outside our project folder
    temp_folder = args.temp_folder if args.temp_folder else os.path.join(
        args.output_dir, 'temp')

    try:
        if args.debug is True:
            # pylint: disable=import-outside-toplevel
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_dir, 'vbet_mem.log')
            retcode, max_obj = ThreadRun(
                vbet, memfile,
                args.flowline_network, args.dem, args.slope, args.hillshade, args.channel_area, args.output_dir,
                args.huc, args.flowline_type, args.unique_stream_field, args.unique_reach_field, args.drain_area_field, level_paths,
                args.pitfill, args.dinfflowdir_ang, args.dinfflowdir_slp, meta=meta, reach_codes=reach_codes, mask=args.mask,
                debug=args.debug, temp_folder=temp_folder
            )
            log.debug(f'Return code: {retcode}, [Max process usage] {max_obj}')
            # Zip up a copy of the temp folder for debugging purposes
            zip_temp_folder(temp_folder, os.path.join(args.output_dir, 'temp'))
        else:
            vbet(
                args.flowline_network, args.dem, args.slope, args.hillshade, args.channel_area, args.output_dir,
                args.huc, args.flowline_type, args.unique_stream_field, args.unique_reach_field, args.drain_area_field, level_paths,
                args.pitfill, args.dinfflowdir_ang, args.dinfflowdir_slp, meta=meta, reach_codes=reach_codes, mask=args.mask,
                debug=args.debug, temp_folder=args.temp_folder
            )

        safe_remove_dir(temp_folder)
    except Exception as err:
        log.error(err)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


def zip_temp_folder(temp_folder: str, base_name: str):
    """There are too many files in the temp folder to upload. We zip them up and delete the folder.

    Args:
        temp_folder (str): _description_
        base_name (str): _description_
    """
    log = Logger('Zip Temp Folder')
    # This takes a while but it's worth it for the visibility when using the --debug flag
    log.debug('Starting zip')
    try:
        shutil.make_archive(base_name, "zip", temp_folder)
    except FileNotFoundError as err:
        log.warning(f'No temp folder found: {err}')
    except Exception as err:
        log.error(err)
    log.debug('Zipping complete')


if __name__ == '__main__':
    main()
