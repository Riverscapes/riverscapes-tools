# Name:     Vbet Centerlines (raster)
#
# Purpose:  Find the vbet centerlines per level path via rasters
#
#
# Author:   Kelly Whitehead
#
# Date:     Apr 11, 2022
# -------------------------------------------------------------------------------

import os
import sys
import argparse
import traceback
import time
from typing import List, Dict

import gdal
from osgeo import ogr
import rasterio
from shapely.geometry import box
import numpy as np
from scipy.ndimage import label, generate_binary_structure, binary_closing

from rscommons import RSProject, RSLayer, ModelConfig, ProgressBar, Logger, GeopackageLayer, dotenv, VectorBase, initGDALOGRErrors
from rscommons.vector_ops import copy_feature_class, polygonize, get_endpoints, difference, collect_feature_class
from rscommons.util import safe_makedirs, parse_metadata, pretty_duration, safe_remove_dir
from rscommons.hand import run_subprocess
from rscommons.vbet_network import copy_vaa_attributes, join_attributes, create_stream_size_zones, get_channel_level_path, get_distance_lookup, get_levelpath_catchment
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons.raster_warp import raster_warp

from vbet.vbet_database import build_vbet_database, load_configuration
from vbet.vbet_raster_ops import mask_rasters_nodata, rasterize_attribute, raster2array, array2raster, new_raster, rasterize, raster_merge
from vbet.vbet_outputs import vbet_merge, threshold
from vbet.vbet_report import VBETReport
from vbet.vbet_segmentation import calculate_segmentation_metrics, generate_segmentation_points, split_vbet_polygons, summerize_vbet_metrics
from scripts.cost_path import least_cost_path
from scripts.raster2line import raster2line_geom
from vbet.__version__ import __version__

Path = str

NCORES = os.environ['TAUDEM_CORES'] if 'TAUDEM_CORES' in os.environ else '2'  # "8"

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/VBET.xsd', __version__)

LayerTypes = {
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'inputs/dem.tif'),
    'SLOPE_RASTER': RSLayer('Slope Raster', 'SLOPE_RASTER', 'Raster', 'inputs/slope.tif'),
    'TWI_RASTER': RSLayer('Topographic Wetness Index (TWI) Raster', 'TWI_RASTER', 'Raster', 'inputs/twi.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/vbet_inputs.gpkg', {
        'FLOWLINES': RSLayer('NHD Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'FLOW_AREAS': RSLayer('NHD Flow Areas', 'FLOW_AREAS', 'Vector', 'flowareas'),
        'FLOWLINES_VAA': RSLayer('NHD Flowlines with Attributes', 'FLOWLINES_VAA', 'Vector', 'Flowlines_VAA'),
        'CHANNEL_AREA_POLYGONS': RSLayer('Channel Area Polygons', 'CHANNEL_AREA_POLYGONS', 'Vector', 'channel_area_polygons'),
        'CATCHMENTS': RSLayer('NHD Catchments', 'CATCHMENTS', 'Vector', 'catchments'),
    }),
    # Taudem intermediate rasters can be provided as inputs, or generated in vbet
    'PITFILL': RSLayer('TauDEM Pitfill', 'PITFILL', 'Raster', 'intermediates/pitfill.tif'),
    'DINFFLOWDIR_ANG': RSLayer('TauDEM D-Inf Flow Directions', 'DINFFLOWDIR_ANG', 'Raster', 'intermediates/dinfflowdir_ang.tif'),
    'DINFFLOWDIR_SLP': RSLayer('TauDEM D-Inf Flow Directions Slope', 'DINFFLOWDIR_SLP', 'Raster', 'intermediates/dinfflowdir_slp.tif'),
    # DYNAMIC: 'DA_ZONE_<RASTER>': RSLayer('Drainage Area Zone Raster', 'DA_ZONE_RASTER', "Raster", "intermediates/.tif"),
    'INTERMEDIATES': RSLayer('Intermediates', 'Intermediates', 'Geopackage', 'intermediates/vbet_intermediates.gpkg', {
        'VBET_NETWORK': RSLayer('VBET Network', 'VBET_NETWORK', 'Vector', 'vbet_network'),
        'TRANSFORM_ZONES': RSLayer('Transform Zones', 'TRANSFORM_ZONES', 'Vector', 'transform_zones'),
        'SEGMENTED_VBET_POLYGONS': RSLayer('Segmented VBET Polygons', 'SEGMENTED_VBET_POLYGONS', 'Vector', 'segmented_vbet_polygons'),
        'SEGMENTATION_POINTS': RSLayer('Segmentation Points', 'SEGMENTATION_POINTS', 'Vector', 'segmentation_points')
        # We also add all tht raw thresholded shapes here but they get added dynamically later
    }),
    # Same here. Sub layers are added dynamically later.
    'COMPOSITE_VBET_EVIDENCE': RSLayer('VBET Evidence Raster', 'VBET_EVIDENCE', 'Raster', 'outputs/VBET_Evidence.tif'),
    'COMPOSITE_HAND_RASTER': RSLayer('Hand Raster', 'HAND_RASTER', 'Raster', 'inputs/HAND.tif'),
    'VBET_OUTPUTS': RSLayer('VBET', 'VBET_OUTPUTS', 'Geopackage', 'outputs/vbet.gpkg', {
        'VBET_FULL': RSLayer('VBET Full Extent', 'VBET_FULL', 'Vector', 'vbet_full'),
        'VBET_IA': RSLayer('VBET Inactive/Active Boundary', 'VBET_IA', 'Vector', 'vbet_ia'),
        'VBET_CHANNEL_AREA': RSLayer('VBET Channel Area', 'VBET_CHANNEL_AREA', 'Vector', 'vbet_channel_area'),
        'ACTIVE_FLOODPLAIN': RSLayer('Active Floodplain', 'ACTIVE_FLOODPLAIN', 'Vector', 'active_floodplain'),
        'INACTIVE_FLOODPLAIN': RSLayer('Inactive Floodplain', 'INACTIVE_FLOODPLAIN', 'Vector', 'inactive_floodplain'),
        'FLOODPLAIN': RSLayer('Floodplain', 'FLOODPLAIN', 'Vector', 'floodplain'),
        'VBET_CENTERLINES': RSLayer('VBET Centerline', 'VBET_CENTERLINES', 'Vector', 'vbet_centerlines')
    }),
    'REPORT': RSLayer('RSContext Report', 'REPORT', 'HTMLFile', 'outputs/vbet.html')
}


def vbet_centerlines(in_line_network, in_dem, in_slope, in_hillshade, in_catchments, in_channel_area, vaa_table, project_folder, scenario_code, huc, level_paths=None, in_pitfill_dem=None, in_dinfflowdir_ang=None, in_dinfflowdir_slp=None, in_twi_raster=None, meta=None, debug=False):

    thresh_vals = {'VBET_IA': 0.90, 'VBET_FULL': 0.68}

    vbet_timer = time.time()
    log = Logger('VBET')
    log.info('Starting VBET v.{}'.format(cfg.version))

    flowline_type = 'NHD'

    project, _realization, proj_nodes = create_project(huc, project_folder, [
        RSMeta('HUC{}'.format(len(huc)), str(huc)),
        RSMeta('HUC', str(huc)),
        RSMeta('VBETVersion', cfg.version),
        RSMeta('VBETTimestamp', str(int(time.time())), RSMetaTypes.TIMESTAMP),
        RSMeta("Scenario Name", scenario_code),
        RSMeta("FlowlineType", flowline_type),
        RSMeta("VBET_Active_Floodplain_Threshold", f"{int(thresh_vals['VBET_IA'] * 100)}", RSMetaTypes.INT),
        RSMeta("VBET_Inactive_Floodplain_Threshold", f"{int(thresh_vals['VBET_FULL'] * 100)}", RSMetaTypes.INT)
    ], meta)

    inputs_gpkg = os.path.join(project_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg = os.path.join(project_folder, LayerTypes['INTERMEDIATES'].rel_path)
    vbet_gpkg = os.path.join(project_folder, LayerTypes['VBET_OUTPUTS'].rel_path)
    GeopackageLayer.delete(inputs_gpkg)
    GeopackageLayer.delete(intermediates_gpkg)
    GeopackageLayer.delete(vbet_gpkg)

    line_network_features = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    copy_feature_class(in_line_network, line_network_features)
    catchment_features = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['CATCHMENTS'].rel_path)
    copy_feature_class(in_catchments, catchment_features)
    channel_area = os.path.join(inputs_gpkg, LayerTypes['INPUTS'].sub_layers['CHANNEL_AREA_POLYGONS'].rel_path)
    copy_feature_class(in_channel_area, channel_area)

    build_vbet_database(inputs_gpkg)
    vbet_run = load_configuration(scenario_code, inputs_gpkg)

    vaa_table_name = copy_vaa_attributes(line_network_features, vaa_table)
    line_network = join_attributes(inputs_gpkg, "flowlines_vaa", os.path.basename(line_network_features), vaa_table_name, 'NHDPlusID', ['LevelPathI', 'DnLevelPat', 'UpLevelPat', 'Divergence'], 4326)
    catchments = join_attributes(inputs_gpkg, "catchments_vaa", os.path.basename(catchment_features), vaa_table_name, 'NHDPlusID', ['LevelPathI', 'DnLevelPat', 'UpLevelPat', 'Divergence'], 4326, geom_type='POLYGON')

    get_channel_level_path(channel_area, line_network, vaa_table)

    catchments_path = os.path.join(intermediates_gpkg, 'transform_zones')
    vaa_table_path = os.path.join(inputs_gpkg, vaa_table_name)
    create_stream_size_zones(catchments, vaa_table_path, 'NHDPlusID', 'StreamOrde', vbet_run['Zones'], catchments_path)

    in_rasters = {}
    _proj_hillshade_node, _hillshade = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], in_hillshade, replace=True)
    _proj_dem_node, dem = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['DEM'], in_dem, replace=True)
    _proj_slope_node, in_rasters['Slope'] = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['SLOPE_RASTER'], in_slope, replace=True)

    # generate top level taudem products if they do not exist
    if in_pitfill_dem is None:
        pitfill_dem = os.path.join(project_folder, LayerTypes['PITFILL'].rel_path)
        pitfill_status = run_subprocess(project_folder, ["mpiexec", "-n", NCORES, "pitremove", "-z", dem, "-fel", pitfill_dem])
        if pitfill_status != 0 or not os.path.isfile(pitfill_dem):
            raise Exception('TauDEM: pitfill failed')
        _proj_hillshade_node, pitfill_dem = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['PITFILL'])
    else:
        _proj_hillshade_node, pitfill_dem = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['PITFILL'], in_pitfill_dem, replace=True)

    if not all([in_dinfflowdir_ang, in_dinfflowdir_slp]):
        dinfflowdir_slp = os.path.join(project_folder, LayerTypes['DINFFLOWDIR_SLP'].rel_path)
        dinfflowdir_ang = os.path.join(project_folder, LayerTypes['DINFFLOWDIR_ANG'].rel_path)
        dinfflowdir_status = run_subprocess(project_folder, ["mpiexec", "-n", NCORES, "dinfflowdir", "-fel", pitfill_dem, "-ang", dinfflowdir_ang, "-slp", dinfflowdir_slp])
        if dinfflowdir_status != 0 or not os.path.isfile(dinfflowdir_ang):
            raise Exception('TauDEM: dinfflowdir failed')
        _proj_dinfflowdir_ang_node, dinfflowdir_ang = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['DINFFLOWDIR_ANG'])
        _proj_dinfflowdir_slp_node, dinfflowdir_slp = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['DINFFLOWDIR_SLP'])
    else:
        _proj_dinfflowdir_ang_node, dinfflowdir_ang = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['DINFFLOWDIR_ANG'], in_dinfflowdir_ang, replace=True)
        _proj_dinfflowdir_slp_node, dinfflowdir_slp = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['DINFFLOWDIR_SLP'], in_dinfflowdir_slp, replace=True)

    if not in_twi_raster:
        twi_raster = os.path.join(project_folder, LayerTypes['TWI_RASTER'].rel_path)
        # sca = os.path.join(project_folder, 'sca.tif')
        twi_status = run_subprocess(project_folder, ["mpiexec", "-n", NCORES, "twi", "-slp", dinfflowdir_slp, '-twi', twi_raster])  # "-sca", sca,
        if twi_status != 0 or not os.path.isfile(twi_raster):
            raise Exception('TauDEM: TWI failed')
        _node_twi, in_rasters['TWI'] = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['TWI_RASTER'])
    else:
        _node_twi, in_rasters['TWI'] = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['TWI_RASTER'], in_twi_raster, replace=True)

    for zone in vbet_run['Zones']:
        log.info(f'Rasterizing stream transform zones for {zone}')
        raster_name = os.path.join(project_folder, 'intermediates', f'{zone.lower()}_transform_zones.tif')
        rasterize_attribute(catchments_path, raster_name, dem, f'{zone}_Zone')
        in_rasters[f'TRANSFORM_ZONE_{zone}'] = raster_name
        transform_zone_rs = RSLayer(f'Transform Zones for {zone}', f'TRANSFORM_ZONE_{zone.upper()}', 'Raster', raster_name)
        project.add_project_raster(proj_nodes['Intermediates'], transform_zone_rs)

    # run for orphan waterbodies??

    # Initialize Outputs
    output_centerlines = os.path.join(vbet_gpkg, LayerTypes['VBET_OUTPUTS'].sub_layers['VBET_CENTERLINES'].rel_path)
    output_vbet = os.path.join(vbet_gpkg, LayerTypes["VBET_OUTPUTS"].sub_layers['VBET_FULL'].rel_path)
    output_vbet_ia = os.path.join(vbet_gpkg, LayerTypes['VBET_OUTPUTS'].sub_layers['VBET_IA'].rel_path)
    with GeopackageLayer(output_centerlines, write=True) as lyr_cl_init, \
        GeopackageLayer(output_vbet, write=True) as lyr_vbet_init, \
        GeopackageLayer(output_vbet_ia, write=True) as lyr_active_vbet_init, \
            GeopackageLayer(line_network) as lyr_ref:
        fields = {'LevelPathI': ogr.OFTString}
        lyr_cl_init.create_layer(ogr.wkbMultiLineString, spatial_ref=lyr_ref.spatial_ref, fields=fields)
        lyr_vbet_init.create_layer(ogr.wkbMultiPolygon, spatial_ref=lyr_ref.spatial_ref, fields=fields)
        lyr_active_vbet_init.create_layer(ogr.wkbMultiPolygon, spatial_ref=lyr_ref.spatial_ref, fields=fields)

    out_hand = os.path.join(project_folder, LayerTypes['COMPOSITE_HAND_RASTER'].rel_path)
    out_vbet_evidence = os.path.join(project_folder, LayerTypes['COMPOSITE_VBET_EVIDENCE'].rel_path)

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
    level_paths_to_run.append(None)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])

    with rasterio.open(dem) as raster:
        t = raster.transform
        raster_bounds = raster.bounds

    bbox = box(*raster_bounds)
    raster_envelope_geom = VectorBase.shapely2ogr(bbox)
    vbet_clip_buffer_size = VectorBase.rough_convert_metres_to_raster_units(dem, 0.25)
    # channel_buffer_size = VectorBase.rough_convert_metres_to_vector_units(dem, 200)

    # current_vbet = ogr.Geometry(ogr.wkbMultiPolygon)
    # iterate for each level path
    for level_path in level_paths_to_run:
        log.info(f'Processing Level Path: {level_path}')
        temp_folder = os.path.join(project_folder, 'temp', f'levelpath_{level_path}')
        safe_makedirs(temp_folder)

        sql = f"LevelPathI = {level_path}" if level_path is not None else "LevelPathI is NULL"
        # Select channel areas that intersect flow lines
        # catchment_polygon = get_levelpath_catchment(level_path, catchments)

        level_path_polygons = os.path.join(temp_folder, 'channel_polygons.gpkg', f'level_path_{level_path}')
        copy_feature_class(channel_area, level_path_polygons, attribute_filter=sql)  # clip_shape=catchment_polygon)  #

        with GeopackageLayer(level_path_polygons) as lyr_polygons:
            if lyr_polygons.ogr_layer.GetFeatureCount() == 0:
                continue
            channel_bbox = lyr_polygons.ogr_layer.GetExtent()
            channel_buffer_size = lyr_polygons.rough_convert_metres_to_vector_units(400)

        # channel_polygons = collect_feature_class(level_path_polygons)
        # channel_polygons = channel_polygons.MakeValid()

        # if current_vbet.IsEmpty() is False:
        #     current_vbet = current_vbet.MakeValid()
        #     if current_vbet.Contains(channel_polygons):
        #         continue

        # channel_polygons_buffer = channel_polygons.Buffer(channel_buffer_size)
        # channel_polygons = None

        (minX, maxX, minY, maxY) = channel_bbox  # channel_polygons_buffer.GetEnvelope()
        # Create ring
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint(minX, minY)
        ring.AddPoint(maxX, minY)
        ring.AddPoint(maxX, maxY)
        ring.AddPoint(minX, maxY)
        ring.AddPoint(minX, minY)
        channel_envelope_geom = ogr.Geometry(ogr.wkbPolygon)
        channel_envelope_geom.AddGeometry(ring)
        geom_channel_buffer = channel_envelope_geom.Buffer(channel_buffer_size)
        envelope_geom = raster_envelope_geom.Intersection(geom_channel_buffer)
        if envelope_geom.IsEmpty():
            log.error(f'Empty processing envelope for level path {level_path}')
            continue
        envelope = os.path.join(temp_folder, 'envelope_polygon.gpkg', f'level_path_{level_path}')
        with GeopackageLayer(envelope, write=True) as lyr_envelope:
            lyr_envelope.create_layer(ogr.wkbPolygon, 4326)
            lyr_envelope_dfn = lyr_envelope.ogr_layer_def
            feat = ogr.Feature(lyr_envelope_dfn)
            feat.SetGeometry(envelope_geom)
            lyr_envelope.ogr_layer.CreateFeature(feat)

        # use the channel extent to mask all hand input raster and channel area extents
        local_dinfflowdir_ang = os.path.join(temp_folder, f'dinfflowdir_ang_{level_path}.tif')
        local_pitfill_dem = os.path.join(temp_folder, f'pitfill_dem_{level_path}.tif')
        raster_warp(dinfflowdir_ang, local_dinfflowdir_ang, 4326, clip=envelope)
        raster_warp(pitfill_dem, local_pitfill_dem, 4326, clip=envelope)

        evidence_raster = os.path.join(temp_folder, f'vbet_evidence_{level_path}.tif')
        rasterized_channel = os.path.join(temp_folder, f'rasterized_channel_{level_path}.tif')
        rasterize(level_path_polygons, rasterized_channel, local_pitfill_dem, all_touched=True)
        in_rasters['Channel'] = rasterized_channel

        # log.info("Generating HAND")
        hand_raster = os.path.join(temp_folder, f'local_hand_{level_path}.tif')
        dinfdistdown_status = run_subprocess(project_folder, ["mpiexec", "-n", NCORES, "dinfdistdown", "-ang", local_dinfflowdir_ang, "-fel", local_pitfill_dem, "-src", rasterized_channel, "-dd", hand_raster, "-m", "ave", "v"])
        if dinfdistdown_status != 0 or not os.path.isfile(hand_raster):
            raise Exception('TauDEM: dinfdistdown failed')
        in_rasters['HAND'] = hand_raster

        # Open evidence rasters concurrently. We're looping over windows so this shouldn't affect
        # memory consumption too much
        read_rasters = {name: rasterio.open(raster) for name, raster in in_rasters.items()}
        out_meta = read_rasters['HAND'].meta
        out_meta['driver'] = 'GTiff'
        out_meta['count'] = 1
        out_meta['compress'] = 'deflate'

        write_rasters = {}  # {name: rasterio.open(raster, 'w', **out_meta) for name, raster in out_rasters.items()}
        write_rasters['VBET_EVIDENCE'] = rasterio.open(evidence_raster, 'w', **out_meta)

        progbar = ProgressBar(len(list(read_rasters['Slope'].block_windows(1))), 50, "Calculating evidence layer")
        counter = 0
        # Again, these rasters should be orthogonal so their windows should also line up
        for _ji, window in read_rasters['HAND'].block_windows(1):
            progbar.update(counter)
            counter += 1
            block = {block_name: raster.read(1, window=window, masked=True) for block_name, raster in read_rasters.items()}

            normalized = {}
            for name in vbet_run['Inputs']:
                if name in vbet_run['Zones']:
                    transforms = [np.ma.MaskedArray(transform(block[name].data), mask=block[name].mask) for transform in vbet_run['Transforms'][name]]
                    normalized[name] = np.ma.MaskedArray(np.choose(block[f'TRANSFORM_ZONE_{name}'].data, transforms, mode='clip'), mask=block[name].mask)
                else:
                    normalized[name] = np.ma.MaskedArray(vbet_run['Transforms'][name][0](block[name].data), mask=block[name].mask)

            fvals_topo = np.ma.mean([normalized['Slope'], normalized['HAND'], normalized['TWI']], axis=0)
            fvals_channel = 0.995 * block['Channel']
            fvals_evidence = np.maximum(fvals_topo, fvals_channel)

            write_rasters['VBET_EVIDENCE'].write(np.ma.filled(np.float32(fvals_evidence), out_meta['nodata']), window=window, indexes=1)
        write_rasters['VBET_EVIDENCE'].close()

        # Generate VBET Polygon
        valley_bottom_raster = os.path.join(temp_folder, f'valley_bottom_{level_path}.tif')
        generate_vbet_polygon(evidence_raster, rasterized_channel, hand_raster, valley_bottom_raster, temp_folder)
        vbet_polygon = os.path.join(temp_folder, f'valley_bottom_{level_path}.shp')
        log.info('Polygonize VBET')
        polygonize(valley_bottom_raster, 1, vbet_polygon, epsg=4326)
        # Add to existing feature class
        log.info('Add VBET polygon to output')
        polygon = vbet_merge(vbet_polygon, output_vbet, level_path=level_path)
        # if polygon is not None:
        #     for geom in polygon:
        #         current_vbet.AddGeometry(geom)
        # else:
        #     log.warning(f'vbet polygon for level path {level_path} may be empty')

        active_valley_bottom_raster = os.path.join(temp_folder, f'active_valley_bottom_{level_path}.tif')
        generate_vbet_polygon(evidence_raster, rasterized_channel, hand_raster, active_valley_bottom_raster, temp_folder, thresh_value=0.90)
        active_vbet_polygon = os.path.join(temp_folder, f'active_valley_bottom_{level_path}.shp')
        log.info("Polygonize active floodplain polygon")
        polygonize(active_valley_bottom_raster, 1, active_vbet_polygon, epsg=4326)
        # Add to existing feature class
        log.info("Add active floodplain polygon to output")
        _active_polygon = vbet_merge(active_vbet_polygon, output_vbet_ia, level_path=level_path)

        # Generate centerline for level paths only
        if level_path is not None:
            log.info('Generating Centerline from cost path')
            # Generate Centerline from Cost Path
            cost_path_raster = os.path.join(temp_folder, f'cost_path_{level_path}.tif')
            generate_centerline_surface(valley_bottom_raster, cost_path_raster, temp_folder)
            centerline_raster = os.path.join(temp_folder, f'centerline_{level_path}.tif')
            coords = get_endpoints(line_network, 'LevelPathI', level_path, clip_shape=polygon)
            if len(coords) != 2:
                log.error(f'Unable to generate centerline for level path {level_path}: found {len(coords)} target coordinates instead of expected 2.')
                continue
            log.info('Find least cost path for centerline')
            least_cost_path(cost_path_raster, centerline_raster, coords[0], coords[1])
            log.info('Vectorize centerline from Raster')
            centerline_full = raster2line_geom(centerline_raster, 1)

            if polygon is not None:
                polygon = polygon.Buffer(vbet_clip_buffer_size)
                centerline_intersected = polygon.Intersection(centerline_full)
                if centerline_intersected.GetGeometryName() == 'GEOMETRYCOLLECTION':
                    for line in centerline_intersected:
                        line.MakeValid()
                        centerline = ogr.Geometry(ogr.wkbMultiLineString)
                        if line.GetGeometryName() == 'LINESTRING':
                            centerline.AddGeometry(line)
                else:
                    centerline = centerline_intersected
            else:
                centerline = centerline_full

            with GeopackageLayer(output_centerlines, write=True) as lyr_cl:
                out_feature = ogr.Feature(lyr_cl.ogr_layer_def)
                out_feature.SetGeometry(centerline)
                out_feature.SetField('LevelPathI', str(level_path))
                lyr_cl.ogr_layer.CreateFeature(out_feature)
                out_feature = None

        # clean up rasters
        for out_raster, in_raster in {out_hand: hand_raster, out_vbet_evidence: evidence_raster}.items():
            raster_merge(in_raster, out_raster, dem, valley_bottom_raster, temp_folder)

        if debug is False:
            safe_remove_dir(temp_folder)

    # Geomorphic layers
    log.info('Generating geomorphic layers')
    output_floodplain = os.path.join(vbet_gpkg, LayerTypes['VBET_OUTPUTS'].sub_layers['FLOODPLAIN'].rel_path)
    output_active_fp = os.path.join(vbet_gpkg, LayerTypes['VBET_OUTPUTS'].sub_layers['ACTIVE_FLOODPLAIN'].rel_path)
    output_inactive_fp = os.path.join(vbet_gpkg, LayerTypes['VBET_OUTPUTS'].sub_layers['INACTIVE_FLOODPLAIN'].rel_path)

    difference(channel_area, output_vbet, output_floodplain)
    difference(channel_area, output_vbet_ia, output_active_fp)
    difference(output_vbet_ia, output_vbet, output_inactive_fp)

    # Calculate VBET Metrics
    log.info('Generating VBET Segmentation Points')
    segmentation_points = os.path.join(intermediates_gpkg, LayerTypes['INTERMEDIATES'].sub_layers['SEGMENTATION_POINTS'].rel_path)
    stream_size_lookup = get_distance_lookup(inputs_gpkg, intermediates_gpkg, level_paths_to_run)
    generate_segmentation_points(output_centerlines, segmentation_points, stream_size_lookup, distance=50)

    log.info('Generating VBET Segment Polygons')
    segmentation_polygons = os.path.join(intermediates_gpkg, LayerTypes['INTERMEDIATES'].sub_layers['SEGMENTED_VBET_POLYGONS'].rel_path)
    split_vbet_polygons(output_vbet, segmentation_points, segmentation_polygons)

    log.info('Calculating Segment Metrics')
    metric_layers = {'active_floodplain': output_active_fp, 'active_channel': channel_area, 'inactive_floodplain': output_inactive_fp, 'floodplain': output_floodplain}
    calculate_segmentation_metrics(segmentation_polygons, output_centerlines, metric_layers)

    log.info('Summerizing VBET Metrics')
    distance_lookup = get_distance_lookup(inputs_gpkg, intermediates_gpkg, level_paths_to_run, {0: 100.0, 1: 250.0, 2: 1000.0})
    metric_fields = list(metric_layers.keys())
    summerize_vbet_metrics(segmentation_points, segmentation_polygons, level_paths_to_run, distance_lookup, metric_fields)

    if debug is False:
        safe_remove_dir(os.path.join(project_folder, 'temp'))

    # TODO: CREATE INDEX ix_segment_vbet_polygons ON segmented_vbet_polygons(SegDistance);

    # Now add our Geopackages to the project XML
    project.add_project_raster(proj_nodes['Outputs'], LayerTypes['COMPOSITE_VBET_EVIDENCE'])
    project.add_project_raster(proj_nodes['Outputs'], LayerTypes['COMPOSITE_HAND_RASTER'])

    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['VBET_OUTPUTS'])

    # Processing time in hours
    ellapsed_time = time.time() - vbet_timer
    project.add_metadata([
        RSMeta("ProcTimeS", "{:.2f}".format(ellapsed_time), RSMetaTypes.INT),
        RSMeta("ProcTimeHuman", pretty_duration(ellapsed_time))
    ])

    # Report
    report_path = os.path.join(project.project_dir, LayerTypes['REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)
    report = VBETReport(report_path, project)
    report.write()

    log.info('VBET Completed Successfully')


def generate_vbet_polygon(vbet_evidence_raster, rasterized_channel, channel_hand, out_valley_bottom, temp_folder, thresh_value=0.68):

    # Mask to Hand area
    vbet_evidence_masked = os.path.join(temp_folder, f"vbet_evidence_masked_{thresh_value}.tif")
    mask_rasters_nodata(vbet_evidence_raster, channel_hand, vbet_evidence_masked)

    # Threshold Valley Bottom
    valley_bottom_raw = os.path.join(temp_folder, f"valley_bottom_raw_{thresh_value}.tif")
    threshold(vbet_evidence_masked, thresh_value, valley_bottom_raw)

    ds_valley_bottom = gdal.Open(valley_bottom_raw, gdal.GA_Update)
    band_valley_bottom = ds_valley_bottom.GetRasterBand(1)

    # Sieve and Clean Raster
    gdal.SieveFilter(srcBand=band_valley_bottom, maskBand=None, dstBand=band_valley_bottom, threshold=10, connectedness=8, callback=gdal.TermProgress_nocb)
    band_valley_bottom.SetNoDataValue(0)
    band_valley_bottom.FlushCache()
    valley_bottom_sieved = band_valley_bottom.ReadAsArray()

    # Region Tool to find only connected areas
    struct = generate_binary_structure(2, 2)
    regions, _num = label(valley_bottom_sieved, structure=struct)
    valley_bottom_sieved = None

    chan = raster2array(rasterized_channel)
    selection = regions * chan
    chan = None

    values = np.unique(selection)
    valley_bottom_region = np.isin(regions, values.nonzero())
    array2raster(os.path.join(temp_folder, f'regions_{thresh_value}.tif'), vbet_evidence_raster, regions, data_type=gdal.GDT_Int32)
    array2raster(os.path.join(temp_folder, f'valley_bottom_region_{thresh_value}.tif'), vbet_evidence_raster, valley_bottom_region.astype(int), data_type=gdal.GDT_Int32)

    # Clean Raster Edges
    valley_bottom_clean = binary_closing(valley_bottom_region.astype(int), iterations=2)
    array2raster(out_valley_bottom, vbet_evidence_raster, valley_bottom_clean, data_type=gdal.GDT_Int32)


def generate_centerline_surface(vbet_raster, out_cost_path, temp_folder):

    vbet = raster2array(vbet_raster)

    # Generate Inverse Raster for Proximity
    valley_bottom_inverse = (vbet != 1)
    inverse_mask_raster = os.path.join(temp_folder, 'inverse_mask.tif')
    array2raster(inverse_mask_raster, vbet_raster, valley_bottom_inverse)

    # Proximity Raster
    ds_valley_bottom_inverse = gdal.Open(inverse_mask_raster)
    band_valley_bottom_inverse = ds_valley_bottom_inverse.GetRasterBand(1)
    proximity_raster = os.path.join(temp_folder, 'proximity.tif')
    _ds_proximity, band_proximity = new_raster(proximity_raster, vbet_raster, data_type=gdal.GDT_Int32)
    gdal.ComputeProximity(band_valley_bottom_inverse, band_proximity, ['VALUES=1', "DISTUNITS=PIXEL", "COMPRESS=DEFLATE"])
    band_proximity.SetNoDataValue(0)
    band_proximity.FlushCache()
    proximity = band_proximity.ReadAsArray()

    # Rescale Raster
    rescaled = np.interp(proximity, (proximity.min(), proximity.max()), (0.0, 10.0))
    rescaled_raster = os.path.join(temp_folder, 'rescaled.tif')
    array2raster(rescaled_raster, vbet_raster, rescaled, data_type=gdal.GDT_Float32)

    # Centerline Cost Path
    cost_path = 10**((rescaled * -1) + 10) + (rescaled <= 0) * 1000000000000  # 10** (((A) * -1) + 10) + (A <= 0) * 1000000000000
    array2raster(out_cost_path, vbet_raster, cost_path, data_type=gdal.GDT_Float32)


def create_project(huc, output_dir: str, meta: List[RSMeta], meta_dict: Dict[str, str]):
    project_name = 'VBET for HUC {}'.format(huc)
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'VBET', meta, meta_dict)

    realization = project.add_realization(project_name, 'VBET', cfg.version)

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
    parser = argparse.ArgumentParser(
        description='Riverscapes VBET Tool',
        # epilog="This is an epilog"
    )
    parser.add_argument('huc', help='nhd watershed id', type=str)
    parser.add_argument('scenario_code', help='machine code for vbet scenario', type=str)
    parser.add_argument('flowline_network', help='full nhd line network', type=str)
    parser.add_argument('dem', help='dem', type=str)
    parser.add_argument('slope', help='slope', type=str)
    parser.add_argument('hillshade', type=str)
    parser.add_argument('catchments', type=str)
    parser.add_argument('channel_area', type=str)
    parser.add_argument('vaa_table', type=str)
    parser.add_argument('output_dir', help='Folder where output VBET project will be created', type=str)
    parser.add_argument('--level_paths', help='csv list of level paths', type=str, default="")
    parser.add_argument('--pitfill', help='riverscapes project metadata as comma separated key=value pairs', default=None)
    parser.add_argument('--dinfflowdir_ang', help='(optional) a little extra logging ', default=None)
    parser.add_argument('--dinfflowdir_slp', help='Add debug tools for tracing things like memory usage at a performance cost.', default=None)
    parser.add_argument('--twi_raster', help='Add debug tools for tracing things like memory usage at a performance cost.', default=None)
    parser.add_argument('--reach_codes', help='Comma delimited reach codes (FCode) to retain when filtering features. Omitting this option retains all features.', type=str)
    parser.add_argument('--flowline_type', type=str, default='NHD')
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='Add debug tools for tracing things like memory usage at a performance cost.', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # make sure the output folder exists
    safe_makedirs(args.output_dir)

    # Initiate the log file
    log = Logger('VBET')
    log.setup(logPath=os.path.join(args.output_dir, 'vbet.log'), verbose=args.verbose)
    log.title('Riverscapes VBET For HUC: {}'.format(args.huc))

    meta = parse_metadata(args.meta)
    reach_codes = args.reach_codes.split(',') if args.reach_codes else None
    level_paths = args.level_paths.split(',')
    level_paths = level_paths if level_paths != ['.'] else None

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_dir, 'vbet_mem.log')
            retcode, max_obj = ThreadRun(vbet_centerlines, memfile, args.flowline_network, args.dem, args.slope, args.hillshade, args.catchments, args.channel_area, args.vaa_table, args.output_dir, args.scenario_code, args.huc, level_paths, args.pitfill, args.dinfflowdir_ang, args.dinfflowdir_slp, args.twi_raster, meta=meta)
            log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))
        else:
            vbet_centerlines(args.flowline_network, args.dem, args.slope, args.hillshade, args.catchments, args.channel_area, args.vaa_table, args.output_dir, args.scenario_code, args.huc, level_paths, args.pitfill, args.dinfflowdir_ang, args.dinfflowdir_slp, args.twi_raster)
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
