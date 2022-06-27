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
from rasterio import shutil
import numpy as np
from scipy.ndimage import label, generate_binary_structure, binary_closing

from rscommons import RSProject, RSLayer, ModelConfig, ProgressBar, Logger, GeopackageLayer, dotenv, initGDALOGRErrors
from rscommons.vector_ops import copy_feature_class, polygonize, get_endpoints
from rscommons.util import safe_makedirs, parse_metadata, pretty_duration
from rscommons.hand import run_subprocess
from rscommons.vbet_network import copy_vaa_attributes, join_attributes, create_stream_size_zones, get_channel_level_path
from rscommons.classes.rs_project import RSMeta, RSMetaTypes

from vbet.vbet_database import build_vbet_database, load_configuration
from vbet.vbet_raster_ops import rasterize_attribute, raster2array, array2raster, new_raster, rasterize
from vbet.vbet_outputs import vbet_merge
from vbet.vbet_report import VBETReport
from scripts.cost_path import least_cost_path
from scripts.raster2line import raster2line_geom
from vbet.__version__ import __version__

Path = str

NCORES = "8"

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V1/VBET.xsd', __version__)

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
    # 'CHANNEL_AREA_RASTER': RSLayer('Channel Area Raster', 'CHANNEL_AREA_RASTER', 'Raster', 'intermediates/channelarea.tif'),
    # DYNAMIC: 'DA_ZONE_<RASTER>': RSLayer('Drainage Area Zone Raster', 'DA_ZONE_RASTER', "Raster", "intermediates/.tif"),
    # 'NORMALIZED_SLOPE': RSLayer('Normalized Slope', 'NORMALIZED_SLOPE', "Raster", "intermediates/nLoE_Slope.tif"),
    # 'NORMALIZED_HAND': RSLayer('Normalized HAND', 'NORMALIZED_HAND', "Raster", "intermediates/nLoE_Hand.tif"),
    # 'NORMALIZED_TWI': RSLayer('Normalized Topographic Wetness Index (TWI)', 'NORMALIZED_TWI', "Raster", "intermediates/nLoE_TWI.tif"),
    # 'EVIDENCE_TOPO': RSLayer('Topo Evidence', 'EVIDENCE_TOPO', 'Raster', 'intermediates/Topographic_Evidence.tif'),
    # 'EVIDENCE_CHANNEL': RSLayer('Channel Evidence', 'EVIDENCE_CHANNEL', 'Raster', 'intermediates/Channel_Evidence.tif'),
    'INTERMEDIATES': RSLayer('Intermediates', 'Intermediates', 'Geopackage', 'intermediates/vbet_intermediates.gpkg', {
        'VBET_NETWORK': RSLayer('VBET Network', 'VBET_NETWORK', 'Vector', 'vbet_network'),
        'TRANSFORM_ZONES': RSLayer('Transform Zones', 'TRANSFORM_ZONES', 'Vector', 'transform_zones'),
        # 'THIESSEN_POINTS': RSLayer('Thiessen Reach Points', 'THIESSEN_POINTS', 'Vector', 'ThiessenPoints'),
        # 'THIESSEN_AREAS': RSLayer('Thiessen Reach Areas', 'THIESSEN_AREAS', 'Vector', 'ThiessenPolygonsDissolved'),
        # 'CHANNEL_AREA_INTERSECTION': RSLayer('Channel Area Intetrsected by VBET', 'CHANNEL_AREA_INTERSECTION', 'Vector', 'channel_area_intersection')
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
        'VBET_CENTERLINE': RSLayer('VBET Centerline', 'VBET_CENTERLINE', 'Vector', 'vbet_centerline')
    }),
    'REPORT': RSLayer('RSContext Report', 'REPORT', 'HTMLFile', 'outputs/vbet.html')
}


def vbet_centerlines(in_line_network, dem, slope, in_catchments, in_channel_area, vaa_table, project_folder, scenario_code, huc, level_paths=None, pitfill_dem=None, dinfflowdir_ang=None, dinfflowdir_slp=None, twi_raster=None, meta=None, debug=True):

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
    in_rasters['Slope'] = slope

    # generate top level taudem products if they do not exist
    if pitfill_dem is None:
        pitfill_dem = os.path.join(project_folder, 'pitfill.tif')
        pitfill_status = run_subprocess(project_folder, ["mpiexec", "-n", NCORES, "pitremove", "-z", dem, "-fel", pitfill_dem])
        if pitfill_status != 0 or not os.path.isfile(pitfill_dem):
            raise Exception('TauDEM: pitfill failed')

    if not all([dinfflowdir_ang, dinfflowdir_slp]):
        dinfflowdir_slp = os.path.join(project_folder, 'dinfflowdir_slp.tif')
        dinfflowdir_ang = os.path.join(project_folder, 'dinfflowdir_ang.tif')
        dinfflowdir_status = run_subprocess(project_folder, ["mpiexec", "-n", NCORES, "dinfflowdir", "-fel", pitfill_dem, "-ang", dinfflowdir_ang, "-slp", dinfflowdir_slp])
        if dinfflowdir_status != 0 or not os.path.isfile(dinfflowdir_ang):
            raise Exception('TauDEM: dinfflowdir failed')

    if not twi_raster:
        twi_raster = os.path.join(project_folder, 'local_twi')
        sca = os.path.join(project_folder, 'sca.tif')
        twi_status = run_subprocess(project_folder, ["mpiexec", "-n", NCORES, "twi", "-slp", dinfflowdir_slp, "-sca", sca, '-twi', twi_raster])
        if twi_status != 0 or not os.path.isfile(twi_raster):
            raise Exception('TauDEM: TWI failed')
    in_rasters['TWI'] = twi_raster

    for zone in vbet_run['Zones']:
        log.info(f'Rasterizing stream transform zones for {zone}')
        raster_name = os.path.join(project_folder, 'intermediates', f'{zone.lower()}_transform_zones.tif')
        rasterize_attribute(catchments_path, raster_name, dem, f'{zone}_Zone')
        in_rasters[f'TRANSFORM_ZONE_{zone}'] = raster_name
        transform_zone_rs = RSLayer(f'Transform Zones for {zone}', f'TRANSFORM_ZONE_{zone.upper()}', 'Raster', raster_name)
        project.add_project_raster(proj_nodes['Intermediates'], transform_zone_rs)

    # run for orphan waterbodies??

    # Initialize Outputs
    output_centerlines = os.path.join(vbet_gpkg, "vbet_centerlines")
    output_vbet = os.path.join(vbet_gpkg, LayerTypes["OUTPUTS"].sub_layers['VBET_FULL'])
    output_vbet_ia = os.path.join(vbet_gpkg, LayerTypes['OUTPUTS'].sub_layers['VBET_IA'])
    with GeopackageLayer(output_centerlines, write=True) as lyr_cl_init, \
        GeopackageLayer(output_vbet, write=True) as lyr_vbet_init, \
        GeopackageLayer(output_vbet_ia, write=True) as lyr_active_vbet_init, \
            GeopackageLayer(line_network) as lyr_ref:
        fields = {'LevelPathI': ogr.OFTString}
        lyr_cl_init.create_layer(ogr.wkbMultiLineString, spatial_ref=lyr_ref.spatial_ref, fields=fields)
        lyr_vbet_init.create_layer(ogr.wkbPolygon, spatial_ref=lyr_ref.spatial_ref, fields=fields)
        lyr_active_vbet_init.create_layer(ogr.wkbPolygon, spatial_ref=lyr_ref.spatial_ref, fields=fields)

    out_hand = os.path.join(project_folder, 'outputs', "composite_hand.tif")
    out_vbet_evidence = os.path.join(project_folder, 'outputs', 'composite_vbet_evidence.tif')

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

    # iterate for each level path
    for level_path in level_paths_to_run:

        temp_folder = os.path.join(project_folder, 'temp', f'levelpath_{level_path}')
        safe_makedirs(temp_folder)

        sql = f"LevelPathI = {level_path}" if level_path is not None else "LevelPathI is NULL"
        # Select channel areas that intersect flow lines
        level_path_polygons = os.path.join(project_folder, 'temp', 'channel_polygons.gpkg', f'level_path_{level_path}')
        copy_feature_class(channel_area, level_path_polygons, attribute_filter=sql)

        # current_vbet = collect_feature_class(output_vbet)
        # if current_vbet is not None:
        #     channel_polygons = collect_feature_class(level_path_polygons)
        #     if current_vbet.Contains(channel_polygons):
        #         continue

        evidence_raster = os.path.join(temp_folder, f'vbet_evidence_{level_path}.tif')
        rasterized_channel = os.path.join(temp_folder, f'rasterized_channel_{level_path}.tif')
        rasterize(level_path_polygons, rasterized_channel, dem, all_touched=True)
        in_rasters['Channel'] = rasterized_channel

        # log.info("Generating HAND")
        hand_raster = os.path.join(temp_folder, f'local_hand_{level_path}.tif')
        dinfdistdown_status = run_subprocess(project_folder, ["mpiexec", "-n", NCORES, "dinfdistdown", "-ang", dinfflowdir_ang, "-fel", pitfill_dem, "-src", rasterized_channel, "-dd", hand_raster, "-m", "ave", "v"])
        if dinfdistdown_status != 0 or not os.path.isfile(hand_raster):
            raise Exception('TauDEM: dinfdistdown failed')
        in_rasters['HAND'] = hand_raster

        # Open evidence rasters concurrently. We're looping over windows so this shouldn't affect
        # memory consumption too much
        read_rasters = {name: rasterio.open(raster) for name, raster in in_rasters.items()}
        out_meta = read_rasters['Slope'].meta
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
        polygonize(valley_bottom_raster, 1, vbet_polygon, epsg=4326)
        # Add to existing feature class
        polygon = vbet_merge(vbet_polygon, output_vbet, level_path=level_path)

        active_valley_bottom_raster = os.path.join(temp_folder, f'active_valley_bottom_{level_path}.tif')
        generate_vbet_polygon(evidence_raster, rasterized_channel, hand_raster, active_valley_bottom_raster, temp_folder, threshold=0.90)
        active_vbet_polygon = os.path.join(temp_folder, f'active_valley_bottom_{level_path}.shp')
        polygonize(active_valley_bottom_raster, 1, active_vbet_polygon, epsg=4326)
        # Add to existing feature class
        active_polygon = vbet_merge(active_vbet_polygon, output_vbet_ia, level_path=level_path)

        # Generate centerline for level paths only
        if level_path is not None:
            # Generate Centerline from Cost Path
            cost_path_raster = os.path.join(temp_folder, f'cost_path_{level_path}.tif')
            generate_centerline_surface(valley_bottom_raster, cost_path_raster, temp_folder)
            centerline_raster = os.path.join(temp_folder, f'centerline_{level_path}.tif')
            coords = get_endpoints(line_network, 'LevelPathI', level_path)
            least_cost_path(cost_path_raster, centerline_raster, coords[0], coords[1])
            centerline_full = raster2line_geom(centerline_raster, 1)

            if polygon is not None:
                centerline_intersected = polygon.Intersection(centerline_full)
                if centerline_intersected.GetGeometryName() == 'GeometryCollection':
                    for line in centerline_intersected:
                        centerline = ogr.Geometry(ogr.wkbMultiLineString)
                        if line.GetGeometryName() == 'LineString':
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
        rio_vbet = rasterio.open(valley_bottom_raster)

        for out_raster, in_raster in {out_hand: hand_raster, out_vbet_evidence: evidence_raster}.items():
            if os.path.exists(out_raster):
                out_temp = os.path.join(temp_folder, 'temp_raster')
                rio_dest = rasterio.open(out_raster)
                out_meta = rio_dest.meta
                out_meta['driver'] = 'GTiff'
                out_meta['count'] = 1
                out_meta['compress'] = 'deflate'
                rio_temp = rasterio.open(out_temp, 'w', **out_meta)
                rio_source = rasterio.open(in_raster)
                for _ji, window in rio_source.block_windows(1):
                    # progbar.update(counter)
                    # counter += 1
                    array_vbet_mask = np.ma.MaskedArray(rio_vbet.read(1, window=window, masked=True).data)
                    array_source = np.ma.MaskedArray(rio_source.read(1, window=window, masked=True).data, mask=array_vbet_mask.mask)
                    array_dest = np.ma.MaskedArray(rio_dest.read(1, window=window, masked=True).data, mask=array_vbet_mask.mask)
                    array_out = np.choose(array_vbet_mask, [array_dest, array_source])

                    rio_temp.write(np.ma.filled(np.float32(array_out), out_meta['nodata']), window=window, indexes=1)
                rio_temp.close()
                rio_dest.close()
                rio_source.close()
                shutil.copyfiles(out_temp, out_raster)
            else:
                rio_source = rasterio.open(in_raster)
                out_meta = rio_source.meta
                out_meta['driver'] = 'GTiff'
                out_meta['count'] = 1
                out_meta['compress'] = 'deflate'
                rio_dest = rasterio.open(out_raster, 'w', **out_meta)
                for _ji, window in rio_source.block_windows(1):
                    array_vbet_mask = np.ma.MaskedArray(rio_vbet.read(1, window=window, masked=True).data)
                    array_source = np.ma.MaskedArray(rio_source.read(1, window=window, masked=True).data, mask=array_vbet_mask.mask)
                    rio_dest.write(np.ma.filled(np.float32(array_source), out_meta['nodata']), window=window, indexes=1)
                rio_dest.close()
                rio_source.close()

        if debug is False:
            os.rmdir(temp_folder)

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


def generate_vbet_polygon(vbet_evidence_raster, rasterized_channel, channel_hand, out_valley_bottom, temp_folder, threshold=0.68):

    # Read initial rasters as arrays
    vbet = raster2array(vbet_evidence_raster)
    chan = raster2array(rasterized_channel)
    hand = raster2array(channel_hand)

    # Generate Valley Bottom
    valley_bottom = ((vbet + chan) >= threshold) * ((hand + chan) > 0)  # ((A + B) < 0.68) * (C > 0)
    valley_bottom_raw = os.path.join(temp_folder, "valley_bottom_raw.tif")
    array2raster(valley_bottom_raw, vbet_evidence_raster, valley_bottom, data_type=gdal.GDT_Int32)

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
    selection = regions * chan
    values = np.unique(selection)
    valley_bottom_region = np.isin(regions, values.nonzero())
    array2raster(os.path.join(temp_folder, 'regions.tif'), vbet_evidence_raster, regions, data_type=gdal.GDT_Int32)
    array2raster(os.path.join(temp_folder, 'valley_bottom_region.tif'), vbet_evidence_raster, valley_bottom_region.astype(int), data_type=gdal.GDT_Int32)

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
    parser.add_argument('catchments', type=str)
    parser.add_argument('channel_area', type=str)
    parser.add_argument('vaa_table')
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

    # try:
    if args.debug is True:
        from rscommons.debug import ThreadRun
        memfile = os.path.join(args.output_dir, 'vbet_mem.log')
        retcode, max_obj = ThreadRun(vbet_centerlines, memfile, args.flowline_network, args.dem, args.slope, args.catchments, args.channel_area, args.vaa_table, args.output_dir, args.scenario_code, args.huc, args.pitfill, args.dinfflowdir_ang, args.dinfflowdir_slp, args.twi_raster, level_paths=level_paths, meta=meta)
        log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))

    else:

        vbet_centerlines(args.flowline_network, args.dem, args.slope, args.catchments, args.channel_area, args.vaa_table, args.output_dir, args.scenario_code, args.huc, level_paths, args.pitfill, args.dinfflowdir_ang, args.dinfflowdir_slp, args.twi_raster)

    # except Exception as e:
    #     log.error(e)
    #     traceback.print_exc(file=sys.stdout)
    #     sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
