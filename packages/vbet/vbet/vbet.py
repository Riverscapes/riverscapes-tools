# Name:     Valley Bottom
#
# Purpose:  Perform initial VBET analysis that can be used by the BRAT conservation
#           module
#
# Author:   Matt Reimer, Kelly Whitehead
#
# Date:     April 9, 2021
#
# Vectorize polygons from raster
# https://gis.stackexchange.com/questions/187877/how-to-polygonize-raster-to-shapely-polygons
# -------------------------------------------------------------------------------
import argparse
import os
import sys
import traceback
import json
import time
from typing import List, Dict
# Leave OSGEO import alone. It is necessary even if it looks unused
from osgeo import gdal, ogr
import rasterio
from rasterio.features import shapes
import rasterio.mask
import numpy as np
from shapely.geometry import MultiPolygon
from rscommons.classes.rs_project import RSMeta, RSMetaTypes

from rscommons.util import safe_makedirs, parse_metadata, pretty_duration
from rscommons import RSProject, RSLayer, ModelConfig, ProgressBar, Logger, dotenv, initGDALOGRErrors, TempRaster, VectorBase
from rscommons import GeopackageLayer
from rscommons.vector_ops import difference, copy_feature_class, dissolve_feature_class, intersect_feature_classes, remove_holes_feature_class, geom_validity_fix
from rscommons.thiessen.vor import NARVoronoi
from rscommons.thiessen.shapes import centerline_points
from rscommons.vbet_network import vbet_network, create_stream_size_zones, copy_vaa_attributes, join_attributes
from rscommons.classes.raster import get_data_polygon

from vbet.vbet_database import load_configuration, build_vbet_database
from vbet.vbet_metrics import build_vbet_metric_tables
from vbet.vbet_report import VBETReport
from vbet.vbet_raster_ops import rasterize, raster_clean, rasterize_attribute
from vbet.vbet_outputs import threshold, sanitize
from vbet.vbet_centerline import vbet_centerline
from vbet.__version__ import __version__

Path = str

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/VBET.xsd', __version__)

LayerTypes = {
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'inputs/dem.tif'),
    'SLOPE_RASTER': RSLayer('Slope Raster', 'SLOPE_RASTER', 'Raster', 'inputs/slope.tif'),
    'HAND_RASTER': RSLayer('Hand Raster', 'HAND_RASTER', 'Raster', 'inputs/HAND.tif'),
    'TWI_RASTER': RSLayer('Topographic Wetness Index (TWI) Raster', 'TWI_RASTER', 'Raster', 'inputs/twi.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/vbet_inputs.gpkg', {
        'FLOWLINES': RSLayer('NHD Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'FLOW_AREAS': RSLayer('NHD Flow Areas', 'FLOW_AREAS', 'Vector', 'flowareas'),
        'FLOWLINES_VAA': RSLayer('NHD Flowlines with Attributes', 'FLOWLINES_VAA', 'Vector', 'Flowlines_VAA'),
        'CHANNEL_AREA_POLYGONS': RSLayer('Channel Area Polygons', 'CHANNEL_AREA_POLYGONS', 'Vector', 'channel_area_polygons'),
        'CATCHMENTS': RSLayer('NHD Catchments', 'CATCHMENTS', 'Vector', 'catchments'),
    }),
    'CHANNEL_AREA_RASTER': RSLayer('Channel Area Raster', 'CHANNEL_AREA_RASTER', 'Raster', 'intermediates/channelarea.tif'),
    # 'CHANNEL_DISTANCE': RSLayer('Channel Euclidean Distance', 'CHANNEL_DISTANCE', "Raster", "intermediates/ChannelEuclideanDist.tif"),
    # DYNAMIC: 'DA_ZONE_<RASTER>': RSLayer('Drainage Area Zone Raster', 'DA_ZONE_RASTER', "Raster", "intermediates/.tif"),
    'NORMALIZED_SLOPE': RSLayer('Normalized Slope', 'NORMALIZED_SLOPE', "Raster", "intermediates/nLoE_Slope.tif"),
    'NORMALIZED_HAND': RSLayer('Normalized HAND', 'NORMALIZED_HAND', "Raster", "intermediates/nLoE_Hand.tif"),
    # 'NORMALIZED_CHANNEL_DISTANCE': RSLayer('Normalized Channel Distance', 'NORMALIZED_CHANNEL_DISTANCE', "Raster", "intermediates/nLoE_ChannelDist.tif"),
    'NORMALIZED_TWI': RSLayer('Normalized Topographic Wetness Index (TWI)', 'NORMALIZED_TWI', "Raster", "intermediates/nLoE_TWI.tif"),
    'EVIDENCE_TOPO': RSLayer('Topo Evidence', 'EVIDENCE_TOPO', 'Raster', 'intermediates/Topographic_Evidence.tif'),
    'EVIDENCE_CHANNEL': RSLayer('Channel Evidence', 'EVIDENCE_CHANNEL', 'Raster', 'intermediates/Channel_Evidence.tif'),
    'INTERMEDIATES': RSLayer('Intermediates', 'Intermediates', 'Geopackage', 'intermediates/vbet_intermediates.gpkg', {
        'VBET_NETWORK': RSLayer('VBET Network', 'VBET_NETWORK', 'Vector', 'vbet_network'),
        'TRANSFORM_ZONES': RSLayer('Transform Zones', 'TRANSFORM_ZONES', 'Vector', 'transform_zones'),
        'THIESSEN_POINTS': RSLayer('Thiessen Reach Points', 'THIESSEN_POINTS', 'Vector', 'ThiessenPoints'),
        'THIESSEN_AREAS': RSLayer('Thiessen Reach Areas', 'THIESSEN_AREAS', 'Vector', 'ThiessenPolygonsDissolved'),
        # 'CHANNEL_AREA_INTERSECTION': RSLayer('Channel Area Intetrsected by VBET', 'CHANNEL_AREA_INTERSECTION', 'Vector', 'channel_area_intersection')
        # We also add all tht raw thresholded shapes here but they get added dynamically later
    }),
    # Same here. Sub layers are added dynamically later.
    'VBET_EVIDENCE': RSLayer('VBET Evidence Raster', 'VBET_EVIDENCE', 'Raster', 'outputs/VBET_Evidence.tif'),
    'VBET_OUTPUTS': RSLayer('VBET', 'VBET_OUTPUTS', 'Geopackage', 'outputs/vbet.gpkg', {
        'VBET_FULL': RSLayer('VBET Full Extent', 'VBET_FULL', 'Vector', 'vbet_full'),
        'VBET_IA': RSLayer('VBET Inactive/Active Boundary', 'VBET_IA', 'Vector', 'vbet_ia'),
        'VBET_CHANNEL_AREA': RSLayer('VBET Channel Area', 'VBET_CHANNEL_AREA', 'Vector', 'vbet_channel_area'),
        'ACTIVE_FLOODPLAIN': RSLayer('Active Floodplain', 'ACTIVE_FLOODPLAIN', 'Vector', 'active_floodplain'),
        'INACTIVE_FLOODPLAIN': RSLayer('Inactive Floodplain', 'INACTIVE_FLOODPLAIN', 'Vector', 'inactive_floodplain')
    }),
    'REPORT': RSLayer('RSContext Report', 'REPORT', 'HTMLFile', 'outputs/vbet.html')
}

flowline_fieldmap = {
    'NHD': {
        'ReachID': 'NHDPlusID',
        'PathID': 'LevelPathI',
        'DownPathID': 'DnLevelPat',
        'Divergence': 'Divergence',
        'StreamOrder': 'StreamOrde'},
    'NetMap': {
        'ReachID': 'ID',
        'PathID': 'CHAN_ID',
        'DownPathID': 'DOWN_ID',
        'Divergence': '',
        'StreamOrder': 'STRM_ORDER'},
    'Custom': {
        'ReachID': 'ReachID',
        'PathID': 'PathID',
        'DownPathID': 'DownPathID',
        'Divergence': 'Divergence',
        'StreamOrder': 'StreamOrder'
    }}


def vbet(huc: int, scenario_code: str, inputs: Dict[str, str], vaa_table: Path, project_folder: Path, reach_codes: List[str], meta: Dict[str, str], flowline_type: str = 'NHD', epsg=cfg.OUTPUT_EPSG, thresh_vals={'VBET_IA': 0.90, 'VBET_FULL': 0.68}):
    """generate vbet evidence raster and threshold polygons for a watershed

    Args:
        huc (int): HUC code for watershed
        scenario_code (str): database machine code for scenario to run
        inputs (dict): input names and path
        vaa_table (Path): NHD VAA table to join with flowlines
        project_folder (Path): path for project results
        reach_codes (List[int]): NHD reach codes for features to include in outputs
        meta (Dict[str,str]): dictionary of riverscapes metadata key: value pairs
    """

    vbet_timer = time.time()
    log = Logger('VBET')
    log.info('Starting VBET v.{}'.format(cfg.version))

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

    flowline_fields = flowline_fieldmap[flowline_type]

    vbet_summary_field = 'vbet_level_path'  # This value is in vbet_metrics.sql, modify with care...

    # Input Preparation
    # Make sure we're starting with a fresh slate of new geopackages
    inputs_gpkg_path = os.path.join(project_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg_path = os.path.join(project_folder, LayerTypes['INTERMEDIATES'].rel_path)
    vbet_path = os.path.join(project_folder, LayerTypes['VBET_OUTPUTS'].rel_path)
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(intermediates_gpkg_path)
    GeopackageLayer.delete(vbet_path)

    raster_extent = MultiPolygon(get_data_polygon(inputs['DEM']))
    raster_extent_geom = VectorBase.shapely2ogr(raster_extent)

    project_inputs = {}
    for input_name, input_path in inputs.items():
        if os.path.splitext(input_path)[1] in ['.tif', '.tiff', '.TIF', '.TIFF']:
            _proj_slope_node, project_inputs[input_name] = project.add_project_raster(proj_nodes['Inputs'], LayerTypes[input_name], input_path)
        else:
            project_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers[input_name].rel_path)
            copy_feature_class(input_path, project_path, epsg=epsg, clip_shape=raster_extent_geom)
            project_inputs[input_name] = project_path
    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])

    vaa_table_name = copy_vaa_attributes(project_inputs['FLOWLINES'], vaa_table)
    if flowline_type == 'NHD':
        vaa_fields = ['LevelPathI', 'Divergence', 'DnLevelPat']  # + ["StreamOrde"]  # + ['HydroSeq', 'DnHydroSeq', 'UpHydroSeq']
        flowlines_vaa_path = join_attributes(inputs_gpkg_path, "Flowlines_VAA", os.path.basename(project_inputs['FLOWLINES']), vaa_table_name, 'NHDPlusID', vaa_fields, epsg)
        catchments_vaa_path = join_attributes(inputs_gpkg_path, "Catchments_VAA", os.path.basename(project_inputs['CATCHMENTS']), vaa_table_name, 'NHDPlusID', vaa_fields, epsg, geom_type='POLYGON')
    else:
        flowlines_vaa_path = project_inputs['FLOWLINES']

    # Build Transformation Tables
    build_vbet_database(inputs_gpkg_path)
    # with GeopackageLayer(project_inputs['FLOWLINES']) as lyr:
    #     degree_factor = lyr.rough_convert_metres_to_vector_units(1)

    # Load configuration from table
    vbet_run = load_configuration(scenario_code, inputs_gpkg_path)

    # Create a copy of the flow lines with just the perennial and also connectors inside flow areas
    log.info('Building vbet network')
    network_path = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['VBET_NETWORK'].rel_path)
    vbet_network(flowlines_vaa_path, project_inputs['FLOW_AREAS'], network_path, epsg, reach_codes)

    # Create Zones
    log.info('Building drainage area zones')
    catchments_path = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['TRANSFORM_ZONES'].rel_path)
    vaa_table_path = os.path.join(inputs_gpkg_path, vaa_table_name)
    create_stream_size_zones(project_inputs['CATCHMENTS'], vaa_table_path, 'NHDPlusID', 'StreamOrde', vbet_run['Zones'], catchments_path)  # TODO not fully generic here. Relies on NHD catchments and vaa attributes

    # Create Scenario Input Rasters
    in_rasters = {}
    out_rasters = {}
    if 'Slope' in vbet_run['Inputs']:
        log.info("Adding Slope Input")
        in_rasters['Slope'] = project_inputs['SLOPE_RASTER']
        out_rasters['NORMALIZED_SLOPE'] = os.path.join(project_folder, LayerTypes['NORMALIZED_SLOPE'].rel_path)

    # Rasterize the channel polygon and write to raster
    log.info('Writing channel raster using slope as a template')
    channel_area_raster = os.path.join(project_folder, LayerTypes['CHANNEL_AREA_RASTER'].rel_path)
    rasterize(project_inputs['CHANNEL_AREA_POLYGONS'], channel_area_raster, project_inputs['SLOPE_RASTER'], all_touched=True)
    project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['CHANNEL_AREA_RASTER'])
    in_rasters['Channel'] = channel_area_raster

    # Generate HAND from dem and rasterized flow polygons
    if 'HAND' in vbet_run['Inputs']:
        hand_raster = os.path.join(project_folder, LayerTypes['HAND_RASTER'].rel_path)
        twi_raster = os.path.join(project_folder, LayerTypes['TWI_RASTER'].rel_path)
        in_rasters['HAND'] = hand_raster
        out_rasters['NORMALIZED_HAND'] = os.path.join(project_folder, LayerTypes['NORMALIZED_HAND'].rel_path)

        in_rasters['TWI'] = twi_raster
        out_rasters['NORMALIZED_TWI'] = os.path.join(project_folder, LayerTypes['NORMALIZED_TWI'].rel_path)

    # Generate da Zone rasters
    for zone in vbet_run['Zones']:
        log.info(f'Rasterizing stream transform zones for {zone}')
        raster_name = os.path.join(project_folder, 'intermediates', f'{zone.lower()}_transform_zones.tif')
        rasterize_attribute(catchments_path, raster_name, project_inputs['SLOPE_RASTER'], f'{zone}_Zone')
        in_rasters[f'TRANSFORM_ZONE_{zone}'] = raster_name
        transform_zone_rs = RSLayer(f'Transform Zones for {zone}', f'TRANSFORM_ZONE_{zone.upper()}', 'Raster', raster_name)
        project.add_project_raster(proj_nodes['Intermediates'], transform_zone_rs)

    for raster_name in ['EVIDENCE_TOPO', 'EVIDENCE_CHANNEL']:
        out_rasters[raster_name] = os.path.join(project_folder, LayerTypes[raster_name].rel_path)
    evidence_raster = os.path.join(project_folder, LayerTypes['VBET_EVIDENCE'].rel_path)

    # Open evidence rasters concurrently. We're looping over windows so this shouldn't affect
    # memory consumption too much
    read_rasters = {name: rasterio.open(raster) for name, raster in in_rasters.items()}
    out_meta = read_rasters['Slope'].meta
    out_meta['driver'] = 'GTiff'
    out_meta['count'] = 1
    out_meta['compress'] = 'deflate'

    # We use this to buffer the output
    cell_size = abs(read_rasters['Slope'].get_transform()[1])

    write_rasters = {name: rasterio.open(raster, 'w', **out_meta) for name, raster in out_rasters.items()}
    write_rasters['VBET_EVIDENCE'] = rasterio.open(evidence_raster, 'w', **out_meta)

    progbar = ProgressBar(len(list(read_rasters['Slope'].block_windows(1))), 50, "Calculating evidence layer")
    counter = 0
    # Again, these rasters should be orthogonal so their windows should also line up
    for _ji, window in read_rasters['Slope'].block_windows(1):
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

        # Fill the masked values with the appropriate nodata vals
        # Unthresholded in the base band (mostly for debugging)
        write_rasters['VBET_EVIDENCE'].write(np.ma.filled(np.float32(fvals_evidence), out_meta['nodata']), window=window, indexes=1)

        write_rasters['NORMALIZED_SLOPE'].write(normalized['Slope'].astype('float32').filled(out_meta['nodata']), window=window, indexes=1)
        write_rasters['NORMALIZED_HAND'].write(normalized['HAND'].astype('float32').filled(out_meta['nodata']), window=window, indexes=1)
        write_rasters['NORMALIZED_TWI'].write(normalized['TWI'].astype('float32').filled(out_meta['nodata']), window=window, indexes=1)

        write_rasters['EVIDENCE_CHANNEL'].write(np.ma.filled(np.float32(fvals_channel), out_meta['nodata']), window=window, indexes=1)
        write_rasters['EVIDENCE_TOPO'].write(np.ma.filled(np.float32(fvals_topo), out_meta['nodata']), window=window, indexes=1)
    progbar.finish()

    # Close all rasters here
    for raster_obj in list(read_rasters.values()) + list(write_rasters.values()):
        raster_obj.close()

    # The remaining rasters get added to the project
    for raster_name in out_rasters:
        project.add_project_raster(proj_nodes["Intermediates"], LayerTypes[raster_name])
    project.add_project_raster(proj_nodes['Outputs'], LayerTypes['VBET_EVIDENCE'])

    min_geom_size = 10 * (cell_size ** 2)

    # log.info('Subdividing the network along regular intervals')
    # flowline_thiessen_points_groups = centerline_points(network_path, distance=degree_factor * 10, fields=[flowline_fields['PathID']], divergence_field=flowline_fields['Divergence'], downlevel_field=flowline_fields['DownPathID'])  # 'LevelPathI'
    # flowline_thiessen_points = [pt for group in flowline_thiessen_points_groups.values() for pt in group]

    # Exterior is the shell and there is only ever 1
    # log.info("Creating Thiessen Polygons")
    # myVorL = NARVoronoi(flowline_thiessen_points)

    # Generate Thiessen Polys
    # myVorL.createshapes()

    # with GeopackageLayer(network_path) as flow_lyr:
    #     # Set the output spatial ref as this for the whole project
    #     out_srs = flow_lyr.spatial_ref

    # Dissolve by flowlines
    # log.info("Dissolving Thiessen Polygons")
    # dissolved_polys = myVorL.dissolve_by_property(flowline_fields['PathID'])  # 'LevelPathI'

    # dissolved_attributes = {flowline_fields['PathID']: ogr.OFTString}  # 'LevelPathI'

    # simple_save([{'geom': pt.point} for pt in flowline_thiessen_points], ogr.wkbPoint, out_srs, "ThiessenPoints", intermediates_gpkg_path)
    # simple_save([{'geom': g, 'attributes': {flowline_fields['PathID']: k}} for k, g in dissolved_polys.items()], ogr.wkbPolygon, out_srs, "ThiessenPolygonsDissolved", intermediates_gpkg_path, dissolved_attributes)

    # thiessen_fc = os.path.join(intermediates_gpkg_path, "ThiessenPolygonsDissolved")
    # modified_catchments_vaa_path = os.path.join(intermediates_gpkg_path, 'catchments_vaa_modified_path')
    # copy_feature_class(catchments_vaa_path, modified_catchments_vaa_path)

    # with GeopackageLayer(modified_catchments_vaa_path, write=True) as lyr:
    #     for feat, *_ in lyr.iterate_features("Updating divergent reach paths..."):
    #         if feat.GetField("Divergence") == 2:
    #             new_feat = feat.Clone()
    #             output = feat.GetField("DnLevelPat")
    #             new_feat.SetField("LevelPathI", output)
    #             lyr.ogr_layer.SetFeature(new_feat)

    catchments_dissolved_path = os.path.join(intermediates_gpkg_path, "catchments_dissolved")
    dissolve_feature_class(catchments_vaa_path, catchments_dissolved_path, epsg, vbet_summary_field)

    vbet_threshold = {}
    for str_val, thr_val in thresh_vals.items():

        plgnize_id = f'THRESH_{int(thr_val * 100)}'
        with TempRaster(f'vbet_raw_thresh_{int(thr_val * 100)}') as tmp_raw_thresh, \
                TempRaster(f'vbet_cleaned_thresh_{int(thr_val * 100)}') as tmp_cleaned_thresh:

            log.debug('Temporary threshold raster: {}'.format(tmp_raw_thresh.filepath))
            threshold(evidence_raster, thr_val, tmp_raw_thresh.filepath)
            raster_clean(tmp_raw_thresh.filepath, tmp_cleaned_thresh.filepath, buffer_pixels=1)

            # Threshold and VBET output layers
            plgnize_lyr = RSLayer(f'Raw Threshold at {int(thr_val * 100)}%', plgnize_id, 'Vector', plgnize_id.lower())
            LayerTypes['INTERMEDIATES'].add_sub_layer(plgnize_id, plgnize_lyr)
            vbet_lyr = LayerTypes['VBET_OUTPUTS'].sub_layers[str_val]

            with rasterio.open(tmp_cleaned_thresh.filepath, 'r') as raster:
                with GeopackageLayer(catchments_dissolved_path, write=True) as lyr_reaches, \
                        GeopackageLayer(project_inputs['CHANNEL_AREA_POLYGONS']) as lyr_channel_area_polygons, \
                        GeopackageLayer(os.path.join(intermediates_gpkg_path, plgnize_lyr.rel_path), write=True) as lyr_output:

                    lyr_output.create_layer_from_ref(lyr_reaches)

                    out_layer_defn = lyr_output.ogr_layer.GetLayerDefn()
                    field_count = out_layer_defn.GetFieldCount()

                    rejected_geoms = []

                    lyr_output.ogr_layer.StartTransaction()

                    for reach_feat, *_ in lyr_reaches.iterate_features("Processing Reaches"):
                        reach_attributes = {}
                        for n in range(field_count):
                            field = out_layer_defn.GetFieldDefn(n)
                            value = reach_feat.GetField(field.name)
                            reach_attributes[field.name] = value

                        geom = reach_feat.GetGeometryRef()
                        buff = geom.Buffer(cell_size)
                        geom_json = buff.ExportToJson()
                        poly = json.loads(geom_json)
                        data, mask_transform = rasterio.mask.mask(raster, [poly], crop=True)

                        if all(x > 0 for x in data.shape):
                            out_shapes = list(g for g, v in shapes(data, transform=mask_transform) if v == 1)
                            for out_shape in out_shapes:
                                shape_intersected = False
                                out_geom = ogr.CreateGeometryFromJson(json.dumps(out_shape))
                                if not out_geom.IsValid():
                                    out_geom = geom_validity_fix(out_geom)
                                out_feat = ogr.Feature(out_layer_defn)
                                out_feat.SetGeometry(out_geom)
                                for field, value in reach_attributes.items():
                                    out_feat.SetField(field, value)
                                for test_feat, *_ in lyr_channel_area_polygons.iterate_features(clip_shape=out_geom):
                                    test_geom = test_feat.GetGeometryRef()
                                    if test_geom.Intersects(out_geom):
                                        shape_intersected = True
                                        lyr_output.ogr_layer.CreateFeature(out_feat)
                                        break
                                if shape_intersected is False:
                                    rejected_geoms.append(out_feat)
                    lyr_output.ogr_layer.CommitTransaction()

                    save_feats = []
                    iterate = True
                    while iterate is True:
                        log.info(f"Checking {len(rejected_geoms)} features for inclusion by adjacency")
                        iterate = False
                        rejected_geoms2 = []
                        for out_feat in rejected_geoms:
                            shape_intersected = False
                            out_geom = out_feat.GetGeometryRef()
                            for test_feat, *_ in lyr_output.iterate_features(clip_shape=out_geom):
                                test_geom = test_feat.GetGeometryRef()
                                if test_geom.Intersects(out_geom):  # .Buffer(0.0001)
                                    save_feats.append(out_feat)
                                    iterate = True
                                    shape_intersected = True
                                    break
                            if shape_intersected is False:
                                rejected_geoms2.append(out_feat)
                        rejected_geoms = rejected_geoms2

                    lyr_output.ogr_layer.StartTransaction()
                    for feat in save_feats:
                        lyr_output.ogr_layer.CreateFeature(feat)
                    lyr_output.ogr_layer.CommitTransaction()

        # Now the final sanitization
        # sanitize(
        #     str_val,
        #     '{}/{}'.format(intermediates_gpkg_path, plgnize_lyr.rel_path),
        #     '{}/{}'.format(vbet_path, vbet_lyr.rel_path),
        #     buff_dist,
        #     network_path
        # )
        # log.info('Completed thresholding at {}'.format(thr_val))

        remove_holes_feature_class(os.path.join(intermediates_gpkg_path, plgnize_lyr.rel_path), os.path.join(vbet_path, vbet_lyr.rel_path), min_geom_size, min_geom_size)
        vbet_threshold[str_val] = os.path.join(vbet_path, vbet_lyr.rel_path)

    # Geomorphic Layers
    vbet_channel_area = os.path.join(vbet_path, LayerTypes['VBET_OUTPUTS'].sub_layers['VBET_CHANNEL_AREA'].rel_path)
    with GeopackageLayer(vbet_channel_area, write=True) as output_lyr, \
        GeopackageLayer(vbet_threshold['VBET_FULL']) as clipping_lyr, \
            GeopackageLayer(project_inputs['CHANNEL_AREA_POLYGONS']) as target_layer:  # use catchments? likely has slightly different edges

        output_lyr.create_layer(target_layer.ogr_geom_type, spatial_ref=target_layer.spatial_ref)
        clipping_lyr_defn = clipping_lyr.ogr_layer_def

        for i in range(clipping_lyr_defn.GetFieldCount()):
            field_defn = clipping_lyr_defn.GetFieldDefn(i)
            if field_defn.GetName() == vbet_summary_field:
                output_lyr.ogr_layer.CreateField(field_defn)
        output_lyr.ogr_layer.StartTransaction()
        for clipping_feat, *_ in clipping_lyr.iterate_features('Finding features'):
            clipping_geom = clipping_feat.GetGeometryRef().Clone()
            clip_value = clipping_feat.GetField(vbet_summary_field)

            for target_feat, *_ in target_layer.iterate_features(clip_shape=clipping_geom):
                target_geom = target_feat.GetGeometryRef().Clone()
                if not target_geom.IsValid():
                    target_geom = geom_validity_fix(target_geom)
                out_geom = clipping_geom.Intersection(target_geom)
                if not out_geom.IsValid():
                    out_geom = geom_validity_fix(out_geom)
                out_feat = ogr.Feature(output_lyr.ogr_layer_def)
                out_feat.SetGeometry(out_geom)
                out_feat.SetField(vbet_summary_field, clip_value)
                output_lyr.ogr_layer.CreateFeature(out_feat)
        output_lyr.ogr_layer.CommitTransaction()

    active_floodplain = os.path.join(vbet_path, LayerTypes['VBET_OUTPUTS'].sub_layers['ACTIVE_FLOODPLAIN'].rel_path)
    difference(vbet_channel_area, vbet_threshold['VBET_IA'], active_floodplain, epsg)

    inactive_floodplain = os.path.join(vbet_path, LayerTypes['VBET_OUTPUTS'].sub_layers['INACTIVE_FLOODPLAIN'].rel_path)
    difference(vbet_threshold['VBET_IA'], vbet_threshold['VBET_FULL'], inactive_floodplain, epsg)

    # Area Calculations
    for layer in [active_floodplain, vbet_channel_area, inactive_floodplain, vbet_threshold['VBET_IA'], vbet_threshold['VBET_FULL']]:
        log.info(f'Calcuating area for {layer}')
        calculate_area(layer, "area_ha")

    # TODO is this redundant now that we clip channel area to vbet? can we join those features like we join the floodplain layers?
    with GeopackageLayer(vbet_threshold['VBET_FULL'], write=True) as lyr_vbet, \
            GeopackageLayer(vbet_channel_area) as lyr_channel:

        if lyr_vbet.ogr_layer.GetLayerDefn().GetFieldIndex('active_channel_ha') < 0:
            lyr_vbet.create_field('active_channel_ha', ogr.OFTReal)
        srs = lyr_vbet.get_srs_from_epsg(cfg.OUTPUT_EPSG)
        _sr, transform = lyr_vbet.get_transform_from_epsg(srs, 5070)
        lyr_vbet.ogr_layer.StartTransaction()
        for feat, *_ in lyr_vbet.iterate_features("Intersecting vbet features with channel area"):
            vbet_geom = feat.GetGeometryRef().Clone()
            sum_area = 0.0
            for channel_feat, *_ in lyr_channel.iterate_features(clip_shape=vbet_geom):
                channel_geom = channel_feat.GetGeometryRef().Clone()
                clip_geom = vbet_geom.Intersection(channel_geom)
                clip_geom.Transform(transform)
                area = clip_geom.GetArea()
                hectares = area / 10000.0
                sum_area = sum_area + hectares
            feat.SetField('active_channel_ha', hectares)
            lyr_vbet.ogr_layer.SetFeature(feat)
        lyr_vbet.ogr_layer.CommitTransaction()
    # End of redundant section...

    build_vbet_metric_tables(vbet_path)

    # Generate Centerline
    # centerline_lyr = RSLayer('VBET Centerlines', 'VBET_CENTERLINES', 'Vector', 'vbet_centerlines')
    # log.info('Creating a centerlines')
    # LayerTypes['VBET_OUTPUTS'].add_sub_layer('VBET_CENTERLINES', centerline_lyr)
    # centerline = os.path.join(vbet_path, centerline_lyr.rel_path)
    # vbet_centerline(network_path, os.path.join(vbet_path, 'vbet_68'), centerline)

    # Now add our Geopackages to the project XML
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


def simple_save(list_geoms, ogr_type, srs, layer_name, gpkg_path, attributes={}):

    with GeopackageLayer(gpkg_path, layer_name, write=True) as lyr:
        lyr.create_layer(ogr_type, spatial_ref=srs)
        for fname, ftype in attributes.items():
            lyr.create_field(fname, ftype)

        progbar = ProgressBar(len(list_geoms), 50, f"Saving {gpkg_path}/{layer_name}")
        counter = 0
        progbar.update(counter)

        lyr.ogr_layer.StartTransaction()
        for feat in list_geoms:
            counter += 1
            progbar.update(counter)

            geom = feat['geom']

            feature = ogr.Feature(lyr.ogr_layer_def)
            geom_ogr = VectorBase.shapely2ogr(geom)
            feature.SetGeometry(geom_ogr)
            if attributes:
                for field, value in feat['attributes'].items():
                    feature.SetField(field, value)
            lyr.ogr_layer.CreateFeature(feature)
            feature = None

        progbar.finish()
        lyr.ogr_layer.CommitTransaction()


def calculate_area(layer: Path, field_name: str, transform_epsg: int = 5070):
    """Calcuate and store area (as hectares) for all features.

    Args:
        layer (Path): path of layer to add area calculations
        field_name (str): output field name. will overwrite data if field exists
        transform_epsg (int, optional): epsg id of spatial reference (must be in meters!). Defaults to 5070 (Albers North America).
    """

    with GeopackageLayer(layer, write=True) as lyr:
        if lyr.ogr_layer.GetLayerDefn().GetFieldIndex(field_name) < 0:
            lyr.create_field(field_name, ogr.OFTReal)
        srs = lyr.get_srs_from_epsg(cfg.OUTPUT_EPSG)
        _sr, transform = lyr.get_transform_from_epsg(srs, transform_epsg)
        lyr.ogr_layer.StartTransaction()
        for feat, *_ in lyr.iterate_features(f'Calculating area for features in {layer}'):
            geom = feat.GetGeometryRef().Clone()
            geom.Transform(transform)
            area = geom.GetArea()
            hectares = area / 10000.0
            feat.SetField(field_name, hectares)
            lyr.ogr_layer.SetFeature(feat)
        lyr.ogr_layer.CommitTransaction()


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
    parser.add_argument('inputs', help='key-value pairs of input name and path', type=str)
    parser.add_argument('vaa_table', type=str)
    parser.add_argument('output_dir', help='Folder where output VBET project will be created', type=str)
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
    inputs = parse_metadata(args.inputs)

    reach_codes = args.reach_codes.split(',') if args.reach_codes else None

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_dir, 'vbet_mem.log')
            retcode, max_obj = ThreadRun(vbet, memfile, args.huc, args.scenario_code, inputs, args.vaa_table, args.output_dir, reach_codes, meta)
            log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))

        else:
            vbet(args.huc, args.scenario_code, inputs, args.vaa_table, args.output_dir, reach_codes, meta, flowline_type=args.flowline_type)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
