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
import uuid
import traceback
import datetime
import json
import sqlite3
import time
from typing import List, Dict
# LEave OSGEO import alone. It is necessary even if it looks unused
from osgeo import gdal
import rasterio
import numpy as np
from scipy import interpolate
from rscommons.util import safe_makedirs, parse_metadata
from rscommons import RSProject, RSLayer, ModelConfig, ProgressBar, Logger, dotenv, initGDALOGRErrors, TempRaster
from rscommons import GeopackageLayer
from rscommons.vector_ops import polygonize, buffer_by_field, copy_feature_class
from rscommons.hand import create_hand_raster

from vbet.vbet_database import load_configuration, build_vbet_database
from vbet.vbet_network import vbet_network, create_drainage_area_zones
from vbet.vbet_report import VBETReport
from vbet.vbet_raster_ops import rasterize, proximity_raster, raster_clean, rasterize_attribute
from vbet.vbet_outputs import threshold, sanitize
from vbet.__version__ import __version__

Path = str

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/VBET.xsd', __version__)

thresh_vals = {"50": 0.5, "60": 0.6, "70": 0.7, "80": 0.8, "90": 0.9, "100": 1}

LayerTypes = {
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'inputs/dem.tif'),
    'SLOPE_RASTER': RSLayer('Slope Raster', 'SLOPE_RASTER', 'Raster', 'inputs/slope.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/vbet_inputs.gpkg', {
        'FLOWLINES': RSLayer('NHD Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'FLOW_AREA': RSLayer('NHD Flow Areas', 'FLOW_AREA', 'Vector', 'flow_areas'),
        'CATCHMENTS': RSLayer('NHD Catchments', 'CATCHMENTS', 'Vector', 'catchments')
    }),
    'CHANNEL_BUFFER_RASTER': RSLayer('Channel Buffer Raster', 'CHANNEL_BUFFER_RASTER', 'Raster', 'intermediates/channelbuffer.tif'),
    'FLOW_AREA_RASTER': RSLayer('Flow Area Raster', 'FLOW_AREA_RASTER', 'Raster', 'intermediates/flowarea.tif'),
    'HAND_RASTER': RSLayer('Hand Raster', 'HAND_RASTER', 'Raster', 'intermediates/HAND.tif'),
    'CHANNEL_DISTANCE': RSLayer('Channel Euclidean Distance', 'CHANNEL_DISTANCE', "Raster", "intermediates/ChannelEuclideanDist.tif"),
    'FLOW_AREA_DISTANCE': RSLayer('Flow Area Euclidean Distance', 'FLOW_AREA_DISTANCE', "Raster", "intermediates/FlowAreaEuclideanDist.tif"),
    # DYNAMIC: 'DA_ZONE_<RASTER>': RSLayer('Drainage Area Zone Raster', 'DA_ZONE_RASTER', "Raster", "intermediates/.tif"),
    'NORMALIZED_SLOPE': RSLayer('Normalized Slope', 'NORMALIZED_SLOPE', "Raster", "intermediates/nLoE_Slope.tif"),
    'NORMALIZED_HAND': RSLayer('Normalized HAND', 'NORMALIZED_HAND', "Raster", "intermediates/nLoE_Hand.tif"),
    'NORMALIZED_CHANNEL_DISTANCE': RSLayer('Normalized Channel Distance', 'NORMALIZED_CHANNEL_DISTANCE', "Raster", "intermediates/nLoE_ChannelDist.tif"),
    'NORMALIZED_FLOWAREA_DISTANCE': RSLayer('Normalized Flow Area Distance', 'NORMALIZED_FLOWAREA_DISTANCE', "Raster", "intermediates/nLoE_FlowAreaDist.tif"),
    'EVIDENCE_TOPO': RSLayer('Topo Evidence', 'EVIDENCE_TOPO', 'Raster', 'intermediates/Topographic_Evidence.tif'),
    'EVIDENCE_CHANNEL': RSLayer('Channel Evidence', 'EVIDENCE_CHANNEL', 'Raster', 'intermediates/Channel_Evidence.tif'),
    'INTERMEDIATES': RSLayer('Intermediates', 'Intermediates', 'Geopackage', 'intermediates/vbet_intermediates.gpkg', {
        'VBET_NETWORK': RSLayer('VBET Network', 'VBET_NETWORK', 'Vector', 'vbet_network'),
        'VBET_NETWORK_BUFFERED': RSLayer('VBET Network Buffer', 'VBET_NETWORK_BUFFERED', 'Vector', 'vbet_network_buffered'),
        'DA_ZONES': RSLayer('Drainage Area Zones', 'DA_ZONES', 'Vector', 'da_zones')
        # We also add all tht raw thresholded shapes here but they get added dynamically later
    }),
    # Same here. Sub layers are added dynamically later.
    'VBET_EVIDENCE': RSLayer('VBET Evidence Raster', 'VBET_EVIDENCE', 'Raster', 'outputs/VBET_Evidence.tif'),
    'VBET_OUTPUTS': RSLayer('VBET', 'VBET_OUTPUTS', 'Geopackage', 'outputs/vbet.gpkg'),
    'REPORT': RSLayer('RSContext Report', 'REPORT', 'HTMLFile', 'outputs/vbet.html')
}


def vbet(huc: int, scenario_code: str, inputs: Dict[str, str], project_folder: Path, reach_codes: List[str], meta: Dict[str, str]):
    """generate vbet evidence raster and threshold polygons for a watershed

    Args:
        huc (int): HUC code for watershed
        scenario_code (str): database machine code for scenario to run
        inputs (dict): input names and path
        project_folder (Path): path for project results
        reach_codes (List[int]): NHD reach codes for features to include in outputs
        meta (Dict[str,str]): dictionary of riverscapes metadata key: value pairs
    """

    log = Logger('VBET')
    log.info('Starting VBET v.{}'.format(cfg.version))

    project, _realization, proj_nodes = create_project(huc, project_folder)

    # Incorporate project metadata to the riverscapes project
    if meta is not None:
        project.add_metadata(meta)
    project.add_metadata({"Scenario Name": scenario_code})

    # Input Preparation
    # Make sure we're starting with a fresh slate of new geopackages
    inputs_gpkg_path = os.path.join(project_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg_path = os.path.join(project_folder, LayerTypes['INTERMEDIATES'].rel_path)
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(intermediates_gpkg_path)

    project_inputs = {}
    for input_name, input_path in inputs.items():
        if os.path.splitext(input_path)[1] in ['.tif', '.tiff', '.TIF', '.TIFF']:
            _proj_slope_node, project_inputs[input_name] = project.add_project_raster(proj_nodes['Inputs'], LayerTypes[input_name], input_path)
        else:
            project_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers[input_name].rel_path)
            copy_feature_class(input_path, project_path, epsg=cfg.OUTPUT_EPSG)
            project_inputs[input_name] = project_path
    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])

    # Build Transformation Tables
    build_vbet_database(inputs_gpkg_path)

    # Load configuration from table
    vbet_run = load_configuration(scenario_code, inputs_gpkg_path)

    # Create a copy of the flow lines with just the perennial and also connectors inside flow areas
    log.info('Building vbet network')
    network_path = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['VBET_NETWORK'].rel_path)
    vbet_network(project_inputs['FLOWLINES'], project_inputs['FLOW_AREA'], network_path, cfg.OUTPUT_EPSG, reach_codes)

    # Create Zones
    log.info('Building drainage area zones')
    catchments_path = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['DA_ZONES'].rel_path)
    create_drainage_area_zones(project_inputs['CATCHMENTS'], project_inputs['FLOWLINES'], 'NHDPlusID', 'TotDASqKm', vbet_run['Zones'], catchments_path)

    # Create Scenario Input Rasters
    in_rasters = {}
    out_rasters = {}
    if 'Slope' in vbet_run['Inputs']:
        log.info("Adding Slope Input")
        in_rasters['Slope'] = project_inputs['SLOPE_RASTER']
        out_rasters['NORMALIZED_SLOPE'] = os.path.join(project_folder, LayerTypes['NORMALIZED_SLOPE'].rel_path)

    # Generate HAND from dem and vbet_network
    if 'HAND' in vbet_run['Inputs']:
        log.info("Adding HAND Input")
        temp_hand_dir = os.path.join(project_folder, "intermediates", "hand_processing")
        safe_makedirs(temp_hand_dir)
        hand_raster = os.path.join(project_folder, LayerTypes['HAND_RASTER'].rel_path)
        create_hand_raster(project_inputs['DEM'], network_path, temp_hand_dir, hand_raster)
        project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['HAND_RASTER'])
        in_rasters['HAND'] = hand_raster
        out_rasters['NORMALIZED_HAND'] = os.path.join(project_folder, LayerTypes['NORMALIZED_HAND'].rel_path)

    # Rasterize the channel polygon and write to raster
    if 'Channel' in vbet_run['Inputs']:
        log.info('Adding Channel Distance Input')
        # Get raster resolution as min buffer and apply bankfull width buffer to reaches
        with rasterio.open(project_inputs['SLOPE_RASTER']) as raster:
            t = raster.transform
            min_buffer = (t[0] + abs(t[4])) / 2

        log.info("Buffering Polyine by bankfull width buffers")
        network_path_buffered = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['VBET_NETWORK_BUFFERED'].rel_path)
        buffer_by_field(network_path, network_path_buffered, "BFwidth", cfg.OUTPUT_EPSG, min_buffer)

        log.info('Writing channel raster using slope as a template')
        channel_buffer_raster = os.path.join(project_folder, LayerTypes['CHANNEL_BUFFER_RASTER'].rel_path)
        rasterize(network_path_buffered, channel_buffer_raster, project_inputs['SLOPE_RASTER'])
        project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['CHANNEL_BUFFER_RASTER'])

        log.info('Generating Channel Proximity raster')
        channel_dist_raster = os.path.join(project_folder, LayerTypes['CHANNEL_DISTANCE'].rel_path)
        proximity_raster(channel_buffer_raster, channel_dist_raster)
        project.add_project_raster(proj_nodes["Intermediates"], LayerTypes['CHANNEL_DISTANCE'])

        in_rasters['Channel'] = channel_dist_raster
        out_rasters['NORMALIZED_CHANNEL_DISTANCE'] = os.path.join(project_folder, LayerTypes['NORMALIZED_CHANNEL_DISTANCE'].rel_path)

    if 'Flow Areas' in vbet_run['Inputs']:
        log.info('Adding Flow Area Distance Input')
        flow_area_raster = os.path.join(project_folder, LayerTypes['FLOW_AREA_RASTER'].rel_path)
        rasterize(project_inputs['FLOW_AREA'], flow_area_raster, project_inputs['SLOPE_RASTER'])
        project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['FLOW_AREA_RASTER'])
        fa_dist_raster = os.path.join(project_folder, LayerTypes['FLOW_AREA_DISTANCE'].rel_path)
        proximity_raster(flow_area_raster, fa_dist_raster)
        project.add_project_raster(proj_nodes["Intermediates"], LayerTypes['FLOW_AREA_DISTANCE'])

        in_rasters['Flow Areas'] = fa_dist_raster
        out_rasters['NORMALIZED_FLOWAREA_DISTANCE'] = os.path.join(project_folder, LayerTypes['NORMALIZED_FLOWAREA_DISTANCE'].rel_path)

    for vbet_input in vbet_run['Inputs']:
        if vbet_input not in ['Slope', 'HAND', 'Channel', 'Flow Areas']:
            log.info(f'Adding generic {vbet_input} input')

    # Generate da Zone rasters
    for zone in vbet_run['Zones']:
        log.info(f'Rasterizing drainage area zones for {zone}')
        raster_name = os.path.join(project_folder, 'intermediates', f'da_{zone}_zones.tif')
        rasterize_attribute(catchments_path, raster_name, project_inputs['SLOPE_RASTER'], f'{zone}_Zone')
        in_rasters[f'DA_ZONE_{zone}'] = raster_name
        da_zone_rs = RSLayer(f'Drainage Area Zones for {zone}', f'DA_ZONE_{zone.upper()}', 'Raster', raster_name)
        project.add_project_raster(proj_nodes['Intermediates'], da_zone_rs)

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
                normalized[name] = np.ma.MaskedArray(np.choose(block[f'DA_ZONE_{name}'].data, transforms, mode='clip'), mask=block[name].mask)
            else:
                normalized[name] = np.ma.MaskedArray(vbet_run['Transforms'][name][0](block[name].data), mask=block[name].mask)

        # slope_transform_zones = [np.ma.MaskedArray(transform(block['Slope'].data), mask=block['Slope'].mask) for transform in vbet_run['Transforms']['Slope']]
        # hand_transform_zones = [np.ma.MaskedArray(transform(block['HAND'].data), mask=block['HAND'].mask) for transform in vbet_run['Transforms']['HAND']]
        # slope_transform = np.ma.MaskedArray(np.choose(block['DA_ZONE_Slope'].data, slope_transform_zones, mode='clip'), mask=block['Slope'].mask)
        # hand_transform = np.ma.MaskedArray(np.choose(block['DA_ZONE_HAND'].data, hand_transform_zones, mode='clip'), mask=block['HAND'].mask)

        # channel_dist_transform = np.ma.MaskedArray(vbet_run['Transforms']["Channel"][0](block['Channel'].data), mask=block['Channel'].mask)
        # fa_dist_transform = np.ma.MaskedArray(vbet_run['Transforms']["Flow Areas"][0](block['Flow Areas'].data), mask=block['Flow Areas'].mask)

        fvals_topo = normalized['Slope'] * normalized['HAND']
        fvals_channel = np.maximum(normalized['Channel'], normalized['Flow Areas'])
        fvals_evidence = np.maximum(fvals_topo, fvals_channel)

        # Fill the masked values with the appropriate nodata vals
        # Unthresholded in the base band (mostly for debugging)
        write_rasters['VBET_EVIDENCE'].write(np.ma.filled(np.float32(fvals_evidence), out_meta['nodata']), window=window, indexes=1)

        write_rasters['NORMALIZED_SLOPE'].write(normalized['Slope'].astype('float32').filled(out_meta['nodata']), window=window, indexes=1)
        write_rasters['NORMALIZED_HAND'].write(normalized['HAND'].astype('float32').filled(out_meta['nodata']), window=window, indexes=1)
        write_rasters['NORMALIZED_CHANNEL_DISTANCE'].write(normalized['Channel'].astype('float32').filled(out_meta['nodata']), window=window, indexes=1)
        write_rasters['NORMALIZED_FLOWAREA_DISTANCE'].write(normalized['Flow Areas'].astype('float32').filled(out_meta['nodata']), window=window, indexes=1)

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

    # Get the length of a meter (roughly)
    #degree_factor = GeopackageLayer.rough_convert_metres_to_raster_units(project_inputs['SLOPE_RASTER'], 1)
    buff_dist = cell_size
    # min_hole_degrees = min_hole_area_m * (degree_factor ** 2)

    # Get the full paths to the geopackages
    vbet_path = os.path.join(project_folder, LayerTypes['VBET_OUTPUTS'].rel_path)
    threshold_outputs = True

    if threshold_outputs:
        for str_val, thr_val in thresh_vals.items():  # {"50": 0.5}.items():  #
            plgnize_id = 'THRESH_{}'.format(str_val)
            with TempRaster('vbet_raw_thresh_{}'.format(plgnize_id)) as tmp_raw_thresh, \
                    TempRaster('vbet_cleaned_thresh_{}'.format(plgnize_id)) as tmp_cleaned_thresh:

                log.debug('Temporary threshold raster: {}'.format(tmp_raw_thresh.filepath))
                threshold(evidence_raster, thr_val, tmp_raw_thresh.filepath)

                raster_clean(tmp_raw_thresh.filepath, tmp_cleaned_thresh.filepath, buffer_pixels=1)

                plgnize_lyr = RSLayer('Raw Threshold at {}%'.format(str_val), plgnize_id, 'Vector', plgnize_id.lower())
                # Add a project node for this thresholded vector
                LayerTypes['INTERMEDIATES'].add_sub_layer(plgnize_id, plgnize_lyr)

                vbet_id = 'VBET_{}'.format(str_val)
                vbet_lyr = RSLayer('Threshold at {}%'.format(str_val), vbet_id, 'Vector', vbet_id.lower())
                # Add a project node for this thresholded vector
                LayerTypes['VBET_OUTPUTS'].add_sub_layer(vbet_id, vbet_lyr)
                # Now polygonize the raster
                log.info('Polygonizing')
                polygonize(tmp_cleaned_thresh.filepath, 1, '{}/{}'.format(intermediates_gpkg_path, plgnize_lyr.rel_path), cfg.OUTPUT_EPSG)
                log.info('Done')

            # Now the final sanitization
            sanitize(
                str_val,
                '{}/{}'.format(intermediates_gpkg_path, plgnize_lyr.rel_path),
                '{}/{}'.format(vbet_path, vbet_lyr.rel_path),
                buff_dist,
                network_path
            )
            log.info('Completed thresholding at {}'.format(thr_val))

    # Now add our Geopackages to the project XML
    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    if threshold_outputs:
        project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['VBET_OUTPUTS'])

    report_path = os.path.join(project.project_dir, LayerTypes['REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)

    report = VBETReport(scenario_code, inputs_gpkg_path, vbet_path, report_path, project)
    report.write()

    log.info('VBET Completed Successfully')


def load_transform_functions(json_transforms, database):

    conn = sqlite3.connect(database)
    conn.execute('pragma foreign_keys=ON')
    curs = conn.cursor()

    transform_functions = {}

    for input_name, transform_id in json.loads(json_transforms).items():
        transform_type = curs.execute("""SELECT transform_types.name from transforms INNER JOIN transform_types ON transform_types.type_id = transforms.type_id where transforms.transform_id = ?""", [transform_id]).fetchone()[0]
        values = curs.execute("""SELECT input_value, output_value FROM inflections WHERE transform_id = ? ORDER BY input_value """, [transform_id]).fetchall()

        transform_functions[input_name] = interpolate.interp1d(np.array([v[0] for v in values]), np.array([v[1] for v in values]), kind=transform_type, bounds_error=False, fill_value=0.0)

        if transform_type == "Polynomial":
            # add polynomial function
            transform_functions[input_name] = None

    return transform_functions


def create_project(huc, output_dir):
    project_name = 'VBET for HUC {}'.format(huc)
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'VBET')

    project.add_metadata({
        'HUC{}'.format(len(huc)): str(huc),
        'HUC': str(huc),
        'VBETVersion': cfg.version,
        'VBETTimestamp': str(int(time.time()))
    })

    realizations = project.XMLBuilder.add_sub_element(project.XMLBuilder.root, 'Realizations')
    realization = project.XMLBuilder.add_sub_element(realizations, 'VBET', None, {
        'id': 'VBET',
        'dateCreated': datetime.datetime.now().isoformat(),
        'guid': str(uuid.uuid1()),
        'productVersion': cfg.version
    })

    project.XMLBuilder.add_sub_element(realization, 'Name', project_name)
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
    parser.add_argument('huc', help='NHD flow line ShapeFile path', type=str)
    parser.add_argument('scenario_code', help='machine code for vbet scenario', type=str)
    parser.add_argument('inputs', help='key-value pairs of input name and path', type=str)
    # parser.add_argument('flowlines', help='NHD flow line ShapeFile path', type=str)
    # parser.add_argument('flowareas', help='NHD flow areas ShapeFile path', type=str)
    # parser.add_argument('slope', help='Slope raster path', type=str)
    # parser.add_argument('dem', help='DEM raster path', type=str)
    # parser.add_argument('hillshade', help='Hillshade raster path', type=str)
    # parser.add_argument('catchments', help='NHD Catchment polygons path', type=str)
    parser.add_argument('output_dir', help='Folder where output VBET project will be created', type=str)
    parser.add_argument('--reach_codes', help='Comma delimited reach codes (FCode) to retain when filtering features. Omitting this option retains all features.', type=str)
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

    # json_transform = json.dumps({"Slope": 1, "HAND": 2, "Channel": 3, "Flow Areas": 4, 'Slope MID': 5, "Slope LARGE": 6, "HAND MID": 7, "HAND LARGE": 8})
    reach_codes = args.reach_codes.split(',') if args.reach_codes else None

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_dir, 'vbet_mem.log')
            retcode, max_obj = ThreadRun(vbet, memfile, args.huc, args.flowlines, args.flowareas, args.slope, json_transform, args.dem, args.hillshade, args.catchments, args.output_dir, reach_codes, meta)
            log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))

        else:
            vbet(args.huc, args.scenario_code, inputs, args.output_dir, reach_codes, meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
