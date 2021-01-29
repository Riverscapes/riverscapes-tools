# Name:     Valley Bottom
#
# Purpose:  Perform initial VBET analysis that can be used by the BRAT conservation
#           module
#
# Author:   Matt Reimer
#
# Date:     November 20, 2020
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
import time
from tempfile import NamedTemporaryFile
import json
import glob
import sqlite3
from osgeo import gdal
import rasterio
import numpy as np
from scipy import interpolate
from rscommons.util import safe_makedirs, parse_metadata
from rscommons import RSProject, RSLayer, ModelConfig, ProgressBar, Logger, dotenv, initGDALOGRErrors
from rscommons import GeopackageLayer
from rscommons.database import execute_query
from rscommons.vector_ops import polygonize, buffer_by_field, copy_feature_class
from vbet.vbet_network import vbet_network
from vbet.vbet_report import VBETReport
from vbet.vbet_raster_ops import rasterize, proximity_raster, translate, raster_clean
from vbet.vbet_outputs import threshold, sanitize
from vbet.__version__ import __version__

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/VBET.xsd', __version__)

thresh_vals = {"50": 0.5, "60": 0.6, "70": 0.7, "80": 0.8, "90": 0.9, "100": 1}

LayerTypes = {
    'SLOPE_RASTER': RSLayer('Slope Raster', 'SLOPE_RASTER', 'Raster', 'inputs/slope.tif'),
    'HAND_RASTER': RSLayer('Hand Raster', 'HAND_RASTER', 'Raster', 'inputs/hand.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    # 'CHANNEL_RASTER': RSLayer('Channel Raster', 'CHANNEL_RASTER', 'Raster', 'inputs/channel.tif'),
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/vbet_inputs.gpkg', {
        'FLOWLINES': RSLayer('NHD Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'FLOW_AREA': RSLayer('NHD Flow Areas', 'FLOW_AREA', 'Vector', 'flow_areas'),
    }),
    'CHANNEL_BUFFER_RASTER': RSLayer('Channel Buffer Raster', 'CHANNEL_BUFFER_RASTER', 'Raster', 'intermediates/channelbuffer.tif'),
    'FLOW_AREA_RASTER': RSLayer('Flow Area Raster', 'FLOW_AREA_RASTER', 'Raster', 'intermediates/flowarea.tif'),
    'TEMP_SLOPE': RSLayer('Evidence Raster', 'TEMP_SLOPE', 'Raster', 'intermediates/TempSlope.tif'),
    'TEMP_HAND': RSLayer('Evidence Raster', 'TEMP_HAND', 'Raster', 'intermediates/TempHAND.tif'),
    # 'CHANNEL_MASK': RSLayer('Evidence Raster', 'CH_MASK', 'Raster', 'intermediates/nLOE_Channels.tif'),
    'CHANNEL_DISTANCE': RSLayer('Channel Euclidean Distance', 'CHANNEL_DISTANCE', "Raster", "intermediates/ChannelEuclideanDist.tif"),
    'FLOW_AREA_DISTANCE': RSLayer('Flow Area Euclidean Distance', 'FLOW_AREA_DISTANCE', "Raster", "intermediates/FlowAreaEuclideanDist.tif"),
    'NORMALIZED_SLOPE': RSLayer('Normalized Slope', 'NORMALIZED_SLOPE', "Raster", "intermediates/nLoE_Slope.tif"),
    'NORMALIZED_HAND': RSLayer('Normalized HAND', 'NORMALIZED_HAND', "Raster", "intermediates/nLoE_Hand.tif"),
    'NORMALIZED_CHANNEL_DISTANCE': RSLayer('Normalized Channel Distance', 'NORMALIZED_CHANNEL_DISTANCE', "Raster", "intermediates/nLoE_ChannelDist.tif"),
    'NORMALIZED_FLOWAREA_DISTANCE': RSLayer('Normalized Flow Area Distance', 'NORMALIZED_FLOWAREA_DISTANCE', "Raster", "intermediates/nLoE_FlowAreaDist.tif"),
    'EVIDENCE_TOPO': RSLayer('Evidence Raster', 'EVIDENCE_TOPO', 'Raster', 'intermediates/Topographic_Evidence.tif'),
    'EVIDENCE_CHANNEL': RSLayer('Evidence Raster', 'EVIDENCE_CHANNEL', 'Raster', 'intermediates/Channel_Evidence.tif'),
    'COMBINED_VRT': RSLayer('Combined VRT', 'COMBINED_VRT', 'VRT', 'intermediates/slope-hand-channel.vrt'),
    'INTERMEDIATES': RSLayer('Intermediates', 'Intermediates', 'Geopackage', 'intermediates/vbet_intermediates.gpkg', {
        'VBET_NETWORK': RSLayer('VBET Network', 'VBET_NETWORK', 'Vector', 'vbet_network'),
        'VBET_NETWORK_BUFFERED': RSLayer('VBET Network', 'VBET_NETWORK', 'Vector', 'vbet_network_buffered'),
        # 'CHANNEL_POLYGON': RSLayer('Combined VRT', 'CHANNEL_POLYGON', 'Vector', 'channel')
        # We also add all tht raw thresholded shapes here but they get added dynamically later
    }),
    # Same here. Sub layers are added dynamically later.
    'VBET_EVIDENCE': RSLayer('VBET Evidence Raster', 'VBET_EVIDENCE', 'Raster', 'outputs/VBET_Evidence.tif'),
    'VBET': RSLayer('VBET', 'VBET Outputs', 'Geopackage', 'outputs/vbet.gpkg'),
    'REPORT': RSLayer('RSContext Report', 'REPORT', 'HTMLFile', 'outputs/vbet.html')
}


def vbet(huc, flowlines_orig, flowareas_orig, orig_slope, json_transforms, orig_hand, hillshade, max_hand, min_hole_area_m, project_folder, meta):

    log = Logger('VBET')
    log.info('Starting VBET v.{}'.format(cfg.version))

    project, _realization, proj_nodes = create_project(huc, project_folder)

    # Copy the inp
    _proj_slope_node, proj_slope = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['SLOPE_RASTER'], orig_slope)
    _proj_hand_node, proj_hand = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HAND_RASTER'], orig_hand)
    _hillshade_node, hillshade = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)

    # Copy input shapes to a geopackage
    inputs_gpkg_path = os.path.join(project_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg_path = os.path.join(project_folder, LayerTypes['INTERMEDIATES'].rel_path)

    flowlines_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    flowareas_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['FLOW_AREA'].rel_path)

    # Make sure we're starting with a fresh slate of new geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(intermediates_gpkg_path)

    copy_feature_class(flowlines_orig, flowlines_path, epsg=cfg.OUTPUT_EPSG)
    copy_feature_class(flowareas_orig, flowareas_path, epsg=cfg.OUTPUT_EPSG)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])

    # Create a copy of the flow lines with just the perennial and also connectors inside flow areas
    fcodes = [33400, 46003, 46006, 46007, 55800]  # TODO expose included fcodes as a tool parameter?

    network_path = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['VBET_NETWORK'].rel_path)
    vbet_network(flowlines_path, flowareas_path, network_path, cfg.OUTPUT_EPSG, fcodes)

    # Create a VRT that combines all the evidence rasters
    log.info('Creating combined evidence VRT')
    vrtpath = os.path.join(project_folder, LayerTypes['COMBINED_VRT'].rel_path)
    vrt_options = gdal.BuildVRTOptions(resampleAlg='nearest', separate=True, resolution='average')

    gdal.BuildVRT(vrtpath, [
        proj_slope,
        proj_hand
    ], options=vrt_options)

    slope_ev = os.path.join(project_folder, LayerTypes['TEMP_SLOPE'].rel_path)
    hand_ev = os.path.join(project_folder, LayerTypes['TEMP_HAND'].rel_path)

    # Generate the evidence raster from the VRT. This is a little annoying but reading across
    # different dtypes in one VRT is not supported in GDAL > 3.0 so we dump them into individual rasters
    translate(vrtpath, slope_ev, 1)
    translate(vrtpath, hand_ev, 2)

    # Build Transformation Tables
    with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'vbet_schema.sql')) as sqlfile:
        for sql_command in sqlfile.read().split(';'):
            execute_query(intermediates_gpkg_path, sql_command)

    # Load tables from sql or database
    # TODO obviously we need to read data from external source here...
    for sqldata in glob.glob(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'data', '**', '*.sql'), recursive=True):
        with open(sqldata) as sqlfile:
            for sql_command in sqlfile.read().split(';'):
                execute_query(intermediates_gpkg_path, sql_command, message=f"Excecuting SQL command for {sqldata}")

    # Load transforms from table
    transforms = load_transform_functions(json_transforms, intermediates_gpkg_path)

    # Get raster resolution as min buffer and apply bankfull width buffer to reaches
    with rasterio.open(slope_ev) as raster:
        t = raster.transform
        min_buffer = (t[0] + abs(t[4])) / 2

    log.info("Buffering Polyine by bankfull width buffers")

    network_path_buffered = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['VBET_NETWORK_BUFFERED'].rel_path)
    buffer_by_field(network_path, network_path_buffered, "BFwidth", cfg.OUTPUT_EPSG, min_buffer)

    # Rasterize the channel polygon and write to raster
    log.info('Writing channel raster using slope as a template')
    flow_area_raster = os.path.join(project_folder, LayerTypes['FLOW_AREA_RASTER'].rel_path)
    channel_buffer_raster = os.path.join(project_folder, LayerTypes['CHANNEL_BUFFER_RASTER'].rel_path)
    #channel_raster = os.path.join(project_folder, LayerTypes['CHANNEL_RASTER'].rel_path)

    rasterize(network_path_buffered, channel_buffer_raster, slope_ev)
    project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['CHANNEL_BUFFER_RASTER'])

    rasterize(flowareas_path, flow_area_raster, slope_ev)
    project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['FLOW_AREA_RASTER'])

    # Open evidence rasters concurrently. We're looping over windows so this shouldn't affect
    # memory consumption too much
    # with rasterio.open(channel_buffer_raster) as ch_buff, rasterio.open(flow_area_raster) as fl_arr:
    #     # All 3 rasters should have the same extent and properties. They differ only in dtype
    #     out_meta = ch_buff.meta
    #     out_meta['compress'] = 'deflate'

    #     with rasterio.open(channel_raster, 'w', **out_meta) as out_raster:
    #         progbar = ProgressBar(len(list(ch_buff.block_windows(1))), 50, "Combining flow area and buffered network rasters")
    #         counter = 0
    #         # Again, these rasters should be orthogonal so their windows should also line up
    #         for ji, window in ch_buff.block_windows(1):
    #             progbar.update(counter)
    #             counter += 1
    #             # These rasterizations don't begin life with a mask.
    #             ch_buff_data = ch_buff.read(1, window=window, masked=True)
    #             fl_arr_data = fl_arr.read(1, window=window, masked=True)

    #             out_raster.write(ch_buff_data | fl_arr_data, window=window, indexes=1)

    #         progbar.finish()
    # project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['CHANNEL_RASTER'])

    channel_dist_raster = os.path.join(project_folder, LayerTypes['CHANNEL_DISTANCE'].rel_path)
    fa_dist_raster = os.path.join(project_folder, LayerTypes['FLOW_AREA_DISTANCE'].rel_path)
    proximity_raster(channel_buffer_raster, channel_dist_raster)
    proximity_raster(flow_area_raster, fa_dist_raster)

    slope_transform_raster = os.path.join(project_folder, LayerTypes['NORMALIZED_SLOPE'].rel_path)
    hand_transform_raster = os.path.join(project_folder, LayerTypes['NORMALIZED_HAND'].rel_path)
    chan_dist_transform_raster = os.path.join(project_folder, LayerTypes['NORMALIZED_CHANNEL_DISTANCE'].rel_path)
    fa_dist_transform_raster = os.path.join(project_folder, LayerTypes['NORMALIZED_FLOWAREA_DISTANCE'].rel_path)
    topo_evidence_raster = os.path.join(project_folder, LayerTypes['EVIDENCE_TOPO'].rel_path)
    channel_evidence_raster = os.path.join(project_folder, LayerTypes['EVIDENCE_CHANNEL'].rel_path)
    evidence_raster = os.path.join(project_folder, LayerTypes['VBET_EVIDENCE'].rel_path)

    # Open evidence rasters concurrently. We're looping over windows so this shouldn't affect
    # memory consumption too much
    with rasterio.open(slope_ev) as slp_src, \
            rasterio.open(hand_ev) as hand_src, \
            rasterio.open(channel_dist_raster) as cdist_src, \
            rasterio.open(fa_dist_raster) as fadist_src:
        # All 3 rasters should have the same extent and properties. They differ only in dtype
        out_meta = slp_src.meta
        # Rasterio can't write back to a VRT so rest the driver and number of bands for the output
        out_meta['driver'] = 'GTiff'
        out_meta['count'] = 1
        out_meta['compress'] = 'deflate'
        # out_meta['dtype'] = rasterio.uint8
        # We use this to buffer the output
        cell_size = abs(slp_src.get_transform()[1])

        # Transform Functions
        # f_slope = interpolate.interp1d(tcurve_slope['values'], tcurve_slope['output'], bounds_error=False, fill_value=0.0)
        # f_hand = interpolate.interp1d(tcurve_hand['values'], tcurve_hand['output'], bounds_error=False, fill_value=0.0)
        # f_chan_dist = interpolate.interp1d(tcurve_ch_dist['values'], tcurve_ch_dist['output'], bounds_error=False, fill_value=0.0)
        # f_fa_dist = interpolate.interp1d(tcurve_fa_dist['values'], tcurve_fa_dist['output'], bounds_error=False, fill_value=0.0)

        with rasterio.open(evidence_raster, 'w', **out_meta) as dest_evidence, \
                rasterio.open(topo_evidence_raster, "w", **out_meta) as dest, \
                rasterio.open(channel_evidence_raster, 'w', **out_meta) as dest_channel, \
                rasterio.open(slope_transform_raster, "w", **out_meta) as slope_ev_out, \
                rasterio.open(hand_transform_raster, 'w', **out_meta) as hand_ev_out, \
                rasterio.open(chan_dist_transform_raster, 'w', **out_meta) as chan_dist_ev_out, \
                rasterio.open(fa_dist_transform_raster, 'w', **out_meta) as fa_dist_ev_out:

            progbar = ProgressBar(len(list(slp_src.block_windows(1))), 50, "Calculating evidence layer")
            counter = 0
            # Again, these rasters should be orthogonal so their windows should also line up
            for ji, window in slp_src.block_windows(1):
                progbar.update(counter)
                counter += 1
                slope_data = slp_src.read(1, window=window, masked=True)
                hand_data = hand_src.read(1, window=window, masked=True)
                cdist_data = cdist_src.read(1, window=window, masked=True)
                fadist_data = fadist_src.read(1, window=window, masked=True)

                slope_transform = np.ma.MaskedArray(transforms["Slope"](slope_data.data), mask=slope_data.mask)
                hand_transform = np.ma.MaskedArray(transforms["HAND"](hand_data.data), mask=hand_data.mask)
                channel_dist_transform = np.ma.MaskedArray(transforms["Channel"](cdist_data.data), mask=cdist_data.mask)
                fa_dist_transform = np.ma.MaskedArray(transforms["Flow Areas"](fadist_data.data), mask=fadist_data.mask)

                fvals_topo = slope_transform * hand_transform
                fvals_channel = np.maximum(channel_dist_transform, fa_dist_transform)
                fvals_evidence = np.maximum(fvals_topo, fvals_channel)

                # Fill the masked values with the appropriate nodata vals
                # Unthresholded in the base band (mostly for debugging)
                dest.write(np.ma.filled(np.float32(fvals_topo), out_meta['nodata']), window=window, indexes=1)

                slope_ev_out.write(slope_transform.astype('float32').filled(out_meta['nodata']), window=window, indexes=1)
                hand_ev_out.write(hand_transform.astype('float32').filled(out_meta['nodata']), window=window, indexes=1)
                chan_dist_ev_out.write(channel_dist_transform.astype('float32').filled(out_meta['nodata']), window=window, indexes=1)
                fa_dist_ev_out.write(fa_dist_transform.astype('float32').filled(out_meta['nodata']), window=window, indexes=1)

                dest_channel.write(np.ma.filled(np.float32(fvals_channel), out_meta['nodata']), window=window, indexes=1)
                dest_evidence.write(np.ma.filled(np.float32(fvals_evidence), out_meta['nodata']), window=window, indexes=1)
            progbar.finish()

        # The remaining rasters get added to the project
        project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['EVIDENCE_TOPO'])
        project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['EVIDENCE_CHANNEL'])

    # Get the length of a meter (roughly)
    degree_factor = GeopackageLayer.rough_convert_metres_to_raster_units(slope_ev, 1)
    buff_dist = cell_size
    min_hole_degrees = min_hole_area_m * (degree_factor ** 2)

    # Get the full paths to the geopackages
    intermed_gpkg_path = os.path.join(project_folder, LayerTypes['INTERMEDIATES'].rel_path)
    vbet_path = os.path.join(project_folder, LayerTypes['VBET'].rel_path)

    for str_val, thr_val in thresh_vals.items():
        with NamedTemporaryFile(suffix='.tif', mode="w+", delete=False) as tmp_raw_thresh, \
                NamedTemporaryFile(suffix='.tif', mode="w+", delete=False) as tmp_cleaned_thresh:

            log.debug('Temporary threshold raster: {}'.format(tmp_raw_thresh.name))
            threshold(evidence_raster, thr_val, tmp_raw_thresh.name)

            raster_clean(tmp_raw_thresh.name, tmp_cleaned_thresh.name, buffer_pixels=1)

            plgnize_id = 'THRESH_{}'.format(str_val)
            plgnize_lyr = RSLayer('Raw Threshold at {}%'.format(str_val), plgnize_id, 'Vector', plgnize_id.lower())
            # Add a project node for this thresholded vector
            LayerTypes['INTERMEDIATES'].add_sub_layer(plgnize_id, plgnize_lyr)

            vbet_id = 'VBET_{}'.format(str_val)
            vbet_lyr = RSLayer('Threshold at {}%'.format(str_val), vbet_id, 'Vector', vbet_id.lower())
            # Add a project node for this thresholded vector
            LayerTypes['VBET'].add_sub_layer(vbet_id, vbet_lyr)
            # Now polygonize the raster
            log.info('Polygonizing')
            polygonize(tmp_cleaned_thresh.name, 1, '{}/{}'.format(intermed_gpkg_path, plgnize_lyr.rel_path), cfg.OUTPUT_EPSG)
            log.info('Done')

            for f in [tmp_raw_thresh, tmp_cleaned_thresh]:
                f.close()
                os.unlink(f.name)

        # Now the final sanitization
        log.info('Sanitizing')
        sanitize(
            '{}/{}'.format(intermed_gpkg_path, plgnize_lyr.rel_path),
            '{}/{}'.format(vbet_path, vbet_lyr.rel_path),
            min_hole_degrees,
            buff_dist
        )
        log.info('Completed thresholding at {}'.format(thr_val))

    # Now add our Geopackages to the project XML
    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['VBET'])

    report_path = os.path.join(project.project_dir, LayerTypes['REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)

    report = VBETReport(report_path, project, project_folder)
    report.write()

    # Incorporate project metadata to the riverscapes project
    project.add_metadata(meta)

    log.info('VBET Completed Successfully')


def load_transform_functions(json_transforms, database):

    conn = sqlite3.connect(database)
    conn.execute('pragma foreign_keys=ON')
    curs = conn.cursor()

    transform_functions = {}

    # TODO how to handle missing transforms? use defaults?

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
    parser.add_argument('flowlines', help='NHD flow line ShapeFile path', type=str)
    parser.add_argument('flowareas', help='NHD flow areas ShapeFile path', type=str)
    parser.add_argument('slope', help='Slope raster path', type=str)
    parser.add_argument('hand', help='HAND raster path', type=str)
    parser.add_argument('hillshade', help='Hillshade raster path', type=str)
    parser.add_argument('output_dir', help='Folder where output VBET project will be created', type=str)
    parser.add_argument('--max_slope', help='Maximum slope to be considered', type=float, default=12)
    parser.add_argument('--max_hand', help='Maximum HAND to be considered', type=float, default=50)
    parser.add_argument('--min_hole_area', help='Minimum hole retained in valley bottom (sq m)', type=float, default=50000)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)

    args = dotenv.parse_args_env(parser)

    # make sure the output folder exists
    safe_makedirs(args.output_dir)

    # Initiate the log file
    log = Logger('VBET')
    log.setup(logPath=os.path.join(args.output_dir, 'vbet.log'), verbose=args.verbose)
    log.title('Riverscapes VBET For HUC: {}'.format(args.huc))

    meta = parse_metadata(args.meta)

    json_transform = json.dumps({"Slope": 1, "HAND": 2, "Channel": 3, "Flow Areas": 4})

    try:
        vbet(args.huc, args.flowlines, args.flowareas, args.slope, json_transform, args.hand, args.hillshade, args.max_hand, args.min_hole_area, args.output_dir, meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
