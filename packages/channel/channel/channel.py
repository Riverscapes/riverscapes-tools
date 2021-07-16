"""[summary]
"""
# Name:     Channel Area Tool
#
# Purpose:  Generate bankfull and merge with flow areas to create channel polygons
#
# Author:   Kelly Whitehead
#
# Date:     July 14, 2021
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
from typing import List, Dict

from osgeo import ogr

from rscommons.util import safe_makedirs, parse_metadata, pretty_duration
from rscommons import RSProject, RSLayer, ModelConfig, Logger, dotenv, initGDALOGRErrors
from rscommons import GeopackageLayer
from rscommons.math import safe_eval
from rscommons.raster_buffer_stats import raster_buffer_stats2
from rscommons.vector_ops import get_geometry_unary_union, buffer_by_field, copy_feature_class, merge_feature_classes, remove_holes_feature_class, difference
from rscommons.vbet_network import vbet_network

from channel.channel_report import ChannelReport
from channel.__version__ import __version__

Path = str

DEFAULT_FUNCTION = "0.177 * (a ** 0.397) * (p ** 0.453)"
DEFAULT_FUNCTION_PARAMS = "a=TotDASqKm"

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/channel_area.xsd', __version__)

LayerTypes = {
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'FLOWLINES': RSLayer('NHD Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'FLOWAREAS': RSLayer('NHD Flow Areas', 'FLOWAREAS', 'Vector', 'flowareas'),
        # 'WATERBODY': RSLayer('NHD Water Body Areas', 'WATER_BODIES', 'Vector', 'waterbody'),
        # 'CATCHMENTS': RSLayer('NHD Catchments', 'CATCHMENTS', 'Vector', 'catchments'),
    }),
    'INTERMEDIATES': RSLayer('Intermediates', 'Intermediates', 'Geopackage', 'intermediates/intermediates.gpkg', {
        'FLOW_AREA_NO_ISLANDS': RSLayer('Flow Areas No Islands', 'FLOW_AREA_NO_ISLANDS', 'Vector', 'flow_area_no_islands'),
        'BANKFULL_NETWORK': RSLayer('Bankfull Network', 'BANKFULL_NETWORK', 'Vector', 'bankfull_network'),
        'BANKFULL_POLYGONS': RSLayer('Bankfull Polygons', 'BANKFULL_POLYGONS', 'Vector', 'bankfull_polygons'),
        'DIFFERENCE_POLYGONS': RSLayer('Difference Polygons', 'DIFFERENCE_POLYGONS', 'Vector', 'difference_polygons'),
        # 'DISSOLVED_POLYGON': RSLayer('Dissolved Polygon', 'DISSOLVED_POLYGON', 'Vector', 'dissolved_polygon')
    }),
    'OUTPUTS': RSLayer('VBET', 'OUTPUTS', 'Geopackage', 'outputs/channel_area.gpkg', {
        'CHANNEL_AREA': RSLayer('Channel Area Polygons', 'CHANNEL_AREA', 'Vector', 'channel_areas'),

    }),
    'REPORT': RSLayer('RSContext Report', 'REPORT', 'HTMLFile', 'outputs/channel_area.html')
}


def channel(huc: int,
            flowlines: Path,
            flowareas: Path,
            bankfull_function: str,
            bankfull_function_params: dict,
            project_folder: Path,
            reach_code_field: str,
            reach_codes: List[str] = None,
            meta: Dict[str, str] = None):
    """[summary]

    Args:
        huc (int): [description]
        flowlines (Path): [description]
        flowareas (Path): [description]
        bankfull_function (str): [description]
        bankfull_function_params (dict): [description]
        project_folder (Path): [description]
        reach_code_field (str): [description]
        reach_codes (List[str], optional): [description]. Defaults to None.
        meta (Dict[str, str], optional): [description]. Defaults to None.
    """

    timer = time.time()
    log = Logger('ChannelAreaTool')
    log.info('Starting Channel Area Tool v.{}'.format(cfg.version))
    log.info('Using Equation: "{}" and params: "{}"'.format(bankfull_function, bankfull_function_params))

    meta['Bankfull Equation'] = bankfull_function
    meta['Reach Codes'] = str(reach_codes)

    project, _realization, proj_nodes = create_project(huc, project_folder, meta)

    # Input Preparation
    # Make sure we're starting with a fresh slate of new geopackages
    inputs_gpkg_path = os.path.join(project_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg_path = os.path.join(project_folder, LayerTypes['INTERMEDIATES'].rel_path)
    output_gpkg_path = os.path.join(project_folder, LayerTypes['OUTPUTS'].rel_path)

    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(intermediates_gpkg_path)

    proj_flowlines = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    copy_feature_class(flowlines, proj_flowlines, epsg=cfg.OUTPUT_EPSG)

    proj_flowareas = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['FLOWAREAS'].rel_path)
    copy_feature_class(flowareas, proj_flowareas, epsg=cfg.OUTPUT_EPSG)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])

    # TODO prepare waterbodies here...
    # if "WATERBODY" in project_inputs:
    #     log.info('Filter and merge waterbody polygons with Flow Areas')
    #     filtered_waterbody = os.path.join(intermediates_gpkg_path, "waterbody_filtered")
    #     wb_fcodes = [39000, 39001, 39004, 39005, 39006, 39009, 39010, 39011, 39012, 36100, 46600, 46601, 46602]
    #     fcode_filter = "FCode = " + " or FCode = ".join([f"'{fcode}'" for fcode in wb_fcodes]) if len(wb_fcodes) > 0 else ""
    #     copy_feature_class(project_inputs["WATERBODY"], filtered_waterbody, attribute_filter=fcode_filter)
    #     merge_feature_classes([filtered_waterbody, project_inputs['FLOW_AREA']], flow_polygons)
    # else:
    #     copy_feature_class(project_inputs['FLOW_AREA'], flow_polygons)

    bankfull_network = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['BANKFULL_NETWORK'].rel_path)
    vbet_network(proj_flowlines, None, bankfull_network, cfg.OUTPUT_EPSG, reach_codes, reach_code_field)

    calculate_bankfull(bankfull_network, 'bankfull_m', bankfull_function, bankfull_function_params)

    bankfull_polygons = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['BANKFULL_POLYGONS'].rel_path)
    buffer_by_field(bankfull_network, bankfull_polygons, "bankfull_m", cfg.OUTPUT_EPSG, centered=True)

    flow_area_no_islands = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['FLOW_AREA_NO_ISLANDS'].rel_path)
    remove_holes_feature_class(proj_flowareas, flow_area_no_islands)

    channel_polygons = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['DIFFERENCE_POLYGONS'].rel_path)
    difference(flow_area_no_islands, bankfull_polygons, channel_polygons, cfg.OUTPUT_EPSG)

    merged_channel_polygons = os.path.join(output_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['CHANNEL_AREA'].rel_path)
    merge_feature_classes([channel_polygons, flow_area_no_islands], merged_channel_polygons)

    # dissolved_channel_polygon = os.path.join(channel_gpkg_path, LayerTypes['CHANNEL_INTERMEDIATES'].sub_layers['DISSOLVED_POLYGON'].rel_path)
    # dissolve_feature_class(merged_channel_polygons, dissolved_channel_polygon, cfg.OUTPUT_EPSG)
    # geom = get_geometry_unary_union(merged_channel_polygons)

    # copy_feature_class(catchments_path, channel_polygons, clip_shape=geom)

    # Now add our Geopackages to the project XML
    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['OUTPUTS'])

    # Processing time in hours
    ellapsed_time = time.time() - timer
    project.add_metadata({"ProcTimeS": "{:.2f}".format(ellapsed_time)})
    project.add_metadata({"ProcTimeHuman": pretty_duration(ellapsed_time)})

    # Report
    report_path = os.path.join(project.project_dir, LayerTypes['REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)
    report = ChannelReport(report_path, project)
    report.write()

    log.info('Channel Area Completed Successfully')


def calculate_bankfull(network_layer: str, out_field: str, eval_fn: str, function_params: dict):
    """[summary]

    Args:
        network_layer (str): [description]
        out_field (str): [description]
        eval_fn (str): [description]
        function_params (dict): [description]

    Raises:
        ne: [description]
    """
    with GeopackageLayer(network_layer, write=True) as layer:

        layer.create_field(out_field, ogr.OFTReal)

        for feat, *_ in layer.iterate_features("Calculating bankfull"):

            fn_params = {}
            for param, value in function_params.items():
                if isinstance(value, str):
                    field_value = feat.GetField(value)
                    fn_params[param] = field_value if field_value is not None else 0
                else:
                    fn_params[param] = value
            # eval seems to mutate the fn_params object so we pass in a copy so that we can report on the errors if needed
            result = safe_eval(eval_fn, fn_params)
            feat.SetField(out_field, result)

            layer.ogr_layer.SetFeature(feat)


def create_project(huc, output_dir, meta=None):
    """[summary]

    Args:
        huc ([type]): [description]
        output_dir ([type]): [description]
        meta ([type], optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """
    project_name = 'Channel Area for HUC {}'.format(huc)
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'ChannelArea')

    project.add_metadata({
        'HUC{}'.format(len(huc)): str(huc),
        'HUC': str(huc),
        'ChannelAreaVersion': cfg.version,
        'ChannelAreaTimestamp': str(int(time.time()))
    })

    # Incorporate project metadata to the riverscapes project
    if meta is not None:
        project.add_metadata(meta)

    realizations = project.XMLBuilder.add_sub_element(project.XMLBuilder.root, 'Realizations')
    realization = project.XMLBuilder.add_sub_element(realizations, 'ChannelArea', None, {
        'id': 'ChannelArea',
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
    """[summary]

    Raises:
        ValueError: [description]
    """
    parser = argparse.ArgumentParser(
        description='Riverscapes Channel Area Tool',
        # epilog="This is an epilog"
    )
    parser.add_argument('huc', help='NHD flow line ShapeFile path', type=str)
    parser.add_argument('flowlines', help='flowlines feature class', type=str)
    parser.add_argument('flowareas', help='flowareas feature class', type=str)
    parser.add_argument('output_dir', help='Folder where output VBET project will be created', type=str)

    parser.add_argument(
        '--bankfull_function',
        help='width field in flowlines feature class (e.g. BFWidth). Default: "{}"'.format(DEFAULT_FUNCTION),
        type=str,
        default=DEFAULT_FUNCTION
    )
    parser.add_argument(
        '--bankfull_function_params',
        help='Field that contains reach code (e.g. FCode). Omitting this option retains all features. Default: "{}"'.format(DEFAULT_FUNCTION_PARAMS),
        type=str,
        default=DEFAULT_FUNCTION_PARAMS
    )

    parser.add_argument('--reach_code_field', help='Field that contains reach code (e.g. FCode). Omitting this option retains all features.', type=str)
    parser.add_argument('--reach_codes', help='Comma delimited reach codes (FCode) to retain when filtering features. Omitting this option retains all features.', type=str)
    parser.add_argument('--precip', help='mean annual precipiation in cm')
    parser.add_argument('--prism_data')
    parser.add_argument('--huc8boundary')
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='Add debug tools for tracing things like memory usage at a performance cost.', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # make sure the output folder exists
    safe_makedirs(args.output_dir)

    # Initiate the log file
    log = Logger('Channel Area')
    log.setup(logPath=os.path.join(args.output_dir, 'channel_area.log'), verbose=args.verbose)
    log.title('Riverscapes Channel Area For HUC: {}'.format(args.huc))

    meta = parse_metadata(args.meta)
    bankfull_params = parse_metadata(args.bankfull_function_params)

    reach_codes = args.reach_codes.split(',') if args.reach_codes else None

    if args.precip is not None:
        precip = args.precip
    elif args.prism_data is not None and args.huc8boundary is not None:
        polygon = get_geometry_unary_union(args.huc8boundary, epsg=cfg.OUTPUT_EPSG)
        precip = raster_buffer_stats2({1: polygon}, args.prism_data)[1]['Mean'] / 10
        log.info('Mean annual precipitation for HUC {} is {} cm'.format(args.huc, precip))

    else:
        raise ValueError('precip or prism_data and huc8boundary not provided.')

    bankfull_params['p'] = precip

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_dir, 'vbet_mem.log')
            retcode, max_obj = ThreadRun(channel, memfile, args.huc, args.flowlines, args.flowareas, args.bankfull_function, bankfull_params, args.output_dir, args.reach_code_field, reach_codes, meta)
            log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))

        else:
            channel(args.huc, args.flowlines, args.flowareas, args.bankfull_function, bankfull_params, args.output_dir, args.reach_code_field, reach_codes, meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
