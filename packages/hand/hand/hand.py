# Name:     Height Above Nearest Drainage (HAND)
#
# Purpose:  Perform HAND
#
# Author:   Matt Reimer
#
# Date:     May 5, 2021
#
# -------------------------------------------------------------------------------
import argparse
import os
from subprocess import run
import sys
import uuid
import traceback
import datetime
import time
from typing import List, Dict

# LEave OSGEO import alone. It is necessary even if it looks unused
from osgeo import gdal

from rscommons.util import safe_makedirs, parse_metadata, safe_remove_dir
from rscommons import RSProject, RSLayer, ModelConfig, Logger, dotenv, initGDALOGRErrors
from rscommons import GeopackageLayer
from rscommons.vector_ops import buffer_by_field, copy_feature_class, merge_feature_classes
from rscommons.hand import create_hand_raster, hand_rasterize, run_subprocess
from rscommons.raster_warp import raster_warp
from rscommons.vbet_network import vbet_network

from hand.hand_report import HANDReport
from hand.__version__ import __version__

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/HAND.xsd', __version__)

LayerTypes = {
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'inputs/dem.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/hand_inputs.gpkg', {
        'FLOWLINES': RSLayer('NHD Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'DEM_MASK_POLY': RSLayer('DEM Mask Polygon', 'DEM_MASK_POLY', 'Vector', 'dem_mask_poly'),
        'FLOW_AREA': RSLayer('NHD Flow Areas', 'FLOW_AREA', 'Vector', 'flow_areas'),
        'HAND_NETWORK': RSLayer('HAND Network', 'HAND_NETWORK', 'Vector', 'hand_network'),
    }),

    # Intermediate Products
    'DEM_MASKED': RSLayer('DEM Masked', 'DEM_MASKED', 'Raster', 'intermediates/dem_masked.tif'),
    'PITFILL': RSLayer('TauDEM Pitfill', 'PITFILL', 'Raster', 'intermediates/hand_processing/pitfill.tif'),
    'DINFFLOWDIR_SLP': RSLayer('TauDEM D-Inf Flow Directions Slope', 'DINFFLOWDIR_SLP', 'Raster', 'intermediates/hand_processing/dinfflowdir_slp.tif'),
    'DINFFLOWDIR_ANG': RSLayer('TauDEM D-Inf Flow Directions', 'DINFFLOWDIR_ANG', 'Raster', 'intermediates/hand_processing/dinfflowdir_ang.tif'),
    'RASTERIZED_FLOWLINES': RSLayer('Rasterized Flowlines', 'RASTERIZED_FLOWLINES', 'Raster', 'intermediates/rasterized_flowline.tif'),
    'RASTERIZED_FLOWAREAS': RSLayer('Rasterized Flowareas', 'RASTERIZED_FLOWAREAS', 'Raster', 'intermediates/rasterized_flowareas.tif'),
    'RASTERIZED_DRAINAGE': RSLayer('Rasterized Drainage', 'RASTERIZED_DRAINAGE', 'Raster', 'intermediates/rasterized_drainage.tif'),

    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEIDATES', 'Geopackage', 'intermediates/hand_intermediates.gpkg', {
        'BUFFERED_FLOWLINES': RSLayer('Buffered Flowlines', 'BUFFERED_FLOWLINES', 'Vector', 'buffered_flowlines'),
        'DRAINAGE_POLYGONS': RSLayer('Drainage Polygons', 'DRAINAGE_POLYGONS', 'Vector', 'drainage_polygons'),
    }),

    # Outputs:
    'HAND_RASTER': RSLayer('Hand Raster', 'HAND_RASTER', 'Raster', 'outputs/HAND.tif'),
    'REPORT': RSLayer('RSContext Report', 'REPORT', 'HTMLFile', 'outputs/hand.html')
}


def hand(huc, flowlines_orig, orig_dem, hillshade, project_folder, flowareas_orig=None, mask_lyr_path: str = None, keep_intermediates: bool = True, reach_codes: List[str] = None, meta: Dict[str, str] = None, buffer_field: str = None):
    """[summary]

    Args:
        huc ([str]): [description]
        flowlines_orig ([str]): [description]
        flowareas_orig ([str]): [description]
        orig_dem ([str]): [description]
        hillshade ([str]): [description]
        project_folder ([str]): [description]
        mask_lyr_path ([str]): [description]
        reach_codes (List[int]): NHD reach codes for features to include in outputs
        meta (Dict[str,str]): dictionary of riverscapes metadata key: value pairs
    """
    log = Logger('HAND')
    log.info('Starting HAND v.{}'.format(cfg.version))

    project, _realization, proj_nodes = create_project(huc, project_folder)

    # Incorporate project metadata to the riverscapes project
    if meta is not None:
        project.add_metadata(meta)

    # Copy the inp
    _proj_dem_node, proj_dem = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['DEM'], orig_dem)
    if hillshade:
        _hillshade_node, hillshade = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)

    # Copy input shapes to a geopackage
    inputs_gpkg_path = os.path.join(project_folder, LayerTypes['INPUTS'].rel_path)
    intermeidates_gpkg_path = os.path.join(project_folder, LayerTypes['INTERMEDIATES'].rel_path)

    flowlines_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    flowareas_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['FLOW_AREA'].rel_path) if flowareas_orig else None
    dem_mask_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['DEM_MASK_POLY'].rel_path) if mask_lyr_path else None

    # Make sure we're starting with a fresh slate of new geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(intermeidates_gpkg_path)

    copy_feature_class(flowlines_orig, flowlines_path, epsg=cfg.OUTPUT_EPSG)

    if mask_lyr_path is not None:
        copy_feature_class(mask_lyr_path, dem_mask_path, epsg=cfg.OUTPUT_EPSG)

    if flowareas_orig is not None:
        copy_feature_class(flowareas_orig, flowareas_path, epsg=cfg.OUTPUT_EPSG)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])

    # Create a copy of the flow lines with just the perennial and also connectors inside flow areas
    network_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['HAND_NETWORK'].rel_path)
    if reach_codes is not None:
        vbet_network(flowlines_path, flowareas_path, network_path, cfg.OUTPUT_EPSG, reach_codes)
    else:
        copy_feature_class(flowlines_path, network_path, epsg=cfg.OUTPUT_EPSG)
    ##########################################################################
    # The main event:ce
    ##########################################################################

    # Generate HAND from dem and vbet_network
    # TODO make a place for this temporary folder. it can be removed after hand is generated.
    temp_hand_dir = os.path.join(project_folder, "intermediates", "hand_processing")
    safe_makedirs(temp_hand_dir)

    hand_raster = os.path.join(project_folder, LayerTypes['HAND_RASTER'].rel_path)

    # If there's no mask we use the original DEM as-is
    hand_dem = proj_dem

    # We might need to mask the incoming DEM
    if mask_lyr_path is not None:
        new_proj_dem = os.path.join(project_folder, LayerTypes['DEM_MASKED'].rel_path)
        raster_warp(proj_dem, new_proj_dem, epsg=cfg.OUTPUT_EPSG, clip=dem_mask_path, raster_compression=" -co COMPRESS=LZW -co PREDICTOR=3")
        hand_dem = new_proj_dem

    if buffer_field is not None:
        buffered_flowlines = os.path.join(intermeidates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['BUFFERED_FLOWLINES'].rel_path)
        buffer_by_field(network_path, buffered_flowlines, buffer_field, epsg=cfg.OUTPUT_EPSG, centered=True)
    else:
        buffered_flowlines = network_path

    path_rasterized_flowline = os.path.join(project_folder, LayerTypes['RASTERIZED_FLOWLINES'].rel_path)
    hand_rasterize(buffered_flowlines, hand_dem, path_rasterized_flowline)

    if flowareas_path:
        path_rasterized_flowarea = os.path.join(project_folder, LayerTypes['RASTERIZED_FLOWAREAS'].rel_path)
        hand_rasterize(flowareas_path, hand_dem, path_rasterized_flowarea)

        drainage_raster = os.path.join(project_folder, LayerTypes['RASTERIZED_DRAINAGE'].rel_path)
        scrips_dir = next(p for p in sys.path if p.endswith('.venv'))
        script = os.path.join(scrips_dir, 'Scripts', 'gdal_calc.py')
        run_subprocess(os.path.join(project_folder, "Intermediates"), ['python', script, '-A', path_rasterized_flowline, '-B', path_rasterized_flowarea, f'--outfile={drainage_raster}', '--calc=(A+B)>0', '--co=COMPRESS=LZW'])
    else:
        drainage_raster = path_rasterized_flowline

    create_hand_raster(hand_dem, drainage_raster, temp_hand_dir, hand_raster)

    if keep_intermediates is True:
        project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['DEM_MASKED'])
        project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['PITFILL'])
        project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['DINFFLOWDIR_SLP'])
        project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['DINFFLOWDIR_ANG'])
        project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['RASTERIZED_FLOWLINES'])
        project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    else:
        safe_remove_dir(temp_hand_dir)

    project.add_project_raster(proj_nodes['Outputs'], LayerTypes['HAND_RASTER'])

    report_path = os.path.join(project.project_dir, LayerTypes['REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)

    report = HANDReport(report_path, project)
    report.write()

    log.info('HAND Completed Successfully')


def create_project(huc, output_dir):
    project_name = 'HAND for HUC {}'.format(huc)
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'HAND')

    project.add_metadata({
        'HUC{}'.format(len(huc)): str(huc),
        'HUC': str(huc),
        'HANDVersion': cfg.version,
        'HANDTimestamp': str(int(time.time()))
    })

    realizations = project.XMLBuilder.add_sub_element(project.XMLBuilder.root, 'Realizations')
    realization = project.XMLBuilder.add_sub_element(realizations, 'HAND', None, {
        'id': 'HAND',
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
        description='Riverscapes HAND Tool',
        # epilog="This is an epilog"
    )
    parser.add_argument('huc', help='NHD flow line ShapeFile path', type=str)
    parser.add_argument('flowlines', help='NHD flow line ShapeFile path', type=str)
    parser.add_argument('dem', help='DEM raster path', type=str)
    parser.add_argument('output_dir', help='Folder where output HAND project will be created', type=str)

    parser.add_argument('--hillshade', help='Hillshade raster path', type=str)
    parser.add_argument('--mask', help='Optional shapefile to mask by', type=str, default=None)
    parser.add_argument('--flowareas', help='NHD flow areas ShapeFile path', type=str, default=None)
    parser.add_argument('--reach_codes', help='Comma delimited reach codes (FCode) to retain when filtering features. Omitting this option retains all features.', type=str)
    parser.add_argument('--buffer_field', help='buffer field.', type=str)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--intermediates', help='(optional) keep the intermediate products', action='store_true', default=False)
    parser.add_argument('--debug', help='Add debug tools for tracing things like memory usage at a performance cost.', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # make sure the output folder exists
    safe_makedirs(args.output_dir)

    # Initiate the log file
    log = Logger('HAND')
    log.setup(logPath=os.path.join(args.output_dir, 'hand.log'), verbose=args.verbose)
    log.title('Riverscapes HAND For HUC: {}'.format(args.huc))

    meta = parse_metadata(args.meta)

    reach_codes = args.reach_codes.split(',') if args.reach_codes else None

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_dir, 'hand_mem.log')
            retcode, max_obj = ThreadRun(hand, memfile, args.huc, args.flowlines, args.dem, args.hillshade, args.output_dir, args.flowareas, args.mask, args.intermediates, reach_codes, meta)
            log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))

        else:
            hand(args.huc, args.flowlines, args.dem, args.hillshade, args.output_dir, args.flowareas, args.mask, args.intermediates, reach_codes, meta, buffer_field=args.buffer_field)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
