#!/usr/bin/env python3
# Name:     RVD
#
# Purpose:  Build a Riparian Vegetation Departure project.
#
# Author:   Philip Bailey
#           Adapted from Jordan Gilbert
#
# Date:     1 Oct 2020
# -------------------------------------------------------------------------------
import argparse
import sys
import os
import glob
import traceback
import uuid
import datetime
import time
from osgeo import ogr
from osgeo import gdal

from rscommons.util import safe_makedirs
from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors
from rscommons.util import safe_makedirs, safe_remove_dir
from rscommons.build_network import build_network
from rscommons.segment_network import segment_network
from rscommons.database import create_database
from rscommons.database import populate_database
from rscommons.reach_attributes import write_attributes, write_reach_attributes

from rscommons.thiessen.vor import NARVoronoi
from rscommons.thiessen.shapes import RiverPoint, get_riverpoints

from rvd.report import report

from rvd.__version__ import __version__

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/RVD.xsd', __version__)

LayerTypes = {
    # key: (name, id, tag, relpath)
    'EXVEG': RSLayer('Existing Vegetation', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HISTVEG': RSLayer('Historic Vegetation', 'HISTVEG', 'Raster', 'inputs/historic_veg.tif'),
    'FLOWLINES': RSLayer('NHD Flowlines', 'FLOWLINES', 'Vector', 'inputs/NHDFlowline.shp'),
    'FLOW_AREA': RSLayer('NHD Flow Area', 'FLOW_AREA', 'Vector', 'inputs/NHDArea.shp'),
    'WATERBODIES': RSLayer('NHD Waterbody', 'WATERBODIES', 'Vector', 'inputs/NHDWaterbody.shp'),
    'VALLEY_BOTTOM': RSLayer('Valley Bottom', 'VALLEY_BOTTOM', 'Vector', 'inputs/valley_bottom.shp'),
    'CLEANED': RSLayer('Cleaned Network', 'CLEANED', 'Vector', 'intermediates/intermediate_nhd_network.shp'),
    'NETWORK': RSLayer('Network', 'NETWORK', 'Vector', 'intermediates/network.shp'),
    'THIESSEN': RSLayer('Network', 'THIESSEN', 'Vector', 'intermediates/thiessen.shp'),
    'SEGMENTED': RSLayer('BRAT Network', 'SEGMENTED', 'Vector', 'outputs/rvd.shp'),
    'SQLITEDB': RSLayer('BRAT Database', 'BRATDB', 'SQLiteDB', 'outputs/rvd.sqlite'),
    'REPORT': RSLayer('RVD Report', 'RVD_REPORT', 'HTMLFile', 'outputs/rvd.html')
}

# Dictionary of fields that this process outputs, keyed by ShapeFile data type
output_fields = {
    ogr.OFTString: ['Risk', 'Limitation', 'Opportunity'],
    ogr.OFTInteger: ['RiskID', 'LimitationID', 'OpportunityID'],
    ogr.OFTReal: ['iVeg100EX', 'iVeg_30EX', 'iVeg100HPE', 'iVeg_30HPE', 'iPC_LU',
                  'iPC_VLowLU', 'iPC_LowLU', 'iPC_ModLU', 'iPC_HighLU', 'iHyd_QLow',
                  'iHyd_Q2', 'iHyd_SPLow', 'iHyd_SP2', 'oVC_HPE', 'oVC_EX', 'oCC_HPE',
                  'mCC_HPE_CT', 'oCC_EX', 'mCC_EX_CT', 'mCC_HisDep']
}

# This dictionary reassigns databae column names to 10 character limit for the ShapeFile
shapefile_field_aliases = {
    'WatershedID': 'HUC'
}

Epochs = [
    # (epoch, prefix, LayerType, OrigId)
    ('Existing', 'EX', 'EXVEG_SUIT', 'EXVEG'),
    ('Historic', 'HPE', 'HISTVEG_SUIT', 'HISTVEG')
]


def rvd(huc, max_length, min_length, flowlines, existing_veg, historic_veg, valley_bottom, output_folder, reach_codes, flow_areas, waterbodies):
    """[summary]

    Args:
        huc ([type]): [description]

    Raises:
        Exception: [description]
        Exception: [description]
        Exception: [description]
        Exception: [description]

    Returns:
        [type]: [description]
    """

    log = Logger("RVD")
    log.info('RVD v.{}'.format(cfg.version))

    try:
        int(huc)
    except ValueError:
        raise Exception('Invalid HUC identifier "{}". Must be an integer'.format(huc))

    if not (len(huc) == 4 or len(huc) == 8):
        raise Exception('Invalid HUC identifier. Must be four digit integer')

    safe_makedirs(output_folder)

    project, _realization, proj_nodes = create_project(huc, output_folder)

    log.info('Adding inputs to project')
    _prj_existing_path_node, prj_existing_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg)
    _prj_historic_path_node, prj_historic_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HISTVEG'], historic_veg)

    # Copy in the vectors we need
    _prj_flowlines_node, prj_flowlines = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['FLOWLINES'], flowlines, att_filter="\"ReachCode\" Like '{}%'".format(huc))
    _prj_flow_areas_node, prj_flow_areas = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['FLOW_AREA'], flow_areas) if flow_areas else None
    _prj_waterbodies_node, prj_waterbodies = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['WATERBODIES'], waterbodies) if waterbodies else None
    _prj_valley_bottom_node, prj_valley_bottom = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['VALLEY_BOTTOM'], valley_bottom) if valley_bottom else None

    # Other layers we need
    _cleaned_path_node, cleaned_path = project.add_project_vector(proj_nodes['Intermediates'], LayerTypes['CLEANED'], replace=True)
    _thiessen_path_node, thiessen_path = project.add_project_vector(proj_nodes['Intermediates'], LayerTypes['THIESSEN'], replace=True)
    _segmented_path_node, segmented_path = project.add_project_vector(proj_nodes['Outputs'], LayerTypes['SEGMENTED'], replace=True)
    _report_path_node, report_path = project.add_project_vector(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)

    # Filter the flow lines to just the required features and then segment to desired length
    build_network(prj_flowlines, prj_flow_areas, prj_waterbodies, cleaned_path, cfg.OUTPUT_EPSG, reach_codes, None)
    segment_network(cleaned_path, segmented_path, max_length, min_length)

    metadata = {
        'RVD_DateTime': datetime.datetime.now().isoformat(),
        'Max_Length': max_length,
        'Min_Length': min_length,
        'Reach_Codes': reach_codes,
    }

    db_path = os.path.join(output_folder, LayerTypes['SQLITEDB'].rel_path)
    watesrhed_name = create_database(huc, db_path, metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'rvd_schema.sql'))
    populate_database(db_path, segmented_path, huc)
    project.add_metadata({'Watershed': watesrhed_name})

    # Add this to the project file
    project.add_dataset(proj_nodes['Outputs'], db_path, LayerTypes['SQLITEDB'], 'SQLiteDB')

    # Generate Voroni polygons
    log.info("Calculating Voronoi Polygons...")

    # Add all the points (including islands) to the list
    # TODO: Not sure this is the right shape to use. I'm plugging it in just
    # to test THiessen
    points = get_riverpoints(prj_valley_bottom, cfg.OUTPUT_EPSG)

    # Exterior is the shell and there is only ever 1
    myVorL = NARVoronoi(points)

    # This is the call that actually makes the polygons
    myVorL.createshapes()

    # TODO: Intersect the valley bottom with the Thiessen polygons.

    # Calculate the proportion of vegetation for each vegetation Epoch
    for epoch, prefix, ltype, orig_id in Epochs:
        log.info('Processing {} epoch'.format(epoch))

        # TODO: summarize the vegetation within each Thiessen polygon

        # TODO: calculate vegetation departure

        # Copy BRAT build output fields from SQLite to ShapeFile in batches according to data type
    log.info('Copying values from SQLite to output ShapeFile')
    write_reach_attributes(segmented_path, db_path, output_fields, shapefile_field_aliases)

    report(db_path, report_path)

    log.info('RVD complete')


def create_project(huc, output_dir):

    project_name = 'RVD for HUC {}'.format(huc)
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'RVD')

    project.add_metadata({
        'HUC{}'.format(len(huc)): str(huc),
        'RVDVersion': cfg.version,
        'RVDTimestamp': str(int(time.time()))
    })

    realizations = project.XMLBuilder.add_sub_element(project.XMLBuilder.root, 'Realizations')
    realization = project.XMLBuilder.add_sub_element(realizations, 'RVD', None, {
        'id': 'RVD1',
        'dateCreated': datetime.datetime.now().isoformat(),
        'guid': str(uuid.uuid1()),
        'productVersion': cfg.version
    })

    proj_nodes = {
        'Name': project.XMLBuilder.add_sub_element(realization, 'Name', project_name),
        'Inputs': project.XMLBuilder.add_sub_element(realization, 'Inputs'),
        'Intermediates': project.XMLBuilder.add_sub_element(realization, 'Intermediates'),
        'Outputs': project.XMLBuilder.add_sub_element(realization, 'Outputs')
    }

    project.XMLBuilder.write()
    return project, realization, proj_nodes


def main():
    parser = argparse.ArgumentParser(
        description='RVD',
        # epilog="This is an epilog"
    )

    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('max_length', help='Maximum length of features when segmenting. Zero causes no segmentation.', type=float)
    parser.add_argument('min_length', help='min_length input', type=float)
    parser.add_argument('flowlines', help='flowlines input', type=str)
    parser.add_argument('existing', help='National existing vegetation raster', type=str)
    parser.add_argument('historic', help='National historic vegetation raster', type=str)
    parser.add_argument('valley_bottom', help='Valley bottom shapeFile', type=str)
    parser.add_argument('output_folder', help='output_folder input', type=str)

    parser.add_argument('--reach_codes', help='Comma delimited reach codes (FCode) to retain when filtering features. Omitting this option retains all features.', type=str)
    parser.add_argument('--flow_areas', help='(optional) path to the flow area polygon feature class containing artificial paths', type=str)
    parser.add_argument('--waterbodies', help='(optional) waterbodies input', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    reach_codes = args.reach_codes.split(',') if args.reach_codes else None

    # Initiate the log file
    log = Logger("RVD")
    log.setup(logPath=os.path.join(args.output_folder, "rvd.log"), verbose=args.verbose)
    log.title('RVD For HUC: {}'.format(args.huc))

    try:
        rvd(args.huc,
            args.max_length, args.min_length, args.flowlines,
            args.existing, args.historic, args.valley_bottom,
            args.output_folder,
            reach_codes,
            args.flow_areas, args.waterbodies)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
