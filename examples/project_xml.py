from typing import Dict
import sys
import os
import traceback
import uuid
import datetime
import uuid
import argparse
from osgeo import ogr

from rscommons import RSProject, RSLayer, ModelConfig, RSMeta, RSMetaTypes
from rscommons.__version__ import __version__
from rscommons.util import safe_makedirs
from rsxml import Logger, dotenv
# Import your own version. Don't just use RSCommons
"""
Instructuions:
    1. create a file called .env in the root and add the following line (without the quotes):
                "PROJECTFILE=C:/path/to/my/folder/that/will/have/project.rs.xml"
    2. Run `BuildXML` from VSCode's Run dropdown menu
"""

# Set up a class to track thigns like default EPSG, xsd validation and tool version
cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V1/Inundation.xsd', __version__)

# Define the types of layers we're going to use up top so we can re-use them later
# We use our handy RSLayer() class here.
LayerTypes = {
    # RSLayer(name, id, tag, rel_path)
    'AP_01': RSLayer('2019 August', 'APORTHO_01', 'Raster', '01_Inputs/01_Imagery/AP_01/orthomosaic.tif'),
    'APSTR_01': RSLayer('2019 August Flowlines', 'APSTR_01', 'Vector', '01_Inputs/01_Imagery/AP_01/flowlines.shp'),
}


def build_xml(projectpath):
    """Here's an example of how to build a project.rs.xml file

    Args:
        projectpath ([type]): [description]
    """
    # Create the top-level nodes
    log = Logger('build_xml')
    log.info('Starting the build of the XML')
    project_name = 'Inundation Mapper'
    project = RSProject(cfg, projectpath)
    project.create(project_name, 'Inundation')

    # Add the root metadata
    project.add_metadata([
        RSMeta('ModelVersion', cfg.version),
        RSMeta('dateCreated', datetime.datetime.now().isoformat(), RSMetaTypes.ISODATE),
        RSMeta('HUC8', '16010201'),
        RSMeta('InundationVersion', cfg.version),
        RSMeta('watershed', 'Upper Bear'),
        RSMeta('site_name', 'Mill Creek'),
    ])

    # Example InundationContext Realization
    # ================================================================================================
    r1_node = project.add_realization(project_name, 'INN_CTX01', cfg.version)

    # Realization <MetaData>
    project.add_metadata([
        RSMeta('mapper', 'Karen Bartelt'),
        RSMeta('date_mapped', '02042020'),
        RSMeta('year1', 'estimated pre beaver'),
        RSMeta('year2', '2019'),
        RSMeta('RS_used', 'RS_01')
    ], r1_node)

    # Add an <Input> and <Output> nodes
    r1_inputs = project.XMLBuilder.add_sub_element(r1_node, 'Inputs')
    r1_outputs = project.XMLBuilder.add_sub_element(r1_node, 'Outputs')

    # Now we can add inputs to the context raster
    # Note the return is an HTML node and a raster path we can use for other things
    raster_node, raster_path = project.add_project_raster(r1_inputs, LayerTypes['AP_01'], replace=False)

    # Here we add a vector node
    vector_node, vector_path = project.add_project_vector(r1_inputs, LayerTypes['APSTR_01'], replace=False)

    # Example DCE Realization
    # ================================================================================================
    r2_node = project.XMLBuilder.add_sub_element(project.realizations_node, 'InundationDCE', None, {
        'id': 'DCE_01',
        'dateCreated': datetime.datetime.now().isoformat(),
        'guid': str(uuid.uuid1()),
        'productVersion': cfg.version
    })

    r2_name_node = project.XMLBuilder.add_sub_element(r2_node, 'Name', 'August 2019')
    # Add an <Input> and <Output> nodes
    r2_inputs = project.XMLBuilder.add_sub_element(r2_node, 'Inputs')
    r2_outputs = project.XMLBuilder.add_sub_element(r2_node, 'Outputs')

    # Example CD Realization
    # ================================================================================================
    r3_node = project.XMLBuilder.add_sub_element(project.realizations_node, 'InundationCD', None, {
        'id': '',
        'dateCreated': datetime.datetime.now().isoformat(),
        'guid': str(uuid.uuid1()),
        'productVersion': cfg.version
    })
    r3_name_node = project.XMLBuilder.add_sub_element(r3_node, 'Name', '2019 vs estimated pre beaver')
    # Add an <Input> and <Output> nodes
    r3_inputs = project.XMLBuilder.add_sub_element(r3_node, 'Inputs')
    r3_outputs = project.XMLBuilder.add_sub_element(r3_node, 'Outputs')

    # Finally write the file
    log.info('Writing file')
    project.XMLBuilder.write()
    log.info('Done')


def edit_xml(projectpath):
    """Here's an example of how to edit a pre-existing project.rs.xml file

    Args:
        projectpath ([type]): [description]
    """
    log = Logger('edit_xml')
    log.info('Loading the XML to make edits...')
    # Load up a new RSProject class
    project = RSProject(cfg, projectpath)

    # Now, instead of creating nodes we can just find them
    r1_node = project.XMLBuilder.find_by_id('INN_CTX01')

    # Now we can add new metadata values to this node
    # Note that we specify r1_node. If you don't do this then it writes to the project metadata
    project.add_metadata({'EditedVal': 'Some Realization Value here'}, r1_node)

    # Same is true for Rasters if we want
    r1_input_raster_node = project.XMLBuilder.find_by_id('AP_01')
    project.add_metadata({'EditedVal Raster': 'Some Raster Value here'}, r1_input_raster_node)

    # Don't forget to write back to the file
    log.info('Writing file')
    project.XMLBuilder.write()
    log.info('Done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('projectpath', help='NHD flow line ShapeFile path', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    if args.projectpath is None or len(args.projectpath) < 10:
        raise Exception('projectpath has invalid value')
    safe_makedirs(args.projectpath)
    # Initiate the log file
    log = Logger('Inundation XML')
    log.setup(log_path=os.path.join(args.projectpath, 'Inundation.log'), verbose=args.verbose)

    try:
        log.info('Starting')
        build_xml(args.projectpath)
        edit_xml(args.projectpath)
        log.info('Exiting')

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
