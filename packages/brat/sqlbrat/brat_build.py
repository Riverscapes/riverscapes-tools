# Name:   BRAT Build
#
#         Build a BRAT project by segmenting a river network to a specified
#         length and then extract the input values required to run the
#         BRAT model for each reach segment from various GIS layers.
#
# Author: Philip Bailey
#
# Date:   30 May 2019
# -------------------------------------------------------------------------------
import argparse
import os
import sys
import uuid
import traceback
import datetime
import time
import shutil
from osgeo import ogr
import rasterio.shutil
from rscommons.shapefile import copy_feature_class
from rscommons import Logger, initGDALOGRErrors, RSLayer, RSProject, ModelConfig, dotenv
from rscommons.util import safe_makedirs
from sqlbrat.lib.build_network import build_network
from sqlbrat.lib.database import create_database
from sqlbrat.lib.database import populate_database
from sqlbrat.lib.reach_attributes import write_reach_attributes
from sqlbrat.utils.vegetation_summary import vegetation_summary
from sqlbrat.utils.segment_network import segment_network
from sqlbrat.utils.reach_geometry import reach_geometry
from sqlbrat.utils.conflict_attributes import conflict_attributes
from sqlbrat.__version__ import __version__

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/BRAT.xsd', __version__)

LayerTypes = {
    'DEM': RSLayer('NED 10m DEM', 'DEM', 'DEM', 'inputs/dem.tif'),
    'FA': RSLayer('Flow Accumulation', 'FA', 'Raster', 'inputs/flow_accum.tif'),
    'DA': RSLayer('Drainage Area in sqkm', 'DA', 'Raster', 'inputs/drainarea_sqkm.tif'),
    'SLOPE': RSLayer('Slope Raster', 'SLOPE', 'Raster', 'inputs/slope.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'VALLEY_BOTTOM': RSLayer('Valley Bottom', 'VALLEY_BOTTOM', 'Vector', 'inputs/valley_bottom.shp'),

    'EXVEG': RSLayer('Existing Vegetation', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HISTVEG': RSLayer('Historic Vegetation', 'HISTVEG', 'Raster', 'inputs/historic_veg.tif'),

    'CLEANED': RSLayer('Cleaned Network', 'CLEANED', 'Vector', 'intermediates/intermediate_nhd_network.shp'),
    'NETWORK': RSLayer('Network', 'NETWORK', 'Vector', 'intermediates/network.shp'),

    'FLOWLINES': RSLayer('NHD Flowlines', 'FLOWLINES', 'Vector', 'inputs/NHDFlowline.shp'),
    'FLOW_AREA': RSLayer('NHD Flow Area', 'FLOW_AREA', 'Vector', 'inputs/NHDArea.shp'),
    'WATERBODIES': RSLayer('NHD Waterbody', 'WATERBODIES', 'Vector', 'inputs/NHDWaterbody.shp'),

    'SEGMENTED': RSLayer('BRAT Network', 'SEGMENTED', 'Vector', 'outputs/brat.shp'),
    'BRATDB': RSLayer('BRAT Database', 'BRATDB', 'SQLiteDB', 'outputs/brat.sqlite')
}

# Dictionary of fields that this process outputs, keyed by ShapeFile data type
output_fields = {
    ogr.OFTString: ['Agency', 'WatershedID'],
    ogr.OFTInteger: ['AgencyID', 'ReachCode', 'IsPeren'],
    ogr.OFTReal: [
        'iGeo_Slope', 'iGeo_ElMax', 'iGeo_ElMin', 'iGeo_Len', 'iPC_Road', 'iPC_RoadX',
        'iPC_RoadVB', 'iPC_Rail', 'iPC_RailVB', 'iPC_Canal', 'iPC_DivPts', 'oPC_Dist', 'iPC_Privat',
        'Orig_DA'
    ]
}

# This dictionary reassigns databae column names to 10 character limit for the ShapeFile
shapefile_field_aliases = {
    'WatershedID': 'HUC'
}


def brat_build(huc, flowlines, max_length, min_length,
               dem, slope, hillshade, flow_accum, drainarea_sqkm, existing_veg, historical_veg, output_folder,
               streamside_buffer, riparian_buffer, max_drainage_area,
               reach_codes, canal_codes,
               flow_areas, waterbodies, max_waterbody,
               valley_bottom, roads, rail, canals, ownership,
               elevation_buffer):
    """Build a BRAT project by segmenting a reach network and copying
    all the necessary layers into the resultant BRAT project

    Arguments:
        huc {str} -- Watershed identifier
        flowlines {str} -- Path to the raw, original polyline flowline ShapeFile
        flow_areas {str} -- Path to the polygon ShapeFile that contains large river outlines
        waterbodies {str} -- Path to the polygon ShapeFile containing water bodies
        max_length {float} -- Maximum allowable flow line segment after segmentation
        min_length {float} -- Shortest allowable flow line segment after segmentation
        dem {str} -- Path to the DEM raster for the watershed
        slope {str} -- Path to the slope raster
        hillshade {str} -- Path to the DEM hillshade raster
        flow_accum {str} -- Path to the flow accumulation raster
        drainarea_sqkm {str} -- Path to the drainage area raster
        existing_veg {str} -- Path to the excisting vegetation raster
        historical_veg {str} -- Path to the historical vegetation raster
        output_folder {str} -- Output folder where the BRAT project will get created
        streamside_buffer {float} -- Streamside vegetation buffer (meters)
        riparian_buffer {float} -- Riparian vegetation buffer (meters)
        max_drainage_area {float} -- Maximum drainage area above which dam capacity will be zero
        intermittent {bool} -- True to keep intermittent streams. False discard them.
        ephemeral {bool} -- True to keep ephemeral streams. False to discard them.
        max_waterbody {float} -- Area (sqm) of largest waterbody to be retained.
        valley_bottom {str} -- Path to valley bottom polygon layer.
        roads {str} -- Path to polyline roads ShapeFile
        rail {str} -- Path to polyline railway ShapeFile
        canals {str} -- Path to polyline canals ShapeFile
        ownership {str} -- Path to land ownership polygon ShapeFile
        elevation_buffer {float} -- Distance to buffer DEM when sampling elevation
    """

    log = Logger("BRAT Build")
    log.info('HUC: {}'.format(huc))
    log.info('EPSG: {}'.format(cfg.OUTPUT_EPSG))

    project, realization, proj_nodes = create_project(huc, output_folder)

    log.info('Adding input rasters to project')
    dem_raster_path_node, dem_raster_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['DEM'], dem)
    prj_existing_path_node, prj_existing_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg)
    prj_historic_path_node, prj_historic_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HISTVEG'], historical_veg)

    # Copy in the rasters we need
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['FA'], flow_accum)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['DA'], drainarea_sqkm)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['SLOPE'], slope)

    # Copy in the vectors we need
    prj_flowlines_node, prj_flowlines = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['FLOWLINES'], flowlines, att_filter="\"ReachCode\" Like '{}%'".format(huc))
    prj_flow_areas_node, prj_flow_areas = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['FLOW_AREA'], flow_areas) if flow_areas else None
    prj_waterbodies_node, prj_waterbodies = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['WATERBODIES'], waterbodies) if waterbodies else None
    prj_valley_bottom_node, prj_valley_bottom = project.add_project_vector(proj_nodes['Inputs'], LayerTypes['VALLEY_BOTTOM'], valley_bottom) if valley_bottom else None

    # Other layers we need
    cleaned_path_node, cleaned_path = project.add_project_vector(proj_nodes['Intermediates'], LayerTypes['CLEANED'], replace=True)
    segmented_path_node, segmented_path = project.add_project_vector(proj_nodes['Outputs'], LayerTypes['SEGMENTED'], replace=True)

    # Filter the flow lines to just the required features and then segment to desired length
    build_network(prj_flowlines, prj_flow_areas, prj_waterbodies, cleaned_path, cfg.OUTPUT_EPSG, reach_codes, max_waterbody)
    segment_network(cleaned_path, segmented_path, max_length, min_length)

    metadata = {
        'BRAT_Build_DateTime': datetime.datetime.now().isoformat(),
        'Max_Length': max_length,
        'Min_Length': min_length,
        'Streamside_Buffer': streamside_buffer,
        'Riparian_Buffer': riparian_buffer,
        'Reach_Codes': reach_codes,
        'Canal_Codes': canal_codes,
        'Max_Waterbody': max_waterbody,
        'Elevation_Buffer': elevation_buffer
    }

    db_path = os.path.join(output_folder, LayerTypes['BRATDB'].rel_path)
    watesrhed_name = create_database(huc, db_path, metadata, cfg.OUTPUT_EPSG)
    populate_database(db_path, segmented_path, huc)
    project.add_metadata({'Watershed': watesrhed_name})

    # Add this to the project file
    project.add_dataset(proj_nodes['Outputs'], db_path, LayerTypes['BRATDB'], 'SQLiteDB')

    # Calculate the geophysical properties slope, min and max elevations
    reach_geometry(db_path, dem_raster_path, elevation_buffer, cfg.OUTPUT_EPSG)

    # Calculate the conflict attributes ready for conservation
    conflict_attributes(db_path, valley_bottom, roads, rail, canals, ownership, 30, 5, cfg.OUTPUT_EPSG)

    # Calculate the vegetation cell counts for each epoch and buffer
    for veg_raster in [prj_existing_path, prj_historic_path]:
        [vegetation_summary(db_path, veg_raster, buffer) for buffer in [streamside_buffer, riparian_buffer]]

    # Copy BRAT build output fields from SQLite to ShapeFile
    log.info('Copying values from SQLite to output ShapeFile')
    write_reach_attributes(segmented_path, db_path, output_fields, shapefile_field_aliases)

    log.info('BRAT build completed successfully.')


def create_project(huc, output_dir):

    project_name = 'BRAT for HUC {}'.format(huc)
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'BRAT')

    project.add_metadata({
        'HUC{}'.format(len(huc)): str(huc),
        'HUC': str(huc),
        'BRATBuildVersion': cfg.version,
        'BRATBuildTimestamp': str(int(time.time()))
    })

    realizations = project.XMLBuilder.add_sub_element(project.XMLBuilder.root, 'Realizations')
    realization = project.XMLBuilder.add_sub_element(realizations, 'BRAT', None, {
        'id': 'BRAT1',
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
        description='Build the inputs for an eventual brat_run:',
        # epilog="This is an epilog"
    )
    parser.add_argument('huc', help='huc input', type=str)
    parser.add_argument('max_length', help='Maximum length of features when segmenting. Zero causes no segmentation.', type=float)
    parser.add_argument('min_length', help='min_length input', type=float)

    parser.add_argument('dem', help='dem input', type=str)
    parser.add_argument('slope', help='slope input', type=str)
    parser.add_argument('hillshade', help='hillshade input', type=str)
    parser.add_argument('flow_accum', help='flow accumulation input', type=str)
    parser.add_argument('drainarea_sqkm', help='drainage area input', type=str)

    parser.add_argument('flowlines', help='flowlines input', type=str)
    parser.add_argument('existing_veg', help='existing_veg input', type=str)
    parser.add_argument('historical_veg', help='historical_veg input', type=str)

    parser.add_argument('valley_bottom', help='Valley bottom shapeFile', type=str)
    parser.add_argument('roads', help='Roads shapeFile', type=str)
    parser.add_argument('rail', help='Railways shapefile', type=str)
    parser.add_argument('canals', help='Canals shapefile', type=str)
    parser.add_argument('ownership', help='Ownership shapefile', type=str)

    parser.add_argument('streamside_buffer', help='streamside_buffer input', type=float)
    parser.add_argument('riparian_buffer', help='riparian_buffer input', type=float)
    parser.add_argument('max_drainage_area', help='max_drainage_area input', type=float)

    parser.add_argument('elevation_buffer', help='elevation_buffer input', type=float)
    parser.add_argument('output_folder', help='output_folder input', type=str)

    parser.add_argument('--reach_codes', help='Comma delimited reach codes (FCode) to retain when filtering features. Omitting this option retains all features.', type=str)
    parser.add_argument('--canal_codes', help='Comma delimited reach codes (FCode) representing canals. Omitting this option retains all features.', type=str)
    parser.add_argument('--flow_areas', help='(optional) path to the flow area polygon feature class containing artificial paths', type=str)
    parser.add_argument('--waterbodies', help='(optional) waterbodies input', type=str)
    parser.add_argument('--max_waterbody', help='(optional) maximum size of small waterbody artificial flows to be retained', type=float)

    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)

    # We can substitute patters for environment varaibles
    args = dotenv.parse_args_env(parser)

    reach_codes = args.reach_codes.split(',') if args.reach_codes else None
    canal_codes = args.canal_codes.split(',') if args.canal_codes else None

    # Initiate the log file
    log = Logger("BRAT Build")
    log.setup(logPath=os.path.join(args.output_folder, "brat_build.log"), verbose=args.verbose)
    log.title('BRAT Build Tool For HUC: {}'.format(args.huc))
    try:
        brat_build(
            args.huc, args.flowlines, args.max_length, args.min_length, args.dem, args.slope,
            args.hillshade, args.flow_accum, args.drainarea_sqkm, args.existing_veg, args.historical_veg, args.output_folder, args.streamside_buffer,
            args.riparian_buffer,
            args.max_drainage_area,
            reach_codes,
            canal_codes,
            args.flow_areas, args.waterbodies, args.max_waterbody,
            args.valley_bottom, args.roads, args.rail, args.canals, args.ownership,
            args.elevation_buffer
        )

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
