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
import sqlite3
from typing import List
import ogr
from rscommons import GeopackageLayer
from rscommons.vector_ops import copy_feature_class
from rscommons import Logger, initGDALOGRErrors, RSLayer, RSProject, ModelConfig, dotenv
from rscommons.build_network import build_network_NEW
from rscommons.database import create_database_NEW
from sqlbrat.utils.vegetation_summary import vegetation_summary
from sqlbrat.utils.reach_geometry import reach_geometry_NEW
from sqlbrat.utils.conflict_attributes import conflict_attributes
from rscommons.database import SQLiteCon
from sqlbrat.__version__ import __version__

Path = str

initGDALOGRErrors()

PERENNIAL_REACH_CODE = 46006

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/BRAT.xsd', __version__)

LayerTypes = {
    'DEM': RSLayer('NED 10m DEM', 'DEM', 'DEM', 'inputs/dem.tif'),
    'SLOPE': RSLayer('Slope Raster', 'SLOPE', 'Raster', 'inputs/slope.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'EXVEG': RSLayer('Existing Vegetation', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HISTVEG': RSLayer('Historic Vegetation', 'HISTVEG', 'Raster', 'inputs/historic_veg.tif'),
    'INPUTS': RSLayer('Confinement', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'FLOWLINES': RSLayer('Segmented Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'FLOW_AREA': RSLayer('NHD Flow Area', 'FLOW_AREA', 'Vector', 'flowareas'),
        'WATERBODIES': RSLayer('NHD Waterbody', 'WATERBODIES', 'Vector', 'waterbodies'),
        'VALLEY_BOTTOM': RSLayer('Valley Bottom', 'VALLEY_BOTTOM', 'Vector', 'valley_bottom'),
    }),
    'OUTPUTS': RSLayer('BRAT', 'OUTPUTS', 'Geopackage', 'outputs/brat.gpkg', {
        'BRAT': RSLayer('BRAT', 'SEGMENTED', 'Vector', 'Reaches')
    })
}

# Attributes in the output BRAT geopackage Reaches feature class layer
brat_reaches_fields = {
    'WatershedID': ogr.OFTString,
    'ReachCode': ogr.OFTInteger,
    'IsPeren': ogr.OFTInteger,
    'StreamName ': ogr.OFTString,
    'Orig_DA': ogr.OFTReal,
    'iGeo_Slope': ogr.OFTReal,
    'iGeo_ElMax': ogr.OFTReal,
    'iGeo_ElMin': ogr.OFTReal,
    'iGeo_Len': ogr.OFTReal,
    'iGeo_DA': ogr.OFTReal,
    'iVeg100EX': ogr.OFTReal,
    'iVeg_30EX': ogr.OFTReal,
    'iVeg100HPE': ogr.OFTReal,
    'iVeg_30HPE': ogr.OFTReal,
    'iPC_Road': ogr.OFTReal,
    'iPC_RoadX': ogr.OFTReal,
    'iPC_RoadVB': ogr.OFTReal,
    'iPC_Rail': ogr.OFTReal,
    'iPC_RailVB': ogr.OFTReal,
    'iPC_LU': ogr.OFTReal,
    'iPC_VLowLU': ogr.OFTReal,
    'iPC_LowLU': ogr.OFTReal,
    'iPC_ModLU': ogr.OFTReal,
    'iPC_HighLU': ogr.OFTReal,
    'iHyd_QLow': ogr.OFTReal,
    'iHyd_Q2': ogr.OFTReal,
    'iHyd_SPLow': ogr.OFTReal,
    'iHyd_SP2': ogr.OFTReal,
    'AgencyID': ogr.OFTInteger,
    'oVC_HPE': ogr.OFTReal,
    'oVC_EX': ogr.OFTReal,
    'oCC_HPE': ogr.OFTReal,
    'mCC_HPE_CT': ogr.OFTReal,
    'oCC_EX': ogr.OFTReal,
    'mCC_EX_CT': ogr.OFTReal,
    'LimitationID': ogr.OFTInteger,
    'RiskID': ogr.OFTInteger,
    'OpportunityID': ogr.OFTInteger,
    'iPC_Canal': ogr.OFTReal,
    'iPC_DivPts': ogr.OFTReal,
    'iPC_Privat': ogr.OFTReal,
    'oPC_Dist': ogr.OFTReal,
    'IsMainCh': ogr.OFTInteger,
    'IsMultiCh': ogr.OFTInteger,
    'mCC_HisDep': ogr.OFTReal
}


def brat_build(huc: int, flowlines: Path, dem: Path, slope: Path, hillshade: Path,
               existing_veg: Path, historical_veg: Path, output_folder: Path,
               streamside_buffer: float, riparian_buffer: float,
               reach_codes: List[str], canal_codes: List[str],
               flow_areas: Path, waterbodies: Path, max_waterbody: float,
               valley_bottom: Path, roads: Path, rail: Path, canals: Path, ownership: Path,
               elevation_buffer: float):
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
        existing_veg {str} -- Path to the excisting vegetation raster
        historical_veg {str} -- Path to the historical vegetation raster
        output_folder {str} -- Output folder where the BRAT project will get created
        streamside_buffer {float} -- Streamside vegetation buffer (meters)
        riparian_buffer {float} -- Riparian vegetation buffer (meters)
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

    project, _realization, proj_nodes = create_project(huc, output_folder)

    log.info('Adding input rasters to project')
    _dem_raster_path_node, dem_raster_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['DEM'], dem)
    _existing_path_node, prj_existing_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg)
    _historic_path_node, prj_historic_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HISTVEG'], historical_veg)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['SLOPE'], slope)

    inputs_gpkg_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path)
    outputs_gpkg_path = os.path.join(output_folder, LayerTypes['OUTPUTS'].rel_path)

    # Make sure we're starting with empty/fresh geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(outputs_gpkg_path)

    # Copy our input layers and also find the difference in the geometry for the valley bottom
    flowlines_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    vbottom_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['VALLEY_BOTTOM'].rel_path)
    waterbodies_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['WATERBODIES'].rel_path)
    flowareas_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['FLOW_AREA'].rel_path)

    copy_feature_class(flowlines, flowlines_path, epsg=cfg.OUTPUT_EPSG)
    copy_feature_class(valley_bottom, vbottom_path, epsg=cfg.OUTPUT_EPSG)
    copy_feature_class(waterbodies, waterbodies_path, epsg=cfg.OUTPUT_EPSG)
    copy_feature_class(flow_areas, flowareas_path, epsg=cfg.OUTPUT_EPSG)

    with GeopackageLayer(flowlines_path) as flow_lyr:
        # Set the output spatial ref as this for the whole project
        out_srs = flow_lyr.spatial_ref

    # Create the output feature class fields
    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['BRAT'].rel_path, delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiLineString, spatial_ref=out_srs, options=['FID=ReachID'], fields={
            'WatershedID': ogr.OFTString,
            'FCode': ogr.OFTInteger,
            'TotDASqKm': ogr.OFTReal,
            'GNIS_Name': ogr.OFTString,
            'NHDPlusID': ogr.OFTReal
        })

    metadata = {
        'BRAT_Build_DateTime': datetime.datetime.now().isoformat(),
        'Streamside_Buffer': streamside_buffer,
        'Riparian_Buffer': riparian_buffer,
        'Reach_Codes': reach_codes,
        'Canal_Codes': canal_codes,
        'Max_Waterbody': max_waterbody,
        'Elevation_Buffer': elevation_buffer
    }

    # Execute the SQL to create the lookup tables in the output geopackage
    watershed_name = create_database_NEW(huc, outputs_gpkg_path, metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'brat_schema.sql'))
    project.add_metadata({'Watershed': watershed_name})

    # Copy the reaches into the output feature class layer, filtering by reach codes
    cleaned_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['BRAT'].rel_path)
    out_srs = build_network_NEW(flowlines_path, flowareas_path, cleaned_path, waterbodies_path=waterbodies_path, epsg=cfg.OUTPUT_EPSG, reach_codes=reach_codes, create_layer=False)

    # Data preparation SQL statements to handle any weird attributes
    with SQLiteCon(outputs_gpkg_path) as database:
        database.curs.execute('INSERT INTO ReachAttributes (ReachID, Orig_DA, iGeo_DA, ReachCode, WatershedID, StreamName) SELECT ReachID, TotDASqKm, TotDASqKm, FCode, WatershedID, GNIS_NAME FROM Reaches')
        database.curs.execute('UPDATE ReachAttributes SET IsPeren = 1 WHERE (ReachCode = ?)', [PERENNIAL_REACH_CODE])
        database.curs.execute('UPDATE ReachAttributes SET iGeo_DA = 0 WHERE iGeo_DA IS NULL')
        database.conn.commit()

    # Calculate the geophysical properties slope, min and max elevations
    reach_geometry_NEW(cleaned_path, dem_raster_path, elevation_buffer, cfg.OUTPUT_EPSG)

    # Calculate the conflict attributes ready for conservation
    conflict_attributes(outputs_gpkg_path, valley_bottom, roads, rail, canals, ownership, 30, 5, cfg.OUTPUT_EPSG)

    # Calculate the vegetation cell counts for each epoch and buffer
    for veg_raster in [prj_existing_path, prj_historic_path]:
        for buffer in [streamside_buffer, riparian_buffer]:
            vegetation_summary(outputs_gpkg_path, veg_raster, buffer)

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

    parser.add_argument('dem', help='dem input', type=str)
    parser.add_argument('slope', help='slope input', type=str)
    parser.add_argument('hillshade', help='hillshade input', type=str)

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
            args.huc, args.flowlines, args.dem, args.slope, args.hillshade,
            args.existing_veg, args.historical_veg, args.output_folder,
            args.streamside_buffer, args.riparian_buffer,
            reach_codes, canal_codes,
            args.flow_areas, args.waterbodies, args.max_waterbody,
            args.valley_bottom, args.roads, args.rail, args.canals, args.ownership,
            args.elevation_buffer
        )

    except Exception as ex:
        log.error(ex)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
