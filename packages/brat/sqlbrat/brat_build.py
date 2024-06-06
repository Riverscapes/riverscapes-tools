""" Build a BRAT project by segmenting a river network to a specified
    length and then extract the input values required to run the
    BRAT model for each reach segment from various GIS layers.

    Philip Bailey
    30 May 2019

    Returns:
        [type]: [description]
"""
import argparse
import os
import sys
import traceback
import datetime
import time
import json
from typing import List, Dict
from osgeo import ogr
from rscommons import GeopackageLayer
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons.vector_ops import copy_feature_class
from rscommons import Logger, initGDALOGRErrors, RSLayer, RSProject, ModelConfig, dotenv
from rscommons.util import parse_metadata, pretty_duration
from rscommons.build_network import build_network
from rscommons.segment_network import segment_network
from rscommons.database import create_database, SQLiteCon
from rscommons.reach_geometry import reach_geometry
from sqlbrat.utils.vegetation_summary import vegetation_summary
from sqlbrat.utils.conflict_attributes import conflict_attributes
from sqlbrat.utils.dgo_geometry import dgo_geometry
from sqlbrat.__version__ import __version__

Path = str

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    'DEM': RSLayer('NED 10m DEM', 'DEM', 'DEM', 'inputs/dem.tif'),
    'SLOPE': RSLayer('Slope Raster', 'SLOPE', 'Raster', 'inputs/slope.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'EXVEG': RSLayer('Existing Vegetation', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HISTVEG': RSLayer('Historic Vegetation', 'HISTVEG', 'Raster', 'inputs/historic_veg.tif'),
    'INPUTS': RSLayer('Confinement', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'FLOWLINES': RSLayer('Segmented Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'IGOS': RSLayer('Integrated Geographic Objects', 'IGOS', 'Vector', 'igos'),
        'DGOS': RSLayer('Discrete Geographic Objects', 'DGOS', 'Vector', 'dgos'),
        'FLOW_AREA': RSLayer('NHD Flow Area', 'FLOW_AREA', 'Vector', 'flowareas'),
        'WATERBODIES': RSLayer('NHD Waterbody', 'WATERBODIES', 'Vector', 'waterbodies'),
        'VALLEY_BOTTOM': RSLayer('Valley Bottom', 'VALLEY_BOTTOM', 'Vector', 'valley_bottom'),
        'ROADS': RSLayer('Roads', 'ROADS', 'Vector', 'roads'),
        'RAIL': RSLayer('Rail', 'RAIL', 'Vector', 'rail'),
        'CANALS': RSLayer('Canals', 'CANALS', 'Vector', 'canals'),
        'OWNERSHIP': RSLayer('Land Ownership', 'OWNERSHIP', 'Vector', 'ownership')
    }),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/intermediates.gpkg', {
        'SEGMENTED_NETWORK': RSLayer('Segmented Network', 'SEGMENTED_NETWORK', 'Vector', 'segmented_network'),
    }),
    'OUTPUTS': RSLayer('BRAT', 'OUTPUTS', 'Geopackage', 'outputs/brat.gpkg', {
        'BRAT_GEOMETRY': RSLayer('BRAT Geometry', 'BRAT_GEOMETRY', 'Vector', 'ReachGeometry'),
        'BRAT': RSLayer('BRAT', 'BRAT_RESULTS', 'Vector', 'vwReaches'),
        'DGO_GEOM': RSLayer('Discrete Geographic Objects Geometry', 'DGO_GEOM', 'Vector', 'DGOGeometry'),
        'BRAT_DGOS': RSLayer('BRAT Discrete Geographic Objects', 'BRAT_DGOS', 'Vector', 'vwDgos'),
        'IGO_GEOM': RSLayer('Integrated Geographic Objects Geometry', 'IGO_GEOM', 'Vector', 'IGOGeometry'),
        'BRAT_IGOS': RSLayer('BRAT Integrated Geographic Objects', 'BRAT_IGOS', 'Vector', 'vwIgos'),
    })
}


def brat_build(huc: int, flowlines: Path, dgos: Path, igos: Path, dem: Path, slope: Path, hillshade: Path,
               existing_veg: Path, historical_veg: Path, output_folder: Path,
               streamside_buffer: float, riparian_buffer: float,
               reach_codes: List[str], canal_codes: List[str], peren_codes: List[str],
               flow_areas: Path, waterbodies: Path, max_waterbody: float,
               valley_bottom: Path, roads: Path, rail: Path, canals: Path, ownership: Path,
               elevation_buffer: float, meta: Dict[str, str]):
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
        meta (Dict[str,str]): dictionary of riverscapes metadata key: value pairs
    """

    log = Logger("BRAT Build")
    log.info('HUC: {}'.format(huc))
    log.info('EPSG: {}'.format(cfg.OUTPUT_EPSG))

    augment_layermeta()

    start_time = time.time()

    project_name = 'BRAT for HUC {}'.format(huc)
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'BRAT', [
        RSMeta('HUC{}'.format(len(huc)), str(huc)),
        RSMeta('HUC', str(huc)),
        RSMeta('BRATBuildVersion', cfg.version),
        RSMeta('BRATBuildTimestamp', str(int(time.time())))
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Inputs', 'Intermediates', 'Outputs'])

    log.info('Adding input rasters to project')
    _dem_raster_path_node, dem_raster_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['DEM'], dem)
    _existing_path_node, prj_existing_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg)
    _historic_path_node, prj_historic_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HISTVEG'], historical_veg)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['SLOPE'], slope)
    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    db_node, _db_path, *_ = project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['OUTPUTS'])

    inputs_gpkg_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg_path = os.path.join(output_folder, LayerTypes['INTERMEDIATES'].rel_path)
    outputs_gpkg_path = os.path.join(output_folder, LayerTypes['OUTPUTS'].rel_path)

    # Make sure we're starting with empty/fresh geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(intermediates_gpkg_path)
    GeopackageLayer.delete(outputs_gpkg_path)

    # Copy all the original vectors to the inputs geopackage. This will ensure on same spatial reference
    source_layers = {
        'FLOWLINES': flowlines,
        'DGOS': dgos,
        'IGOS': igos,
        'FLOW_AREA': flow_areas,
        'WATERBODIES': waterbodies,
        'VALLEY_BOTTOM': valley_bottom,
        'ROADS': roads,
        'RAIL': rail,
        'CANALS': canals,
        'OWNERSHIP': ownership
    }

    input_layers = {}
    for input_key, rslayer in LayerTypes['INPUTS'].sub_layers.items():
        input_layers[input_key] = os.path.join(inputs_gpkg_path, rslayer.rel_path)
        copy_feature_class(source_layers[input_key], input_layers[input_key], cfg.OUTPUT_EPSG)

    # segment the input flowlines and store them as intermediate
    segmented_network_path = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['SEGMENTED_NETWORK'].rel_path)
    segment_network(input_layers['FLOWLINES'], segmented_network_path, 300, 30, huc, create_layer=True)

    # Create the output feature class fields. Only those listed here will get copied from the source
    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['BRAT_GEOMETRY'].rel_path, delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiLineString, epsg=cfg.OUTPUT_EPSG, options=['FID=ReachID'], fields={
            'WatershedID': ogr.OFTString,
            'FCode': ogr.OFTInteger,
            'TotDASqKm': ogr.OFTReal,
            'DivDASqKm': ogr.OFTReal,
            'GNIS_Name': ogr.OFTString,
            'NHDPlusID': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['DGO_GEOM'].rel_path, delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbPolygon, epsg=cfg.OUTPUT_EPSG, options=['FID=DGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'centerline_length': ogr.OFTInteger,
            'segment_area': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['IGO_GEOM'].rel_path, delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbPolygon, epsg=cfg.OUTPUT_EPSG, options=['FID=IGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'stream_size': ogr.OFTInteger
        })

    db_metadata = {
        'BRAT_Build_DateTime': datetime.datetime.now().isoformat(),
        'Streamside_Buffer': str(streamside_buffer),
        'Riparian_Buffer': str(riparian_buffer),
        'Reach_Codes': ','.join(reach_codes),
        'Canal_Codes': ','.join(canal_codes),
        'Max_Waterbody': str(max_waterbody),
        'Elevation_Buffer': str(elevation_buffer)
    }

    # Execute the SQL to create the lookup tables in the output geopackage
    watershed_name = create_database(huc, outputs_gpkg_path, db_metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'brat_schema.sql'))
    # Just for fun add the db metadata back to the xml
    project.add_metadata_simple(db_metadata)

    project.add_metadata([RSMeta('HUC8_Watershed', watershed_name)])

    # Copy the reaches into the output feature class layer, filtering by reach codes
    reach_geometry_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['BRAT_GEOMETRY'].rel_path)
    build_network(segmented_network_path, input_layers['FLOW_AREA'], reach_geometry_path, waterbodies_path=input_layers['WATERBODIES'], waterbody_max_size=max_waterbody, epsg=cfg.OUTPUT_EPSG, reach_codes=reach_codes, create_layer=False)

    with GeopackageLayer(reach_geometry_path, write=True) as reach_lyr:
        for feat, *_ in reach_lyr.iterate_features('Add WatershedID to ReachGeometry'):
            feat.SetField('WatershedID', huc[:8])
            reach_lyr.ogr_layer.SetFeature(feat)

    with SQLiteCon(outputs_gpkg_path) as database:
        # Data preparation SQL statements to handle any weird attributes
        database.curs.execute('INSERT INTO ReachAttributes (ReachID, Orig_DA, iGeo_DA, ReachCode, WatershedID, StreamName) SELECT ReachID, TotDASqKm, DivDASqKm, FCode, SUBSTR(WatershedID, 1, 8), GNIS_NAME FROM ReachGeometry')
        database.curs.execute('UPDATE ReachAttributes SET IsPeren = 1 WHERE (ReachCode IN ({}))'.format(','.join(peren_codes)))
        database.curs.execute('UPDATE ReachAttributes SET iGeo_DA = 0 WHERE iGeo_DA IS NULL')

        # Register vwReaches as a feature layer as well as its geometry column
        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwReaches', data_type, 'Reaches', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'ReachGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwReaches', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'ReachGeometry'""")

        database.conn.commit()

    # Calculate the geophysical properties slope, min and max elevations
    reach_geometry(reach_geometry_path, dem_raster_path, elevation_buffer)

    # Calculate geophysical attributes for the DGOs
    dgo_geometry(input_layers['DGOS'], input_layers['FLOWLINES'], dem_raster_path, 100, outputs_gpkg_path)

    # Calculate the conflict attributes ready for conservation
    conflict_attributes(outputs_gpkg_path, reach_geometry_path,
                        input_layers['VALLEY_BOTTOM'], input_layers['ROADS'], input_layers['RAIL'], input_layers['CANALS'],
                        input_layers['OWNERSHIP'], 30, 5, cfg.OUTPUT_EPSG, canal_codes, intermediates_gpkg_path)

    # Calculate the vegetation cell counts for each epoch and buffer
    buffer_paths = []
    for label, veg_raster in [('Existing Veg', prj_existing_path), ('Historical Veg', prj_historic_path)]:
        for buffer in [streamside_buffer, riparian_buffer]:
            buffer_path = os.path.join(intermediates_gpkg_path, f'buffer_{int(buffer)}m')
            polygon_path = buffer_path if buffer_path in buffer_paths else None
            vegetation_summary(outputs_gpkg_path, '{} {}m'.format(label, buffer), veg_raster, buffer, polygon_path)
            buffer_paths.append(buffer_path)

    # add buffers to project
    for buffer in [streamside_buffer, riparian_buffer]:
        LayerTypes['INTERMEDIATES'].add_sub_layer(f'{int(buffer)}M_BUFFER', RSLayer(f'{int(buffer)}m Buffer', f'{int(buffer)}M_BUFFER', 'Vector', f'buffer_{int(buffer)}m'))
        LayerTypes['INTERMEDIATES'].sub_layers[f'{int(buffer)}M_BUFFER'].lyr_meta = [RSMeta('Description', f'A polygon of the input drainage network buffered by {int(buffer)} meters.'),
                                                                                     RSMeta('SourceUrl', '', RSMetaTypes.URL),
                                                                                     RSMeta('DataProductVersion', cfg.version),
                                                                                     RSMeta('DocsUrl', f'https://tools.riverscapes.net/brat/data/#{int(buffer)}M_BUFFER', RSMetaTypes.URL)]

    ellapsed_time = time.time() - start_time

    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    project.add_metadata([
        RSMeta("BratBuildProcTimeS", "{:.2f}".format(ellapsed_time), RSMetaTypes.INT),
        RSMeta("BratBuildProcTimeHuman", pretty_duration(ellapsed_time))
    ])

    log.info('BRAT build completed successfully.')


def augment_layermeta():
    """
    For RSContext we've written a JSON file with extra layer meta. We may use this pattern elsewhere but it's just here for now
    """
    with open(LYR_DESCRIPTIONS_JSON, 'r') as f:
        json_data = json.load(f)

    for k, lyr in LayerTypes.items():
        if lyr.sub_layers is not None:
            for h, sublyr in lyr.sub_layers.items():
                if h in json_data and len(json_data[h]) > 0:
                    sublyr.lyr_meta = [
                        RSMeta('Description', json_data[h][0]),
                        RSMeta('SourceUrl', json_data[h][1], RSMetaTypes.URL),
                        RSMeta('DataProductVersion', json_data[h][2]),
                        RSMeta('DocsUrl', 'https://tools.riverscapes.net/brat/data/#{}'.format(sublyr.id), RSMetaTypes.URL)
                    ]
        if k in json_data and len(json_data[k]) > 0:
            lyr.lyr_meta = [
                RSMeta('Description', json_data[k][0]),
                RSMeta('SourceUrl', json_data[k][1], RSMetaTypes.URL),
                RSMeta('DataProductVersion', json_data[k][2]),
                RSMeta('DocsUrl', 'https://tools.riverscapes.net/brat/data/#{}'.format(lyr.id), RSMetaTypes.URL)
            ]


def main():
    """ Main BRAT Build routine
    """

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
    parser.add_argument('--peren_codes', help='Comma delimited reach codes (FCode) representing perennial features', type=str)
    parser.add_argument('--flow_areas', help='(optional) path to the flow area polygon feature class containing artificial paths', type=str)
    parser.add_argument('--waterbodies', help='(optional) waterbodies input', type=str)
    parser.add_argument('--max_waterbody', help='(optional) maximum size of small waterbody artificial flows to be retained', type=float)

    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about things like memory usage. There is a performance cost', action='store_true', default=False)

    # Substitute patterns for environment varaibles
    args = dotenv.parse_args_env(parser)

    reach_codes = args.reach_codes.split(',') if args.reach_codes else None
    canal_codes = args.canal_codes.split(',') if args.canal_codes else None
    peren_codes = args.peren_codes.split(',') if args.peren_codes else None

    # Initiate the log file
    log = Logger("BRAT Build")
    log.setup(logPath=os.path.join(args.output_folder, "brat_build.log"), verbose=args.verbose)
    log.title('BRAT Build Tool For HUC: {}'.format(args.huc))

    meta = parse_metadata(args.meta)

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_folder, 'brat_build_memusage.log')
            retcode, max_obj = ThreadRun(brat_build, memfile,
                                         args.huc, args.flowlines, args.dem, args.slope, args.hillshade,
                                         args.existing_veg, args.historical_veg, args.output_folder,
                                         args.streamside_buffer, args.riparian_buffer,
                                         reach_codes, canal_codes, peren_codes,
                                         args.flow_areas, args.waterbodies, args.max_waterbody,
                                         args.valley_bottom, args.roads, args.rail, args.canals, args.ownership,
                                         args.elevation_buffer,
                                         meta
                                         )
            log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))
        else:
            brat_build(
                args.huc, args.flowlines, args.dem, args.slope, args.hillshade,
                args.existing_veg, args.historical_veg, args.output_folder,
                args.streamside_buffer, args.riparian_buffer,
                reach_codes, canal_codes, peren_codes,
                args.flow_areas, args.waterbodies, args.max_waterbody,
                args.valley_bottom, args.roads, args.rail, args.canals, args.ownership,
                args.elevation_buffer,
                meta
            )

    except Exception as ex:
        log.error(ex)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
