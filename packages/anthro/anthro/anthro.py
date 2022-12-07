""" Build an Anthropogenic Context project

Jordan Gilbert Dec 2022
"""

import argparse
import os
import traceback
import sys
import time
import datetime
from typing import Dict, List
from osgeo import ogr

from rscommons import GeopackageLayer
from rscommons.util import parse_metadata
from rscommons.classes.rs_project import RSMeta
from rscommons.vector_ops import copy_feature_class
from rscommons import Logger, initGDALOGRErrors, RSLayer, RSProject, ModelConfig, dotenv
from rscommons.database import create_database, SQLiteCon
from rscommons.copy_features import copy_features_fields

from anthro.utils.conflict_attributes import conflict_attributes
from anthro.__version__ import __version__

Path = str

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    'EXVEG': RSLayer('Existing Land Cover', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INPUTS': RSLayer('Anthropologic Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'IGO': RSLayer('Integrated Geographic Objects', 'IGO', 'Vector', 'igo'),
        'DGO': RSLayer('Discrete Geographic Object', 'DGO', 'Vector', 'dgo'),
        'FLOWLINES': RSLayer('Segmented Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'VALLEYBOTTOM': RSLayer('Valley Bottom', 'VALLEY', 'Vector', 'valley_bottom'),
        'OWNERSHIP': RSLayer('Ownership', 'OWNERSHIP', 'Vector', 'ownership'),
        'CANALS': RSLayer('Canals', 'CANAL', 'Vector', 'canals'),
        'ROADS': RSLayer('Roads', 'ROADS', 'Vector', 'roads'),
        'RAILS': RSLayer('Railroads', 'RAIL', 'Vector', 'rails'),
        # 'LEVEES': RSLayer('Levees', 'LEVEE', 'Vector', 'levees')
    }),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/intermediates.gpkg', {}),
    'OUTPUTS': RSLayer('Anthropologic Outputs', 'OUTPUTS', 'Geopackage', 'outputs/outputs.gpkg', {
        'ANTHRO_GEOM_POINTS': RSLayer('Anthropogenic Points Geometry', 'ANTHRO_GEOM_POINTS', 'Vector', 'anthro_igo_geom'),
        'ANTHRO_POINTS': RSLayer('Anthropogenic Output Points', 'ANTRHO_POINTS', 'Vector', 'vwIgos'),
        'ANTHRO_GEOM_LINES': RSLayer('Anthropogenic Lines Geometry', 'ANTHRO_GEOM_LINES', 'Vector', 'anthro_lines_geom'),
        'ANTHRO_LINES': RSLayer('Anthropogenic Output Lines', 'ANTHRO_LINES', 'Vector', 'vwReaches')
    })
}


def anthro_context(huc: int, existing_veg: Path, hillshade: Path, igo: Path, dgo: Path,
                   flowlines: Path, valley_bottom: Path, ownership: Path, canals: Path, roads: Path, railroads: Path,
                   canal_codes: List[str], output_folder: Path, meta: Dict[str, str]):
    """
    """

    log = Logger("Anthropogenic Context")
    log.info(f'HUC: {huc}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')

    start_time = time.time()

    project_name = f'Anthropogenic Context for HUC {huc}'
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'Anthro', [
        RSMeta(f'HUC{len(huc)}', str(huc)),
        RSMeta('HUC', str(huc)),
        RSMeta('AnthroVersion', cfg.version),
        RSMeta('AnthroTimeStamp', str(int(time.time())))
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Inputs', 'Outputs'])

    log.info('Adding input rasters to project')
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['OUTPUTS'])

    inputs_gpkg_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg_path = os.path.join(output_folder, LayerTypes['INTERMEDIATES'].rel_path)
    outputs_gpkg_path = os.path.join(output_folder, LayerTypes['OUTPUTS'].rel_path)

    # Make sure we're starting with empty/fresh geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(intermediates_gpkg_path)
    GeopackageLayer.delete(outputs_gpkg_path)

    # copy original vectors to inputs geopackage
    src_layers = {
        'IGO': igo,
        'DGO': dgo,
        'FLOWLINES': flowlines,
        'VALLEYBOTTOM': valley_bottom,
        'CANALS': canals,
        'ROADS': roads,
        'RAILS': railroads,
        'OWNERSHIP': ownership
    }  # include levees once rs context gets them

    input_layers = {}
    for input_key, rslayer in LayerTypes['INPUTS'].sub_layers.items():
        input_layers[input_key] = os.path.join(inputs_gpkg_path, rslayer.rel_path)
        copy_feature_class(src_layers[input_key], input_layers[input_key], cfg.OUTPUT_EPSG)

    # Create the output feature class fields. Only those listed here will get copied from the source.
    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['ANTHRO_GEOM_POINTS'].rel_path, delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, options=['FID=IGOID'], fields={
            'LevelPathI': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'stream_size': ogr.OFTInteger,
            'window_size': ogr.OFTReal,
            'window_area': ogr.OFTReal,
            'centerline_length': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['ANTHRO_GEOM_LINES'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiLineString, epsg=cfg.OUTPUT_EPSG, options=['FID=ReachID'], fields={
            'WatershedID': ogr.OFTString,
            'FCode': ogr.OFTInteger,
            'TotDASqKm': ogr.OFTReal,
            'DivDASqKm': ogr.OFTReal,
            'GNIS_Name': ogr.OFTString,
            'NHDPlusID': ogr.OFTReal
        })

    db_metadata = {
        'AnthroDateTime': datetime.datetime.now().isoformat(),
        'CanalCodes': ','.join(canal_codes)}

    # Execute the SQL to create the lookup tables in the output geopackage
    watershed_name = create_database(huc, outputs_gpkg_path, db_metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'anthro_schema.sql'))

    project.add_metadata_simple(db_metadata)
    project.add_metadata([RSMeta('Watershed', watershed_name)])

    igo_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['ANTHRO_GEOM_POINTS'].rel_path)
    line_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['ANTHRO_GEOM_LINES'].rel_path)
    copy_features_fields(input_layers['IGO'], igo_geom_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(input_layers['FLOWLINES'], line_geom_path, epsg=cfg.OUTPUT_EPSG)

    with SQLiteCon(outputs_gpkg_path) as database:
        database.curs.execute('INSERT INTO ReachAttributes (ReachID, ReachCode, WatershedID, StreamName) SELECT ReachID, FCode, WatershedID, GNIS_NAME FROM anthro_lines_geom')

        # Register vwReaches as a feature layer as well as its geometry column
        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwReaches', data_type, 'Reaches', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'anthro_geom_lines'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwReaches', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'anthro_geom_lines'""")

        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwIgos', data_type, 'igos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'anthro_geom_points'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwIgos', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'anthro_geom_points'""")

        database.conn.commit()

    conflict_attributes(outputs_gpkg_path, line_geom_path, input_layers['DGO'], input_layers['VALLEY_BOTTOM'], input_layers['ROADS'],
                        input_layers['RAILS'], input_layers['CANALS'], input_layers['OWNERSHIP'], 5, cfg.OUTPUT_EPSG, canal_codes, intermediates_gpkg_path)

    buffers = []


def main():
    """
    """

    parser = argparse.ArgumentParser(
        description='Create an Anthropologic Context Project'
    )

    parser.add_argument('huc', help='huc input', type=str)
    parser.add_argument('existing_veg', help='existing vegetation raster input', type=str)
    parser.add_argument('hillshade', help='hillshade input', type=str)
    parser.add_argument('igo', help='integrated geographic object input', type=str)
    parser.add_argument('dgo', help='discrete geographic object input', type=str)
    parser.add_argument('flowlines', help='segmented flowlines input', type=str)
    parser.add_argument('valley_bottom', help='valley bottom input', type=str)
    parser.add_argument('ownership', help='ownership input', type=str)
    parser.add_argument('canals', help='canals input', type=str)
    parser.add_argument('roads', help='roads input', type=str)
    parser.add_argument('railroads', help='railroads input', type=str)
    parser.add_argument('output_folder', help='output folder', type=str)

    parser.add_argument('--canal_codes', help='Comma delimited reach codes (FCode) representing canals. Omitting this option retains all features', type=str)

    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about thigs like memory usage. There is a performance cost', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    canal_codes = args.canal_codes.split(',') if args.canal_codes else None

    log = Logger('Anthropogenic Context')
    log.setup(logPath=os.path.join(args.output_folder, "anthro.log"), verbose=args.verbose)
    log.title(f'Anthropogenic Context for HUC: {args.huc}')

    meta = parse_metadata(args.meta)

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_folder, 'anthro_memusage.log')
            retcode, max_obj = ThreadRun(anthro_context, memfile,
                                         args.huc, args.existing_veg, args.hillshade, args.igo, args.dgo,
                                         args.flowlines, args.valley_bottom, args.ownership, args.canals, args.roads, args.railroads,
                                         canal_codes, args.output_folder, meta)
            log.debug(f'Return code: {retcode} [Max process usage] {max_obj}')
        else:
            anthro_context(
                args.huc, args.existing_veg, args.hillshade, args.igo, args.dgo,
                args.flowlines, args.valley_bottom, args.ownership, args.canals, args.roads, args.railroads,
                canal_codes, args.output_folder, meta
            )

    except Exception as ex:
        log.error(ex)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
