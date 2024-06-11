""" Build a Hydrologic Context project

Jordan Gilbert June 2024"""

import argparse
import os
import traceback
import sys
import time
import datetime
from typing import Dict, List
from osgeo import ogr

from rscommons import Logger, initGDALOGRErrors, dotenv
from rscommons import RSLayer, RSProject, ModelConfig
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons import GeopackageLayer
from rscommons.vector_ops import copy_feature_class
from rscommons.database import create_database, SQLiteCon
from rscommons.copy_features import copy_features_fields
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions, raster_resolution_meta

from hydro.utils.hydrology import hydrology
from hydro.__version__ import __version__

Path = str

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INPUTS': RSLayer('Hydrologic Inputs', 'INPUTS', 'Geopackage', 'inputs/hydro_inputs.gpkg', {
        'IGO': RSLayer('Integrated Geographic Objects', 'IGO', 'Vector', 'igo'),
        'DGO': RSLayer('Discrete Geographic Objects', 'DGO', 'Vector', 'dgo'),
        'FLOWLINES': RSLayer('Segmented Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
    }),
    'OUTPUTS': RSLayer('Hydrologic Outputs', 'OUTPUTS', 'Geopackage', 'outputs/hydro.gpkg', {
        'HYDRO_GEOM_POINTS': RSLayer('Hydrologic IGO Point Geometry', 'HYDRO_GEOM_POINTS', 'Vector', 'IGOGeometry'),
        'HYDRO_POINTS': RSLayer('Hydrologic Output Points', 'ANTRHO_POINTS', 'Vector', 'vwIgos'),
        'HYDRO_GEOM_LINES': RSLayer('Hydrologic Reach Geometry', 'HYDRO_GEOM_LINES', 'Vector', 'ReachGeometry'),
        'HYDRO_LINES': RSLayer('Hydrologic Output Lines', 'HYDRO_LINES', 'Vector', 'vwReaches'),
        'HYDRO_GEOM_DGOS': RSLayer('Hydrologic Output DGOs Polygons', 'HYDRO_GEOM_DGOS', 'Vector', 'DGOGeometry'),
        'HYDRO_DGOS': RSLayer('Hydrologic Output DGOs', 'HYDRO_DGOS', 'Vector', 'vwDgos')
    }),
    'REPORT': RSLayer('Hydrologic Context Report', 'REPORT', 'HTMLFile', 'outputs/hydro.html')
}


def hydro_context(huc: int, hillshade: Path, igo: Path, dgo: Path, flowlines: Path,
                  output_folder: Path, meta: Dict[str, str]):

    log = Logger('Hydrologic Context')
    log.info(f'Starting Hydrologic Context for HUC {huc}')

    augment_layermeta('hydro', LYR_DESCRIPTIONS_JSON, LayerTypes)

    start_time = time.time()

    project_name = f'Hydrologic Context for HUC {huc}'
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'Hydro', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/hydro', RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True)
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Inputs', 'Outputs'])

    log.info('Adding input rasters to project')
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['OUTPUTS'])

    inputs_gpkg_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path)
    outputs_gpkg_path = os.path.join(output_folder, LayerTypes['OUTPUTS'].rel_path)

    # Make sure we're starting with empty/fresh geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(outputs_gpkg_path)

    # copy original vectors to inputs geopackage
    src_layers = {
        'IGO': igo,
        'DGO': dgo,
        'FLOWLINES': flowlines
    }

    input_layers = {}
    for input_key, rslayer in LayerTypes['INPUTS'].sub_layers.items():
        input_layers[input_key] = os.path.join(inputs_gpkg_path, rslayer.rel_path)
        copy_feature_class(src_layers[input_key], input_layers[input_key], cfg.OUTPUT_EPSG)

    # Create the output feature class fields. Only those listed here will get copied from the source.
    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['HYDRO_GEOM_POINTS'].rel_path, delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, options=['FID=IGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'stream_size': ogr.OFTInteger,
            'centerline_length': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['HYDRO_GEOM_DGOS'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPolygon, epsg=cfg.OUTPUT_EPSG, options=['FID=DGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'centerline_length': ogr.OFTReal,
            'segment_area': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['HYDRO_GEOM_LINES'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiLineString, epsg=cfg.OUTPUT_EPSG, options=['FID=ReachID'], fields={
            'FCode': ogr.OFTInteger,
            'TotDASqKm': ogr.OFTReal,
            'DivDASqKm': ogr.OFTReal,
            'GNIS_Name': ogr.OFTString,
            'NHDPlusID': ogr.OFTReal,
            'level_path': ogr.OFTReal,
            'ownership': ogr.OFTString
        })

    db_metadata = {
        'Hydro DateTime': datetime.datetime.now().isoformat()
    }
    create_database(huc, outputs_gpkg_path, db_metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'hydro_schema.sql'))

    igo_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['ANTHRO_GEOM_POINTS'].rel_path)
    line_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['ANTHRO_GEOM_LINES'].rel_path)
    dgo_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['ANTHRO_GEOM_DGOS'].rel_path)
    copy_features_fields(input_layers['IGO'], igo_geom_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(input_layers['FLOWLINES'], line_geom_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(input_layers['DGO'], dgo_geom_path, epsg=cfg.OUTPUT_EPSG)

    with SQLiteCon(outputs_gpkg_path) as database:
        database.curs.execute('INSERT INTO ReachAttributes (ReachID, FCode, NHDPlusID, StreamName, level_path, ownership), SELECT ReachID, FCode, NHDPlusID, GNIS_Name, level_path, ownership FROM ReachGeometry')
        database.curs.execute('INSERT INTO DGOAttributes (DGOID, FCode, level_path, seg_distance, centerline_length, segment_area) SELECT DGOID, FCode, level_path, seg_distance, centerline_length, segment_area FROM DGOGeometry')
        database.curs.execute('INSERT INTO IGOAttributes (IGOID, FCode, level_path, seg_distance,) SELECT IGOID, FCode, level_path, seg_distance, FROM IGOGeometry')

        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwReaches', data_type, 'Reaches', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'ReachGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwReaches', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'ReachGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwIgos', data_type, 'igos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'IGOGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwIgos', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'IGOGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwDgos', data_type, 'dgos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'DGOGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwDgos', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'DGOGeometry'""")

        database.conn.execute('CREATE INDEX ix_igo_levelpath on IGOGeometry(level_path)')
        database.conn.execute('CREATE INDEX ix_igo_segdist on IGOGeometry(seg_distance)')
        database.conn.execute('CREATE INDEX ix_igo_size on IGOGeometry(stream_size)')
        database.conn.execute('CREATE INDEX ix_dgo_levelpath on DGOGeometry(level_path)')
        database.conn.execute('CREATE INDEX ix_dgo_segdist on DGOGeometry(seg_distance)')

        database.conn.commit()

    for suf in ['Low', '2']:
        hydrology(outputs_gpkg_path, suf, huc)
