""" Build an Anthropogenic Context project

Jordan Gilbert Dec 2022
"""

import argparse
import os
import time
from typing import Dict
from osgeo import ogr

from rscommons import GeopackageLayer
from rscommons.classes.rs_project import RSMeta
from rscommons.vector_ops import copy_feature_class
from rscommons import Logger, initGDALOGRErrors, RSLayer, RSProject, ModelConfig, dotenv
from rscommons.database import create_database, SQLiteCon
from rscommons.copy_igos import copy_igos

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
        'VALLEYBOTTOM': RSLayer('Valley Bottom', 'VALLEY', 'Vector', 'valley_bottom'),
        'CANALS': RSLayer('Canals', 'CANAL', 'Vector', 'canals'),
        'ROADS': RSLayer('Roads', 'ROADS', 'Vector', 'roads'),
        'RAILS': RSLayer('Railroads', 'RAIL', 'Vector', 'rails'),
        'LEVEES': RSLayer('Levees', 'LEVEE', 'Vector', 'levees')
    }),
    'OUTPUTS': RSLayer('Anthropologic Outputs', 'OUTPUTS', 'Geopackage', 'outputs/outputs.gpkg', {
        'ANTHRO_GEOMETRY': RSLayer('Anthropogenic Output Geometry', 'ANTHRO_GEOMETRY', 'Vector', 'igo')
    })
}


def anthro_context(huc: int, existing_veg: Path, hillshade: Path, igo: Path, dgo: Path,
                   valley_bottom: Path, canals: Path, roads: Path, railroads: Path,
                   levees: Path, output_folder: Path, meta: Dict[str, str]):
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
    outputs_gpkg_path = os.path.join(output_folder, LayerTypes['OUTPUTS'].rel_path)

    # Make sure we're starting with empty/fresh geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(outputs_gpkg_path)

    # copy original vectors to inputs geopackage
    src_layers = {
        'IGO': igo,
        'DGO': dgo,
        'VALLEYBOTTOM': valley_bottom,
        'CANALS': canals,
        'ROADS': roads,
        'RAILS': railroads
    }  # include levees once rs context gets them

    input_layers = {}
    for input_key, rslayer in LayerTypes['INPUTS'].sub_layers.items():
        input_layers[input_key] = os.path.join(inputs_gpkg_path, rslayer.rel_path)
        copy_feature_class(src_layers[input_key], input_layers[input_key], cfg.OUTPUT_EPSG)

    # Create the output feature class fields. Only those listed here will get copied from the source.
    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['ANTHRO_GEOMETRY'].rel_path, delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, options=[], fields={
            'LevelPathI': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'stream_size': ogr.OFTInteger,
            'window_size': ogr.OFTReal,
            'window_area': ogr.OFTReal,
            'centerline_length': ogr.OFTReal
        })

    db_metadata = {}

    # Execute the SQL to create the lookup tables in the output geopackage
    watershed_name = create_database(huc, outputs_gpkg_path, db_metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'anthro_schema.sql'))

    project.add_metadata_simple(db_metadata)
    project.add_metadata([RSMeta('Watershed', watershed_name)])

    igo_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['ANTHRO_GEOMETRY'].rel_path)
    copy_igos(input_layers['IGO'], igo_geom_path, epsg=cfg.OUTPUT_EPSG)
