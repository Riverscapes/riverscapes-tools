""" Riparian Condition Assessment Tool

Jordan Gilbert Jan 2023
"""

import argparse
import os
import time
import datetime

from typing import Dict
from osgeo import ogr, gdal

from rscommons import initGDALOGRErrors, ModelConfig, RSLayer, RSProject, RSMeta, Logger, GeopackageLayer
from rscommons.vector_ops import copy_feature_class, get_geometry_unary_union
from rscommons.database import create_database, SQLiteCon
from rscommons.copy_features import copy_features_fields
# from rscommons.reach_vegetation import vegetation_summary
# from rscommons.igo_vegetation import igo_vegetation

from rcat.__version__ import __version__

Path = str


initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    'EXVEG': RSLayer('Existing Vegetation', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HISTVEG': RSLayer('Historic Vegetation', 'EXVEG', 'Raster', 'inputs/historic_veg.tif'),
    'EXRIPARIAN': RSLayer('Existing Riparian', 'EXRIPARIAN', 'Raster', 'intermediates/ex_riparian.tif'),
    'HISTRIPARIAN': RSLayer('Historic Riparian', 'HISTRIPARIAN', 'Raster', 'intermediates/hist_riparian.tif'),
    'EXVEGETATED': RSLayer('Existing Vegetated', 'EXVEGETATED', 'Raster', 'intermediates/ex_vegetated.tif'),
    'HISTVEGETATED': RSLayer('Historic Vegetated', 'HISTVEGETATED', 'Raster', 'intermediates/hist_vegetated.tif'),
    'PITFILL': RSLayer('Pitfilled DEM', 'PITFILL', 'Raster', 'inputs/pitfill.tif'),
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'ANTHROIGO': RSLayer('Integrated Geographic Objects', 'IGO', 'Vector', 'igo'),
        'ANTHRODGO': RSLayer('Discrete Geographic Objects', 'DGO', 'Vector', 'dgo'),
        'ANTHROREACHES': RSLayer('Segmented Flowlines', 'REACHES', 'Vector', 'reaches'),
        'CANALS': RSLayer('Canals', 'CANAL', 'Vector', 'canals'),
        'ROADS': RSLayer('Roads', 'ROADS', 'Vector', 'roads'),
        'RAILS': RSLayer('Railroads', 'RAIL', 'Vector', 'rails'),
        'VALLEYBOTTOM': RSLayer('Valley Bottom', 'VALLEY', 'Vector', 'valley_bottom'),
        'FLOW_AREA': RSLayer('NHD Flow Area', 'FLOW_AREA', 'Vector', 'flowareas'),
        'WATERBODIES': RSLayer('NHD Waterbody', 'WATERBODIES', 'Vector', 'waterbodies')
    }),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/intermediates.gpkg', {}),
    'OUTPUTS': RSLayer('RCAT Outputs', 'OUTPUTS', 'Geopackage', 'outputs/rcat.gpkg', {
        'GEOM_POINTS': RSLayer('Anthropogenic IGO Point Geometry', 'ANTHRO_GEOM_POINTS', 'Vector', 'IGOGeometry'),
        'IGO': RSLayer('Anthropogenic Output Points', 'ANTRHO_POINTS', 'Vector', 'vwIgos'),
        'GEOM_LINES': RSLayer('Anthropogenic Reach Geometry', 'ANTHRO_GEOM_LINES', 'Vector', 'ReachGeometry'),
        'REACHES': RSLayer('Anthropogenic Output Lines', 'ANTHRO_LINES', 'Vector', 'vwReaches')
    }),
    'REPORT': RSLayer('RCAT Report', 'REPORT', 'HTMLFile', 'outputs/rcat.html')
}

rvd_columns = ['FromConifer',
               'FromDevegetated',
               'FromGrassShrubland',
               'FromDeciduous',
               'NoChange',
               'Deciduous',
               'GrassShrubland',
               'Devegetation',
               'Conifer',
               'Invasive',
               'Development',
               'Agriculture',
               'RiparianTotal',
               'ConversionID']

departure_type_columns = ['ExistingRiparianMean',
                          'HistoricRiparianMean',
                          'RiparianDeparture',
                          'RiparianDepartureID',
                          'ExistingNativeRiparianMean',
                          'HistoricNativeRiparianMean',
                          'NativeRiparianDeparture']


def rcat(huc: int, existing_veg: Path, historic_veg: Path, pitfilled: Path, igo: Path, dgo: Path,
         reaches: Path, roads: Path, rails: Path, canals: Path, valley: Path, flow_areas: Path,
         waterbodies: Path, output_folder: Path, meta: Dict[str, str]):

    log = Logger('RCAT')
    log.info(f'HUC: {huc}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')

    project_name = f'RCAT for HUC {huc}'
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'RCAT', [
        RSMeta(f'HUC{len(huc)}', str(huc)),
        RSMeta('HUC', str(huc)),
        RSMeta('RCATVersion', cfg.version),
        RSMeta('RCATTimeStamp', str(int(time.time())))
    ])

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Inputs', 'Intermediates', 'Ouptuts'])

    log.info('Adding input rasters to project')
    _prj_existing_path_node, prj_existing_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg)
    _prj_historic_path_node, prj_historic_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HISTVEG'], historic_veg)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['PITFILL'], pitfilled)

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
        'ANTHROIGO': igo,
        'ANTHRODGO': dgo,
        'ANTHROREACHES': reaches,
        'VALLEYBOTTOM': valley,
        'CANALS': canals,
        'ROADS': roads,
        'RAILS': rails
    }

    input_layers = {}
    for input_key, rslayer in LayerTypes['INPUTS'].sub_layers.items():
        input_layers[input_key] = os.path.join(inputs_gpkg_path, rslayer.rel_path)
        copy_feature_class(src_layers[input_key], input_layers[input_key], cfg.OUTPUT_EPSG)

    # Create the output feature class fields. Only those listed here will get copied from the source.
    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['GEOM_POINTS'].rel_path, delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, options=['FID=IGOID'], fields={
            'LevelPathI': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'stream_size': ogr.OFTInteger,
            'LUI': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['GEOM_LINES'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiLineString, epsg=cfg.OUTPUT_EPSG, options=['FID=ReachID'], fields={
            'WatershedID': ogr.OFTString,
            'ReachCode': ogr.OFTInteger,
            'StreamName': ogr.OFTString,
            'NHDPlusID': ogr.OFTReal,
            'iPC_LU': ogr.OFTReal
        })

    db_metadata = {
        'RCATDateTime': datetime.datetime.now().isoformat(),
    }

    watershed_name = create_database(huc, outputs_gpkg_path, db_metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'rvd_schema.sql'))

    project.add_metadata_simple(db_metadata)
    project.add_metadata([RSMeta('Watershed', watershed_name)])

    igo_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['GEOM_POINTS'].rel_path)
    reach_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['GEOM_LINES'].rel_path)
    copy_features_fields(input_layers['ANTHROIGO'], igo_geom_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(input_layers['ANTHROREACHES'], reach_geom_path, cfg.OUTPUT_EPSG)

    # remove larger rivers and waterbodies from dgos
    flowareas_path = None
    if flow_areas:
        flowareas_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['FLOW_AREA'].rel_path)
        copy_feature_class(flow_areas, flowareas_path, epsg=cfg.OUTPUT_EPSG)
        geom_flow_areas = get_geometry_unary_union(flowareas_path)
        # Difference with existing vbottom
        geom_vbottom = geom_vbottom.difference(geom_flow_areas)
    else:
        del LayerTypes['INPUTS'].sub_layers['FLOW_AREA']

    waterbodies_path = None
    if waterbodies:
        waterbodies_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['WATERBODIES'].rel_path)
        copy_feature_class(waterbodies, waterbodies_path, epsg=cfg.OUTPUT_EPSG)
        geom_waterbodies = get_geometry_unary_union(waterbodies_path)
        # Difference with existing vbottom
        geom_vbottom = geom_vbottom.difference(geom_waterbodies)
    else:
        del LayerTypes['INPUTS'].sub_layers['WATERBODIES']
