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

from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions, raster_resolution_meta


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
            'window_size': ogr.OFTReal,
            'window_area': ogr.OFTReal,
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
            'ReachCode': ogr.OFTString,
            'TotDASqKm': ogr.OFTReal,
            'DivDASqKm': ogr.OFTReal,
            'GNIS_Name': ogr.OFTString,
            'NHDPlusID': ogr.OFTReal,
            'level_path': ogr.OFTReal,
            'ownership': ogr.OFTString
        })
