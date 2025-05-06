import argparse
import os
import sys
import time
from typing import Dict, List

from osgeo import gdal, ogr

from rscommons import GeopackageLayer
from rscommons.util import parse_metadata, pretty_duration
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons.vector_ops import copy_feature_class
from rscommons import Logger, initGDALOGRErrors, RSLayer, RSProject, ModelConfig, dotenv
from rscommons.database import create_database, SQLiteCon
from rscommons.copy_features import copy_features_fields
from rscommons.moving_window import moving_window_dgo_ids
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions, raster_resolution_meta

from grazing import __version__


Path = str

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'lyr_descriptions.json')

LayerTypes = {
    'EXVEG': RSLayer('Existing Land Cover', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'SLOPE': RSLayer('DEM Slope', 'SLOPE', 'Raster', 'inputs/dem_slope.tif'),
    'INPUTS': RSLayer('Grazing Inputs', 'INPUTS', 'Vector', 'inputs/inputs.gpkg', {
        'IGO': RSLayer('Integrated Geographic Objects', 'IGO', 'Vector', 'igo'),
        'DGO': RSLayer('Discrete Geographic Object', 'DGO', 'Vector', 'dgo'),
        'CHANNEL': RSLayer('Channel Area', 'CHANNELS', 'Vector', 'channel'),
        'WATERBODIES': RSLayer('Waterbodies', 'WATERBODIES', 'Vector', 'waterbodies'),
    }),
    'VEGSUIT': RSLayer('Grazing Vegetation Suitability', 'VEGSUIT', 'Raster', 'intermediates/veg_suitability.tif'),
    'INTERMEDIATES': RSLayer('Grazing Intermediates', 'INTERMEDIATES', 'Vector', 'intermediates/intermediates.gpkg', {
        'WATER': RSLayer('Merged Channel and Waterbodies', 'WATER', 'Vector', 'water')
    }),
    'LIKELIHOOD': RSLayer('Grazing Likelihood', 'LIKELIHOOD', 'Raster', 'outputs/likelihood.tif'),
    'OUTPUTS': RSLayer('Grazing Outputs', 'OUTPUTS', 'Vector', 'outputs/outputs.gpkg', {
        'GRAZING_DGO_GEOM': RSLayer('Grazing DGO Polygon Geometry', 'GRAZING_DGO_GEOM', 'Vector', 'dgo_geometry'),
        'GRAZING_DGOS': RSLayer('Grazing Likelihood (DGOs)', 'GRAZING_LIKELIHOOD_DGO', 'Vector', 'grazing_dgos'),
        'GRAZING_IGO_GEOM': RSLayer('Grazing IGO Point Geometry', 'GRAZING_IGO_GEOM', 'Vector', 'igo_geometry'),
        'GRAZING_IGOS': RSLayer('Grazing Likelihood (IGOs)', 'GRAZING_LIKELIHOOD_IGO', 'Vector', 'grazing_igos')
    }),
}


def grazing_likelihood(huc: int, existing_veg: Path, slope: Path, hillshade: Path, igo: Path, dgo: Path,
                       waterbodies: Path, channel: Path, output_dir: Path, meta: Dict[str, str]) -> None:
    """
    Main function to run the Grazing Likelihood model.

    :param huc: HUC number for the project
    :param existing_veg: Path to existing vegetation raster
    :param slope: Path to slope raster
    :param hillshade: Path to hillshade raster
    :param inputs: Path to inputs geopackage
    :param waterbodies: Path to waterbodies geopackage
    :param channel: Path to channel geopackage
    :param output_dir: Directory to save outputs
    """
    # Initialize logger and project
    log = Logger('GrazingLikelihood')
    log.info(f'Starting Grazing Likelihood model (v{cfg.version})')
    log.info(f'HUC: {huc}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')

    augment_layermeta('grazing', LYR_DESCRIPTIONS_JSON, LayerTypes)

    project_name = f'Grazing Likelihood for HUC {huc}'
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'Grazing', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/grazing', RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True)
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Inputs', 'Intermediates', 'Outputs'])

    log.info('Adding input rasters to project')
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['OUTPUTS'])

    inputs_gpkg_path = os.path.join(output_dir, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg_path = os.path.join(output_dir, LayerTypes['INTERMEDIATES'].rel_path)
    outputs_gpkg_path = os.path.join(output_dir, LayerTypes['OUTPUTS'].rel_path)

    # Make sure we're starting with empty/fresh geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(intermediates_gpkg_path)
    GeopackageLayer.delete(outputs_gpkg_path)

    # copy original vectors to inputs geopackage
    src_layers = {
        'IGO': igo,
        'DGO': dgo,
        'CHANNEL': channel,
        'WATERBODIES': waterbodies
    }

    input_layers = {}
    for input_key, rslayer in LayerTypes['INPUTS'].sub_layers.items():
        input_layers[input_key] = os.path.join(inputs_gpkg_path, rslayer.rel_path)
        copy_feature_class(src_layers[input_key], input_layers[input_key], cfg.OUTPUT_EPSG)

    # Create the output feature class fields. Only those listed here will get copied from the source.
    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['GRAZING_IGO_GEOM'].rel_path, delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, options=['FID=IGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'stream_size': ogr.OFTInteger,
            'centerline_length': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['GRAZING_DGO_GEOM'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPolygon, epsg=cfg.OUTPUT_EPSG, options=['FID=DGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'centerline_length': ogr.OFTReal,
            'segment_area': ogr.OFTReal
        })
