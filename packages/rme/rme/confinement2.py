"""Confinement 

Author: Jordan Gilbert and Kelly Whitehead

Date: 01 Aug 2023
"""

import argparse
import os
import sys
import traceback
from typing import List, Dict
import time

from osgeo import ogr
from osgeo import gdal
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors, ProgressBar
from rscommons import GeopackageLayer
from rscommons.vector_ops import collect_feature_class, get_geometry_unary_union, copy_feature_class
from rscommons.util import safe_makedirs, parse_metadata
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions

from shapely.ops import split, nearest_points, linemerge, substring
from shapely.geometry import Point, MultiPoint, LineString, MultiLineString

from rme.__version__ import __version__

Path = str

initGDALOGRErrors()
gdal.UseExceptions()

cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V1/Confinement.xsd', __version__)

LYR_DESCRIPTION_JSON = os.path.join(os.path.dirname(__file__), 'confinement_layer_descriptions.json')
LayerTypes = {
    # key: (name, id, tag, relpath)]
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'FLOWLINES': RSLayer('Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
        'CHANNEL_AREA': RSLayer('Channel_Area', 'CHANNEL_AREA', 'Vector', 'channel_area'),
        'CONFINING_DGOS': RSLayer('Confining DGOs', 'CONFINING_DGOS', 'Vector', 'confining_dgos'),
    }),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/confinement_intermediates.gpkg', {
        'FLOWLINE_SEGMENTS': RSLayer('Flowline Segments', 'FLOWLINE_SEGMENTS', 'Vector', 'Flowline_Segments'),
        'ERROR_POLYLINES': RSLayer('Error Polylines', 'ERROR_POLYLINES', 'Vector', 'Error_Polylines'),
        'ERROR_POLYGONS': RSLayer('Error Polygons', 'ERROR_POLYGONS', 'Vector', 'Error_Polygons'),
        'CHANNEL_AREA_BUFFERED': RSLayer('Channel Area Buffered', 'CHANNEL_AREA_BUFFERED', 'Vector', 'channel_area_buffered'),
        'CONFINEMENT_BUFFER_SPLIT': RSLayer('Active Channel Split Buffers', 'CONFINEMENT_BUFFER_SPLITS', 'Vector', 'Confinement_Buffers_Split'),
        'CONFINEMENT_ZONES': RSLayer('Zones of Confinement', 'CONFINEMENT_ZONES', 'Vector', 'confinement_zones'),
        'CONFINING_POLYGONS_UNION': RSLayer('Confinement Polygons (unioned)', 'CONFINING_POLYGONS_UNION', 'Vector', 'confining_polygons_union')
    }),
    'CONFINEMENT_RUN_REPORT': RSLayer('Confinement Report', 'CONFINEMENT_RUN_REPORT', 'HTMLFile', 'outputs/confinement.html'),
    'CONFINEMENT': RSLayer('Confinement', 'CONFINEMENT', 'Geopackage', 'outputs/confinement.gpkg', {
        'CONFINEMENT_RAW': RSLayer('Confinement Raw', 'CONFINEMENT_RAW', 'Vector', 'Confinement_Raw'),
        'CONFINEMENT_MARGINS': RSLayer('Confinement Margins', 'CONFINEMENT_MARGINS', 'Vector', 'Confining_Margins'),
        'CONFINEMENT_RATIO': RSLayer('Confinement Ratio', 'CONFINEMENT_RATIO', 'Vector', 'Confinement_Ratio'),
        'CONFINEMENT_BUFFERS': RSLayer('Active Channel Buffer', 'CONFINEMENT_BUFFERS', 'Vector', 'Confinement_Buffers')
    }),
}


def confinement(huc: int, flowlines: Path, channel_area: Path, confining_dgos: Path, output_folder: Path, confinement_type: str, buffer: float = 0.0, meta=None):

    log = Logger('Confinement')
    log.info(f'Confinement v.{cfg.version}')

    augment_layermeta('confinement', LayerTypes, LYR_DESCRIPTION_JSON)

    start_time = time.time()

    # Make the projectXML
    project_name = f'Confinement for HUC {huc}'
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'Confinement', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/confinement', RSMetaTypes.URL, locked=True),
        RSMeta(f'HUC{len(huc)}', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True),
        RSMeta('ConfinementType', confinement_type)
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, "REALIZATION1", cfg.version, data_nodes=['Inputs', 'Intermediates', 'Outputs'])

    # Copy input shapes to a geopackage
    flowlines_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['FLOWLINES'].rel_path)
    confining_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['CONFINING_POLYGON'].rel_path)
    channel_area = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path, LayerTypes['INPUTS'].sub_layers['CHANNEL_AREA'].rel_path)

    copy_feature_class(flowlines, flowlines_path, epsg=cfg.OUTPUT_EPSG)
    copy_feature_class(channel_area, channel_area, epsg=cfg.OUTPUT_EPSG)
    copy_feature_class(confining_dgos, confining_path, epsg=cfg.OUTPUT_EPSG)

    output_gpkg = os.path.join(output_folder, LayerTypes['CONFINEMENT'].rel_path)
    intermediates_gpkg = os.path.join(output_folder, LayerTypes['INTERMEDIATES'].rel_path)

    # Creates an empty geopackage and replaces the old one
    GeopackageLayer(output_gpkg, delete_dataset=True)
    GeopackageLayer(intermediates_gpkg, delete_dataset=True)

    # Add the confinement polygon
    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    _nd, _inputs_gpkg_path, out_gpkg_lyrs = project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['CONFINEMENT'])

    # Additional Metadata
    project.add_metadata([
        RSMeta('Buffer', str(buffer), RSMetaTypes.FLOAT)
    ], out_gpkg_lyrs['CONFINEMENT_BUFFERS'][0])

    # Load input datasets and set the global srs and a meter conversion factor
    with GeopackageLayer(flowlines_path) as flw_lyr:
        srs = flw_lyr.spatial_ref
        meter_conversion = flw_lyr.rough_convert_metres_to_vector_units(1)
        offset = flw_lyr.rough_convert_metres_to_vector_units(0.1)
        selection_buffer = flw_lyr.rough_convert_metres_to_vector_units(0.1)

    # Standard Outputs
    field_lookup = {
        'side': ogr.FieldDefn("Side", ogr.OFTString),
        'flowlineID': ogr.FieldDefn("NHDPlusID", ogr.OFTString),  # ArcGIS cannot read Int64 and will show up as 0, however data is stored correctly in GPKG
        'vbet_level_path': ogr.FieldDefn("vbet_level_path", ogr.OFTString),
        'confinement_type': ogr.FieldDefn("Confinement_Type", ogr.OFTString),
        'confinement_ratio': ogr.FieldDefn("Confinement_Ratio", ogr.OFTReal),
        'constriction_ratio': ogr.FieldDefn("Constriction_Ratio", ogr.OFTReal),
        'length': ogr.FieldDefn("ApproxLeng", ogr.OFTReal),
        'confined_length': ogr.FieldDefn("ConfinLeng", ogr.OFTReal),
        'constricted_length': ogr.FieldDefn("ConstrLeng", ogr.OFTReal),
        # Couple of Debug fields too
        'process': ogr.FieldDefn("ErrorProcess", ogr.OFTString),
        'message': ogr.FieldDefn("ErrorMessage", ogr.OFTString)
    }

    field_lookup['side'].SetWidth(5)
    field_lookup['confinement_type'].SetWidth(5)
