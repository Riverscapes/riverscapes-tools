#!/usr/bin/env python3
# Name:     Riverscapes Context
#
# Purpose:  Generate HUC10 version of the national BLM project. Brings in NHD and hillshade from RSContext and DGO and IGO from VBET.
#
# Author:   Kelly Whitehead
#
# Date:     29 Jul 2024
# -------------------------------------------------------------------------------
import argparse
import glob
import json
import os
import sys
import shutil
import time
import traceback
import uuid
from typing import Dict

from osgeo import ogr

from rscommons import (Logger, ModelConfig, RSLayer, RSProject, get_shp_or_gpkg, dotenv, initGDALOGRErrors)
from rscommons.classes.rs_project import RSMeta, RSMetaTypes

from rscommons.util import (parse_metadata, pretty_duration, safe_makedirs, safe_remove_dir)
from rscommons.vector_ops import copy_feature_class
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions, raster_resolution_meta

from blm_context.__version__ import __version__

initGDALOGRErrors()

cfg = ModelConfig(
    'https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(
    os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    # key: (name, id, tag, relpath)
    # 'DEM': RSLayer('NED 10m DEM', 'DEM', 'Raster', 'topography/dem.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'topography/dem_hillshade.tif'),
    # NHD Geopackage Layers
    'NHDPLUSHR': RSLayer('NHD HR Plus', 'NHDPLUSHR', 'Geopackage', 'hydrology/nhdplushr.gpkg', {
        # NHD Shapefiles
        'NHDFlowline': RSLayer('NHD Flowlines', 'NHDFlowline', 'Vector', 'NHDFlowline'),
        'NHDFlowlineVAA': RSLayer('NHD Flowlines VAA', 'NHDFlowlineVAA', 'Vector', 'vw_NHDFlowlineVAA'),
        'NHDCatchmentVAA': RSLayer('NHD Catchments VAA', 'NHDCatchmentVAA', 'Vector', 'vw_NHDPlusCatchmentVAA'),
        'NHDArea': RSLayer('NHD Area', 'NHDArea', 'Vector', 'NHDArea'),
        'NHDPlusCatchment': RSLayer('NHD Plus Catchment', 'NHDPlusCatchment', 'Vector', 'NHDPlusCatchment'),
        'NHDWaterbody': RSLayer('NHD Waterbody', 'NHDWaterbody', 'Vector', 'NHDWaterbody'),
        'WBDHU2': RSLayer('HUC2', 'WBDHU2', 'Vector', 'WBDHU2'),
        'WBDHU4': RSLayer('HUC4', 'WBDHU4', 'Vector', 'WBDHU4'),
        'WBDHU6': RSLayer('HUC6', 'WBDHU6', 'Vector', 'WBDHU6'),
        'WBDHU8': RSLayer('HUC8', 'WBDHU8', 'Vector', 'WBDHU8'),
        'WBDHU10': RSLayer('HUC10', 'WBDHU10', 'Vector', 'WBDHU10'),
        'WBDHU12': RSLayer('HUC12', 'WBDHU12', 'Vector', 'WBDHU12'),
        'VAATABLE': RSLayer('NHDPlusFlowlineVAA', 'VAA', 'DataTable', 'NHDPlusFlowlineVAA')
    }),
    'HYDRODERIVATIVES': RSLayer('Hydrology Derivatives', 'HYDRODERIVATIVES', 'Geopackage', 'hydrology/hydro_derivatives.gpkg', {
        'BUFFEREDCLIP100': RSLayer('Buffered Clip Shape 100m', 'BUFFERED_CLIP100', 'Vector', 'buffered_clip100m'),
        'BUFFEREDCLIP500': RSLayer('Buffered Clip Shape 500m', 'BUFFERED_CLIP500', 'Vector', 'buffered_clip500m'),
        'NETWORK_CROSSINGS': RSLayer('NHD Flowlines with road, rail and ownership crossings', 'NETWORK_CROSSINGS', 'Vector', 'network_crossings'),
        'NETWORK_INTERSECTION': RSLayer('NHD Flowlines intersected with road, rail and ownership', 'NETWORK_INTERSECTION', 'Vector', 'network_intersected'),
        'NETWORK_SEGMENTED': RSLayer('NHD Flowlines segmented to 300 m (max) segments', 'NETWORK_SEGMENTED', 'Vector', 'network_segmented'),
        'CATCHMENTS': RSLayer('NHD Catchments', 'CATCHMENTS', 'Vector', 'catchments'),
        'PROCESSING_EXTENT': RSLayer('Processing Extent of HUC-DEM Intersection', 'PROCESSING_EXTENT', 'Vector', 'processing_extent'),
        'NHDAREASPLIT': RSLayer('NDH Area layer split by NHDPlusCatchments', 'NHDAreaSplit', 'Vector', 'NHDAreaSplit'),
        'NHDWATERBODYSPLIT': RSLayer('NDH Waterbody layer split by NHDPlusCatchments', 'NHDWaterbodySplit', 'Vector', 'NHDWaterbodySplit')
    }),
    'VBET': RSLayer('VBET', 'VBET', 'Geopackage', 'vbet/vbet.gpkg', {
        'VBET_FULL': RSLayer('VBET Full Extent', 'VBET_FULL', 'Vector', 'vbet_full'),
        'VBET_IA': RSLayer('VBET Low lying/Elevated Boundary', 'VBET_IA', 'Vector', 'low_lying_valley_bottom'),
        'LOW_LYING_FLOODPLAIN': RSLayer('Low Lying Floodplain', 'LOW_LYING_FLOODPLAIN', 'Vector', 'low_lying_floodplain'),
        'ELEVATED_FLOODPLAIN': RSLayer('Elevated Floodplain', 'ELEVATED_FLOODPLAIN', 'Vector', 'elevated_floodplain'),
        'FLOODPLAIN': RSLayer('Floodplain', 'FLOODPLAIN', 'Vector', 'floodplain'),
        'VBET_CENTERLINES': RSLayer('VBET Centerline', 'VBET_CENTERLINES', 'Vector', 'vbet_centerlines'),
        'SEGMENTATION_POINTS': RSLayer('Segmentation Points', 'SEGMENTATION_POINTS', 'Vector', 'vbet_igos')
    }),
    # 'Habitat Designations'
    'USFWS_CRITICAL_HABITAT_A': RSLayer('USFWS Critical Habitat A', 'USFWS_CRITICAL_HABITAT_A', 'Geopackage', 'Habitat_Designations/USFWS_Critical_Habitat_A.gpkg', {
        'USFWS_CRITICAL_HABITAT_A': RSLayer('USFWS Critical Habitat A', 'USFWS_CRITICAL_HABITAT_A', 'Vector', 'USFWS_Critical_Habitat_A')
    }),
    'USFWS_CRITICAL_HABITAT_L': RSLayer('USFWS Critical Habitat L', 'USFWS_CRITICAL_HABITAT_L', 'Geopackage', 'Habitat_Designations/USFWS_Critical_Habitat_L.gpkg', {
        'USFWS_CRITICAL_HABITAT_L': RSLayer('USFWS Critical Habitat L', 'USFWS_CRITICAL_HABITAT_L', 'Vector', 'USFWS_Critical_Habitat_A')
    }),
    # 'Land Use Planning'
    'NIFC_FUEL_POLYS': RSLayer('NIFC Fuel Polys', 'NIFC_FUEL_POLYS', 'Geopackage', 'Land_Use_Planning/NIFC_Fuel_Polys.gpkg', {
        'NIFC_FUEL_POLYS': RSLayer('NIFC Fuel Polys', 'NIFC_FUEL_POLYS', 'Vector', 'NIFC_Fuel_Polys')
    }),
    'BLM_NATL_FIRE_PERIMETERS_P': RSLayer('BLM Natl Fire Perimeters P', 'BLM_NATL_FIRE_PERIMETERS_P', 'Geopackage', 'Land_Use_Planning/BLM_Natl_Fire_Perimeters_P.gpkg', {
        'BLM_NATL_FIRE_PERIMETERS_P': RSLayer('BLM Natl Fire Perimeters P', 'BLM_NATL_FIRE_PERIMETERS_P', 'Vector', 'BLM_Natl_Fire_Perimeters_P')
    }),
    'BLM_NATL_VISUAL_RESOURCE_INVENTORY_CLASSES_POLYGON_A': RSLayer('BLM Natl Visual Resource Inventory Classes', 'BLM_NATL_VISUAL_RESOURCE_INVENTORY_CLASSES_POLYGON_A', 'Geopackage', 'Land_Use_Planning/BLM_Natl_Visual_Resource_Inventory_Classes_Polygon_A.gpkg', {
        'BLM_NATL_VISUAL_RESOURCE_INVENTORY_CLASSES_POLYGON_A': RSLayer('BLM Natl Visual Resource Inventory Classes', 'BLM_NATL_VISUAL_RESOURCE_INVENTORY_CLASSES_POLYGON_A', 'Vector', 'BLM_Natl_Visual_Resource_Inventory_Classes_Polygon_A')
    }),
    'BLM_NATL_AREA_CRITICAL_ENV_CONCERN_A': RSLayer('BLM Natl Area Critical Env Concern A', 'BLM_NATL_AREA_CRITICAL_ENV_CONCERN_A', 'Geopackage', 'Land_Use_Planning/BLM_Natl_Area_Critical_Env_Concern_A.gpkg', {
        'BLM_NATL_AREA_CRITICAL_ENV_CONCERN_A': RSLayer('BLM Natl Area Critical Env Concern A', 'BLM_NATL_AREA_CRITICAL_ENV_CONCERN_A', 'Vector', 'BLM_Natl_Area_Critical_Env_Concern_A')
    }),
    'BLM_NATL_WILD_HORSE_AND_BURRO_HERD_MGMT_AREA_A': RSLayer('BLM Natl Wild Horse and Burro Herd Mgmt Area A', 'BLM_NATL_WILD_HORSE_AND_BURRO_HERD_MGMT_AREA_A', 'Geopackage', 'Land_Use_Planning/BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_A.gpkg', {
        'BLM_NATL_WILD_HORSE_AND_BURRO_HERD_MGMT_AREA_A': RSLayer('BLM Natl Wild Horse and Burro Herd Mgmt Area A', 'BLM_NATL_WILD_HORSE_AND_BURRO_HERD_MGMT_AREA_A', 'Vector', 'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_A')
    }),
    'BLM_NATL_GRAZING_ALLOTMENT_P': RSLayer('BLM Natl Graziing Allotment P', 'BLM_NATL_GRAZING_ALLOTMENT_P', 'Geopackage', 'Land_Use_Planning/BLM_Natl_Grazing_Allotment_P.gpkg', {
        'BLM_NATL_GRAZING_ALLOTMENT_P': RSLayer('BLM Natl Graziing Allotment P', 'BLM_NATL_GRAZING_ALLOTMENT_P', 'Vector', 'BLM_Natl_Grazing_Allotment_P')
    }),
    'BLM_NATL_WESTERNUS_GRSG_ROD_HABITAT_MGMT_AREAS_AUG22_A': RSLayer('BLM Natl WesternUS GRSG ROD Habitat Mgmt Areas Aug22 A', 'BLM_NATL_WESTERNUS_GRSG_ROD_HABITAT_MGMT_AREAS_AUG22_A', 'Geopackage', 'Land_Use_Planning/BLM_Natl_WesternUS_GRSG_ROD_Habitat_Mgmt_Areas_Aug22_A.gpkg', {
        'BLM_NATL_WESTERNUS_GRSG_ROD_HABITAT_MGMT_AREAS_AUG22_A': RSLayer('BLM Natl WesternUS GRSG ROD Habitat Mgmt Areas Aug22 A', 'BLM_NATL_WESTERNUS_GRSG_ROD_HABITAT_MGMT_AREAS_AUG22_A', 'Vector', 'BLM_Natl_WesternUS_GRSG_ROD_Habitat_Mgmt_Areas_Aug22_A')
    }),
    'BLM_NATL_LAND_USE_PLANS_2022_A': RSLayer('BLM Natl Land Use Plans 2022 A', 'BLM_NATL_LAND_USE_PLANS_2022_A', 'Geopackage', 'Land_Use_Planning/BLM_Natl_Land_Use_Plans_2022_A.gpkg', {
        'BLM_NATL_LAND_USE_PLANS_2022_A': RSLayer('BLM Natl Land Use Plans 2022 A', 'BLM_NATL_LAND_USE_PLANS_2022_A', 'Vector', 'BLM_Natl_Land_Use_Plans_2022_A')
    }),
    'BLM_NATL_REVISION_DEVELOPMENT_LAND_USE_PLANS_A': RSLayer('BLM Natl Revision Development Land Use Plans A', 'BLM_NATL_REVISION_DEVELOPMENT_LAND_USE_PLANS_A', 'Geopackage', 'Land_Use_Planning/BLM_Natl_Revision_Development_Land_Use_Plans_A.gpkg', {
        'BLM_NATL_REVISION_DEVELOPMENT_LAND_USE_PLANS_A': RSLayer('BLM Natl Revision Development Land Use Plans A', 'BLM_NATL_REVISION_DEVELOPMENT_LAND_USE_PLANS_A', 'Vector', 'BLM_Natl_Revision_Development_Land_Use_Plans_A')
    }),
    'BLM_ES_SO_NATL_SCENIC_HISTORIC_TRAILS_NLCS_L': RSLayer('BLM ES SO Natl Scenic Historic Trails NLCS L', 'BLM_ES_SO_NATL_SCENIC_HISTORIC_TRAILS_NLCS_L', 'Geopackage', 'Land_Use_Planning/BLM_ES_SO_Natl_Scenic_Historic_Trails_NLCS_L.gpkg', {
        'BLM_ES_SO_NATL_SCENIC_HISTORIC_TRAILS_NLCS_L': RSLayer('BLM ES SO Natl Scenic Historic Trails NLCS L', 'BLM_ES_SO_NATL_SCENIC_HISTORIC_TRAILS_NLCS_L', 'Vector', 'BLM_ES_SO_Natl_Scenic_Historic_Trails_NLCS_L')
    }),
    'BLM_NATL_RECREATION_SITE_POLYGONS': RSLayer('BLM Natl Recreation Site Polygons', 'BLM_NATL_RECREATION_SITE_POLYGONS', 'Geopackage', 'Land_Use_Planning/BLM_Natl_Recreation_Site_Polygons.gpkg', {
        'BLM_NATL_RECREATION_SITE_POLYGONS': RSLayer('BLM Natl Recreation Site Polygons', 'BLM_NATL_RECREATION_SITE_POLYGONS', 'Vector', 'BLM_Natl_Recreation_Site_Polygons')
    }),
    # 'National Priority Areas'
    'BLM_RESTORATION_LANDSCAPES_A': RSLayer('BLM Restoration Landscapes A', 'BLM_RESTORATION_LANDSCAPES_A', 'Geopackage', 'BLM_National_Priority_Areas/BLM_Restoration_Landscapes_A.gpkg', {
        'BLM_RESTORATION_LANDSCAPES_A': RSLayer('BLM Restoration Landscapes A', 'BLM_RESTORATION_LANDSCAPES_A', 'Vector', 'BLM_Restoration_Landscapes_A')
    }),
    'DOI_KEYSTONE_INITIATIVES_A': RSLayer('DOI Keystone Initiatives A', 'DOI_KEYSTONE_INITIATIVES_A', 'Geopackage', 'National_Priority_Areas/DOI_Keystone_Initiatives_A.gpkg', {
        'DOI_KEYSTONE_INITIATIVES_A': RSLayer('DOI Keystone Initiatives A', 'DOI_KEYSTONE_INITIATIVES_A', 'Vector', 'DOI_Keystone_Initiatives_A')
    }),
    # 'National Landscape Conservation
    'BLM_NATL_NLCS_WILDERNESS_AREAS_A': RSLayer('BLM Natl NLCS Wilderness Areas A', 'BLM_NATL_NLCS_WILDERNESS_AREAS_A', 'Geopackage', 'National_Landscape_Conservation_System/BLM_Natl_NLCS_Wilderness_Areas_A.gpkg', {
        'BLM_NATL_NLCS_WILDERNESS_AREAS_A': RSLayer('BLM Natl NLCS Wilderness Areas A', 'BLM_NATL_NLCS_WILDERNESS_AREAS_A', 'Vector', 'BLM_Natl_NLCS_Wilderness_Areas_A')
    }),
    'BLM_NATL_NLCS_WILDERNESS_STUDY_AREAS_A': RSLayer('BLM Natl NLCS Wilderness Study Areas A', 'BLM_NATL_NLCS_WILDERNESS_STUDY_AREAS_A', 'Geopackage', 'National_Landscape_Conservation_System/BLM_Natl_NLCS_Wilderness_Study_Areas_A.gpkg', {
        'BLM_NATL_NLCS_WILDERNESS_STUDY_AREAS_A': RSLayer('BLM Natl NLCS Wilderness Study Areas A', 'BLM_NATL_NLCS_WILDERNESS_STUDY_AREAS', 'Vector', 'BLM_Natl_NLCS_Wilderness_Study_Areas_A')
    }),
    'BLM_NLCS_NATL_MONUMENTS_CONS_AREAS_A': RSLayer('BLM NLCS Natl Monuments Cons Areas A', 'BLM_NLCS_NATL_MONUMENTS_CONS_AREAS_A', 'Geopackage', 'National_Landscape_Conservation_System/BLM_NLCS_Natl_Monuments_Cons_Areas_A.gpkg', {
        'BLM_NLCS_NATL_MONUMENTS_CONS_AREAS_A': RSLayer('BLM NLCS Natl Monuments Cons Areas A', 'BLM_NLCS_NATL_MONUMENTS_CONS_AREAS_A', 'Vector', 'BLM_NLCS_Natl_Monuments_Cons_Areas_A')
    })
}


def blm_context(huc: int, blm_context_folder: str, rsc_folder: str, vbet_folder: str, output_folder: str, meta: Dict[str, str]):

    log = Logger("BLM Context")

    # Add the layer metadata immediately before we write anything
    augment_layermeta('rscontext', LYR_DESCRIPTIONS_JSON, LayerTypes)

    log.info(f'Starting BLM Context v. {__version__}')

    try:
        int(huc)
    except ValueError:
        raise Exception(f'Invalid HUC identifier "{huc}". Must be an integer')
    if not (len(huc) in [4, 6, 8, 10, 12]):
        raise Exception('Invalid HUC identifier. Must be 4, 8, 10 or 12 digit integer')

    if os.path.exists(output_folder):
        safe_remove_dir(output_folder)
    safe_makedirs(output_folder)

    project_name = f'Riverscapes Context for HUC {huc}'
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'RSContext', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/rscontext',
               RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True)
    ])
    project.add_metadata(
        [RSMeta(key, val, RSMetaTypes.HIDDEN, locked=True) for key, val in meta.items()])

    realization = project.add_realization(
        project_name, 'REALIZATION1', cfg.version)
    datasets = project.XMLBuilder.add_sub_element(realization, 'Datasets')

    hydrology_folder = os.path.join(output_folder, 'hydrology')
    safe_makedirs(hydrology_folder)

    # Copy the NHDPlusHR geopackage
    nhdplushr_gpkg = os.path.join(hydrology_folder, 'nhdplushr.gpkg')
    nhdplushr_gpkg_src = os.path.join(rsc_folder, 'hydrology', 'nhdplushr.gpkg')
    log.info(f'Copying NHDPlusHR geopackage from {nhdplushr_gpkg_src} to {nhdplushr_gpkg}')
    shutil.copy(nhdplushr_gpkg_src, nhdplushr_gpkg)

    # Copy the derived hydrology geopackage
    hydro_derivatives_gpkg = os.path.join(hydrology_folder, 'hydro_derivatives.gpkg')
    hydro_derivatives_gpkg_src = os.path.join(rsc_folder, 'hydrology', 'hydro_derivatives.gpkg')
    log.info(f'Copying Hydrology Derivatives geopackage from {hydro_derivatives_gpkg_src} to {hydro_derivatives_gpkg}')
    shutil.copy(hydro_derivatives_gpkg_src, hydro_derivatives_gpkg)

    # Copy the hillshade
    topogrpahy_folder = os.path.join(output_folder, 'topography')
    safe_makedirs(topogrpahy_folder)
    hillshade = os.path.join(topogrpahy_folder, 'dem_hillshade.tif')
    hillshade_src = os.path.join(rsc_folder, 'topography', 'dem_hillshade.tif')
    log.info(f'Copying hillshade from {hillshade_src} to {hillshade}')
    shutil.copy(hillshade_src, hillshade)

    # Get the DGO and IGO from VBET
    vbet_output_folder = os.path.join(output_folder, 'vbet')
    safe_makedirs(vbet_output_folder)
    vbet_gpkg = os.path.join(vbet_output_folder, 'vbet.gpkg')
    vbet_gpkg_src = os.path.join(vbet_folder, 'outputs', 'vbet.gpkg')
    log.info(f'Copying VBET geopackage from {vbet_gpkg_src} to {vbet_gpkg}')
    shutil.copy(vbet_gpkg_src, vbet_gpkg)

    # Get the HUC10 boundary layer
    huc_boundary = os.path.join(nhdplushr_gpkg, 'WBDHU10')
    with get_shp_or_gpkg(huc_boundary) as huc_lyr:
        huc_boundary_geom = None
        for feat, *_ in huc_lyr.iterate_features('Finding HUC10 boundary'):
            geom = feat.GetGeometryRef()
            if huc_boundary_geom is None:
                huc_boundary_geom = geom.Clone()
            else:
                huc_boundary_geom = huc_boundary_geom.Union(geom)

    list_of_blm_context_layers = [
        'Habitat_Designations/USFWS_Critical_Habitat_A.gpkg/USFWS_Critical_Habitat_A',
        'Habitat_Designations/USFWS_Critical_Habitat_L.gpkg/USFWS_Critical_Habitat_L',
        'Land_Use_Planning/NIFC_Fuel_Polys.gpkg/NIFC_Fuel_Polys',
        'Land_Use_Planning/BLM_Natl_Fire_Perimeters_P.gpkg/BLM_Natl_Fire_Perimeters_P',
        'Land_Use_Planning/BLM_Natl_Visual_Resource_Inventory_Classes_Polygon_A.gpkg/BLM_Natl_Visual_Resource_Inventory_Classes_Polygon_A',
        'Land_Use_Planning/BLM_Natl_Area_Critical_Env_Concern_A.gpkg/BLM_Natl_Area_Critical_Env_Concern_A',
        'Land_Use_Planning/BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_A.gpkg/BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_A',
        'Land_Use_Planning/BLM_Natl_Grazing_Allotment_P.gpkg/BLM_Natl_Grazing_Allotment_P',
        'Land_Use_Planning/BLM_Natl_WesternUS_GRSG_ROD_Habitat_Mgmt_Areas_Aug22_A.gpkg/BLM_Natl_WesternUS_GRSG_ROD_Habitat_Mgmt_Areas_Aug22_A',
        'Land_Use_Planning/BLM_Natl_Land_Use_Plans_2022_A.gpkg/BLM_Natl_Land_Use_Plans_2022_A',
        'Land_Use_Planning/BLM_Natl_Revision_Development_Land_Use_Plans_A.gpkg/BLM_Natl_Revision_Development_Land_Use_Plans_A',
        'Land_Use_Planning/BLM_ES_SO_Natl_Scenic_Historic_Trails_NLCS_L.gpkg/BLM_ES_SO_Natl_Scenic_Historic_Trails_NLCS_L',
        'Land_Use_Planning/BLM_Natl_Recreation_Site_Polygons.gpkg/BLM_Natl_Recreation_Site_Polygons',
        'BLM_National_Priority_Areas/BLM_Restoration_Landscapes_A.gpkg/BLM_Restoration_Landscapes_A',
        'BLM_National_Priority_Areas/DOI_Keystone_Initiatives_A.gpkg/DOI_Keystone_Initiatives_A',
        'National_Landscape_Conservation_System/BLM_Natl_NLCS_Wilderness_Areas_A.gpkg/BLM_Natl_NLCS_Wilderness_Areas_A',
        'National_Landscape_Conservation_System/BLM_Natl_NLCS_Wilderness_Study_Areas_A.gpkg/BLM_Natl_NLCS_Wilderness_Study_Areas_A',
        'National_Landscape_Conservation_System/BLM_NLCS_Natl_Monuments_Cons_Areas_A.gpkg/BLM_NLCS_Natl_Monuments_Cons_Areas_A'
    ]
    for layer in list_of_blm_context_layers:
        layer_path = os.path.join(blm_context_folder, layer)
        output_layer = os.path.join(output_folder, layer)
        safe_makedirs(os.path.dirname(os.path.dirname(output_layer)))
        # Clip the layer to the HUC10 boundary
        log.info(f'Clipping {layer} to HUC10 boundary')
        copy_feature_class(layer_path, output_layer, epsg=cfg.OUTPUT_EPSG, clip_shape=huc_boundary_geom, hard_clip=True)
        log.info(f'Clipped {layer_path} to {output_layer}')


def main():
    """
    This is the main function of the BLM Context Tool.

    It parses command line arguments, sets up logging, and calls the `blm_context` function
    to generate BLM context for a given HUC.
    """

    parser = argparse.ArgumentParser(
        description='BLM Context Tool',
        # epilog="This is an epilog"
    )
    # huc: int, blm_context_folder: str, rsc_folder: str, vbet_folder: str, output_folder: str, meta: Dict[str, str]
    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('blm_context_folder', help='Folder containing national blm context project', type=str)
    parser.add_argument('rsc_folder', help='Folder containing riverscapes context project', type=str)
    parser.add_argument('vbet_folder', help='Folder containing vbet project', type=str)
    parser.add_argument('output_folder', help='Path to the output folder', type=str)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about things like memory usage. There is a performance cost', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("BLM Context")
    log.setup(logPath=os.path.join(
        args.output_folder, "blm_context.log"), verbose=args.verbose)
    log.title(f'BLM Context For HUC: {args.huc}')

    log.info(f'HUC: {args.huc}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')
    log.info(f'National BLM Context Project: {args.blm_context_folder}')
    log.info(f'RSC Project: {args.rsc_folder}')
    log.info(f'Vbet Project: {args.vbet_folder}')
    log.info(f'Output folder: {args.output_folder}')

    meta = parse_metadata(args.meta)

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_folder, 'blm_context_memusage.log')
            retcode, max_obj = ThreadRun(blm_context, memfile, args.huc, args.blm_context_folder, args.rsc_folder, args.vbet_folder, args.output_folder, meta)
            log.debug(f'Return code: {retcode}, [Max process usage] {max_obj}')
        else:
            blm_context(args.huc, args.blm_context_folder, args.rsc_folder, args.vbet_folder, args.output_folder, meta)
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
