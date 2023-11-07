#!/usr/bin/env python3
# Name:     Riverscapes Context
#
# Purpose:  Build a riverscapes context project by downloading and preparing
#           commonly used data layers for several riverscapes tools.
#
# Author:   Philip Bailey
#
# Date:     9 Sep 2019
# -------------------------------------------------------------------------------
import argparse
import glob
import json
import os
import sys
import time
import traceback
import uuid
from typing import Dict, List

from matplotlib.pyplot import hist
from osgeo import ogr
from regex import B
from rscommons import (Logger, ModelConfig, RSLayer, RSProject, get_shp_or_gpkg, Timer, dotenv,
                       initGDALOGRErrors)
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons.clean_nhd_data import clean_nhd_data
from rscommons.clean_ntd_data import clean_ntd_data
from rscommons.download_dem import download_dem, verify_areas
from rscommons.filegdb import export_table
from rscommons.geographic_raster import gdal_dem_geographic
# from rscommons.prism import calculate_bankfull_width
from rscommons.project_bounds import generate_project_extents_from_layer
from rscommons.raster_warp import raster_vrt_stitch, raster_warp
from rscommons.science_base import (download_shapefile_collection,
                                    get_ntd_urls, us_states)
from rscommons.util import (parse_metadata, pretty_duration, safe_makedirs,
                            safe_remove_dir)
# from rscommons.raster_buffer_stats import raster_buffer_stats2
from rscommons.vector_ops import copy_feature_class, get_geometry_unary_union
from rscommons.geometry_ops import get_rectangle_as_geom
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions, raster_resolution_meta

from rscontext.__version__ import __version__
from rscontext.boundary_management import raster_area_intersection
from rscontext.clean_catchments import clean_nhdplus_catchments
from rscontext.clip_vector import clip_vector_layer
from rscontext.nhdarea import split_nhd_area
from rscontext.rs_context_report import RSContextReport
from rscontext.rs_segmentation import rs_segmentation, create_spatial_view
from rscontext.vegetation import clip_vegetation

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

# These are the Prism BIL types we expect
PrismTypes = ['PPT', 'TMEAN', 'TMIN', 'TMAX', 'TDMEAN', 'VPDMIN', 'VPDMAX']

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    # key: (name, id, tag, relpath)
    'DEM': RSLayer('NED 10m DEM', 'DEM', 'Raster', 'topography/dem.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'topography/dem_hillshade.tif'),
    'SLOPE': RSLayer('Slope', 'SLOPE', 'Raster', 'topography/slope.tif'),
    # Veg Layers
    'EXVEG': RSLayer('Existing Vegetation', 'EXVEG', 'Raster', 'vegetation/existing_veg.tif'),
    'HISTVEG': RSLayer('Historic Vegetation', 'HISTVEG', 'Raster', 'vegetation/historic_veg.tif'),
    'VEGCOVER': RSLayer('Vegetation Cover', 'VEGCOVER', 'Raster', 'vegetation/veg_cover.tif'),
    'VEGHEIGHT': RSLayer('Vegetation Height', 'VEGHEIGHT', 'Raster', 'vegetation/veg_height.tif'),
    'HDIST': RSLayer('Historic Disturbance', 'HDIST', 'Raster', 'vegetation/historic_disturbance.tif'),
    'FDIST': RSLayer('Fuel Disturbance', 'FDIST', 'Raster', 'vegetation/fuel_disturbance.tif'),
    'FCCS': RSLayer('Fuel Characteristic Classification System', 'FCCS', 'Raster', 'vegetation/fccs.tif'),
    'VEGCONDITION': RSLayer('Vegetation Condition', 'VEGCONDITION', 'Raster', 'vegetation/vegetation_condition.tif'),
    'VEGDEPARTURE': RSLayer('Vegetation Departure', 'VEGDEPARTURE', 'Raster', 'vegetation/vegetation_departure.tif'),
    'SCLASS': RSLayer('Succession Classes', 'SCLASS', 'Raster', 'vegetation/succession_classes.tif'),
    # Inputs

    'OWNERSHIP': RSLayer('Ownership', 'Ownership', 'Vector', 'ownership/ownership.shp'),
    'FAIR_MARKET': RSLayer('Fair Market Value', 'FAIRMARKETVALUE', 'Raster', 'ownership/fair_market_value.tif'),
    'ECOREGIONS': RSLayer('Ecoregions', 'Ecoregions', 'Vector', 'ecoregions/ecoregions.shp'),
    'STATES': RSLayer('States', 'States', 'Vector', 'political_boundaries/states.shp'),
    'COUNTIES': RSLayer('Counties', 'Counties', 'Vector', 'political_boundaries/counties.shp'),
    'GEOLOGY': RSLayer('Geology', 'GEOLOGY', 'Vector', 'geology/geology.shp'),

    # NHD Geopackage Layers
    'NHDPLUSHR': RSLayer('NHD HR Plus', 'NHDPLUSHR', 'Geopackage', 'hydrology/nhdplushr.gpkg', {
        # NHD Shapefiles
        'NHDFlowline': RSLayer('NHD Flowlines', 'NHDFlowline', 'Vector', 'NHDFlowline'),
        'NHDFlowlineVAA': RSLayer('NHD Flowlines VAA', 'NHDFlowlineVAA', 'Vector', 'NHDFlowlineVAA'),
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
        'NETWORK300M': RSLayer('NHD Flowlines Segmented 300m', 'NETWORK300M', 'Vector', 'network_300m'),
        'NETWORK300M_INTERSECTION': RSLayer('NHD Flowlines intersected with road, rail and ownership', 'NETWORK300M_INTERSECTION', 'Vector', 'network_intersected'),
        'NETWORK300M_CROSSINGS': RSLayer('NHD Flowlines intersected with road, rail and ownership, segmented to 300m', 'NETWORK300MCROSSINGS', 'Vector', 'network_intersected_300m'),
        'PROCESSING_EXTENT': RSLayer('Processing Extent of HUC-DEM Intersection', 'PROCESSING_EXTENT', 'Vector', 'processing_extent'),
        'NHDAREASPLIT': RSLayer('NDH Area layer split by NHDPlusCatchments', 'NHDAreaSplit', 'Vector', 'NHDAreaSplit')
    }),

    # Prism Layers
    'PPT': RSLayer('Precipitation', 'Precip', 'Raster', 'climate/precipitation.tif'),
    'TMEAN': RSLayer('Mean Temperature', 'MeanTemp', 'Raster', 'climate/mean_temp.tif'),
    'TMIN': RSLayer('Minimum Temperature', 'MinTemp', 'Raster', 'climate/min_temp.tif'),
    'TMAX': RSLayer('Maximum Temperature', 'MaxTemp', 'Raster', 'climate/max_temp.tif'),
    'TDMEAN': RSLayer('Mean Dew Point Temperature', 'MeanDew', 'Raster', 'climate/mean_dew_temp.tif'),
    'VPDMIN': RSLayer('Minimum Vapor Pressure Deficit', 'MinVap', 'Raster', 'climate/min_vapor_pressure.tif'),
    'VPDMAX': RSLayer('Maximum Vapor Pressure Deficit', 'MaxVap', 'Raster', 'climate/max_vapor_pressure.tif'),
    'REPORT': RSLayer('RSContext Report', 'REPORT', 'HTMLFile', 'rs_context.html'),
}

SEGMENTATION = {
    'Max': 300,
    'Min': 50
}


def rs_context(huc, landfire_dir, ownership, fair_market, ecoregions, us_states, us_counties, geology, prism_folder, output_folder, download_folder, scratch_dir, parallel, force_download, meta: Dict[str, str]):
    """

    Download riverscapes context layers for the specified HUC and organize them as a Riverscapes project

    :param huc: Eight, 10 or 12 digit HUC identification number
    :param existing_veg: Path to the existing vegetation conditions raster
    :param historic_veg: Path to the historical vegetation conditions raster
    :param ownership: Path to the national land ownership Shapefile
    :param us_states: Path to the national states shapefile
    :param us_counties: Path to the national counties shapefile 
    :param output_folder: Output location for the riverscapes context project
    :param download_folder: Temporary folder where downloads are cached. This can be shared between rs_context processes
    :param force_download: If false then downloads can be skipped if the files already exist
    :param prism_folder: folder containing PRISM rasters in *.bil format
    :param meta (Dict[str,str]): dictionary of riverscapes metadata key: value pairs
    :return:
    """
    rsc_timer = time.time()
    log = Logger("RS Context")

    # Add the layer metadata immediately before we write anything
    augment_layermeta('rscontext', LYR_DESCRIPTIONS_JSON, LayerTypes)

    log.info('Starting RSContext v.{}'.format(cfg.version))

    try:
        int(huc)
    except ValueError:
        raise Exception('Invalid HUC identifier "{}". Must be an integer'.format(huc))

    if not (len(huc) in [4, 6, 8, 10, 12]):
        raise Exception('Invalid HUC identifier. Must be 4, 8, 10 or 12 digit integer')

    safe_makedirs(output_folder)
    safe_makedirs(download_folder)

    # We need a temporary folder for slope rasters, Stitching inputs, intermeditary products, etc.
    scratch_dem_folder = os.path.join(scratch_dir, 'rs_context', huc)
    safe_makedirs(scratch_dem_folder)

    project_name = 'Riverscapes Context for HUC {}'.format(huc)
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'RSContext', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/rscontext', RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True)
    ])
    project.add_metadata([RSMeta(key, val, RSMetaTypes.HIDDEN, locked=True) for key, val in meta.items()])

    realization = project.add_realization(project_name, 'REALIZATION1', cfg.version)
    datasets = project.XMLBuilder.add_sub_element(realization, 'Datasets')

    nhd_gpkg_path = os.path.join(output_folder, LayerTypes['NHDPLUSHR'].rel_path)
    hydro_deriv_gpkg_path = os.path.join(output_folder, LayerTypes['HYDRODERIVATIVES'].rel_path)

    dem_node, dem_raster = project.add_project_raster(datasets, LayerTypes['DEM'])
    hillshade_node, hill_raster = project.add_project_raster(datasets, LayerTypes['HILLSHADE'])
    slope_node, slope_raster = project.add_project_raster(datasets, LayerTypes['SLOPE'])
    existing_node, existing_clip = project.add_project_raster(datasets, LayerTypes['EXVEG'])
    historic_node, historic_clip = project.add_project_raster(datasets, LayerTypes['HISTVEG'])
    vegcover_node, veg_cover_clip = project.add_project_raster(datasets, LayerTypes['VEGCOVER'])
    vegheight_node, veg_height_clip = project.add_project_raster(datasets, LayerTypes['VEGHEIGHT'])
    hdist_node, hdist_clip = project.add_project_raster(datasets, LayerTypes['HDIST'])
    fdist_node, fdist_clip = project.add_project_raster(datasets, LayerTypes['FDIST'])
    fccs_node, fccs_clip = project.add_project_raster(datasets, LayerTypes['FCCS'])
    vegcond_node, veg_condition_clip = project.add_project_raster(datasets, LayerTypes['VEGCONDITION'])
    vegdep_node, veg_departure_clip = project.add_project_raster(datasets, LayerTypes['VEGDEPARTURE'])
    sclass_node, sclass_clip = project.add_project_raster(datasets, LayerTypes['SCLASS'])
    fairmarket_node, fair_market_clip = project.add_project_raster(datasets, LayerTypes['FAIR_MARKET'])
    input_rasters = [[dem_node, dem_raster], [hillshade_node, hill_raster], [slope_node, slope_raster], [existing_node, existing_clip],
                     [historic_node, historic_clip], [vegcover_node, veg_cover_clip], [vegheight_node, veg_height_clip],
                     [hdist_node, hdist_clip], [fdist_node, fdist_clip], [fccs_node, fccs_clip], [vegcond_node, veg_condition_clip],
                     [vegdep_node, veg_departure_clip], [sclass_node, sclass_clip], [fairmarket_node, fair_market_clip]]

    # Download the four digit NHD archive containing the flow lines and watershed boundaries
    log.info('Processing NHD')

    nhd_download_folder = os.path.join(download_folder, 'nhd', huc[:4])
    nhd_unzip_folder = os.path.join(scratch_dir, 'nhd', huc[:4])

    nhd, filegdb, huc_name, nhd_url = clean_nhd_data(huc, nhd_download_folder, nhd_unzip_folder, nhd_unzip_folder, cfg.OUTPUT_EPSG, False)
    nhdarea_split = split_nhd_area(nhd['NHDArea'], nhd['NHDPlusCatchment'], os.path.join(nhd_unzip_folder, 'NHDAreaSplit.shp'))

    index_dict = {
        'NHDArea': ['FCode', 'NHDPlusID'],
        'NHDFlowline': ['ReachCode', 'FCode', 'NHDPlusID'],
        'NHDPlusCatchment': ['NHDPlusID'],
        'NHDPlusFlowlineVAA': ['NHDPlusID', 'LevelPathl', 'ReachCode'],
        'NHDWaterbody': ['FCode', 'ReachCode']
    }
    for key in nhd.keys():
        if key in index_dict.keys():
            idx = index_dict[key]
        else:
            idx = None
        out_path = os.path.join(nhd_gpkg_path, key)
        copy_feature_class(nhd[key], out_path, epsg=cfg.OUTPUT_EPSG, indexes=idx)

    boundary = 'WBDHU{}'.format(len(huc))

    buffered_clip_path100 = os.path.join(hydro_deriv_gpkg_path, LayerTypes['HYDRODERIVATIVES'].sub_layers['BUFFEREDCLIP100'].rel_path)
    copy_feature_class(nhd[boundary], buffered_clip_path100, epsg=cfg.OUTPUT_EPSG, buffer=100)

    buffered_clip_path500 = os.path.join(hydro_deriv_gpkg_path, LayerTypes['HYDRODERIVATIVES'].sub_layers['BUFFEREDCLIP500'].rel_path)
    copy_feature_class(nhd[boundary], buffered_clip_path500, epsg=cfg.OUTPUT_EPSG, buffer=500)

    area_split_out = os.path.join(hydro_deriv_gpkg_path, LayerTypes['HYDRODERIVATIVES'].sub_layers['NHDAREASPLIT'].rel_path)
    copy_feature_class(nhdarea_split, area_split_out, epsg=cfg.OUTPUT_EPSG, indexes=['FCode', 'NHDPlusID'])

    export_table(filegdb, 'NHDPlusFlowlineVAA', nhd_gpkg_path, None, "ReachCode LIKE '{}%'".format(huc[:8]))

    # Clean up NHDPlusCatchment dataset
    clean_nhdplus_catchments(nhd_gpkg_path, boundary, str(huc))

    # HUC 8 extent polygon
    nhd['HUC8Extent'] = os.path.join(os.path.dirname(nhd['WBDHU8']), 'max_extent.shp')
    with get_shp_or_gpkg(nhd['WBDHU8']) as huc8lyr, get_shp_or_gpkg(nhd['HUC8Extent'], write=True) as outlyr:
        bbox = huc8lyr.ogr_layer.GetExtent()
        extent_box = get_rectangle_as_geom(bbox)

        outlyr.create_layer(ogr.wkbPolygon, 4326)
        outlyr_def = outlyr.ogr_layer_def
        feat = ogr.Feature(outlyr_def)
        feat.SetGeometry(extent_box)
        outlyr.ogr_layer.CreateFeature(feat)

    project.add_metadata([RSMeta('Watershed', huc_name)])

    # PRISM climate rasters
    # mean_annual_precip = None
    bil_files = glob.glob(os.path.join(prism_folder, '*.bil'))
    if (len(bil_files) == 0):
        all_files = glob.glob(os.path.join(prism_folder, '*'))
        raise Exception('Could not find any .bil files in the prism folder: {}. Found: \n{}'.format(prism_folder, "\n".join(all_files)))
    for ptype in PrismTypes:
        try:
            # Next should always be guarded
            source_raster_path = next(x for x in bil_files if ptype.lower() in os.path.basename(x).lower())
        except StopIteration:
            raise Exception('Could not find .bil file corresponding to "{}"'.format(ptype))
        prism_node, project_raster_path = project.add_project_raster(datasets, LayerTypes[ptype])
        raster_warp(source_raster_path, project_raster_path, cfg.OUTPUT_EPSG, buffered_clip_path500, {"cutlineBlend": 1})
        raster_resolution_meta(project, project_raster_path, prism_node)

    states = get_nhd_states(nhd[boundary])

    # Download the NTD archive containing roads and rail
    log.info('Processing NTD')
    ntd_raw = {}
    ntd_unzip_folders = []
    ntd_urls = get_ntd_urls(states)
    for state, ntd_url in ntd_urls.items():
        ntd_download_folder = os.path.join(download_folder, 'ntd', state.lower())
        ntd_unzip_folder = os.path.join(scratch_dir, 'ntd', state.lower(), 'unzipped')  # a little awkward but I need a folder for this and this was the best name I could find
        ntd_raw[state] = download_shapefile_collection(ntd_url, ntd_download_folder, ntd_unzip_folder, force_download)
        ntd_unzip_folders.append(ntd_unzip_folder)

    ntd_clean = clean_ntd_data(ntd_raw, nhd['NHDFlowline'], nhd[boundary], os.path.join(output_folder, 'transportation'), cfg.OUTPUT_EPSG)

    # clean up the NTD Unzip folder. We won't need it again
    if parallel:
        for unzip_path in ntd_unzip_folders:
            safe_remove_dir(unzip_path)

    # Write transportation layers to project file
    log.info('Write transportation layers to project file')

    # Add any results to project XML
    for name, file_path in ntd_clean.items():
        lyr_obj = RSLayer(name, name, 'Vector', os.path.relpath(file_path, output_folder))
        ntd_node, _fpath = project.add_project_vector(datasets, lyr_obj)
        project.XMLBuilder.add_sub_element(ntd_node, 'Description', 'The USGS Transportation downloadable data from The National Map (TNM) is based on TIGER/Line data provided through U.S. Census Bureau and supplemented with HERE road data to create tile cache base maps. Some of the TIGER/Line data includes limited corrections done by USGS. Transportation data consists of roads, railroads, trails, airports, and other features associated with the transport of people or commerce. The data is downloaded from science base by state then clipped to the project extent.')
        project.add_metadata([RSMeta('SourceUrl', 'https://data.usgs.gov/datacatalog/data/USGS:ad3d631d-f51f-4b6a-91a3-e617d6a58b4e', RSMetaTypes.URL),
                              RSMeta('DataProductVersion', '2020'),
                              RSMeta('DocsUrl', f'https://tools.riverscapes.net/data/html#{name}', RSMetaTypes.URL)], ntd_node)
        project.add_metadata([RSMeta(k, v, RSMetaTypes.URL) for k, v in ntd_urls.items()], ntd_node)

    # download contributing DEM rasters, mosaic and reproject into compressed GeoTIF
    ned_download_folder = os.path.join(download_folder, 'ned')
    ned_unzip_folder = os.path.join(scratch_dir, 'ned')
    dem_rasters, urls = download_dem(nhd[boundary], cfg.OUTPUT_EPSG, 0.01, ned_download_folder, ned_unzip_folder, force_download)

    processing_boundary = os.path.join(hydro_deriv_gpkg_path, LayerTypes['HYDRODERIVATIVES'].sub_layers['PROCESSING_EXTENT'].rel_path)
    raster_area_intersection(dem_rasters, nhd[boundary], processing_boundary)
    need_dem_rebuild = force_download or not os.path.exists(dem_raster)
    if need_dem_rebuild:
        raster_vrt_stitch(dem_rasters, dem_raster, cfg.OUTPUT_EPSG, clip=processing_boundary, warp_options={"cutlineBlend": 1})
        area_ratio = verify_areas(dem_raster, nhd[boundary])
        if area_ratio < 0.85:
            log.warning(f'DEM data less than 85%% of nhd extent ({area_ratio:%})')
            # raise Exception(f'DEM data less than 85%% of nhd extent ({area_ratio:%})')

    # Calculate slope rasters seperately and then stitch them
    slope_parts = []
    hillshade_parts = []

    need_slope_build = need_dem_rebuild or not os.path.isfile(slope_raster)
    need_hs_build = need_dem_rebuild or not os.path.isfile(hill_raster)

    project.add_metadata([
        RSMeta('NumRasters', str(len(urls)), RSMetaTypes.INT),
        RSMeta('OriginUrls', json.dumps(urls), RSMetaTypes.JSON)
    ], dem_node)

    for dem_r in dem_rasters:
        slope_part_path = os.path.join(scratch_dem_folder, 'SLOPE__' + os.path.basename(dem_r).split('.')[0] + '.tif')
        hs_part_path = os.path.join(scratch_dem_folder, 'HS__' + os.path.basename(dem_r).split('.')[0] + '.tif')
        slope_parts.append(slope_part_path)
        hillshade_parts.append(hs_part_path)

        if force_download or need_dem_rebuild or not os.path.exists(slope_part_path):
            gdal_dem_geographic(dem_r, slope_part_path, 'slope')
            need_slope_build = True

        if force_download or need_dem_rebuild or not os.path.exists(hs_part_path):
            gdal_dem_geographic(dem_r, hs_part_path, 'hillshade')
            need_hs_build = True

    if need_slope_build:
        raster_vrt_stitch(slope_parts, slope_raster, cfg.OUTPUT_EPSG, clip=processing_boundary, clean=parallel, warp_options={"cutlineBlend": 1})
        verify_areas(slope_raster, nhd[boundary])
    else:
        log.info('Skipping slope build because nothing has changed.')

    if need_hs_build:
        raster_vrt_stitch(hillshade_parts, hill_raster, cfg.OUTPUT_EPSG, clip=processing_boundary, clean=parallel, warp_options={"cutlineBlend": 1})
        verify_areas(hill_raster, nhd[boundary])
    else:
        log.info('Skipping hillshade build because nothing has changed.')

    # Remove the unzipped rasters. We won't need them anymore
    if parallel:
        safe_remove_dir(ned_unzip_folder)

    # Clip and re-project the existing and historic vegetation
    log.info('Processing existing and historic vegetation rasters.')
    in_veg_rasters = [os.path.join(landfire_dir, 'LC20_EVT_220.tif'), os.path.join(landfire_dir, 'LC20_BPS_220.tif'), os.path.join(landfire_dir, 'LC22_EVC_220.tif'),
                      os.path.join(landfire_dir, 'LC22_EVH_220.tif'), os.path.join(landfire_dir, 'LC20_HDst_220.tif'), os.path.join(landfire_dir, 'LC22_FDst_220.tif'),
                      os.path.join(landfire_dir, 'LC22_FCCS_220.tif'), os.path.join(landfire_dir, 'LC20_VCC_220.tif'), os.path.join(landfire_dir, 'LC20_VDep_220.tif'),
                      os.path.join(landfire_dir, 'LC20_SCla_220.tif')]
    out_veg_rasters = [existing_clip, historic_clip, veg_cover_clip, veg_height_clip, hdist_clip, fdist_clip, fccs_clip, veg_condition_clip, veg_departure_clip, sclass_clip]
    clip_vegetation(buffered_clip_path100, in_veg_rasters, out_veg_rasters, cfg.OUTPUT_EPSG)

    log.info('Process the Fair Market Value Raster.')
    raster_warp(fair_market, fair_market_clip, cfg.OUTPUT_EPSG, clip=buffered_clip_path500, warp_options={"cutlineBlend": 1})

    # Clip the landownership Shapefile to a 10km buffer around the watershed boundary
    own_path = os.path.join(output_folder, LayerTypes['OWNERSHIP'].rel_path)
    project.add_dataset(datasets, own_path, LayerTypes['OWNERSHIP'], 'Vector')
    clip_vector_layer(nhd['HUC8Extent'], ownership, own_path, cfg.OUTPUT_EPSG, 10000, clip=True)

    # Clip the states shapefile to a 10km buffer around the watershed boundary
    states_path = os.path.join(output_folder, LayerTypes['STATES'].rel_path)
    project.add_dataset(datasets, states_path, LayerTypes['STATES'], 'Vector')
    clip_vector_layer(nhd['HUC8Extent'], us_states, states_path, cfg.OUTPUT_EPSG, 1000)

    # Clip the counties shapefile to a 10km buffer around the watershed boundary
    counties_path = os.path.join(output_folder, LayerTypes['COUNTIES'].rel_path)
    project.add_dataset(datasets, counties_path, LayerTypes['COUNTIES'], 'Vector')
    clip_vector_layer(nhd['HUC8Extent'], us_counties, counties_path, cfg.OUTPUT_EPSG, 1000)

    # Clip the geology shapefile to a 10km buffer around the watershed boundary
    # geology is in national project - can also be retrieved from science base
    geo_path = os.path.join(output_folder, LayerTypes['GEOLOGY'].rel_path)
    project.add_dataset(datasets, geo_path, LayerTypes['GEOLOGY'], 'Vector')
    clip_vector_layer(nhd['HUC8Extent'], geology, geo_path, cfg.OUTPUT_EPSG, 10000, clip=True)

    # Filter the ecoregions Shapefile to only include attributes that intersect with our HUC
    eco_path = os.path.join(output_folder, 'ecoregions', 'ecoregions.shp')
    project.add_dataset(datasets, eco_path, LayerTypes['ECOREGIONS'], 'Vector')
    clip_vector_layer(nhd['HUC8Extent'], ecoregions, eco_path, cfg.OUTPUT_EPSG, 1000)

    #######################################################
    # Segmentation
    #######################################################

    # create spatial view of NHD Flowlines and VAA table
    fields = {"LevelPathI": "level_path", "DnLevelPat": "downstream_level_path", 'UpLevelPat': "upstream_level_path", 'Divergence': 'divergence'}
    network_fields = {'fid': 'fid', 'geom': 'geom', 'GNIS_ID': 'GNIS_ID', 'GNIS_Name': 'GNIS_Name', 'ReachCode':'ReachCode', 'FType': 'FType', 'FCode': 'FCode', 'NHDPlusID': 'NHDPlusID', 'TotDASqKM': 'TotDASqKM'} #'WatershedID':'WatershedID'
    view_vaa_flowline = create_spatial_view(nhd_gpkg_path, 'NHDFlowline', 'NHDPlusFlowlineVAA', 'vw_NHDFlowlineVAA', network_fields, fields, 'NHDPlusID')

    # add to the project xml
    # vaa_meta = []
    # for field_name, field_alias in fields.items():
    #     vaa_meta.append(RS)
    # LayerTypes['NHDFlowlineVAA'].lyr_meta = [
    #     RSMeta()
    # ]
    node_flowline_vaa = project.add_dataset(datasets, view_vaa_flowline, LayerTypes['NHDPLUSHR'].sub_layers['NHDFlowlineVAA'], 'Vector')
    # add metadata to this
    # node_flowline_vaa.addMetadata()

    # For now let's just make a copy of the NHD FLowlines
    tmr = Timer()
    lines = {'roads': ntd_clean['Roads'], 'railways': ntd_clean['Rail']}
    areas = [{'name': 'ownership',
              'path': own_path, 
              'attributes':[{
                'in_field': 'ADMIN_AGEN',
                'out_field': 'ownership'}]},
             {'name': 'states',
              'path': states_path,
              'attributes':[{
                'in_field': 'STUSPS',
                'out_field': 'us_state'}]},
             {'name': 'ecoregions',
              'path': eco_path,
              'attributes':[{
                'in_field': 'US_L3NAME',
                'out_field': 'ecoregion_iii'},{
                'in_field': 'US_L4NAME',
                'out_field': 'ecoregion_iv'}]}, ]

    rs_segmentation(view_vaa_flowline, lines, areas, hydro_deriv_gpkg_path)
    log.debug('Segmentation done in {:.1f} seconds'.format(tmr.ellapsed()))

    # add geopackages to project xml
    project.add_project_geopackage(datasets, LayerTypes['NHDPLUSHR'])
    project.add_project_geopackage(datasets, LayerTypes['HYDRODERIVATIVES'])

    # Add the report
    report_path = os.path.join(project.project_dir, LayerTypes['REPORT'].rel_path)
    project.add_report(datasets, LayerTypes['REPORT'], replace=True)

    # Add Project Extents
    extents_json_path = os.path.join(output_folder, 'project_bounds.geojson')
    extents = generate_project_extents_from_layer(processing_boundary, extents_json_path)
    project.add_project_extent(extents_json_path, extents['CENTROID'], extents['BBOX'])

    ellapsed_time = time.time() - rsc_timer
    project.add_metadata([
        RSMeta("ProcTimeS", "{:.2f}".format(ellapsed_time), RSMetaTypes.HIDDEN, locked=True),
        RSMeta("Processing Time", pretty_duration(ellapsed_time), locked=True)
    ])

    for raster in input_rasters:
        raster_resolution_meta(project, raster[1], raster[0])
    add_layer_descriptions(project, LYR_DESCRIPTIONS_JSON, LayerTypes)

    # Clean up the unzipped nhd files
    if parallel:
        safe_remove_dir(nhd_unzip_folder)

    report = RSContextReport(report_path, project, output_folder)
    report.write()

    log.info('Process completed successfully.')
    return {
        'DEM': dem_raster,
        'Slope': slope_raster,
        'ExistingVeg': os.path.join(landfire_dir, 'LC20_EVT_220.tif'),
        'HistoricVeg': os.path.join(landfire_dir, 'LC20_BPS_220.tif'),
        'NHD': nhd
    }


def get_nhd_states(inpath):
    """
    Gets the list of US States that an NHD HUC encompasses

    This relies on the watershed boundary ShapeFile having a column called
    'States' that stores a comma separated list of state abbreviations
    such as 'OR,WA'. A dcitionary is used to retrieve the full names.
    :param inpath: Path to the watershed boundary ShapeFile
    :return: List of full US state names that the watershed touches (.e.g. Oregon)
    """
    log = Logger('RS Context')

    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.Open(inpath, 0)
    layer = data_source.GetLayer()
    states = []
    for feature in layer:
        value = feature.GetField('States')
        [states.append(us_states[acronym]) for acronym in value.split(',')]

    data_source = None

    if 'Canada' in states:
        if len(states) == 1:
            log.error('HUC is entirely within Canada. No DEMs will be available.')
        else:
            log.warning('HUC is partially in Canada. Certain data will only be available for US portion.')

    log.info('HUC intersects {} state(s): {}'.format(len(states), ', '.join(states)))
    return list(dict.fromkeys(states))


def main():
    parser = argparse.ArgumentParser(
        description='Riverscapes Context Tool',
        # epilog="This is an epilog"
    )
    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('landfire_dir', help='Folder containing national landfire raster tifs', type=str)
    parser.add_argument('ownership', help='National land ownership shapefile', type=str)
    parser.add_argument('fairmarket', help='National fair market value raster', type=str)
    parser.add_argument('ecoregions', help='National EcoRegions shapefile', type=str)
    parser.add_argument('states', help='National states shapefile', type=str)
    parser.add_argument('counties', help='National counties shapefile', type=str)
    parser.add_argument('geology', help='National SGMC geology shapefile', type=str)
    parser.add_argument('prism', help='Folder containing PRISM rasters in BIL format', type=str)
    parser.add_argument('output', help='Path to the output folder', type=str)
    parser.add_argument('download', help='Temporary folder for downloading data. Different HUCs may share this', type=str)
    parser.add_argument('--force', help='(optional) download existing files ', action='store_true', default=False)
    parser.add_argument('--parallel', help='(optional) for running multiple instances of this at the same time', action='store_true', default=False)
    parser.add_argument('--temp_folder', help='(optional) cache folder for downloading files ', type=str)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about things like memory usage. There is a performance cost', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("RS Context")
    log.setup(logPath=os.path.join(args.output, "rs_context.log"), verbose=args.verbose)
    log.title('Riverscapes Context For HUC: {}'.format(args.huc))

    log.info('HUC: {}'.format(args.huc))
    log.info('EPSG: {}'.format(cfg.OUTPUT_EPSG))
    log.info('LandFire: {}'.format(args.landfire_dir))
    log.info('Ownership: {}'.format(args.ownership))
    log.info('Fair Market Value Raster: {}'.format(args.fairmarket))
    log.info('Output folder: {}'.format(args.output))
    log.info('Download folder: {}'.format(args.download))
    log.info('Force download: {}'.format(args.force))

    # This is a general place for unzipping downloaded files and other temporary work.
    # We use GUIDS to make it specific to a particular run of the tool to avoid unzip collisions
    parallel_code = "-" + str(uuid.uuid4()) if args.parallel is True else ""
    scratch_dir = args.temp_folder if args.temp_folder else os.path.join(args.download, 'scratch', 'rs_context{}'.format(parallel_code))
    safe_makedirs(scratch_dir)

    meta = parse_metadata(args.meta)

    try:

        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output, 'rs_context_memusage.log')
            retcode, max_obj = ThreadRun(rs_context, memfile, args.huc, args.landfire_dir, args.ownership, args.fairmarket, args.ecoregions, args.states, args.counties, args.geology, args.prism, args.output, args.download, scratch_dir, args.parallel, args.force, meta)
            log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))
        else:
            rs_context(args.huc, args.landfire_dir, args.ownership, args.fairmarket, args.ecoregions, args.states, args.counties, args.geology, args.prism, args.output, args.download, scratch_dir, args.parallel, args.force, meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        # Cleaning up the scratch folder is essential
        safe_remove_dir(scratch_dir)
        sys.exit(1)

    # Cleaning up the scratch folder is essential
    safe_remove_dir(scratch_dir)
    sys.exit(0)


if __name__ == '__main__':
    main()
