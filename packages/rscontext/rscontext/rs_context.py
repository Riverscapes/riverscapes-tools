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
import sys
import os
import traceback
import uuid
import datetime
from osgeo import ogr
from osgeo import gdal

from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors
from rscommons.util import safe_makedirs
from rscommons.clean_nhd_data import clean_nhd_data
from rscommons.clean_ntd_data import clean_ntd_data
from rscommons.raster_warp import raster_warp, raster_vrt_stitch
from rscommons.download_dem import download_dem, verify_areas
from rscommons.science_base import download_shapefile_collection, get_ntd_urls, us_states
from rscommons.geographic_raster import gdal_dem_geographic
from rscommons.download_hand import download_hand
from rscommons.prism import mean_area_precip, calculate_bankfull_width

from rscontext.flow_accumulation import flow_accumulation, flow_accum_to_drainage_area
from rscontext.clip_ownership import clip_ownership
from rscontext.filter_ecoregions import filter_ecoregions
from rscontext.__version__ import __version__

initGDALOGRErrors()

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/RSContext.xsd', __version__)

LayerTypes = {
    # key: (name, id, tag, relpath)
    'DEM': RSLayer('NED 10m DEM', 'DEM', 'DEM', 'topography/dem.tif'),
    'FA': RSLayer('Flow Accumulation', 'FA', 'Raster', 'topography/flow_accum.tif'),
    'DA': RSLayer('Drainage Area', 'DA', 'Raster', 'topography/drainarea_sqkm.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'topography/dem_hillshade.tif'),
    'SLOPE': RSLayer('Slope', 'SLOPE', 'Raster', 'topography/slope.tif'),
    'HAND': RSLayer('Height above nearest drainage', 'HAND', 'Raster', 'topography/hand.tif'),
    'EXVEG': RSLayer('Existing Vegetation', 'EXVEG', 'Raster', 'vegetation/existing_veg.tif'),
    'HISTVEG': RSLayer('Historic Vegetation', 'HISTVEG', 'Raster', 'vegetation/historic_veg.tif'),
    'NETWORK': RSLayer('NHD Flowlines', 'NETWORK', 'Vector', 'inputs/network.shp'),
    'OWNERSHIP': RSLayer('Ownership', 'Ownership', 'Vector', 'inputs/ownership.shp'),
    'ECOREGIONS': RSLayer('Ecoregions', 'Ecoregions', 'Vector', 'inputs/ecoregions.shp'),
    'PRISM': RSLayer('Prism', 'Prism', 'Vector', 'inputs/Prism.gpkg')
}


def rs_context(huc, existing_veg, historic_veg, ownership, ecoregions, prism, output_folder, download_folder, temp_folder, force_download):
    """
    Download riverscapes context layers for the specified HUC and organize them as a Riverscapes project
    :param huc: Eight digit HUC identification number
    :param existing_veg: Path to the existing vegetation conditions raster
    :param historic_veg: Path to the historical vegetation conditions raster
    :param ownership: Path to the national land ownership Shapefile
    :param output_folder: Output location for the riverscapes context project
    :param download_folder: Temporary folder where downloads are cached. This can be shared between rs_context processes
    :param temp_folder: (optional) Temporary folder for unzipping etc. Download_folder is used if this is ommitted.
    :param force_download: If false then downloads can be skipped if the files already exist
    :param prism: prism data geopackage
    :return:
    """

    log = Logger("RS Context")
    log.info('Starting RSContext v.{}'.format(cfg.version))

    try:
        int(huc)
    except ValueError:
        raise Exception('Invalid HUC identifier "{}". Must be an integer'.format(huc))

    if not (len(huc) == 4 or len(huc) == 8):
        raise Exception('Invalid HUC identifier. Must be four digit integer')

    safe_makedirs(output_folder)
    safe_makedirs(download_folder)

    # This is a general place for unzipping downloaded files and other temporary work.
    scratch_dir = temp_folder if temp_folder else os.path.join(download_folder, 'scratch')
    safe_makedirs(scratch_dir)

    # We need a temporary folder for slope rasters, Stitching inputs, intermeditary products, etc.
    scratch_dem_folder = os.path.join(scratch_dir, 'rs_context', huc)
    safe_makedirs(scratch_dem_folder)

    project, realization = create_project(huc, output_folder)

    dem_raster_node, dem_raster = project.add_project_raster(realization, LayerTypes['DEM'])
    hill_raster_node, hill_raster = project.add_project_raster(realization, LayerTypes['HILLSHADE'])
    flow_accum_node, flow_accum = project.add_project_raster(realization, LayerTypes['FA'])
    drain_area_node, drain_area = project.add_project_raster(realization, LayerTypes['DA'])
    hand_raster_node, hand_raster = project.add_project_raster(realization, LayerTypes['HAND'])
    slope_raster_node, slope_raster = project.add_project_raster(realization, LayerTypes['SLOPE'])
    existing_clip_node, existing_clip = project.add_project_raster(realization, LayerTypes['EXVEG'])
    historic_clip_node, historic_clip = project.add_project_raster(realization, LayerTypes['HISTVEG'])

    # Download the four digit NHD archive containing the flow lines and watershed boundaries
    log.info('Processing NHD')

    nhd_download_folder = os.path.join(download_folder, 'nhd', huc[:4])
    nhd_unzip_folder = os.path.join(scratch_dir, 'nhd', huc[:4])

    nhd, db_path, huc_name = clean_nhd_data(huc, nhd_download_folder, nhd_unzip_folder, os.path.join(output_folder, 'hydrology'), cfg.OUTPUT_EPSG, False)
    project.add_metadata({'Watershed': huc_name})

    precip = mean_area_precip(nhd['WBDHU{}'.format(len(huc))], prism)
    calculate_bankfull_width(nhd['NHDFlowline'], precip)

    # Add the DB record to the Project XML
    db_lyr = RSLayer('NHD Tables', 'NHDTABLES', 'SQLiteDB', os.path.relpath(db_path, output_folder))
    project.add_dataset(realization, db_path, db_lyr, 'SQLiteDB')

    # Add any results to project XML
    for name, file_path in nhd.items():
        lyr_obj = RSLayer(name, name, 'Vector', os.path.relpath(file_path, output_folder))
        project.add_project_vector(realization, lyr_obj)

    boundary = 'WBDHU{}'.format(len(huc))
    states = get_nhd_states(nhd[boundary])

    # Download the NTD archive containing roads and rail
    log.info('Processing NTD')
    ntd_raw = {}
    for state, ntd_url in get_ntd_urls(states).items():
        ntd_download_folder = os.path.join(download_folder, 'ntd', state.lower())
        ntd_unzip_folder = os.path.join(scratch_dir, 'ntd', state.lower(), 'unzipped')  # a little awkward but I need a folder for this and this was the best name I could find
        ntd_raw[state] = download_shapefile_collection(ntd_url, ntd_download_folder, ntd_unzip_folder, force_download)

    ntd_clean = clean_ntd_data(ntd_raw, nhd['NHDFlowline'], nhd[boundary], os.path.join(output_folder, 'transportation'), cfg.OUTPUT_EPSG)

    # Write transportation layers to project file
    log.info('Write transportation layers to project file')

    # Add any results to project XML
    for name, file_path in ntd_clean.items():
        lyr_obj = RSLayer(name, name, 'Vector', os.path.relpath(file_path, output_folder))
        project.add_project_vector(realization, lyr_obj)

    # Download the HAND raster
    huc6 = huc[0:6]
    hand_download_folder = os.path.join(download_folder, 'hand')
    download_hand(huc6, cfg.OUTPUT_EPSG, hand_download_folder, nhd[boundary], hand_raster)

    # download contributing DEM rasters, mosaic and reproject into compressed GeoTIF
    ned_download_folder = os.path.join(download_folder, 'ned')
    ned_unzip_folder = os.path.join(scratch_dir, 'ned')
    dem_rasters = download_dem(nhd[boundary], cfg.OUTPUT_EPSG, 0.01, ned_download_folder, ned_unzip_folder, force_download)

    need_dem_rebuild = force_download or not os.path.exists(dem_raster)
    if need_dem_rebuild:
        raster_vrt_stitch(dem_rasters, dem_raster, cfg.OUTPUT_EPSG, nhd[boundary])
        verify_areas(dem_raster, nhd[boundary])

    # Calculate slope rasters seperately and then stitch them
    slope_parts = []
    hillshade_parts = []

    need_slope_build = need_dem_rebuild or not os.path.isfile(slope_raster)
    need_hs_build = need_dem_rebuild or not os.path.isfile(hill_raster)

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

    if (need_slope_build):
        raster_vrt_stitch(slope_parts, slope_raster, cfg.OUTPUT_EPSG, nhd[boundary])
        verify_areas(slope_raster, nhd[boundary])
    else:
        log.info('Skipping slope build because nothing has changed.')

    if (need_hs_build):
        raster_vrt_stitch(hillshade_parts, hill_raster, cfg.OUTPUT_EPSG, nhd[boundary])
        verify_areas(hill_raster, nhd[boundary])
    else:
        log.info('Skipping hillshade build because nothing has changed.')

    # Calculate flow accumulation raster based on the DEM
    log.info('Running flow accumulation and converting to drainage area.')
    flow_accumulation(dem_raster, flow_accum, dinfinity=False, pitfill=True)
    flow_accum_to_drainage_area(flow_accum, drain_area)

    # Clip and re-project the existing and historic vegetation
    log.info('Processing existing and historic vegetation rasters.')
    raster_warp(existing_veg, existing_clip, cfg.OUTPUT_EPSG, nhd[boundary])
    raster_warp(historic_veg, historic_clip, cfg.OUTPUT_EPSG, nhd[boundary])

    # Clip the landownership Shapefile to a 10km buffer around the watershed boundary
    own_path = os.path.join(output_folder, 'ownership', 'ownership.shp')
    project.add_dataset(realization, own_path, LayerTypes['OWNERSHIP'], 'Vector')
    clip_ownership(nhd[boundary], ownership, own_path, cfg.OUTPUT_EPSG, 10000)

    # Filter the ecoregions Shapefile to only include attributes that intersect with our HUC
    eco_path = os.path.join(output_folder, 'ecoregions', 'ecoregions.shp')
    project.add_dataset(realization, eco_path, LayerTypes['ECOREGIONS'], 'Vector')
    filter_ecoregions(nhd[boundary], ecoregions, eco_path, cfg.OUTPUT_EPSG, 10000)

    log.info('Process completed successfully.')
    return {
        'DEM': dem_raster,
        'Slope': slope_raster,
        'ExistingVeg': existing_veg,
        'HistoricVeg': historic_veg,
        'NHD': nhd
    }


def create_project(huc, output_dir):

    project_name = 'Riverscapes Context for HUC {}'.format(huc)
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'RSContext')

    project.add_metadata({
        'HUC{}'.format(len(huc)): str(huc)
    })

    realizations = project.XMLBuilder.add_sub_element(project.XMLBuilder.root, 'Realizations')
    realization = project.XMLBuilder.add_sub_element(realizations, 'RSContext', None, {
        'id': 'RSContext1',
        'dateCreated': datetime.datetime.now().isoformat(),
        'guid': str(uuid.uuid1()),
        'productVersion': cfg.version
    })
    project.XMLBuilder.add_sub_element(realization, 'Name', project_name)

    project.XMLBuilder.write()
    return project, realization


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
    parser.add_argument('existing', help='National existing vegetation raster', type=str)
    parser.add_argument('historic', help='National historic vegetation raster', type=str)
    parser.add_argument('ownership', help='National land ownership shapefile', type=str)
    parser.add_argument('ecoregions', help='National EcoRegions shapefile', type=str)
    parser.add_argument('prism', help='Prism Data Geopackage', type=str)
    parser.add_argument('output', help='Path to the output folder', type=str)
    parser.add_argument('download', help='Temporary folder for downloading data. Different HUCs may share this', type=str)
    parser.add_argument('--force', help='(optional) download existing files ', action='store_true', default=False)
    parser.add_argument('--temp_folder', help='(optional) cache folder for downloading files ', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("RS Context")
    log.setup(logPath=os.path.join(args.output, "rs_context.log"), verbose=args.verbose)
    log.title('Riverscapes Context For HUC: {}'.format(args.huc))

    log.info('HUC: {}'.format(args.huc))
    log.info('EPSG: {}'.format(cfg.OUTPUT_EPSG))
    log.info('Existing veg: {}'.format(args.existing))
    log.info('Historical veg: {}'.format(args.historic))
    log.info('Ownership: {}'.format(args.ownership))
    log.info('Output folder: {}'.format(args.output))
    log.info('Download folder: {}'.format(args.download))
    log.info('Force download: {}'.format(args.force))

    try:
        rs_context(args.huc, args.existing, args.historic, args.ownership, args.ecoregions, args.prism, args.output, args.download, args.temp_folder, args.force)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
