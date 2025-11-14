"""
Name:       Riverscapes Dynamics

Purpose:    Build a Riverscapes Dynamics project for a single New Zealand watershed

Setup:

Author:     Philip Bailey

Date:       1 Oct 2025
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
import re
import os
import sys
import argparse
import sqlite3
import traceback
from typing import Tuple, List
from datetime import datetime
import geopandas as gpd
import rasterio
import rasterio.mask
import numpy as np

from rscommons import Logger, ModelConfig, dotenv, initGDALOGRErrors
from rscommons.classes.rs_project import RSLayer, RSProject, RSMeta, RSMetaTypes
from rscommons.classes.vector_classes import VectorBase
from rscommons.raster_warp import raster_warp
from rscommons.util import safe_makedirs
from rscommons.vector_ops import copy_feature_class
from rsdynamics.__version__ import __version__

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)
cfg.OUTPUT_EPSG = 2193  # NZTM

initGDALOGRErrors()

# regex for epoch raster names
epoch_raster_pattern = r'mosaic_(\w*)_frequency_([0-9]{1,2})_.*_([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{4})-([0-9]{2})-([0-9]{2})\.tif'
classified_raster_pattern = r'.*_([0-9]{8})\.tif'


LayerTypes = {
    # key: (name, id, tag, relpath)
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'topography/dem_hillshade.tif'),
    'RSDYNAMICS_DGOS': RSLayer('Riverscapes Dynamics DGOs', 'RSDYNAMICS_DGOS', 'Geopackage', 'dgos/rsdynamics.gpkg', {
        'DGOS': RSLayer('DGOs', 'DGOS', 'Vector', 'vbet_dgos'),
    })
}


def rsdynamics(watershed_id: str, vbet_project_xml: str, thresholds: List[float], epochs: List[int], raster_folder: str, output_dir: str):
    """ Main function for running Riverscapes Dynamics"""

    log = Logger('RS Dynamics')

    rsd_gpkg = os.path.join(output_dir, 'dgos', 'rsdynamics.gpkg')
    if os.path.isfile(rsd_gpkg):
        log.info(f'Removing existing Riverscapes Dynamics GeoPackage: {rsd_gpkg}')
        os.remove(rsd_gpkg)

    vbet_full = copy_vbet_layer(vbet_project_xml, 'VBET_OUTPUTS', 'vbet_full', rsd_gpkg)
    vbet_dgos = copy_vbet_layer(vbet_project_xml, 'Intermediates', 'vbet_dgos', rsd_gpkg)

    input_rasters = []  # process_classified_rasters(raster_folder, output_dir, vbet_full)
    epoch_rasters = process_epoch_rasters(raster_folder, output_dir, vbet_full)
    __hillshade_raster = copy_hillshade(vbet_project_xml, output_dir)

    # Group the epoch rasters into sets of wet and alluvial rasters for stats calculation
    epoch_sets = get_raster_sets(epoch_rasters)

    # As we are calculating stats, keep track of the spatial views we need to create later
    spatial_view_attributes = []

    # Loop over each threshold (e.g. 0.68, 0.95) and calculate stats for each epoch raster set
    for threshold in thresholds:

        log.info(f'Calculating zonal stats for threshold: {threshold}')
        for epoch_key, epoch_data in epoch_sets.items():

            if epoch_data['epoch_length'] not in epochs:
                log.info(f'Skipping epoch {epoch_key} because its length ({epoch_data["epoch_length"]}) is not in the specified epochs list')
                continue

            if epoch_data['wet'] is None or epoch_data['active'] is None:
                log.warning(f'Skipping epoch {epoch_key} because either wet or active raster rasters are missing')
                continue

            att_prefix = f'{epoch_key}_{int(threshold*100)}pc'.replace('-', '_')
            log.info(f'Processing epoch: {epoch_key} to produce attribute with prefix: {att_prefix}')

            if epoch_data['epoch_length'] > 1:
                calc_raster_stats(epoch_data['active'], vbet_dgos, f'active_{att_prefix}', threshold, spatial_view_attributes)
                calc_raster_stats(epoch_data['wet'], vbet_dgos, f'wet_{att_prefix}', threshold, spatial_view_attributes)

            # TODO: Reintroduce stable
            # calc_stable_stats(vbet_dgos, att_prefix, threshold, spatial_view_attributes)

    # Create the output Riverscapes project.rs.xml based on the rscontext XML
    rsd_project = build_dynamics_project(vbet_project_xml, output_dir)
    nod_realization = rsd_project.add_realization('Riverscapes Dynamics', 'REALIZATION1', cfg.version)
    nod_inputs = rsd_project.XMLBuilder.add_sub_element(nod_realization, 'Inputs')
    nod_outputs = rsd_project.XMLBuilder.add_sub_element(nod_realization, 'Outputs')
    rsd_project.add_project_raster(nod_inputs, LayerTypes['HILLSHADE'])

    for i, classified_info in enumerate(input_rasters):
        raster_path, raster_name, raster_id = classified_info
        rel_layer_path = rsd_project.get_relative_path(raster_path)
        layer_rs = RSLayer(raster_name, raster_id, 'Raster', rel_layer_path)
        rsd_project.add_project_raster(nod_inputs, layer_rs)

    for i, epoch_info in enumerate(epoch_rasters):
        raster_path, raster_name, raster_id, __att_prefix, __epoch_data = epoch_info
        rel_layer_path = rsd_project.get_relative_path(raster_path)
        layer_rs = RSLayer(raster_name, raster_id, 'Raster', rel_layer_path)
        rsd_project.add_project_raster(nod_outputs, layer_rs)

    spatial_view_attributes = list(set(spatial_view_attributes))
    for view in spatial_view_attributes:
        spatial_view = create_spatial_view(rsd_gpkg, view)
        display_name = spatial_view.replace('vw_', '').replace('_', ' ').replace('veg', 'vegetation').replace('yr', ' year').title()
        LayerTypes['RSDYNAMICS_DGOS'].add_sub_layer(view, RSLayer(display_name, spatial_view, 'Vector', spatial_view))

    # This has to come after all the spatial views have been added
    rsd_project.add_project_geopackage(nod_outputs, LayerTypes['RSDYNAMICS_DGOS'])

    rsd_project.XMLBuilder.write()

    return rsd_project


def get_raster_sets(epoch_rasters: List[Tuple[str, str, str, str, dict]]) -> dict:
    """ 
    Reshape the rasters into the right structure for calculating stats. We need a pair
    of wet and alluvial rasters for each epoch.

    Returns a dict of epoch_key: {'wet': raster_path, 'active': raster_path}
    """

    stats_sets = {}
    for raster_path, __raster_name, __raster_id, _att_prefix, epoch_data in epoch_rasters:
        epoch_length = int(epoch_data['length'])
        epoch_start_year = int(epoch_data['start_year'])
        epoch_end_year = int(epoch_data['end_year'])
        epoch_key = f'{epoch_length}yr_{epoch_start_year}_{epoch_end_year}'
        if epoch_key not in stats_sets:
            stats_sets[epoch_key] = {
                'wet': None,
                'active': None,
                'epoch_length': epoch_length,
                'start_year': epoch_start_year,
                'end_year': epoch_end_year
            }

        if epoch_data['type'].lower() == 'wetted':
            stats_sets[epoch_key]['wet'] = raster_path
        elif epoch_data['type'].lower() == 'alluvial':
            stats_sets[epoch_key]['active'] = raster_path

    log = Logger('Raster Sets')
    log.info(f'Found {len(stats_sets)} epoch raster sets for stats calculation')

    # Verify that each set has both a water and active raster
    for epoch_key, raster_paths in stats_sets.items():
        if raster_paths['wet'] is None:
            log.warning(f'No wet raster found for epoch: {epoch_key}')
        if raster_paths['active'] is None:
            log.warning(f'No alluvial raster found for epoch: {epoch_key}')

    return stats_sets


def create_spatial_view(rsd_gpkg: str, attribute_name: str) -> str:
    """
    Create a spatial view for each of the statistics from each epoch raster
    in the Riverscapes Dynamics GeoPackage.

    This makes it easier to visualize the statistics in QGIS because we can 
    reuse symbology files that expect a single 'value' field.
    """

    log = Logger('Spatial View')

    # Deconstruct the epoch characteristics
    match = re.match(r'(active|wet|stable)(.*)_([0-9]{2})pc_(.*)', attribute_name)
    if not match:
        raise ValueError(f"Invalid attribute name format: {attribute_name}")
    stat_type = match.group(1)  # wet, active, stable
    stat_epoch = match.group(2)  # 5yr-2000-2005
    stat_measure = match.group(3)  # 68, 95
    stat_metric = match.group(4)  # area, pc, width

    # Create spatial views for each of the statistics from each epoch raster
    view_name = f'vw_{stat_metric}_{stat_measure}_{stat_type}_{stat_epoch}'.replace('-', '_').replace('__', '_')
    log.info(f'Creating spatial view: {view_name} for attribute: {attribute_name}')

    with sqlite3.connect(rsd_gpkg) as conn:
        c = conn.cursor()

        c.execute(f'DROP VIEW IF EXISTS {view_name}')
        c.execute(f'CREATE VIEW {view_name} AS SELECT fid, geom, "{attribute_name}" AS value FROM vbet_dgos')

        c.execute(f"""
            INSERT INTO gpkg_contents (
                table_name,
                data_type,
                identifier,
                min_x,
                min_y,
                max_x,
                max_y,
                srs_id
            ) SELECT
                '{view_name}',
                data_type,
                '{view_name}',
                min_x,
                min_y,
                max_x,
                max_y,
                srs_id
            FROM gpkg_contents
            WHERE table_name='vbet_dgos'
        """)

        c.execute(f"""
            INSERT INTO gpkg_geometry_columns (
                table_name,
                column_name,
                geometry_type_name,
                srs_id,
                z,
                m
            ) SELECT
                '{view_name}',
                'geom',
                geometry_type_name,
                srs_id,
                z,
                m
            FROM gpkg_geometry_columns
            WHERE table_name='vbet_dgos'
        """)

        conn.commit()
    return view_name


def copy_vbet_layer(vbet_project_xml: str, gpkg_id: str, layer_name: str, output_gpkg: str) -> str:
    """
    Copy a specific VBET layer to the output folder.
    """

    vbet_project = RSProject(None, vbet_project_xml)
    nod_gpkg = vbet_project.XMLBuilder.find_by_id(gpkg_id)
    if nod_gpkg is None:
        raise Exception(f'No {gpkg_id} layer found in the input VBET project')
    rel_gpkg_path = nod_gpkg.find('Path').text
    vbet_gpkg = os.path.join(os.path.dirname(vbet_project_xml), rel_gpkg_path)

    vbet_layer_path = os.path.join(vbet_gpkg, layer_name)
    rsd_layer_path = os.path.join(output_gpkg, layer_name)

    copy_feature_class(vbet_layer_path, rsd_layer_path)
    return rsd_layer_path


def build_dynamics_project(vbet_project_xml: str, output_dir: str) -> RSProject:
    """
    Create a new Riverscapes project for Riverscapes Dynamics based on the input
    Riverscapes Context project.
    """

    # Create the output Riverscapes project
    vbet_project = RSProject(None, vbet_project_xml)
    vbet_project_name = vbet_project.XMLBuilder.find('Name').text
    vbet_metadata = vbet_project.get_metadata()
    vbet_id = vbet_project.XMLBuilder.find('Warehouse').attrib.get('id', None)

    rs_meta = [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/rsdynamics', meta_type=RSMetaTypes.URL, locked=True),
        RSMeta('Model Version', __version__, locked=True),
        RSMeta('Date Created', datetime.now().isoformat(), locked=True),
        RSMeta('VBET Input',  f'https://data.riverscapes.net/p/{vbet_id}', meta_type=RSMetaTypes.URL, locked=True)
    ]

    for k, v in vbet_metadata.items():
        if k in ['HUC', 'Hydrologic Unit Code']:
            rs_meta.append(v)
        elif 'rscontextnz' in k.lower() or 'taudem' in k.lower() or 'channelarea' in k.lower():
            rs_meta.append(v)

    # Build the Riverscapes Context project
    project_name = vbet_project_name.replace('VBET', 'Riverscapes Dynamics')
    rsd_project = RSProject(cfg, output_dir)
    rsd_project.create(project_name, 'rsdynamics', meta=rs_meta)

    # Reuse the project extents from the input Riverscapes Context project
    rsd_project.rs_copy_project_extents(vbet_project_xml)

    return rsd_project


def copy_hillshade(rscontext_xml: str, output_dir: str) -> str:
    """
    Copy the DEM hillshade from the input Riverscapes Context project to the output folder.
    """

    log = Logger('DEM Hillshade')

    rsc_project = RSProject(None, rscontext_xml)
    nod_hillshade = rsc_project.XMLBuilder.find_by_id('HILLSHADE')
    if nod_hillshade is None:
        log.warning('No DEM Hillshade found in the input Riverscapes Context project')
        return

    input_hillshade = os.path.join(os.path.dirname(rscontext_xml), nod_hillshade.find('Path').text)
    output_hillshade = os.path.join(output_dir, 'topography', 'dem_hillshade.tif')
    safe_makedirs(os.path.dirname(output_hillshade))
    raster_warp(input_hillshade, output_hillshade, cfg.OUTPUT_EPSG)
    return output_hillshade


def process_classified_rasters(raster_folder: str, output_dir: str, dgos: str) -> List[Tuple[str, str, str]]:
    """
    Copy the input classified rasters for alluvial, vegetated, wetted to the output folder
    and clip them to the DGO polygons.

    Returns tuple of (raster_path, raster plain english display name, project raster ID)
    """

    log = Logger('Classified Rasters')

    raster_paths = []
    for class_type in ['01_wetted', '00_alluvial']:
        sub_dir = os.path.join('classified_scenes', class_type)
        freq_dir = os.path.join(raster_folder, sub_dir)
        if not os.path.exists(freq_dir):
            log.warning(f'Classified raster folder not found: {freq_dir}')
            continue

        for raster_file in os.listdir(freq_dir):
            if not raster_file.endswith('.tif'):
                continue

            match = re.match(classified_raster_pattern, raster_file)
            if not match:
                log.warning(f'Raster file does not match pattern: {raster_file}')
                continue

            raster_date_str = match.group(1)
            raster_class = class_type.split('_')[1]  # wetted or alluvial

            # parse the date as yyyymmdd
            try:
                raster_date = datetime.strptime(raster_date_str, '%Y%m%d').date()
            except ValueError:
                log.warning(f'Invalid raster date format: {raster_date_str} in file {raster_file}')
                continue

            raster_name = f'Classified Binary - {raster_class.capitalize()} - {raster_date.strftime("%d %b %Y")}'
            raster_id = f'CLASS_{raster_class.upper()}_{raster_date.strftime("%Y%m%d")}'

            out_raster = os.path.join(output_dir, sub_dir, os.path.basename(raster_file))
            safe_makedirs(os.path.dirname(out_raster))
            raster_warp(os.path.join(freq_dir, raster_file), out_raster, cfg.OUTPUT_EPSG, dgos)
            raster_paths.append((out_raster, raster_name, raster_id))

    return raster_paths


def process_epoch_rasters(raster_folder: str, output_dir: str, vbet_dgos: str) -> List[Tuple[str, str, str, str, dict]]:
    """
    Copy the epoch rasters (5yr, 30yr etc) to the output folder and clip them to the
    DGO ppolygons.

    Returns list of raster info tuple (
        raster_path,
        raster plain english display name,
        project raster ID,
        attribute prefix for DGO feature class
    )
    """

    log = Logger('Epoch Rasters')

    raster_paths = []
    epoch_parent_dir = os.path.join(raster_folder, 'frequency_outputs')
    if not os.path.exists(epoch_parent_dir):
        log.warning(f'Frequency outputs folder not found: {epoch_parent_dir}')
    else:
        # Get the subfolders that refer to each epoch
        for epoch in os.listdir(epoch_parent_dir):
            epoch_dir = os.path.join(epoch_parent_dir, epoch)
            if not os.path.isdir(epoch_dir):
                continue

            for raster_file in os.listdir(epoch_dir):
                if not raster_file.endswith('.tif'):
                    continue

                # Match the epoch pattern to get the epoch name
                match = re.match(epoch_raster_pattern, os.path.basename(raster_file))
                if match:
                    epoch_type = match.group(1)
                    epoch_length = match.group(2)
                    epoch_start_year = match.group(3)
                    epoch_end_year = match.group(6)
                    epoch_prefix = 'veg' if epoch_type == 'vegetation' else epoch_type

                    att_prefix = f'{epoch_length}yr_{epoch_prefix}_{epoch_start_year}_{epoch_end_year}'
                    epoch_data = {
                        'type': epoch_type,
                        'length': epoch_length,
                        'start_year': epoch_start_year,
                        'end_year': epoch_end_year
                    }
                else:
                    log.warning(f'Could not parse epoch from raster name: {epoch}')
                    continue

                out_raster = os.path.join(output_dir, 'frequency_outputs', epoch, os.path.basename(raster_file))
                safe_makedirs(os.path.dirname(out_raster))
                raster_warp(os.path.join(epoch_dir, raster_file), out_raster, cfg.OUTPUT_EPSG, vbet_dgos)

                raster_name = f'Epoch Raster - {epoch_type.capitalize()} - {epoch_length} yr{"s" if int(epoch_length) > 1 else ""} - {epoch_start_year}-{epoch_end_year}'
                raster_id = f'EPOCH_{epoch_type.upper()}_{epoch_length}yr_{epoch_start_year}_{epoch_end_year}'
                raster_paths.append((out_raster, raster_name, raster_id, att_prefix, epoch_data))

    return raster_paths


def calc_raster_stats(raster_path: str, polygon_path: str, prefix: str, threshold: float, spatial_view_attributes: list) -> None:
    """
    Calculate zonal statistics for a raster based on polygons in a vector file.
    The stats are added as new columns to the input vector file and saved to a new
    output file (GeoPackage or Shapefile).
    """

    log = Logger('Raster Stats')
    log.info(f'Calculating raster stats for {prefix} at {raster_path}')

    # Load polygons from the feature class that is already in the RS Dynamics project
    gpkg, layer = VectorBase.path_sorter(polygon_path)
    gdf = gpd.read_file(gpkg, layer=layer)

    # Make lists to hold the stats
    stats = {
        "area": [],
        "areapc": [],
        "width": [],
        "widthpc": []
    }

    # Set up the columns in the GeoDataFrame
    # Specify which spatial views are needed for the attributes
    for stat_key in stats:
        attribute_name = f'{prefix}_{stat_key}'
        gdf[attribute_name] = np.nan
        spatial_view_attributes.append(attribute_name)

    # Open the raster and calculate stats for each polygon using a mask
    with rasterio.open(raster_path) as src:
        for _, row in gdf.iterrows():

            # Get the area of the polygon
            dgo_area = row.geometry.area
            dgo_length = row['centerline_length']
            dgo_width = dgo_area / dgo_length if dgo_length != 0 else 0
            # dgo_areas.append(dgo_area)

            out_image, __out_transform = rasterio.mask.mask(src, [row.geometry], crop=True)
            data = out_image[0]
            data = data[data != src.nodata]
            data = data[~np.isnan(data)]
            if data.size > 0:

                # Count the number of pixels with value 0.95 or above
                count = np.sum(data >= threshold)
                raster_area = float(count) * (src.res[0] * src.res[1])
                stats['area'].append(raster_area)

                row[f'{prefix}_area'] = raster_area

                areapc = (raster_area / dgo_area) * 100 if dgo_area != 0 and raster_area != 0 else np.nan
                areapc = min(areapc, 100.0)
                areapc = max(areapc, 0.0)
                stats['areapc'].append(areapc)

                width = raster_area / dgo_length if dgo_length != 0 else np.nan
                stats['width'].append(width)

                widthpc = (width / dgo_width) * 100 if dgo_width != 0 and width != 0 else np.nan
                widthpc = min(widthpc, 100.0)
                widthpc = max(widthpc, 0.0)
                stats['widthpc'].append(widthpc)
            else:
                for key, __value in stats.items():
                    stats[key].append(np.nan)

    # Assign the stats back to the GeoDataFrame
    for stat_key, values in stats.items():
        attribute_name = f'{prefix}_{stat_key}'
        gdf[attribute_name] = values

    # Write back to GeoPackage
    gdf.to_file(gpkg, layer=layer, driver="GPKG")


def calc_stable_stats(vbet_dgos: str, att_prefix: str, threshold: float, spatial_view_attributes: list) -> None:
    """
    Calculate the stable area as the residual area after subtracting wetted,
    and alluvial areas from the total DGO area."""

    # Load polygons from the feature class that is already in the RS Dynamics project
    gpkg, layer = VectorBase.path_sorter(vbet_dgos)
    gdf = gpd.read_file(gpkg, layer=layer)

    stable_areas = []
    stable_area_pcs = []
    stable_widths = []
    for _, row in gdf.iterrows():
        dgo_area = row.geometry.area
        dgo_length = row['centerline_length']

        if f'wet_{att_prefix}_area' not in row or f'active_{att_prefix}_area' not in row:
            stable_areas.append(np.nan)
            stable_area_pcs.append(np.nan)
            stable_widths.append(np.nan)
            continue

        wetted_area = row[f'wet_{att_prefix}_area']
        alluvial_area = row[f'active_{att_prefix}_area']

        if np.isnan(wetted_area) or np.isnan(alluvial_area):
            stable_areas.append(np.nan)
            stable_area_pcs.append(np.nan)
            stable_widths.append(np.nan)
            continue

        stable_area = dgo_area - (wetted_area + alluvial_area)
        stable_areas.append(stable_area)

        stable_area_pc = (stable_area / dgo_area * 100) if dgo_area != 0 else 0
        stable_area_pc = min(stable_area_pc, 100.0)
        stable_area_pc = max(stable_area_pc, 0.0)
        stable_area_pcs.append(stable_area_pc)

        stable_width = stable_area / dgo_length if dgo_length != 0 else 0
        stable_widths.append(stable_width)

    gdf[f'stable_{att_prefix}_area'] = stable_areas
    gdf[f'stable_{att_prefix}_pc'] = stable_area_pcs
    gdf[f'stable_{att_prefix}_width'] = stable_widths

    spatial_view_attributes.append(f'stable_{att_prefix}_area')
    spatial_view_attributes.append(f'stable_{att_prefix}_pc')
    spatial_view_attributes.append(f'stable_{att_prefix}_width')

    gdf.to_file(gpkg, layer=layer, driver="GPKG")


def main():
    """ Main entry point for New Zealand RS Context"""
    parser = argparse.ArgumentParser(description='Riverscapes Context Tool for New Zealand')
    parser.add_argument('watershed_id', help='Watershed/HUC identifier', type=int)
    parser.add_argument('vbet_project_xml', help='Path to VBET project XML file.', type=str)
    parser.add_argument('thresholds', help='Comma-separated list of thresholds to process (e.g. 0.68,0.95)', type=str)
    parser.add_argument('epochs', help='Comma-separated list of epochs to process (e.g. 1,5,30)', type=str)
    parser.add_argument('raster_folder', help='Path to the folder containing the river dynamics raster files', type=str)
    parser.add_argument('output_dir', help='Path to the output folder', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    log = Logger('RS Dynamics')
    log.setup(logPath=os.path.join(args.output_dir, 'rsdynamics.log'), verbose=args.verbose)
    log.title(f'Riverscapes Dynamics For Watershed: {args.watershed_id}')

    log.info(f'Watershed ID: {args.watershed_id}')
    log.info(f'Model Version: {__version__}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')
    log.info(f'Output folder: {args.output_dir}')
    log.info(f'Thresholds: {args.thresholds}')
    log.info(f'Epochs: {args.epochs}')

    thresholds_list = [float(x) for x in args.thresholds.split(',')]
    epochs_list = [int(x) for x in args.epochs.split(',')]

    try:
        rsdynamics(args.watershed_id, args.vbet_project_xml, thresholds_list, epochs_list, args.raster_folder, args.output_dir)
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
