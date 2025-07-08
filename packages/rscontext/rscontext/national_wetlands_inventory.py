'''
Download National Wetlands Inventory (NWI) for a given HUC8 and clip all the available
ShapeFiles to a HUC10 GeoPackage.

NWI data are available online as either HUC8 or US State zip files. This script
downloads the HUC8 zip file, unzips it, and then clips all the ShapeFiles to a
HUC10 boundary layer. The output is a GeoPackage containing the clipped layers.

Philip Bailey
May 2025
'''
from typing import List
import os
import zipfile
import argparse
import requests
from osgeo import ogr
from shapely.wkb import loads as wkbload
from rscommons import Logger, dotenv, get_shp_or_gpkg
from rscommons.util import safe_makedirs
from rscommons.vector_ops import copy_feature_class, get_geometry_unary_union
from rscommons.classes.vector_base import VectorBase

NWI_BASE_URL = 'https://documentst.ecosphere.fws.gov/wetlands/downloads/watershed/HU8_{}_Watershed.zip'


def national_wetlands_inventory(huc10: str, download_dir: str, clip_layer_path: str, output_gpkg: str, output_epsg: int, download_timeout: int = 120) -> List[str]:
    '''
    Download National Wetlands Inventory data for a given HUC8 watershed,
    unzip the data, and clip all available ShapeFiles to a HUC10 boundary layer.

    :param huc10: HUC10 code for the watershed.
    :param download_dir: Directory for downloading and unzipping data.
    :param clip_layer_path: Path to the HUC10 boundary layer (must be a valid feature class).
    :param output_gpkg: Output GeoPackage path where clipped NWI layers will be saved.
    :param output_epsg: Output spatial reference EPSG code (default is 4326).
    :param download_timeout: Timeout for the download request in seconds (default is 120).
    :return: List of output layer paths in the GeoPackage.
    :raises ValueError: If the HUC10 code is not all digits.
    :raises FileNotFoundError: If the clip layer path or output GeoPackage path does not exist.
    :raises NotADirectoryError: If the download directory does not exist.
    :raises requests.exceptions.RequestException: If there is an error during the download request.
    '''

    log = Logger('NWI')

    if not huc10.isdigit():
        log.error(f'HUC10 code must be 10 digits long, got: {huc10}')
        raise ValueError('HUC10 code must be 10 characters long.')
    huc8 = huc10[:8]

    if not os.path.isdir(os.path.dirname(clip_layer_path)):
        log.error(f'Clip layer path does not exist: {clip_layer_path}')
        raise FileNotFoundError(f'Clip layer path does not exist: {clip_layer_path}')

    # Load the geometry unary union of the HUC10 boundary layer.
    # Do this before attempting to download the NWI data or cluttering disk.
    # Need the spatial ref because the NWI data is in a different projection.
    log.info(f'Loading HUC10 boundary layer from {clip_layer_path}')
    raw_huc10_boundary = get_geometry_unary_union(clip_layer_path, output_epsg)
    with get_shp_or_gpkg(clip_layer_path) as huc10_layer:
        huc10_spatial_ref = huc10_layer.spatial_ref

    nwi_url = NWI_BASE_URL.format(huc8)
    log.info(f'National Wetlands Inventory URL for HUC8 {huc8}: {nwi_url}')

    # Download HUC8 NWI data
    save_path = os.path.join(download_dir, f'HU8_{huc8}_Watershed.zip')
    log.info(f'Downloading NWI data to {save_path}')
    try:
        safe_makedirs(os.path.dirname(save_path))
        response = requests.get(nwi_url, stream=True, timeout=download_timeout)
        response.raise_for_status()

        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        log.info(f'Downloaded NWI data to {save_path}')
    except requests.exceptions.RequestException as e:
        log.error(f'Error downloading NWI data: {e}')
        raise

    unzip_dir = os.path.join(download_dir, 'unzip')
    safe_makedirs(unzip_dir)
    log.info(f'Unzipping NWI data to {unzip_dir}')
    with zipfile.ZipFile(save_path, 'r') as zip_ref:
        zip_ref.extractall(unzip_dir)

    # loop over the ShapeFiles files in the unzipped directory
    shapefile_path = None
    output_layers = []
    for dirpath, _dirnames, filenames in os.walk(unzip_dir):
        for filename in filenames:
            if filename.lower().endswith('.shp'):
                shapefile_path = os.path.join(dirpath, filename)
                log.info(f'Found shapefile: {shapefile_path}')
                layer_name = os.path.splitext(os.path.basename(shapefile_path))[0]

                # Remove the HUC8 from the layer name to make it predictable
                if layer_name.startswith(f'HU8_{huc8}_'):
                    layer_name = layer_name[len(f'HU8_{huc8}_'):]
                elif layer_name.startswith(f'HU8{huc8}_'):
                    layer_name = layer_name[len(f'HU8{huc8}_'):]
                elif layer_name.startswith(f'HU8{huc8}'):
                    layer_name = layer_name[len(f'HU8{huc8}'):]

                # Transform the HUC10 boundary to the NWI layer's spatial reference
                with get_shp_or_gpkg(shapefile_path) as nwi_layer:
                    transform = VectorBase.get_transform(huc10_spatial_ref, nwi_layer.spatial_ref)
                    proj_huc10_boundary_ogr = ogr.CreateGeometryFromWkb(raw_huc10_boundary.wkb)
                    proj_huc10_boundary_ogr.Transform(transform)
                    proj_huc10_shapely = wkbload(bytes(proj_huc10_boundary_ogr.ExportToWkb()))

                output_layer_path = os.path.join(output_gpkg, layer_name)
                log.info(f'Creating output layer: {output_layer_path}')
                try:
                    copy_feature_class(shapefile_path, output_layer_path, output_epsg, clip_shape=proj_huc10_shapely, make_valid=True)
                    output_layers.append(output_layer_path)
                except Exception as e:
                    log.error(f'Error creating output layer {output_layer_path}: {e}')

    return output_layers


def main():
    '''
    Download NWI for a given HUC8 watershed and clip to HUC10 boundary.
    '''
    parser = argparse.ArgumentParser(description='National Wetlands Inventory')
    parser.add_argument('huc10', help='HUC10 code for the watershed', type=str)
    parser.add_argument('download_dir', help='Directory for downloading and unzipping data', type=str)
    parser.add_argument('clip_layer_path', help='Path to the HUC10 boundary layer.', type=str)
    parser.add_argument('output_gpkg', help='Output GeoPackage path', type=str)
    parser.add_argument('--output_epsg', help='Output spatial reference EPSG', default=4326, type=int)
    args = dotenv.parse_args_env(parser)

    log = Logger('NWI')
    log.setup(logPath=os.path.join(os.path.dirname(args.output_gpkg), 'nwi.log'), verbose=True)
    log.title(f'NWI for HUC10: {args.huc10}')

    nwi_url = national_wetlands_inventory(args.huc10, args.download_dir, args.clip_layer_path, args.output_gpkg, args.output_epsg)
    print(f'National Wetlands Inventory URL: {nwi_url}')


if __name__ == "__main__":
    main()
