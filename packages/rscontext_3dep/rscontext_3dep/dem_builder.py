#!/usr/bin/env python3
# Name:     DEM Builder
#
# Purpose:  Take a polygon and download all the necessary DEM tiles to create it.
#           Mosaic them together and produce a single DEM GeoTiFF at specified resolution.
#           And build a Riverscapes project with it.
#
# Author:   Lorin Gaertner
#
# Date:     28 Apr 2025
# -------------------------------------------------------------------------------
import argparse
import json
import os
import sys
import traceback
import uuid
from collections import Counter
from osgeo import gdal, osr

from rscommons import (Logger, dotenv, initGDALOGRErrors, ModelConfig, RSProject, RSLayer)
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons.download_dem import download_dem, verify_areas
from rscommons.geographic_raster import gdal_dem_geographic
from rscommons.project_bounds import generate_project_extents_from_layer
from rscommons.raster_warp import raster_vrt_stitch
from rscommons.util import safe_makedirs, safe_remove_dir, safe_remove_file
from rscontext_3dep.__version__ import __version__

initGDALOGRErrors()

cfg = ModelConfig(
    'https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LayerTypes = {
    # key: RSLayer(name, id, tag, relpath)
    'DEM': RSLayer('3DEP 1m DEM', '3DEPDEM', 'Raster', 'topography/dem.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'topography/dem_hillshade.tif'),
}


def get_epsg(raster_path: str) -> int | None:
    """
    Gets the EPSG code from a raster file using GDAL.

    Also outputs some extra CRS info via logging at the DEBUG level.

    Args:
        raster_path (str): Path to the raster file.

    Returns:
        int | None: EPSG code for the CRS if found and identifiable, otherwise None.
    """
    log = Logger("get_epsg")
    dataset = None  # Initialize dataset to None to ensure it exists for finally block

    try:
        # Optional: Enable GDAL exceptions (might be cleaner than checking return codes)
        # gdal.UseExceptions()

        dataset = gdal.Open(raster_path, gdal.GA_ReadOnly)

        if dataset is None:
            log.error(f"Could not open file {raster_path} with GDAL.")
            # If exceptions aren't enabled, check GDAL error message if needed
            # log.error(f"GDAL Error: {gdal.GetLastErrorMsg()}")
            return None  # Dataset is None, finally block handles it

        # log.debug(f"Successfully opened: {raster_path} with GDAL")

        wkt_projection = dataset.GetProjection()

        if wkt_projection:
            # log.debug("CRS Found.")  # Corrected typo from debut to debug
            # log.debug(f"WKT:\n{wkt_projection}")

            srs = osr.SpatialReference()
            # Handle potential errors during WKT import
            if srs.ImportFromWkt(wkt_projection) != 0:  # Returns 0 on success
                log.error(f"Failed to import WKT for {raster_path}")
                return None  # Cannot proceed without valid SRS

            srs.AutoIdentifyEPSG()
            # GDAL returns authority code as string
            authority_code_str = srs.GetAuthorityCode(None)

            if authority_code_str:
                authority_name = srs.GetAuthorityName(None)
                log.debug(f"Identified Authority: {authority_name}, Code: {authority_code_str}")
                try:
                    # Convert string code to integer for return
                    epsg_code_int = int(authority_code_str)
                    log.info(f"Returning EPSG: {epsg_code_int} for {raster_path}")
                    return epsg_code_int  # Return the integer EPSG code
                except ValueError:
                    log.error(f"Could not convert identified authority code '{authority_code_str}' to an integer.")
                    return None  # Return None as we couldn't get an integer code
            else:
                log.warning(f"EPSG Code: Could not automatically identify from WKT for {raster_path}.")
                # Log extra details that might help diagnose why
                log.debug(f"  Is Projected? {srs.IsProjected()}")
                log.debug(f"  Is Geographic? {srs.IsGeographic()}")
                proj_name = srs.GetAttrValue('PROJCS')
                geog_name = srs.GetAttrValue('GEOGCS')
                if proj_name:
                    log.debug(f"  Projected CS Name: {proj_name}")
                if geog_name:
                    log.debug(f"  Geographic CS Name: {geog_name}")
                return None  # No EPSG code identified

        else:
            log.warning(f"No CRS/projection information found in the file: {raster_path}")
            return None  # No WKT found

    except Exception as e:
        # Log any unexpected exceptions during the process
        log.error(f"An unexpected error occurred while processing {raster_path}: {e}", traceback.format_exc())  # Add stack trace
        return None  # Return None on unexpected error

    finally:
        # This block ALWAYS runs, ensuring the dataset is closed (dereferenced)
        if dataset is not None:
            # log.debug(f"Closing GDAL dataset for: {raster_path}")
            dataset = None
        # else:
            # log.debug("GDAL dataset was already None or not opened.")
        # If you used PushErrorHandler, Pop it here:
        # gdal.PopErrorHandler()


def get_best_crs(raster_paths: list[str]) -> int | None:
    """
    Determines the Coordinate Reference System (EPSG) that best represents the supplied rasters.

    Checks the CRS of all inputs in the supplied list by calling get_epsg().
    Returns the EPSG code used by the majority of the rasters for which an EPSG
    could be determined. If there's a tie for the majority, it returns the
    lowest EPSG number among the tied codes.

    Args:
        raster_paths (list[str]): List of paths to raster files to check.

    Returns:
        Optional[int]: The EPSG code (integer) that represents the best fit
                       for this set of rasters, or None if no valid EPSG codes
                       could be determined from any input raster.
    """
    log = Logger('get_best_crs')
    if not raster_paths:
        log.warning("Input raster_paths list is empty.")
        return None

    raster_codes = []
    log.info(f"Checking EPSG codes for {len(raster_paths)} raster(s)...")
    for i, raster_path in enumerate(raster_paths):
        log.debug(f"Processing raster {i+1}/{len(raster_paths)}: {raster_path}")
        epsg_code = get_epsg(raster_path)  # Call the previously defined function
        raster_codes.append(epsg_code)
        # Optional: Log intermediate results
        # if epsg_code is not None:
        #     log.debug(f"  -> Found EPSG: {epsg_code}")
        # else:
        #     log.debug(f"  -> EPSG not found or identifiable.")

    # Filter out None values (where EPSG couldn't be determined)
    valid_codes = [code for code in raster_codes if code is not None]

    if not valid_codes:
        log.error("Could not determine a valid EPSG code for any of the input rasters.")
        return None

    # Count the frequency of each valid EPSG code
    code_counts = Counter(valid_codes)
    log.debug(f"Counts of valid EPSG codes found: {dict(code_counts)}")  # Log the counts

    # Find the maximum frequency
    max_frequency = code_counts.most_common(1)[0][1]  # Gets the count of the most common item

    # Find all codes that have this maximum frequency
    majority_codes = [code for code, freq in code_counts.items() if freq == max_frequency]

    # Apply the tie-breaking rule: return the lowest EPSG number among the most frequent ones
    if len(majority_codes) == 1:
        best_epsg = majority_codes[0]
        log.debug(f"Majority EPSG code found: {best_epsg} (Frequency: {max_frequency})")
    else:
        best_epsg = min(majority_codes)  # Tie-breaker: choose the smallest EPSG code
        log.debug(f"Tie detected for majority frequency ({max_frequency}). Choosing lowest EPSG: {best_epsg} from tied codes {sorted(majority_codes)}.")

    return best_epsg


def is_geographic_epsg(epsg_code: int) -> bool:
    """
    Determines if the given EPSG code corresponds to a geographic coordinate system.

    Args:
        epsg_code (int): EPSG code to check.

    Returns:
        bool: True if the EPSG is geographic, False otherwise.
    """
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg_code)
    return srs.IsGeographic() == 1


def should_resample(dem_rasters: list[str], output_res: float, threshold: float = 0.1) -> bool:
    """
    Determines whether resampling is necessary based on the resolution of input rasters.

    Args:
        dem_rasters (list[str]): List of paths to input DEM rasters.
        output_res (float): Desired output resolution in meters.
        threshold (float): Relative difference threshold (default: 0.1, i.e., 10%).

    Returns:
        bool: True if resampling is necessary, False otherwise.
    """
    log = Logger("Resolution Check")
    log.info(f"Checking resolution of source rasters vs desired target resolution ({output_res})")
    resolutions = []

    for raster_path in dem_rasters:
        dataset = gdal.Open(raster_path, gdal.GA_ReadOnly)
        if dataset is None:
            log.warning(f"Could not open raster: {raster_path}")
            continue

        # Get the geotransform to calculate pixel size
        geotransform = dataset.GetGeoTransform()
        if geotransform is None:
            log.warning(f"Could not retrieve geotransform for raster: {raster_path}")
            continue

        # Pixel size (resolution) is in geotransform[1] (x) and geotransform[5] (y, negative)
        pixel_size_x = abs(geotransform[1])
        pixel_size_y = abs(geotransform[5])

        # Assume square pixels and take the average resolution
        avg_resolution = (pixel_size_x + pixel_size_y) / 2
        resolutions.append(avg_resolution)

        log.debug(f"Raster: {raster_path}, Resolution: {avg_resolution:.2f}m")

    if not resolutions:
        log.error("No valid resolutions found for input rasters. Resampling will proceed by default.")
        return True

    # Calculate the average resolution of all input rasters
    avg_input_resolution = sum(resolutions) / len(resolutions)
    log.info(f"Average source resolution: {avg_input_resolution:.2f}m")

    # Check if the relative difference exceeds the threshold
    relative_difference = abs(avg_input_resolution - output_res) / avg_input_resolution
    log.info(f"Relative difference: {relative_difference:.2%}")

    if relative_difference <= threshold:
        log.info("Resampling is not necessary. Source resolution is close to the target resolution.")
        return False

    log.info("Resampling is necessary. Source resolution differs significantly from the target resolution.")
    return True


def dem_builder(bounds_path: str,  output_res: float, download_folder: str, scratch_dir: str, output_path: str, force_download: bool):
    """Build a mosaiced raster for input area from 3DEP 1m DEM then put it together in a Riverscapes Project
    Args:
        bounds_path (str): path to the layer of the area we are building 
        output_res (float): target resolution of the raster (downsample if needed)
        download_folder (str): where the source DEMs will go (outside of and not included in final project)
        scratch_dir (str): folder for unzipping files (outside of and not included in final project)
        output_path (str): path to folder where the outputs will go (project file, log file, and dem files will be placed in subfolder)
        force_download (bool): if True, download from source even if we already have local copy

    """
    log = Logger('DEM Builder')
    log.title('Prepare DEM and Hillshade')

    ned_download_folder = os.path.join(download_folder, 'ned')
    ned_unzip_folder = os.path.join(scratch_dir, 'ned')

    dem_rasters, dem_raster_source_urls = download_dem(bounds_path, None, 0.01, ned_download_folder, ned_unzip_folder, force_download, '1m')
    output_dem_file_path = os.path.join(output_path, LayerTypes['DEM'].rel_path)
    resample = should_resample(dem_rasters, output_res)
    # this forces rebuild if there is a resampling which isn't always needed but helps in case the only difference between two runs is the output_res
    need_dem_rebuild = force_download or not os.path.exists(output_dem_file_path) or resample

    output_epsg = get_best_crs(dem_rasters)
    if need_dem_rebuild:
        log.info('Building mosaiced DEM')
        if os.path.exists(output_dem_file_path):
            safe_remove_file(output_dem_file_path)
        warp_options = {"cutlineBlend": 1}
        if resample:
            warp_options.update({
                "xRes": output_res,
                "yRes": output_res,
                "resampleAlg": "bilinear"  # Use bilinear resampling for downsampling
            })

        raster_vrt_stitch(dem_rasters, output_dem_file_path, output_epsg, clip=bounds_path, warp_options=warp_options)
    else:
        log.info('Skipping DEM build as it already exists. Use force option to trigger rebuild anyway.')

    area_ratio = verify_areas(output_dem_file_path, bounds_path)
    if area_ratio < 0.85:
        log.warning(f'DEM data less than 85%% of bounds extent ({area_ratio:%})')
        # raise Exception(f'DEM data less than 85%% of nhd extent ({area_ratio:%})')

    # build hillshade
    hillshade_path = os.path.join(output_path, LayerTypes['HILLSHADE'].rel_path)
    need_hs_rebuild = need_dem_rebuild or not os.path.isfile(hillshade_path)
    if need_hs_rebuild:
        log.info('Building hillshade from DEM')
        if is_geographic_epsg(output_epsg):
            gdal_dem_geographic(output_dem_file_path, hillshade_path, 'hillshade')
        else:
            gdal.DEMProcessing(hillshade_path, output_dem_file_path, 'hillshade', creationOptions=["COMPRESS=DEFLATE"])
    else:
        log.info('Skipping hillshade build as one already exists. Use force option to trigger rebuild anyway.')

    log.info(f'Area Ratio: {area_ratio:%}')
    log.info(f'Output DEM: {output_dem_file_path}')
    log.info(f'Output DEM Size: {os.path.getsize(output_dem_file_path) / 1024 / 1024:.2f} MB')
    log.info(f'Output DEM Resolution: {output_res} m')
    log.info(f'Output DEM Projection: {output_epsg}')
    log.info('DEM building portion complete')

    # STEP 2. Build the Riverscapes project of type RSContext but with just the 3DEP 1m DEM products
    # This is a much simplified version of rs_context.rs_context

    log.title("RS Context 3DEP project builder")
    project_identifier = os.path.basename(bounds_path)

    project_name = f'Riverscapes Context-3DEP for {project_identifier}'
    project_description = 'Riverscapes Context-3DEP built from high resolution topographic data from USGS 3DEP Program. See dem_builder.log for additional details of the processing performed.'
    project = RSProject(cfg, output_path)

    project.create(project_name, 'RSContext')

    realization = project.add_realization(
        project_name, 'REALIZATION1', cfg.version)
    datasets = project.XMLBuilder.add_sub_element(realization, 'Datasets')

    dem_node, _dem_raster = project.add_project_raster(datasets, LayerTypes['DEM'])
    project.add_metadata([
        RSMeta('NumRasters', str(len(dem_raster_source_urls)), RSMetaTypes.INT),
        RSMeta('OriginUrls', json.dumps(dem_raster_source_urls), RSMetaTypes.JSON),
    ], dem_node)
    if resample:
        project.add_metadata([RSMeta('Processing', str(f'Resampled to {output_res} m'))], dem_node)

    project.add_project_raster(datasets, LayerTypes['HILLSHADE'])

    name_node = project.XMLBuilder.find('Name')
    name_node.text = project_name

    project.XMLBuilder.add_sub_element(project.XMLBuilder.root, 'Description', project_description)

    # Add Project Extents
    log.info("Use input bounds to generate project bounds geojson for RS project")
    extents_json_path = os.path.join(output_path, 'project_bounds.geojson')
    extents = generate_project_extents_from_layer(
        bounds_path, extents_json_path)
    project.add_project_extent(
        extents_json_path, extents['CENTROID'], extents['BBOX'])

    log.info('Riverscapes project build completed successfully.')


def is_valid_output_res(output_res) -> bool:
    """check if supplied output resolution is within expected bounds"""
    log = Logger("check arguments")
    is_valid = output_res >= 1 and output_res <= 10
    if not is_valid:
        log.error(f"Supplied output resolution of {output_res} is not within expected value of 1 to 10 inclusive (representing metres).")
    return is_valid


def main():
    """Main function to run the DEM Builder tool."""
    parser = argparse.ArgumentParser(description='DEM Builder Tool')
    parser.add_argument('bounds_path', help='Path to feature class (e.g. geopackage layer) containing polygon bounds feature', type=str)
    parser.add_argument('output_res', help='Horizontal resolution of output DEM in metres (1-10)', type=float)
    parser.add_argument('output_path', help='Path to folder where the project and output rasters will go', type=str)
    parser.add_argument('download_dir', help='Temporary folder for downloading data. Different HUCs may share this', type=str)
    parser.add_argument('--force', help='(optional) download existing files ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Hard-code certain args
    args.parallel = False
    args.verbose = True
    args.temp_folder = r'/workspaces/data/temp'

    # verify args
    safe_makedirs(args.output_path)
    if not os.path.isdir(args.output_path):
        raise ValueError(f"Expect `output_path` argument to be path to a folder. Value supplied: {args.output_path}")
    if not is_valid_output_res(args.output_res):
        raise ValueError("Output resolution not within expected bounds.")

    log = Logger('DEM Builder')
    log.setup(logPath=os.path.join(args.output_path, 'dem_builder.log'), verbose=args.verbose)
    log.title('DEM Builder')
    log.info(f'Bounds Path: {args.bounds_path}')
    log.info(f'Output Resolution: {args.output_res}m')
    log.info(f'Output Path: {args.output_path}')

    # This is a general place for unzipping downloaded files and other temporary work.
    # We use GUIDS to make it specific to a particular run of the tool to avoid unzip collisions
    parallel_code = "-" + str(uuid.uuid4()) if args.parallel is True else ""
    scratch_dir = args.temp_folder if args.temp_folder else os.path.join(args.download, 'scratch', f'rs_context{parallel_code}')
    safe_makedirs(scratch_dir)

    try:
        dem_builder(args.bounds_path, args.output_res, args.download_dir, scratch_dir, args.output_path, args.force)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        # Cleaning up the scratch folder is essential
        safe_remove_dir(scratch_dir)
        sys.exit(1)

    # Cleaning up the scratch folder is essential
    safe_remove_dir(scratch_dir)
    log.info("DEM Builder complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
