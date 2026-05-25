"""
Name:       Riverscapes Context Neo

Purpose:    Build a Riverscapes Context project scaffold for a single watershed.
            Currently creates an empty project.rs.xml in the output folder.

Author:     Riverscapes

Date:       2026-05-20
"""
import argparse
import os
import sys
import traceback

from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs, parse_metadata
from rsxml.project_xml import Project, MetaData
from rscommons import initGDALOGRErrors
from rscontextneo.__version__ import __version__
from rscontextneo.lib.aoi import validate_copy_aoi
from rscontextneo.lib.dem import dem_to_geojson
from rscontextneo.lib.huc import fetch_huc_geometry


def rs_context_neo(
    output_folder: str,
    meta: dict[str, str],
    *,
    huc: str | None = None,
    aoi: str | None = None,
    dem: str | None = None,
) -> None:
    """
    Run the Riverscapes Context Neo tool for a single watershed.

    Creates a minimal Riverscapes project XML file (project.rs.xml) in
    the output folder. Exactly one of huc, aoi, or dem must be provided.

    Parameters:
        output_folder (str): Directory where the output files will be saved.
        meta (dict[str, str]): Optional metadata key=value pairs.
        huc (str): Watershed/HUC identifier (HUC8, HUC10, or HUC12).
        aoi (str): Path to a geojson defining the area of interest.
        dem (str): Path to an already-downloaded DEM raster file.
    """

    log = Logger('RS Context Neo')

    if sum(v is not None for v in (huc, aoi, dem)) != 1:
        raise ValueError('Exactly one of huc, aoi, or dem must be provided.')

    safe_makedirs(output_folder)

    # Determine project descriptor from the provided input mode.
    if huc is not None:
        log.info(f'Using HUC code: {huc}')
        descriptor = f'HUC {huc}'
        bounds_geojson = fetch_huc_geometry(huc, output_folder)
        # TODO: download DEM from 3DEP for the HUC AOI
    elif aoi is not None:
        log.info(f'Using AOI GeoJSON: {aoi}')
        descriptor = 'Custom AOI'
        bounds_geojson = validate_copy_aoi(aoi, output_folder)
        # TODO: download DEM from 3DEP for the AOI
    elif dem is not None:
        log.info(f'Using user-supplied DEM: {dem}')
        descriptor = 'User-supplied DEM'
        bounds_geojson = dem_to_geojson(dem, output_folder)
    else:
        raise AssertionError('Unreachable: runtime guard ensures exactly one source arg is set.')

    # Build project metadata
    project_meta = MetaData()
    project_meta.add_meta('ModelVersion', __version__)
    project_meta.add_meta('Model Documentation', 'https://tools.riverscapes.net/rscontext', 'url')

    if huc is not None:
        project_meta.add_meta('HUC', str(huc), 'hidden')
        project_meta.add_meta('Hydrologic Unit Code', str(huc))
    elif aoi is not None:
        project_meta.add_meta('AOI', aoi, 'hidden')
    elif dem is not None:
        project_meta.add_meta('DEM', dem, 'hidden')

    for key, val in meta.items():
        project_meta.add_meta(key, val, 'hidden')

    project = Project(
        name=f'RSContext Neo for {descriptor}',
        project_type='rscontextneo',
        bounds=None,  # type: ignore[arg-type]  # TODO: replace with real ProjectBounds once available
        proj_path=os.path.join(output_folder, 'project.rs.xml'),
        meta_data=project_meta,
    )

    project.write()

    log.info('RS Context Neo processing complete')


def main():
    """Main entry point for RS Context Neo."""
    parser = argparse.ArgumentParser(description='Riverscapes Context Neo Tool')

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument('--huc', help='HUC code (HUC8, HUC10, or HUC12) for the area of interest', type=str)
    source_group.add_argument('--aoi', help='Path to a geojson defining the area of interest', type=str)
    source_group.add_argument('--dem', help='Path to an already-downloaded DEM raster file', type=str)

    parser.add_argument('--output', '-o', help='Path to the output folder', type=str, required=True)
    parser.add_argument('--meta', help='Riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about things like memory usage. There is a performance cost', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    initGDALOGRErrors()

    log = Logger('RS Context Neo')
    log.setup(log_path=os.path.join(args.output, 'RSContextNeo.log'), verbose=args.verbose)
    log.title('Riverscapes Context Neo')

    log.info(f'Model Version: {__version__}')
    log.info(f'Output folder: {args.output}')

    meta = parse_metadata(args.meta) if args.meta else {}

    try:
        rs_context_neo(args.output, meta, huc=args.huc, aoi=args.aoi, dem=args.dem)
    except Exception as e:
        log.error(e)
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
