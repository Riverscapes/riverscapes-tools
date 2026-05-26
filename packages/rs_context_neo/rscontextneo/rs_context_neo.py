"""
Name:       Riverscapes Context Neo

Purpose:    Build a Riverscapes Context project for a single watershed by
            downloading a 1-metre 3DEP DEM and running the standard TauDEM
            D8 hydrology processing chain.

Author:     Riverscapes
Date:       2026-05-20
"""
import argparse
import json
import os
import sys
import traceback

from shapely.geometry import shape
from shapely.ops import unary_union

from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs, parse_metadata
from rscommons import ModelConfig, RSLayer, RSProject, initGDALOGRErrors
from rscommons.classes.rs_project import RSMeta, RSMetaTypes

from rscontextneo.__version__ import __version__
from rscontextneo.src.aoi import validate_copy_aoi
from rscontextneo.src.dem import dem_to_geojson
from rscontextneo.src.fetch_dem import fetch_dem_from_3dep
from rscontextneo.src.hydrology import run_d8_hydrology, DEFAULT_THRESHOLD

initGDALOGRErrors()

# ── Project configuration ──────────────────────────────────────────────────────
cfg = ModelConfig(
    'https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd',
    __version__,
)

# ── Layer registry ─────────────────────────────────────────────────────────────
# All paths are relative to the project output_folder root.
# Keys are referenced by name when adding nodes to the project XML.
LayerTypes = {
    # ── Inputs ──────────────────────────────────────────────────────────────
    'DEM': RSLayer(
        'DEM', 'DEM', 'Raster', 'topography/dem.tif',
        lyr_meta=[RSMeta('Description', '1-metre 3DEP DEM downloaded from The National Map')],
    ),
    'HILLSHADE': RSLayer(
        'DEM Hillshade', 'HILLSHADE', 'Raster', 'topography/dem_hillshade.tif',
    ),

    # ── Intermediates ────────────────────────────────────────────────────────
    'DEM_FILLED': RSLayer(
        'Pit-filled DEM', 'DEM_FILLED', 'Raster', 'hydrology/dem_filled.tif',
        lyr_meta=[RSMeta('Description', 'DEM with topographic sinks filled (TauDEM pitremove)')],
    ),
    'D8_FLOW': RSLayer(
        'D8 Flow Direction', 'D8_FLOW', 'Raster', 'hydrology/d8_flow.tif',
        lyr_meta=[RSMeta('Description', 'D8 flow direction raster (1-8 encoding, TauDEM d8flowdir)')],
    ),
    'D8_SLOPE': RSLayer(
        'D8 Slope', 'D8_SLOPE', 'Raster', 'hydrology/d8_slope.tif',
        lyr_meta=[RSMeta('Description', 'D8 slope raster — dimensionless rise/run (m/m) per cell (TauDEM d8flowdir)')],
    ),
    'D8_CONTRIB_AREA': RSLayer(
        'D8 Contributing Area', 'D8_CONTRIB_AREA', 'Raster', 'hydrology/d8_contributing_area.tif',
        lyr_meta=[RSMeta('Description', 'D8 flow accumulation raster — cell values are upstream cell counts (TauDEM aread8). Multiply by (cell size in metres)² to convert to m².')],
    ),

    # ── Outputs ──────────────────────────────────────────────────────────────
    'STREAM_RASTER': RSLayer(
        'Stream Raster', 'STREAM_RASTER', 'Raster', 'hydrology/stream_raster.tif',
        lyr_meta=[RSMeta('Description', 'Binary stream mask: 1 where upstream contributing area ≥ StreamThreshold cells, 0 elsewhere (TauDEM threshold)')],
    ),
    'STREAM_ORDER': RSLayer(
        'Stream Order', 'STREAM_ORDER', 'Raster', 'hydrology/stream_order.tif',
        lyr_meta=[RSMeta('Description', 'Strahler stream-order raster (TauDEM streamnet)')],
    ),
    'HYDROLOGY_GPKG': RSLayer(
        'Hydrology', 'HYDROLOGY_GPKG', 'Geopackage', 'hydrology/hydrology.gpkg',
        sub_layers={
            'network': RSLayer('Stream Network Reaches', 'NETWORK', 'Vector', 'network'),
            'subwatersheds': RSLayer('Subwatersheds', 'SUBWATERSHEDS_VEC', 'Vector', 'subwatersheds'),
        },
    ),
    'SUBWATERSHEDS': RSLayer(
        'Subwatersheds', 'SUBWATERSHEDS', 'Raster', 'hydrology/subwatersheds.tif',
        lyr_meta=[RSMeta('Description', 'Subwatershed raster — one unique value per stream reach (TauDEM streamnet)')],
    ),
}


def rs_context_neo(
    output_folder: str,
    meta: dict[str, str],
    *,
    aoi: str | None = None,
    dem: str | None = None,
    download_folder: str | None = None,
    scratch_folder: str | None = None,
    output_res: float = 1.0,
    force_download: bool = False,
    threshold: int = DEFAULT_THRESHOLD,
    cores: int | None = None,
) -> None:
    """
    Run the Riverscapes Context Neo tool for a single watershed.

    Writes a complete Riverscapes project (``project.rs.xml``) in
    *output_folder* that references all topography and hydrology outputs.
    Exactly one of *aoi* or *dem* must be provided.

    Parameters:
        output_folder (str): Directory where the output files will be saved.
        meta (dict[str, str]): Optional metadata key=value pairs.
        aoi (str): Path to a GeoJSON defining the area of interest.
        dem (str): Path to an already-downloaded DEM raster file.
        download_folder (str): Cache folder for raw 3DEP tile downloads.
                               Required when using --aoi.
        scratch_folder (str): Temporary folder for unzipping tiles.
                               Defaults to <download_folder>/scratch if not set.
        output_res (float): Target DEM resolution in metres (1–10). Default 1.0.
        force_download (bool): Re-download tiles and re-run all steps even if
                               outputs are already cached. Default False.
        threshold (int): Minimum upstream cell count for stream classification
                         (units: cells). A cell is classified as stream where
                         its contributing area ≥ this value. At 1 m resolution,
                         multiply by 1 m² to get contributing area in m²
                         (e.g. 50 000 cells ≈ 0.05 km²). Default: 50 000 cells.
        cores (int | None): Number of MPI ranks for TauDEM steps. None reads
                            the TAUDEM_CORES env var, falling back to 2.
    """
    log = Logger('RS Context Neo')

    if sum(v is not None for v in (aoi, dem)) != 1:
        raise ValueError('Exactly one of aoi or dem must be provided.')

    safe_makedirs(output_folder)

    # ── Step 1: Acquire bounds GeoJSON and DEM ─────────────────────────────────
    if aoi is not None:
        log.info(f'Using AOI GeoJSON: {aoi}')
        descriptor = 'Custom AOI'
        bounds_geojson = validate_copy_aoi(aoi, output_folder)
        dem_path, _hillshade_path = _fetch_3dep(
            bounds_geojson, output_folder, download_folder, scratch_folder,
            output_res, force_download,
        )
    elif dem is not None:
        log.info(f'Using user-supplied DEM: {dem}')
        descriptor = 'User-supplied DEM'
        bounds_geojson = dem_to_geojson(dem, output_folder)
        dem_path = dem
    else:
        raise AssertionError('Unreachable: runtime guard above ensures exactly one source is set.')

    # ── Step 2: D8 Hydrology ──────────────────────────────────────────────────
    run_d8_hydrology(
        dem_path,
        output_folder,
        threshold=threshold,
        cores=cores,
        force=force_download,
    )

    # ── Step 3: Write project XML ─────────────────────────────────────────────
    _write_project_xml(
        output_folder=output_folder,
        descriptor=descriptor,
        bounds_geojson=bounds_geojson,
        meta=meta,
        aoi=aoi,
        dem=dem,
        threshold=threshold,
        log=log,
    )

    log.info('RS Context Neo processing complete')


def _write_project_xml(
    output_folder: str,
    descriptor: str,
    bounds_geojson: str,
    meta: dict[str, str],
    aoi: str | None,
    dem: str | None,
    threshold: int,
    log: Logger,
) -> None:
    """
    Create or overwrite the Riverscapes project XML file.

    Registers all topography and hydrology outputs as datasets under the
    appropriate Inputs / Intermediates / Outputs nodes, and records project
    bounds from the bounds GeoJSON.

    Parameters
    ----------
    output_folder : str
        Root of the RS Context Neo project folder.
    descriptor : str
        Short human-readable description of the input source (e.g.
        ``'Custom AOI'`` or ``'User-supplied DEM'``).
    bounds_geojson : str
        Absolute path to the project bounds GeoJSON (WGS84).
    meta : dict[str, str]
        Extra metadata key=value pairs supplied by the caller.
    aoi : str or None
        Original AOI path (recorded as hidden metadata).
    dem : str or None
        Original DEM path (recorded as hidden metadata).
    threshold : int
        Minimum upstream cell count for stream classification (units: cells).
        Recorded in project metadata as ``StreamThreshold``.
    log : Logger
        Caller-supplied logger.
    """
    log.info('Writing project XML')

    project_name = f'RSContext Neo — {descriptor}'

    project = RSProject(cfg, output_folder)
    project.create(project_name, 'rscontextneo', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/rscontext', RSMetaTypes.URL, locked=True),
        RSMeta('StreamThreshold', str(threshold), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('StreamThresholdUnits', 'cells', RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Stream Threshold', f'{threshold:,} cells', locked=True),
    ])

    # Caller-supplied metadata (hidden)
    if aoi is not None:
        project.add_metadata([RSMeta('AOI', aoi, RSMetaTypes.HIDDEN, locked=True)])
    elif dem is not None:
        project.add_metadata([RSMeta('DEM', dem, RSMetaTypes.HIDDEN, locked=True)])
    if meta:
        project.add_metadata([RSMeta(k, v, RSMetaTypes.HIDDEN, locked=True) for k, v in meta.items()])

    # ── Realization ────────────────────────────────────────────────────────────
    _realization, nodes = project.add_realization(
        project_name,
        'REALIZATION1',
        cfg.version,
        data_nodes=['Inputs', 'Intermediates', 'Outputs'],
        create_folders=False,  # folders already exist from processing
    )

    # ── Inputs: DEM + hillshade ────────────────────────────────────────────────
    log.info('  Registering input layers')
    project.add_project_raster(nodes['Inputs'], LayerTypes['DEM'])
    project.add_project_raster(nodes['Inputs'], LayerTypes['HILLSHADE'])

    # ── Intermediates: pit-fill, flow dir, slope, contributing area ───────────
    log.info('  Registering intermediate layers')
    project.add_project_raster(nodes['Intermediates'], LayerTypes['DEM_FILLED'])
    project.add_project_raster(nodes['Intermediates'], LayerTypes['D8_FLOW'])
    project.add_project_raster(nodes['Intermediates'], LayerTypes['D8_SLOPE'])
    project.add_project_raster(nodes['Intermediates'], LayerTypes['D8_CONTRIB_AREA'])

    # ── Outputs: stream products ───────────────────────────────────────────────
    log.info('  Registering output layers')
    project.add_project_raster(nodes['Outputs'], LayerTypes['STREAM_RASTER'])
    project.add_project_raster(nodes['Outputs'], LayerTypes['STREAM_ORDER'])
    project.add_project_geopackage(nodes['Outputs'], LayerTypes['HYDROLOGY_GPKG'])
    project.add_project_raster(nodes['Outputs'], LayerTypes['SUBWATERSHEDS'])

    # ── Project extent (bounds GeoJSON → centroid + bbox) ─────────────────────
    _register_project_bounds(project, bounds_geojson, log)

    log.info(f'Project XML written: {project.xml_path}')


def _register_project_bounds(project: RSProject, bounds_geojson: str, log: Logger) -> None:
    """
    Parse the bounds GeoJSON and register centroid + bounding box in the project XML.

    Uses shapely to compute the centroid and envelope of the AOI polygon so the
    Riverscapes Viewer can display the project on the map index.

    Parameters
    ----------
    project : RSProject
        The project object (XML already created).
    bounds_geojson : str
        Absolute path to the WGS84 project bounds GeoJSON.
    log : Logger
        Caller-supplied logger.
    """
    try:
        with open(bounds_geojson, encoding='utf-8') as f:
            data = json.load(f)

        geoms = []
        geoj_type = data.get('type', '')
        if geoj_type == 'FeatureCollection':
            geoms = [shape(feat['geometry']) for feat in data.get('features', []) if feat.get('geometry')]
        elif geoj_type == 'Feature':
            if data.get('geometry'):
                geoms = [shape(data['geometry'])]
        else:
            geoms = [shape(data)]

        if not geoms:
            log.warning('Could not extract geometries from bounds GeoJSON — skipping ProjectBounds registration')
            return

        merged = unary_union(geoms)
        centroid = merged.centroid
        minx, miny, maxx, maxy = merged.bounds  # (minLng, minLat, maxLng, maxLat)

        # add_project_extent expects (minX, maxX, minY, maxY) = (minLng, maxLng, minLat, maxLat)
        project.add_project_extent(
            geojson_path=bounds_geojson,
            centroid=(centroid.x, centroid.y),
            bbox=(minx, maxx, miny, maxy),
        )
        log.info(f'  ProjectBounds: centroid ({centroid.x:.4f}, {centroid.y:.4f}), '
                 f'bbox [{minx:.4f}, {miny:.4f} → {maxx:.4f}, {maxy:.4f}]')

    except Exception as exc:
        # A bounds failure is non-fatal — the project XML is otherwise complete
        log.warning(f'Failed to register project bounds: {exc}')


def _fetch_3dep(
    bounds_geojson: str,
    output_folder: str,
    download_folder: str | None,
    scratch_folder: str | None,
    output_res: float,
    force_download: bool,
) -> tuple[str, str]:
    """Validate 3DEP fetch arguments and delegate to fetch_dem_from_3dep."""
    if download_folder is None:
        raise ValueError(
            'download_folder is required when using --aoi. '
            'Provide a persistent cache directory with --download_dir.'
        )
    effective_scratch = scratch_folder or os.path.join(download_folder, 'scratch')
    return fetch_dem_from_3dep(
        bounds_geojson, output_folder, download_folder, effective_scratch,
        output_res, force_download,
    )


def main():
    """Main entry point for RS Context Neo."""
    parser = argparse.ArgumentParser(description='Riverscapes Context Neo Tool')

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument('--aoi', help='Path to a GeoJSON defining the area of interest', type=str)
    source_group.add_argument('--dem', help='Path to an already-downloaded DEM raster file', type=str)

    parser.add_argument('--output', '-o', help='Path to the output folder', type=str, required=True)
    parser.add_argument('--download_dir', help='Cache folder for 3DEP tile downloads (required for --aoi)', type=str)
    parser.add_argument('--scratch_dir', help='Temporary folder for unzipping tiles (default: <download_dir>/scratch)', type=str)
    parser.add_argument('--output_res', help='Target DEM resolution in metres (1–10, default: 1.0 m)', type=float, default=1.0)
    parser.add_argument('--force', help='Re-download 3DEP tiles and re-run all processing steps even if already cached', action='store_true', default=False)
    parser.add_argument('--threshold', help=f'Minimum upstream contributing-area cell count for stream classification (units: cells; default: {DEFAULT_THRESHOLD:,} cells). At 1 m resolution, {DEFAULT_THRESHOLD:,} cells ≈ {DEFAULT_THRESHOLD / 1e6:.2f} km². Lower values produce denser networks. See docs/STREAM_THRESHOLD.md.', type=int, default=DEFAULT_THRESHOLD)
    parser.add_argument('--cores', help='Number of MPI ranks (parallel processes) for TauDEM steps (default: TAUDEM_CORES env var, or 2)', type=int, default=None)
    parser.add_argument('--meta', help='Riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about things like memory usage. There is a performance cost', action='store_true', default=False)

    # Load the .env file sitting next to this script so {env:VAR} tokens in
    # args are resolved even when the variable is not in the shell environment.
    _env_path = os.path.join(os.path.dirname(__file__), '.env')
    args = dotenv.parse_args_env(parser, env_path=_env_path)

    log = Logger('RS Context Neo')
    log.setup(log_path=os.path.join(args.output, 'RSContextNeo.log'), verbose=args.verbose)
    log.title('Riverscapes Context Neo')

    log.info(f'Model Version: {__version__}')
    log.info(f'Output folder: {args.output}')
    if args.download_dir:
        log.info(f'Download cache: {args.download_dir}')
    log.info(f'Output resolution: {args.output_res} m')
    log.info(f'Stream threshold:  {args.threshold:,} cells')

    meta = parse_metadata(args.meta) if args.meta else {}

    try:
        rs_context_neo(
            args.output,
            meta,
            aoi=args.aoi,
            dem=args.dem,
            download_folder=args.download_dir,
            scratch_folder=args.scratch_dir,
            output_res=args.output_res,
            force_download=args.force,
            threshold=args.threshold,
            cores=args.cores,
        )
    except Exception as e:
        log.error(e)
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
