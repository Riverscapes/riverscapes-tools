from __future__ import annotations

import os
from datetime import datetime

from osgeo import ogr
from rscommons import initGDALOGRErrors
from rsxml import Logger
from rsxml.project_xml import (
    BoundingBox,
    Coords,
    Dataset,
    Geopackage,
    GeoPackageDatasetTypes,
    GeopackageLayer,
    Meta,
    MetaData,
    Project,
    ProjectBounds,
    Realization,
)
from rsxml.util import pretty_duration

from rscontextneo.__version__ import __version__
from rscontextneo.src.fetch_dem import (
    SLOPE_RELPATH,
    TILE_FOOTPRINTS_RELPATH,
)
from rscontextneo.src.utils.breach_diff import (
    BREACH_DIFF_GPKG_RELPATH,
    BREACH_DIFF_LAYER_NAME,
)
from rscontextneo.src.utils.geom import load_geojson_geometry

initGDALOGRErrors()

# ── Layer registry ─────────────────────────────────────────────────────────────
# All paths are relative to the project output_folder root.
# Keys are referenced by name when adding datasets to the realization.
LayerTypes: dict[str, Dataset | Geopackage] = {
    # ── Inputs ──────────────────────────────────────────────────────────────
    "DEM": Dataset(
        xml_id="DEM",
        name="DEM",
        path="topography/dem.tif",
        ds_type="Raster",
        meta_data=MetaData(
            [
                Meta(
                    "Description", "1-metre 3DEP DEM downloaded from The National Map"
                ),
            ]
        ),
    ),
    "HILLSHADE": Dataset(
        xml_id="HILLSHADE",
        name="DEM Hillshade",
        path="topography/dem_hillshade.tif",
        ds_type="Raster",
    ),
    "TILE_FOOTPRINTS": Geopackage(
        xml_id="TILE_FOOTPRINTS",
        name="DEM Tile Footprints",
        path=TILE_FOOTPRINTS_RELPATH,
        layers=[
            GeopackageLayer(
                lyr_name="tile_footprints",
                name="DEM Tile Footprints",
                ds_type=GeoPackageDatasetTypes.VECTOR,
            ),
        ],
        meta_data=MetaData(
            [
                Meta(
                    "Description",
                    "Polygon footprint of every 3DEP tile downloaded for this project. "
                    "Attributes record each tile's original filename, native CRS (original_epsg / "
                    "original_crs_name), whether it required reprojection to be included in the "
                    "mosaic (is_reprojected = 1), the final mosaic CRS (final_epsg / final_crs_name), "
                    "pixel dimensions, resolution, file size, and nodata value.",
                ),
            ]
        ),
    ),
    # ── Intermediates ────────────────────────────────────────────────────────
    "DEM_FILLED": Dataset(
        xml_id="DEM_FILLED",
        name="Pit-filled DEM",
        path="hydrology/dem_filled.tif",
        ds_type="Raster",
        meta_data=MetaData(
            [
                Meta(
                    "Description",
                    "DEM with topographic sinks filled (TauDEM pitremove)",
                ),
            ]
        ),
    ),
    "DEM_BREACH": Dataset(
        xml_id="DEM_BREACH",
        name="Breach-conditioned DEM",
        path="hydrology/dem_breach.tif",
        ds_type="Raster",
        meta_data=MetaData(
            [
                Meta(
                    "Description",
                    "DEM hydrologically conditioned via least-cost depression breaching (WhiteboxTools BreachDepressionsLeastCost)",
                ),
            ]
        ),
    ),
    "D8_FLOW": Dataset(
        xml_id="D8_FLOW",
        name="D8 Flow Direction",
        path="hydrology/d8_flow.tif",
        ds_type="Raster",
        meta_data=MetaData(
            [
                Meta(
                    "Description",
                    "D8 flow direction raster (1-8 encoding, TauDEM d8flowdir)",
                ),
            ]
        ),
    ),
    "SLOPE": Dataset(
        xml_id="SLOPE",
        name="Slope",
        path=SLOPE_RELPATH,
        ds_type="Raster",
        meta_data=MetaData(
            [
                Meta(
                    "Description",
                    "Topographic slope in degrees (gdal.DEMProcessing Horn method, calculated from the raw DEM)",
                ),
            ]
        ),
    ),
    "D8_CONTRIB_AREA": Dataset(
        xml_id="D8_CONTRIB_AREA",
        name="D8 Contributing Area",
        path="hydrology/d8_contributing_area.tif",
        ds_type="Raster",
        meta_data=MetaData(
            [
                Meta(
                    "Description",
                    "D8 flow accumulation raster — cell values are upstream cell counts (TauDEM aread8). Multiply by (cell size in metres)² to convert to m².",
                ),
            ]
        ),
    ),
    # ── Outputs ──────────────────────────────────────────────────────────────
    "STREAM_RASTER": Dataset(
        xml_id="STREAM_RASTER",
        name="Stream Raster",
        path="hydrology/stream_raster.tif",
        ds_type="Raster",
        meta_data=MetaData(
            [
                Meta(
                    "Description",
                    "Binary stream mask: 1 where upstream contributing area ≥ StreamThreshold cells, 0 elsewhere (TauDEM threshold)",
                ),
            ]
        ),
    ),
    "STREAM_ORDER": Dataset(
        xml_id="STREAM_ORDER",
        name="Stream Order",
        path="hydrology/stream_order.tif",
        ds_type="Raster",
        meta_data=MetaData(
            [
                Meta("Description", "Strahler stream-order raster (TauDEM streamnet)"),
            ]
        ),
    ),
    "HYDRODERIVATIVES": Geopackage(
        xml_id="HYDRODERIVATIVES",
        name="Hydrology Derivatives",
        path="hydrology/hydro_derivatives.gpkg",
        layers=[
            GeopackageLayer(
                lyr_name="network_intersected",
                name="Stream Network Reaches",
                ds_type=GeoPackageDatasetTypes.VECTOR,
            ),
            GeopackageLayer(
                lyr_name="subwatersheds",
                name="Subwatersheds",
                ds_type=GeoPackageDatasetTypes.VECTOR,
            ),
        ],
    ),
    "SUBWATERSHEDS": Dataset(
        xml_id="SUBWATERSHEDS",
        name="Subwatersheds",
        path="hydrology/subwatersheds.tif",
        ds_type="Raster",
        meta_data=MetaData(
            [
                Meta(
                    "Description",
                    "Subwatershed raster — one unique value per stream reach (TauDEM streamnet)",
                ),
            ]
        ),
    ),
    "BREACH_DIFF_POINTS": Geopackage(
        xml_id="BREACH_DIFF_POINTS",
        name="Breach Difference Points",
        path=BREACH_DIFF_GPKG_RELPATH,
        layers=[
            GeopackageLayer(
                lyr_name=BREACH_DIFF_LAYER_NAME,
                name="Breach Difference Points",
                ds_type=GeoPackageDatasetTypes.VECTOR,
            ),
        ],
        meta_data=MetaData(
            [
                Meta(
                    "Description",
                    "Debug layer: point feature at every pixel where the original DEM "
                    "elevation exceeds the breach-conditioned DEM by \u2265 1 m. "
                    "The diff_m attribute records the magnitude of the elevation change. "
                    "Only produced when --debug is set.",
                ),
            ]
        ),
    ),
}


def build_project_bounds(
    bounds_geojson: str,
    output_folder: str,
    log: Logger,
) -> ProjectBounds | None:
    """
    Parse the bounds GeoJSON and return a ``ProjectBounds`` instance for use
    in the project XML.

    Uses shapely to compute the centroid and envelope of the AOI polygon so the
    Riverscapes Viewer can display the project on the map index.

    Parameters
    ----------
    bounds_geojson : str
        Absolute path to the WGS84 project bounds GeoJSON.
    output_folder : str
        Root of the RS Context Neo project folder (used to compute the
        relative path stored in ``<ProjectBounds><Path>``).
    log : Logger
        Caller-supplied logger.

    Returns
    -------
    ProjectBounds or None
        ``None`` if the bounds could not be computed (non-fatal).
    """
    try:
        merged = load_geojson_geometry(bounds_geojson)
        centroid = merged.centroid
        minx, miny, maxx, maxy = merged.bounds  # (minLng, minLat, maxLng, maxLat)

        rel_path = os.path.relpath(bounds_geojson, output_folder)

        bounds = ProjectBounds(
            centroid=Coords(lng=centroid.x, lat=centroid.y),
            bounding_box=BoundingBox(
                minLng=minx, minLat=miny, maxLng=maxx, maxLat=maxy
            ),
            filepath=rel_path,
        )
        log.info(
            f"  ProjectBounds: centroid ({centroid.x:.4f}, {centroid.y:.4f}), "
            f"bbox [{minx:.4f}, {miny:.4f} → {maxx:.4f}, {maxy:.4f}]"
        )
        return bounds

    except Exception as exc:
        # A bounds failure is non-fatal — the project XML is otherwise complete
        log.warning(f"Failed to build project bounds: {exc}")
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────


def _dataset_exists(output_folder: str, dataset: Dataset | Geopackage) -> bool:
    """
    Return True if *dataset* is present and complete on disk.

    For plain ``Dataset`` objects this means the file at
    ``<output_folder>/<dataset.path>`` exists.

    For ``Geopackage`` objects the file must exist **and** every layer
    declared in ``dataset.layers`` must be present inside the GPKG.
    This catches the case where the GPKG was created but a layer was
    skipped (e.g. the TNM tile-footprints layer when using the WCS source).
    """
    abs_path = os.path.join(output_folder, dataset.path)
    if not os.path.isfile(abs_path):
        return False

    if isinstance(dataset, Geopackage) and dataset.layers:
        gpkg_ds = ogr.Open(abs_path)
        if gpkg_ds is None:
            return False
        existing_layers = {
            gpkg_ds.GetLayerByIndex(i).GetName() for i in range(gpkg_ds.GetLayerCount())
        }
        gpkg_ds = None  # close
        for lyr in dataset.layers:
            if lyr.lyr_name not in existing_layers:
                return False

    return True


def write_project_xml(
    output_folder: str,
    descriptor: str,
    bounds_geojson: str,
    meta: dict[str, str],
    aoi: str | None,
    dem: str | None,
    threshold: int,
    output_res: float,
    breach_dist: int,
    elapsed_time: float,
    log: Logger,
    debug: bool = False,
    dem_source: str = "tnm",
    rail: str | None = None,
    roads: str | None = None,
) -> None:
    """
    Create or overwrite the Riverscapes project XML file.

    Registers all topography and hydrology outputs as datasets under the
    realization, and records project bounds from the bounds GeoJSON.

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
    output_res : float
        Target DEM resolution in metres. Recorded as ``OutputResolution``.
    breach_dist : int
        Maximum breach search distance in cells. Recorded as ``BreachDist``.
    elapsed_time : float
        Total processing time in seconds (``time.time()`` delta). Recorded as
        ``ProcTimeS`` (hidden) and ``Processing Time`` (human-readable).
    log : Logger
        Caller-supplied logger.
    debug : bool
        If True, the BREACH_DIFF_POINTS debug layer is registered.
    dem_source : str
        DEM backend used (``'tnm'`` or ``'wcs'``).  Only the TNM path writes
        a tile footprints GeoPackage, so ``TILE_FOOTPRINTS`` is only
        registered when this is ``'tnm'``.
    rail : str or None
        S3 Tables path for the rail layer (``'<catalog>/<namespace>/<table>'``).
        When provided a ``'rail'`` layer is registered in the transportation
        GeoPackage dataset.  Pass ``None`` to omit.
    roads : str or None
        S3 Tables path for the roads layer (same format as *rail*).
        When provided a ``'roads'`` layer is registered.  Pass ``None`` to omit.
    """
    log.info("Writing project XML")

    project_name = f"RSContext Neo — {descriptor}"
    xml_path = os.path.join(output_folder, "project.rs.xml")

    # ── Project metadata ───────────────────────────────────────────────────────
    project_meta = MetaData(
        [
            Meta("ModelVersion", __version__),
            Meta(
                "Model Documentation", "https://tools.riverscapes.net/rscontext", "url"
            ),
            Meta("StreamThreshold", str(threshold), "hidden"),
            Meta("StreamThresholdUnits", "cells", "hidden"),
            Meta("Stream Threshold", f"{threshold:,} cells"),
            Meta("OutputResolution", str(output_res), "hidden"),
            Meta("Output Resolution", f"{output_res} m"),
            Meta("BreachDist", str(breach_dist), "hidden"),
            Meta("Breach Distance", f"{breach_dist} cells"),
        ]
    )

    # Source path (hidden)
    if aoi is not None:
        project_meta.add_meta("AOI", aoi, "hidden")
    elif dem is not None:
        project_meta.add_meta("DEM", dem, "hidden")

    # Caller-supplied metadata (hidden)
    if meta:
        for k, v in meta.items():
            project_meta.add_meta(k, v, "hidden")

    # Timing metadata
    project_meta.add_meta("ProcTimeS", f"{elapsed_time:.2f}", "hidden")
    project_meta.add_meta("Processing Time", pretty_duration(elapsed_time))

    # ── Realization datasets ───────────────────────────────────────────────────
    # Filter to only layers whose files (and GPKG layers) actually exist on
    # disk.  This prevents FILE_MAP errors for outputs that were skipped or
    # belong to a different DEM source (e.g. TILE_FOOTPRINTS with --dem_source wcs).
    log.info("  Registering project layers")
    candidate_datasets: list[Dataset | Geopackage] = [
        LayerTypes["DEM"],
        LayerTypes["HILLSHADE"],
        LayerTypes["SLOPE"],
        LayerTypes["DEM_FILLED"],
        LayerTypes["DEM_BREACH"],
        LayerTypes["D8_FLOW"],
        LayerTypes["D8_CONTRIB_AREA"],
        LayerTypes["STREAM_RASTER"],
        LayerTypes["STREAM_ORDER"],
        LayerTypes["HYDRODERIVATIVES"],
        LayerTypes["SUBWATERSHEDS"],
    ]
    if aoi is not None and dem_source == "tnm":
        candidate_datasets.append(LayerTypes["TILE_FOOTPRINTS"])
    if debug:
        candidate_datasets.append(LayerTypes["BREACH_DIFF_POINTS"])
    if rail is not None or roads is not None:
        transport_layers = []
        if roads is not None:
            transport_layers.append(
                GeopackageLayer(
                    lyr_name="roads",
                    name="Roads",
                    ds_type=GeoPackageDatasetTypes.VECTOR,
                )
            )
        if rail is not None:
            transport_layers.append(
                GeopackageLayer(
                    lyr_name="rail", name="Rail", ds_type=GeoPackageDatasetTypes.VECTOR
                )
            )
        transport_ds = Geopackage(
            xml_id="TRANSPORTATION",
            name="Transportation",
            path="transportation/transportation.gpkg",
            layers=transport_layers,
            meta_data=MetaData(
                [
                    Meta(
                        "Description",
                        "Transportation features (roads and rail) sourced from AWS S3 Tables via Athena.",
                    ),
                ]
            ),
        )
        candidate_datasets.append(transport_ds)

    realization_datasets = []
    for ds in candidate_datasets:
        if _dataset_exists(output_folder, ds):
            realization_datasets.append(ds)
        else:
            log.warning(
                f"  Skipping dataset '{ds.xml_id}' — not found on disk: "
                f"{os.path.join(output_folder, ds.path)}"
            )

    # ── Project bounds ─────────────────────────────────────────────────────────
    bounds = build_project_bounds(bounds_geojson, output_folder, log)

    # ── Realization ────────────────────────────────────────────────────────────
    realization = Realization(
        name=project_name,
        xml_id="REALIZATION1",
        date_created=datetime.now(),
        product_version=__version__,
        datasets=realization_datasets,
    )

    # ── Project ────────────────────────────────────────────────────────────────
    project = Project(
        name=project_name,
        project_type="RSContext",
        bounds=bounds,
        proj_path=xml_path,
        meta_data=project_meta,
        realizations=[realization],
    )
    project.write()
    log.info(f"Project XML written: {xml_path}")
