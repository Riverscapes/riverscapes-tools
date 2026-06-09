from __future__ import annotations

import json
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
from rscontextneo.src.config import AppConfig, RasterLayerConfig, S3TablesLayerConfig
from rscontextneo.src.utils.geom import load_geojson_geometry

initGDALOGRErrors()

# ── JSON layer definitions ─────────────────────────────────────────────────────
_JSON_PATH = os.path.join(os.path.dirname(__file__), "layer_definitions.json")

# Sub-layer structure for Geopackage datasets.  The JSON captures metadata but
# not the internal layer list (lyr_name / display name pairs), so those are
# declared here as a single source of truth.
_GPKG_SUB_LAYERS: dict[str, list[GeopackageLayer]] = {
    "TILE_FOOTPRINTS": [
        GeopackageLayer(
            lyr_name="tile_footprints",
            name="DEM Tile Footprints",
            ds_type=GeoPackageDatasetTypes.VECTOR,
        ),
        GeopackageLayer(
            lyr_name="data_footprints",
            name="DEM Data Footprints",
            ds_type=GeoPackageDatasetTypes.VECTOR,
        ),
    ],
    "HYDRODERIVATIVES": [
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
    "BREACH_DIFF_POINTS": [
        GeopackageLayer(
            lyr_name="breach_diff_points",
            name="Breach Difference Points",
            ds_type=GeoPackageDatasetTypes.VECTOR,
        ),
    ],
}


def _build_meta_data(layer: dict) -> MetaData | None:
    """Build a MetaData object from a layer's ``xml_metadata`` dict."""
    items = [Meta(k, v) for k, v in layer.get("xml_metadata", {}).items()]
    return MetaData(items) if items else None


def _load_layer_types() -> dict[str, Dataset | Geopackage]:
    """
    Parse ``layer_definitions.json`` and return a ``LayerTypes`` registry
    keyed by ``layer_id``.

    Non-Geopackage layers become :class:`Dataset` instances; Geopackage layers
    become :class:`Geopackage` instances whose sub-layers are sourced from
    :data:`_GPKG_SUB_LAYERS`.
    """
    with open(_JSON_PATH, encoding="utf-8") as fh:
        data = json.load(fh)

    result: dict[str, Dataset | Geopackage] = {}
    for layer in data["layers"]:
        layer_id: str = layer["layer_id"]
        name: str = layer["layer_name"]
        path: str = layer["path"]
        layer_type: str = layer["layer_type"]
        description: str | None = layer.get("description")
        summary: str | None = layer.get("summary")
        citation: str | None = layer.get("citation")
        meta_data = _build_meta_data(layer)

        if layer_type == "Geopackage":
            result[layer_id] = Geopackage(
                xml_id=layer_id,
                name=name,
                path=path,
                layers=_GPKG_SUB_LAYERS.get(layer_id, []),
                description=description,
                summary=summary,
                citation=citation,
                meta_data=meta_data,
            )
        else:
            result[layer_id] = Dataset(
                xml_id=layer_id,
                name=name,
                path=path,
                ds_type=layer_type,
                description=description,
                summary=summary,
                citation=citation,
                meta_data=meta_data,
            )

    return result


# ── Layer registry ─────────────────────────────────────────────────────────────
# All paths are relative to the project output_folder root.
# Keys are referenced by name when adding datasets to the realization.
LayerTypes: dict[str, Dataset | Geopackage] = _load_layer_types()


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


def _build_merged_layer_types(
    config_layers: list,
    output_folder: str,
    log: Logger,
) -> dict[str, "Dataset | Geopackage"]:
    """
    Return a per-run copy of the LayerTypes registry with overrides applied
    from config layer entries where layer_ids match.

    Merge mapping (config field -> definition field):
      label        -> name  (display name)
      output_path  -> path
      description  -> description

    For RasterLayerConfig: extra Meta items (source_url, data_product_version,
    docs_url, CellSizeX, CellSizeY) are attached as meta_data on the Dataset.

    For S3TablesLayerConfig: sub-layers are derived from layer_cfg.layer_name.

    The module-level LayerTypes dict is NEVER mutated.
    """
    merged: dict[str, Dataset | Geopackage] = dict(LayerTypes)  # shallow copy

    for lyr in config_layers:
        lid: str = getattr(lyr, "layer_id", None)
        if lid not in LayerTypes:
            continue  # no definition entry -> keep inline construction path

        existing = LayerTypes[lid]

        # -- Resolve field overrides --------------------------------------------
        name = getattr(lyr, "label", None) or existing.name
        path = getattr(lyr, "output_path", None) or existing.path

        raw_desc = getattr(lyr, "description", None)
        description = raw_desc if raw_desc is not None else existing.description

        summary = existing.summary
        citation = existing.citation

        # -- Build meta_data for RasterLayerConfig ------------------------------
        meta_data = existing.meta_data  # None for all current definitions
        if isinstance(lyr, RasterLayerConfig):
            meta_items: list[Meta] = []
            if lyr.source_url:
                meta_items.append(Meta("SourceUrl", lyr.source_url, "url"))
            if lyr.data_product_version:
                meta_items.append(Meta("DataProductVersion", lyr.data_product_version))
            if lyr.docs_url:
                meta_items.append(Meta("DocsUrl", lyr.docs_url, "url"))
            abs_path = os.path.join(output_folder, path)
            if os.path.isfile(abs_path):
                try:
                    from rscontextneo.src.raster_clip import (  # pylint: disable=import-outside-toplevel
                        get_raster_cell_size,
                    )
                    cx, cy = get_raster_cell_size(abs_path)
                    meta_items.append(Meta("CellSizeX", str(cx)))
                    meta_items.append(Meta("CellSizeY", str(cy)))
                except Exception as exc:  # pylint: disable=broad-except
                    log.warning(f"  Could not read cell size for {lid}: {exc}")
            if meta_items:
                meta_data = MetaData(meta_items)

        # -- Reconstruct object (never mutate existing) -------------------------
        if isinstance(existing, Geopackage):
            # For S3Tables layers: derive sub-layers from config layer_name
            if isinstance(lyr, S3TablesLayerConfig):
                sub_layers = [
                    GeopackageLayer(
                        lyr_name=lyr.layer_name,
                        name=lyr.layer_name.capitalize(),
                        ds_type=GeoPackageDatasetTypes.VECTOR,
                    )
                ]
            else:
                sub_layers = existing.layers
            merged[lid] = Geopackage(
                xml_id=lid,
                name=name,
                path=path,
                layers=sub_layers,
                description=description,
                summary=summary,
                citation=citation,
                meta_data=meta_data,
            )
        else:
            merged[lid] = Dataset(
                xml_id=lid,
                name=name,
                path=path,
                ds_type=existing.ds_type,
                description=description,
                summary=summary,
                citation=citation,
                meta_data=meta_data,
            )

    return merged


def write_project_xml(
    output_folder: str,
    descriptor: str,
    bounds_geojson: str,
    meta: dict[str, str],
    aoi: str | None,
    dem: str | None,
    elapsed_time: float,
    log: Logger,
    debug: bool = False,
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
        Short human-readable description of the input source.
    bounds_geojson : str
        Absolute path to the project bounds GeoJSON (WGS84).
    meta : dict[str, str]
        Extra metadata key=value pairs supplied by the caller.
    aoi : str or None
        Original AOI path (recorded as hidden metadata).
    dem : str or None
        Original DEM path (recorded as hidden metadata).
    elapsed_time : float
        Total processing time in seconds. Recorded as ``ProcTimeS`` (hidden)
        and ``Processing Time`` (human-readable).
    log : Logger
        Caller-supplied logger.
    debug : bool
        If True, the BREACH_DIFF_POINTS debug layer is registered.
    """
    log.info("Writing project XML")

    cfg = AppConfig.get()
    merged_layer_types = _build_merged_layer_types(cfg.layers, output_folder, log)
    threshold = cfg.hydrology.threshold
    output_res = cfg.dem.resolution
    breach_dist = cfg.hydrology.breach_dist
    dem_source = cfg.dem.source

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
        merged_layer_types["DEM"],
        merged_layer_types["HILLSHADE"],
        merged_layer_types["SLOPE"],
        merged_layer_types["DEM_FILLED"],
        merged_layer_types["DEM_BREACH"],
        merged_layer_types["D8_FLOW"],
        merged_layer_types["D8_CONTRIB_AREA"],
        merged_layer_types["STREAM_RASTER"],
        merged_layer_types["STREAM_ORDER"],
        merged_layer_types["HYDRODERIVATIVES"],
        merged_layer_types["SUBWATERSHEDS"],
    ]
    if aoi is not None and dem_source == "tnm":
        candidate_datasets.append(merged_layer_types["TILE_FOOTPRINTS"])
    if debug:
        candidate_datasets.append(merged_layer_types["BREACH_DIFF_POINTS"])

    # Register optional layers declared in the config.
    # When a config layer_id has a matching entry in layer_definitions.json,
    # _build_merged_layer_types() has already produced a merged Dataset/Geopackage
    # in merged_layer_types.  Use that entry directly.
    # Fall back to inline construction only when there is no definition entry.
    for layer_cfg in cfg.layers:
        lid = layer_cfg.layer_id
        if lid in merged_layer_types:
            candidate_datasets.append(merged_layer_types[lid])
        elif isinstance(layer_cfg, S3TablesLayerConfig):
            transport_ds = Geopackage(
                xml_id=lid,
                name=layer_cfg.label,
                path=layer_cfg.output_path,
                layers=[
                    GeopackageLayer(
                        lyr_name=layer_cfg.layer_name,
                        name=layer_cfg.layer_name.capitalize(),
                        ds_type=GeoPackageDatasetTypes.VECTOR,
                    )
                ],
                description="Transportation features sourced from AWS S3 Tables via Athena.",
            )
            candidate_datasets.append(transport_ds)
        elif isinstance(layer_cfg, RasterLayerConfig):
            from rscontextneo.src.raster_clip import (  # pylint: disable=import-outside-toplevel
                get_raster_cell_size,
            )
            abs_path = os.path.join(output_folder, layer_cfg.output_path)
            meta_items: list[Meta] = []
            if layer_cfg.source_url:
                meta_items.append(Meta("SourceUrl", layer_cfg.source_url, "url"))
            if layer_cfg.data_product_version:
                meta_items.append(
                    Meta("DataProductVersion", layer_cfg.data_product_version)
                )
            if layer_cfg.docs_url:
                meta_items.append(Meta("DocsUrl", layer_cfg.docs_url, "url"))
            if os.path.isfile(abs_path):
                try:
                    cx, cy = get_raster_cell_size(abs_path)
                    meta_items.append(Meta("CellSizeX", str(cx)))
                    meta_items.append(Meta("CellSizeY", str(cy)))
                except Exception as exc:  # pylint: disable=broad-except
                    log.warning(
                        f"  Could not read cell size for {lid}: {exc}"
                    )
            raster_ds = Dataset(
                xml_id=lid,
                name=layer_cfg.label,
                path=layer_cfg.output_path,
                ds_type="Raster",
                meta_data=MetaData(meta_items) if meta_items else None,
                description=layer_cfg.description,
            )
            candidate_datasets.append(raster_ds)

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
