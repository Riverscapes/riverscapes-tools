"""Utility to convert legacy VBET layer metadata to the unified schema.

Usage example:

.. code-block:: bash

   python -m vbet.scripts.convert_layer_descriptions \
       packages/vbet/vbet/layer_descriptions.json

Running without a destination argument converts the file in place. Use the
optional ``dest`` argument to write to a new path instead."""
from __future__ import annotations
import argparse
import json
import os
import shutil
from collections.abc import Iterator
from rscommons.classes.rs_project import RSLayer


LayerTypes = {
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'inputs/dem.tif'),
    'SLOPE_RASTER': RSLayer('Slope Raster', 'SLOPE_RASTER', 'Raster', 'inputs/slope.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/vbet_inputs.gpkg', {
        'NETWORK_INTERSECTION': RSLayer('NHD Flowlines intersected with road, rail and ownership', 'NETWORK_INTERSECTION', 'Vector', 'network_intersected'),
        # 'FLOWLINES': RSLayer('NHD Flowlines intersected with road, rail and ownership', 'FLOWLINES', 'Vector', 'flowlines'),
        'CHANNEL_AREA_POLYGONS': RSLayer('Channel Area Polygons', 'CHANNEL_AREA_POLYGONS', 'Vector', 'channel_area_polygons')
    }),
    # Taudem intermediate rasters can be provided as inputs, or generated in vbet
    'PITFILL': RSLayer('TauDEM Pitfill', 'PITFILL', 'Raster', 'intermediates/pitfill.tif'),
    'DINFFLOWDIR_ANG': RSLayer('TauDEM D-Inf Flow Directions', 'DINFFLOWDIR_ANG', 'Raster', 'intermediates/dinfflowdir_ang.tif'),
    'DINFFLOWDIR_SLP': RSLayer('TauDEM D-Inf Flow Directions Slope', 'DINFFLOWDIR_SLP', 'Raster', 'intermediates/dinfflowdir_slp.tif'),
    'INTERMEDIATES': RSLayer('Intermediates', 'Intermediates', 'Geopackage', 'intermediates/vbet_intermediates.gpkg', {
        'VBET_DGO_POLYGONS': RSLayer('VBET DGO Polygons', 'VBET_DGO_POLYGONS', 'Vector', 'vbet_dgos')
        # We also add all tht raw thresholded shapes here but they get added dynamically later
    }),
    # Same here. Sub layers are added dynamically later.
    'COMPOSITE_VBET_EVIDENCE': RSLayer('VBET Evidence Raster', 'VBET_EVIDENCE', 'Raster', 'outputs/vbet_evidence.tif'),
    'COMPOSITE_VBET_EVIDENCE_INTERIOR': RSLayer('Topo Evidence (Interior)', 'EVIDENCE_TOPO_INTERIOR', 'Raster', 'intermediates/topographic_evidence_interior.tif'),

    'COMPOSITE_HAND': RSLayer('Hand Raster', 'HAND_RASTER', 'Raster', 'intermediates/hand_composite.tif'),
    'COMPOSITE_HAND_INTERIOR': RSLayer('Hand Raster (Interior)', 'HAND_RASTER_INTERIOR', 'Raster', 'intermediates/hand_composite_interior.tif'),

    'TRANSFORMED_HAND': RSLayer('Transformed HAND Evidence', 'TRANSFORMED_HAND', 'Raster', 'intermediates/hand_transformed.tif'),
    'TRANSFORMED_HAND_INTERIOR': RSLayer('Transformed HAND Evidence (Interior)', 'TRANSFORMED_HAND_INTERIOR', 'Raster', 'intermediates/hand_transformed_interior.tif'),

    'EVIDENCE_TOPO': RSLayer('Topo Evidence', 'EVIDENCE_TOPO', 'Raster', 'intermediates/topographic_evidence.tif'),

    'TRANSFORMED_SLOPE': RSLayer('Transformed Slope', 'TRANSFORMED_SLOPE', 'Raster', 'intermediates/slope_transformed.tif'),
    'TRANSFORMED_SLOPE_INTERIOR': RSLayer('Transformed Slope (Interior)', 'TRANSFORMED_SLOPE_INTERIOR', 'Raster', 'intermediates/slope_transformed_interior.tif'),

    'VBET_ZONES': RSLayer('VBET LevelPath Zones', 'VBET_ZONES', 'Raster', 'intermediates/vbet_level_path_zones.tif'),
    'LOWLYING_FP_ZONES': RSLayer('Active Floodplain LevelPath Zones', 'LOWLYING_FP_ZONES', 'Raster', 'intermediates/lowlying_fp_level_path_zones.tif'),
    'ELEVATED_FP_ZONES': RSLayer('Inactive Floodplain LevelPath Zones', 'ELEVATED_FP_ZONES', 'Raster', 'intermediates/elevated_fp_level_path_zones.tif'),

    'VBET_OUTPUTS': RSLayer('VBET', 'VBET_OUTPUTS', 'Geopackage', 'outputs/vbet.gpkg', {
        'VBET_FULL': RSLayer('VBET Full Extent', 'VBET_FULL', 'Vector', 'vbet_full'),
        'VBET_IA': RSLayer('VBET Low lying/Elevated Boundary', 'VBET_IA', 'Vector', 'low_lying_valley_bottom'),
        'LOW_LYING_FLOODPLAIN': RSLayer('Low Lying Floodplain', 'LOW_LYING_FLOODPLAIN', 'Vector', 'low_lying_floodplain'),
        'ELEVATED_FLOODPLAIN': RSLayer('Elevated Floodplain', 'ELEVATED_FLOODPLAIN', 'Vector', 'elevated_floodplain'),
        'FLOODPLAIN': RSLayer('Floodplain', 'FLOODPLAIN', 'Vector', 'floodplain'),
        'VBET_CENTERLINES': RSLayer('VBET Centerline', 'VBET_CENTERLINES', 'Vector', 'vbet_centerlines'),
        'SEGMENTATION_POINTS': RSLayer('Segmentation Points', 'SEGMENTATION_POINTS', 'Vector', 'vbet_igos')
    }),
    'REPORT': RSLayer('VBET Report', 'REPORT', 'HTMLFile', 'outputs/vbet.html')
}


SCHEMA_URL = "https://xml.riverscapes.net/riverscapes_metadata/schema/layer_definitions.schema.json"


def _layer_lookup(layer_types: dict[str, RSLayer]) -> dict[str, tuple[RSLayer, str]]:
    """Generate a mapping of layer keys to their metadata and paths."""

    lookup: dict[str, tuple[RSLayer, str]] = {}
    for layer_key, rs_layer in layer_types.items():
        lookup[layer_key] = (rs_layer, rs_layer.rel_path)
        if rs_layer.id not in lookup:
            lookup[rs_layer.id] = (rs_layer, rs_layer.rel_path)
        if rs_layer.sub_layers:
            for sub_key, sub in rs_layer.sub_layers.items():
                sub_path = f"{rs_layer.rel_path}/{sub.rel_path}" if rs_layer.rel_path else sub.rel_path
                lookup[sub_key] = (sub, sub_path)
                if sub.id not in lookup:
                    lookup[sub.id] = (sub, sub_path)
    return lookup


def _load_legacy_layers(path: str) -> Iterator[tuple[str, tuple[str, str, str]]]:
    """Yield legacy layer metadata tuples from the given JSON file."""
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    for layer_id, values in data.items():
        description, source_url, version = (values + [""] * 3)[:3]
        yield layer_id, (description, source_url, version)


def convert_legacy_layer_descriptions(
    src_path: str,
    dst_path: str,
    authority_name: str,
    tool_schema_version: str,
) -> None:
    """Write the unified layer-definition payload for a legacy JSON file."""
    layer_lookup = _layer_lookup(LayerTypes)
    new_layers = []
    for layer_id, (description, source_url, version) in _load_legacy_layers(src_path):
        if layer_id not in layer_lookup:
            raise KeyError(f"Unknown layer id '{layer_id}' in {src_path}")
        rs_layer, layer_path = layer_lookup[layer_id]
        layer_entry = {
            "layer_id": layer_id,
            "layer_name": rs_layer.name,
            "layer_type": rs_layer.tag,
            "description": description,
        }
        if layer_path:
            layer_entry["path"] = layer_path
        if source_url:
            layer_entry["source_url"] = source_url
        if version:
            layer_entry["data_product_version"] = version
        new_layers.append(layer_entry)

    output_payload = {
        "$schema": SCHEMA_URL,
        "authority_name": authority_name,
        "tool_schema_version": tool_schema_version,
        "layers": new_layers,
    }

    os.makedirs(os.path.dirname(dst_path) or ".", exist_ok=True)
    if os.path.abspath(src_path) == os.path.abspath(dst_path) and os.path.isfile(src_path):
        stem, ext = os.path.splitext(dst_path)
        backup_path = f"{stem}_legacy{ext}"
        shutil.copyfile(src_path, backup_path)
    with open(dst_path, "w", encoding="utf-8") as handle:
        json.dump(output_payload, handle, indent=2)
        handle.write("\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert VBET layer descriptions to unified schema")
    parser.add_argument("src", help="Path to legacy layer_descriptions.json")
    parser.add_argument("dest", nargs="?", help="Destination file (defaults to src)")
    parser.add_argument("--authority-name", default="vbet", help="authority_name value to embed")
    parser.add_argument(
        "--tool-schema-version",
        default='1.0.0',
        help="tool_schema_version value to embed",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    dest_path = args.dest or args.src
    convert_legacy_layer_descriptions(
        src_path=args.src,
        dst_path=dest_path,
        authority_name=args.authority_name,
        tool_schema_version=args.tool_schema_version,
    )
    print(f'Converted file: {dest_path}')


if __name__ == "__main__":
    main()
