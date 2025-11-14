"""Utility to convert legacy VBET layer metadata to the unified schema.

Usage example:

.. code-block:: bash

   python -m vbet.scripts.convert_layer_descriptions \
       packages/vbet/vbet/layer_descriptions.json

Running without a destination argument converts the file in place. Use the
optional ``dest`` argument to write to a new path instead.
Written mostly by copilot, guided by Lorin Gaertner.
"""
from __future__ import annotations
import argparse
import json
import os
import shutil
from collections.abc import Iterator

from riverscapes_metadata import SCHEMA_URL
from rscommons.classes.rs_project import RSLayer
from vbet.vbet import LayerTypes


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
    unique_layers: dict[int, tuple[RSLayer, str]] = {}
    legacy_aliases: dict[str, str] = {}
    for key, (rs_layer, layer_path) in layer_lookup.items():
        existing = unique_layers.get(id(rs_layer))
        if existing is None or (not existing[1] and layer_path):
            unique_layers[id(rs_layer)] = (rs_layer, layer_path)
        if key != rs_layer.id:
            legacy_aliases[key] = rs_layer.id

    legacy_metadata_raw = dict(_load_legacy_layers(src_path))
    legacy_metadata: dict[str, tuple[str, str, str]] = {}
    unknown_ids: list[str] = []
    for legacy_id, metadata in legacy_metadata_raw.items():
        lookup_entry = layer_lookup.get(legacy_id)
        if lookup_entry is None:
            unknown_ids.append(legacy_id)
            continue
        layer, _ = lookup_entry
        legacy_metadata[layer.id] = metadata
    if unknown_ids:
        missing = ", ".join(sorted(unknown_ids))
        raise KeyError(f"Unknown layer id(s) {missing} in {src_path}")
    new_layers: list[dict[str, str]] = []
    for rs_layer, layer_path in unique_layers.values():
        description, source_url, version = legacy_metadata.get(
            rs_layer.id, ("", "", "")
        )
        layer_entry: dict[str, str] = {
            "layer_id": rs_layer.id,
            "layer_name": rs_layer.name,
            "layer_type": rs_layer.tag,
        }
        if description:
            layer_entry["description"] = description
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

    if legacy_aliases:
        alias_path = os.path.splitext(dst_path)[0] + "_aliases.json"
        with open(alias_path, "w", encoding="utf-8") as handle:
            json.dump(legacy_aliases, handle, indent=2)
            handle.write("\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert VBET layer descriptions to unified schema")
    parser.add_argument("src", help="Path to legacy layer_descriptions.json")
    parser.add_argument("dest", nargs="?", help="Destination file (defaults to layer_definitions.json)")
    parser.add_argument("--authority-name", default="vbet", help="authority_name value to embed")
    parser.add_argument(
        "--tool-schema-version",
        default='1.0.0',
        help="tool_schema_version value to embed",
    )
    args = parser.parse_args()
    if not args.dest:
        # If no destination is provided, use layer_definitions.json in the same directory as src
        args.dest = os.path.join(os.path.dirname(args.src), "layer_definitions.json")
    if os.path.abspath(args.src) == os.path.abspath(args.dest):
        parser.error("Input and output file paths must be different.")
    return args


def main() -> None:
    args = _parse_args()
    convert_legacy_layer_descriptions(
        src_path=args.src,
        dst_path=args.dest,
        authority_name=args.authority_name,
        tool_schema_version=args.tool_schema_version,
    )
    print(f'Converted file: {args.dest}')


if __name__ == "__main__":
    main()
