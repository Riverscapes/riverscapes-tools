"""Helpers for Riverscapes layer definition manifests.
Once this is stable, consider porting to RiverscapesXML and add accompanying tests there
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable

from riverscapes_metadata import SCHEMA_URL
from rscommons.classes.rs_project import RSLayer


@dataclass
class LayerColumn:
    """Representation of a column (or raster band) definition."""

    name: str
    dtype: str | None = None
    friendly_name: str | None = None
    theme: str | None = None
    data_unit: str | None = None
    description: str | None = None
    is_key: bool | None = None
    is_required: bool | None = None
    default_value: Any | None = None
    preferred_bin_definition: str | None = None


@dataclass
class LayerDefinition:
    """Unified description of a Riverscapes layer."""

    layer_id: str
    layer_name: str
    layer_type: str | None = None
    path: str | None = None
    description: str | None = None
    source_url: str | None = None
    data_product_version: str | None = None
    theme: str | None = None
    columns: list[LayerColumn] = field(default_factory=list)


def _parse_columns(raw_columns: list[dict[str, Any]] | None) -> list[LayerColumn]:
    if not raw_columns:
        return []
    columns: list[LayerColumn] = []
    for raw in raw_columns:
        column = LayerColumn(
            name=raw.get("name"),
            dtype=raw.get("dtype"),
            friendly_name=raw.get("friendly_name"),
            theme=raw.get("theme"),
            data_unit=raw.get("data_unit"),
            description=raw.get("description"),
            is_key=raw.get("is_key"),
            is_required=raw.get("is_required"),
            default_value=raw.get("default_value"),
            preferred_bin_definition=raw.get("preferred_bin_definition"),
        )
        columns.append(column)
    return columns


def load_layer_definitions(path: str) -> dict[str, LayerDefinition]:
    """Load and validate layer definitions that follow the unified Riverscapes schema."""

    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict) or "layers" not in payload:
        raise ValueError(f"Unsupported layer definitions format in {path}")

    schema_ref = payload.get("$schema")
    if schema_ref != SCHEMA_URL:
        raise ValueError(
            f"Layer definitions file {path} declares schema '{schema_ref}', expected '{SCHEMA_URL}'"
        )

    definitions: dict[str, LayerDefinition] = {}
    for entry in payload.get("layers", []):
        definition = LayerDefinition(
            layer_id=entry["layer_id"],
            layer_name=entry.get("layer_name", entry["layer_id"]),
            layer_type=entry.get("layer_type"),
            path=entry.get("path"),
            description=entry.get("description"),
            source_url=entry.get("source_url"),
            data_product_version=entry.get("data_product_version"),
            theme=entry.get("theme"),
            columns=_parse_columns(entry.get("columns")),
        )
        definitions.setdefault(definition.layer_id, definition)

    return definitions


def _split_container_path(path: str | None) -> tuple[str | None, str | None]:
    """Split a layer path into container and sublayer components."""

    if not path:
        return None, None
    if ".gpkg/" in path:
        container, sub_layer = path.split(".gpkg/", 1)
        return f"{container}.gpkg", sub_layer
    if ".gdb/" in path:
        container, sub_layer = path.split(".gdb/", 1)
        return f"{container}.gdb", sub_layer
    if ".sqlite/" in path:
        container, sub_layer = path.split(".sqlite/", 1)
        return f"{container}.sqlite", sub_layer
    return path, None


def build_layer_types(definitions: Iterable[LayerDefinition]) -> dict[str, RSLayer]:
    """Construct an RSLayer hierarchy keyed by layer_id from layer definitions.
    In riverscapes tools, this is often declared as global variable named LayerTypes. 
    A newer preferred approach would bypass this construction and 
    instead use the LayerDefinition dataclass from this module.
    """

    layer_defs: list[LayerDefinition] = list(definitions)
    container_map: dict[str, RSLayer] = {}
    layer_types: dict[str, RSLayer] = {}

    def register_container(key: str | None, layer: RSLayer) -> None:
        if key and key not in container_map:
            container_map[key] = layer

    # First pass: create top-level layers (including geopackage containers)
    for definition in layer_defs:
        _, sub_layer_name = _split_container_path(definition.path)
        if sub_layer_name:
            continue
        is_geopackage = definition.layer_type == "Geopackage"
        rs_layer = RSLayer(
            name=definition.layer_name,
            lyr_id=definition.layer_id,
            tag=definition.layer_type or "Vector",
            rel_path=definition.path or "",
            sub_layers={},
        ) if is_geopackage else RSLayer(
            name=definition.layer_name,
            lyr_id=definition.layer_id,
            tag=definition.layer_type or "Vector",
            rel_path=definition.path or "",
        )
        layer_types.setdefault(definition.layer_id, rs_layer)
        register_container(definition.path, rs_layer)
        if is_geopackage:
            if rs_layer.sub_layers is None:
                rs_layer.sub_layers = {}
            register_container(definition.layer_id, rs_layer)

    # Second pass: attach sublayers to their containers
    for definition in layer_defs:
        container_path, sub_layer_name = _split_container_path(definition.path)
        if not sub_layer_name:
            continue
        parent_layer = container_map.get(container_path)
        if parent_layer is None and container_path:
            container_hint = container_path.split("/", 1)[0].upper()
            parent_layer = container_map.get(container_hint)
        if parent_layer is None:
            # Fallback: create a container if the manifest omitted it
            parent_layer = RSLayer(
                name=container_path or definition.layer_id,
                lyr_id=container_path or definition.layer_id,
                tag="Geopackage",
                rel_path=container_path or "",
                sub_layers={},
            )
            register_container(container_path, parent_layer)
            register_container(parent_layer.id, parent_layer)
            layer_types.setdefault(parent_layer.id, parent_layer)
        if parent_layer.sub_layers is None:
            parent_layer.sub_layers = {}
        sub_layer = RSLayer(
            name=definition.layer_name,
            lyr_id=definition.layer_id,
            tag=definition.layer_type or "Vector",
            rel_path=sub_layer_name,
        )
        parent_layer.sub_layers.setdefault(definition.layer_id, sub_layer)
        layer_types.setdefault(definition.layer_id, sub_layer)

    return layer_types
