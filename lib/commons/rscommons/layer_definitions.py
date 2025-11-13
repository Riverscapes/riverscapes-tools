"""Helpers for Riverscapes layer definition manifests."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict


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


def load_layer_definitions(path: str) -> Dict[str, LayerDefinition]:
    """Load layer definitions that follow the unified Riverscapes schema."""

    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict) or "layers" not in payload:
        raise ValueError(f"Unsupported layer definitions format in {path}")

    definitions: Dict[str, LayerDefinition] = {}
    for entry in payload.get("layers", []):
        definition = LayerDefinition(
            layer_id=entry["layer_id"],
            layer_name=entry.get("layer_name", entry["layer_id"]),
            layer_type=entry.get("layer_type"),
            path=entry.get("path"),
            description=entry.get("description"),
            source_url=entry.get("source_url"),
            data_product_version=entry.get("data_product_version"),
            columns=_parse_columns(entry.get("columns")),
        )
        definitions.setdefault(definition.layer_id, definition)
        if definition.layer_name:
            definitions.setdefault(definition.layer_name, definition)

    return definitions
