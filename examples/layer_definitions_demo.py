"""Demonstrate legacy ``LayerTypes`` usage vs. new schema-driven helpers."""

from __future__ import annotations
from rscommons.classes.rs_project import RSLayer  # type: ignore[import-not-found]
from rscommons.layer_definitions import build_layer_types, load_layer_definitions  # type: ignore[import-not-found]
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
COMMONS = ROOT / "lib" / "commons"
for path in (ROOT, COMMONS):
    if str(path) not in sys.path:
        sys.path.append(str(path))


def legacy_layer_types() -> dict[str, RSLayer]:
    """Classic hand-authored ``LayerTypes`` dictionary."""

    return {
        "DEM": RSLayer("DEM", "DEM", "Raster", "inputs/dem.tif"),
        "INTERMEDIATES": RSLayer(
            "Intermediates",
            "INTERMEDIATES",
            "Geopackage",
            "intermediates/intermediates.gpkg",
            {
                "SOME_LAYER": RSLayer(
                    "Example Layer",
                    "SOME_LAYER",
                    "Vector",
                    "example_layer",
                ),
            },
        ),
    }


def schema_driven_layer_types(definitions_path: Path) -> dict[str, RSLayer]:
    """Modern pattern: build ``LayerTypes`` from the JSON manifest."""

    definitions = load_layer_definitions(str(definitions_path))
    return build_layer_types(definitions.values())


def tool_main():
    LayerTypes = legacy_layer_types()
    print("Legacy keys:", sorted(LayerTypes))

    manifest_path = Path(__file__).resolve().parent / ".." / "packages" / "vbet" / "vbet" / "layer_definitions.json"
    generated = schema_driven_layer_types(manifest_path)
    print("Generated keys:", sorted(generated))
    for key, layer in generated.items():
        if layer.sub_layers:
            print(f"Layer {key} sublayers:", sorted(layer.sub_layers))
            break


if __name__ == "__main__":
    tool_main()
