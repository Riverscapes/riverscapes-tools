"""
Select, rename, and retype fields in a GeoPackage vector layer.

Takes an input GeoPackage layer and a field mapping (supplied as a JSON file or
JSON string) that specifies:
  - Which existing fields to keep (any fields not in the mapping are dropped)
  - New output field names
  - Output field data types: "int", "float", or "string"

Geometries are preserved unchanged. CRS is not altered.

Typically used as a post-processing step after vector_prep.py.

Field mapping JSON structure:
  {
    "<existing_field>": {"name": "<new_field>", "type": "<int|float|string>"},
    ...
  }

Philip Bailey
9 Jun 2026
"""
import argparse
import sys
import os
import json
import sqlite3
import traceback
from pathlib import Path
from typing import Dict

import geopandas as gpd
import pandas as pd
from rsxml import Logger, dotenv

OUTPUT_DRIVER = "GPKG"

SUPPORTED_TYPES = {"int", "float", "string"}


def vector_prep_fields(
    input_dataset: Path | str,
    layer_name: str,
    field_map: Dict[str, dict],
    output_dataset: Path | str,
    output_layer: str | None = None,
    set_empty_null: bool = True,
    trim_strings: bool = True,
) -> None:
    """
    Select, rename, and retype fields from a GeoPackage layer.

    Args:
        input_dataset:   Path to the input GeoPackage.
        layer_name:      Name of the layer to read from the input GeoPackage.
        field_map:       Dict keyed by existing field name, values are dicts with
                         keys "name" (new field name) and "type" ("int", "float", or "string").
        output_dataset:  Path to the output GeoPackage.
        output_layer:    Layer name to write in the output GeoPackage. Defaults to
                         the input layer name.
        set_empty_null:  Replace empty strings with None in string fields (default: True).
        trim_strings:    Strip leading/trailing whitespace from string fields (default: True).
    """
    log = Logger("Vector Prep Fields")
    input_dataset = Path(input_dataset)
    output_dataset = Path(output_dataset)

    if not input_dataset.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_dataset}")

    # Validate field map structure
    for existing_name, spec in field_map.items():
        if "name" not in spec:
            raise ValueError(f"Field mapping for '{existing_name}' is missing required key 'name'.")
        if "type" not in spec:
            raise ValueError(f"Field mapping for '{existing_name}' is missing required key 'type'.")
        if spec["type"] not in SUPPORTED_TYPES:
            raise ValueError(
                f"Field mapping for '{existing_name}' has unsupported type '{spec['type']}'. "
                f"Supported types: {SUPPORTED_TYPES}"
            )

    log.info(f"Reading layer '{layer_name}' from: {input_dataset}")
    try:
        gdf = gpd.read_file(input_dataset, layer=layer_name)
    except Exception as e:
        raise Exception(f"Failed to read layer '{layer_name}' from '{input_dataset}': {e}") from e

    log.info(f"Loaded {len(gdf):,} features. CRS: {gdf.crs}")

    # Check that all mapped fields actually exist in the source layer
    existing_cols = set(gdf.columns)
    missing = [f for f in field_map if f not in existing_cols]
    if missing:
        raise ValueError(
            f"The following field(s) in the mapping do not exist in layer '{layer_name}': {missing}\n"
            f"Available fields: {sorted(existing_cols - {'geometry'})}"
        )

    # Select only the mapped columns plus geometry
    cols_to_keep = list(field_map.keys())
    log.info(f"Keeping {len(cols_to_keep)} field(s) (of {len(existing_cols) - 1} non-geometry fields): {cols_to_keep}")
    gdf = gdf[cols_to_keep + ["geometry"]]

    # Rename columns
    rename_map = {existing: spec["name"] for existing, spec in field_map.items()}
    gdf = gdf.rename(columns=rename_map)
    log.info(f"Renamed columns: {rename_map}")

    # Retype columns
    type_errors = []
    for existing_name, spec in field_map.items():
        new_name = spec["name"]
        target_type = spec["type"]
        try:
            if target_type == "int":
                gdf[new_name] = pd.to_numeric(gdf[new_name], errors="coerce").astype("Int64")
            elif target_type == "float":
                gdf[new_name] = pd.to_numeric(gdf[new_name], errors="coerce").astype("float64")
            elif target_type == "string":
                gdf[new_name] = gdf[new_name].where(gdf[new_name].notna(), other=None).astype(str)
                # Restore NaN/None rather than the string "None" / "nan"
                gdf[new_name] = gdf[new_name].apply(
                    lambda x: None if x in ("None", "nan", "<NA>", "") else x
                )
                if trim_strings:
                    gdf[new_name] = gdf[new_name].apply(
                        lambda x: x.strip() if isinstance(x, str) else x
                    )
                if set_empty_null:
                    gdf[new_name] = gdf[new_name].apply(
                        lambda x: None if isinstance(x, str) and x == "" else x
                    )
            log.debug(f"  '{new_name}': cast to {target_type}")
        except Exception as e:
            type_errors.append(f"  '{new_name}' -> {target_type}: {e}")

    if type_errors:
        raise Exception("Type conversion errors:\n" + "\n".join(type_errors))

    # Determine output layer name
    out_layer = output_layer or layer_name

    # Remove existing output file if present
    if output_dataset.exists():
        try:
            os.remove(output_dataset)
            log.info(f"Removed existing output file: {output_dataset}")
        except Exception as exc:
            log.debug(f"Could not remove existing output file (continuing): {exc}")

    log.info(f"Writing {len(gdf):,} features to layer '{out_layer}' in: {output_dataset}")
    try:
        gdf.to_file(output_dataset, layer=out_layer, driver=OUTPUT_DRIVER)
        log.info("Write complete.")
    except Exception as e:
        raise Exception(f"Failed to write output: {e}") from e

    log.info(f"Output fields: {[c for c in gdf.columns if c != 'geometry']}")

    log.info("Running VACUUM on output GeoPackage...")
    try:
        with sqlite3.connect(output_dataset) as conn:
            conn.execute("VACUUM")
        log.info("VACUUM complete.")
    except Exception as e:
        log.warning(f"VACUUM failed (non-fatal): {e}")


def load_field_map(field_map_arg: str) -> dict:
    """Accept a path to a JSON file or a raw JSON string."""
    path = Path(field_map_arg)
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    # Try parsing as a literal JSON string
    try:
        return json.loads(field_map_arg)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"--fields argument is neither a valid file path nor valid JSON: {e}"
        ) from e


def main():
    parser = argparse.ArgumentParser(description=("Vector Prep Fields: select, rename, and retype fields in a GeoPackage layer."))
    parser.add_argument("input", help="Path to the input GeoPackage.")
    parser.add_argument("layer", help="Layer name to read from the input GeoPackage.")
    parser.add_argument("fields",help="Field map. Path to a JSON file or a JSON string. 'Structure: {\"existing_field\": {\"name\": \"new_field\", \"type\": \"int|float|string\"}, ...}'"),
    parser.add_argument("output", help="Path to the output GeoPackage.")
    parser.add_argument("--output-layer", help="Layer name to write in the output GeoPackage. Defaults to the input layer name.", default=None)
    parser.add_argument("--no-set-empty-null", help="Disable replacing empty strings with null in string fields.", action="store_true", default=False)
    parser.add_argument("--no-trim-strings", help="Disable stripping leading/trailing whitespace from string fields.", action="store_true", default=False)
    parser.add_argument("--verbose", help="Extra logging.", action="store_true", default=False)
    args = dotenv.parse_args_env(parser)

    log = Logger("Vector Prep Fields")
    log.setup(log_path=os.path.join(os.path.dirname(args.output), "vector_prep_fields.log"),verbose=args.verbose,)

    try:
        field_map = load_field_map(args.fields)
        vector_prep_fields(
            input_dataset=args.input,
            layer_name=args.layer,
            field_map=field_map,
            output_dataset=args.output,
            output_layer=args.output_layer,
            set_empty_null=not args.no_set_empty_null,
            trim_strings=not args.no_trim_strings,
        )
    except Exception as e:
        log.error("Vector prep fields failed: %s", e)
        log.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
