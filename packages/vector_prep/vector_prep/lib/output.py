"""output_gdf: write a cleaned GeoDataFrame to GeoPackage in EPSG:4326."""
from __future__ import annotations

import os
from typing import Dict

import geopandas as gpd
from rsxml import Logger

# This module always produces output in GeoPackage format
OUTPUT_DRIVER = "GPKG"


def output_gdf_chunk(
    chunk_gdf: gpd.GeoDataFrame,
    output_dataset: str,
    layer_name: str | None,
    first_chunk: bool,
) -> None:
    """Write a single chunk to a GeoPackage incrementally.

    On the first chunk the file is (re-)created from scratch; subsequent chunks
    are appended so the full dataset accumulates without holding everything in
    memory.

    Args:
        chunk_gdf: GeoDataFrame slice to write (already cleaned).
        output_dataset: File path for the output GeoPackage.
        layer_name: Layer name inside the GeoPackage; defaults to the filename stem.
        first_chunk: When True the existing file is removed and a fresh file is
            created.  When False the chunk is appended to the existing file.
    """
    log = Logger("Output GDF")

    layername = (
        layer_name
        if layer_name
        else os.path.splitext(os.path.basename(output_dataset))[0]
    )

    if first_chunk and os.path.exists(output_dataset):
        try:
            if not os.path.isdir(output_dataset):
                os.remove(output_dataset)
                log.info(f"Removed existing output file: {output_dataset}")
        except Exception:
            log.debug("Could not remove existing output file prior to first chunk write.")

    # Reproject to EPSG:4326 for the output file.
    chunk_out = chunk_gdf.to_crs(epsg=4326)

    write_mode = "w" if first_chunk else "a"
    try:
        chunk_out.to_file(
            output_dataset,
            driver=OUTPUT_DRIVER,
            layer=layername,
            mode=write_mode,
        )
    except Exception as e:
        raise Exception(f"Failed to write output chunk (first={first_chunk}): {e}") from e


def output_gdf(gdf: gpd.GeoDataFrame, output_dataset: str, layer_name: str | None) -> None:
    """Save the GeoDataFrame to a GeoPackage in EPSG:4326.

    Args:
        gdf: GeoDataFrame to write.
        output_dataset: File path for the output GeoPackage.
        layer_name: Layer name to use inside the GeoPackage.  If None the
            output filename stem is used.
    """
    log = Logger("Output GDF")

    if os.path.exists(output_dataset):
        try:
            if not os.path.isdir(output_dataset):
                os.remove(output_dataset)
                log.info(f"Overwrote existing file: {output_dataset}")
        except Exception:
            log.debug("Could not remove existing file prior to write (continuing)...")

    log.info("Reprojecting to EPSG:4326 for output")
    gdf = gdf.to_crs(epsg=4326)

    log.info(f"Writing cleaned layer to {output_dataset} (driver={OUTPUT_DRIVER})...")
    try:
        write_kwargs: Dict = {}
        if OUTPUT_DRIVER == "GPKG":
            layername = (
                layer_name
                if layer_name
                else os.path.splitext(os.path.basename(output_dataset))[0]
            )
            write_kwargs["layer"] = layername
        gdf.to_file(output_dataset, driver=OUTPUT_DRIVER, **write_kwargs)
        log.info("Write complete.")
    except Exception as e:
        raise Exception(f"Failed to write output: {e}") from e
