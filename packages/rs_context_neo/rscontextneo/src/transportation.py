"""
Transportation layer fetcher for RS Context Neo.

Fetches rail and/or roads data from AWS Athena S3 Tables and writes them
into a GeoPackage under the project output folder.

Author:     Matt Reimer
Date:       2026-06-02
"""

import os
from typing import Optional

from rsxml import Logger
from rsxml.util import safe_makedirs

from rscontextneo.src.utils.athena_to_gpkg import athena_to_gpkg

# Relative path (from output_folder) for the transportation GeoPackage.
TRANSPORTATION_GPKG_RELPATH = "transportation/transportation.gpkg"


def parse_s3tables_arg(arg: str) -> tuple[str, str, str]:
    """Parse a ``'catalog/namespace/table'`` argument into its three parts.

    Splits on ``'/'`` from the right so the catalog component may itself
    contain slashes (e.g. ``'s3tablescatalog/riverscapes-data'``).

    Parameters
    ----------
    arg : str
        Path of the form ``'<catalog>/<namespace>/<table>'``, e.g.
        ``'s3tablescatalog/riverscapes-data/demo/transportation_rail'``.

    Returns
    -------
    tuple of (catalog, namespace, table)
        E.g. ``('s3tablescatalog/riverscapes-data', 'demo', 'transportation_rail')``.

    Raises
    ------
    ValueError
        If *arg* does not contain at least two ``'/'`` separators (i.e. cannot
        be split into exactly three parts).
    """
    parts = arg.rsplit("/", 2)
    if len(parts) != 3:
        raise ValueError(
            f"Expected format '<catalog>/<namespace>/<table>', got: {arg!r}"
        )
    catalog, namespace, table = parts
    return catalog, namespace, table


def fetch_transportation(
    output_folder: str,
    rail_arg: Optional[str],
    roads_arg: Optional[str],
    athena_output: str,
    log: Logger,
) -> None:
    """Fetch rail and/or roads layers from Athena S3 Tables into a GeoPackage.

    Creates (or updates) ``transportation/transportation.gpkg`` inside
    *output_folder*.  Layers are named ``'rail'`` and ``'roads'``
    respectively.

    The function is a no-op when both *rail_arg* and *roads_arg* are ``None``.

    Parameters
    ----------
    output_folder : str
        Root of the RS Context Neo project output folder.
    rail_arg : str or None
        S3 Tables path for the rail layer, e.g.
        ``'s3tablescatalog/riverscapes-data/demo/transportation_rail'``.
        Pass ``None`` to skip.
    roads_arg : str or None
        S3 Tables path for the roads layer.  Pass ``None`` to skip.
    athena_output : str
        S3 URI where Athena should write query result files, e.g.
        ``'s3://riverscapes-data/athena-results/'``.
    log :
        Caller-supplied rsxml ``Logger`` (or any object with ``.info()`` /
        ``.warning()`` methods).
    """
    if rail_arg is None and roads_arg is None:
        log.info("  No transportation args provided — skipping transportation fetch")
        return

    # Import boto3 lazily so that missing boto3 doesn't break non-transportation runs.
    import boto3  # pylint: disable=import-outside-toplevel

    gpkg_path = os.path.join(output_folder, TRANSPORTATION_GPKG_RELPATH)
    safe_makedirs(os.path.dirname(gpkg_path))

    athena_client = boto3.client("athena")

    layers: list[tuple[str, str]] = []
    if roads_arg is not None:
        layers.append(("roads", roads_arg))
    if rail_arg is not None:
        layers.append(("rail", rail_arg))

    for layer_name, arg in layers:
        catalog, namespace, table_name = parse_s3tables_arg(arg)
        log.info(
            f"  Fetching transportation layer '{layer_name}' "
            f"from {catalog}/{namespace}/{table_name}"
        )
        athena_to_gpkg(
            athena_client=athena_client,
            database=namespace,
            table_name=table_name,
            gpkg_path=gpkg_path,
            s3_output_location=athena_output,
            layer_name=layer_name,
            catalog=catalog,
        )
        log.info(f"  Transportation layer '{layer_name}' written to {gpkg_path}")
