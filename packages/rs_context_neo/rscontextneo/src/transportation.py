"""
Transportation layer fetcher for RS Context Neo.

Fetches rail and/or roads data from AWS Athena S3 Tables and writes them
into a GeoPackage under the project output folder.

Author:     Matt Reimer
Date:       2026-06-02
"""

import json
import os
from typing import Optional

from rsxml import Logger
from rsxml.util import safe_makedirs

from rscontextneo.src.utils.athena_to_gpkg import athena_to_gpkg



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
    layer_name: str,
    s3tables_path: str,
    output_path: str,
    athena_output: str,
    log: Logger,
    aoi_geojson: Optional[str] = None,
) -> None:
    """Fetch a single transportation layer from Athena S3 Tables into a GeoPackage.

    Writes the layer into *output_path* (an absolute path to a GeoPackage).
    The layer is named *layer_name* inside the GeoPackage.

    Parameters
    ----------
    output_folder : str
        Root of the RS Context Neo project output folder (used only for
        creating parent directories).
    layer_name : str
        OGR layer name inside the output GeoPackage (e.g. ``'roads'`` or
        ``'rail'``).
    s3tables_path : str
        S3 Tables path in format ``'<catalog>/<namespace>/<table>'``.
    output_path : str
        Absolute path to the output GeoPackage file.
    athena_output : str
        S3 URI where Athena should write query result files.
    log :
        Caller-supplied rsxml ``Logger`` (or any object with ``.info()`` /
        ``.warning()`` methods).
    aoi_geojson : str or None
        Path to a GeoJSON file (EPSG:4326, Polygon or MultiPolygon) defining
        the area of interest.  When provided, the Athena queries are filtered
        to only return features that intersect the AOI bounding geometry.
        Pass ``None`` (default) to fetch all rows.
    """
    # Import boto3 lazily so that missing boto3 doesn't break non-transportation runs.
    import boto3  # pylint: disable=import-outside-toplevel

    safe_makedirs(os.path.dirname(output_path))

    athena_client = boto3.client("athena")

    # Build AOI WKT from the GeoJSON file when provided.
    aoi_wkt: Optional[str] = None
    if aoi_geojson is not None:
        from shapely.geometry import shape  # pylint: disable=import-outside-toplevel
        from shapely.ops import unary_union  # pylint: disable=import-outside-toplevel

        with open(aoi_geojson, "r", encoding="utf-8") as fh:
            geojson_data = json.load(fh)

        try:
            geom_type = geojson_data.get("type", "")
            if geom_type == "FeatureCollection":
                geometries = [
                    shape(feature["geometry"])
                    for feature in geojson_data.get("features", [])
                    if feature.get("geometry") is not None
                ]
            elif geom_type == "Feature":
                geometries = [shape(geojson_data["geometry"])]
            else:
                geometries = [shape(geojson_data)]

            if geometries:
                aoi_wkt = unary_union(geometries).wkt
                log.info(f"  AOI WKT derived from {aoi_geojson} — will filter Athena queries")
            else:
                log.warning("  AOI GeoJSON contains no geometries — proceeding without spatial filter")
        except Exception as exc:  # pylint: disable=broad-except
            log.warning(f"  Failed to derive AOI WKT from GeoJSON — spatial filter skipped: {exc}")
            aoi_wkt = None

    catalog, namespace, table_name = parse_s3tables_arg(s3tables_path)
    log.info(
        f"  Fetching transportation layer '{layer_name}' "
        f"from {catalog}/{namespace}/{table_name}"
    )
    athena_to_gpkg(
        athena_client=athena_client,
        database=namespace,
        table_name=table_name,
        gpkg_path=output_path,
        s3_output_location=athena_output,
        layer_name=layer_name,
        catalog=catalog,
        aoi_wkt=aoi_wkt,
    )
    log.info(f"  Transportation layer '{layer_name}' written to {output_path}")
