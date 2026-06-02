"""
Athena → GeoPackage export utility.

Queries an AWS Athena (S3 Tables / Iceberg) table, introspects its schema
via DESCRIBE, maps Athena column types to OGR field types, then writes all
rows — including WKB geometry from a designated VARBINARY column — into a
OGR GeoPackage layer.  The output is 100% compliant with QGIS, OGR, and
ArcGIS.

Author:     Matt Reimer
Date:       2026-06-01
"""

import sqlite3
import time
from typing import Optional

from osgeo import ogr, osr
from rscommons import GeopackageLayer
from rsxml import Logger

from rscontextneo.src.utils.gpkg import drop_table_triggers, restore_table_triggers

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maximum seconds to wait for an Athena query to finish before raising.
_ATHENA_POLL_TIMEOUT_S = 600

# Seconds between Athena status-poll requests.
_ATHENA_POLL_INTERVAL_S = 1

# GeoPackage field names must not exceed 31 characters.
_GPKG_MAX_FIELD_NAME_LEN = 31


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def athena_to_gpkg(
    athena_client,
    database: str,
    table_name: str,
    gpkg_path: str,
    s3_output_location: str,
    layer_name: Optional[str] = None,
    output_epsg: int = 4326,
    geom_col: str = "geom_wkb",
    field_rename: Optional[dict] = None,
    workgroup: str = "primary",
    catalog: Optional[str] = None,
) -> int:
    """Export an Athena table to a GeoPackage layer.

    Queries the named Athena table, maps its column types to OGR field types,
    samples the first non-null WKB geometry to determine the layer geometry
    type, then bulk-inserts every row into a GeoPackage layer.

    The GeoPackage is created if it does not exist.  If it already exists the
    target layer is dropped and recreated so the export is idempotent.

    Parameters
    ----------
    athena_client :
        A boto3 Athena client (``boto3.client("athena", ...)``) already
        configured with the correct region and credentials.
    database : str
        Athena database name, e.g. ``"my_catalog.my_db"`` or just
        ``"my_db"`` depending on how your Athena workgroup is configured.
    table_name : str
        Athena table name, e.g. ``"transportation_rail"``.
    gpkg_path : str
        Absolute or relative path to the output GeoPackage.  The file is
        created (including parent directories) if it does not yet exist.
    s3_output_location : str
        S3 URI where Athena should write query result files, e.g.
        ``"s3://my-bucket/athena-results/"``.
    layer_name : str, optional
        GeoPackage layer name.  Defaults to *table_name*.
    output_epsg : int
        EPSG code for the layer's spatial reference.  Defaults to 4326.
    geom_col : str
        Name of the ``VARBINARY`` column that stores WKB geometry.
        Defaults to ``"geom_wkb"``.
    field_rename : dict, optional
        Optional mapping ``{athena_column_name: gpkg_field_name}`` for
        columns that need renaming.  Only columns with entries in this dict
        are renamed; all others keep their original Athena names.
    workgroup : str
        Athena workgroup.  Defaults to ``"primary"``.
    catalog : str, optional
        Athena data catalog name (e.g. ``"s3tablescatalog/riverscapes-data"``).
        When provided it is passed as ``Catalog`` in the
        ``QueryExecutionContext``.  Pass ``None`` (default) to use the
        workgroup's default catalog.

    Returns
    -------
    int
        Number of rows successfully written to the GeoPackage layer.

    Raises
    ------
    RuntimeError
        If the Athena query fails, the GeoPackage cannot be opened/created,
        or no geometry column is found in the table schema.
    TimeoutError
        If an Athena query does not finish within ``_ATHENA_POLL_TIMEOUT_S``
        seconds.
    """
    log = Logger("AthenaToGpkg")

    if layer_name is None:
        layer_name = table_name

    if field_rename is None:
        field_rename = {}

    log.info(f"Exporting Athena table '{database}.{table_name}' → '{gpkg_path}' / layer '{layer_name}'")

    # ------------------------------------------------------------------
    # 1. Introspect schema via DESCRIBE
    # ------------------------------------------------------------------
    columns = _describe_table(athena_client, database, table_name, s3_output_location, workgroup, log, catalog=catalog)
    if not columns:
        raise RuntimeError(f"DESCRIBE {table_name} returned no columns")

    # Verify a geometry column exists.
    geom_col_found = any(col_name == geom_col for col_name, _ in columns)
    if not geom_col_found:
        raise RuntimeError(
            f"Geometry column '{geom_col}' not found in table '{table_name}'. "
            f"Available columns: {[c for c, _ in columns]}"
        )

    log.info(f"Schema: {len(columns)} column(s) — geometry column: '{geom_col}'")

    # ------------------------------------------------------------------
    # 2. Fetch all rows from Athena (paginated)
    # ------------------------------------------------------------------
    log.info(f"Running SELECT * FROM \"{table_name}\" ...")
    query_sql = f'SELECT * FROM "{table_name}"'
    pages = _run_query_paginated(
        athena_client, database, query_sql, s3_output_location, workgroup, log, catalog=catalog
    )

    # Flatten pages into a row iterator so we can sample the first WKB
    # *before* creating the layer without consuming the entire result set.
    # We buffer all pages in memory (list of page dicts) since Athena
    # pagination tokens expire and cannot be replayed.
    all_pages = list(pages)
    log.info(f"Query complete — {len(all_pages)} page(s) retrieved")

    # Extract column order from the first page header (row index 0).
    header_cols: list[str] = []
    for page in all_pages:
        rows = page.get("ResultSet", {}).get("Rows", [])
        if rows:
            header_cols = [d.get("VarCharValue", "") for d in rows[0].get("Data", [])]
            break

    if not header_cols:
        raise RuntimeError("Athena query returned no header row — cannot map columns")

    # Build column-index lookup.
    col_index: dict[str, int] = {name: idx for idx, name in enumerate(header_cols)}
    geom_col_idx: Optional[int] = col_index.get(geom_col)
    if geom_col_idx is None:
        raise RuntimeError(
            f"Geometry column '{geom_col}' not found in Athena result header: {header_cols}"
        )

    # ------------------------------------------------------------------
    # 3. Sample first non-null WKB to detect geometry type
    # ------------------------------------------------------------------
    geom_type = _detect_geom_type(all_pages, geom_col_idx, log)

    # ------------------------------------------------------------------
    # 4. Open or create GeoPackage layer via GeopackageLayer
    # ------------------------------------------------------------------
    # GeopackageLayer.create() handles both the "create new GPKG" and
    # "open existing GPKG and replace layer" cases, so there is no need
    # for a separate _open_or_create_gpkg() helper.
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(output_epsg)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    with GeopackageLayer(gpkg_path, layer_name, write=True) as gpkg_layer:
        gpkg_layer.create(geom_type, spatial_ref=srs)

        # ------------------------------------------------------------------
        # 5. Build attribute field definitions (skip geom_col)
        # ------------------------------------------------------------------
        attr_fields: list[tuple[str, int]] = []  # (gpkg_field_name, ogr_type)
        for col_name, athena_type in columns:
            if col_name == geom_col:
                continue  # geometry handled separately

            ogr_type = _map_athena_type(col_name, athena_type, log)

            # Apply optional rename.
            gpkg_name = field_rename.get(col_name, col_name)

            # Enforce GeoPackage 31-char field name limit.
            if len(gpkg_name) > _GPKG_MAX_FIELD_NAME_LEN:
                truncated = gpkg_name[:_GPKG_MAX_FIELD_NAME_LEN]
                log.warning(
                    f"Field name '{gpkg_name}' exceeds {_GPKG_MAX_FIELD_NAME_LEN} chars — "
                    f"truncating to '{truncated}'"
                )
                gpkg_name = truncated

            # create_field() is idempotent (returns existing def if name+type match)
            # and automatically sets width=18 / precision=10 for OFTReal fields.
            gpkg_layer.create_field(gpkg_name, ogr_type)
            attr_fields.append((col_name, ogr_type))

        feat_defn = gpkg_layer.ogr_layer.GetLayerDefn()
        layer = gpkg_layer.ogr_layer  # keep a short alias for the feature loop below

        # ------------------------------------------------------------------
        # 6. Bulk insert with transaction + rtree trigger management
        # ------------------------------------------------------------------
        conn = sqlite3.connect(gpkg_path)
        triggers = drop_table_triggers(conn, layer_name)
        layer.StartTransaction()
        try:
            try:
                rows_written = 0
                rows_skipped = 0
                # Absolute row index across all pages (used for skip warnings).
                global_row_idx = 0

                for page_num, page in enumerate(all_pages):
                    rows = page.get("ResultSet", {}).get("Rows", [])

                    # Skip header row on the first page only.
                    start_row = 1 if page_num == 0 else 0

                    for row_data in rows[start_row:]:
                        cells = row_data.get("Data", [])

                        # Extract geometry
                        wkb_bytes = _extract_wkb(cells, geom_col_idx, global_row_idx, log)
                        if wkb_bytes is None:
                            rows_skipped += 1
                            global_row_idx += 1
                            continue

                        geom = ogr.CreateGeometryFromWkb(wkb_bytes)
                        if geom is None:
                            log.warning(f"Row {global_row_idx}: ogr.CreateGeometryFromWkb() returned None — skipping")
                            rows_skipped += 1
                            global_row_idx += 1
                            continue

                        feat = ogr.Feature(feat_defn)
                        feat.SetGeometry(geom)

                        # Set attribute fields
                        for field_idx, (col_name, ogr_type) in enumerate(attr_fields):
                            src_idx = col_index.get(col_name)
                            if src_idx is None:
                                continue
                            raw_val = cells[src_idx].get("VarCharValue", "") if src_idx < len(cells) else ""

                            # NULL sentinel: Athena represents NULLs as empty VarCharValue
                            if raw_val == "":
                                # Leave field unset (OGR will write NULL)
                                pass
                            elif ogr_type == ogr.OFTInteger:
                                try:
                                    feat.SetField(field_idx, int(raw_val))
                                except (ValueError, TypeError):
                                    pass
                            elif ogr_type == ogr.OFTInteger64:
                                try:
                                    feat.SetField(field_idx, int(raw_val))
                                except (ValueError, TypeError):
                                    pass
                            elif ogr_type == ogr.OFTReal:
                                try:
                                    feat.SetField(field_idx, float(raw_val))
                                except (ValueError, TypeError):
                                    pass
                            else:
                                feat.SetField(field_idx, raw_val)

                        layer.CreateFeature(feat)
                        feat = None
                        rows_written += 1
                        global_row_idx += 1

                layer.CommitTransaction()
            except Exception:
                try:
                    layer.RollbackTransaction()
                except Exception as rb_err:
                    log.warning(f"RollbackTransaction failed: {rb_err}")
                raise
        finally:
            restore_table_triggers(conn, triggers)
            conn.commit()
            conn.close()

    # GeopackageLayer context manager flushes and releases the datasource on exit.

    log.info(
        f"Layer '{layer_name}' written to '{gpkg_path}' — "
        f"{rows_written} row(s) written, {rows_skipped} skipped"
    )
    return rows_written


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _describe_table(
    athena_client,
    database: str,
    table_name: str,
    s3_output_location: str,
    workgroup: str,
    log: Logger,
    catalog: Optional[str] = None,
) -> list[tuple[str, str]]:
    """Run ``DESCRIBE <table>`` and return a list of ``(col_name, data_type)`` tuples.

    Parameters
    ----------
    athena_client :
        boto3 Athena client.
    database : str
        Athena database name.
    table_name : str
        Table to describe.
    s3_output_location : str
        S3 URI for Athena result output.
    workgroup : str
        Athena workgroup name.
    log : Logger
        Caller-supplied logger.
    catalog : str, optional
        Athena data catalog name.  Passed as ``Catalog`` in the
        ``QueryExecutionContext`` when provided.

    Returns
    -------
    list of (col_name, data_type) tuples
        Column definitions in declaration order.
    """
    sql = f'DESCRIBE "{table_name}"'
    pages = _run_query_paginated(
        athena_client, database, sql, s3_output_location, workgroup, log, catalog=catalog
    )
    columns: list[tuple[str, str]] = []
    first_page = True
    for page in pages:
        rows = page.get("ResultSet", {}).get("Rows", [])
        start = 1 if first_page else 0
        first_page = False
        for row in rows[start:]:
            cells = row.get("Data", [])
            if len(cells) < 2:
                continue
            col_name = cells[0].get("VarCharValue", "").strip()
            data_type = cells[1].get("VarCharValue", "").strip().upper()
            if col_name and not col_name.startswith("#"):
                columns.append((col_name, data_type))
    return columns


def _run_query_paginated(
    athena_client,
    database: str,
    query_sql: str,
    s3_output_location: str,
    workgroup: str,
    log: Logger,
    catalog: Optional[str] = None,
):
    """Execute a query and yield result pages from ``get_query_results``.

    Starts the query, polls until it reaches a terminal state, then paginates
    through all result pages using ``NextToken``.

    Parameters
    ----------
    athena_client :
        boto3 Athena client.
    database : str
        Athena database name.
    query_sql : str
        SQL statement to execute.
    s3_output_location : str
        S3 URI for Athena result output.
    workgroup : str
        Athena workgroup name.
    log : Logger
        Caller-supplied logger.
    catalog : str, optional
        Athena data catalog name.  Passed as ``Catalog`` in the
        ``QueryExecutionContext`` when provided.

    Yields
    ------
    dict
        Raw boto3 ``get_query_results`` response page dictionaries.

    Raises
    ------
    RuntimeError
        If the query fails or is cancelled by Athena.
    TimeoutError
        If the query does not finish within ``_ATHENA_POLL_TIMEOUT_S`` seconds.
    """
    # Start execution.
    ctx: dict = {"Database": database}
    if catalog:
        ctx["Catalog"] = catalog
    response = athena_client.start_query_execution(
        QueryString=query_sql,
        QueryExecutionContext=ctx,
        ResultConfiguration={"OutputLocation": s3_output_location},
        WorkGroup=workgroup,
    )
    execution_id = response["QueryExecutionId"]
    log.info(f"Athena query started: {execution_id}  SQL: {query_sql[:120]}")

    # Poll until terminal state.
    elapsed = 0.0
    while True:
        status_resp = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = status_resp["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            log.info(f"Athena query SUCCEEDED ({execution_id})")
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status_resp["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise RuntimeError(
                f"Athena query {execution_id} {state}: {reason}  SQL: {query_sql[:200]}"
            )

        if elapsed >= _ATHENA_POLL_TIMEOUT_S:
            raise TimeoutError(
                f"Athena query {execution_id} did not finish within "
                f"{_ATHENA_POLL_TIMEOUT_S}s (current state: {state})"
            )

        time.sleep(_ATHENA_POLL_INTERVAL_S)
        elapsed += _ATHENA_POLL_INTERVAL_S

    # Paginate results.
    kwargs: dict = {"QueryExecutionId": execution_id}
    while True:
        page = athena_client.get_query_results(**kwargs)
        yield page
        next_token = page.get("NextToken")
        if not next_token:
            break
        kwargs["NextToken"] = next_token


def _detect_geom_type(
    all_pages: list[dict],
    geom_col_idx: int,
    log: Logger,
) -> int:
    """Sample the first non-null WKB row to infer the OGR geometry type.

    Parameters
    ----------
    all_pages : list of dict
        All paginated Athena result pages (page 0 includes the header row).
    geom_col_idx : int
        Column index (0-based) of the WKB geometry column.
    log : Logger
        Caller-supplied logger.

    Returns
    -------
    int
        OGR 2D geometry type constant (e.g. ``ogr.wkbLineString``).
        Returns ``ogr.wkbUnknown`` if no valid WKB is found.
    """
    for page_num, page in enumerate(all_pages):
        rows = page.get("ResultSet", {}).get("Rows", [])
        start = 1 if page_num == 0 else 0
        for row_data in rows[start:]:
            cells = row_data.get("Data", [])
            wkb_bytes = _extract_wkb(cells, geom_col_idx, -1, log)
            if wkb_bytes is None:
                continue
            geom = ogr.CreateGeometryFromWkb(wkb_bytes)
            if geom is not None:
                raw_type = geom.GetGeometryType()
                flat_type = ogr.GT_Flatten(raw_type)
                log.info(
                    f"Detected geometry type from first non-null WKB: "
                    f"{ogr.GeometryTypeToName(raw_type)} → 2D: {ogr.GeometryTypeToName(flat_type)}"
                )
                return flat_type

    log.warning("No non-null WKB found for geometry type detection — using wkbUnknown")
    return ogr.wkbUnknown


def _extract_wkb(
    cells: list[dict],
    geom_col_idx: int,
    row_idx: int,
    log: Logger,
) -> Optional[bytes]:
    """Extract WKB bytes from an Athena result row cell.

    Athena returns binary columns either as raw ``bytes`` or as a hex string
    in ``VarCharValue`` (depending on the driver version and column type).
    Both cases are handled.

    Parameters
    ----------
    cells : list of dict
        The ``Data`` list from one Athena result row.
    geom_col_idx : int
        Index of the geometry cell in *cells*.
    row_idx : int
        Absolute row index (used only for warning messages; pass -1 to suppress).
    log : Logger
        Caller-supplied logger.

    Returns
    -------
    bytes or None
        WKB bytes, or ``None`` if the cell is null or cannot be decoded.
    """
    if geom_col_idx >= len(cells):
        if row_idx >= 0:
            log.warning(f"Row {row_idx}: geometry column index out of range — skipping")
        return None

    cell = cells[geom_col_idx]

    # boto3 may deliver binary columns as raw bytes under the key ``VarCharValue``
    # (in practice they always come back as bytes for VARBINARY columns when
    # the result is fetched via get_query_results).
    raw = cell.get("VarCharValue")

    if raw is None or raw == "":
        return None

    if isinstance(raw, bytes):
        return raw

    # Try hex-decode (Athena sometimes returns VARBINARY as a hex string).
    if isinstance(raw, str):
        try:
            hex_str = raw[2:] if raw[:2].upper() == "0X" else raw
            return bytes.fromhex(hex_str)
        except ValueError:
            if row_idx >= 0:
                log.warning(
                    f"Row {row_idx}: geometry cell is not valid hex ('{raw[:40]}...') — skipping"
                )
            return None

    if row_idx >= 0:
        log.warning(f"Row {row_idx}: unexpected geometry cell type {type(raw)} — skipping")
    return None


def _map_athena_type(col_name: str, athena_type: str, log: Logger) -> int:
    """Map an Athena/Iceberg column type string to an OGR field type constant.

    Parameters
    ----------
    col_name : str
        Column name (used only for warning messages).
    athena_type : str
        Upper-cased Athena type string, e.g. ``"BIGINT"``, ``"DOUBLE"``,
        ``"STRING"``.
    log : Logger
        Caller-supplied logger.

    Returns
    -------
    int
        OGR field type constant (e.g. ``ogr.OFTString``).
    """
    # Normalise: strip any length/precision qualifiers, e.g. "VARCHAR(255)" → "VARCHAR"
    base_type = athena_type.split("(")[0].strip()

    if base_type == "BIGINT":
        log.warning(
            f"Column '{col_name}': OFTInteger64 is not supported by ESRI (Athena type: BIGINT)"
        )
        return ogr.OFTInteger64

    if base_type in ("INT", "INTEGER", "TINYINT", "SMALLINT"):
        return ogr.OFTInteger

    if base_type in ("DOUBLE", "FLOAT", "REAL", "DECIMAL"):
        return ogr.OFTReal

    if base_type in ("STRING", "VARCHAR", "CHAR"):
        return ogr.OFTString

    # VARBINARY is the geometry column — callers should never pass it here,
    # but guard defensively.
    if base_type == "VARBINARY":
        log.warning(
            f"Column '{col_name}': VARBINARY passed to _map_athena_type — "
            "this column should be the geometry column and not mapped to a field"
        )
        return ogr.OFTString

    log.warning(
        f"Column '{col_name}': unmapped Athena type '{athena_type}' — falling back to OFTString"
    )
    return ogr.OFTString
