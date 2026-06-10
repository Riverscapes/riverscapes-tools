"""
Athena → GeoPackage export utility.

Queries an AWS Athena (S3 Tables / Iceberg OR standard Glue-backed) table,
introspects its schema, maps Athena column types to OGR field types, then
writes all rows — including WKB geometry from a designated BINARY/VARBINARY
column — into a OGR GeoPackage layer.  The output is 100% compliant with
QGIS, OGR, and ArcGIS.

Catalog-type handling
---------------------
S3 Tables / Iceberg catalogs (catalog name starts with ``s3tablescatalog``):
  * Schema is discovered via ``DESCRIBE`` (``SELECT *`` not supported).
  * The full 3-part identifier is embedded in the SQL:
    ``"catalog"."database"."table"``.
  * ``Catalog`` is **not** sent in the ``QueryExecutionContext`` for DQL
    queries; Athena rejects it for federated catalog types.

Standard Glue-backed catalog (``AwsDataCatalog`` or any non-federated):
  * Schema is discovered via ``SELECT * … LIMIT 0`` and
    ``ResultSetMetadata.ColumnInfo`` — no fragile text parsing required.
  * The 2-part identifier is used in SQL: ``"database"."table"``.
  * ``Catalog`` is passed in the ``QueryExecutionContext``.

Author:     Matt Reimer
Date:       2026-06-01
"""

import sqlite3
import time
from typing import Optional

from osgeo import ogr, osr
from rscommons import GeopackageLayer
from rsxml import Logger, ProgressBar

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
    aoi_wkt: Optional[str] = None,
    debug: bool = False,
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
        Athena database name, e.g. ``"transportation_db"``.
    table_name : str
        Athena table name, e.g. ``"roads"``.
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
        Name of the BINARY/VARBINARY column that stores WKB geometry.
        Defaults to ``"geom_wkb"``.
    field_rename : dict, optional
        Optional mapping ``{athena_column_name: gpkg_field_name}`` for
        columns that need renaming.
    workgroup : str
        Athena workgroup.  Defaults to ``"primary"``.
    catalog : str, optional
        Athena data catalog name.  For S3 Tables use the full catalog path
        (e.g. ``"s3tablescatalog/riverscapes-data"``).  For the standard
        Glue-backed catalog use ``"AwsDataCatalog"`` or ``None``.
    aoi_wkt : str, optional
        WKT string for the area of interest (EPSG:4326).  When provided a
        ``WHERE ST_Intersects(...)`` clause is appended to the SELECT query.
    debug : bool
        When ``True``, run diagnostic COUNT and sample-geometry queries
        before the main fetch and log every SQL statement in full so it can
        be copied and pasted directly into the Athena console.

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

    if not s3_output_location.startswith("s3://"):
        raise ValueError(
            f"s3_output_location must begin with 's3://' (got: {s3_output_location!r}). "
            "Example: 's3://my-bucket/athena-results/'"
        )
    if not s3_output_location.endswith("/"):
        log.warning(
            f"s3_output_location does not end with '/': {s3_output_location!r}. "
            "Appending trailing slash."
        )
        s3_output_location = s3_output_location + "/"

    log.info(
        f"Exporting Athena table '{database}.{table_name}' "
        f"→ '{gpkg_path}' / layer '{layer_name}'"
    )

    # ------------------------------------------------------------------
    # Catalog-type routing
    # ------------------------------------------------------------------
    is_s3tables = _is_s3tables_catalog(catalog)
    log.info(
        f"Catalog type: {'S3 Tables / Iceberg (federated)' if is_s3tables else 'Standard Glue-backed Athena'}"
    )

    if is_s3tables:
        # 3-part identifier in SQL; Catalog must NOT be in SELECT context.
        from_clause = f'"{catalog}"."{database}"."{table_name}"'
        select_ctx_catalog: Optional[str] = None
    else:
        # 2-part identifier in SQL; Catalog in context is fine (and recommended).
        from_clause = f'"{database}"."{table_name}"'
        select_ctx_catalog = catalog  # may be "AwsDataCatalog" or None

    log.info(f"FROM clause: {from_clause}")

    # ------------------------------------------------------------------
    # 1. Schema discovery
    # ------------------------------------------------------------------
    if is_s3tables:
        columns = _describe_table(
            athena_client, database, table_name,
            s3_output_location, workgroup, log, catalog=catalog,
        )
    else:
        columns = _get_columns_from_select_metadata(
            athena_client, from_clause, database,
            s3_output_location, workgroup, log, catalog=select_ctx_catalog,
        )

    if not columns:
        raise RuntimeError(
            f"Schema discovery returned no columns for table '{table_name}' "
            f"(database='{database}', catalog='{catalog}'). "
            "Check that the table exists and the catalog/database names are correct."
        )

    geom_col_found = any(col_name == geom_col for col_name, _ in columns)
    if not geom_col_found:
        available = [c for c, _ in columns]
        raise RuntimeError(
            f"Geometry column '{geom_col}' not found in table '{table_name}'. "
            f"Available columns: {available}"
        )

    geom_col_athena_type = next(
        (t for col, t in columns if col == geom_col), "BINARY"
    )
    log.info(
        f"Schema: {len(columns)} column(s) — "
        f"geometry column: '{geom_col}' (Athena type: {geom_col_athena_type})"
    )
    if debug:
        log.info(f"  All columns: {columns}")

    # ------------------------------------------------------------------
    # 2. Build SELECT SQL
    # ------------------------------------------------------------------
    col_list = ", ".join(f'"{c}"' for c, _ in columns)
    query_sql = f"SELECT {col_list} FROM {from_clause}"

    if aoi_wkt:
        log.info("Applying AOI spatial filter to query")
        safe_wkt = aoi_wkt.replace("'", "''")
        geom_expr = _build_geom_sql_expr(geom_col, geom_col_athena_type, log)
        query_sql += (
            f" WHERE ST_Intersects({geom_expr},"
            f" ST_GeometryFromText('{safe_wkt}'))"
        )

    # ------------------------------------------------------------------
    # 2a. Debug diagnostic probes (run before the main fetch)
    # ------------------------------------------------------------------
    if debug:
        _run_debug_probes(
            athena_client, from_clause, geom_col, geom_col_athena_type,
            aoi_wkt, database, s3_output_location, workgroup,
            log, catalog=select_ctx_catalog,
        )
        log.info(
            "\n=== Athena SELECT SQL (copy-paste ready) ===\n"
            f"{query_sql}\n"
            "=== End SQL ==="
        )
    else:
        log.info(
            f"Running: {query_sql[:300]}{'...' if len(query_sql) > 300 else ''}"
        )

    # ------------------------------------------------------------------
    # 3. Execute main SELECT
    # ------------------------------------------------------------------
    pages = _run_query_paginated(
        athena_client, database, query_sql,
        s3_output_location, workgroup, log,
        catalog=select_ctx_catalog,
    )
    all_pages = list(pages)
    log.info(f"Query complete — {len(all_pages)} page(s) retrieved")

    # Extract column order from the result header (row 0 of page 0).
    header_cols: list[str] = []
    for page in all_pages:
        rows = page.get("ResultSet", {}).get("Rows", [])
        if rows:
            header_cols = [
                d.get("VarCharValue", "") for d in rows[0].get("Data", [])
            ]
            break

    if not header_cols:
        raise RuntimeError(
            "Athena SELECT returned no header row — cannot map columns"
        )

    col_index: dict[str, int] = {name: idx for idx, name in enumerate(header_cols)}
    geom_col_idx: Optional[int] = col_index.get(geom_col)
    if geom_col_idx is None:
        raise RuntimeError(
            f"Geometry column '{geom_col}' not found in result header: {header_cols}"
        )

    # ------------------------------------------------------------------
    # 4. Sample first WKB to detect geometry type
    # ------------------------------------------------------------------
    geom_type = _detect_geom_type(all_pages, geom_col_idx, log)

    # ------------------------------------------------------------------
    # 5. Open / create GeoPackage layer
    # ------------------------------------------------------------------
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(output_epsg)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    with GeopackageLayer(gpkg_path, layer_name, write=True) as gpkg_layer:
        gpkg_layer.create(geom_type, spatial_ref=srs)

        # ------------------------------------------------------------------
        # 6. Build attribute field definitions (skip geom_col)
        # ------------------------------------------------------------------
        attr_fields: list[tuple[int, int, int]] = []
        for col_name, athena_type in columns:
            if col_name == geom_col:
                continue

            ogr_type = _map_athena_type(col_name, athena_type, log)
            gpkg_name = field_rename.get(col_name, col_name)

            if len(gpkg_name) > _GPKG_MAX_FIELD_NAME_LEN:
                truncated = gpkg_name[:_GPKG_MAX_FIELD_NAME_LEN]
                log.warning(
                    f"Field name '{gpkg_name}' exceeds {_GPKG_MAX_FIELD_NAME_LEN} chars — "
                    f"truncating to '{truncated}'"
                )
                gpkg_name = truncated

            field_idx = len(attr_fields)
            gpkg_layer.create_field(gpkg_name, ogr_type)
            src_idx = col_index.get(col_name)
            if src_idx is not None:
                attr_fields.append((src_idx, ogr_type, field_idx))

        feat_defn = gpkg_layer.ogr_layer.GetLayerDefn()
        layer = gpkg_layer.ogr_layer

        # ------------------------------------------------------------------
        # 7. Bulk insert with transaction + rtree trigger management
        # ------------------------------------------------------------------
        total_rows = sum(
            len(page.get("ResultSet", {}).get("Rows", [])) - (1 if i == 0 else 0)
            for i, page in enumerate(all_pages)
        )
        log.info(f"Writing {total_rows:,} rows to layer '{layer_name}' ...")
        progress_bar = ProgressBar(total_rows, text=f"Writing '{layer_name}'")

        conn = sqlite3.connect(gpkg_path)
        triggers = drop_table_triggers(conn, layer_name)
        layer.StartTransaction()
        try:
            rows_written = 0
            rows_skipped = 0

            for page_num, page in enumerate(all_pages):
                rows = page.get("ResultSet", {}).get("Rows", [])
                start_row = 1 if page_num == 0 else 0

                for global_row_idx, row_data in enumerate(
                    rows[start_row:], start=rows_written + rows_skipped
                ):
                    cells = row_data.get("Data", [])

                    wkb_bytes = _extract_wkb(cells, geom_col_idx, global_row_idx, log)
                    if wkb_bytes is None:
                        rows_skipped += 1
                        progress_bar.update(rows_written + rows_skipped)
                        continue

                    geom = ogr.CreateGeometryFromWkb(wkb_bytes)
                    if geom is None:
                        log.warning(
                            f"Row {global_row_idx}: ogr.CreateGeometryFromWkb() "
                            "returned None — skipping"
                        )
                        rows_skipped += 1
                        progress_bar.update(rows_written + rows_skipped)
                        continue

                    feat = ogr.Feature(feat_defn)
                    feat.SetGeometry(geom)

                    for src_idx, ogr_type, field_idx in attr_fields:
                        raw_val = (
                            cells[src_idx].get("VarCharValue", "")
                            if src_idx < len(cells)
                            else ""
                        )
                        if raw_val == "":
                            pass
                        elif ogr_type in (ogr.OFTInteger, ogr.OFTInteger64):
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
                    progress_bar.update(rows_written + rows_skipped)

            layer.CommitTransaction()
            progress_bar.finish()
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

    log.info(
        f"Layer '{layer_name}' written to '{gpkg_path}' — "
        f"{rows_written} row(s) written, {rows_skipped} skipped"
    )
    return rows_written


# ---------------------------------------------------------------------------
# Private helpers — catalog / SQL building
# ---------------------------------------------------------------------------


def _is_s3tables_catalog(catalog: Optional[str]) -> bool:
    """Return ``True`` for S3 Tables / federated catalogs.

    These catalogs require the full 3-part identifier embedded in SQL and
    must NOT have ``Catalog`` in the ``QueryExecutionContext`` for DQL
    statements.

    Parameters
    ----------
    catalog : str or None
        Catalog name from config, e.g. ``"s3tablescatalog/riverscapes-data"``
        or ``"AwsDataCatalog"``.
    """
    if not catalog:
        return False
    lower = catalog.lower()
    return lower.startswith("s3tablescatalog") or lower.startswith("s3tables/")


def _build_geom_sql_expr(geom_col: str, athena_type: str, log: Logger) -> str:
    """Return the Presto SQL expression that converts *geom_col* to a geometry.

    Selects the appropriate function call based on the column's declared
    Athena type so that ``ST_Intersects`` receives a valid geometry operand
    regardless of whether the table lives in S3 Tables/Iceberg (VARBINARY)
    or a standard Glue-backed catalog (BINARY).
    """
    safe_col = geom_col.replace('"', '""')
    base_type = athena_type.split("(")[0].strip().upper()

    if base_type == "VARBINARY":
        # S3 Tables / Iceberg — already varbinary; no cast needed.
        return f'ST_GeomFromBinary("{safe_col}")'

    if base_type == "BINARY":
        # Standard Glue-backed (Parquet/ORC) table.  Hive BINARY maps to
        # Presto varbinary but an explicit CAST avoids any type-mismatch
        # error from ST_GeomFromBinary.
        log.info(
            f"Geometry column '{geom_col}' is BINARY (Glue catalog) — "
            "using CAST(… AS VARBINARY) for ST_GeomFromBinary"
        )
        return f'ST_GeomFromBinary(CAST("{safe_col}" AS VARBINARY))'

    if base_type in ("VARCHAR", "STRING", "CHAR"):
        log.warning(
            f"Geometry column '{geom_col}' has string type '{athena_type}' — "
            "assuming hex-encoded WKB and using from_hex() for spatial filter."
        )
        return f'ST_GeomFromBinary(from_hex("{safe_col}"))'

    if base_type == "GEOMETRY":
        # Native Presto geometry type — pass directly to ST_Intersects.
        return f'"{safe_col}"'

    log.warning(
        f"Geometry column '{geom_col}' has unrecognised type '{athena_type}' — "
        "falling back to ST_GeomFromBinary without cast; query may fail"
    )
    return f'ST_GeomFromBinary("{safe_col}")'


def _run_debug_probes(
    athena_client,
    from_clause: str,
    geom_col: str,
    geom_col_athena_type: str,
    aoi_wkt: Optional[str],
    database: str,
    s3_output_location: str,
    workgroup: str,
    log: Logger,
    catalog: Optional[str] = None,
) -> None:
    """Run lightweight diagnostic queries and log the results + full SQL.

    Called only when ``debug=True``.  Runs three queries in sequence:

    1. ``COUNT(*)`` with no spatial filter — verifies the table has rows.
    2. ``COUNT(*)`` with only the geometry-non-null filter — confirms the
       WKB column is populated and ``ST_GeomFromBinary`` doesn't error out.
    3. ``COUNT(*)`` with the full spatial filter — shows whether the AOI
       intersects any features.

    All three SQL strings are logged at ``INFO`` level so they can be
    copy-pasted directly into the Athena console.
    """
    geom_expr = _build_geom_sql_expr(geom_col, geom_col_athena_type, log)

    # ── Probe 1: total row count ──────────────────────────────────────────────
    sql1 = f"SELECT COUNT(*) FROM {from_clause}"
    log.info(f"\n[DEBUG PROBE 1] Total row count:\n  {sql1}")
    try:
        pages1 = list(_run_query_paginated(
            athena_client, database, sql1,
            s3_output_location, workgroup, log, catalog=catalog,
        ))
        count1 = _first_scalar(pages1)
        log.info(f"  → {count1} total rows in table")
    except Exception as exc:  # pylint: disable=broad-except
        log.warning(f"  → Probe 1 failed: {exc}")

    # ── Probe 2: rows with non-null, readable WKB ─────────────────────────────
    sql2 = (
        f"SELECT COUNT(*) FROM {from_clause} "
        f"WHERE {geom_expr} IS NOT NULL"
    )
    log.info(f"\n[DEBUG PROBE 2] Rows with valid geometry:\n  {sql2}")
    try:
        pages2 = list(_run_query_paginated(
            athena_client, database, sql2,
            s3_output_location, workgroup, log, catalog=catalog,
        ))
        count2 = _first_scalar(pages2)
        log.info(f"  → {count2} rows with non-null geometry")
    except Exception as exc:  # pylint: disable=broad-except
        log.warning(
            f"  → Probe 2 failed — ST_GeomFromBinary may not accept the "
            f"'{geom_col_athena_type}' column as-is: {exc}"
        )

    # ── Probe 3: spatial intersection count ───────────────────────────────────
    if aoi_wkt:
        safe_wkt = aoi_wkt.replace("'", "''")
        sql3 = (
            f"SELECT COUNT(*) FROM {from_clause} "
            f"WHERE ST_Intersects({geom_expr}, ST_GeometryFromText('{safe_wkt}'))"
        )
        log.info(f"\n[DEBUG PROBE 3] Rows intersecting AOI:\n  {sql3}")
        try:
            pages3 = list(_run_query_paginated(
                athena_client, database, sql3,
                s3_output_location, workgroup, log, catalog=catalog,
            ))
            count3 = _first_scalar(pages3)
            log.info(f"  → {count3} rows intersect the AOI bounding box")
        except Exception as exc:  # pylint: disable=broad-except
            log.warning(f"  → Probe 3 failed: {exc}")


def _first_scalar(pages: list[dict]) -> str:
    """Extract the first data cell from paginated Athena results."""
    for page in pages:
        rows = page.get("ResultSet", {}).get("Rows", [])
        if len(rows) > 1:
            return rows[1].get("Data", [{}])[0].get("VarCharValue", "?")
        if len(rows) == 1:
            return rows[0].get("Data", [{}])[0].get("VarCharValue", "?")
    return "?"


# ---------------------------------------------------------------------------
# Private helpers — schema discovery
# ---------------------------------------------------------------------------


def _get_columns_from_select_metadata(
    athena_client,
    from_clause: str,
    database: str,
    s3_output_location: str,
    workgroup: str,
    log: Logger,
    catalog: Optional[str] = None,
) -> list[tuple[str, str]]:
    """Discover table schema for standard Glue-backed catalogs.

    Runs ``SELECT * FROM <table> LIMIT 0`` and reads the column names /
    types from ``ResultSet.ResultSetMetadata.ColumnInfo`` in the first
    result page.  This is much more reliable than parsing ``DESCRIBE``
    output because:

    * No text-parsing of tab-delimited rows needed.
    * ``ColumnInfo`` always reflects the exact types Athena will use in the
      subsequent SELECT — no header-row ambiguity.
    * Works for all Glue-backed table formats (Parquet, ORC, CSV).

    Parameters
    ----------
    from_clause : str
        Fully-qualified FROM clause, e.g. ``'"my_db"."roads"'``.
    database : str
        Athena database name (used in ``QueryExecutionContext``).
    catalog : str or None
        Catalog name for ``QueryExecutionContext``.

    Returns
    -------
    list of (col_name, data_type) tuples
        Column definitions in declaration order, matching exactly the
        columns returned by a subsequent ``SELECT *``.
    """
    sql = f"SELECT * FROM {from_clause} LIMIT 0"
    log.info(f"Schema probe: {sql}")
    pages = list(_run_query_paginated(
        athena_client, database, sql,
        s3_output_location, workgroup, log, catalog=catalog,
    ))
    if not pages:
        return []
    col_info = (
        pages[0]
        .get("ResultSet", {})
        .get("ResultSetMetadata", {})
        .get("ColumnInfo", [])
    )
    columns = [(c["Name"], c["Type"].upper()) for c in col_info]
    log.info(f"Schema discovery complete: {len(columns)} column(s) — {columns}")
    return columns


def _describe_table(
    athena_client,
    database: str,
    table_name: str,
    s3_output_location: str,
    workgroup: str,
    log: Logger,
    catalog: Optional[str] = None,
) -> list[tuple[str, str]]:
    """Run ``DESCRIBE <table>`` for S3 Tables / Iceberg catalogs.

    Used only when ``_is_s3tables_catalog(catalog)`` is True.  For standard
    Glue-backed tables use :func:`_get_columns_from_select_metadata` instead.

    Athena's ``get_query_results`` always returns a column-header row as
    row-0 of the first page.  For S3 Tables (Iceberg), DESCRIBE returns
    rows as single tab-delimited cells (``"col_name\\tdata_type\\tcomment"``).
    The header row is also a single cell (``"col_name\\tdata_type\\tcomment"``
    literally) which, after splitting, yields ``data_type = "DATA_TYPE"``
    — we detect and skip it.

    Parameters
    ----------
    catalog : str, optional
        Passed as ``Catalog`` in the ``QueryExecutionContext``.
    """
    sql = f"DESCRIBE `{table_name}`"
    pages = _run_query_paginated(
        athena_client, database, sql,
        s3_output_location, workgroup, log, catalog=catalog,
    )
    columns: list[tuple[str, str]] = []
    for page in pages:
        rows = page.get("ResultSet", {}).get("Rows", [])
        log.debug(f"DESCRIBE raw rows ({len(rows)}): {rows}")
        for row in rows:
            cells = row.get("Data", [])
            if not cells:
                continue

            if len(cells) == 1:
                # S3 Tables (Iceberg) format: single tab-delimited cell.
                parts = cells[0].get("VarCharValue", "").split("\t")
                col_name = parts[0].strip() if len(parts) > 0 else ""
                data_type = parts[1].strip().upper() if len(parts) > 1 else ""
            else:
                # Multi-cell format (shouldn't appear for S3 Tables but
                # handle defensively).
                col_name = cells[0].get("VarCharValue", "").strip()
                data_type = (
                    cells[1].get("VarCharValue", "").strip().upper()
                    if len(cells) > 1
                    else ""
                )

            # Skip blank / comment rows.
            if not col_name or col_name.startswith("#"):
                continue

            # Skip the standard Athena result-set header row.
            # It appears as the literal string "col_name" with data_type
            # "DATA_TYPE" (single-cell tab format) or similar.
            if col_name.lower() == "col_name" and data_type in (
                "DATA_TYPE", "DATATYPE", "TYPE",
            ):
                continue

            # A non-empty col_name with empty data_type signals the end of
            # the column section (partition / bucketing metadata follows).
            if data_type == "":
                return columns

            columns.append((col_name, data_type))
    return columns


# ---------------------------------------------------------------------------
# Private helpers — query execution
# ---------------------------------------------------------------------------


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
    if not s3_output_location.endswith("/"):
        s3_output_location = s3_output_location + "/"

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
    log.info(f"Athena query started: {execution_id}")
    log.debug(f"  Full SQL: {query_sql}")

    # Poll until terminal state.
    elapsed = 0.0
    _HEARTBEAT_INTERVAL_S = 30
    _next_heartbeat = _HEARTBEAT_INTERVAL_S
    while True:
        status_resp = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = status_resp["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            log.info(f"Athena query SUCCEEDED ({execution_id}) after {int(elapsed)}s")
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status_resp["QueryExecution"]["Status"].get(
                "StateChangeReason", "unknown"
            )
            raise RuntimeError(
                f"Athena query {execution_id} {state}: {reason}\n"
                f"  SQL: {query_sql}"
            )

        if elapsed >= _ATHENA_POLL_TIMEOUT_S:
            raise TimeoutError(
                f"Athena query {execution_id} did not finish within "
                f"{_ATHENA_POLL_TIMEOUT_S}s (current state: {state})"
            )

        if elapsed >= _next_heartbeat:
            log.info(
                f"Athena query still running ({execution_id}) — "
                f"{int(elapsed)}s elapsed, state: {state}"
            )
            _next_heartbeat += _HEARTBEAT_INTERVAL_S

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


# ---------------------------------------------------------------------------
# Private helpers — geometry / WKB
# ---------------------------------------------------------------------------


def _detect_geom_type(
    all_pages: list[dict],
    geom_col_idx: int,
    log: Logger,
) -> int:
    """Sample the first non-null WKB row to infer the OGR geometry type."""
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
                    f"{ogr.GeometryTypeToName(raw_type)} "
                    f"→ 2D: {ogr.GeometryTypeToName(flat_type)}"
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

    Athena returns BINARY/VARBINARY columns as a hex string in
    ``VarCharValue``.  Both ``0x…`` prefixed and bare hex strings are
    handled.
    """
    if geom_col_idx >= len(cells):
        if row_idx >= 0:
            log.warning(
                f"Row {row_idx}: geometry column index out of range — skipping"
            )
        return None

    cell = cells[geom_col_idx]
    raw = cell.get("VarCharValue")

    if raw is None or raw == "":
        return None

    if isinstance(raw, bytes):
        return raw

    if isinstance(raw, str):
        try:
            hex_str = raw[2:] if raw[:2].upper() == "0X" else raw
            return bytes.fromhex(hex_str)
        except ValueError:
            if row_idx >= 0:
                log.warning(
                    f"Row {row_idx}: geometry cell is not valid hex "
                    f"('{raw[:40]}...') — skipping"
                )
            return None

    if row_idx >= 0:
        log.warning(
            f"Row {row_idx}: unexpected geometry cell type {type(raw)} — skipping"
        )
    return None


# ---------------------------------------------------------------------------
# Private helpers — attribute field typing
# ---------------------------------------------------------------------------


def _map_athena_type(col_name: str, athena_type: str, log: Logger) -> int:
    """Map an Athena/Iceberg column type string to an OGR field type constant."""
    base_type = athena_type.split("(")[0].strip()

    _TYPE_MAP: dict[str, int] = {
        "INT": ogr.OFTInteger,
        "INTEGER": ogr.OFTInteger,
        "TINYINT": ogr.OFTInteger,
        "SMALLINT": ogr.OFTInteger,
        "BIGINT": ogr.OFTInteger64,
        "DOUBLE": ogr.OFTReal,
        "FLOAT": ogr.OFTReal,
        "REAL": ogr.OFTReal,
        "DECIMAL": ogr.OFTReal,
        "STRING": ogr.OFTString,
        "VARCHAR": ogr.OFTString,
        "CHAR": ogr.OFTString,
    }

    if base_type == "BIGINT":
        log.warning(
            f"Column '{col_name}': OFTInteger64 is not supported by ESRI "
            "(Athena type: BIGINT)"
        )

    if base_type in ("VARBINARY", "BINARY"):
        log.warning(
            f"Column '{col_name}': {base_type} passed to _map_athena_type — "
            "this should be the geometry column and not mapped to a field"
        )
        return ogr.OFTString

    if base_type in _TYPE_MAP:
        return _TYPE_MAP[base_type]

    log.warning(
        f"Column '{col_name}': unmapped Athena type '{athena_type}' — "
        "falling back to OFTString"
    )
    return ogr.OFTString
