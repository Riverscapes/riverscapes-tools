"""
GeoPackage and SQLite utilities for RS Context Neo.

Provides helpers for creating indexes, managing GeoPackage rtree triggers
around bulk UPDATE operations, and converting GeoJSON files to GeoPackage
format.

Author:     Matt Reimer
Date:       2026-05-25
"""

import os
import sqlite3
from typing import List, Union

from osgeo import gdal


def create_index(
    curs: sqlite3.Cursor,
    table: str,
    columns: Union[str, List[str]],
) -> None:
    """Create a covering index on *columns* in *table* if it does not exist.

    The index is named ``ix_<table>_<col1>_<col2>_…``.

    Parameters
    ----------
    curs : sqlite3.Cursor
        Active cursor.
    table : str
        Table name.
    columns : str or list[str]
        Column name(s) to index.
    """
    col_list: List[str] = [columns] if isinstance(columns, str) else list(columns)
    index_name = f"ix_{table}_{'_'.join(col_list)}"
    curs.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name = ?",
        (index_name,),
    )
    if curs.fetchone() is None:
        curs.execute(f"CREATE INDEX {index_name} ON {table}({', '.join(col_list)})")


def drop_table_triggers(
    conn: sqlite3.Connection,
    table: str,
) -> List[tuple]:
    """Drop all triggers on *table* and return their DDL for later restoration.

    GeoPackage rtree triggers reference spatial functions (``ST_IsEmpty``,
    ``ST_MinX``, etc.) that are only available when SpatiaLite is loaded.
    SQLite compiles *all* trigger SQL the first time a statement touches the
    table, so those calls fail even when the trigger's WHEN clause would never
    fire.  Dropping triggers before a bulk non-geometry UPDATE avoids the
    compile-time failure entirely.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open connection (not inside an active transaction).
    table : str
        Table whose triggers should be dropped.

    Returns
    -------
    list of (name, sql) tuples
        The original trigger definitions so they can be recreated by
        :func:`_restore_table_triggers`.
    """
    curs = conn.cursor()
    curs.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
        (table,),
    )
    triggers = curs.fetchall()  # [(name, sql), ...]
    for name, _sql in triggers:
        curs.execute(f'DROP TRIGGER IF EXISTS "{name}"')
    return triggers


def restore_table_triggers(
    conn: sqlite3.Connection,
    triggers: List[tuple],
) -> None:
    """Recreate triggers that were previously removed by :func:`_drop_table_triggers`.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open connection (not inside an active transaction).
    triggers : list of (name, sql) tuples
        As returned by :func:`_drop_table_triggers`.
    """
    curs = conn.cursor()
    for _name, sql in triggers:
        if sql:
            curs.execute(sql)


def geojson_to_gpkg(geojson_path: str, gpkg_path: str) -> None:
    """
    Convert a GeoJSON file to a single-layer GeoPackage.

    rscommons functions that accept vector bounds (download_dem, verify_areas,
    etc.) route through get_shp_or_gpkg which forces the GPKG OGR driver.
    This helper produces a compatible file from the GeoJSON bounds that are
    the native format for rs_context_neo project bounds.

    Raises:
        RuntimeError: If GDAL VectorTranslate fails to produce an output file.
    """
    # Always regenerate: this is a small scratch file derived from the GeoJSON.
    # A mtime-based cache is unsafe because the scratch folder may be shared
    # across different projects, causing a stale bounds.gpkg from a previous
    # run (with different AOI coordinates) to be silently reused.
    if os.path.exists(gpkg_path):
        os.remove(gpkg_path)

    result = gdal.VectorTranslate(
        gpkg_path,
        geojson_path,
        format="GPKG",
        layerName="bounds",
    )

    # VectorTranslate returns a DataSource on success, None on failure
    if result is None:
        raise RuntimeError(
            f"GDAL VectorTranslate failed to convert {geojson_path} → {gpkg_path}. "
            f"GDAL error: {gdal.GetLastErrorMsg()}"
        )
    result = None  # dereference / flush
