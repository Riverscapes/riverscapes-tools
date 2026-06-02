"""
Proposed additions: new module rscommons/gpkg_utils.py
======================================================

Create ``lib/commons/rscommons/gpkg_utils.py`` with the content below,
then expose the public functions in ``rscommons/__init__.py``:

    from rscommons.gpkg_utils import drop_gpkg_triggers, restore_gpkg_triggers

Background
----------
GeoPackage stores rtree spatial index triggers that reference SpatiaLite
functions (``ST_IsEmpty``, ``ST_MinX``, etc.).  SQLite compiles *all*
trigger SQL the moment any statement first touches the table, so those
function calls fail at compile time even when the trigger's WHEN clause
would never fire.  This causes bulk non-geometry UPDATE statements (like
writing ``level_path`` values or Athena attribute data) to fail with::

    OperationalError: no such function: ST_IsEmpty

The fix is to drop the triggers before the bulk operation and restore them
afterwards.  This pattern appears in three places in rs_context_neo today
(level_path.py, athena_to_gpkg.py, gpkg.py).  Centralising it in rscommons
makes it available to every tool that does bulk GeoPackage updates.

Once merged, rs_context_neo can replace:
    from rscontextneo.src.utils.gpkg import drop_table_triggers, restore_table_triggers
with:
    from rscommons.gpkg_utils import drop_gpkg_triggers, restore_gpkg_triggers
"""

import sqlite3
from typing import List, Tuple


def drop_gpkg_triggers(
    conn: sqlite3.Connection,
    table: str,
) -> List[Tuple[str, str]]:
    """Drop all triggers on *table* and return their DDL for later restoration.

    GeoPackage rtree triggers reference SpatiaLite functions
    (``ST_IsEmpty``, ``ST_MinX``, etc.) that are only available when
    SpatiaLite is loaded.  SQLite compiles *all* trigger SQL the first time
    a statement touches the table, so those calls fail even when the
    trigger's WHEN clause would never fire.  Dropping triggers before a bulk
    non-geometry UPDATE avoids the compile-time failure entirely.

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
        :func:`restore_gpkg_triggers`.
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


def restore_gpkg_triggers(
    conn: sqlite3.Connection,
    triggers: List[Tuple[str, str]],
) -> None:
    """Recreate triggers that were previously removed by :func:`drop_gpkg_triggers`.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open connection (not inside an active transaction).
    triggers : list of (name, sql) tuples
        As returned by :func:`drop_gpkg_triggers`.
    """
    curs = conn.cursor()
    for _name, sql in triggers:
        if sql:
            curs.execute(sql)
