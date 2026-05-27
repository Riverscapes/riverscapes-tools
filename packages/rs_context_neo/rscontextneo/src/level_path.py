"""
Level path calculation for a river network stored in a GeoPackage.

Traverses the network from headwaters downstream, assigning a unique integer
level-path identifier to each reach.  Reaches that share a level path form a
single continuous flow path from a headwater to an outlet (or to the point
where a longer tributary takes over).

Algorithm (adapted from Philip Bailey, 12 Jun 2025):
Source: https://github.com/Riverscapes/riverscapes-tools/blob/master/packages/taudem/taudem/scripts/calc_level_path_taudem.py
    1. Find all headwater reaches (USLINKNO1 = -1) that lack a level_path.
    2. For each headwater, walk downstream summing reach lengths to obtain the
       total flow-path length from that headwater to the network outlet.
    3. Sort headwaters by descending flow-path length so the longest path gets
       the lowest level-path number (highest priority).
    4. Walk downstream from each headwater, stamping the level-path value on
       every reach whose level_path is still NULL.  Because the longest path is
       processed first, tributary junctions will already carry the main-stem
       value when shorter tributaries are processed — they stop stamping as soon
       as they merge into an already-labelled reach.

The result is consistent with NHD-style level-path convention: the main stem
of each watershed carries level_path = 1 000 000 001, the longest unassigned
tributary gets 1 000 000 002, and so on.

Public API
----------
calc_level_paths(gpkg_path, layer_name, force, log) -> None

Author:     Matt Reimer (adapted from Philip Bailey)
Date:       2026-05-27
"""
import sqlite3
from typing import List, Union

from rsxml import Logger

# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

# Given an upstream reach (us), look up its length and the LINKNO of the
# single reach immediately downstream (ds).  Returns zero or one row.
NEXT_REACH_QUERY = (
    "SELECT us.Length, ds.LINKNO "
    "FROM {0} us "
    "LEFT JOIN {0} ds ON us.DSLINKNO = ds.LINKNO "
    "WHERE us.LINKNO = ?"
)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def calc_level_paths(gpkg_path: str, layer_name: str, force: bool, log: Logger) -> None:
    """Calculate and persist level-path values for every reach in *layer_name*.

    Opens *gpkg_path* as a SQLite database, ensures the ``level_path`` column
    and supporting indexes exist, then runs the traversal algorithm.  The
    entire operation is wrapped in a single transaction; any failure triggers
    a rollback so the table is never left in a partially-updated state.

    Parameters
    ----------
    gpkg_path : str
        Absolute path to the hydrology GeoPackage (SQLite file).
    layer_name : str
        Name of the vector layer / table that holds the stream-reach features.
        Expected columns: ``LINKNO``, ``DSLINKNO``, ``USLINKNO1``, ``Length``.
    force : bool
        If ``True``, wipe any existing ``level_path`` values and recalculate
        from scratch.  If ``False`` and all rows already have a non-NULL
        ``level_path``, the step is skipped entirely.
    log : Logger
        rsxml Logger instance used for progress messages.

    Raises
    ------
    sqlite3.DatabaseError
        If the GeoPackage cannot be opened or an SQL statement fails.
    RuntimeError
        If the network topology is ambiguous (more than one downstream reach
        found for a given LINKNO).
    """
    log.info(f"Opening GeoPackage for level-path calculation: {gpkg_path}")

    conn = sqlite3.connect(gpkg_path)
    try:
        curs = conn.cursor()

        # Ensure the destination column and lookup indexes exist before the
        # transaction so that DDL changes are durable even on error.
        _add_column(curs, layer_name, 'level_path', 'REAL')
        _create_index(curs, layer_name, ['LINKNO'])
        _create_index(curs, layer_name, ['DSLINKNO'])
        _create_index(curs, layer_name, ['USLINKNO1'])
        conn.commit()

        # Check whether all rows are already labelled.
        if not force:
            curs.execute(f"SELECT COUNT(*) FROM {layer_name} WHERE level_path IS NULL")
            null_count = curs.fetchone()[0]
            if null_count == 0:
                curs.execute(f"SELECT COUNT(*) FROM {layer_name}")
                total = curs.fetchone()[0]
                log.info(
                    f"All {total} reaches already have a level_path value — skipping."
                )
                return

        # GeoPackage rtree triggers reference ST_IsEmpty / ST_MinX etc.
        # SQLite compiles all trigger SQL the moment the table is first touched,
        # so those calls fail immediately even when the WHEN clause would never
        # have fired.  Drop them before the bulk UPDATE and restore afterwards.
        triggers = _drop_table_triggers(conn, layer_name)
        if triggers:
            log.debug(f"Dropped {len(triggers)} GeoPackage trigger(s) on '{layer_name}' for bulk UPDATE.")

        conn.execute("BEGIN")
        try:
            _calc_level_path_core(curs, layer_name, force, log)
            conn.commit()
            log.info("Level-path transaction committed successfully.")
        except Exception:
            conn.rollback()
            log.error("Level-path calculation failed — rolled back all changes.")
            raise
        finally:
            # Always restore triggers — even on error — so the GeoPackage
            # remains spatially consistent for subsequent operations.
            if triggers:
                _restore_table_triggers(conn, triggers)
                conn.commit()
                log.debug(f"Restored {len(triggers)} GeoPackage trigger(s) on '{layer_name}'.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Core traversal logic
# ---------------------------------------------------------------------------

def _calc_level_path_core(
    curs: sqlite3.Cursor,
    feature_class: str,
    reset_first: bool,
    log: Logger,
) -> None:
    """Perform the level-path traversal and UPDATE statements.

    Parameters
    ----------
    curs : sqlite3.Cursor
        Active cursor on an open, writable connection.
    feature_class : str
        Table / layer name.
    reset_first : bool
        If ``True``, NULL-out all existing ``level_path`` values before
        traversal so that everything is recalculated.
    log : Logger
        rsxml Logger for progress messages.
    """
    curs.execute(f"SELECT COUNT(*) FROM {feature_class}")
    total_reaches = curs.fetchone()[0]
    log.info(f"Total reaches in '{feature_class}': {total_reaches:,}")

    if reset_first:
        log.info("force=True — resetting all level_path values to NULL.")
        curs.execute(f"UPDATE {feature_class} SET level_path = NULL")

    curs.execute(
        f"SELECT COUNT(*) FROM {feature_class} WHERE level_path IS NULL"
    )
    unlabelled = curs.fetchone()[0]
    log.info(f"Reaches without a level_path: {unlabelled:,}")

    # Collect all headwater LINKNOs (no upstream contributor).
    curs.execute(
        f"SELECT LINKNO FROM {feature_class} "
        f"WHERE USLINKNO1 = -1 AND level_path IS NULL"
    )
    headwaters: List[int] = [row[0] for row in curs.fetchall()]
    log.info(f"Headwater reaches found: {len(headwaters):,}")

    # Compute cumulative flow-path length from each headwater to the outlet.
    log.info("Computing flow-path lengths from each headwater to the outlet …")
    level_path_lengths = {
        hw: _calculate_length(curs, feature_class, hw)
        for hw in headwaters
    }

    # Process longest paths first so the main stem is labelled before
    # shorter tributaries try to merge into it.
    log.info("Assigning level-path values (longest path first) …")
    num_processed = 0
    for hydro_id, _length in sorted(
        level_path_lengths.items(), key=lambda item: item[1], reverse=True
    ):
        num_processed += 1
        new_level_path = float(10**9 + num_processed)
        num_reaches = _assign_level_path(curs, feature_class, hydro_id, new_level_path)
        log.debug(
            f"  level_path {new_level_path:.0f}: {num_reaches} reaches "
            f"(headwater LINKNO={hydro_id}, path_length={_length:.1f})"
        )

    log.info(f"Level-path assignment complete: {num_processed:,} paths created.")


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _calculate_length(
    curs: sqlite3.Cursor, feature_class: str, hydro_id: int
) -> float:
    """Walk downstream from *hydro_id*, accumulating reach lengths.

    Traversal stops when a reach has no downstream neighbour (outlet) or when
    the downstream LINKNO is NULL (edge of the network).

    Parameters
    ----------
    curs : sqlite3.Cursor
        Active cursor on an open connection.
    feature_class : str
        Table name.
    hydro_id : int
        Starting LINKNO (typically a headwater).

    Returns
    -------
    float
        Cumulative length of the flow path from *hydro_id* to the network
        outlet (or to the furthest reachable downstream reach).

    Raises
    ------
    RuntimeError
        If more than one downstream reach is found for any LINKNO.
    """
    cum_length = 0.0
    current_id: Union[int, None] = hydro_id

    while current_id is not None:
        curs.execute(
            NEXT_REACH_QUERY.format(feature_class),
            (current_id,),
        )
        rows = curs.fetchall()
        if len(rows) == 1:
            cum_length += rows[0][0] or 0.0
            current_id = rows[0][1]  # may be None at the outlet
        elif not rows:
            break
        else:
            raise RuntimeError(
                f"More than one downstream reach found for LINKNO {current_id}"
            )

    return cum_length


def _assign_level_path(
    curs: sqlite3.Cursor,
    feature_class: str,
    headwater_hydro_id: int,
    level_path: float,
) -> int:
    """Walk downstream from *headwater_hydro_id*, stamping *level_path*.

    Stamping stops when:
    * the current reach already has a ``level_path`` value (another — longer —
      path has already claimed it), or
    * the network outlet is reached (no downstream neighbour).

    Parameters
    ----------
    curs : sqlite3.Cursor
        Active cursor on a writable connection.
    feature_class : str
        Table name.
    headwater_hydro_id : int
        LINKNO of the headwater reach to start from.
    level_path : float
        Value to write into the ``level_path`` column.

    Returns
    -------
    int
        Number of reaches that were updated (stamped) in this call.

    Raises
    ------
    RuntimeError
        If more than one downstream reach is found for any LINKNO.
    """
    num_reaches = 0
    current_id: Union[int, None] = headwater_hydro_id

    while current_id is not None:
        curs.execute(
            f"UPDATE {feature_class} "
            f"SET level_path = ? "
            f"WHERE LINKNO = ? AND level_path IS NULL",
            (level_path, current_id),
        )
        if curs.rowcount == 0:
            # Reach already labelled — merged into a previously assigned path.
            break
        num_reaches += 1

        curs.execute(
            NEXT_REACH_QUERY.format(feature_class),
            (current_id,),
        )
        rows = curs.fetchall()
        if len(rows) == 1:
            current_id = rows[0][1]  # may be None at the outlet
        elif not rows:
            break
        else:
            raise RuntimeError(
                f"More than one downstream reach found for LINKNO {current_id}"
            )

    return num_reaches


def _add_column(
    curs: sqlite3.Cursor,
    table: str,
    column_name: str,
    column_type: str,
) -> None:
    """Add *column_name* to *table* if it does not already exist.

    Parameters
    ----------
    curs : sqlite3.Cursor
        Active cursor.
    table : str
        Table name.
    column_name : str
        Name of the column to add.
    column_type : str
        SQLite type affinity string, e.g. ``'REAL'`` or ``'INTEGER'``.
    """
    curs.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in curs.fetchall()}
    if column_name not in existing:
        curs.execute(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}")
    else:
        pass  # column already present — nothing to do


def _create_index(
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
        curs.execute(
            f"CREATE INDEX {index_name} ON {table}({', '.join(col_list)})"
        )


def _drop_table_triggers(
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


def _restore_table_triggers(
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
