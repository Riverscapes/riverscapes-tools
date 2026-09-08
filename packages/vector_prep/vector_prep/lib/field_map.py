"""
field_map.py — Field selection, renaming, and type-casting for vector_prep.

When a ``field_map`` is present in the layer config, this module handles:
  - Loading and validating a ``layer_definitions.json`` file that defines the
    expected output schema (column names and dtypes).
  - Validating that every output name referenced in the field_map exists in
    the layer_definitions schema.
  - Applying the field map to each GeoDataFrame chunk: selecting the mapped
    source columns, renaming them to the output names, and casting each column
    to the dtype declared in layer_definitions.

Public API
----------
ColumnSpec              — dataclass describing one output column
FieldMapConfig          — dataclass bundling field_map + column_specs together
DTYPE_MAP               — maps layer_definitions dtype strings → pandas dtypes
load_layer_definitions  — load a layer_definitions.json → Dict[name, ColumnSpec]
validate_field_map      — raise ValueError if output names missing from specs
load_and_validate_field_map — convenience: load + validate → FieldMapConfig
apply_field_map         — apply a FieldMapConfig to a GeoDataFrame chunk
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maps the dtype strings used in layer_definitions.json to the pandas dtype
# used when casting a Series.  The special value ``"str"`` is handled
# procedurally in apply_field_map (None-safe string coercion).
DTYPE_MAP: dict[str, str] = {
    "STRING": "string",  # key guards dtype presence; STRING branch handles casting manually
    "INTEGER": "Int64",  # pandas nullable integer
    "FLOAT": "float64",
    "BOOLEAN": "boolean",  # pandas nullable boolean
    "DATETIME": "datetime64[ns]",  # pandas datetime (timezone-naive)
}


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ColumnSpec:
    """Describes a single output column from a layer_definitions.json file.

    Attributes:
        name:          The canonical output column name.
        dtype:         The raw dtype string from layer_definitions
                       (``"STRING"``, ``"INTEGER"``, ``"FLOAT"``, ``"BOOLEAN"``, ``"DATETIME"``).
        friendly_name: Optional human-readable label (informational only).
        description:   Optional description text (informational only).
    """

    name: str
    dtype: str
    friendly_name: str | None = None
    description: str | None = None


@dataclass
class FieldMapConfig:
    """Bundles a validated field_map with the corresponding column specs.

    Attributes:
        field_map:    Mapping of source_field_name → output_field_name.
        column_specs: Mapping of output_field_name → ColumnSpec (from
                      layer_definitions).  Every value in ``field_map``
                      is guaranteed to have an entry here.
    """

    field_map: dict[str, str]
    column_specs: dict[str, ColumnSpec] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Loading helpers
# ---------------------------------------------------------------------------


def _read_layers_array(path: Path) -> list[dict]:
    """Read a layer_definitions file and return its ``layers`` array."""
    path = Path(path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"layer_definitions file not found: {path}")

    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Could not parse layer_definitions JSON: {path}\n{exc}"
        ) from exc

    layers: list[dict] = data.get("layers", [])
    if not layers:
        raise ValueError(f"layer_definitions file has no 'layers' array: {path}")

    return layers


def list_layer_definitions(path: Path) -> list[tuple[str, str | None]]:
    """Return available ``(layer_id, layer_name)`` entries from layer_definitions."""
    layers = _read_layers_array(path)
    summary: list[tuple[str, str | None]] = []
    for idx, layer_obj in enumerate(layers, start=1):
        layer_id = layer_obj.get("layer_id")
        if not layer_id:
            raise ValueError(
                f"layer_definitions layer at index {idx} is missing required 'layer_id'."
            )
        summary.append((str(layer_id), layer_obj.get("layer_name")))
    return summary


def load_layer_definitions(
    path: Path,
    layer_id: str | None = None,
) -> dict[str, ColumnSpec]:
    """Load a layer_definitions.json and return a flat column-spec dict.

    The file may contain multiple layer objects inside ``layers``.  All
    ``columns`` arrays are merged into a single dict keyed by column ``name``.
    If two layers define the same column name, the later definition wins and a
    warning is logged.

    Args:
        path: Absolute or relative path to the ``layer_definitions.json`` file.

    Returns:
        Dict mapping output column name → :class:`ColumnSpec`.

    Raises:
        FileNotFoundError: If *path* does not exist.
        ValueError:        If the file cannot be parsed or is missing required
                           fields.
    """
    layers = _read_layers_array(path)

    selected_layer: dict
    if layer_id:
        matching = [layer for layer in layers if layer.get("layer_id") == layer_id]
        if not matching:
            available = ", ".join(layer[0] for layer in list_layer_definitions(path))
            raise ValueError(
                f"layer_id '{layer_id}' not found in layer_definitions. "
                f"Available layer_id values: {available}"
            )
        selected_layer = matching[0]
    elif len(layers) == 1:
        selected_layer = layers[0]
    else:
        available = ", ".join(layer[0] for layer in list_layer_definitions(path))
        raise ValueError(
            "layer_definitions contains multiple layers. "
            f"Specify layer_id. Available layer_id values: {available}"
        )

    column_specs: dict[str, ColumnSpec] = {}

    selected_layer_id = selected_layer.get("layer_id", "<unknown>")
    columns: list[dict] = selected_layer.get("columns", [])
    for col in columns:
        col_name = col.get("name")
        dtype = col.get("dtype")
        if not col_name:
            log.warning(
                "layer_definitions layer '%s' has a column with no 'name'; skipping.",
                selected_layer_id,
            )
            continue
        if not dtype:
            log.warning(
                "layer_definitions layer '%s' column '%s' has no 'dtype'; defaulting to STRING.",
                selected_layer_id,
                col_name,
            )
            dtype = "STRING"
        if col_name in column_specs:
            log.warning(
                "Duplicate column name '%s' found in layer_definitions layer '%s'; later definition wins.",
                col_name,
                selected_layer_id,
            )
        column_specs[col_name] = ColumnSpec(
            name=col_name,
            dtype=dtype,
            friendly_name=col.get("friendly_name"),
            description=col.get("description"),
        )

    return column_specs


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_field_map(
    field_map: dict[str, str],
    column_specs: dict[str, ColumnSpec],
    allow_missing_output_names: set[str] | None = None,
) -> None:
    """Validate that every output name in *field_map* exists in *column_specs*.

    Source names (keys) are **not** validated here — they come from the raw
    input data and are checked at apply-time in :func:`apply_field_map`.

    Args:
        field_map:    Mapping of source_name → output_name.
        column_specs: Dict of output_name → :class:`ColumnSpec` (from
                      :func:`load_layer_definitions`).
        allow_missing_output_names: Optional output column names that may be
                      absent from ``column_specs``. This is used for
                      passthrough fields that should be preserved in output
                      without dtype coercion from layer_definitions.

    Raises:
        ValueError: If any output name is absent from *column_specs*, listing
                    all missing names in the error message.
    """
    output_names = list(field_map.values())
    duplicates = sorted({n for n in output_names if output_names.count(n) > 1})
    if duplicates:
        raise ValueError(
            f"field_map maps multiple source fields to the same output name(s): {', '.join(duplicates)}"
        )

    allowed_missing = allow_missing_output_names or set()
    missing = [
        output_name
        for output_name in field_map.values()
        if output_name not in column_specs and output_name not in allowed_missing
    ]
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(
            f"field_map references output column(s) not found in layer_definitions: {missing_str}"
        )


# ---------------------------------------------------------------------------
# Convenience loader
# ---------------------------------------------------------------------------


def load_and_validate_field_map(
    raw_field_map: dict[str, str],
    layer_defs_path: Path,
    layer_id: str | None = None,
    allow_missing_output_names: set[str] | None = None,
) -> FieldMapConfig:
    """Load layer_definitions, validate the field_map, and return a FieldMapConfig.

    This is the primary entry-point used by the CLI.

    Args:
        raw_field_map:    The ``field_map`` dict from the config file
                          (source_name → output_name).
        layer_defs_path:  Path to the ``layer_definitions.json`` file.

    Returns:
        A fully-validated :class:`FieldMapConfig`.

    Raises:
        FileNotFoundError: If *layer_defs_path* does not exist.
        ValueError:        If any output name in *raw_field_map* is missing
                           from the layer_definitions schema.
    """
    column_specs = load_layer_definitions(layer_defs_path, layer_id=layer_id)
    validate_field_map(
        raw_field_map,
        column_specs,
        allow_missing_output_names=allow_missing_output_names,
    )
    # Keep only the specs that are actually referenced as output names in the
    # field_map — every other column in the layer_definitions file is irrelevant
    # to this run and would waste memory / cause confusion during casting.
    allowed_missing = allow_missing_output_names or set()
    relevant = set(raw_field_map.values())
    # Restrict column_specs to only what the field_map references before the
    # dtype check so that the error message only mentions relevant columns.
    filtered_specs = {k: v for k, v in column_specs.items() if k in relevant}
    bad_dtypes = [
        f"{output_name!r} (dtype={filtered_specs[output_name].dtype!r})"
        for output_name in raw_field_map.values()
        if output_name not in allowed_missing
        if filtered_specs[output_name].dtype not in DTYPE_MAP
    ]
    if bad_dtypes:
        raise ValueError(
            f"layer_definitions contains unsupported dtype(s): {'; '.join(bad_dtypes)}. "
            f"Supported dtypes: {', '.join(sorted(DTYPE_MAP))}"
        )
    return FieldMapConfig(
        field_map=raw_field_map,
        column_specs=filtered_specs,
    )


# ---------------------------------------------------------------------------
# Per-chunk application
# ---------------------------------------------------------------------------


def _cast_column(series: pd.Series, dtype: str) -> pd.Series:
    """Cast *series* to the pandas dtype corresponding to the given *dtype* string.

    Dispatch is driven by :data:`DTYPE_MAP`: if *dtype* is not a key in that
    mapping a ``ValueError`` is raised immediately rather than silently
    no-oping.

    Args:
        series: The column to cast.
        dtype:  Layer-definitions dtype string: ``"STRING"``, ``"INTEGER"``,
            ``"FLOAT"``, ``"BOOLEAN"``, or ``"DATETIME"``.

    Returns:
        The cast Series (a new object; the original is not mutated).

    Raises:
        ValueError: If *dtype* is not present in :data:`DTYPE_MAP`.
    """
    if dtype not in DTYPE_MAP:
        raise ValueError(
            f"Unknown dtype '{dtype}' for column '{series.name}'. "
            f"Valid dtypes are: {', '.join(sorted(DTYPE_MAP))}"
        )

    if dtype == "STRING":
        # Convert to str first, then replace sentinel "None"/"nan"/"<NA>"
        # strings and empty strings with actual None (pd.NA), then cast to
        # pandas nullable StringDtype so the return type is always consistent.
        def _to_clean_str(val: object) -> str | None:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            try:
                if pd.isna(cast(Any, val)):
                    return None
            except (TypeError, ValueError):
                pass
            s = str(val).strip()
            if s in ("None", "nan", "<NA>", ""):
                return None
            return s

        return series.apply(_to_clean_str).astype("string")

    if dtype == "BOOLEAN":
        pandas_dtype = cast(Any, DTYPE_MAP[dtype])

        def _to_bool(val: object) -> bool | None:
            if val is None:
                return None
            try:
                if pd.isna(cast(Any, val)):
                    return None
            except (TypeError, ValueError):
                pass
            return bool(val)

        # Use pandas BooleanArray (nullable) via pd.array so that None → pd.NA.
        # Wrap in pd.Series and pass index=series.index to preserve the original
        # (possibly non-contiguous) index, preventing row misalignment when the
        # GeoDataFrame has had features dropped earlier in the pipeline.
        bool_list = [_to_bool(v) for v in series]
        return pd.Series(
            pd.array(bool_list, dtype=pandas_dtype),
            dtype=pandas_dtype,
            index=series.index,
        )

    if dtype == "DATETIME":
        # Parse to UTC to handle mixed timezone / naive inputs consistently,
        # then drop timezone so GeoPackage writers receive datetime64[ns].
        parsed = pd.to_datetime(series, errors="coerce", utc=True)
        return pd.Series(parsed.dt.tz_localize(None), index=series.index)

    # INTEGER and FLOAT — simple numeric coerce + astype driven by DTYPE_MAP.
    pandas_dtype = cast(Any, DTYPE_MAP[dtype])
    return pd.to_numeric(series, errors="coerce").astype(pandas_dtype)


def apply_field_map(
    chunk_gdf: gpd.GeoDataFrame,
    fmc: FieldMapConfig,
) -> gpd.GeoDataFrame:
    """Select, rename, and cast columns according to a :class:`FieldMapConfig`.

    Called once per chunk immediately before writing output.  All checks and
    geometry cleaning must have already been applied; FID assignment happens
    *after* this function returns.

    Steps:
      1. Verify every source field (key in ``fmc.field_map``) exists in the
         GeoDataFrame columns.  Raises ``ValueError`` if any are absent.
      2. Select only the mapped source columns plus the geometry column.
      3. Rename source columns to their output names per ``fmc.field_map``.
      4. Cast each output column to the dtype declared in
         ``fmc.column_specs[output_name]`` via :data:`DTYPE_MAP`.

    Args:
        chunk_gdf: GeoDataFrame chunk (already cleaned, not yet FID-stamped).
        fmc:       Validated :class:`FieldMapConfig`.

    Returns:
        A new GeoDataFrame with only the mapped (and renamed) columns plus
        the geometry column.

    Raises:
        ValueError: If any source field in ``fmc.field_map`` is absent from
                    ``chunk_gdf.columns``.
    """
    # Step 1 — verify all source fields are present in the chunk.
    geom_col = str(chunk_gdf.geometry.name)
    data_cols = set(chunk_gdf.columns) - {geom_col}
    missing_src = [src for src in fmc.field_map if src not in data_cols]
    if missing_src:
        missing_str = ", ".join(sorted(missing_src))
        raise ValueError(
            f"field_map references source column(s) not found in data chunk: {missing_str}"
        )

    # Step 2 — select only the source columns we care about + geometry.
    src_cols = list(fmc.field_map.keys())
    chunk_gdf = chunk_gdf[src_cols + [geom_col]].copy()

    # Step 3 — rename source columns to their output names.
    chunk_gdf = chunk_gdf.rename(columns=fmc.field_map)

    # Step 4 — cast each output column to the declared dtype.
    for output_name, spec in fmc.column_specs.items():
        # Invariant: all output_names in column_specs are present in chunk_gdf after step 3.
        # This is guaranteed by load_and_validate_field_map + the rename in step 3.
        assert output_name in chunk_gdf.columns, (
            f"Internal invariant violated: '{output_name}' missing after rename step. "
            "This should not happen — check load_and_validate_field_map."
        )
        chunk_gdf[output_name] = _cast_column(chunk_gdf[output_name], spec.dtype)

    return chunk_gdf
