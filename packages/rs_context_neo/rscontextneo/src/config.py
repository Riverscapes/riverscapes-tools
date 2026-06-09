"""
RS Context Neo — Configuration Loader

Loads, validates, and parses a regional profile JSON file into typed
dataclasses that the rest of the tool consumes.

Workflow
--------
1.  Load the raw JSON from disk.
2.  Substitute ``{env:VAR}`` tokens using environment variables (same pattern
    as the existing ``.env`` / ``rsxml.dotenv`` mechanism).
3.  Validate the substituted document against the JSON Schema in
    ``config/schema.json`` using ``jsonschema``.
4.  Parse the validated dict into :class:`RSContextNeoConfig` and its nested
    dataclasses.

Environment-variable substitution
----------------------------------
Any string value in the JSON file may contain one or more ``{env:VAR}``
tokens.  Substitution is performed recursively before schema validation so
the validator sees the final resolved values.  A missing environment variable
raises :class:`ConfigEnvError` with a clear message pointing at the variable
name and the JSON key path where it was referenced.

Skipping validation
-------------------
Pass ``validate=False`` to :func:`load_config` to skip the jsonschema step
(e.g. in unit tests that construct config dicts directly).

Author:     Matt Reimer
Date:       2026-06-03
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Union

# ── Schema path ───────────────────────────────────────────────────────────────
# Resolved relative to this file: rscontextneo/src/config.py
#   → ../../config/schema.json  (i.e. packages/rs_context_neo/config/schema.json)
_SCHEMA_PATH: Path = Path(__file__).parent.parent.parent / "config.schema.json"

# Matches {env:VAR_NAME} tokens anywhere in a string value.
_ENV_TOKEN_RE = re.compile(r"\{env:([^}]+)\}")


# ── Exceptions ────────────────────────────────────────────────────────────────


class ConfigValidationError(ValueError):
    """Raised when the config file fails JSON Schema validation."""


class ConfigEnvError(KeyError):
    """Raised when a referenced environment variable is not set."""


# ── Dataclasses ───────────────────────────────────────────────────────────────


@dataclass
class TnmOptions:
    """Fine-tuning knobs for the USGS National Map tile backend."""

    download_workers: int = 4
    buffer_deg: float = 0.01


@dataclass
class WcsOptions:
    """Configuration for any OGC WCS 1.0.0 DEM endpoint."""

    url: str = ""
    coverage: str = ""
    version: str = "1.0.0"
    format: str = "GeoTIFF"
    tile_pixels: int = 2000
    download_workers: int = 4
    timeout_s: int = 120
    max_attempts: int = 5
    retry_backoff_s: float = 5.0


@dataclass
class DemConfig:
    """DEM acquisition parameters."""

    source: str  # "tnm" | "wcs" | "file"
    resolution: float = 1.0
    download_dir: Optional[str] = None
    scratch_dir: Optional[str] = None
    aoi: Optional[str] = None
    tnm_options: Optional[TnmOptions] = field(default_factory=lambda: TnmOptions())
    wcs_options: Optional[WcsOptions] = None
    filepath: Optional[str] = None  # Required when source is "file"


@dataclass
class HydrologyConfig:
    """Parameters for the D8 hydrology processing chain."""

    threshold: int = 50_000
    breach_dist: int = 100
    cores: Optional[int] = None


@dataclass
class S3TablesSubLayer:
    """One sublayer (table) within an S3 Tables layer."""

    layer_name: str
    s3tables_path: str


@dataclass
class S3TablesLayerConfig:
    """Fetches vector sublayers from AWS Athena S3 Tables."""

    id: str
    label: str
    output_path: str
    athena_output: str
    sublayers: list  # list[S3TablesSubLayer]
    type: str = "s3tables"


@dataclass
class CogClipLayerConfig:
    """Clips a Cloud-Optimized GeoTIFF (or any GDAL-readable raster) to the AOI."""

    id: str
    label: str
    output_path: str
    url: str
    band: int = 1
    nodata: Optional[float] = None
    type: str = "cog_clip"


@dataclass
class WfsLayerConfig:
    """Fetches vector features from an OGC WFS endpoint."""

    id: str
    label: str
    output_path: str
    url: str
    typename: str
    layer_name: str
    version: str = "2.0.0"
    type: str = "wfs"


@dataclass
class WcsRasterLayerConfig:
    """Fetches a raster coverage from an OGC WCS endpoint."""

    id: str
    label: str
    output_path: str
    url: str
    coverage: str
    layer_name: str
    version: str = "1.0.0"
    type: str = "wcs_raster"


# Union type for all layer configs — used for type hints elsewhere.
# Using typing.Union so this works on Python 3.9 (the | operator between
# types at module-level is only valid at runtime on Python ≥ 3.10).
LayerConfig = Union[
    S3TablesLayerConfig, CogClipLayerConfig, WfsLayerConfig, WcsRasterLayerConfig
]


@dataclass
class RSContextNeoConfig:
    """Top-level validated, parsed configuration for one RS Context Neo run."""

    dem: DemConfig
    hydrology: HydrologyConfig
    layers: list  # list[LayerConfig]
    metadata: dict = field(default_factory=dict)
    profile_name: str = ""
    description: str = ""


# ── Borg singleton ───────────────────────────────────────────────────────────


class AppConfig:
    """Classmethod-based global config namespace for the active run.

    This is a simple class-level singleton: all state lives in the class
    attribute ``_shared_state`` and is accessed exclusively through the
    :meth:`set`, :meth:`get`, and :meth:`reset` classmethods.  No instances
    of this class are ever created.
    """

    _shared_state: dict[str, Any] = {}

    @classmethod
    def set(cls, config: RSContextNeoConfig) -> None:  # noqa: A003
        """Store *config* in the shared class-level state."""
        cls._shared_state["_config"] = config

    @classmethod
    def get(cls) -> RSContextNeoConfig:
        """Return the active config, raising ``RuntimeError`` if not yet set."""
        cfg = cls._shared_state.get("_config")
        if cfg is None:
            raise RuntimeError(
                "AppConfig has not been initialised — call AppConfig.set(config) before accessing it"
            )
        return cfg

    @classmethod
    def reset(cls) -> None:
        """Clear the stored config (useful in tests to avoid cross-test state pollution)."""
        cls._shared_state.pop("_config", None)


# ── Public API ────────────────────────────────────────────────────────────────


def load_config(
    config_path: str | Path,
    *,
    env_path: str | Path | None = None,
    validate: bool = True,
) -> RSContextNeoConfig:
    """Load a regional profile JSON file and return a parsed config object.

    Parameters
    ----------
    config_path : str or Path
        Path to the profile JSON file (e.g. ``config/us_conus.json``).
    env_path : str or Path or None
        Optional path to a ``.env`` file whose ``KEY=VALUE`` pairs are loaded
        into ``os.environ`` before substitution.  Variables already present in
        the environment take precedence (the file does not overwrite them).
        Defaults to the ``rscontextneo/.env`` file alongside this package.
    validate : bool
        If True (default), validate the substituted document against the JSON
        Schema.  Pass False to skip validation (useful in tests).

    Returns
    -------
    RSContextNeoConfig
        Fully populated, validated configuration object.

    Raises
    ------
    FileNotFoundError
        If *config_path* does not exist.
    ConfigEnvError
        If a ``{env:VAR}`` token references a variable that is not set.
    ConfigValidationError
        If the config fails JSON Schema validation (only when validate=True).
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # ── 1. Load .env ─────────────────────────────────────────────────────────
    if env_path is None:
        # Default: the .env alongside rscontextneo/ (the same one dotenv uses)
        env_path = Path(__file__).parent.parent / ".env"
    _load_dotenv(env_path)

    # ── 2. Load raw JSON ──────────────────────────────────────────────────────
    with open(config_path, encoding="utf-8") as fh:
        raw: dict = json.load(fh)

    # ── 3. Substitute {env:VAR} tokens ────────────────────────────────────────
    substituted = _substitute_env_vars(raw, path="<root>")

    # ── 4. Validate against schema ────────────────────────────────────────────
    if validate:
        _validate(substituted, config_path)

    # ── 5. Parse into dataclasses ─────────────────────────────────────────────
    return _parse_config(substituted)


# ── Internal: .env loader ─────────────────────────────────────────────────────


def _load_dotenv(env_path: Path) -> None:
    """Load KEY=VALUE pairs from *env_path* into os.environ.

    Variables already present in the environment are *not* overwritten (same
    behaviour as python-dotenv with ``override=False``).  Lines starting with
    ``#`` and blank lines are skipped.  Inline ``#`` comments are not stripped
    (values are taken verbatim after the ``=``).
    """
    env_path = Path(env_path)
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if key and key not in os.environ:
                os.environ[key] = value


# ── Internal: env-var substitution ───────────────────────────────────────────


def _substitute_env_vars(value: Any, path: str = "") -> Any:
    """Recursively replace ``{env:VAR}`` tokens in *value*.

    Parameters
    ----------
    value : Any
        A JSON-decoded value (str, dict, list, int, float, bool, None).
    path : str
        Dot-separated key path used in error messages (e.g. ``'dem.download_dir'``).

    Returns
    -------
    Any
        The value with all ``{env:VAR}`` tokens replaced.

    Raises
    ------
    ConfigEnvError
        If any referenced variable is absent from the environment.
    """
    if isinstance(value, str):

        def _replace(match: re.Match) -> str:
            var_name = match.group(1)
            if var_name not in os.environ:
                raise ConfigEnvError(
                    f"Environment variable '{var_name}' is referenced in the config "
                    f"at '{path}' but is not set. "
                    f"Add it to your .env file or shell environment."
                )
            return os.environ[var_name]

        return _ENV_TOKEN_RE.sub(_replace, value)

    if isinstance(value, dict):
        return {
            k: _substitute_env_vars(v, path=f"{path}.{k}" if path else k)
            for k, v in value.items()
        }

    if isinstance(value, list):
        return [
            _substitute_env_vars(item, path=f"{path}[{i}]")
            for i, item in enumerate(value)
        ]

    # int, float, bool, None — pass through unchanged
    return value


# ── Internal: schema validation ───────────────────────────────────────────────


def _validate(doc: dict, config_path: Path) -> None:
    """Validate *doc* against the JSON Schema at :data:`_SCHEMA_PATH`.

    Raises
    ------
    ConfigValidationError
        With a human-readable message pointing at the failing field.
    ImportError
        If ``jsonschema`` is not installed (only raised here to keep the
        import at call-time so the rest of the module works without it).
    """
    try:
        import jsonschema  # pylint: disable=import-outside-toplevel
    except ImportError as exc:
        raise ImportError(
            "The 'jsonschema' package is required for config validation. "
            "Install it with: pip install jsonschema"
        ) from exc

    with open(_SCHEMA_PATH, encoding="utf-8") as fh:
        schema = json.load(fh)

    try:
        jsonschema.validate(doc, schema)
    except jsonschema.ValidationError as exc:
        path_str = " → ".join(str(p) for p in exc.absolute_path) or "<root>"
        raise ConfigValidationError(
            f"Config file '{config_path}' failed schema validation.\n"
            f"  Location : {path_str}\n"
            f"  Problem  : {exc.message}"
        ) from exc


# ── Internal: parsing ─────────────────────────────────────────────────────────


def _parse_config(raw: dict) -> RSContextNeoConfig:
    dem = _parse_dem(raw["dem"])
    # Top-level download_dir / scratch_dir are the canonical location in the
    # config JSON.  Propagate them into DemConfig when the dem-level ones are absent.
    if dem.download_dir is None:
        dem.download_dir = raw.get("download_dir")
    if dem.scratch_dir is None:
        dem.scratch_dir = raw.get("scratch_dir")
    return RSContextNeoConfig(
        dem=dem,
        hydrology=_parse_hydrology(raw.get("hydrology", {})),
        layers=[_parse_layer(lyr) for lyr in raw.get("layers", [])],
        metadata=raw.get("metadata", {}),
        profile_name=raw.get("profile_name", ""),
        description=raw.get("description", ""),
    )


def _parse_dem(raw: dict) -> DemConfig:
    source = raw["source"]

    # ── Validate conditional structure based on source type ───────────────────
    if source in ("tnm", "wcs"):
        if "aoi" not in raw:
            raise ConfigValidationError(
                f"dem.source is '{source}' but 'aoi' is missing. "
                f'When using \'tnm\' or \'wcs\', provide: {{"source": "{source}", "aoi": "...", "resolution": ..., "tnm_options"/"wcs_options": ...}}'
            )
        if "resolution" not in raw:
            raise ConfigValidationError(
                f"dem.source is '{source}' but 'resolution' is missing. "
                f"Provide a resolution value (default: 1.0)."
            )
        tnm_raw = raw.get("tnm_options") or {}
        tnm_opts = TnmOptions(
            download_workers=tnm_raw.get("download_workers", 4),
            buffer_deg=tnm_raw.get("buffer_deg", 0.01),
        )

        wcs_raw = raw.get("wcs_options")
        wcs_opts: Optional[WcsOptions] = None
        if wcs_raw is not None:
            wcs_opts = WcsOptions(
                url=wcs_raw["url"],
                coverage=wcs_raw["coverage"],
                version=wcs_raw.get("version", "1.0.0"),
                format=wcs_raw.get("format", "GeoTIFF"),
                tile_pixels=wcs_raw.get("tile_pixels", 2000),
                download_workers=wcs_raw.get("download_workers", 4),
                timeout_s=wcs_raw.get("timeout_s", 120),
                max_attempts=wcs_raw.get("max_attempts", 5),
                retry_backoff_s=wcs_raw.get("retry_backoff_s", 5.0),
            )

    elif source == "file":
        if "filepath" not in raw or not os.path.isfile(raw["filepath"]):
            raise ConfigValidationError(
                "dem.source is 'file' but 'filepath' is missing or invalid. "
                'Provide: {"source": "file", "filepath": "..."}'
            )
        tnm_raw = raw.get("tnm_options") or {}
        tnm_opts = TnmOptions(
            download_workers=tnm_raw.get("download_workers", 4),
            buffer_deg=tnm_raw.get("buffer_deg", 0.01),
        )
        wcs_raw = raw.get("wcs_options")
        wcs_opts: Optional[WcsOptions] = None
    else:
        raise ConfigValidationError(
            f"dem.source must be one of 'tnm', 'wcs', or 'file'. Got: '{source}'"
        )

    return DemConfig(
        source=source,
        resolution=raw.get("resolution", 1.0),
        download_dir=raw.get("download_dir"),
        scratch_dir=raw.get("scratch_dir"),
        aoi=raw.get("aoi"),
        tnm_options=tnm_opts,
        wcs_options=wcs_opts,
        filepath=raw.get("filepath"),
    )


def _parse_hydrology(raw: dict) -> HydrologyConfig:
    return HydrologyConfig(
        threshold=raw.get("threshold", 50_000),
        breach_dist=raw.get("breach_dist", 100),
        cores=raw.get("cores"),
    )


def _parse_layer(raw: dict) -> LayerConfig:
    ltype = raw["type"]

    if ltype == "s3tables":
        sublayers = [
            S3TablesSubLayer(
                layer_name=s["layer_name"],
                s3tables_path=s["s3tables_path"],
            )
            for s in raw["sublayers"]
        ]
        return S3TablesLayerConfig(
            id=raw["id"],
            label=raw["label"],
            output_path=raw["output_path"],
            athena_output=raw["athena_output"],
            sublayers=sublayers,
        )

    if ltype == "cog_clip":
        return CogClipLayerConfig(
            id=raw["id"],
            label=raw["label"],
            output_path=raw["output_path"],
            url=raw["url"],
            band=raw.get("band", 1),
            nodata=raw.get("nodata"),
        )

    if ltype == "wfs":
        return WfsLayerConfig(
            id=raw["id"],
            label=raw["label"],
            output_path=raw["output_path"],
            url=raw["url"],
            typename=raw["typename"],
            layer_name=raw["layer_name"],
            version=raw.get("version", "2.0.0"),
        )

    if ltype == "wcs_raster":
        return WcsRasterLayerConfig(
            id=raw["id"],
            label=raw["label"],
            output_path=raw["output_path"],
            url=raw["url"],
            coverage=raw["coverage"],
            layer_name=raw["layer_name"],
            version=raw.get("version", "1.0.0"),
        )

    raise ValueError(
        f"Unknown layer type '{ltype}'. "
        f"Valid types: s3tables, cog_clip, wfs, wcs_raster."
    )
