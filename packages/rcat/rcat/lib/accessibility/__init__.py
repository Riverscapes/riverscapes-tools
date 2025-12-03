"""Cython-powered helpers for the floodplain accessibility tools."""

from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - imported only for typing help
    from . import access as access_module


def _safe_import() -> ModuleType:
    """Import the compiled access module with a clear error if missing."""
    try:
        return import_module("rcat.lib.accessibility.access")
    except ModuleNotFoundError as exc:  # pragma: no cover - happens only before build
        raise RuntimeError(
            "The Cython extension rcat.lib.accessibility.access is not built."
            " Run `uv build` or `uv pip install -e .` to compile it."
        ) from exc


access: "access_module" = _safe_import()  # type: ignore[assignment]

__all__ = ["access"]
