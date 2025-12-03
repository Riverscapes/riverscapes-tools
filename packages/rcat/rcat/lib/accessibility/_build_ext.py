"""Custom build_ext command that cythonizes the floodplain accessibility module."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from Cython.Build import cythonize
import numpy
from setuptools.command.build_ext import build_ext as _build_ext


class BuildExt(_build_ext):
    """Run cythonize and ensure numpy headers are available before building."""

    def get_ext_fullpath(self, ext_name: str) -> str:  # pragma: no cover - exercised by build backend
        fullpath = super().get_ext_fullpath(ext_name)
        if not self.inplace or not ext_name.startswith("rcat."):
            return fullpath

        path = Path(fullpath)
        parts = list(path.parts)
        try:
            idx = parts.index("rcat")
        except ValueError:
            return fullpath

        needs_insert = (
            idx > 0
            and parts[idx - 1] == "packages"
            and (idx + 1 == len(parts) or parts[idx + 1] != "rcat")
        )
        if not needs_insert:
            return fullpath

        parts.insert(idx + 1, "rcat")
        return str(Path(*parts))

    def build_extensions(self) -> None:
        directives: dict[str, Any] = {"language_level": "3"}
        self.extensions = cythonize(self.extensions, compiler_directives=directives)

        numpy_include = numpy.get_include()
        for ext in self.extensions:
            if numpy_include not in ext.include_dirs:
                ext.include_dirs.append(numpy_include)
            if not hasattr(ext, "_needs_stub"):
                ext._needs_stub = False  # type: ignore[attr-defined]

        super().build_extensions()
