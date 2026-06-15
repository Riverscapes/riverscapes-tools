# vector_prep package
from pathlib import Path
import re

_ver_file = Path(__file__).parent.parent / "__version__.py"
_match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', _ver_file.read_text())
__version__: str = _match.group(1) if _match else "0.0.0"

__all__ = ["__version__"]
