"""Root conftest — adds core/ and runtime/ to sys.path for pytest."""
import sys
import pathlib

_ROOT = pathlib.Path(__file__).resolve().parent
for _d in (_ROOT / "core", _ROOT / "runtime"):
    _ds = str(_d)
    if _ds not in sys.path:
        sys.path.insert(0, _ds)
