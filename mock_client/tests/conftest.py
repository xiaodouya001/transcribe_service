"""Local test bootstrap for mock-client tests."""

from pathlib import Path
import sys


_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC_ROOT = _REPO_ROOT / "src"

for _path in (_REPO_ROOT, _SRC_ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
