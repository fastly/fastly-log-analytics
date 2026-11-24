"""Thin wrapper delegating to scripts.check_secret_values."""

from __future__ import annotations

import os  # noqa: F401
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.check_secret_values import (  # noqa: E402, F401
    SENSITIVE,
    credential_values,
    find_exposures,
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
