"""Environment, geography and error-position fields of a RUM beacon, resolved at write time.

Resolution lives in the fact writer (not the decoder) so a replay of an older archive, whose
rows only carry the raw edge-log keys, produces the same fact columns as a live ingest. Absent
values stay empty: an unknown browser is reported as unknown, never defaulted to a real one.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

_TEXT_ALIASES: dict[str, tuple[str, ...]] = {
    "country": ("country", "geo_country_code"),
    "browser": ("browser",),
    "os": ("os",),
    "device": ("device",),
    "city": ("city", "geo_city"),
    "region": ("region", "geo_region"),
    "pop": ("pop", "server_pop"),
    "tls": ("tls", "tls_version"),
}
_TTFB_KEYS = ("ttfb", "time_to_first_byte")


def _text(value: Any) -> str:
    if value is None or isinstance(value, bool):
        return ""
    return str(value).strip()


def _first_text(row: Mapping[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        text = _text(row.get(key))
        if text:
            return text
    return ""


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).strip())
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def rum_environment(row: Mapping[str, Any]) -> dict[str, Any]:
    """Text environment fields plus ``ttfb`` (None when absent or not finite)."""
    env: dict[str, Any] = {name: _first_text(row, keys) for name, keys in _TEXT_ALIASES.items()}
    ttfb: float | None = None
    for key in _TTFB_KEYS:
        ttfb = _finite(row.get(key))
        if ttfb is not None:
            break
    env["ttfb"] = ttfb
    return env


def _position(row: Mapping[str, Any], keys: tuple[str, ...]) -> int:
    for key in keys:
        number = _finite(row.get(key))
        if number is not None and number >= 0:
            return int(number)
    return 0


def rum_error_position(row: Mapping[str, Any]) -> tuple[int, int]:
    """``(line, column)`` of a JavaScript error; ``0`` when the beacon did not report one."""
    return _position(row, ("rum_error_line", "error_line")), _position(row, ("rum_error_col", "error_col"))
