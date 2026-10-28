"""Fastly POP location cache and Haversine SQL expression builders."""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from collections.abc import Callable
from typing import Any

from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

logger = logging.getLogger(__name__)

CACHE_FILE = "cache/pop_locations.json"

_DEFAULT_POP_COORDS: dict[str, tuple[float, float]] = {
    "SJC": (37.3382, -121.8863),
    "IAD": (38.9531, -77.4565),
    "NRT": (35.7720, 140.3929),
}
_SAFE_POP_CODE_RE = re.compile(r"^[A-Z0-9_]+$")

_POP_CACHE_TTL_S = 300.0
_lat_lon_cache: BoundedTTLCache | tuple[int, dict] | None = BoundedTTLCache(maxsize=16, ttl_seconds=_POP_CACHE_TTL_S)
_lat_lon_cache_lock = threading.Lock()

_loc_cache: BoundedTTLCache | tuple[int, dict] | None = BoundedTTLCache(maxsize=16, ttl_seconds=_POP_CACHE_TTL_S)
_loc_cache_lock = threading.Lock()

_CacheRegistry.register("utils.pop_utils._lat_lon_cache", _lat_lon_cache)
_CacheRegistry.register("utils.pop_utils._loc_cache", _loc_cache)


def fetch_pop_locations(api_key: str) -> bool:
    """Fetch Fastly POP locations using the provided token and cache them.
    Returns True if successful.
    """
    if not api_key:
        return False

    try:
        from backend.core.fastly.client import fastly

        pops = fastly("GET", "/datacenters", token=api_key)
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump(pops, f)
        return True
    except Exception:
        logger.warning("POP fetch failed", exc_info=True)
        return False


def get_pop_locations():
    """Return cached POP locations if available, else empty list."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            logger.debug("get_pop_locations: step failed (ignored)", exc_info=True)
    return []


def _load_mtime_cached(slot: str, lock: threading.Lock, builder: Callable[[list[Any]], dict]) -> dict:
    try:
        mtime_ns = os.stat(CACHE_FILE).st_mtime_ns
    except FileNotFoundError:
        return {}

    cache_obj = globals()[slot]
    if isinstance(cache_obj, (BoundedTTLCache, dict)):
        cached = cache_obj.get(CACHE_FILE)
    else:
        cached = cache_obj
    if cached is not None and cached[0] == mtime_ns:
        return cached[1]

    resp_data = get_pop_locations()
    if not isinstance(resp_data, list):
        return {}

    result = builder(resp_data)
    with lock:
        target = globals()[slot]
        if isinstance(target, (BoundedTTLCache, dict)):
            target[CACHE_FILE] = (mtime_ns, result)
        else:
            globals()[slot] = (mtime_ns, result)
    return result


def get_pop_lat_lon_map():
    """Return a mapping of POP code to (lat, lon) using cached data."""
    return _load_mtime_cached(
        "_lat_lon_cache",
        _lat_lon_cache_lock,
        lambda resp_data: {
            str(p["code"]).upper(): (p["coordinates"]["latitude"], p["coordinates"]["longitude"])
            for p in resp_data
            if p.get("code")
            and "coordinates" in p
            and p["coordinates"].get("latitude") is not None
            and p["coordinates"].get("longitude") is not None
        },
    )


def get_pop_location_map() -> dict[str, dict]:
    """Return ``{CODE: {"city", "region", "country"}}`` parsed from cached Fastly ``/datacenters``."""

    def _build(resp_data: list[Any]) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for p in resp_data:
            if not isinstance(p, dict) or not p.get("code"):
                continue
            shield_parts = str(p.get("shield") or "").split("-")
            region = shield_parts[1] if len(shield_parts) == 3 else ""
            country = shield_parts[-1] if len(shield_parts) >= 2 else ""
            result[str(p["code"]).upper()] = {
                "city": str(p.get("name") or ""),
                "region": region,
                "country": country,
            }
        return result

    return _load_mtime_cached("_loc_cache", _loc_cache_lock, _build)


def _effective_pop_lat_lon_map() -> dict[str, tuple[float, float]]:
    merged = dict(_DEFAULT_POP_COORDS)
    for code, coords in get_pop_lat_lon_map().items():
        norm = str(code).upper()
        if _SAFE_POP_CODE_RE.fullmatch(norm) and isinstance(coords, (tuple, list)) and len(coords) == 2:
            try:
                merged[norm] = (float(coords[0]), float(coords[1]))
            except (TypeError, ValueError):
                continue
    return merged


def _haversine_sql_expr(lat_col: str, lon_col: str, pop_lat: float, pop_lon: float) -> str:
    return (
        f"6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS({lat_col} - {pop_lat}) / 2), 2) "
        f"+ COS(RADIANS({pop_lat})) * COS(RADIANS({lat_col})) "
        f"* POWER(SIN(RADIANS({lon_col} - {pop_lon}) / 2), 2)))"
    )


def _build_pop_case_sql(
    pop_col: str,
    value_fn: Callable[[float, float], object],
    else_expr: object,
) -> str:
    coords = _effective_pop_lat_lon_map()
    whens = [f"WHEN {pop_col} = '{code}' THEN {value_fn(plat, plon)}" for code, (plat, plon) in coords.items()]
    return "CASE " + " ".join(whens) + f" ELSE {else_expr} END"


def build_pop_haversine_case_sql(
    pop_col: str = "pop",
    lat_col: str = "lat",
    lon_col: str = "lon",
    default_lat: float = 38.0,
    default_lon: float = -97.0,
) -> str:
    """Build a SQL CASE expression computing Haversine distance (km) from POP to client (lat, lon)."""
    return _build_pop_case_sql(
        pop_col,
        lambda plat, plon: _haversine_sql_expr(lat_col, lon_col, plat, plon),
        _haversine_sql_expr(lat_col, lon_col, float(default_lat), float(default_lon)),
    )


def build_pop_lat_case_sql(pop_col: str = "pop", default_lat: float = 38.0) -> str:
    """Build a SQL CASE expression mapping POP code to latitude."""
    return _build_pop_case_sql(pop_col, lambda plat, _plon: plat, float(default_lat))


def build_pop_lon_case_sql(pop_col: str = "pop", default_lon: float = -97.0) -> str:
    """Build a SQL CASE expression mapping POP code to longitude."""
    return _build_pop_case_sql(pop_col, lambda _plat, plon: plon, float(default_lon))
