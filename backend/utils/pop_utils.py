import json
import logging
import os
import threading

logger = logging.getLogger(__name__)

CACHE_FILE = "cache/pop_locations.json"

# mtime-revalidated cache for the parsed lat/lon map. Bootstrap calls
# get_pop_lat_lon_map() on every dashboard refresh; the POPs file rarely
# changes (Fastly adds/removes datacenters maybe monthly) so the open +
# json.loads + dict-comp on each request is wasted work.
_lat_lon_cache: tuple[int, dict] | None = None
_lat_lon_cache_lock = threading.Lock()


def fetch_pop_locations(api_key: str) -> bool:
    """Fetch Fastly POP locations using the provided token and cache them.
    Returns True if successful.
    """
    if not api_key:
        return False

    # /datacenters endpoint requires auth but provides the exact flat
    # list of coordinates we need. fastly() handles auth + retry +
    # telemetry internally — replaces the old urllib.request flow plus
    # the redundant outer tracked_call wrapper.
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
            pass
    return []


def get_pop_lat_lon_map():
    """Return a mapping of POP code to (lat, lon) using cached data.

    Result is memoized and revalidated by CACHE_FILE's st_mtime_ns —
    fetch_pop_locations writes via overwrite so a real change bumps the
    mtime and busts the cache automatically.
    """
    global _lat_lon_cache
    try:
        mtime_ns = os.stat(CACHE_FILE).st_mtime_ns
    except FileNotFoundError:
        return {}

    cached = _lat_lon_cache
    if cached is not None and cached[0] == mtime_ns:
        return cached[1]

    resp_data = get_pop_locations()
    if not isinstance(resp_data, list):
        return {}
    # Standard /datacenters format (flat list of dicts with 'code', 'coordinates')
    result = {
        str(p["code"]).upper(): (p["coordinates"]["latitude"], p["coordinates"]["longitude"])
        for p in resp_data
        if p.get("code")
        and "coordinates" in p
        and p["coordinates"].get("latitude") is not None
        and p["coordinates"].get("longitude") is not None
    }
    with _lat_lon_cache_lock:
        _lat_lon_cache = (mtime_ns, result)
    return result
