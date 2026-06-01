import json
import os
import threading
import urllib.error
import urllib.request

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

    try:
        from backend.utils.telemetry import tracked_call
    except ImportError:
        tracked_call = None

    def _do_fetch():
        try:
            headers = {"User-Agent": "FastlyLogAnalysis/1.0", "Accept": "application/json", "Fastly-Key": api_key}
            # /datacenters endpoint requires auth but provides the exact flat list of coordinates we need
            req = urllib.request.Request("https://api.fastly.com/datacenters", headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                pops = json.loads(resp.read().decode("utf-8"))

            os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
            with open(CACHE_FILE, "w") as f:
                json.dump(pops, f)
            return True
        except Exception as e:
            print(f"Warning: POP fetch failed: {e}")
            return False

    if tracked_call:
        with tracked_call("GET", "/datacenters", service="Fastly API"):
            return _do_fetch()
    return _do_fetch()


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
