import json
import logging
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

API_BASE = "https://api.fastly.com"


def fastly(method, path, body=None, *, token, expect_empty=False, max_retries=3):
    """Make a Fastly API request and return parsed JSON."""
    try:
        from backend.utils.telemetry import tracked_call

        telemetry_context = tracked_call(method, path, service="Fastly API")
    except ImportError:
        telemetry_context = None

    def _do_call():
        url = API_BASE + path
        data = json.dumps(body).encode() if body is not None else None
        hdrs = {"Fastly-Key": token, "Accept": "application/json"}
        if data:
            hdrs["Content-Type"] = "application/json"

        last_exc = None
        for attempt in range(max_retries + 1):
            req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    raw = resp.read().decode()
                    if expect_empty or not raw.strip():
                        return {}
                    return json.loads(raw)
            except urllib.error.HTTPError as exc:
                if exc.code in (429, 500, 502, 503, 504) and attempt < max_retries:
                    time.sleep(2**attempt)
                    continue
                body_text = exc.read().decode(errors="replace")
                raise RuntimeError(f"HTTP {exc.code} {method} {path}\n    {body_text}") from exc
            except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
                last_exc = exc
                if attempt < max_retries:
                    time.sleep(2**attempt)
                    continue
                raise RuntimeError(f"Network error on {method} {path}: {exc}") from exc

        raise RuntimeError(f"Failed after {max_retries} retries: {last_exc}")

    if telemetry_context:
        with telemetry_context:
            return _do_call()
    else:
        return _do_call()
