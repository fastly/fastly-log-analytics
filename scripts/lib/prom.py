"""Minimal Prometheus instant-query client for operator scripts.

Uses ``admin_urlopen`` so a configured mTLS origin presents its client
identity. Every failure mode (unreachable, bad JSON, empty result) collapses
to ``None`` so a missing metric never aborts a harness run.
"""

import json
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import Request

# Keep direct `python scripts/lib/prom.py` imports working.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.lib.admin_tls import admin_urlopen  # noqa: E402


def histogram_quantile(quantile: float, bucket_metric: str, window: str = "5m") -> str:
    return f"histogram_quantile({quantile},sum by (le)(rate({bucket_metric}[{window}])))"


def instant(base_url: str, query: str, timeout: float = 10) -> float | None:
    """First sample of an instant query as a float, or ``None``."""
    request = Request(f"{base_url.rstrip('/')}/api/v1/query?query={quote(query)}", method="GET")
    try:
        with admin_urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read())
        return float(payload["data"]["result"][0]["value"][1])
    except (OSError, URLError, TimeoutError, json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError):
        return None
