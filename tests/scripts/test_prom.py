from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from scripts.lib import prom


def test_histogram_quantile_matches_the_legacy_query_text() -> None:
    assert prom.histogram_quantile(0.95, "http_server_duration_milliseconds_bucket") == (
        "histogram_quantile(0.95,sum by (le)(rate(http_server_duration_milliseconds_bucket[5m])))"
    )


@pytest.fixture
def prom_server() -> Iterator[tuple[str, list[str]]]:
    seen: list[str] = []
    body: dict[str, object] = {"data": {"result": [{"value": [1700000000, "12.5"]}]}}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            seen.append(parse_qs(urlparse(self.path).query)["query"][0])
            if "boom" in seen[-1]:
                self.send_response(500)
                self.end_headers()
                return
            payload = {"data": {"result": []}} if "empty" in seen[-1] else body
            raw = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args: object) -> None:
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/", seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_instant_returns_first_sample_and_sends_the_query(prom_server: tuple[str, list[str]]) -> None:
    base, seen = prom_server
    assert prom.instant(base, "up{job='a b'}") == 12.5
    assert seen == ["up{job='a b'}"]


def test_instant_is_none_for_empty_result_http_error_and_unreachable(prom_server: tuple[str, list[str]]) -> None:
    base, _ = prom_server
    assert prom.instant(base, "empty") is None
    assert prom.instant(base, "boom") is None
    assert prom.instant("http://127.0.0.1:1", "up", timeout=1) is None
