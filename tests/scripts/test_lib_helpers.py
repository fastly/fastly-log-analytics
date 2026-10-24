from __future__ import annotations

import asyncio
import ssl
from urllib.request import Request

import httpx
import pytest

from scripts.lib import admin_http, admin_tls


def test_total_deadline_cancels_a_stalled_response_body(monkeypatch: pytest.MonkeyPatch) -> None:
    closed = []

    class Stalled(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"{"
            await asyncio.Event().wait()

        async def aclose(self) -> None:
            closed.append(True)

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=Stalled()))
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})
    monkeypatch.setattr(admin_tls, "_context", lambda config: ssl.create_default_context())
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(**kwargs, transport=transport))
    with pytest.raises(TimeoutError):
        asyncio.run(admin_http.async_admin_json(Request("https://example.com/api/test", data=b"{}"), timeout=0.05))
    assert closed


def test_deadline_transport_preserves_request_and_refuses_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(302, headers={"location": "https://other.example.com/private"})

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(respond)
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})
    monkeypatch.setattr(admin_tls, "_context", lambda config: ssl.create_default_context())
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(**kwargs, transport=transport))
    request = Request(
        "https://example.com/api/test", data=b'{"filters":{}}', headers={"X-Page-Load-ID": "test-correlation"}
    )
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(admin_http.async_admin_json(request, timeout=1))
    assert len(seen) == 1
    assert seen[0].method == "POST"
    assert seen[0].content == b'{"filters":{}}'
    assert seen[0].headers["x-page-load-id"] == "test-correlation"


@pytest.mark.parametrize(
    "url,headers",
    [
        ("https://other.example.com/api/test", {}),
        ("https://example.com/api/test", {"X-Admin-Gateway-Token": "do-not-send"}),
    ],
)
def test_deadline_transport_rejects_untrusted_identity_use_before_tls(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    headers: dict,
) -> None:
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})

    def forbidden(config: dict) -> ssl.SSLContext:
        pytest.fail("TLS identity must not be loaded")

    monkeypatch.setattr(admin_tls, "_context", forbidden)
    with pytest.raises(ValueError):
        asyncio.run(admin_http.async_admin_json(Request(url, headers=headers), timeout=1))


# ── Merged from test_cli.py ──

import argparse

import pytest

from scripts.lib.cli import add_common_args


def _parser(**kwargs: object) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="t")
    add_common_args(parser, **kwargs)  # type: ignore[arg-type]
    return parser


def test_adds_nothing_unless_asked() -> None:
    with pytest.raises(SystemExit):
        _parser().parse_args(["--timeout", "1"])
    with pytest.raises(SystemExit):
        _parser().parse_args(["--json"])


def test_timeout_default_type_and_help() -> None:
    parser = _parser(timeout_default=5.0, timeout_label="HTTP request")
    assert parser.parse_args([]).timeout == 5.0
    assert parser.parse_args(["--timeout", "2.5"]).timeout == 2.5
    assert "HTTP request timeout in seconds (default: 5.0)" in parser.format_help()


def test_json_flag() -> None:
    parser = _parser(json_help="Output raw JSON results")
    assert parser.parse_args([]).json is False
    assert parser.parse_args(["--json"]).json is True
    assert "Output raw JSON results" in parser.format_help()


@pytest.mark.parametrize(
    ("module", "default"),
    [("scripts.check_environment_health", 5.0), ("scripts.dev.audit_environments", 10.0)],
)
def test_adopting_scripts_keep_their_timeout_defaults(
    module: str, default: float, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import importlib

    mod = importlib.import_module(module)
    monkeypatch.setattr("sys.argv", ["prog", "--help"])
    with pytest.raises(SystemExit):
        mod.main()
    out = capsys.readouterr().out
    assert f"timeout in seconds (default: {default})" in out
    assert "--json" in out


# ── Merged from test_prom.py ──

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


# ── Merged from test_streaming_e2e_fixture.py ──

import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripts import run_contract_backend
from scripts.lib.streaming_e2e_fixture import install_streaming_fixture


def test_streaming_fixture_requires_an_isolated_contract_sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTRACT_STREAMING_FIXTURES", "1")
    monkeypatch.delenv("CONTRACT_CONFIGS_DIR", raising=False)
    monkeypatch.delenv("CONTRACT_DATA_DIR", raising=False)
    monkeypatch.setattr(sys, "argv", ["run_contract_backend.py"])
    with pytest.raises(ValueError, match="isolated contract sandbox"):
        run_contract_backend.main()


def test_streaming_fixture_is_cookie_scoped_and_records_ssr_requests() -> None:
    app = FastAPI()

    @app.post("/api/cmcd/aggregates")
    def aggregates() -> dict:
        return {"available": False}

    install_streaming_fixture(app)
    with TestClient(app) as browser, TestClient(app) as other_browser:
        payload = {"available": True, "overview": {"active_sessions": 1234}}
        assert browser.post("/api/__e2e__/streaming-fixture", json=payload).status_code == 200
        body = {"range_token": "24h", "bucket_seconds": 300, "top_n": 30}
        with TestClient(app) as ssr:
            ssr.cookies.update(browser.cookies)
            assert ssr.post("/api/cmcd/aggregates", json=body).json() == payload
        assert browser.get("/api/__e2e__/streaming-fixture").json() == {"bodies": [body]}
        assert other_browser.post("/api/cmcd/aggregates", json=body).json() == {"available": False}
        assert browser.delete("/api/__e2e__/streaming-fixture").status_code == 200
        assert browser.get("/api/__e2e__/streaming-fixture").status_code == 404
        assert browser.post("/api/cmcd/aggregates", json=body).json() == {"available": False}


def test_unavailable_fixture_does_not_reuse_another_browsers_populated_response() -> None:
    app = FastAPI()
    install_streaming_fixture(app)
    with TestClient(app) as populated, TestClient(app) as unavailable:
        populated.post("/api/__e2e__/streaming-fixture", json={"available": True})
        unavailable.post("/api/__e2e__/streaming-fixture", json={"available": False, "has_data": False})
        assert populated.post("/api/cmcd/aggregates", json={}).json() == {"available": True}
        assert unavailable.post("/api/cmcd/aggregates", json={}).json() == {"available": False, "has_data": False}


# ── Merged from test_profile_insights.py ──

from pathlib import Path

from scripts.dev.profile_insights import format_markdown_report, main, run_profile


def test_run_profile_collects_phases_coalesced_and_insights():
    res = run_profile(total_rows=1000, runs=2, num_files=3, window_hours=1.0, baseline_hours=24.0)

    assert res["runs"] == 2
    assert res["dataset"]["total_rows"] == 1000
    assert res["cold_total"]["wall_p50_ms"] > 0
    assert res["warm_total"]["wall_p50_ms"] >= 0
    assert len(res["top_10_by_wall_p50"]) == 10
    assert len(res["top_10_by_cpu_p50"]) == 10

    op_names = {op["operation"] for op in res["all_operations"]}
    assert "phase:scratch_table_build" in op_names
    assert "coalesced:city" in op_names
    assert "coalesced:url" in op_names
    assert "coalesced:ip" in op_names
    assert "coalesced:waf" in op_names
    assert any(name.startswith("insight:") for name in op_names)

    md = format_markdown_report(res)
    assert "# Insights Performance Profile" in md
    assert "Top 10 Costs by Wall-Clock p50" in md


def test_profile_insights_cli_writes_markdown_and_json(tmp_path: Path):
    md_out = tmp_path / "profile.md"
    json_out = tmp_path / "profile.json"
    rc = main(
        [
            "--rows",
            "500",
            "--runs",
            "1",
            "--files",
            "2",
            "--output",
            str(md_out),
            "--json-output",
            str(json_out),
        ]
    )
    assert rc == 0
    assert md_out.exists()
    assert json_out.exists()
    payload = json.loads(json_out.read_text())
    assert payload["dataset"]["total_rows"] == 500


# ── Merged from test_check_osv.py ──

import importlib.util

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_osv.py"


@pytest.fixture(scope="module")
def osv():
    spec = importlib.util.spec_from_file_location("check_osv", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_osv"] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "vector,expected",
    [
        # Log4Shell (CVE-2021-44228) — the canonical 10.0 CRITICAL.
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H", 10.0),
        # A textbook HIGH (7.5): network, no priv, availability impact only.
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:H", 7.5),
        # A MEDIUM (6.1): reflected XSS shape, scope changed.
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:R/S:C/C:L/I:L/A:N", 6.1),
        # A LOW (3.1).
        ("CVSS:3.1/AV:N/AC:H/PR:L/UI:N/S:U/C:L/I:N/A:N", 3.1),
    ],
)
def test_cvss3_base_score_known_vectors(osv, vector, expected):
    assert osv.cvss3_base_score(vector) == pytest.approx(expected, abs=0.05)


def test_cvss3_base_score_rejects_v4_vector(osv):
    # CVSS_V4 uses a different formula we do not implement → None.
    assert osv.cvss3_base_score("CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N") is None


def test_severity_from_cvss_array_when_database_specific_absent(osv):
    """The fail-open regression: CRITICAL severity carried ONLY in the CVSS
    array (no database_specific) must still resolve to CRITICAL."""
    vuln = {
        "id": "CVE-2021-44228",
        "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"}],
    }
    assert osv.severity_for_vuln(vuln) == "CRITICAL"


def test_severity_takes_max_across_sources(osv):
    """database_specific says LOW but the CVSS vector says CRITICAL → CRITICAL."""
    vuln = {
        "id": "X",
        "database_specific": {"severity": "LOW"},
        "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"}],
    }
    assert osv.severity_for_vuln(vuln) == "CRITICAL"


def test_severity_database_specific_still_honored(osv):
    vuln = {"id": "X", "database_specific": {"severity": "critical"}}
    assert osv.severity_for_vuln(vuln) == "CRITICAL"


def test_severity_unknown_when_no_signal(osv):
    assert osv.severity_for_vuln({"id": "X"}) == "UNKNOWN"


def test_unparsed_v4_only_vector_is_unknown_not_crash(osv):
    vuln = {
        "id": "CVE-2099-0001",
        "severity": [{"type": "CVSS_V4", "score": "CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N"}],
    }
    # No qualitative source and an unparseable vector → UNKNOWN (surfaced via stderr).
    assert osv.severity_for_vuln(vuln) == "UNKNOWN"
