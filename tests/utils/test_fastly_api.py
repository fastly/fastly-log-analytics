"""Consolidated tests for test_fastly_api."""

from __future__ import annotations

import json
import urllib.parse
from unittest.mock import MagicMock, patch

import pytest

from backend import config as svcconfig
from backend.provision import fastly_api
from backend.provision import session_scoring_orchestrator as sso
from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES, cmcd_snippet_names
from backend.provision.declarative.reconciler import ReconciliationResult
from backend.provision.fastly_api import (
    _log_sampling_edge_clause,
    generate_capture_vcl,
    get_capture_vcl_statements,
    get_scrub_vcl_statements,
    load_log_format,
)
from backend.provision.session_scoring_orchestrator import scoring_snippet_names

# ── load_log_format: defaults + override ───────────────────────────────────


@pytest.fixture()
def _isolate_configs_dir(tmp_path, monkeypatch):
    """Redirect svcconfig.CONFIGS_DIR to tmp_path so tests never write into repo configs/."""
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", cfg_dir)


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_load_log_format_uses_standard_preset_and_respects_groups():
    """No config supplied uses standard preset; custom groups produce a distinct log format."""
    out = fastly_api.load_log_format(None)
    assert isinstance(out, str) and "%{" in out and len(out) > 50
    out_min = fastly_api.load_log_format({"groups": ["A"], "field_overrides": {}})
    assert out_min != out


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_load_log_format_keeps_standard_fields_when_only_custom_fields_present():
    """A log_fields carrying custom_fields but no groups/preset must still emit standard fields."""
    fmt = load_log_format(
        {
            "custom_fields": [
                {
                    "name": "edge_score",
                    "enabled": True,
                    "collection_stage": "deliver",
                    "vcl_log_expression": "req.http.x-edge-score:score",
                }
            ]
        }
    )
    for token in ("req.url", "req.method", "visits_this_service", "edge_score"):
        assert token in fmt
    assert "req.url" in load_log_format({})
    assert "req.url" in load_log_format(None)


# ── validate_log_format: length + regex paths ──────────────────────────────


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_validate_log_format_standard_preset_and_falco_fallback():
    """Default preset is valid with or without falco on PATH."""
    errors = fastly_api.validate_log_format(None)
    assert isinstance(errors, list) and (errors == [] or all("warning" not in e.lower() for e in errors))
    with patch("backend.provision.fastly_api.shutil.which", return_value=None):
        fallback_errors = fastly_api.validate_log_format(None)
    assert isinstance(fallback_errors, list)


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_validate_log_format_flags_oversize_format_with_attribution():
    """Log format > 16000 chars → LOG_FORMAT_TOO_LONG with built-in vs custom breakdown."""
    cfg = {
        "groups": ["A"],
        "custom_fields": [{"name": f"f{i}", "enabled": True, "vcl_log_expression": "X" * 100} for i in range(50)],
    }
    with patch("backend.provision.fastly_api.load_log_format", return_value="X" * 17000):
        errors = fastly_api.validate_log_format(cfg)

    assert len(errors) == 1 and errors[0].startswith("LOG_FORMAT_TOO_LONG")
    assert "Built-in fields" in errors[0] and "Custom fields" in errors[0]


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_validate_log_format_swallows_generation_failure():
    """If log_fields.generate_log_format raises, return a single error rather than crashing."""
    with patch("backend.provision.fastly_api.lf.generate_log_format", side_effect=ValueError("bad config")):
        errors = fastly_api.validate_log_format({"groups": ["A"]})
    assert len(errors) == 1 and "Could not generate log format" in errors[0] and "bad config" in errors[0]


# ── _validate_log_format_regex: pure VCL syntax checks ─────────────────────


@pytest.mark.usefixtures("_isolate_configs_dir")
@pytest.mark.parametrize(
    ("raw", "expected_substrings"),
    [
        ('%{client.ip}V %{req.url}V %{if(req.method == "POST", "y", "n")}V', []),
        ('%{if(> 100, "big", "small")}V', ["missing left-hand side"]),
        ('%{if(req.url == "/foo", "a", "b"}V', ["unclosed if() parenthesis"]),
        ('%{client.ip}V %{if(> 100, "x", "y")}V %{req.url}V', ["missing left-hand side"]),
        ("plain text with if(broken: %{client.ip}V", []),
        ('%{if(regsub(req.url, "/foo/", ""), "yes", "no")}V', []),
    ],
    ids=[
        "clean_input",
        "bare_comparison_in_if",
        "unclosed_if_parenthesis",
        "multiple_vcl_slots_isolated",
        "ignores_text_outside_vcl_slots",
        "nested_parens_closed",
    ],
)
def test_validate_log_format_regex_cases(raw: str, expected_substrings: list[str]):
    """Validate pure VCL regex checks across clean, broken, multi-slot, and nested inputs."""
    errors = fastly_api._validate_log_format_regex(raw)
    if not expected_substrings:
        assert errors == []
    else:
        assert len(errors) == len(expected_substrings)
        for sub in expected_substrings:
            assert any(sub in e for e in errors)


@pytest.mark.usefixtures("_isolate_configs_dir")
@pytest.mark.parametrize("op", [">", "<", ">=", "<=", "!=", "=="])
def test_validate_log_format_regex_catches_bare_comparison_for_each_operator(op):
    """All 6 comparison operators trigger the missing left-hand side check."""
    errors = fastly_api._validate_log_format_regex(f'%{{if({op} 5, "a", "b")}}V')
    assert any("missing left-hand side" in e for e in errors)


# ── generate_capture_vcl: snippet keys & invariants ────────────────────────


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_generate_capture_vcl_subroutine_keys_and_group_l():
    """Base subroutines always emitted; Group L adds origin subroutines and instrumentation."""
    base = fastly_api.generate_capture_vcl({"groups": ["A"], "field_overrides": {}})
    assert {"recv", "miss", "pass"} <= base.keys()
    assert not ({"fetch", "error", "deliver"} & base.keys())
    assert "recv" in fastly_api.generate_capture_vcl(None)

    with_l = fastly_api.generate_capture_vcl({"groups": ["A", "L"], "field_overrides": {}})
    assert {"fetch", "error", "deliver"} <= with_l.keys()
    assert "x-req-id" in with_l["recv"] and "randomstr(8)" in with_l["recv"]
    assert "x-of-start" in with_l["miss"] and "time.elapsed.usec" in with_l["miss"]
    assert "x-of-ttfb" in with_l["fetch"] and "x-of-ttlb" in with_l["deliver"]


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_capture_vcl_uses_fastly_ff_edge_detect_and_never_references_shield_auth():
    """Edge scrub/capture guards key on fastly.ff.visits_this_service == 0; X-Edge-Shield-Auth is retired."""
    snippets = fastly_api.generate_capture_vcl({"groups": ["A"]})
    assert "if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {" in snippets["recv"]
    for kind in ("recv", "miss", "pass"):
        assert "X-Edge-Shield-Auth" not in snippets[kind], f"{kind}: retired shield-auth header reappeared"


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_edge_data_mapping_and_safe_max_invariants():
    """EDGE_DATA_MAPPING covers documented subfields with non-empty VCL expressions; safe max is within bounds."""
    m = fastly_api.EDGE_DATA_MAPPING
    for key in ("country", "city", "lat", "lon", "asn", "rtt", "ja3", "ja4"):
        assert key in m
    for name, expr in m.items():
        assert isinstance(expr, str) and expr.strip(), f"empty VCL expr for {name!r}"
    assert 4000 <= fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX <= 16384


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_generate_capture_vcl_custom_edge_and_origin_fields():
    """Custom edge fields land in recv; custom origin fields emit fetch snippet even without Group L."""
    edge_out = fastly_api.generate_capture_vcl(
        {
            "groups": ["A"],
            "field_overrides": {},
            "custom_fields": [
                {
                    "name": "my_custom",
                    "enabled": True,
                    "collection_stage": "edge",
                    "duckdb_type": "VARCHAR",
                    "value_type": "string",
                    "vcl_log_expression": "req.url",
                    "bytes_estimate": 20,
                }
            ],
        }
    )
    assert "my_custom" in edge_out["recv"] and "req.url" in edge_out["recv"]

    origin_out = fastly_api.generate_capture_vcl(
        {
            "groups": ["A"],
            "field_overrides": {},
            "custom_fields": [
                {
                    "name": "my_origin_field",
                    "enabled": True,
                    "collection_stage": "origin",
                    "duckdb_type": "VARCHAR",
                    "value_type": "string",
                    "vcl_log_expression": "beresp.status",
                    "bytes_estimate": 20,
                }
            ],
        }
    )
    assert (
        "fetch" in origin_out
        and "my_origin_field" in origin_out["fetch"]
        and "x-fos-origin-data" in origin_out["fetch"]
    )


# ── miss_pass deliver-gate (falco-free string coverage, py SEAM-01) ─────────


def _miss_pass_cfg(frequency: str) -> dict:
    return {
        "groups": ["A"],
        "field_overrides": {},
        "custom_fields": [
            {
                "name": "origin_cache_state",
                "enabled": True,
                "collection_stage": "origin",
                "duckdb_type": "VARCHAR",
                "value_type": "string",
                "vcl_log_expression": "beresp.http.X-Cache",
                "origin_log_frequency": frequency,
                "bytes_estimate": 20,
            }
        ],
    }


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_generate_capture_vcl_miss_pass_vs_all_frequency_deliver_gate():
    """miss_pass fields promote only inside `fastly_info.state !~ "HIT"`, while `all` promotes unconditionally."""
    deliver = fastly_api.generate_capture_vcl(_miss_pass_cfg("miss_pass"))["deliver"]
    lines = deliver.splitlines()
    guard = '  if (fastly_info.state !~ "HIT") {'
    assert guard in lines, f"miss_pass deliver-gate missing or changed.\nDELIVER:\n{deliver}"
    gi = lines.index(guard)
    promote = "    set req.http.x-fos-origin-data:origin_cache_state = resp.http.x-fos-origin-data:origin_cache_state;"
    assert lines[gi + 1] == promote and lines[gi + 2].strip() == "}"
    unconditional = (
        "  set req.http.x-fos-origin-data:origin_cache_state = resp.http.x-fos-origin-data:origin_cache_state;"
    )
    assert unconditional not in lines

    deliver_all = fastly_api.generate_capture_vcl(_miss_pass_cfg("all"))["deliver"]
    assert "fastly_info.state" not in deliver_all
    assert unconditional in deliver_all.splitlines()


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_pure_statement_helpers_and_sampling_and_unified_edge_block():
    cfg = {"groups": ["A"]}
    scrubs = get_scrub_vcl_statements(cfg)
    captures = get_capture_vcl_statements(cfg)
    assert any("unset req.http.x-is-cluster-fetch" in s for s in scrubs)
    assert any("set req.http.x-fos-edge-data:ip" in s for s in captures)
    assert not any("if (" in s for s in scrubs)
    assert not any("if (" in s for s in captures if "Fastly-Client-IP" not in s)

    for scoring in (False, True):
        clause = _log_sampling_edge_clause(scoring)
        assert "req.restarts" not in clause and clause == "fastly.ff.visits_this_service == 0"

    cf_cfg = {
        "groups": [],
        "custom_fields": [
            {"name": "cf1", "enabled": True, "collection_stage": "edge", "vcl_log_expression": "req.http.host"}
        ],
    }
    for scoring in (True, False):
        recv = generate_capture_vcl(cf_cfg, scoring_enabled=scoring)["recv"]
        assert "if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {" in recv
        assert "set req.http.x-fos-edge-data:cf1 = req.http.host;" in recv
        assert "set req.http.x-fos-edge-data:ip = req.http.Fastly-Client-IP;" in recv
        assert "set req.http.x-fos-edge-data:ip = client.ip;" in recv


# ── Merged from test_fastly_api_orchestrators.py ─────────────────────────────


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_delete_cdn_service_deactivates_active_version_before_deleting():
    """Must deactivate the active version FIRST and emit status callback events."""
    calls, statuses = [], []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path))
        if method == "GET" and "/version" in path:
            return [{"number": 1, "active": False}, {"number": 2, "active": True}]
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        fastly_api.delete_cdn_service("svc-id", "MyService", "tok", status_cb=statuses.append)

    assert ("GET", "/service/svc-id/version") in calls
    assert ("PUT", "/service/svc-id/version/2/deactivate") in calls
    assert ("DELETE", "/service/svc-id") in calls
    assert any("Deleting" in s for s in statuses) and any("deleted" in s.lower() for s in statuses)


@pytest.mark.usefixtures("_isolate_configs_dir")
@pytest.mark.parametrize(
    ("list_exc", "delete_exc", "expected_calls", "expect_raise"),
    [
        (RuntimeError("HTTP 404: service not found"), None, 1, None),
        (None, RuntimeError("HTTP 404: not found"), 2, None),
        (None, RuntimeError("HTTP 502: bad gateway"), 2, "502"),
    ],
    ids=["404_on_list", "404_on_delete", "502_on_delete_reraises"],
)
def test_delete_cdn_service_error_handling(list_exc, delete_exc, expected_calls, expect_raise):
    """404 on version-list or DELETE is a no-op success; non-404 on DELETE propagates."""
    call_count = [0]

    def fake_fastly(method, path, **kwargs):
        call_count[0] += 1
        if method == "GET" and "/version" in path:
            if list_exc:
                raise list_exc
            return [{"number": 1, "active": False}]
        if method == "DELETE" and delete_exc:
            raise delete_exc
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        if expect_raise:
            with pytest.raises(RuntimeError, match=expect_raise):
                fastly_api.delete_cdn_service("svc-id", "MyService", "tok")
        else:
            fastly_api.delete_cdn_service("svc-id", "MyService", "tok")

    assert call_count[0] == expected_calls


# ── remove_logging_endpoint ───────────────────────────────────────────────


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_remove_logging_endpoint_no_op_when_no_active_version():
    """No active version → warn and return (no API mutations)."""
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=None),
        patch("backend.provision.fastly_api.fastly") as mock_fastly,
    ):
        fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")
    mock_fastly.assert_not_called()


def _run_remove_endpoint_scenario(
    endpoints: list[str],
    *,
    snippets: list[dict] | None = None,
    snippet_delete_404: bool = False,
    validate_ok: bool = True,
) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path))
        if "/clone" in path:
            return {"number": 6}
        if "/validate" in path:
            return {"status": "ok"} if validate_ok else {"status": "error", "errors": ["bad vcl"]}
        if method == "GET" and path.endswith("/snippet") and snippets is not None:
            return snippets
        if snippet_delete_404 and method == "DELETE" and "snippet/" in path:
            raise RuntimeError("HTTP 404: snippet not found")
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=endpoints),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")
    return calls


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_remove_logging_endpoint_absent_vs_present_and_404_tolerance():
    """Absent endpoint skips endpoint DELETE; present endpoint deletes endpoint+snippets; 404 on snippet is swallowed."""
    absent_calls = _run_remove_endpoint_scenario(["OtherEndpoint"])
    assert any("clone" in p for _, p in absent_calls)
    assert not any("logging/s3/MyEndpoint" in p for m, p in absent_calls if m == "DELETE")
    assert any("validate" in p for _, p in absent_calls)

    present_calls = _run_remove_endpoint_scenario(
        ["MyEndpoint"],
        snippets=[{"name": "Fastly Log Analytics - vcl_recv"}, {"name": "Fastly Log Analytics - vcl_deliver"}],
    )
    assert any("logging/s3/MyEndpoint" in p for m, p in present_calls if m == "DELETE")
    assert any("snippet/" in p for m, p in present_calls if m == "DELETE")
    assert any("activate" in p for m, p in present_calls if m == "PUT")

    _run_remove_endpoint_scenario(["MyEndpoint"], snippet_delete_404=True)


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_remove_logging_endpoint_rolls_back_on_validation_failure():
    """If validation of the draft fails, the old active version (5) is re-activated."""
    activate_calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        if "/clone" in path:
            return {"number": 6}
        if "/validate" in path:
            return {"status": "error", "errors": ["bad vcl"]}
        if method == "PUT" and path.endswith("/activate"):
            activate_calls.append(path)
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["MyEndpoint"]),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")

    assert any("/version/5/activate" in p for p in activate_calls)


# ── ensure_cdn_service & redeploy_cdn_vcl ─────────────────────────────────


def _reconciler_writing_cdn_id(cdn_id: str, *, then_raise: Exception | None = None, version: int = 1) -> MagicMock:
    """Stand-in for ``reconcile_cdn_service_state`` that persists the CDN id the way the real one does."""

    def _reconcile(logging_service_id, token, status_cb=None, activate=True):
        svcconfig.mutate_config(logging_service_id, lambda c: c.setdefault("fos_proxy", {}).update(service_id=cdn_id))
        if then_raise is not None:
            raise then_raise
        return ReconciliationResult(service_id=cdn_id, activated_version=version)

    return MagicMock(side_effect=_reconcile)


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_ensure_cdn_service_missing_logging_service_id():
    with pytest.raises(ValueError, match="logging_service_id or service_id must be provided"):
        fastly_api.ensure_cdn_service({}, "AK", "SK", "tok")


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_redeploy_cdn_vcl_success_and_error_paths():
    """Verify redeploy_cdn_vcl maps cdn_service_id, persists rate_limiting, and handles missing/failing reconciles."""
    mock_config = {"service_id": "service-id", "cdn_service_id": "cdn-id", "fos_proxy": {}}
    svcconfig.save_config("service-id", dict(mock_config))

    with (
        patch("backend.config.list_configs", return_value=[mock_config]),
        patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state") as mock_reconcile,
    ):
        mock_reconcile.return_value = ReconciliationResult(
            service_id="cdn-id", activated_version=12, changes_applied=None
        )
        assert fastly_api.redeploy_cdn_vcl("cdn-id", "tok", rate_limiting=False) == 12
        assert svcconfig.load_config("service-id")["fos_proxy"]["rate_limiting_enabled"] is False
        mock_reconcile.assert_called_with(logging_service_id="service-id", token="tok", status_cb=None, activate=True)

    with patch("backend.config.list_configs", return_value=[]):
        with pytest.raises(ValueError, match="Could not map cdn_service_id"):
            fastly_api.redeploy_cdn_vcl("cdn-id", "tok")

    with (
        patch("backend.config.list_configs", return_value=[mock_config]),
        patch(
            "backend.provision.declarative.reconciler.reconcile_cdn_service_state",
            side_effect=RuntimeError("reconciler failure"),
        ),
    ):
        with pytest.raises(RuntimeError, match="reconciler failure"):
            fastly_api.redeploy_cdn_vcl("cdn-id", "tok")


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_ensure_cdn_service_success():
    """Verify ensure_cdn_service merges configs, writes them to disk, calls reconciler, and detects rate limiting."""
    input_cfg = {
        "logging_service_id": "log-svc-123",
        "cdn_service_name": "MyCDN",
        "cdn_url": "https://mycdn.example.com",
    }
    statuses = []
    mock_reconcile = _reconciler_writing_cdn_id("cdn-new-123")

    with (
        patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state", mock_reconcile),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=True) as mock_detect,
    ):
        result = fastly_api.ensure_cdn_service(input_cfg, "AK", "SK", "tok", status_cb=statuses.append)
        assert result == {"id": "cdn-new-123", "name": "MyCDN", "rate_limiting": True}
        written_json = svcconfig.load_config("log-svc-123")
        assert written_json["fos_access_key_id"] == "AK" and written_json["fos_secret_access_key"] == "SK"
        assert written_json["cdn_service_name"] == "MyCDN"
        mock_reconcile.assert_called_with(
            logging_service_id="log-svc-123", token="tok", status_cb=statuses.append, activate=True
        )
        mock_detect.assert_called_with("tok", "log-svc-123")


@pytest.mark.usefixtures("_isolate_configs_dir")
@pytest.mark.parametrize("detection_val", [True, False, None])
def test_ensure_cdn_service_rate_limiting_scenarios(detection_val):
    with (
        patch(
            "backend.provision.declarative.reconciler.reconcile_cdn_service_state",
            _reconciler_writing_cdn_id("cdn-new-123"),
        ),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=detection_val),
    ):
        result = fastly_api.ensure_cdn_service({"logging_service_id": "log-svc-123"}, "AK", "SK", "tok")
        assert result["rate_limiting"] is detection_val


@pytest.mark.usefixtures("_isolate_configs_dir")
@pytest.mark.parametrize("then_raise", [None, RuntimeError("Validation failed")], ids=["success", "after_failure"])
def test_ensure_cdn_service_on_created_callback(then_raise):
    created_ids = []
    with (
        patch(
            "backend.provision.declarative.reconciler.reconcile_cdn_service_state",
            _reconciler_writing_cdn_id("cdn-new-123", then_raise=then_raise),
        ),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
    ):
        if then_raise:
            with pytest.raises(RuntimeError, match="Validation failed"):
                fastly_api.ensure_cdn_service(
                    {"logging_service_id": "svc"}, "ak", "sk", "tok", on_created=created_ids.append
                )
        else:
            fastly_api.ensure_cdn_service(
                {"logging_service_id": "log-svc-123"}, "AK", "SK", "tok", on_created=created_ids.append
            )
    assert created_ids == ["cdn-new-123"]


# ── update_logging_endpoint (SSE generator) ──────────────────────────────


def _drain(gen):
    """Consume an SSE generator into (events, exception_or_None)."""
    events = []
    try:
        for e in gen:
            events.append(e)
    except Exception as exc:
        return events, exc
    return events, None


def _run_reconciled_endpoint_update(
    cfg_in: dict,
    reconcile_result: ReconciliationResult,
    *,
    active_version: int | None = 10,
    stored_cfg: dict | None = None,
):
    if stored_cfg is None:
        stored_cfg = {"logging_enabled": True, "log_fields": {}}
    with (
        patch("backend.config.load_config", return_value=stored_cfg),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.client.get_active_version", return_value=active_version),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state", return_value=reconcile_result),
    ):
        return _drain(fastly_api.update_logging_endpoint(cfg_in, "tok"))


@pytest.mark.usefixtures("_isolate_configs_dir")
@pytest.mark.parametrize(
    ("cfg_in", "reconcile_result", "expected_changed", "expected_version"),
    [
        (
            {
                "logging_service_id": "svc-id",
                "endpoint_name": "MyEndpoint",
                "sample_rate": 100,
                "edge_only": False,
                "log_period": 60,
                "fos_path": "/raw/%Y-%m-%d/%H/",
                "update_format": False,
            },
            ReconciliationResult(service_id="svc-id", activated_version=10, changes_applied=None),
            False,
            10,
        ),
        (
            {"logging_service_id": "svc", "endpoint_name": "MyEndpoint", "log_period": 60},
            ReconciliationResult(
                service_id="svc", activated_version=11, changes_applied={"endpoint_update": {"log_period": 60}}
            ),
            True,
            11,
        ),
        (
            {"logging_service_id": "svc", "endpoint_name": "MyEndpoint", "update_format": True},
            ReconciliationResult(
                service_id="svc",
                activated_version=11,
                changes_applied={
                    "snippets_deleted": [
                        "Fastly Log Analytics Origin Fetch",
                        "Fastly Log Analytics Origin Error",
                        "Fastly Log Analytics Origin Deliver",
                    ]
                },
            ),
            True,
            11,
        ),
    ],
    ids=["no_changes_short_circuit", "changes_detected_activates", "origin_snippets_deleted"],
)
def test_update_logging_endpoint_reconcile_outcomes(cfg_in, reconcile_result, expected_changed, expected_version):
    events, exc = _run_reconciled_endpoint_update(cfg_in, reconcile_result)
    assert exc is None
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None and done["changed"] is expected_changed and done["version"] == expected_version


@pytest.mark.usefixtures("_isolate_configs_dir")
@pytest.mark.parametrize(
    ("cfg_in", "active_version", "error_msg", "expected_match"),
    [
        (
            {"logging_service_id": "svc-id", "endpoint_name": "GhostEndpoint", "log_period": 60},
            10,
            "Logging endpoint 'GhostEndpoint' not found on service",
            "Declarative reconciliation failed",
        ),
        (
            {"logging_service_id": "svc", "endpoint_name": "ep", "log_period": 60},
            None,
            "No active version found for service 'svc'",
            None,
        ),
        (
            {"logging_service_id": "svc", "endpoint_name": "MyEndpoint", "log_period": 60},
            10,
            "Validation exploded mid-update",
            None,
        ),
    ],
    ids=["404_endpoint_lookup", "no_active_version", "validation_explosion_rollback"],
)
def test_update_logging_endpoint_reconcile_errors(cfg_in, active_version, error_msg, expected_match):
    result = ReconciliationResult(service_id=cfg_in["logging_service_id"], error=error_msg)
    _events, exc = _run_reconciled_endpoint_update(cfg_in, result, active_version=active_version)
    assert isinstance(exc, RuntimeError)
    if expected_match:
        assert expected_match in str(exc)


def _run_update_logging_endpoint(cfg_in, stored_cfg):
    svcconfig.save_config("svc-id", dict(stored_cfg))
    with (
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.client.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id="svc-id", activated_version=11, changes_applied=["fmt"]),
        ),
    ):
        events, exc = _drain(fastly_api.update_logging_endpoint(cfg_in, "tok"))
    assert exc is None, f"generator raised: {exc}"
    return json.loads(svcconfig.config_path("svc-id").read_text()), events


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_update_logging_endpoint_cmcd_and_custom_fields_convergence():
    """Preserve custom_fields on group-only updates, re-assert CMCD when enabled, and strip CMCD when disabled."""
    stored_enabled = {
        "logging_enabled": True,
        "cmcd": {"enabled": True, "mode": "query_string", "version": 1},
        "log_fields": {
            "schema_version": 2,
            "groups": ["A", "B"],
            "custom_fields": [
                {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True},
                {"name": "cmcd_sid", "duckdb_type": "VARCHAR", "enabled": True},
            ],
        },
    }
    saved_cfg, _ = _run_update_logging_endpoint(
        {
            "logging_service_id": "svc-id",
            "endpoint_name": "MyEndpoint",
            "log_period": 60,
            "log_fields": {"schema_version": 2, "preset": "standard", "groups": ["A", "B", "C"]},
        },
        stored_enabled,
    )
    saved_names = {cf["name"] for cf in saved_cfg["log_fields"]["custom_fields"]}
    assert "my_custom" in saved_names and set(_CMCD_FIELD_NAMES) <= saved_names
    assert saved_cfg["log_fields"]["groups"] == ["A", "B", "C"]

    stored_stripped = {
        "logging_enabled": True,
        "cmcd": {"enabled": True, "mode": "query_string", "version": 1},
        "log_fields": {"schema_version": 2, "groups": ["A", "B"], "field_overrides": {}},
    }
    reasserted_cfg, _ = _run_update_logging_endpoint(
        {"logging_service_id": "svc-id", "endpoint_name": "MyEndpoint", "log_period": 60},
        stored_stripped,
    )
    assert set(_CMCD_FIELD_NAMES) <= {cf["name"] for cf in reasserted_cfg["log_fields"]["custom_fields"]}

    stored_disabled = {
        "logging_enabled": True,
        "log_fields": dict(stored_enabled["log_fields"]),
    }
    disabled_cfg, _ = _run_update_logging_endpoint(
        {"logging_service_id": "svc-id", "endpoint_name": "MyEndpoint", "log_period": 60},
        stored_disabled,
    )
    assert {cf["name"] for cf in disabled_cfg["log_fields"]["custom_fields"]} == {"my_custom"}


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_update_logging_endpoint_rate_limiting_flag_refresh_and_skip():
    """Refreshes rate_limiting flag when service config exists; skips probe when load_config returns None."""
    svcconfig.save_config("svc", {"provisioning": {"rate_limiting": True}, "log_fields": {"groups": ["A"]}})
    with (
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=False),
        patch("backend.provision.fastly_api.get_active_version", return_value=None),
    ):
        events, exc = _drain(
            fastly_api.update_logging_endpoint(
                {"logging_service_id": "svc", "endpoint_name": "ep", "log_period": 60}, "tok"
            )
        )
    assert json.loads(svcconfig.config_path("svc").read_text())["provisioning"]["rate_limiting"] is False
    assert any(e.get("type") == "status" and "rate limiting" in e.get("message", "").lower() for e in events)
    assert isinstance(exc, RuntimeError)

    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.config.save_config", side_effect=AssertionError("must not save")),
        patch("backend.provision.fastly_api.account_has_rate_limiting", side_effect=AssertionError("must not probe")),
        patch("backend.provision.fastly_api.get_active_version", return_value=None),
    ):
        _events, exc2 = _drain(
            fastly_api.update_logging_endpoint(
                {"logging_service_id": "svc", "endpoint_name": "ep", "log_period": 60}, "tok"
            )
        )
    assert isinstance(exc2, RuntimeError)


# ── account_has_rate_limiting & service_has_scoring_pragmas ──────────────


def _vcl_with(pragma_line: str) -> str:
    return f"sub vcl_recv {{\n  {pragma_line}\n}}\n"


@pytest.mark.usefixtures("_isolate_configs_dir")
@pytest.mark.parametrize(
    ("active_ver", "gen_vcl", "expected"),
    [
        (7, _vcl_with("pragma optional_param ratelimit_opt_in true;"), True),
        (7, _vcl_with("pragma optional_param ratelimit_opt_in false;"), False),
        (7, "sub vcl_recv {}\n", False),
        (None, None, None),
        (25, None, None),
        (ValueError("boom"), None, None),
    ],
    ids=[
        "pragma_true",
        "pragma_false",
        "pragma_absent",
        "no_active_version",
        "no_generated_vcl",
        "unexpected_probe_error_swallowed",
    ],
)
def test_account_has_rate_limiting_single_service_probes(active_ver, gen_vcl, expected):
    ver_kwargs = {"side_effect": active_ver} if isinstance(active_ver, Exception) else {"return_value": active_ver}
    with (
        patch("backend.provision.fastly_api.get_active_version", **ver_kwargs),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value=gen_vcl),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "svc-1") is expected


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_account_has_rate_limiting_multi_service_and_falsy_ids():
    with patch("backend.provision.fastly_api.get_active_version") as m_ver:
        assert fastly_api.account_has_rate_limiting("tok") is None
        assert fastly_api.account_has_rate_limiting("tok", "", None) is None
        m_ver.assert_not_called()

    gens = {"wasm-svc": None, "vcl-svc": _vcl_with("pragma optional_param ratelimit_opt_in true;")}
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=3),
        patch("backend.provision.fastly_api.get_generated_vcl", side_effect=lambda sid, ver, tok: gens[sid]),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "wasm-svc", "vcl-svc") is True

    seen = []
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=3),
        patch(
            "backend.provision.fastly_api.get_generated_vcl",
            side_effect=lambda sid, ver, tok: (seen.append(sid), "sub vcl_recv {}\n")[1],
        ),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "first", "second") is False
    assert seen == ["first"]


@pytest.mark.usefixtures("_isolate_configs_dir")
@pytest.mark.parametrize(
    ("vcl", "expected"),
    [
        (
            "pragma optional_param fix_unsent_body_drain true;\n"
            "pragma optional_param no_body_if_bereq_is_get_or_head true;\n",
            True,
        ),
        ("pragma optional_param fix_unsent_body_drain true;\n", False),
        ("pragma optional_param no_body_if_bereq_is_get_or_head true;\n", False),
        (
            "pragma optional_param fix_unsent_body_drain true;\n"
            "pragma optional_param no_body_if_bereq_is_get_or_head false;\n",
            False,
        ),
        (RuntimeError("boom"), None),
    ],
    ids=["both_true", "only_drain", "only_no_body", "one_false", "runtime_error"],
)
def test_service_has_scoring_pragmas_requires_both_pragmas(vcl, expected):
    gen_kwargs = {"side_effect": vcl} if isinstance(vcl, Exception) else {"return_value": vcl}
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.get_generated_vcl", **gen_kwargs),
    ):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is expected

    assert fastly_api.service_has_scoring_pragmas("", "tok") is None
    assert fastly_api.service_has_scoring_pragmas("svc_1", "") is None
    with patch("backend.provision.fastly_api.get_active_version", return_value=None):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is None


# ── Merged from test_teardown_dereference_order.py ───────────────────────────

CUSTOMER_SNIPPETS = ["Session Tracking - Recv", "Session Tracking - Deliver", "force_ssl", "Cache Overrides"]
OUR_SNIPPETS = [
    "Fastly Log Analytics - vcl_recv",
    "Fastly Log Analytics - vcl_miss",
    "Fastly Log Analytics - vcl_pass",
    "Fastly Log Analytics - vcl_fetch",
    "Fastly Log Analytics - vcl_deliver",
    "Fastly Log Analytics - vcl_error",
]


def _run_remove(snippet_names, deleted_sink):
    """Drive remove_logging_endpoint against a faked Fastly."""

    def fake_fastly(method, path, body=None, **kwargs):
        if method == "GET" and path.endswith("/snippet"):
            return [{"name": n} for n in snippet_names]
        if method == "DELETE" and "/snippet/" in path:
            name = urllib.parse.unquote(path.rsplit("/snippet/", 1)[1])
            if name not in snippet_names:
                raise RuntimeError("HTTP 404 not found")
            deleted_sink.append(name)
            return {}
        if "/clone" in path:
            return {"number": 99}
        if "/validate" in path:
            return {"status": "ok"}
        if method == "GET" and path.endswith(("/backend", "/dictionary", "/condition")):
            return []
        return {}

    with (
        patch.object(fastly_api, "fastly", side_effect=fake_fastly),
        patch.object(fastly_api, "get_active_version", return_value=98),
        patch.object(fastly_api, "list_s3_endpoints", return_value=["Fastly Object Storage Logs"]),
    ):
        fastly_api.remove_logging_endpoint("svc-td", "Fastly Object Storage Logs", "tok")


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_all_capture_snippets_removed_and_customer_snippets_untouched():
    deleted: list[str] = []
    _run_remove(OUR_SNIPPETS + CUSTOMER_SNIPPETS, deleted)
    for name in OUR_SNIPPETS:
        assert name in deleted, f"{name} was left on the service"
    for name in CUSTOMER_SNIPPETS:
        assert name not in deleted, f"deleted customer-owned snippet {name!r}"


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_legacy_and_feature_snippets_removed_from_canonical_lists():
    legacy = ["Fastly Log Analytics Capture", "Fastly Log Analytics Origin Deliver"]
    feature = [*scoring_snippet_names(), *cmcd_snippet_names(), "RUM - Recv", "RUM - Set cookies"]
    for group in (legacy, feature):
        deleted: list[str] = []
        _run_remove(group + CUSTOMER_SNIPPETS, deleted)
        assert set(group) <= set(deleted)


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_unmatched_snippets_do_not_report_silent_success():
    deleted: list[str] = []
    with patch.object(fastly_api, "warn") as mock_warn:
        _run_remove(CUSTOMER_SNIPPETS, deleted)
    assert deleted == []
    assert mock_warn.called, "removing zero analytics snippets must warn"
    assert any("teardown left VCL behind" in str(c) or "still on draft" in str(c) for c in mock_warn.call_args_list)


@pytest.mark.usefixtures("_isolate_configs_dir")
def test_scoring_teardown_aborts_when_vcl_strip_fails_and_proceeds_when_gone():
    with (
        patch.object(sso, "get_active_version", side_effect=RuntimeError("HTTP 500 clone failed")),
        patch.object(sso, "delete_scoring_service") as mock_delete,
    ):
        with pytest.raises(RuntimeError, match="Aborting scoring teardown"):
            sso.teardown_scoring_resources("svc-td", {"scoring_service_id": "compute-1"}, "tok")
    mock_delete.assert_not_called()

    with (
        patch.object(sso, "get_active_version", return_value=None),
        patch.object(sso, "delete_scoring_service", return_value=[]) as mock_delete_ok,
    ):
        sso.teardown_scoring_resources("svc-td", {"scoring_service_id": "compute-1"}, "tok")
    mock_delete_ok.assert_called_once()


# ── Merged from tests/utils/test_fastly_utils.py ──

"""Consolidated tests for test_fastly_utils."""


import pytest

from backend.core.fastly.client import load_vcl, parse_period, region_endpoint


def test_load_vcl_orders_auth_before_native_request_handling():
    """Verify the authentication check (and its 401 Unauthorized block) is
    strictly defined before Fastly's native request handling — the
    ``#FASTLY recv`` macro, which is where the FASTLYPURGE method is
    processed. This ordering prevents an unauthenticated client from
    triggering a cache eviction.
    """
    vcl = load_vcl()
    assert vcl is not None

    # FASTLYPURGE is handled by Fastly's native boilerplate (#FASTLY recv),
    # NOT a manual ``return(purge)`` — which is not a valid return action in
    # vcl_recv and is rejected by both falco and the Fastly compiler.
    assert "return(purge)" not in vcl, "vcl_recv must not use the invalid return(purge) action"

    auth_err_pos = vcl.find('error 401 "Unauthorized"')
    fastly_recv_pos = vcl.find("#FASTLY recv")

    assert auth_err_pos != -1, "Should find the authentication check block"
    assert fastly_recv_pos != -1, "Should find the #FASTLY recv macro (native FASTLYPURGE handling)"

    assert auth_err_pos < fastly_recv_pos, (
        "Authentication check must strictly precede Fastly's native request "
        "handling to prevent unauthenticated cache evictions"
    )


@pytest.mark.security_regression
def test_load_vcl_authenticates_only_at_the_edge():
    """The client-facing auth gate (the ``cdn_auth.secret`` ``?key=`` /
    ``x-fastly-key`` check), the Fastly-Client-IP override, and the
    penaltybox enforcement must run only on the first Fastly POP to see
    the request — gated on ``req.restarts == 0 && fastly.ff.visits_this_service == 0``.

    Rationale: a request reaching the shield with ``visits_this_service > 0``
    has already passed this gate at the edge (unauthenticated requests
    ``error 401`` at the edge and are never forwarded), so the shield must
    NOT re-run it — the ``key`` param is stripped before forwarding and a
    re-check would 401 a legitimate request. A client cannot forge
    ``visits_this_service`` (each ``Fastly-FF`` entry is a salted hash only
    genuine Fastly hops produce), so this is a sound trust boundary and the
    old ``X-Edge-CDN-Auth`` shield marker is no longer needed.
    """
    vcl = load_vcl()

    # The marker mechanism must be fully gone.
    assert "X-Edge-CDN-Auth" not in vcl, "X-Edge-CDN-Auth shield marker should be removed"

    # The auth 401 must live inside the edge-only guard.
    recv_start = vcl.index("sub vcl_recv {")
    recv_body = vcl[recv_start : vcl.index("sub vcl_hash")]
    edge_guard = "if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {"
    guard_pos = recv_body.find(edge_guard)
    auth_401_pos = recv_body.find('error 401 "Unauthorized"')
    assert guard_pos != -1, "missing edge-only guard (req.restarts == 0 && visits_this_service == 0)"
    assert auth_401_pos != -1, "missing auth 401 block in vcl_recv"
    assert guard_pos < auth_401_pos, "auth gate must be inside the edge-only guard"
    # The auth gate must still check the client key from the cdn_auth dict.
    assert 'table.lookup(cdn_auth, "secret"' in recv_body, "auth gate must check cdn_auth.secret"


@pytest.mark.security_regression
def test_load_vcl_x_amz_date_rounds_seconds_to_minute_boundary():
    """Finding 006: a per-request ``x-amz-date`` makes every concurrent
    request to the same FOS object produce a unique SigV4 signature, which
    defeats Fastly's per-(URL, method) collapsed-forwarding / request
    coalescing — instead of one in-flight fetch serving an entire burst,
    every request forwards in parallel and the origin spend scales with
    incoming RPS instead of the cache-key cardinality.

    Pinning the seconds field to ``00`` in the strftime format string
    collapses all requests within the same UTC minute onto the SAME
    signature, restoring coalescing. Still well inside the SigV4 15-minute
    validity window. (VCL doesn't expose ``%`` or ``/`` operators on
    integers, so format-string rounding is the cleanest expression.)
    """
    vcl = load_vcl()
    amz_lines = [line for line in vcl.splitlines() if "x-amz-date" in line and "strftime" in line]
    assert amz_lines, "x-amz-date assignment line missing from VCL"
    assignment = amz_lines[0]
    # The fix replaces ``%S`` (per-second) with literal ``00`` so all
    # requests within a minute produce the same signed timestamp.
    assert "%H%M00Z" in assignment, (
        f"x-amz-date strftime format must pin seconds to 00 (finding 006); got: {assignment!r}"
    )
    # Negative: the pre-fix per-second ``%S`` form must NOT remain.
    assert 'strftime({"%Y%m%dT%H%M%SZ"}, now);' not in vcl, (
        "unrounded x-amz-date strftime still present — fix not applied"
    )


# ── region_endpoint: FOS host construction ─────────────────────────────────────


@pytest.mark.parametrize(
    "region,expected",
    [
        ("us-east-1", "us-east-1.object.fastlystorage.app"),
        ("eu-central-1", "eu-central-1.object.fastlystorage.app"),
    ],
)
def test_region_endpoint(region, expected):
    assert region_endpoint(region) == expected


# ── parse_period: input validation ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("60", 60),
        ("1 minute", 60),
        ("5 minutes", 300),
        ("5m", 300),
        ("5min", 300),
        ("  10  ", 10),
        ("2M", 120),
    ],
)
def test_parse_period_valid_inputs(raw, expected):
    assert parse_period(raw) == expected


@pytest.mark.parametrize("raw", ["", "abc", "12 hours", "5 sec", "minute"])
def test_parse_period_invalid_format_raises(raw):
    with pytest.raises(ValueError, match="Cannot parse period"):
        parse_period(raw)


# ── load_vcl: rate_limiting=False strips the rate-limit blocks ─────────────────


def test_load_vcl_with_rate_limiting_false_strips_ratelimit_blocks():
    """When the deploy disables rate limiting (e.g. a service without
    the ratelimit add-on), the #RATELIMIT_BEGIN/END markers and their
    enclosed VCL must be removed — otherwise the compiled VCL would
    reference undefined `ratecounter`/`penaltybox` symbols and fail to
    upload."""
    vcl_with = load_vcl(rate_limiting=True)
    vcl_without = load_vcl(rate_limiting=False)

    assert "#RATELIMIT_BEGIN" in vcl_with
    assert "#RATELIMIT_END" in vcl_with
    assert "ratecounter auth_fail_rc" in vcl_with
    assert "penaltybox auth_fail_pb" in vcl_with

    assert "#RATELIMIT_BEGIN" not in vcl_without
    assert "#RATELIMIT_END" not in vcl_without
    assert "ratecounter auth_fail_rc" not in vcl_without
    assert "penaltybox auth_fail_pb" not in vcl_without
    # The non-ratelimit auth check (the `error 401` outside the marker
    # block) must still be present — only the rate-limit add-ons were
    # stripped.
    assert 'error 401 "Unauthorized"' in vcl_without


# ── Merged from test_fastly_mock_mode.py ──────────────────────────────────────────────

from backend.core.fastly.client import fastly, is_mock_mode, mock_response
from backend.core.rollups.security_dims import fetch_verified_bots_paged


def test_is_mock_mode_reads_env(monkeypatch):
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    assert is_mock_mode() is False
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    assert is_mock_mode() is True
    monkeypatch.setenv("FASTLY_MOCK_MODE", "0")
    assert is_mock_mode() is False


def test_mock_response_post_service_returns_id_and_name():
    out = mock_response("POST", "/service", {"name": "Test CDN"})
    assert out["id"] == "mock-svc-id"
    assert out["name"] == "Test CDN"
    assert out["version"] == 1


def test_mock_response_get_service_returns_empty_list():
    # find_service_by_name pages this — empty means "name is novel".
    assert mock_response("GET", "/service") == []


def test_mock_response_unknown_endpoint_returns_ok_envelope():
    # Default catch-all — keeps an unfamiliar journey moving without
    # making up specific fields a future test could false-pin against.
    assert mock_response("GET", "/totally/uncovered/endpoint") == {"ok": True}


def test_mock_response_access_key_create_shape():
    # The mock returns the FLAT shape `ensure_fos_access_key` reads
    # (`key["access_key"]` / `key["secret_key"]`), not the JSON:API
    # `data.attributes.*` wrapper. Mirrors the orchestrator's
    # actual access pattern (backend/provision/fos_setup.py:368-376).
    out = mock_response("POST", "/resources/object-storage/access-keys", {"description": "x"})
    assert out["access_key"] == "AKIA_MOCK"
    assert out["secret_key"] == "SECRET_MOCK"  # gitleaks:allow — canned mock secret, not real
    assert out["description"] == "x"


def test_fastly_client_short_circuits_in_mock_mode(monkeypatch):
    """In mock mode the production fastly() must skip the real urlopen
    path entirely and return the fixture shape. Sanity: any token works."""
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    out = fastly("POST", "/service", {"name": "Smoke"}, token="any-token")
    assert out["id"] == "mock-svc-id"
    assert out["name"] == "Smoke"


def test_ngwaf_short_circuits_in_mock_mode(monkeypatch):
    """In mock mode fetch_verified_bots_paged yields one empty page and
    stops — no real /ngwaf/v1/workspaces/:id/requests call is made."""
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    pages = list(
        fetch_verified_bots_paged(
            api_key="any",
            workspace_id="ws-mock",
            from_ts="2026-06-01T00:00:00Z",
        )
    )
    assert pages == [([], None, 0)]


# ── Merged from test_refresh_fastly_cidrs.py ──────────────────────────────────────────────

import importlib.util
import sys
from pathlib import Path

import httpx
import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "refresh_fastly_cidrs.py"


def _load_module():
    """Load ``refresh_fastly_cidrs`` from scripts/ as an importable module."""
    spec = importlib.util.spec_from_file_location("refresh_fastly_cidrs", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["refresh_fastly_cidrs"] = module
    spec.loader.exec_module(module)
    return module


refresh = _load_module()


# Captured 2026-06-03 Fastly public-ip-list payload (trimmed; ipv6 retained
# so we exercise the "ignore v6" path of the script).
SAMPLE_FASTLY_RESPONSE = {
    "addresses": [
        "151.101.0.0/16",
        "23.235.32.0/20",
        "43.249.72.0/22",
        "199.232.0.0/16",
        "146.75.0.0/17",
    ],
    "ipv6_addresses": [
        "2a04:4e40::/32",
        "2a04:4e42::/32",
    ],
}

SAMPLE_CADDYFILE = """\
:80 {
\timport security_headers

\t@from_fastly_v4 {
\t\tremote_ip 1.1.1.0/24 2.2.2.0/24
\t}

\trequest_header @from_fastly_v4 X-Forwarded-For {http.request.header.Fastly-Client-IP}
}
"""


def test_sort_cidrs_orders_by_octet_then_prefix():
    """Sort must produce a deterministic, human-readable order so refreshes
    with no upstream change are a no-op (idempotency)."""
    out = refresh.sort_cidrs(["199.232.0.0/16", "23.235.32.0/20", "151.101.0.0/16"])
    assert out == ["23.235.32.0/20", "151.101.0.0/16", "199.232.0.0/16"]


def test_rewrite_caddyfile_replaces_remote_ip_line_only():
    """The rewrite must touch only the remote_ip line — surrounding
    Caddyfile bytes (matcher name, braces, other directives) stay verbatim.
    Pinned because a botched rewrite could silently delete the request_header
    line and disable the whole Fastly-Client-IP propagation."""
    cidrs = ["23.235.32.0/20", "151.101.0.0/16"]
    out = refresh.rewrite_caddyfile(SAMPLE_CADDYFILE, cidrs)

    assert "remote_ip 23.235.32.0/20 151.101.0.0/16" in out
    # Original CIDRs are gone.
    assert "1.1.1.0/24" not in out
    # Surrounding lines untouched.
    assert "import security_headers" in out
    assert "request_header @from_fastly_v4 X-Forwarded-For" in out
    # Tab indentation preserved.
    assert "\t\tremote_ip" in out


def test_rewrite_caddyfile_is_idempotent():
    """Running twice with the same CIDR list must converge — second pass
    is a no-op. Guards against the script appending instead of replacing."""
    cidrs = ["23.235.32.0/20", "151.101.0.0/16"]
    once = refresh.rewrite_caddyfile(SAMPLE_CADDYFILE, cidrs)
    twice = refresh.rewrite_caddyfile(once, cidrs)
    assert once == twice


def test_rewrite_caddyfile_raises_when_matcher_missing():
    """If somebody renames the matcher the script must fail loud, not
    silently leave the Caddyfile unchanged."""
    with pytest.raises(RuntimeError, match="@from_fastly_v4"):
        refresh.rewrite_caddyfile(":80 {\n\tlog\n}\n", ["1.1.1.0/24"])


def test_fetch_fastly_cidrs_parses_v4_only_and_sorts():
    """Mocked transport — exercises JSON parsing + sort without network."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == refresh.FASTLY_PUBLIC_IP_LIST
        return httpx.Response(200, content=json.dumps(SAMPLE_FASTLY_RESPONSE).encode())

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        out = refresh.fetch_fastly_cidrs(client=client)

    # Sorted by octet; ipv6 dropped entirely.
    assert out == [
        "23.235.32.0/20",
        "43.249.72.0/22",
        "146.75.0.0/17",
        "151.101.0.0/16",
        "199.232.0.0/16",
    ]


def test_fetch_fastly_cidrs_rejects_empty_v4_list():
    """Empty allow-list would lock the matcher to nothing — refuse loudly
    instead of writing an empty CIDR set into production."""

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b'{"addresses": [], "ipv6_addresses": []}')

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError, match="empty allow-list"):
            refresh.fetch_fastly_cidrs(client=client)


# ── Merged from test_pop_utils.py ──────────────────────────────────────────────


import pytest

from backend.utils import pop_utils


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    cache = tmp_path / "pop_locations.json"
    monkeypatch.setattr(pop_utils, "CACHE_FILE", str(cache))
    return cache


# ── fetch_pop_locations ──────────────────────────────────────────────────────


def test_fetch_returns_false_for_empty_api_key(isolated_cache):
    assert pop_utils.fetch_pop_locations("") is False
    assert pop_utils.fetch_pop_locations(None) is False  # type: ignore[arg-type]
    # Nothing was written
    assert not isolated_cache.exists()


def test_fetch_writes_cache_on_success(isolated_cache):
    sample = [{"code": "JFK", "coordinates": {"latitude": 40.6, "longitude": -73.8}}]

    with patch("backend.core.fastly.client.fastly", return_value=sample):
        ok = pop_utils.fetch_pop_locations("api-key")

    assert ok is True
    assert isolated_cache.exists()
    assert json.loads(isolated_cache.read_text()) == sample


def test_fetch_returns_false_on_network_error(isolated_cache):
    with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("boom")):
        ok = pop_utils.fetch_pop_locations("api-key")
    assert ok is False
    assert not isolated_cache.exists()


def test_fetch_passes_api_key_to_fastly_client(isolated_cache):
    """The Fastly-Key header is what the API authenticates on; fastly()
    sets it from the token kwarg. Verify the kwarg is threaded through —
    a missing/wrong token returns the API's public dataset (different
    shape) and silently breaks the cache.
    """
    with patch("backend.core.fastly.client.fastly", return_value=[]) as mock_fastly:
        pop_utils.fetch_pop_locations("my-secret-token")

    mock_fastly.assert_called_once_with("GET", "/datacenters", token="my-secret-token")


# ── get_pop_locations ────────────────────────────────────────────────────────


def test_get_returns_empty_when_cache_missing(isolated_cache):
    assert pop_utils.get_pop_locations() == []


def test_get_returns_cached_data(isolated_cache):
    data = [{"code": "LHR", "coordinates": {"latitude": 51.5, "longitude": -0.1}}]
    isolated_cache.write_text(json.dumps(data))
    assert pop_utils.get_pop_locations() == data


def test_get_returns_empty_for_corrupt_cache(isolated_cache):
    """A truncated or non-JSON file must not raise — just return empty."""
    isolated_cache.write_text("{ not valid json")
    assert pop_utils.get_pop_locations() == []


# ── get_pop_lat_lon_map ──────────────────────────────────────────────────────


def test_lat_lon_map_extracts_coords_per_code(isolated_cache):
    data = [
        {"code": "JFK", "coordinates": {"latitude": 40.6, "longitude": -73.8}},
        {"code": "LHR", "coordinates": {"latitude": 51.5, "longitude": -0.1}},
    ]
    isolated_cache.write_text(json.dumps(data))
    got = pop_utils.get_pop_lat_lon_map()
    assert got == {"JFK": (40.6, -73.8), "LHR": (51.5, -0.1)}


def test_lat_lon_map_skips_entries_missing_required_fields(isolated_cache):
    """A POP without coordinates (or with missing latitude/longitude) must
    be silently skipped — the alternative is a crash in the consumer."""
    data = [
        {"code": "GOOD", "coordinates": {"latitude": 1.0, "longitude": 2.0}},
        {"code": "NO_COORDS"},  # no coordinates key
        {"code": "PARTIAL", "coordinates": {"latitude": 3.0}},  # missing longitude
        {"coordinates": {"latitude": 4.0, "longitude": 5.0}},  # no code
    ]
    isolated_cache.write_text(json.dumps(data))
    got = pop_utils.get_pop_lat_lon_map()
    assert got == {"GOOD": (1.0, 2.0)}


def test_lat_lon_map_returns_empty_for_non_list_cache(isolated_cache):
    """Defensive: if the cache ever contains a dict instead of a list
    (legacy format, manual edit, etc.), don't crash."""
    isolated_cache.write_text(json.dumps({"some": "object"}))
    assert pop_utils.get_pop_lat_lon_map() == {}


# ── Merged from test_geo.py ──

from backend.utils.countries import format_city_label


class TestFormatCityLabel:
    def test_full_label(self):
        result = format_city_label("Portland", "US", "OR")
        assert result == "Portland, OR, United States"

    def test_no_region(self):
        result = format_city_label("London", "GB")
        assert result in ("London, United Kingdom", "London, GB")

    def test_country_only(self):
        result = format_city_label("", "US")
        assert result in ("United States", "US")

    def test_unknown_country_code_uppercased(self):
        result = format_city_label("Metropolis", "ZZ")
        assert "Metropolis" in result
        assert "ZZ" in result

    def test_all_empty_returns_unknown(self):
        assert format_city_label("", "", "") == "Unknown"

    def test_city_title_cased(self):
        result = format_city_label("new york", "US")
        assert result.startswith("New York")

    def test_region_uppercased(self):
        result = format_city_label("Lyon", "FR", "ara")
        assert ", ARA" in result

    def test_none_like_empty_strings(self):
        # The function coerces falsy to "" via `city = city or ""`
        result = format_city_label("", "", "")
        assert result == "Unknown"

    def test_country_without_city_or_region(self):
        result = format_city_label("", "DE", "")
        assert result in ("Germany", "DE")

    def test_country_code_case_insensitive(self):
        lower = format_city_label("Berlin", "de")
        upper = format_city_label("Berlin", "DE")
        assert lower == upper


# ── Merged from test_provision_utils.py ──────────────────────────────────────────────


import pytest

import backend.provision.cli as pu

# ── _highlight: regex-driven quote highlighting ──────────────────────────────


def test_highlight_wraps_single_quoted_substring_in_magenta():
    out = pu._highlight("created service 'svc-123'")
    assert pu.MAG in out  # magenta colour wraps the quoted name
    assert "svc-123" in out
    assert pu.RST in out


def test_highlight_leaves_already_coloured_strings_alone():
    """If the message already contains ANSI escapes, don't double-wrap.
    Otherwise the second pass would corrupt the colour reset codes."""
    pre_coloured = f"{pu.GRN}already coloured{pu.RST}"
    assert pu._highlight(pre_coloured) == pre_coloured


def test_highlight_handles_message_without_quotes():
    """No quotes → no change other than the wrapping the regex would
    have made. Pinned because the regex must be a no-op, not crash."""
    assert pu._highlight("no quotes here") == "no quotes here"


# ── ask: prompt + default handling ───────────────────────────────────────────


def test_ask_returns_user_input_when_provided():
    with patch("builtins.input", return_value="user-value"):
        assert pu.ask("what?") == "user-value"


def test_ask_returns_default_on_empty_input():
    """Bare Enter → fall back to default. Pinned because provisioning
    relies heavily on ``[default]`` prompts to skip past boilerplate."""
    with patch("builtins.input", return_value=""):
        assert pu.ask("region?", default="us-east-1") == "us-east-1"


def test_ask_returns_empty_string_when_no_default_and_no_input():
    """No default and no input → empty string (not None) so callers can
    safely call ``.strip()``, ``len()``, etc. without a None-check."""
    with patch("builtins.input", return_value=""):
        assert pu.ask("optional?") == ""


def test_ask_strips_whitespace_from_input():
    with patch("builtins.input", return_value="  value  "):
        assert pu.ask("what?") == "value"


# ── ask_yes: Y/n parsing ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "answer,expected",
    [
        ("y", True),
        ("Y", True),
        ("yes", True),
        ("YES", True),
        ("n", False),
        ("N", False),
        ("no", False),
        ("NO", False),
        ("", True),  # default=True (the default default)
    ],
)
def test_ask_yes_with_default_true(answer, expected):
    with patch("builtins.input", return_value=answer):
        assert pu.ask_yes("ok?") is expected


def test_ask_yes_default_false_returns_false_on_empty():
    with patch("builtins.input", return_value=""):
        assert pu.ask_yes("ok?", default=False) is False


def test_ask_yes_non_y_prefix_treated_as_no():
    """Anything not starting with 'y' is false. Pinned because a
    permissive interpretation here would let a typo (``q``, ``ok``)
    accidentally confirm a destructive action."""
    with patch("builtins.input", return_value="maybe"):
        assert pu.ask_yes("destroy?", default=False) is False


# ── ask_int: validation retry loop ───────────────────────────────────────────


def test_ask_int_returns_parsed_integer():
    with patch("builtins.input", return_value="42"):
        assert pu.ask_int("n?", default=10) == 42


def test_ask_int_returns_default_on_empty_input():
    with patch("builtins.input", return_value=""):
        assert pu.ask_int("n?", default=7) == 7


def test_ask_int_retries_on_non_integer():
    """First input is garbage → loop, second is valid → return. The
    operator should never get a Python traceback for a typo."""
    inputs = iter(["abc", "9"])
    with patch("builtins.input", side_effect=lambda _: next(inputs)):
        assert pu.ask_int("n?", default=0) == 9


def test_ask_int_rejects_below_min_then_accepts():
    inputs = iter(["3", "10"])
    with patch("builtins.input", side_effect=lambda _: next(inputs)):
        assert pu.ask_int("n?", default=0, min_val=5) == 10


def test_ask_int_rejects_above_max_then_accepts():
    inputs = iter(["999", "50"])
    with patch("builtins.input", side_effect=lambda _: next(inputs)):
        assert pu.ask_int("n?", default=0, max_val=100) == 50


# ── Print helpers: structural pinning (no escape-code lock-in) ──────────────


def test_fail_writes_to_stderr(capsys):
    """``fail`` goes to stderr so the CLI ``set -o pipefail`` users
    catch errors. The other helpers default to stdout."""
    pu.fail("boom")
    captured = capsys.readouterr()
    assert "boom" in captured.err
    assert captured.out == ""


def test_ok_writes_to_stdout(capsys):
    pu.ok("done")
    captured = capsys.readouterr()
    assert "done" in captured.out
    assert captured.err == ""


def test_blank_emits_newline(capsys):
    pu.blank()
    assert capsys.readouterr().out == "\n"


def test_step_includes_progress_and_title(capsys):
    pu.step(2, 5, "Configure Fastly")
    out = capsys.readouterr().out
    assert "[2/5]" in out
    assert "Configure Fastly" in out


def test_banner_draws_separator_around_title(capsys):
    pu.banner("Welcome")
    out = capsys.readouterr().out
    assert "Welcome" in out
    # Bar uses U+2501 BOX DRAWINGS HEAVY HORIZONTAL repeated 64 times
    assert "━" * 10 in out


# ── _c: trivial wrapper, exercised here so it's not "uncovered" ─────────────


def test_c_wraps_text_with_colour_and_reset():
    wrapped = pu._c(pu.GRN, "hello")
    assert wrapped.startswith(pu.GRN)
    assert wrapped.endswith(pu.RST)
    assert "hello" in wrapped


# ── Merged from tests/utils/test_bounded_cache.py ──

import ast
import logging
import math
import os
import re
import shutil
import socket
import threading
import time
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
import structlog
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

import backend.core.metadata.base as pg_connection
import backend.core.request_telemetry as query_registry
import backend.cron.scheduler as system_jobs
import backend.utils.telemetry as active_requests
import backend.utils.telemetry as log_throttle
import backend.utils.telemetry as sqlite_profiler
import backend.utils.telemetry as structlog_config
import backend.utils.tunnel as _session_mod
from backend.core.metadata import base
from backend.core.rollups._common import parse_hour_token
from backend.utils import remote_access, tunnel
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.date_utils import (
    _parse_dt,
    ensure_utc,
    iso_z,
    iso_z_now,
    parse_date_window,
    parse_day_key_strict,
    parse_hour_key,
    parse_hour_key_strict,
    parse_iso_utc,
    parse_relative_time_window,
    parse_window,
    parse_window_str_to_dt,
    safe_iso,
    window_to_epoch,
)
from backend.utils.telemetry import (
    _SQLITE_QUERIES,
    _reset_for_tests,
    active_request_count,
    decrement_active_requests,
    increment_active_requests,
    log_once_per,
    should_defer_cron,
    yield_to_api,
)
from backend.utils.time_window import (
    _BANDS,
    ANCHOR_QUANTUM_SECONDS,
    STATIC_DEFAULT,
    VALID_RANGE_TOKENS,
    history_hours_from_earliest,
    invite_clamp_fingerprint,
    is_valid_range_token,
    pick_insights_default,
    quantize_anchor,
    resolve_window,
)

ra = remote_access

if hasattr(_session_mod, "MAX_TRACKED_IPS"):
    login_mod = _session_mod
else:
    import backend.utils.tunnel as login_mod

ROOT = Path(__file__).resolve().parents[2]
_NOW = datetime(2026, 6, 29, 12, 30, 45, tzinfo=UTC)
_INSIGHTS_NOW = datetime(2026, 7, 7, 12, 0, 0, tzinfo=UTC)
_TS_SOURCE = ROOT / "frontend" / "lib" / "chartUtils.ts"
_HOUR_KEYS = ["2026-10-19-00", "2026-10-19-23", "2026-02-28-23", "2024-02-29-12", "2026-1-5-3"]
_BAD_KEYS = ["", "2026-10-19", "2026-10-19-24", "2026-13-01-00", "garbage", "2026-10-19T05", "2026-10-19-05-00"]
_DROP_TABLE_FILES = sorted([*(ROOT / "backend" / "repositories").glob("*.py"), ROOT / "backend" / "core" / "duckdb.py"])


@pytest.fixture(autouse=True)
def _auto_isolate_all(monkeypatch):
    snapshot_names = CacheRegistry.names()
    monkeypatch.setattr(log_throttle, "_last", {})
    _reset_for_tests()
    sqlite_profiler.clear()
    structlog.reset_defaults()
    structlog_config.configure_structlog()
    with system_jobs._status_lock:
        system_jobs._status.clear()
    yield
    for n in CacheRegistry.names():
        if n not in snapshot_names:
            with CacheRegistry._lock:
                CacheRegistry._entries.pop(n, None)
    _reset_for_tests()
    sqlite_profiler.clear()
    structlog.reset_defaults()
    structlog_config.configure_structlog()


# ── BoundedTTLCache ──────────────────────────────────────────────────────────


def test_basic_set_get_and_miss():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    cache["k1"] = "v1"
    assert cache["k1"] == "v1" and cache.get("k1") == "v1" and "k1" in cache
    assert cache.get("missing") is None and cache.get("missing", "sentinel") == "sentinel"
    assert "missing" not in cache
    with pytest.raises(KeyError):
        _ = cache["missing"]


def test_ttl_expiry_treats_entry_as_absent():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=0.05)
    cache["k1"] = "v1"
    assert cache["k1"] == "v1"
    time.sleep(0.10)
    assert "k1" not in cache and cache.get("k1") is None
    with pytest.raises(KeyError):
        _ = cache["k1"]


def test_maxsize_evicts_lru_and_read_promotes():
    cache = BoundedTTLCache(maxsize=3, ttl_seconds=60)
    cache["a"], cache["b"], cache["c"] = 1, 2, 3
    assert cache["a"] == 1
    cache["d"] = 4
    assert "b" not in cache and (cache["a"], cache["c"], cache["d"], len(cache)) == (1, 3, 4, 3)

    c2 = BoundedTTLCache(maxsize=2, ttl_seconds=60)
    c2["warm"], c2["cold"] = "stays", "dies"
    for _ in range(5):
        _ = c2["warm"]
    c2["new"] = "added"
    assert "cold" not in c2 and c2["warm"] == "stays" and c2["new"] == "added"


def test_pop_and_clear():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    cache["k1"] = "v1"
    assert cache.pop("k1") == "v1" and "k1" not in cache
    assert cache.pop("missing", "default") == "default"
    with pytest.raises(KeyError):
        cache.pop("missing")

    for i in range(5):
        cache[f"k{i}"] = i
    assert len(cache) == 5
    cache.clear()
    assert len(cache) == 0 and all(f"k{i}" not in cache for i in range(5))


def test_lazy_reap_fires_after_n_writes():
    cache = BoundedTTLCache(maxsize=10_000, ttl_seconds=0.05)
    for i in range(50):
        cache[f"expired_{i}"] = i
    time.sleep(0.40)
    for i in range(100):
        cache[f"fresh_{i}"] = i
    assert all(f"expired_{i}" not in cache for i in range(50))
    assert all(cache[f"fresh_{i}"] == i for i in range(100))


def test_iteration_snapshot_invalid_args_and_tuple_values():
    cache = BoundedTTLCache(maxsize=100, ttl_seconds=60)
    for i in range(20):
        cache[f"k{i}"] = i
    keys = list(cache)
    cache["fresh"] = "added"
    assert len(keys) == 20 and "fresh" not in keys

    for maxsize, ttl in ((0, 10), (-1, 10), (10, 0), (10, -0.5)):
        with pytest.raises(ValueError):
            BoundedTTLCache(maxsize=maxsize, ttl_seconds=ttl)

    payload = (1.23, {"rows": [1, 2, 3], "_meta": "x"})
    cache["k"] = payload
    assert cache["k"] == payload and cache["k"] is payload


def test_thread_safety_under_concurrent_writes():
    cache = BoundedTTLCache(maxsize=200, ttl_seconds=60)
    errors: list[Exception] = []

    def writer(tid: int) -> None:
        try:
            for i in range(500):
                cache[f"t{tid}_k{i}"] = i
        except Exception as exc:
            errors.append(exc)

    def reader() -> None:
        try:
            for _ in range(500):
                for k in cache:
                    _ = cache.get(k)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)] + [
        threading.Thread(target=reader) for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors and len(cache) <= 200


# ── CacheRegistry ────────────────────────────────────────────────────────────


def test_cache_registry_register_and_clear():
    d: dict[str, int] = {"x": 1, "y": 2}
    s: set[str] = {"a", "b"}

    class Box:
        def __init__(self):
            self.items = [1, 2, 3]
            self.cleared = False

        def clear(self):
            self.items = []
            self.cleared = True

    b = Box()
    CacheRegistry.register("test.dict_a", d)
    CacheRegistry.register("test.set_a", s)
    CacheRegistry.register("test.box", b)
    CacheRegistry.register("test.bare_int", 42)
    CacheRegistry.clear_all()
    assert d == {} and s == set() and b.cleared is True and b.items == []


def test_storage_caches_are_registered_at_import():
    import backend.core.duckdb  # noqa: F401
    import backend.core.storage  # noqa: F401
    import backend.repositories.dashboard  # noqa: F401
    import backend.repositories.network  # noqa: F401

    names = set(CacheRegistry.names())
    for required in (
        "storage._view_cache",
        "dashboard._dashboard_cache",
        "network._response_cache",
        "duckdb._fos_client_cache",
        "duckdb._initialized_paths",
        "core._duckdb_status._schema_cache",
    ):
        assert required in names, f"missing registration: {required}"


# ── Log throttle ─────────────────────────────────────────────────────────────


def test_log_throttle_interval_independence_and_bounding(monkeypatch, caplog):
    t = [100.0]
    monkeypatch.setattr(log_throttle.time, "monotonic", lambda: t[0])
    assert log_once_per("k", 60) is True
    t[0] += 59
    assert log_once_per("k", 60) is False
    t[0] += 2
    assert log_once_per("k", 60) is True

    assert log_once_per("a", 60) is True and log_once_per("b", 60) is True and log_once_per("a", 60) is False

    monkeypatch.setattr(log_throttle, "_MAX_KEYS", 8)
    for i in range(100):
        log_once_per(f"k{i}", 3600)
    assert len(log_throttle._last) <= 8

    logger = logging.getLogger("t")
    with caplog.at_level(logging.DEBUG, logger="t"):
        for _ in range(500):
            try:
                raise ValueError("bad row")
            except ValueError:
                if log_once_per("row-parse", 60):
                    logger.debug("row parse failed", exc_info=True)
    assert len([r for r in caplog.records if r.exc_info]) == 1


def _unthrottled(path: str, only_funcs: set[str] | None, only_msg: str | None) -> list[str]:
    tree = ast.parse((ROOT / path).read_text())
    out: list[str] = []

    def walk(node: ast.AST, guarded: bool, func: str | None) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func = node.name
        if (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.Call)
            and getattr(node.test.func, "id", "") == "log_once_per"
        ):
            for child in node.body:
                walk(child, True, func)
            for child in node.orelse:
                walk(child, guarded, func)
            return
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "debug"
            and any(k.arg == "exc_info" for k in node.keywords)
            and not guarded
        ):
            msg = node.args[0].value if node.args and isinstance(node.args[0], ast.Constant) else ""
            if (only_funcs is None or func in only_funcs) and (only_msg is None or only_msg in str(msg)):
                out.append(f"{path}:{node.lineno}")
        for child in ast.iter_child_nodes(node):
            walk(child, guarded, func)

    walk(tree, False, None)
    return out


@pytest.mark.parametrize(
    ("path", "funcs", "msg"),
    [
        ("backend/core/duckdb_pool.py", None, None),
        ("backend/core/ingest/parser.py", None, "_parse_rum_line"),
        ("backend/core/ingest/parser.py", {"extract_metrics_from_faro_payload"}, None),
        ("backend/repositories/dashboard.py", None, "analyst"),
        *[(p.relative_to(ROOT).as_posix(), None, "DROP TABLE") for p in _DROP_TABLE_FILES],
    ],
)
def test_hot_path_debug_tracebacks_are_throttled(path, funcs, msg):
    assert _unthrottled(path, funcs, msg) == []


# ── Rate limit characterization ──────────────────────────────────────────────


class Clock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(time, "time", c)
    return c


@pytest.mark.security_regression
def test_quota_request_boundary_is_exact(clock):
    lim = ra._StaticAssetLimiter(req_limit=3)
    assert [lim.check("1.1.1.1", 0) for _ in range(3)] == [True, True, True]
    assert lim.check("1.1.1.1", 0) is False
    assert lim.check("1.1.1.1", 0) is False
    assert lim.check("2.2.2.2", 0) is True


@pytest.mark.security_regression
def test_quota_window_expiry_frees_budget(clock):
    lim = ra._StaticAssetLimiter(req_limit=2, window_s=60)
    assert lim.check("ip", 0) and lim.check("ip", 0) and not lim.check("ip", 0)
    clock.now += 59
    assert lim.check("ip", 0) is False
    clock.now += 1
    assert lim.check("ip", 0) is False
    clock.now += 1
    assert lim.check("ip", 0) is True


@pytest.mark.security_regression
def test_quota_byte_boundary_counts_the_request_that_crosses_it(clock):
    lim = ra._StaticAssetLimiter(req_limit=100, byte_limit=1000)
    assert lim.check("ip", 1000) is True
    assert lim.check("ip", 1) is False and lim.check("ip", 0) is False
    clock.now += 61
    assert lim.check("ip", 1000) is True


@pytest.mark.security_regression
def test_quota_negative_content_length_counts_as_zero(clock):
    lim = ra._StaticAssetLimiter(req_limit=100, byte_limit=10)
    assert all(lim.check("ip", -999) for _ in range(5))


@pytest.mark.security_regression
def test_quota_request_limit_hit_does_not_record_bytes(clock):
    lim = ra._StaticAssetLimiter(req_limit=1, byte_limit=100)
    assert lim.check("ip", 100) is True and lim.check("ip", 100_000) is False
    clock.now += 61
    assert lim.check("ip", 100) is True


def test_quota_defaults_and_stale_key_eviction(clock):
    lim = ra._StaticAssetLimiter()
    assert (lim.REQ_LIMIT, lim.BYTE_LIMIT, lim.window_s, lim.MAX_TRACKED_IPS) == (600, 50 * 1024 * 1024, 60, 10_000)
    assert ra._static_limiter is not ra._analyst_api_limiter

    lim2 = ra._StaticAssetLimiter(max_tracked_ips=5)
    for i in range(5):
        lim2.check(f"old-{i}", 1)
    clock.now += 120
    for i in range(3):
        lim2.check(f"new-{i}", 1)
    assert len(lim2._events) <= 5 and all(lim2.check(f"new-{i}", 1) for i in range(3))


@pytest.mark.security_regression
def test_quota_memory_is_bounded_under_key_flood(clock):
    lim = ra._StaticAssetLimiter(max_tracked_ips=50)
    for i in range(5000):
        lim.check(f"10.0.{i // 256}.{i % 256}", 10)
    assert len(lim._events) <= 50


def test_login_defaults_snapshot_and_lru(monkeypatch, clock):
    assert (login_mod.LOGIN_FAILURE_WINDOW_S, login_mod.LOGIN_FAILURE_THRESHOLD, login_mod.LOGIN_LOCKOUT_S) == (
        60,
        5,
        300,
    )
    assert login_mod.MAX_TRACKED_IPS == 10000

    rl = tunnel._LoginRateLimiter()
    rl.record_failure("lonely")
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("locked")
    snap = rl.snapshot()
    by_ip = {r["ip"]: r for r in snap["failures"]}
    assert by_ip["lonely"] == {"ip": "lonely", "count": 1, "window_s": tunnel.LOGIN_FAILURE_WINDOW_S}
    assert by_ip["locked"]["count"] == tunnel.LOGIN_FAILURE_THRESHOLD
    assert snap["lockouts"] == [{"ip": "locked", "remaining_s": tunnel.LOGIN_LOCKOUT_S}]
    clock.now += tunnel.LOGIN_LOCKOUT_S + 1
    assert rl.snapshot() == {"failures": [], "lockouts": []}

    monkeypatch.setattr(login_mod, "MAX_TRACKED_IPS", 3)
    rl2 = tunnel._LoginRateLimiter()
    for ip in ("a", "b", "c", "a", "d"):
        rl2.record_failure(ip)
    assert {r["ip"] for r in rl2.snapshot()["failures"]} == {"a", "c", "d"}


@pytest.mark.security_regression
def test_login_lockout_triggers_exactly_at_threshold(clock):
    rl = tunnel._LoginRateLimiter()
    results = [rl.record_failure("1.2.3.4") for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD)]
    assert results == [False] * (tunnel.LOGIN_FAILURE_THRESHOLD - 1) + [True]
    assert rl.record_failure("1.2.3.4") is False
    locked, remaining = rl.is_locked("1.2.3.4")
    assert locked and remaining == tunnel.LOGIN_LOCKOUT_S


@pytest.mark.security_regression
def test_login_failures_outside_window_do_not_accumulate(clock):
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD - 1):
        rl.record_failure("ip")
        clock.now += tunnel.LOGIN_FAILURE_WINDOW_S + 1
    assert rl.record_failure("ip") is False and rl.is_locked("ip") == (False, 0)


@pytest.mark.security_regression
def test_login_lockout_expiry_unlocks_and_remaining_counts_down(clock):
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("ip")
    clock.now += 100
    locked, remaining = rl.is_locked("ip")
    assert locked and remaining == tunnel.LOGIN_LOCKOUT_S - 100
    clock.now += tunnel.LOGIN_LOCKOUT_S
    assert rl.is_locked("ip") == (False, 0)


@pytest.mark.security_regression
def test_login_keys_are_isolated(clock):
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("a")
    assert rl.is_locked("a")[0] is True and rl.is_locked("b") == (False, 0)


@pytest.mark.security_regression
def test_login_memory_is_bounded_under_key_flood(monkeypatch, clock):
    monkeypatch.setattr(login_mod, "MAX_TRACKED_IPS", 20)
    rl = tunnel._LoginRateLimiter()
    for i in range(2000):
        rl.record_failure(f"ip-{i}")
    assert len(rl.snapshot()["failures"]) <= 20


@pytest.mark.security_regression
def test_quota_never_admits_more_than_limit_under_contention():
    lim = ra._StaticAssetLimiter(req_limit=100, byte_limit=10**9, window_s=3600)
    admitted: list[int] = []
    lock = threading.Lock()

    def worker() -> None:
        n = sum(1 for _ in range(50) if lim.check("shared", 0))
        with lock:
            admitted.append(n)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(admitted) == 100


@pytest.mark.security_regression
def test_login_lockout_triggers_exactly_once_under_contention():
    rl = tunnel._LoginRateLimiter()
    triggers: list[int] = []
    lock = threading.Lock()

    def worker() -> None:
        n = sum(1 for _ in range(20) if rl.record_failure("shared"))
        with lock:
            triggers.append(n)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(triggers) == 1 and rl.is_locked("shared")[0] is True


def test_concurrent_distinct_keys_stay_within_cap():
    lim = ra._StaticAssetLimiter(max_tracked_ips=100)
    threads = [threading.Thread(target=lambda b=b: [lim.check(f"k-{b}-{i}", 1) for i in range(500)]) for b in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(lim._events) <= 100


# ── Date & time_window utils ─────────────────────────────────────────────────


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def test_date_utils_core_parsers():
    assert parse_iso_utc(None) is None and parse_iso_utc("") is None and parse_iso_utc("invalid-date-string") is None
    dt1 = parse_iso_utc("2026-08-19T12:00:00Z")
    dt2 = parse_iso_utc("2026-08-19")
    assert dt1 is not None and dt1.tzinfo == UTC and dt1.hour == 12
    assert dt2 is not None and dt2.tzinfo == UTC and dt2.hour == 0 and dt2.day == 19
    assert iso_z(datetime(2026, 8, 19, 12, 34, 56, tzinfo=UTC)) == "2026-08-19T12:34:56Z"
    now_str = iso_z_now()
    assert now_str.endswith("Z") and "T" in now_str

    default_val = datetime(2020, 1, 1, tzinfo=UTC)
    assert _parse_dt("2026-08-19T12:00:00Z", default_val).year == 2026
    assert _parse_dt("invalid", default_val) == default_val
    assert parse_date_window("2026-08-10", "2026-08-19") == ("2026-08-10T00:00:00Z", "2026-08-19T23:59:59Z")
    assert parse_date_window("2026-08-10T12:00:00Z", "2026-08-19T12:00:00Z") == (
        "2026-08-10T12:00:00Z",
        "2026-08-19T12:00:00Z",
    )

    assert safe_iso(None) is None and safe_iso("") is None
    assert safe_iso(datetime(2026, 8, 19, 12, 0, 0)) == "2026-08-19T12:00:00Z"
    assert safe_iso("2026-08-19") == "2026-08-19"
    assert safe_iso("2026-08-19 12:00:00") == "2026-08-19T12:00:00Z"
    assert safe_iso("2026-08-19 12:00:00.123") == "2026-08-19T12:00:00.123Z"
    assert safe_iso("2026-08-19T12:00:00Z") == "2026-08-19T12:00:00Z"
    assert safe_iso("2026-08-19T12:00:00+00:00") == "2026-08-19T12:00:00+00:00"

    dt = parse_window_str_to_dt("2026-08-19T12:00:00Z")
    assert (dt.year, dt.month) == (2026, 8)
    with pytest.raises(ValueError, match="Invalid date window string"):
        parse_window_str_to_dt("invalid")
    s, e, from_ts, to_ts = window_to_epoch("2026-08-10T00:00:00Z", "2026-08-19T12:00:00Z")
    assert (s, e) == ("2026-08-10T00:00:00Z", "2026-08-19T12:00:00Z") and 0 < from_ts < to_ts


@pytest.mark.parametrize(
    ("spec", "expected_sec"),
    [("", 3600), ("12h", 12 * 3600), ("5d", 5 * 24 * 3600), ("30m", 30 * 60), ("invalid", 3600)],
)
def test_parse_relative_time_window(spec, expected_sec):
    assert (datetime.now(UTC) - parse_relative_time_window(spec)).total_seconds() == pytest.approx(expected_sec, abs=10)


def test_parse_window_valid_and_invalid():
    assert parse_window("2026-08-05T10:00:00Z", "2026-08-05T12:30:00+00:00") == (
        datetime(2026, 8, 5, 10, tzinfo=UTC),
        datetime(2026, 8, 5, 12, 30, tzinfo=UTC),
    )
    assert parse_window("2026-08-05", "2026-08-06") == (
        datetime(2026, 8, 5, tzinfo=UTC),
        datetime(2026, 8, 6, tzinfo=UTC),
    )
    for start, end in (
        (None, "2026-08-05T12:00:00Z"),
        ("2026-08-05T10:00:00Z", None),
        ("", ""),
        ("garbage", "2026-08-05T12:00:00Z"),
        ("2026-08-05T10:00:00Z", "garbage"),
        ("2026-08-05T12:00:00Z", "2026-08-05T12:00:00Z"),
        ("2026-08-05T12:00:00Z", "2026-08-05T10:00:00Z"),
    ):
        assert parse_window(start, end) is None


def test_hour_and_day_key_parsers_and_ensure_utc():
    for key in _HOUR_KEYS:
        raw = datetime.strptime(key, "%Y-%m-%d-%H").replace(tzinfo=UTC)
        assert parse_hour_key(key) == raw == parse_hour_key_strict(key) and parse_hour_key(key).tzinfo is UTC
    for key in _BAD_KEYS:
        assert parse_hour_key(key) is None and parse_hour_token(key) is None
        with pytest.raises(ValueError):
            parse_hour_key_strict(key)
    for key in ("2026-10-19", "2024-02-29", "2026-1-5"):
        assert parse_day_key_strict(key) == datetime.strptime(key, "%Y-%m-%d").replace(tzinfo=UTC)
    for key in ("", "2026-02-30", "2026-10-19-05", "x"):
        with pytest.raises(ValueError):
            parse_day_key_strict(key)

    assert ensure_utc(datetime(2026, 10, 19, 5)) == datetime(2026, 10, 19, 5, tzinfo=UTC)
    src = datetime(2026, 10, 19, 5, tzinfo=timezone(timedelta(hours=5)))
    out = ensure_utc(src)
    assert out == src and out.utcoffset() == timedelta(0) and out.hour == 0


def test_range_tokens_filterbar_and_quantize_anchor():
    assert all(is_valid_range_token(t) for t in ("24h", "7d", "30d", "auto"))
    assert VALID_RANGE_TOKENS == {"1h", "3h", "6h", "12h", "24h", "3d", "7d", "30d", "auto"}
    assert all(not is_valid_range_token(b) for b in (None, "", "90d", "nonsense"))

    src = (ROOT / "frontend" / "components" / "FilterBar" / "FilterBar.tsx").read_text()
    presets = re.findall(r"pickRelative\(\s*'([^']+)'\s*,\s*([\d.]+)\s*\)", src)
    assert presets and len(presets) == src.count("pickRelative(")
    for label, hours in presets:
        assert is_valid_range_token(label)
        start, end = resolve_window(label, _NOW.isoformat(), now=_NOW)
        assert _dt(end) - _dt(start) == timedelta(hours=float(hours))

    assert ANCHOR_QUANTUM_SECONDS == 60
    assert quantize_anchor("2026-06-29T12:30:45Z") == "2026-06-29T12:30:00Z"
    assert quantize_anchor("2026-06-29T12:30:01Z") == quantize_anchor("2026-06-29T12:30:59Z")
    assert quantize_anchor("2026-06-29T12:30:01Z") != quantize_anchor("2026-06-29T12:31:00Z")
    assert quantize_anchor(None, now=_NOW) == "2026-06-29T12:30:00Z"
    assert quantize_anchor("not-a-date", now=_NOW) == "2026-06-29T12:30:00Z"


@pytest.mark.parametrize(
    ("token", "delta"),
    [
        ("1h", timedelta(hours=1)),
        ("3h", timedelta(hours=3)),
        ("6h", timedelta(hours=6)),
        ("12h", timedelta(hours=12)),
        ("24h", timedelta(hours=24)),
        ("3d", timedelta(days=3)),
        ("7d", timedelta(days=7)),
        ("30d", timedelta(days=30)),
    ],
)
def test_fixed_tokens_resolve_to_anchor_minus_delta(token, delta):
    start, end = resolve_window(token, _NOW.isoformat(), now=_NOW)
    assert end == "2026-06-29T12:30:00Z" and _dt(end) - _dt(start) == delta


def test_resolve_window_auto_and_invite_fingerprint():
    assert resolve_window("7d", "2026-06-29T12:30:05Z", now=_NOW) == resolve_window(
        "7d", "2026-06-29T12:30:55Z", now=_NOW
    )
    with pytest.raises(ValueError):
        resolve_window("90d", _NOW.isoformat(), now=_NOW)

    for history_hours, expected in ((1, "24h"), (6 * 24, "24h"), (7 * 24, "7d"), (29 * 24, "7d"), (45 * 24, "30d")):
        earliest = (_NOW - timedelta(hours=history_hours)).isoformat()
        assert resolve_window("auto", _NOW.isoformat(), earliest_log_at=earliest, now=_NOW) == resolve_window(
            expected, _NOW.isoformat(), now=_NOW
        )
    seven = resolve_window("7d", _NOW.isoformat(), now=_NOW)
    assert resolve_window("auto", _NOW.isoformat(), earliest_log_at=None, now=_NOW) == seven
    assert resolve_window("auto", _NOW.isoformat(), earliest_log_at="2026-06-01", now=_NOW) == seven

    class _Session:
        def __init__(self, qs=None, qe=None, qw=None):
            self.query_start_time, self.query_end_time, self.query_window_hours = qs, qe, qw

    assert invite_clamp_fingerprint(None) is None
    fps = {
        invite_clamp_fingerprint(_Session()),
        invite_clamp_fingerprint(_Session(qs="2026-01-01T00:00:00Z", qe="2026-02-01T00:00:00Z")),
        invite_clamp_fingerprint(_Session(qw=24)),
    }
    assert len(fps) == 3 and invite_clamp_fingerprint(_Session(qw=168)) == invite_clamp_fingerprint(_Session(qw=168))


@pytest.mark.parametrize(
    ("history_hours", "expected"),
    [
        (0.0, (0.25, 1.0)),
        (0.99, (0.25, 1.0)),
        (1.0, (1.0, 1.0)),
        (3.9, (1.0, 1.0)),
        (4.0, (4.0, 1.0)),
        (23.9, (4.0, 1.0)),
        (24.0, (4.0, 24.0)),
        (47.9, (4.0, 24.0)),
        (48.0, (24.0, 24.0)),
        (167.9, (24.0, 24.0)),
        (168.0, (24.0, 168.0)),
        (719.9, (24.0, 168.0)),
        (720.0, (24.0, 168.0)),
        (1270.0, (24.0, 168.0)),
    ],
)
def test_band_table(history_hours: float, expected: tuple[float, float]) -> None:
    assert pick_insights_default(history_hours) == expected


def test_insights_defaults_and_ts_band_mirror() -> None:
    for val in (None, math.nan, math.inf):
        assert pick_insights_default(val) == STATIC_DEFAULT == (24.0, 168.0)
    earliest = (_INSIGHTS_NOW - timedelta(hours=53 * 24)).isoformat()
    assert history_hours_from_earliest(earliest, now=_INSIGHTS_NOW) == pytest.approx(53 * 24)
    assert history_hours_from_earliest("2026-07-01", now=_INSIGHTS_NOW) == pytest.approx(6 * 24 + 12)
    assert history_hours_from_earliest((_INSIGHTS_NOW + timedelta(hours=5)).isoformat(), now=_INSIGHTS_NOW) == 0.0
    for bad in (None, "", "not-a-date"):
        assert history_hours_from_earliest(bad, now=_INSIGHTS_NOW) is None

    src = _TS_SOURCE.read_text()
    body = src[src.index("export function pickInsightsDefault") :]
    band_re = re.compile(
        r"if\s*\(\s*h\s*<\s*([\d.]+)\s*\)\s*return\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}"
    )
    ts_bands = [(float(u), float(w), float(b)) for u, w, b in band_re.findall(body)]
    tails = re.findall(r"^\s*return\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}", body, re.MULTILINE)
    ts_bands.append((math.inf, float(tails[0][0]), float(tails[0][1])))
    assert tuple(ts_bands) == _BANDS
    m = re.search(r"STATIC_DEFAULT\s*=\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}", src)
    assert m is not None and (float(m.group(1)), float(m.group(2))) == STATIC_DEFAULT


# ── Active requests & cron deferral ──────────────────────────────────────────


def test_active_request_counter_primitives_and_reset():
    _reset_for_tests()
    assert active_request_count() == 0
    increment_active_requests()
    increment_active_requests()
    assert active_request_count() == 2
    decrement_active_requests()
    assert active_request_count() == 1
    decrement_active_requests()
    decrement_active_requests()
    assert active_request_count() == 0

    increment_active_requests()
    assert should_defer_cron("sync", "svc-A") is True and active_requests._defer_started_at
    _reset_for_tests()
    assert active_request_count() == 0 and active_requests._defer_started_at == {}


def test_concurrent_increments_and_decrements():
    _reset_for_tests()
    b1 = threading.Barrier(50)
    t1 = [threading.Thread(target=lambda: (b1.wait(), increment_active_requests())) for _ in range(50)]
    for t in t1:
        t.start()
    for t in t1:
        t.join()
    assert active_request_count() == 50

    _reset_for_tests()
    for _ in range(10):
        increment_active_requests()
    b2 = threading.Barrier(30)
    t2 = [threading.Thread(target=lambda: (b2.wait(), decrement_active_requests())) for _ in range(30)]
    for t in t2:
        t.start()
    for t in t2:
        t.join()
    assert active_request_count() == 0


def test_should_defer_cron_lifecycle_and_starvation(monkeypatch, caplog):
    _reset_for_tests()
    assert should_defer_cron("sync", "svc-A") is False
    increment_active_requests()
    assert should_defer_cron("sync", "svc-A", max_defer_secs=30.0) is True
    first_started = active_requests._defer_started_at[("sync", "svc-A")]
    assert should_defer_cron("sync", "svc-A", max_defer_secs=30.0) is True
    assert active_requests._defer_started_at[("sync", "svc-A")] == first_started

    assert should_defer_cron("sync", "svc-B") is True
    assert should_defer_cron("optimize", "svc-A") is True
    assert len(active_requests._defer_started_at) == 3

    decrement_active_requests()
    assert should_defer_cron("sync", "svc-A") is False
    assert ("sync", "svc-A") not in active_requests._defer_started_at

    increment_active_requests()
    assert should_defer_cron("sync", "svc-A", max_defer_secs=0.01) is True
    time.sleep(0.02)
    with caplog.at_level(logging.WARNING, logger="backend.utils"):
        assert should_defer_cron("sync", "svc-A", max_defer_secs=0.01) is False
    assert ("sync", "svc-A") not in active_requests._defer_started_at
    assert any("active-request-gate" in r.getMessage() for r in caplog.records)

    fake_now = [1000.0]
    monkeypatch.setattr(active_requests.time, "monotonic", lambda: fake_now[0])
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is True
    fake_now[0] = 1010.0
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is True
    fake_now[0] = 1040.0
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is False
    _reset_for_tests()


def test_yield_to_api_behavior():
    _reset_for_tests()
    assert yield_to_api(max_wait_secs=1.0, poll_interval=0.05) == 0.0

    increment_active_requests()
    t = threading.Thread(target=lambda: (time.sleep(0.1), decrement_active_requests()))
    t.start()
    t0 = time.monotonic()
    slept = yield_to_api(max_wait_secs=2.0, poll_interval=0.05)
    elapsed = time.monotonic() - t0
    t.join()
    assert elapsed < 0.5 and 0.05 <= slept < 0.5

    increment_active_requests()
    sleep_calls: list[float] = []
    try:
        slept2 = yield_to_api(max_wait_secs=0.2, poll_interval=0.05)
        assert 0.15 <= slept2 <= 0.3 and active_request_count() == 1
        yield_to_api(max_wait_secs=0.2, poll_interval=0.05, sleep_fn=sleep_calls.append)
    finally:
        decrement_active_requests()
    assert len(sleep_calls) >= 2 and all(s == 0.05 for s in sleep_calls)


# ── SQLite / PG profiler ─────────────────────────────────────────────────────


@pytest.fixture()
def _request_collector():
    token = _SQLITE_QUERIES.set(None)
    yield
    _SQLITE_QUERIES.reset(token)


def test_sqlite_profiler_recording_and_ring_buffer(monkeypatch):
    sqlite_profiler._record("CREATE TABLE t (x INTEGER)", None, 0.5, 0, "execute")
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 1 and snap["queries"][0]["sql"] == "CREATE TABLE t (x INTEGER)"

    sqlite_profiler.clear()
    sqlite_profiler._record("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)], 1.2, 3, "executemany")
    assert sqlite_profiler.get_recent()["queries"][0]["params_kind"] == "seq[3]"

    sqlite_profiler.clear()
    secret = "user-pii-do-not-capture@example.com"
    sqlite_profiler._record("INSERT INTO t VALUES (?)", (secret,), 0.4, 1, "execute")
    entry = sqlite_profiler.get_recent()["queries"][0]
    assert secret not in entry["sql"] and all(secret not in str(v) for v in entry.values())

    sqlite_profiler.clear()
    for i in range(1, 5):
        sqlite_profiler._record(f"SELECT {i}", None, 0.1, 1, "execute")
        if i == 2:
            midpoint = sqlite_profiler.get_recent()["last_seq"]
    sqls = [q["sql"] for q in sqlite_profiler.get_recent(since_seq=midpoint)["queries"]]
    assert sqls == ["SELECT 3", "SELECT 4"]

    sqlite_profiler.clear()
    for i in range(20):
        sqlite_profiler._record(f"SELECT {i}", None, 0.1, 1, "execute")
    assert [q["sql"] for q in sqlite_profiler.get_recent(limit=5)["queries"]] == [f"SELECT {i}" for i in range(15, 20)]

    last_seq_before = sqlite_profiler.get_recent()["last_seq"]
    sqlite_profiler.clear()
    assert sqlite_profiler.get_recent()["buffer_size"] == 0
    sqlite_profiler._record("SELECT 2", None, 0.1, 1, "execute")
    assert sqlite_profiler.get_recent()["last_seq"] > last_seq_before

    monkeypatch.setattr(sqlite_profiler, "_buffer", type(sqlite_profiler._buffer)(maxlen=10))
    monkeypatch.setattr(sqlite_profiler, "_dropped", 0)
    for _ in range(25):
        sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    snap_cap = sqlite_profiler.get_recent()
    assert snap_cap["buffer_size"] == 10 and snap_cap["dropped"] >= 15


def test_sqlite_profiler_helpers_and_con_integration(monkeypatch, _request_collector):
    con = base.get_con("test-svc-traffic")
    assert isinstance(con, pg_connection.PgConnectionWrapper)
    con.execute("SELECT 1")
    assert any("SELECT 1" in q["sql"] for q in sqlite_profiler.get_recent()["queries"])

    long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(1000))
    assert "chars]" in sqlite_profiler._summarize_sql(long_sql)
    assert "SELECT 1" in sqlite_profiler._summarize_sql(b"SELECT 1") and sqlite_profiler._summarize_sql(42) == "42"
    assert (
        sqlite_profiler._describe_params(None) == "none"
        and sqlite_profiler._describe_params([1, 2, 3]) == "seq[3]"
        and sqlite_profiler._describe_params((1,)) == "seq[1]"
        and sqlite_profiler._describe_params({"a": 1, "b": 2}) == "map[2]"
        and sqlite_profiler._describe_params(42) == "int"
    )

    sqlite_profiler.clear()
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    assert _SQLITE_QUERIES.get() is None

    log_throttle.start_call_tracking()
    sqlite_profiler._record("CREATE TABLE t (x INTEGER)", None, 0.2, 0, "execute")
    assert len(log_throttle.get_sqlite_queries()) == 1

    def _raise_collector_broken(_e):
        raise RuntimeError("collector broken")

    monkeypatch.setattr(log_throttle, "record_sqlite_query", _raise_collector_broken)
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")

    bad = MagicMock()
    bad.append.side_effect = RuntimeError("buffer borked")
    monkeypatch.setattr(sqlite_profiler, "_buffer", bad)
    sqlite_profiler._record("SELECT 1", None, 0.5, 1, "execute")

    def _raise_down(*_a, **_kw):
        raise RuntimeError("down")

    monkeypatch.setattr(query_registry.query_registry, "register", _raise_down)
    assert sqlite_profiler._live_register("SQLite", "SELECT 1", con=None) == -1
    sqlite_profiler._live_deregister(-1, error=None)
    monkeypatch.setattr(query_registry.query_registry, "deregister", MagicMock(side_effect=RuntimeError("dereg fail")))
    sqlite_profiler._live_deregister(42, error=None)


# ── Structlog config ─────────────────────────────────────────────────────────


def test_structlog_configuration_and_otel_context(monkeypatch):
    structlog_config.configure_structlog()
    structlog_config.configure_structlog()
    assert structlog.is_configured()

    out = structlog_config._add_otel_trace_context(None, "info", {"event": "test"})
    assert "trace_id" not in out and "span_id" not in out

    tracer = TracerProvider().get_tracer("test")
    with patch.object(trace, "get_current_span", wraps=trace.get_current_span):
        with tracer.start_as_current_span("test-span"):
            out2 = structlog_config._add_otel_trace_context(None, "info", {"event": "test"})
    assert len(out2["trace_id"]) == 32 and len(out2["span_id"]) == 16
    int(out2["trace_id"], 16)
    int(out2["span_id"], 16)

    structlog.reset_defaults()
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()
    structlog.get_logger("json-test").info("event", k=1)


def test_structlog_uvicorn_bridge_and_extra_fields(monkeypatch, capsys):
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()
    for name in structlog_config._UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        lg.handlers = [logging.NullHandler()]
        lg.propagate = False
    structlog_config.bridge_uvicorn_loggers()
    for name in structlog_config._UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        assert lg.handlers == [] and lg.propagate is True

    logging.getLogger("uvicorn.access").info('127.0.0.1 - "GET /x HTTP/1.1" 200')
    rec1 = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert rec1["event"] == '127.0.0.1 - "GET /x HTTP/1.1" 200'

    logging.getLogger("backend.web_vitals").info(
        "web_vitals", extra={"web_vitals_name": "LCP", "web_vitals_rating": "good"}
    )
    rec2 = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert (rec2["event"], rec2["web_vitals_name"], rec2["web_vitals_rating"]) == ("web_vitals", "LCP", "good")

    logging.getLogger("uvicorn.error").info("Uvicorn running", extra={"color_message": "Uvicorn \x1b[1mrunning\x1b[0m"})
    rec3 = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert rec3["event"] == "Uvicorn running" and "color_message" not in rec3


def test_console_exception_traceback_never_renders_local_secrets(monkeypatch, capsys):
    monkeypatch.delenv("STRUCTLOG_FORMAT", raising=False)
    structlog_config.configure_structlog()

    def _load_cfg():
        return {
            "fos_access_key_id": "AKIAFAKEEXAMPLE00000",
            "fos_secret_access_key": "fake-fos-secret-do-not-leak-00000000",
            "fastly_api_key": "fake-fastly-token-do-not-leak-11111111",
        }

    def _reconcile(cfg):
        if not cfg.get("nonexistent_marker"):
            raise RuntimeError("boom")

    try:
        _reconcile(_load_cfg())
    except RuntimeError:
        logging.getLogger("test.rum_sync").warning("Faro reconcile failed for svc-fake", exc_info=True)

    output = capsys.readouterr().err
    for secret in (
        "AKIAFAKEEXAMPLE00000",
        "fake-fos-secret-do-not-leak-00000000",
        "fake-fastly-token-do-not-leak-11111111",
    ):
        assert secret not in output
    assert "RuntimeError" in output and "Faro reconcile failed for svc-fake" in output


# ── System jobs & testcontainers smoke ───────────────────────────────────────


def test_system_jobs_status_recording():
    assert system_jobs.get_system_job_status() == {}
    system_jobs.record_job_run("rdns", status="success", duration_s=1.234, detail="processed 12")
    entry = system_jobs.get_system_job_status()["rdns"]
    assert (entry["status"], entry["duration_s"], entry["detail"]) == ("success", 1.23, "processed 12")
    assert entry["last_run_at"].endswith("Z")

    system_jobs.record_job_run("rdns", "error", 2.0, detail="boom")
    snap = system_jobs.get_system_job_status()
    assert (snap["rdns"]["status"], snap["rdns"]["duration_s"], snap["rdns"]["detail"]) == ("error", 2.0, "boom")
    snap.pop("rdns")
    assert "rdns" in system_jobs.get_system_job_status()

    with patch("backend.cron.scheduler.iso_z_now", return_value="2026-05-15T12:30:45Z"):
        system_jobs.record_job_run("bots", "success", 0.1)
    assert system_jobs.get_system_job_status()["bots"]["last_run_at"] == "2026-05-15T12:30:45Z"


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    for path in ("/var/run/docker.sock", f"/Users/{os.environ.get('USER') or ''}/.docker/run/docker.sock"):
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                s.connect(path)
                return True
        except OSError:
            continue
    return False


@pytest.mark.skipif(not _docker_available(), reason="testcontainers requires a running Docker daemon")
def test_can_spawn_a_container_and_read_back_its_id():
    pytest.importorskip("testcontainers.core.container")
    from testcontainers.core.container import DockerContainer

    with DockerContainer("alpine:3.20").with_command("sleep 60") as c:
        assert c.get_wrapped_container().id


# ── Merged from tests/utils/test_fos_setup.py ──

"""Tests for ``backend.provision.fos_setup`` — FOS bucket + access-key CRUD.

This module is the provisioning CLI's S3 layer. Its operations are
hard-to-reverse (creating buckets, generating keys) and rarely run in
production, so the tests pin happy paths + the idempotency branches
(already-exists detection) that protect against double-provisioning
when a user re-runs the wizard.

All boto3 + Fastly API calls are mocked at the client boundary.
"""


import pytest
from botocore.exceptions import ClientError

from backend.provision import fos_setup

# ── _get_fos_s3_client ─────────────────────────────────────────────────────


def test_get_fos_s3_client_returns_raw_boto_when_no_service_id():
    """Without a service_id (e.g. the standalone CLI before a service
    is registered), the helper returns the bare boto3 client and
    skips telemetry. Pinned because wrapping a raw boto3 client in
    the tracker without a service_id would NPE on flush()."""
    with patch("boto3.client") as mock_boto:
        mock_boto.return_value = "raw-client"
        out = fos_setup._get_fos_s3_client("k", "s", "us-east-1")

    assert out == "raw-client"


def test_get_fos_s3_client_wraps_with_tracker_when_service_id_provided():
    """When a service_id is supplied (e.g. by the orchestrator), the
    boto3 client is wrapped in _ProvisioningTracker so every call
    lands in usage_log. Pinned because losing this wrapper was the
    bug that left provisioning ops invisible to dashboards."""
    with patch("boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        out = fos_setup._get_fos_s3_client("k", "s", "us-east-1", service_id="svc-1", bucket_name="my-bucket")

    assert isinstance(out, fos_setup._ProvisioningTracker)
    assert out._service_id == "svc-1"
    assert out._bucket_name == "my-bucket"


def test_get_fos_s3_client_uses_region_endpoint():
    """The endpoint URL is derived from ``region_endpoint(region)``,
    NOT hardcoded to us-east-1. Pinned because losing this would
    silently route eu-west-1 traffic to the US shard."""
    with (
        patch("boto3.client") as mock_boto,
        patch("backend.core.fastly.client.region_endpoint", return_value="eu-west-1.object.fastlystorage.app"),
    ):
        fos_setup._get_fos_s3_client("k", "s", "eu-west-1")

    kwargs = mock_boto.call_args.kwargs
    assert "eu-west-1.object.fastlystorage.app" in kwargs["endpoint_url"]
    assert kwargs["region_name"] == "eu-west-1"


# ── ensure_fos_bucket ──────────────────────────────────────────────────────


def test_ensure_fos_bucket_returns_true_when_bucket_exists():
    """``head_bucket`` succeeds → bucket exists → no create call.
    Pinned because re-creating would fail with BucketAlreadyExists
    and break the provisioning wizard on re-runs."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}  # 200 OK

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        out = fos_setup.ensure_fos_bucket("my-bucket", "us-east-1", "k", "s")

    assert out is True
    fake_s3.create_bucket.assert_not_called()


def test_ensure_fos_bucket_creates_when_head_raises_404():
    """``head_bucket`` raises (bucket missing) → call ``create_bucket``.
    Pinned because a refactor that re-throws on any ClientError would
    abort first-time provisioning."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket")

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        out = fos_setup.ensure_fos_bucket("new-bucket", "us-east-1", "k", "s")

    assert out is True
    fake_s3.create_bucket.assert_called_once_with(Bucket="new-bucket")


def test_ensure_fos_bucket_calls_status_callback_on_create():
    """When a ``status_cb`` is provided, the wizard surfaces "Creating
    bucket..." + "Bucket created" to the user. Pinned because losing
    these would make the provisioning UI silent during a long step."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")

    statuses: list[str] = []
    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s", status_cb=statuses.append)

    assert any("Creating bucket" in s for s in statuses)
    assert any("Bucket created" in s for s in statuses)


def test_ensure_fos_bucket_raises_runtime_error_on_create_failure():
    """A ClientError from ``create_bucket`` (perms denied, quota) →
    RuntimeError with the cause. Pinned because RuntimeError is what
    the wizard's outer try/except catches to render a clear error."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "perms"}}, "CreateBucket"
    )

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        with pytest.raises(RuntimeError, match="Could not create FOS bucket"):
            fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s")


# ── delete_fos_bucket ─────────────────────────────────────────────────────


def test_delete_fos_bucket_short_circuits_when_already_deleted():
    """head_bucket → 404 → no list_objects / delete_bucket calls.
    Pinned because the teardown is idempotent — running it twice
    must not error."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")  # must not raise

    fake_s3.delete_bucket.assert_not_called()


def test_delete_fos_bucket_empties_then_deletes_on_happy_path():
    """Bucket has objects → list_objects + delete_objects + delete_bucket.
    Pinned because skipping the empty step would surface as a
    BucketNotEmpty error after a few attempts."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]},
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {"Deleted": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]}

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    fake_s3.delete_objects.assert_called()
    fake_s3.delete_bucket.assert_called_with(Bucket="b")


def test_delete_fos_bucket_raises_after_max_retries_on_bucket_not_empty():
    """If ``delete_bucket`` repeatedly returns BucketNotEmpty (which
    means deleted-and-undeleted by S3 eventual consistency), the
    helper retries with backoff but eventually raises after 15
    attempts. Pinned because infinite retries would hang the wizard."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}
    fake_s3.get_paginator.return_value.paginate.return_value = []
    fake_s3.delete_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketNotEmpty", "Message": "x"}}, "DeleteBucket"
    )

    with (
        patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3),
        patch("time.sleep"),  # skip the retry backoff
    ):
        with pytest.raises(RuntimeError, match="Could not delete FOS bucket after 15 attempts"):
            fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    # All 15 attempts hit the API
    assert fake_s3.delete_bucket.call_count == 15


def test_delete_fos_bucket_raises_on_unexpected_client_error():
    """Non-BucketNotEmpty ClientError (perms denied) → immediate raise,
    no retry loop. Pinned because retrying through an AccessDenied is
    pointless and slows the failure."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}
    fake_s3.get_paginator.return_value.paginate.return_value = []
    fake_s3.delete_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "perms"}}, "DeleteBucket"
    )

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        with pytest.raises(RuntimeError):
            fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    # Only one attempt, not 15
    assert fake_s3.delete_bucket.call_count == 1


def test_delete_fos_bucket_aborts_multipart_uploads_before_listing_objects():
    """Multipart uploads must be aborted before object deletion, or
    they leak forever as zombie partial uploads (and accrue storage
    charges). Pinned because skipping this is a silent cost leak."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": [{"Key": "partial.gz", "UploadId": "upload-123"}]}
    fake_s3.get_paginator.return_value.paginate.return_value = []

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    fake_s3.abort_multipart_upload.assert_called_once_with(Bucket="b", Key="partial.gz", UploadId="upload-123")


def test_delete_fos_bucket_tolerates_multipart_listing_failure():
    """``list_multipart_uploads`` raises (perms gap) → swallow and
    continue. Pinned because some FOS keys don't have multipart
    perms even though they can list/delete objects."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "x"}}, "ListMultipartUploads"
    )
    fake_s3.get_paginator.return_value.paginate.return_value = []

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")  # must not raise

    fake_s3.delete_bucket.assert_called()


# ── find_fos_key ──────────────────────────────────────────────────────────


def test_find_fos_key_returns_matching_key():
    fake_keys = {
        "data": [
            {"access_key": "AK111", "description": "fos-log-analysis-svc1"},
            {"access_key": "AK222", "description": "manual-key"},
        ]
    }
    with patch("backend.provision.fos_setup.fastly", return_value=fake_keys):
        out = fos_setup.find_fos_key("manual-key", token="tkn")

    assert out["access_key"] == "AK222"


def test_find_fos_key_returns_none_when_no_match():
    with patch("backend.provision.fos_setup.fastly", return_value={"data": []}):
        assert fos_setup.find_fos_key("nonexistent", token="tkn") is None


def test_find_fos_key_swallows_runtime_error_from_fastly_api():
    """A RuntimeError from the Fastly API (rate-limited, 5xx) →
    return None. Pinned because raising here would crash the
    ``ensure_fos_access_key`` idempotency check."""
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("rate limit")):
        assert fos_setup.find_fos_key("any", token="tkn") is None


# ── ensure_fos_access_key ─────────────────────────────────────────────────


def test_ensure_fos_access_key_creates_new_key():
    """No existing key → POST /access-keys → return id+secret."""
    fake_resp = {"access_key": "AK999", "secret_key": "SK999"}
    with (
        patch("backend.provision.fos_setup.find_fos_key", return_value=None),
        patch("backend.provision.fos_setup.fastly", return_value=fake_resp) as mock_fastly,
    ):
        out = fos_setup.ensure_fos_access_key("fos-log-analysis-test", state={}, token="tkn", buckets=["b1"])

    assert out == {"id": "AK999", "access_key": "AK999", "secret_key": "SK999"}
    # The POST body must include both permission and buckets
    args, kwargs = mock_fastly.call_args[:2] if len(mock_fastly.call_args) > 1 else (mock_fastly.call_args[0], {})
    payload = mock_fastly.call_args[0][2] if len(mock_fastly.call_args[0]) > 2 else {}
    assert payload.get("permission") == "read-write-objects"
    assert payload.get("buckets") == ["b1"]


def test_ensure_fos_access_key_recreates_managed_key():
    """An existing key with our ``fos-log-analysis-*`` prefix is a
    leftover from a prior run → DELETE then re-POST. Pinned because
    losing this would cause the provisioning to fail on every
    re-run after a previous partial completion."""
    existing = {"access_key": "AK_OLD", "description": "fos-log-analysis-svc1"}
    new_key = {"access_key": "AK_NEW", "secret_key": "SK_NEW"}

    calls: list[tuple] = []

    def _fastly_spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path))
        if method == "POST":
            return new_key
        return {}

    with (
        patch("backend.provision.fos_setup.find_fos_key", return_value=existing),
        patch("backend.provision.fos_setup.fastly", side_effect=_fastly_spy),
    ):
        out = fos_setup.ensure_fos_access_key("fos-log-analysis-svc1", state={}, token="tkn")

    # DELETE happened first, then POST
    assert ("DELETE", "/resources/object-storage/access-keys/AK_OLD") in calls
    assert any(call[0] == "POST" for call in calls)
    assert out["access_key"] == "AK_NEW"


def test_ensure_fos_access_key_raises_on_unmanaged_existing_key():
    """If an existing key has a description NOT starting with
    ``fos-log-analysis-``, it was created manually → don't delete it,
    raise instead. Pinned because clobbering a customer's hand-rolled
    key would be a data-loss bug."""
    existing = {"access_key": "AK_MANUAL", "description": "my-prod-key"}

    with patch("backend.provision.fos_setup.find_fos_key", return_value=existing):
        with pytest.raises(RuntimeError, match="not managed by this tool"):
            fos_setup.ensure_fos_access_key("my-prod-key", state={}, token="tkn")


def test_ensure_fos_access_key_omits_buckets_when_none_supplied():
    """``buckets=None`` → no ``buckets`` key in the POST payload (so
    the key gets account-wide perms). Pinned because sending
    ``"buckets": null`` would trip Fastly's schema validation."""
    fake_resp = {"access_key": "AK", "secret_key": "SK"}
    with (
        patch("backend.provision.fos_setup.find_fos_key", return_value=None),
        patch("backend.provision.fos_setup.fastly", return_value=fake_resp) as mock_fastly,
    ):
        fos_setup.ensure_fos_access_key("d", state={}, token="tkn", buckets=None)

    payload = mock_fastly.call_args[0][2]
    assert "buckets" not in payload


# ── delete_fos_access_key ─────────────────────────────────────────────────


def test_delete_fos_access_key_calls_delete_endpoint():
    with patch("backend.provision.fos_setup.fastly") as mock_fastly:
        fos_setup.delete_fos_access_key("AK123", token="tkn")

    args, kwargs = mock_fastly.call_args[0], mock_fastly.call_args[1]
    assert args[0] == "DELETE"
    assert "AK123" in args[1]


def test_delete_fos_access_key_treats_404_as_already_deleted():
    """404 from the DELETE is idempotency-friendly — treat as success.
    Pinned because the teardown CLI may run twice (manual retry); the
    second run must not error on a missing key."""
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("404 Not Found")):
        fos_setup.delete_fos_access_key("AK123", token="tkn")  # must not raise


def test_delete_fos_access_key_raises_on_other_errors():
    """Non-404 RuntimeError → re-raise. Pinned distinct from the
    404-tolerant path so transient 5xx errors aren't silently
    swallowed (the teardown would think it succeeded and clean up
    state pointing to a still-existing key)."""
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("500 Internal")):
        with pytest.raises(RuntimeError):
            fos_setup.delete_fos_access_key("AK123", token="tkn")


def test_ensure_fos_access_key_emits_status_callbacks_on_recreate():
    """When recreating a managed key, status_cb receives "Recreating
    existing key..." then "Creating ... access key..." then
    "Access key created.". Pinned because the SSE wizard renders
    these as user-visible progress steps — losing them would leave
    the user staring at a stalled progress bar."""
    statuses = []
    fastly_calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        fastly_calls.append((method, path))
        if method == "POST" and "/access-keys" in path:
            return {"access_key": "AKNEW", "secret_key": "SKNEW", "id": "AKNEW"}
        return {}

    with (
        patch(
            "backend.provision.fos_setup.find_fos_key",
            return_value={"access_key": "AKEXISTING"},
        ),
        patch("backend.provision.fos_setup.fastly", side_effect=fake_fastly),
    ):
        result = fos_setup.ensure_fos_access_key(
            "fos-log-analysis-svc-1",
            state={},
            token="tok",
            permission="read-write-objects",
            buckets=["b"],
            status_cb=statuses.append,
        )

    # Status callbacks fired in expected order
    joined = " ".join(statuses)
    assert "Recreating" in joined
    # The existing key was DELETEd then a new POST happened
    assert any(m == "DELETE" for m, _ in fastly_calls)
    assert any(m == "POST" for m, _ in fastly_calls)
    assert result["access_key"] == "AKNEW"


# ── provisioning capture (writes to usage_log) ────────────────────────────


def test_provisioning_tracker_records_create_bucket_to_usage_log():
    """The headline pin: when service_id is supplied, every S3 call
    made during ``ensure_fos_bucket`` lands in usage_log via
    ``metadata_db.log_usage_calls``. Pinned because provisioning ops
    were silently dropped pre-fix — the wrapper did not exist, so
    head_bucket / create_bucket attempts never appeared in any
    dashboard, hiding a class of cost from the operator."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        fos_setup.ensure_fos_bucket("new-bucket", "us-east-1", "k", "s", service_id="svc-x")

    assert mock_log.called, "log_usage_calls must be invoked on flush"
    args, kwargs = mock_log.call_args
    assert args[0] == "svc-x"
    methods = [c["method"] for c in args[1]]
    # Both the existence probe and the creation land as rows
    assert "HEAD_BUCKET" in methods
    assert "CREATE_BUCKET" in methods
    assert kwargs["process_context"] == "provision:new-bucket"


def test_provisioning_tracker_marks_create_bucket_as_class_a_in_details():
    """``log_usage_calls`` auto-classifies as Class B by default; the
    "Class A" sentinel in details is the documented escape hatch.
    Pinned because miss-classifying CreateBucket as Class B would
    under-count A-class billable ops for every provisioning run."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.return_value = {}

    captured: list[dict] = []

    def _capture(_sid, calls, **_):
        captured.extend(calls)

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s", service_id="svc-x")

    create_row = next(c for c in captured if c["method"] == "CREATE_BUCKET")
    assert "Class A" in create_row["details"]
    head_row = next(c for c in captured if c["method"] == "HEAD_BUCKET")
    assert "Class A" not in head_row["details"]  # HEAD is Class B


def test_provisioning_tracker_records_errors_as_rows_too():
    """A ClientError on create_bucket still results in a usage_log
    row (status="Error:<code>"). Pinned because failed provisioning
    attempts also incur API charges — pretending they didn't happen
    would under-count cost."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "CreateBucket"
    )

    captured: list[dict] = []

    def _capture(_sid, calls, **_):
        captured.extend(calls)

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        with pytest.raises(RuntimeError):
            fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s", service_id="svc-x")

    create_row = next(c for c in captured if c["method"] == "CREATE_BUCKET")
    assert "Error" in str(create_row["status"])
    assert "AccessDenied" in str(create_row["status"])


def test_provisioning_tracker_tags_teardown_context_separately_from_provision():
    """delete_fos_bucket flushes with process_context="teardown:<name>"
    so dashboards can distinguish setup cost from teardown cost.
    Pinned because mixing the two would hide the teardown-Class-A
    spike that happens when emptying a large bucket."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}
    fake_s3.get_paginator.return_value.paginate.return_value = []
    fake_s3.delete_bucket.return_value = {}

    contexts: list[str] = []

    def _capture(_sid, _calls, **kwargs):
        contexts.append(kwargs.get("process_context"))

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        fos_setup.delete_fos_bucket("doomed-bucket", "us-east-1", "k", "s", service_id="svc-x")

    assert "teardown:doomed-bucket" in contexts


def test_provisioning_tracker_skips_logging_when_no_service_id():
    """Standalone CLI runs without a service_id (e.g. the bootstrap
    wizard runs ensure_fos_bucket before logging_service_id is even
    chosen). The wrapper must not be applied and log_usage_calls
    must not be called. Pinned because a NameError trying to flush
    to a missing service file would crash the CLI."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s")  # no service_id

    mock_log.assert_not_called()


def test_provisioning_tracker_records_delete_objects_pagination():
    """delete_fos_bucket calls list_objects_v2 via paginator and
    delete_objects from a ThreadPoolExecutor. Both must land in
    usage_log. Pinned because a large-bucket teardown can issue
    thousands of Class A delete_objects calls — losing them would
    hide a major cost spike on every teardown."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]},
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {"Deleted": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]}
    fake_s3.delete_bucket.return_value = {}

    captured: list[dict] = []

    def _capture(_sid, calls, **_):
        captured.extend(calls)

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        fos_setup.delete_fos_bucket("doomed-bucket", "us-east-1", "k", "s", service_id="svc-x")

    methods = [c["method"] for c in captured]
    assert "LIST_OBJECTS_V2" in methods
    assert "DELETE_OBJECTS" in methods
    assert "DELETE_BUCKET" in methods


def test_ensure_fos_bucket_calls_status_callback_on_already_exists():
    """When the bucket already exists (head_bucket succeeds), the
    status_cb receives an "already exists" message. Pinned because
    the wizard's idempotent re-run UX depends on this distinction
    vs the "creating..." message admins see on first run."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}  # success

    statuses = []

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        result = fos_setup.ensure_fos_bucket(
            "my-bucket",
            "us-east-1",
            "AK",
            "SK",
            status_cb=statuses.append,
        )

    assert result is True
    # create_bucket was NOT called (idempotency)
    fake_s3.create_bucket.assert_not_called()
    # Status callback got the "already exists" signal
    assert any("already exists" in s.lower() for s in statuses)


def test_delete_fos_prefix_happy_path():
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "raw/log1.gz"}, {"Key": "raw/log2.gz"}]},
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {}

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_prefix("my-bucket", "us-east-1", "k", "s", "raw/")

    fake_s3.delete_objects.assert_called_once_with(
        Bucket="my-bucket", Delete={"Objects": [{"Key": "raw/log1.gz"}, {"Key": "raw/log2.gz"}]}
    )


def test_delete_fos_prefix_excludes_matching():
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw/log1.gz"},
                {"Key": "raw/rum/log2.gz"},
                {"Key": "raw/log3.gz"},
            ]
        },
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {}

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_prefix("my-bucket", "us-east-1", "k", "s", "raw/", exclude_prefix="raw/rum/")

    fake_s3.delete_objects.assert_called_once_with(
        Bucket="my-bucket", Delete={"Objects": [{"Key": "raw/log1.gz"}, {"Key": "raw/log3.gz"}]}
    )
