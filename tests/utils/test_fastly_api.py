"""Consolidated tests for test_fastly_api."""

from __future__ import annotations

import json
import urllib.parse
from unittest.mock import MagicMock, patch

import pytest
from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES, cmcd_snippet_names
from backend.provision.session_scoring_vcl import scoring_snippet_names

from backend import config as svcconfig
from backend.provision import fastly_api
from backend.provision import session_scoring_orchestrator as sso
from backend.provision.declarative.reconciler import ReconciliationResult
from backend.provision.fastly_api import (
    _log_sampling_edge_clause,
    generate_capture_vcl,
    get_capture_vcl_statements,
    get_scrub_vcl_statements,
    load_log_format,
)

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
        patch("backend.core.fastly.service.get_active_version", return_value=active_version),
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
        patch("backend.core.fastly.service.get_active_version", return_value=10),
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
from backend.core.fastly.utils import load_vcl, parse_period, region_endpoint


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

from backend.core.fastly.mock_fixtures import is_mock_mode, mock_response
from backend.utils.ngwaf import fetch_verified_bots_paged

from backend.core.fastly.client import fastly


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

from backend.provision import utils as pu

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
