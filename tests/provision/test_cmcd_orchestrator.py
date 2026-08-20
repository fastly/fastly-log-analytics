"""Coverage for the CMCD enable/disable orchestrators.

This module was at 0% coverage across 152 statements while carrying the
clone -> mutate -> validate -> activate lifecycle for CMCD collection — the
exact path whose bugs caused the 2026-08 CMCD outage (fields stripped from the
log format, extraction ordered after capture). Its rollback path in particular
had never been exercised.

Everything is mocked at the Fastly HTTP boundary and the config layer, so these
run without credentials or network.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from backend.provision import cmcd_orchestrator as orch
from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES

SID = "TestLogSvcABC123"
TOKEN = "FAKE_TOKEN"


def _cfg(**over):
    base = {
        "logging_service_id": SID,
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
        "log_fields": {"schema_version": 2, "groups": ["A", "B"], "custom_fields": []},
    }
    base.update(over)
    return base


class _FastlyRecorder:
    """Records calls and answers the shapes the orchestrator depends on."""

    def __init__(self, *, validate_ok=True, clone_number=7):
        self.calls: list[tuple[str, str]] = []
        self.validate_ok = validate_ok
        self.clone_number = clone_number

    def __call__(self, method, path, body=None, **kwargs):
        self.calls.append((method, path))
        if "/clone" in path:
            return {"number": self.clone_number}
        if "/validate" in path:
            return {"status": "ok"} if self.validate_ok else {"status": "error", "errors": ["boom"]}
        return {}

    def paths(self, method=None):
        return [p for m, p in self.calls if method is None or m == method]


def _patches(saved, fake, *, endpoints=("Fastly Object Storage Logs",), snippets=None, cfg=None):
    """Patch every seam enable/disable_cmcd reaches out through."""
    return (
        patch.object(orch.svcconfig, "load_config", return_value=cfg if cfg is not None else _cfg()),
        patch.object(orch.svcconfig, "save_config", side_effect=lambda sid, c: saved.append(c)),
        patch.object(orch, "fastly", side_effect=fake),
        patch.object(orch, "get_active_version", return_value=5),
        patch.object(orch, "ensure_vcl_snippet"),
        patch.object(orch, "list_s3_endpoints", return_value=list(endpoints)),
        patch.object(orch, "list_vcl_snippets", return_value=list(snippets or [])),
        patch("backend.provision.fastly_api.install_capture_snippets"),
        patch("backend.provision.fastly_api.load_log_format", return_value='{"timestamp":"x"}'),
        patch("backend.state_sync.export_admin_state"),
    )


# ── enable_cmcd ──────────────────────────────────────────────────────────────


def _run_enable(saved, fake, **kw):
    ctx = _patches(saved, fake, **{k: v for k, v in kw.items() if k in ("endpoints", "snippets", "cfg")})
    from contextlib import ExitStack

    with ExitStack() as stack:
        for c in ctx:
            stack.enter_context(c)
        return orch.enable_cmcd(
            SID,
            TOKEN,
            mode=kw.get("mode", "query_string"),
            version=kw.get("version", 1),
            status_cb=kw.get("status_cb"),
        )


def _run_disable(saved, fake, **kw):
    ctx = _patches(saved, fake, **{k: v for k, v in kw.items() if k in ("endpoints", "snippets", "cfg")})
    from contextlib import ExitStack

    with ExitStack() as stack:
        for c in ctx:
            stack.enter_context(c)
        return orch.disable_cmcd(SID, TOKEN, status_cb=kw.get("status_cb"))


def test_enable_persists_cmcd_block_and_fields():
    saved: list[dict] = []
    fake = _FastlyRecorder()
    result = _run_enable(saved, fake)

    assert result == {
        "enabled": True,
        "mode": "query_string",
        "version": 1,
        "logging_service_active_version": 7,
    }
    cfg = saved[0]
    assert cfg["cmcd"]["enabled"] is True
    assert cfg["cmcd"]["mode"] == "query_string"
    assert cfg["cmcd"]["version"] == 1
    assert cfg["cmcd"]["enabled_at"]
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert _CMCD_FIELD_NAMES <= names, "all 14 CMCD fields must be persisted"


def test_enable_clones_validates_and_activates():
    saved: list[dict] = []
    fake = _FastlyRecorder()
    _run_enable(saved, fake)

    assert any("/version/5/clone" in p for p in fake.paths("PUT"))
    assert any("/validate" in p for p in fake.paths("GET"))
    assert any("/version/7/activate" in p for p in fake.paths("PUT"))


def test_enable_pushes_new_log_format_to_the_endpoint():
    """The endpoint PUT is what actually makes the edge emit cmcd_* fields."""
    saved: list[dict] = []
    fake = _FastlyRecorder()
    _run_enable(saved, fake)
    assert any("/logging/s3/" in p for p in fake.paths("PUT")), "log format was never pushed"


def test_enable_skips_endpoint_update_when_endpoint_absent():
    saved: list[dict] = []
    fake = _FastlyRecorder()
    _run_enable(saved, fake, endpoints=())
    assert not any("/logging/s3/" in p for p in fake.paths("PUT"))


@pytest.mark.parametrize("bad", [0, 3, -1, 99])
def test_enable_rejects_unknown_version(bad):
    with pytest.raises(ValueError, match="Unknown CMCD version"):
        orch.enable_cmcd(SID, TOKEN, version=bad)


def test_enable_raises_when_no_config():
    with patch.object(orch.svcconfig, "load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            orch.enable_cmcd(SID, TOKEN)


def test_enable_raises_when_no_active_version():
    saved: list[dict] = []
    with (
        patch.object(orch.svcconfig, "load_config", return_value=_cfg()),
        patch.object(orch.svcconfig, "save_config", side_effect=lambda s, c: saved.append(c)),
        patch.object(orch, "get_active_version", return_value=None),
    ):
        with pytest.raises(RuntimeError, match="no active version"):
            orch.enable_cmcd(SID, TOKEN)


def test_enable_rolls_back_on_validation_failure():
    """THE UNTESTED PATH: a failed validate must reactivate the old version and
    un-persist the CMCD config + fields, so a half-enabled state can't linger."""
    saved: list[dict] = []
    fake = _FastlyRecorder(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        _run_enable(saved, fake)

    # Old version reactivated.
    assert any("/version/5/activate" in p for p in fake.paths("PUT"))
    # Final persisted config has CMCD backed out entirely.
    final = saved[-1]
    assert "cmcd" not in final
    remaining = {c["name"] for c in final.get("log_fields", {}).get("custom_fields", [])}
    assert not (_CMCD_FIELD_NAMES & remaining), "CMCD fields survived the rollback"


def test_enable_reports_progress_via_status_cb():
    saved: list[dict] = []
    msgs: list[str] = []
    _run_enable(saved, fake=_FastlyRecorder(), status_cb=msgs.append)
    joined = " ".join(msgs).lower()
    assert "enabling cmcd" in joined
    assert any("activat" in m.lower() for m in msgs)


def test_enable_v2_mode_headers_propagates():
    saved: list[dict] = []
    fake = _FastlyRecorder()
    with patch.object(orch, "generate_cmcd_vcl", wraps=orch.generate_cmcd_vcl) as spy:
        res = _run_enable(saved, fake, mode="headers", version=2)
    spy.assert_called_once_with(mode="headers", version=2)
    assert res["mode"] == "headers"
    assert res["version"] == 2


# ── disable_cmcd ─────────────────────────────────────────────────────────────


def test_disable_is_noop_when_already_disabled():
    saved: list[dict] = []
    fake = _FastlyRecorder()
    msgs: list[str] = []
    _run_disable(saved, fake, cfg=_cfg(), status_cb=msgs.append)  # no cmcd block
    assert fake.calls == [], "must not touch Fastly when CMCD was never enabled"
    assert any("already disabled" in m.lower() for m in msgs)


def test_disable_removes_snippets_fields_and_activates():
    saved: list[dict] = []
    fake = _FastlyRecorder()
    cfg = _cfg(cmcd={"enabled": True, "mode": "query_string", "version": 1})
    cfg["log_fields"]["custom_fields"] = [
        {"name": "cmcd_sid", "duckdb_type": "VARCHAR"},
        {"name": "my_custom", "duckdb_type": "VARCHAR"},
    ]
    _run_disable(saved, fake, cfg=cfg, snippets=["CMCD Extraction"])

    assert any("/snippet/" in p for p in fake.paths("DELETE")), "CMCD snippet not deleted"
    assert any("/version/7/activate" in p for p in fake.paths("PUT"))
    final = saved[-1]
    assert "cmcd" not in final
    names = {c["name"] for c in final["log_fields"]["custom_fields"]}
    assert "cmcd_sid" not in names
    assert "my_custom" in names, "user custom_field wrongly removed"


def test_disable_raises_when_no_config():
    with patch.object(orch.svcconfig, "load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            orch.disable_cmcd(SID, TOKEN)


def test_disable_raises_when_no_active_version():
    cfg = _cfg(cmcd={"enabled": True})
    with (
        patch.object(orch.svcconfig, "load_config", return_value=cfg),
        patch.object(orch.svcconfig, "save_config"),
        patch.object(orch, "get_active_version", return_value=None),
    ):
        with pytest.raises(RuntimeError, match="no active version"):
            orch.disable_cmcd(SID, TOKEN)


def test_disable_rolls_back_on_validation_failure():
    saved: list[dict] = []
    fake = _FastlyRecorder(validate_ok=False)
    cfg = _cfg(cmcd={"enabled": True, "mode": "query_string", "version": 1})
    with pytest.raises(RuntimeError, match="Validation failed"):
        _run_disable(saved, fake, cfg=cfg, snippets=["CMCD Extraction"])
    assert any("/version/5/activate" in p for p in fake.paths("PUT")), "old version not reactivated"


def test_disable_skips_absent_snippet():
    saved: list[dict] = []
    fake = _FastlyRecorder()
    cfg = _cfg(cmcd={"enabled": True})
    _run_disable(saved, fake, cfg=cfg, snippets=[])
    assert not any("/snippet/" in p for p in fake.paths("DELETE"))


def test_disable_tolerates_export_admin_state_failure():
    """admin_state export is best-effort — it must not fail the disable."""
    saved: list[dict] = []
    fake = _FastlyRecorder()
    cfg = _cfg(cmcd={"enabled": True})
    from contextlib import ExitStack

    ctx = _patches(saved, fake, cfg=cfg, snippets=["CMCD Extraction"])
    with ExitStack() as stack:
        for c in ctx[:-1]:
            stack.enter_context(c)
        stack.enter_context(patch("backend.state_sync.export_admin_state", side_effect=RuntimeError("FOS down")))
        orch.disable_cmcd(SID, TOKEN)
    assert "cmcd" not in saved[-1]


# ── field helpers ────────────────────────────────────────────────────────────


def test_add_then_remove_custom_fields_round_trip():
    cfg = {"log_fields": {"custom_fields": [{"name": "mine"}]}}
    orch._add_cmcd_custom_fields(cfg)
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert _CMCD_FIELD_NAMES <= names and "mine" in names

    orch._remove_cmcd_custom_fields(cfg)
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert not (_CMCD_FIELD_NAMES & names)
    assert "mine" in names


def test_add_custom_fields_creates_log_fields_when_absent():
    cfg: dict = {}
    orch._add_cmcd_custom_fields(cfg)
    assert _CMCD_FIELD_NAMES <= {c["name"] for c in cfg["log_fields"]["custom_fields"]}


def test_remove_custom_fields_tolerates_missing_blocks():
    for cfg in ({}, {"log_fields": {}}, {"log_fields": {"custom_fields": []}}):
        orch._remove_cmcd_custom_fields(cfg)  # must not raise
