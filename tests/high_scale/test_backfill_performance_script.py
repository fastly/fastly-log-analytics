from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from backend.high_scale.recovery import RecoveryReport


@pytest.fixture
def script(monkeypatch: pytest.MonkeyPatch):
    from scripts import backfill_high_scale_performance as module

    service = SimpleNamespace(manifest_catalog=object(), archive=object(), client=MagicMock())
    registry = MagicMock()
    registry.resolve.return_value = service
    monkeypatch.setattr(module.app_config, "load_clickhouse_config", lambda: object())
    monkeypatch.setattr(module, "ClickHouseClient", lambda settings: MagicMock())
    monkeypatch.setattr(module, "get_high_scale_service_registry", lambda: registry)
    monkeypatch.setattr(module, "register_high_scale_services_from_environment", lambda *a, **k: None)
    monkeypatch.setattr(module, "PostgresControlPlane", MagicMock)
    monkeypatch.setattr(module, "PostgresBatchManifestStore", lambda control: MagicMock())
    monkeypatch.setattr(module, "AggregateBatchAdapter", lambda client: MagicMock())
    monkeypatch.setattr(module, "ClickHousePublication", lambda manifests, adapter: MagicMock())
    return module


def _run(script, monkeypatch, argv: list[str], *, complete: bool = True):
    seen: dict = {}

    def fake_rebuild(*args, **kwargs):
        seen.update(kwargs)
        return RecoveryReport(True, complete, True, complete, complete, 2, 5, (), "rb-1")

    monkeypatch.setattr(script, "rebuild_performance_projections_from_fos", fake_rebuild)
    monkeypatch.setattr("sys.argv", ["backfill", *argv])
    return script.main(), seen


def test_until_caps_the_window_at_the_worker_deploy_time(script, monkeypatch, capsys) -> None:
    code, seen = _run(script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z", "--days", "7"])

    assert code == 0
    assert seen["now"] == datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
    assert seen["window"] == timedelta(days=7)
    assert seen["service_id"] == "svc"
    assert '"rebuild_id": "rb-1"' in capsys.readouterr().out


def test_incomplete_rebuild_exits_nonzero(script, monkeypatch) -> None:
    assert _run(script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z"], complete=False)[0] == 1


def test_until_is_required_and_must_carry_an_offset(script, monkeypatch) -> None:
    with pytest.raises(SystemExit) as missing:
        _run(script, monkeypatch, ["--service-id", "svc"])
    with pytest.raises(SystemExit) as naive:
        _run(script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00"])

    assert missing.value.code == 2
    assert naive.value.code == 2
