"""Recovery capacity gate wiring: rebuild functions and the backfill CLI."""

from __future__ import annotations

import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from structlog.testing import capture_logs

from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.publication import ClickHousePublication, InMemoryBatchManifestStore
from backend.high_scale.recovery import (
    RecoveryCapacityMeasurement,
    rebuild_origin_projections_from_fos,
    rebuild_serving_state_from_fos,
)
from tests.high_scale.test_recovery import NOW, ReplayClient, _Catalog, _manifest


def _measurement(**overrides) -> RecoveryCapacityMeasurement:
    values = {
        "events_per_second": 2_000_000,
        "recovery_window_seconds": 15 * 60,
        "archive_read_events_per_second": 300_000_000,
        "decode_events_per_second": 300_000_000,
        "aggregate_events_per_second": 300_000_000,
        "clickhouse_insert_events_per_second": 750_000_000,
        "replication_factor": 3,
        "live_ingest_reservation": 0.2,
    }
    values.update(overrides)
    return RecoveryCapacityMeasurement(**values)


def _recovery_env(*, fail_on: set[str] | None = None):
    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    warm, warm_bytes = _manifest(
        "warm", NOW - timedelta(days=10), NOW - timedelta(days=10) + timedelta(minutes=1), ({"event_id": "2"},)
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(recent, recent_bytes)
    archive.publish(warm, warm_bytes)
    client = ReplayClient(fail_on=fail_on)
    publication = ClickHousePublication(InMemoryBatchManifestStore(), client)
    return _Catalog((recent, warm)), archive, publication, client


def _rebuild(env, **kwargs):
    catalog, archive, publication, _ = env
    return rebuild_serving_state_from_fos(
        catalog, archive, publication, service_id="svc", domain="request", now=NOW, **kwargs
    )


def test_feasible_measurement_with_a_complete_rebuild_makes_the_claim_valid() -> None:
    with capture_logs() as logs:
        report = _rebuild(_recovery_env(), capacity=_measurement())

    assert report.full_rebuild_complete is True
    assert report.capacity is not None and report.capacity.feasible is True
    assert report.recovery_claim_valid is True
    gate = next(e for e in logs if e["event"] == "high_scale.recovery_capacity_gate")
    assert gate["feasible"] is True and gate["log_level"] == "info"


def test_infeasible_measurement_flags_the_claim_but_never_blocks_recovery() -> None:
    env = _recovery_env()

    with capture_logs() as logs:
        report = _rebuild(env, capacity=_measurement(clickhouse_insert_events_per_second=400_000_000))

    assert report.full_rebuild_complete is True
    assert report.rows_replayed == 2
    assert len(env[3].insert_calls) == 2
    assert report.capacity is not None and report.capacity.limiting_stage == "clickhouse_insert"
    assert report.recovery_claim_valid is False
    gate = next(e for e in logs if e["event"] == "high_scale.recovery_capacity_gate")
    assert gate["log_level"] == "warning" and gate["limiting_stage"] == "clickhouse_insert"


def test_feasible_capacity_does_not_validate_an_incomplete_rebuild() -> None:
    report = _rebuild(_recovery_env(fail_on={"warm"}), capacity=_measurement())

    assert report.capacity is not None and report.capacity.feasible is True
    assert report.full_rebuild_complete is False
    assert report.recovery_claim_valid is False


def test_without_a_measurement_the_report_carries_no_claim() -> None:
    with capture_logs() as logs:
        report = _rebuild(_recovery_env())

    assert report.capacity is None
    assert report.recovery_claim_valid is None
    assert not [e for e in logs if e["event"] == "high_scale.recovery_capacity_gate"]


def test_invalid_measurement_fails_before_any_replay_work() -> None:
    env = _recovery_env()

    with pytest.raises(ValueError, match="event rate and recovery window must be positive"):
        _rebuild(env, capacity=_measurement(events_per_second=0))

    assert env[3].insert_calls == []


def test_origin_projection_rebuild_evaluates_the_same_gate() -> None:
    catalog, archive, publication, _ = _recovery_env()

    report = rebuild_origin_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id="svc",
        now=NOW,
        window=timedelta(hours=2),
        capacity=_measurement(decode_events_per_second=1_000),
    )

    assert report.capacity is not None and report.capacity.limiting_stage == "decode"
    assert report.recovery_claim_valid is False


# ── CLI: scripts/backfill_high_scale_origin.py --capacity-json ──────────────


@pytest.fixture
def cli(monkeypatch: pytest.MonkeyPatch):
    from scripts import backfill_high_scale_origin as script

    service = SimpleNamespace(manifest_catalog=object(), archive=object(), client=MagicMock())
    registry = MagicMock()
    registry.resolve.return_value = service
    monkeypatch.setattr(script.app_config, "load_clickhouse_config", lambda: object())
    monkeypatch.setattr(script, "ClickHouseClient", lambda settings: MagicMock())
    monkeypatch.setattr(script, "get_high_scale_service_registry", lambda: registry)
    monkeypatch.setattr(script, "register_high_scale_services_from_environment", lambda *a, **k: None)
    monkeypatch.setattr(script, "PostgresControlPlane", MagicMock)
    monkeypatch.setattr(script, "PostgresBatchManifestStore", lambda control: MagicMock())
    monkeypatch.setattr(script, "AggregateBatchAdapter", lambda client: MagicMock())
    monkeypatch.setattr(script, "ClickHousePublication", lambda manifests, adapter: MagicMock())
    return script


def _run_cli(cli, monkeypatch, tmp_path, measurement: dict | None, *, complete: bool = True):
    seen: dict = {}

    def fake_rebuild(*args, **kwargs):
        seen.update(kwargs)
        capacity = kwargs.get("capacity")
        from backend.high_scale.recovery import RecoveryReport, evaluate_recovery_capacity

        report = evaluate_recovery_capacity(capacity) if capacity else None
        return RecoveryReport(
            True, complete, True, complete, complete, 1, 1, (), "rb-1",
            capacity=report,
            recovery_claim_valid=None if report is None else report.feasible and complete,
        )  # fmt: skip

    monkeypatch.setattr(cli, "rebuild_origin_projections_from_fos", fake_rebuild)
    argv = ["backfill", "--service-id", "svc"]
    if measurement is not None:
        path = tmp_path / "capacity.json"
        path.write_text(json.dumps(measurement))
        argv += ["--capacity-json", str(path)]
    monkeypatch.setattr("sys.argv", argv)
    return cli.main(), seen


_GOOD = {
    "events_per_second": 2_000_000,
    "recovery_window_seconds": 900,
    "archive_read_events_per_second": 300_000_000,
    "decode_events_per_second": 300_000_000,
    "aggregate_events_per_second": 300_000_000,
    "clickhouse_insert_events_per_second": 750_000_000,
    "replication_factor": 3,
    "live_ingest_reservation": 0.2,
}


def test_cli_passes_the_measurement_through_and_exits_zero_when_valid(cli, monkeypatch, tmp_path) -> None:
    code, seen = _run_cli(cli, monkeypatch, tmp_path, _GOOD)

    assert code == 0
    assert seen["capacity"] == RecoveryCapacityMeasurement(**_GOOD)


def test_cli_exits_three_when_the_rebuild_completes_but_the_claim_is_invalid(cli, monkeypatch, tmp_path) -> None:
    code, _ = _run_cli(cli, monkeypatch, tmp_path, {**_GOOD, "decode_events_per_second": 1_000})

    assert code == 3


def test_cli_without_a_measurement_keeps_the_previous_exit_codes(cli, monkeypatch, tmp_path) -> None:
    assert _run_cli(cli, monkeypatch, tmp_path, None)[0] == 0
    assert _run_cli(cli, monkeypatch, tmp_path, None, complete=False)[0] == 1


def test_cli_rejects_a_malformed_measurement_file(cli, monkeypatch, tmp_path) -> None:
    with pytest.raises(SystemExit) as excinfo:
        _run_cli(cli, monkeypatch, tmp_path, {**_GOOD, "events_per_second": 0})

    assert excinfo.value.code == 2


def test_cli_replace_existing_purges_origin_rows_before_the_rebuild(cli, monkeypatch, tmp_path) -> None:
    order: list[str] = []
    monkeypatch.setattr(
        cli, "purge_origin_projections", lambda client, **kwargs: order.append(f"purge:{kwargs['service_id']}")
    )

    def fake_rebuild(*args, **kwargs):
        order.append("rebuild")
        from backend.high_scale.recovery import RecoveryReport

        return RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1")

    monkeypatch.setattr(cli, "rebuild_origin_projections_from_fos", fake_rebuild)
    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--replace-existing"])

    assert cli.main() == 0
    assert order == ["purge:svc", "rebuild"]


def test_cli_without_replace_existing_never_purges(cli, monkeypatch, tmp_path) -> None:
    purged: list[str] = []
    monkeypatch.setattr(cli, "purge_origin_projections", lambda client, **kwargs: purged.append("purge"))

    _run_cli(cli, monkeypatch, tmp_path, None)

    assert purged == []


def test_cli_rejects_replace_existing_with_a_resumed_rebuild_id(cli, monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--replace-existing", "--rebuild-id", "rb-1"])

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    assert excinfo.value.code == 2


def test_cli_projection_cmcd_rebuilds_through_the_facts_adapter_and_purges_cmcd_rows(cli, monkeypatch) -> None:
    calls: list[str] = []
    adapters: list[str] = []
    monkeypatch.setattr(cli, "ClickHouseBatchAdapter", lambda client: adapters.append("facts") or MagicMock())
    monkeypatch.setattr(cli, "AggregateBatchAdapter", lambda client: adapters.append("aggregate") or MagicMock())
    monkeypatch.setattr(cli, "purge_cmcd_projections", lambda client, **kwargs: calls.append("purge-cmcd"))
    monkeypatch.setattr(cli, "purge_origin_projections", lambda client, **kwargs: calls.append("purge-origin"))

    def fake_cmcd_rebuild(*args, **kwargs):
        calls.append("rebuild-cmcd")
        from backend.high_scale.recovery import RecoveryReport

        return RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1")

    monkeypatch.setattr(cli, "rebuild_cmcd_projections_from_fos", fake_cmcd_rebuild)
    monkeypatch.setattr(
        cli,
        "rebuild_origin_projections_from_fos",
        lambda *a, **k: pytest.fail("origin rebuild must not run for --projection cmcd"),
    )
    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--projection", "cmcd", "--replace-existing"])

    assert cli.main() == 0
    assert calls == ["purge-cmcd", "rebuild-cmcd"]
    assert adapters == ["facts"]


def test_cli_projection_rum_replays_the_rum_domain_through_the_facts_adapter(cli, monkeypatch) -> None:
    seen: dict = {}
    adapters: list[str] = []
    monkeypatch.setattr(cli, "ClickHouseBatchAdapter", lambda client: adapters.append("facts") or MagicMock())
    monkeypatch.setattr(cli, "purge_rum_facts", lambda client, **kwargs: seen.setdefault("purge", kwargs))

    def fake_rum_rebuild(*args, **kwargs):
        seen["rebuild"] = kwargs
        from backend.high_scale.recovery import RecoveryReport

        return RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1")

    monkeypatch.setattr(cli, "rebuild_rum_facts_from_fos", fake_rum_rebuild)
    monkeypatch.setattr(
        "sys.argv", ["backfill", "--service-id", "svc", "--projection", "rum_errors", "--replace-existing"]
    )

    assert cli.main() == 0
    assert adapters == ["facts"]
    assert seen["purge"]["domain"] == "rum_errors" and seen["purge"]["service_id"] == "svc"
    assert seen["rebuild"]["domain"] == "rum_errors" and seen["rebuild"]["service_id"] == "svc"
