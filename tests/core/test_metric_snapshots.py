"""Tests for backend.core.metric_snapshots."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from backend.core import metric_snapshots
from backend.utils.date_utils import iso_z


def _get_history(
    metric: str,
    *,
    since: datetime | str,
    service_id: str | None = None,
    task: str | None = None,
) -> list[dict]:
    batch = metric_snapshots.get_batch(since=since)
    parts = [metric]
    if service_id:
        parts.append(service_id)
    if task:
        parts.append(task)
    return batch.get("|".join(parts), [])


class _FakePgCursor:
    def __init__(self, rows=(), rowcount=0):
        self._rows = list(rows)
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakePgConnection:
    def __init__(self, rows=(), rowcount=0):
        self.calls = []
        self._rows = rows
        self._rowcount = rowcount

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        return _FakePgCursor(self._rows, self._rowcount)

    def close(self):
        pass


def test_record_and_get_global_metric_roundtrip():
    # Explicit timestamps because the second-resolution PK would otherwise
    # collide on two writes in the same second. Production sampler fires
    # every 60s so this is only a test-pace concern.
    now = datetime.now(UTC)
    metric_snapshots.record_snapshot("cpu_load_1m", 1.5, ts=iso_z(now - timedelta(seconds=10)))
    metric_snapshots.record_snapshot("cpu_load_1m", 2.0, ts=iso_z(now))
    out = _get_history("cpu_load_1m", since=now - timedelta(hours=1))
    assert len(out) == 2
    assert {r["value"] for r in out} == {1.5, 2.0}
    for row in out:
        assert "ts" in row and "value" in row


def test_per_service_and_per_task_filtering():
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 12.3, service_id="svc-a")
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 45.6, service_id="svc-b")
    metric_snapshots.record_snapshot("cron_duration_ms", 1000.0, service_id="svc-a", task="sync")
    metric_snapshots.record_snapshot("cron_duration_ms", 2000.0, service_id="svc-a", task="commit")

    since = datetime.now(UTC) - timedelta(hours=1)

    svc_a_pool = _get_history("pool_wait_p95_ms", since=since, service_id="svc-a")
    assert [r["value"] for r in svc_a_pool] == [12.3]

    svc_b_pool = _get_history("pool_wait_p95_ms", since=since, service_id="svc-b")
    assert [r["value"] for r in svc_b_pool] == [45.6]

    sync_dur = _get_history("cron_duration_ms", since=since, service_id="svc-a", task="sync")
    assert [r["value"] for r in sync_dur] == [1000.0]


def test_global_query_excludes_per_service_rows():
    """A global lookup without service_id must NOT return rows that have a
    service_id — otherwise the global CPU/disk panels would accidentally
    pull in per-service pool_wait samples."""
    metric_snapshots.record_snapshot("cpu_load_1m", 0.5)
    metric_snapshots.record_snapshot("cpu_load_1m", 99.0, service_id="svc-x")

    since = datetime.now(UTC) - timedelta(hours=1)
    out = _get_history("cpu_load_1m", since=since)
    assert [r["value"] for r in out] == [0.5]


def test_get_batch_keys_by_scope():
    metric_snapshots.record_snapshot("cpu_load_1m", 0.7)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 5.0, service_id="svc-a")
    metric_snapshots.record_snapshot("cron_duration_ms", 800.0, service_id="svc-a", task="sync")

    batch = metric_snapshots.get_batch(since=datetime.now(UTC) - timedelta(hours=1))
    assert "cpu_load_1m" in batch
    assert "pool_wait_p95_ms|svc-a" in batch
    assert "cron_duration_ms|svc-a|sync" in batch
    assert len(batch["cpu_load_1m"]) == 1
    assert batch["cpu_load_1m"][0]["value"] == 0.7


def test_since_clamp_excludes_old_rows():
    # Stamp a row in the distant past directly via the ts override.
    old_ts = iso_z(datetime.now(UTC) - timedelta(days=10))
    metric_snapshots.record_snapshot("cpu_load_1m", 1.1, ts=old_ts)
    # And a fresh row.
    metric_snapshots.record_snapshot("cpu_load_1m", 2.2)

    out = _get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))
    assert [r["value"] for r in out] == [2.2]


def test_purge_old_drops_only_aged_rows():
    fresh_ts = iso_z(datetime.now(UTC) - timedelta(days=1))
    old_ts = iso_z(datetime.now(UTC) - timedelta(days=60))
    metric_snapshots.record_snapshot("cpu_load_1m", 1.0, ts=fresh_ts)
    metric_snapshots.record_snapshot("cpu_load_1m", 9.9, ts=old_ts)

    removed = metric_snapshots.purge_old(retention_days=30)
    assert removed == 1

    remaining = _get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(days=90))
    assert [r["value"] for r in remaining] == [1.0]


def test_purge_zero_or_negative_retention_is_noop():
    metric_snapshots.record_snapshot("cpu_load_1m", 1.0)
    assert metric_snapshots.purge_old(retention_days=0) == 0
    assert metric_snapshots.purge_old(retention_days=-5) == 0
    out = _get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))
    assert len(out) == 1


def test_get_batch_returns_empty_before_first_write():
    """Reader path must not raise when no rows exist yet."""
    out = _get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))
    assert out == []
    batch = metric_snapshots.get_batch(since=datetime.now(UTC) - timedelta(hours=1))
    assert batch == {}


def test_idempotent_insert_overwrites_same_pk_per_service():
    """Replays of the same (metric, service_id, ts) for a per-service
    series should not stack up — the composite PK dedupes them."""
    ts = iso_z(datetime.now(UTC))
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 1.0, service_id="svc-a", ts=ts)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 2.0, service_id="svc-a", ts=ts)

    out = _get_history("pool_wait_p95_ms", since=datetime.now(UTC) - timedelta(hours=1), service_id="svc-a")
    assert len(out) == 1
    assert out[0]["value"] == 2.0


def test_postgres_backend_is_shared_for_metric_writes_and_reads(monkeypatch):
    """Durable serving must not silently write metric history to pod-local SQLite."""
    write_con = _FakePgConnection()
    read_con = _FakePgConnection(
        rows=[
            {
                "metric": "pool_wait_p95_ms",
                "service_id": "svc-a",
                "task": "",
                "ts": "2026-01-01T00:00:00Z",
                "value": 4.5,
            }
        ]
    )
    monkeypatch.setattr(metric_snapshots, "_pg_write_connection", lambda: write_con)
    monkeypatch.setattr(metric_snapshots, "_pg_connection", lambda: read_con)

    metric_snapshots.record_snapshot("pool_wait_p95_ms", 4.5, service_id="svc-a", ts="2026-01-01T00:00:00Z")
    out = metric_snapshots.get_batch(since=datetime(2025, 12, 31, tzinfo=UTC))

    assert out["pool_wait_p95_ms|svc-a"][0]["value"] == 4.5
    assert "ON CONFLICT" in write_con.calls[0][0]
    assert write_con.calls[0][1][2] == "svc-a"
    assert read_con.calls[0][1] == ("2025-12-31T00:00:00Z",)
