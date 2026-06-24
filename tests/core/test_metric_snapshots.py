"""Tests for backend.core.metric_snapshots."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from backend.core import metric_snapshots
from backend.utils.date_utils import iso_z


def test_record_and_get_global_metric_roundtrip():
    # Explicit timestamps because the second-resolution PK would otherwise
    # collide on two writes in the same second. Production sampler fires
    # every 60s so this is only a test-pace concern.
    now = datetime.now(UTC)
    metric_snapshots.record_snapshot("cpu_load_1m", 1.5, ts=iso_z(now - timedelta(seconds=10)))
    metric_snapshots.record_snapshot("cpu_load_1m", 2.0, ts=iso_z(now))
    out = metric_snapshots.get_history("cpu_load_1m", since=now - timedelta(hours=1))
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

    svc_a_pool = metric_snapshots.get_history("pool_wait_p95_ms", since=since, service_id="svc-a")
    assert [r["value"] for r in svc_a_pool] == [12.3]

    svc_b_pool = metric_snapshots.get_history("pool_wait_p95_ms", since=since, service_id="svc-b")
    assert [r["value"] for r in svc_b_pool] == [45.6]

    sync_dur = metric_snapshots.get_history("cron_duration_ms", since=since, service_id="svc-a", task="sync")
    assert [r["value"] for r in sync_dur] == [1000.0]


def test_global_query_excludes_per_service_rows():
    """A get_history without service_id must NOT return rows that have a
    service_id — otherwise the global CPU/disk panels would accidentally
    pull in per-service pool_wait samples."""
    metric_snapshots.record_snapshot("cpu_load_1m", 0.5)
    metric_snapshots.record_snapshot("cpu_load_1m", 99.0, service_id="svc-x")

    since = datetime.now(UTC) - timedelta(hours=1)
    out = metric_snapshots.get_history("cpu_load_1m", since=since)
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

    out = metric_snapshots.get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))
    assert [r["value"] for r in out] == [2.2]


def test_purge_old_drops_only_aged_rows():
    fresh_ts = iso_z(datetime.now(UTC) - timedelta(days=1))
    old_ts = iso_z(datetime.now(UTC) - timedelta(days=60))
    metric_snapshots.record_snapshot("cpu_load_1m", 1.0, ts=fresh_ts)
    metric_snapshots.record_snapshot("cpu_load_1m", 9.9, ts=old_ts)

    removed = metric_snapshots.purge_old(retention_days=30)
    assert removed == 1

    remaining = metric_snapshots.get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(days=90))
    assert [r["value"] for r in remaining] == [1.0]


def test_purge_zero_or_negative_retention_is_noop():
    metric_snapshots.record_snapshot("cpu_load_1m", 1.0)
    assert metric_snapshots.purge_old(retention_days=0) == 0
    assert metric_snapshots.purge_old(retention_days=-5) == 0
    out = metric_snapshots.get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))
    assert len(out) == 1


def test_get_history_returns_empty_before_first_write():
    """Reader path must not raise when the file doesn't exist yet."""
    out = metric_snapshots.get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))
    assert out == []
    batch = metric_snapshots.get_batch(since=datetime.now(UTC) - timedelta(hours=1))
    assert batch == {}


def test_idempotent_insert_overwrites_same_pk_per_service():
    """Replays of the same (metric, service_id, ts) for a per-service
    series should not stack up — the composite PK dedupes them."""
    ts = iso_z(datetime.now(UTC))
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 1.0, service_id="svc-a", ts=ts)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 2.0, service_id="svc-a", ts=ts)

    out = metric_snapshots.get_history(
        "pool_wait_p95_ms", since=datetime.now(UTC) - timedelta(hours=1), service_id="svc-a"
    )
    assert len(out) == 1
    assert out[0]["value"] == 2.0
