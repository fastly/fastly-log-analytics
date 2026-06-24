"""Tests for the /api/admin/health-snapshot endpoint.

The endpoint pulls load/memory/disk/in-flight-runs/compaction/pool-wait
from a handful of stdlib + internal sources. Every individual collector
is wrapped in try/except so a single failure degrades to a None field
instead of a 500 — those failure paths are the focus here, since the
happy paths depend on /proc/meminfo etc. which aren't portable to macOS
CI runners.
"""

from __future__ import annotations

from unittest.mock import patch


def _get_health(client):
    resp = client.get("/api/admin/health-snapshot")
    assert resp.status_code == 200
    return resp.json()


# ── Happy-ish: most collectors return data, missing /proc/meminfo
#    + non-Linux disk paths degrade to None on the macOS test runner.


def test_health_snapshot_returns_shape_with_known_collectors(client):
    """Smoke: the endpoint returns the documented keys regardless of
    which collectors succeed on the current host."""
    body = _get_health(client)

    assert "load" in body
    assert "vcpus" in body
    assert "memory" in body
    assert "data_mount" in body
    assert "root_disk" in body
    assert "in_flight_runs" in body
    assert "compaction" in body
    assert "pool_wait" in body
    # SRE observability pass additions (SRE-06 / 03 / 20 / 11).
    assert "scheduler_last_tick_age_s" in body
    assert "recent_cron_failures" in body
    assert "observability" in body
    assert "config_backup" in body


# ── Failure paths: each collector under try/except. ────────────────────────


def test_health_snapshot_load_failure_renders_none(client):
    with patch("os.getloadavg", side_effect=OSError("no load avg")):
        body = _get_health(client)
    assert body["load"] is None


def test_health_snapshot_memory_failure_renders_none(client):
    """Non-Linux runners can't open /proc/meminfo — the endpoint
    returns memory=None rather than 500."""
    with patch("builtins.open", side_effect=FileNotFoundError("no /proc/meminfo")):
        body = _get_health(client)
    assert body["memory"] is None


def test_health_snapshot_memory_succeeds_when_meminfo_readable(client):
    """When /proc/meminfo IS readable (Linux prod), populated stats
    come through with the documented shape."""
    fake_meminfo = (
        "MemTotal:        8388608 kB\n"
        "MemFree:         1048576 kB\n"
        "MemAvailable:    4194304 kB\n"
        "Buffers:          524288 kB\n"
    )

    real_open = open

    def _fake_open(path, *args, **kwargs):
        if path == "/proc/meminfo":
            from io import StringIO

            return StringIO(fake_meminfo)
        return real_open(path, *args, **kwargs)

    with patch("builtins.open", side_effect=_fake_open):
        body = _get_health(client)

    assert body["memory"] is not None
    assert body["memory"]["total_mb"] == 8192
    assert body["memory"]["available_mb"] == 4096
    assert body["memory"]["used_pct"] == 50.0


def test_health_snapshot_disk_failure_renders_none_for_each_mount(client):
    """If shutil.disk_usage raises (e.g. mount point doesn't exist),
    the per-mount field is None — both /app/data and / get the same
    treatment."""
    with patch("shutil.disk_usage", side_effect=OSError("missing mount")):
        body = _get_health(client)
    assert body["data_mount"] is None
    assert body["root_disk"] is None


def test_health_snapshot_disk_returns_stats_when_available(client):
    """shutil.disk_usage returning a (total, used, free) namedtuple
    flows through into the rounded GB fields the UI renders."""
    from collections import namedtuple

    Usage = namedtuple("Usage", ["total", "used", "free"])
    # 100 GB total, 25 GB used.
    fake = Usage(total=100 * 1024**3, used=25 * 1024**3, free=75 * 1024**3)

    with patch("shutil.disk_usage", return_value=fake):
        body = _get_health(client)

    assert body["data_mount"]["total_gb"] == 100.0
    assert body["data_mount"]["used_gb"] == 25.0
    assert body["data_mount"]["free_gb"] == 75.0
    assert body["data_mount"]["used_pct"] == 25.0


def test_health_snapshot_disk_zero_total_renders_pct_none(client):
    """Edge case: tmpfs / virtual mounts can report total=0; ``used_pct``
    must collapse to None rather than ZeroDivisionError."""
    from collections import namedtuple

    Usage = namedtuple("Usage", ["total", "used", "free"])
    fake = Usage(total=0, used=0, free=0)

    with patch("shutil.disk_usage", return_value=fake):
        body = _get_health(client)

    assert body["data_mount"]["used_pct"] is None


def test_health_snapshot_in_flight_runs_failure_renders_empty(client):
    """If list_active_runs raises (corrupt cron-progress state), the
    field degrades to []; the UI's empty state then renders cleanly."""
    with patch("backend.cron_progress.list_active_runs", side_effect=RuntimeError("boom")):
        body = _get_health(client)
    assert body["in_flight_runs"] == []


def test_health_snapshot_in_flight_runs_returns_simplified_shape(client):
    """The endpoint projects the full progress dict down to four fields
    the SystemHealthCard renders. Pinned because dropping a key here
    silently breaks the card."""
    fake_runs = [
        {
            "run_id": 101,
            "service_id": "svc-a",
            "task": "sync",
            "started_at": "2026-06-12T10:00:00Z",
            "rows_done": 1234,  # extra field should NOT leak
        },
        {
            "run_id": 102,
            "service_id": "svc-b",
            "task": "metadata_cleanup",
            "started_at": "2026-06-12T10:05:00Z",
        },
    ]
    with patch("backend.cron_progress.list_active_runs", return_value=fake_runs):
        body = _get_health(client)

    assert len(body["in_flight_runs"]) == 2
    keys_seen = set(body["in_flight_runs"][0].keys())
    assert keys_seen == {"run_id", "service_id", "task", "started_at"}, (
        f"unexpected/missing fields in projected in_flight_runs entry: {keys_seen}"
    )


def test_health_snapshot_in_flight_started_at_float_epoch_coerced_to_string(client):
    """REGRESSION: cron_progress stores ``started_at`` as a float epoch
    (``time.time()``), but ``HealthInFlightRun.started_at`` is typed
    ``str | None``. The endpoint must coerce it — otherwise FastAPI raises
    ResponseValidationError → 500 whenever a cron run is genuinely in flight
    (which is why it only surfaced right after a restart, while a sync was
    running).

    The sibling shape test above used an ISO *string* fixture — what the
    response model wants, not what the producer actually emits — so it never
    exercised this path. Derive the fixture from the producer (float epoch).
    """
    fake_runs = [
        {"run_id": 7, "service_id": "svc-a", "task": "sync", "started_at": 1781758323.4424183},
    ]
    with patch("backend.cron_progress.list_active_runs", return_value=fake_runs):
        body = _get_health(client)  # _get_health asserts 200 — 500 under the bug

    entry = body["in_flight_runs"][0]
    assert isinstance(entry["started_at"], str), "float epoch must be coerced to a string"
    assert "T" in entry["started_at"], "started_at should be an ISO-8601 timestamp"


def test_health_snapshot_compaction_failure_renders_empty_dict(client):
    """The compaction block iterates list_configs(); if THAT raises,
    the whole compaction field collapses to {} (per-service failures
    inside the loop already render as ``None`` per service)."""
    with patch("backend.config.list_configs", side_effect=RuntimeError("boom")):
        body = _get_health(client)
    assert body["compaction"] == {}


def test_health_snapshot_compaction_per_service_failure_renders_none(client):
    """A per-service compaction_stats failure must collapse to None
    for that service WITHOUT taking down the whole compaction map."""
    fake_configs = [{"service_id": "svc-good", "name": "svc-good"}, {"service_id": "svc-bad", "name": "svc-bad"}]

    def _stats(src):
        if src["name"] == "svc-bad":
            raise RuntimeError("simulated per-svc failure")
        # Field is "partitions" — matches both compaction_stats() output
        # and the CompactionStatsResponse model in backend.models.admin.
        return {"partitions": 7}

    with (
        patch("backend.config.list_configs", return_value=fake_configs),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch("backend.core.local_compaction.compaction_stats", side_effect=_stats),
    ):
        body = _get_health(client)

    # The typed CompactionStatsResponse populates the partitions field
    # and fills the rest with defaults. We pin the field we care about
    # (partitions=7 propagates) rather than full equality.
    assert body["compaction"]["svc-good"]["partitions"] == 7
    assert body["compaction"]["svc-bad"] is None


def test_health_snapshot_pool_wait_failure_renders_empty_list(client):
    """get_all_stats raising returns ``pool_wait=[]`` so the Pool Wait
    card renders the empty state instead of disappearing."""
    with patch("backend.core.duckdb_pool.get_all_stats", side_effect=RuntimeError("boom")):
        body = _get_health(client)
    assert body["pool_wait"] == []


def test_health_snapshot_pool_wait_returns_pool_stats(client):
    fake_stats = [
        {"pool": "default", "wait_p50_ms": 5.0, "wait_p95_ms": 12.0},
        {"pool": "cron", "wait_p50_ms": 1.0, "wait_p95_ms": 3.0},
    ]
    with patch("backend.core.duckdb_pool.get_all_stats", return_value=fake_stats):
        body = _get_health(client)
    assert body["pool_wait"] == fake_stats


def test_health_snapshot_vcpus_failure_renders_none(client):
    with patch("multiprocessing.cpu_count", side_effect=NotImplementedError):
        body = _get_health(client)
    assert body["vcpus"] is None
