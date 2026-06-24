"""Tests for :mod:`backend.core.metadata.reconciliation`.

Coverage rationale: the module was at 10% (covers stats, age-based
cleanup, and rollup-cleanup coordination). The two functions exercised
here — ``get_metadata_storage_stats`` and ``cleanup_metadata`` — are
the operational surface admins see in the storage stats endpoint and
the cleanup-now SSE. Both call into per-service SQLite via the
``isolate_metadata_db`` fixture (autouse, see ``tests/conftest.py``).
"""

from __future__ import annotations

from backend.core import metadata as metadata_db
from backend.core.metadata import reconciliation
from backend.core.metadata import usage_log_db as _usage_log_db


def _con(service_id: str):
    return metadata_db.get_con(service_id)


def _seed_usage_log(service_id: str, rows: int, days_ago: int = 0) -> None:
    """Insert ``rows`` usage_log entries dated ``days_ago`` in the past.

    ``usage_log`` lives in its own per-service SQLite (v2.0 cutover); seed
    via :func:`backend.core.metadata.usage_log_db.get_con` so the row
    counts/cleanup paths see them.
    """
    con = _usage_log_db.get_con(service_id)
    con.executemany(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, bytes, count) "
        f"VALUES (datetime('now', '-{days_ago} days'), ?, 'A', 'PUT_OBJECT', 0, 1)",
        [(service_id,) for _ in range(rows)],
    )
    con.commit()


def _seed_ingested_file(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = _con(service_id)
    con.executemany(
        "INSERT OR IGNORE INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
        f"VALUES (?, 'fos', datetime('now', '-{days_ago} days'), 1, 100)",
        [(f"raw/{days_ago}d-{i}.gz",) for i in range(rows)],
    )
    con.commit()


def _seed_cron_run(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = _con(service_id)
    con.executemany(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys) "
        f"VALUES ('sync', datetime('now', '-{days_ago} days'), 1.0, 'success', '[]')",
        [() for _ in range(rows)],
    )
    con.commit()


# ── get_metadata_storage_stats ────────────────────────────────────────────────


def test_storage_stats_returns_zero_rows_on_fresh_db():
    sid = "svc-stats-fresh"
    stats = reconciliation.get_metadata_storage_stats(sid)
    assert "tables" in stats
    # All expected tables are present in the schema (initialised by get_con).
    for table in ("usage_log", "ingested_files", "cron_runs"):
        assert table in stats["tables"]
        assert stats["tables"][table]["rows"] == 0
    # db_bytes is non-None whenever dbstat works (it ships with Python 3.13's
    # built-in sqlite3 on macOS/Linux).
    assert stats["db_bytes"] is None or stats["db_bytes"] >= 0
    assert stats["db_path"].endswith(f"{sid}.metadata.db")


def test_storage_stats_counts_seeded_rows():
    sid = "svc-stats-seeded"
    _seed_usage_log(sid, 7)
    _seed_ingested_file(sid, 3)
    _seed_cron_run(sid, 2)
    stats = reconciliation.get_metadata_storage_stats(sid)
    assert stats["tables"]["usage_log"]["rows"] == 7
    assert stats["tables"]["ingested_files"]["rows"] == 3
    assert stats["tables"]["cron_runs"]["rows"] == 2


def test_stats_tables_sql_names_all_exist_in_schema():
    """Regression guard for the silent-drop bug.

    Every ``sql_table`` in ``_STATS_TABLES`` must be a real table in the
    metadata schema. Four entries previously named non-existent tables
    (``saved_views``/``audit_log``/``in_flight_buffers``/
    ``locally_compacted_files``); ``get_metadata_storage_stats`` swallowed
    the resulting ``OperationalError`` and silently dropped those four rows
    from the admin storage panel. Pin every name against ``sqlite_master``
    so a future rename can't regress the same way. ``usage_log`` lives in a
    separate per-service file and is exempt.
    """
    sid = "svc-stats-schema"
    con = _con(sid)  # initialises base schema + pending migrations
    existing = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    for sql_table, _out_key in reconciliation._STATS_TABLES:
        if sql_table == "usage_log":
            continue
        assert sql_table in existing, f"_STATS_TABLES names missing table: {sql_table}"


def test_storage_stats_reports_reference_tables_under_ui_keys():
    """The four formerly-dropped reference tables now surface under their
    UI-facing keys. Each table exists on a fresh DB, so each key
    must be present with rows >= 0 — not absent."""
    sid = "svc-stats-refs"
    stats = reconciliation.get_metadata_storage_stats(sid)
    for ui_key in ("saved_views", "audit_log", "in_flight_buffers", "locally_compacted_files"):
        assert ui_key in stats["tables"], f"reference table dropped from stats: {ui_key}"
        assert stats["tables"][ui_key]["rows"] == 0


# ── is_ingested_files_dedup_active ───────────────────────────────────────────


def test_dedup_active_default_when_no_config():
    # Service has no config file → defaults to "safe to trim" (True).
    assert reconciliation.is_ingested_files_dedup_active("svc-no-cfg") is True


def test_dedup_active_when_delete_after_true(monkeypatch):
    sid = "svc-delete-after-true"

    def _fake_load_config(s: str):
        return {"provisioning": {"cron_sync": {"delete_after": True}}}

    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", _fake_load_config)
    assert reconciliation.is_ingested_files_dedup_active(sid) is True


def test_dedup_active_returns_false_when_delete_after_false(monkeypatch):
    sid = "svc-delete-after-false"

    def _fake_load_config(s: str):
        return {"provisioning": {"cron_sync": {"delete_after": False}}}

    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", _fake_load_config)
    # Returns False — i.e. ``ingested_files`` is the dedup gate, must not
    # be trimmed.
    assert reconciliation.is_ingested_files_dedup_active(sid) is False


# ── cleanup_metadata ─────────────────────────────────────────────────────────


def test_cleanup_deletes_aged_usage_log_rows():
    sid = "svc-cleanup-aged"
    _seed_usage_log(sid, 5, days_ago=10)
    _seed_usage_log(sid, 3, days_ago=0)

    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 7})

    assert result["deleted"]["usage_log"] == 5
    assert result["after"]["usage_log"] == 3
    assert result["vacuumed"] is True  # Anything deleted → VACUUM runs
    assert result["duration_s"] >= 0


def test_cleanup_zero_retention_disables_table():
    sid = "svc-cleanup-zero"
    _seed_usage_log(sid, 5, days_ago=10)

    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 0})

    # Retention=0 → skip deletion for this table, nothing trimmed.
    assert result["deleted"]["usage_log"] == 0
    assert result["after"]["usage_log"] == 5
    assert result["vacuumed"] is False  # No deletes → no VACUUM


def test_cleanup_uses_default_retention_when_key_missing():
    sid = "svc-cleanup-default"
    # No retention passed → defaults apply. DEFAULT_METADATA_RETENTION
    # picks safe positive values, so rows older than that get trimmed.
    _seed_cron_run(sid, 4, days_ago=400)  # well past any reasonable default
    result = reconciliation.cleanup_metadata(sid)
    assert result["deleted"]["cron_runs"] == 4


def test_cleanup_emits_progress_events():
    sid = "svc-cleanup-events"
    _seed_usage_log(sid, 2, days_ago=10)
    events: list[dict] = []

    def _on_event(e: dict) -> None:
        events.append(e)

    reconciliation.cleanup_metadata(
        sid,
        retention={"usage_log_days": 1, "ingested_files_days": 1, "cron_runs_days": 1},
        on_event=_on_event,
    )

    # At least one status event AND at least one progress event fired.
    assert any(e["type"] == "status" for e in events)
    assert any(e["type"] == "progress" for e in events)
    # Final progress event hits the total step count.
    last_progress = [e for e in events if e["type"] == "progress"][-1]
    assert last_progress["current"] == last_progress["total"]


def test_cleanup_force_disables_ingested_files_when_delete_after_false(monkeypatch):
    sid = "svc-cleanup-forced-off"
    _seed_ingested_file(sid, 5, days_ago=400)

    from backend import config as svcconfig

    monkeypatch.setattr(
        svcconfig,
        "load_config",
        lambda s: {"provisioning": {"cron_sync": {"delete_after": False}}},
    )

    events: list[dict] = []
    result = reconciliation.cleanup_metadata(
        sid,
        retention={"ingested_files_days": 30},  # Caller wants trim
        on_event=events.append,
    )

    # Forced override: nothing deleted from ingested_files.
    assert result["deleted"]["ingested_files"] == 0
    assert result["after"]["ingested_files"] == 5
    # The override surfaces as a status event so the operator sees why.
    override_msgs = [e for e in events if e["type"] == "status" and "dedup gate" in e.get("message", "")]
    assert override_msgs, "expected status event explaining the override"


def test_cleanup_on_event_callback_failure_does_not_abort():
    sid = "svc-cleanup-bad-callback"
    _seed_usage_log(sid, 3, days_ago=10)

    def _bad_callback(e: dict) -> None:
        raise RuntimeError("callback fail")

    # Must not raise — the implementation swallows callback errors so a
    # buggy SSE consumer can't break the cleanup itself.
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1}, on_event=_bad_callback)
    assert result["deleted"]["usage_log"] == 3


def test_cleanup_negative_retention_is_treated_as_disabled():
    sid = "svc-cleanup-neg"
    _seed_usage_log(sid, 5, days_ago=400)
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": -5})
    # Negative → skipped (same as 0).
    assert result["deleted"]["usage_log"] == 0


def test_cleanup_non_int_retention_falls_back_to_disabled():
    sid = "svc-cleanup-bad-type"
    _seed_usage_log(sid, 3, days_ago=400)
    # Garbage value → coerce-fails → 0 days → skip.
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": "not-a-number"})
    assert result["deleted"]["usage_log"] == 0


def test_cleanup_rollups_skipped_when_rollups_days_zero():
    sid = "svc-cleanup-no-rollups"
    _seed_usage_log(sid, 1, days_ago=400)
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1, "rollups_days": 0})
    assert result["rollups_deleted"] == 0


def test_cleanup_rollups_skipped_when_source_missing(monkeypatch):
    sid = "svc-cleanup-no-src"
    _seed_usage_log(sid, 1, days_ago=400)

    from backend.core import duckdb as _db

    monkeypatch.setattr(_db, "get_source_for_service", lambda s: None)

    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1, "rollups_days": 7})
    assert result["rollups_deleted"] == 0


# ── ingested_files_summary drift fix ─────────────────────────────────────────


def _seed_ingested_file_for(service_id: str, rows: int, days_ago: int = 0) -> None:
    """Like ``_seed_ingested_file`` but stamps ``source_name=service_id``.

    The summary rollup is keyed by ``source_name``, and in production each
    per-service ``metadata.db`` only ever holds rows for its own service
    (``source_name == service_id``). The shared helper hardcodes ``'fos'``,
    which is fine for the source-agnostic DELETE/COUNT tests but invisible to
    the source-scoped summary recompute — so seed with the real key here.
    """
    con = _con(service_id)
    con.executemany(
        "INSERT OR IGNORE INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
        f"VALUES (?, ?, datetime('now', '-{days_ago} days'), 1, 100)",
        [(f"raw/{service_id}-{days_ago}d-{i}.gz", service_id) for i in range(rows)],
    )
    con.commit()


def _poison_summary(service_id: str, *, file_count: int, total_rows: int, total_bytes: int) -> None:
    """Force ``ingested_files_summary`` to a value that does NOT match the
    table — simulates the cumulative-ever drift the retention DELETE used to
    cause (it never decremented the incrementally-maintained rollup)."""
    con = _con(service_id)
    con.execute(
        "INSERT INTO ingested_files_summary "
        "(source_name, file_count, total_rows, total_bytes, count_with_bytes, latest_file_name, last_ingested) "
        "VALUES (?, ?, ?, ?, ?, 'raw/poison.gz', datetime('now')) "
        "ON CONFLICT(source_name) DO UPDATE SET "
        "file_count=excluded.file_count, total_rows=excluded.total_rows, "
        "total_bytes=excluded.total_bytes, count_with_bytes=excluded.count_with_bytes",
        (service_id, file_count, total_rows, total_bytes, file_count),
    )
    con.commit()


def test_cleanup_recomputes_ingested_files_summary_after_trim():
    """The retention DELETE on ingested_files must leave the summary rollup
    consistent with the surviving rows, not the pre-trim (drifted) totals.

    Regression guard for the prod drift where the rollup read 10.4M rows /
    4.85M files while the table held ~260k rows — the DELETE never decremented
    the incrementally-maintained counter."""
    sid = "svc-rollup-recompute"
    _seed_ingested_file_for(sid, 5, days_ago=10)  # aged out → trimmed
    _seed_ingested_file_for(sid, 3, days_ago=0)  # recent → survive
    # Drifted rollup far larger than the table (the bug we're fixing).
    _poison_summary(sid, file_count=999_999, total_rows=999_999, total_bytes=99_999_900)

    result = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7})

    assert result["deleted"]["ingested_files"] == 5
    assert result["after"]["ingested_files"] == 3

    # Rollup recomputed to the surviving rows (row_count=1, file_size=100 each).
    summary = metadata_db.get_ingested_files_status_summary(sid)
    assert summary["file_count"] == 3
    assert summary["total_rows"] == 3
    assert summary["total_bytes"] == 300


def test_cleanup_skips_summary_recompute_when_nothing_trimmed():
    """The recompute is gated on an actual deletion: when no ingested_files
    rows are trimmed (all recent / delete_after=false), the rollup is left
    untouched so delete_after=false services don't pay a daily full rescan."""
    sid = "svc-rollup-noop"
    _seed_ingested_file_for(sid, 3, days_ago=0)  # all recent → none trimmed at 7d
    _poison_summary(sid, file_count=42, total_rows=4242, total_bytes=4200)

    result = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7})
    assert result["deleted"]["ingested_files"] == 0

    # Untouched — no deletion means no recompute.
    summary = metadata_db.get_ingested_files_status_summary(sid)
    assert summary["file_count"] == 42
    assert summary["total_rows"] == 4242
