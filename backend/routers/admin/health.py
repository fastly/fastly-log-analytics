"""System health snapshot endpoint for the admin page."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from backend.models.admin import HealthSnapshotResponse

from ._router import router


@router.get("/admin/health-snapshot", response_model=HealthSnapshotResponse)
def health_snapshot(probe_fos: bool = False) -> dict[str, Any]:
    """One-shot health snapshot for the admin page system health card.

    Returns CPU load averages, memory, disk usage of the data mount,
    docker container CPU/memory (if reachable), the count of in-flight
    cron runs, the age of the last scheduler tick (SRE-06), the set of
    services+tasks whose latest terminal cron run errored (SRE-03), the
    effective log/exporter mode (SRE-20), and config-backup freshness
    (SRE-11). Uses only stdlib + per-service SQLite (no psutil dep).

    ``probe_fos=1`` additionally issues a per-service FOS reachability
    probe (SRE-13) — a real network round-trip that bills a Class-B op,
    so it is opt-in and off by default (the card never sets it).
    """
    import shutil

    out: dict = {}

    # ── Load + uptime ─────────────────────────────────────────────────
    try:
        load1, load5, load15 = os.getloadavg()
        out["load"] = {"avg_1m": round(load1, 2), "avg_5m": round(load5, 2), "avg_15m": round(load15, 2)}
    except Exception:
        out["load"] = None

    # vCPU count to interpret load (load > vCPU = backlog).
    try:
        import multiprocessing as _mp

        out["vcpus"] = _mp.cpu_count()
    except Exception:
        out["vcpus"] = None

    # ── Memory (Linux /proc/meminfo) ─────────────────────────────────
    try:
        meminfo: dict[str, int] = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, _, rest = line.partition(":")
                v = rest.strip().split()
                if v and v[0].isdigit():
                    meminfo[k.strip()] = int(v[0]) * 1024  # kB → bytes
        total = meminfo.get("MemTotal", 0)
        avail = meminfo.get("MemAvailable", 0)
        out["memory"] = {
            "total_mb": round(total / 1024 / 1024),
            "available_mb": round(avail / 1024 / 1024),
            "used_pct": round((1 - avail / total) * 100, 1) if total else None,
        }
    except Exception:
        out["memory"] = None

    # ── Data-mount disk usage ────────────────────────────────────────
    for path, label in (("/app/data", "data_mount"), ("/", "root_disk")):
        try:
            d = shutil.disk_usage(path)
            out[label] = {
                "total_gb": round(d.total / 1024 / 1024 / 1024, 1),
                "used_gb": round(d.used / 1024 / 1024 / 1024, 1),
                "free_gb": round(d.free / 1024 / 1024 / 1024, 1),
                "used_pct": round(d.used / d.total * 100, 1) if d.total else None,
            }
        except Exception:
            out[label] = None

    # ── In-flight cron runs ──────────────────────────────────────────
    # Use list_active_runs() (which filters out runs whose last event is
    # done/error) instead of iterating _run_metadata directly. The dict
    # holds entries for an hour after completion (the cleanup TTL), so the
    # raw iteration was showing dozens of stale "sync" entries in the
    # System Health card.
    try:
        from backend.cron_progress import list_active_runs

        in_flight = []
        for entry in list_active_runs():
            # cron_progress stores started_at as a float epoch (time.time());
            # HealthInFlightRun.started_at is typed str|None, so coerce to an
            # ISO-8601 string here. Without this, ANY in-flight run makes
            # FastAPI raise ResponseValidationError → 500 on the health card
            # (only reproduces while a cron is actually running, e.g. right
            # after a restart).
            raw_started = entry.get("started_at")
            started_at = (
                datetime.fromtimestamp(raw_started, tz=UTC).isoformat()
                if isinstance(raw_started, (int, float))
                else raw_started
            )
            in_flight.append(
                {
                    "run_id": entry["run_id"],
                    "service_id": entry.get("service_id"),
                    "task": entry.get("task"),
                    "started_at": started_at,
                }
            )
        out["in_flight_runs"] = in_flight
    except Exception:
        out["in_flight_runs"] = []

    # ── Per-service compaction stats ─────────────────────────────────
    try:
        from backend import config as _svcconfig
        from backend.core import local_compaction as _lc

        stats_by_svc: dict = {}
        for cfg in _svcconfig.list_configs():
            sid = cfg.get("service_id") or cfg.get("name")
            try:
                src = _svcconfig.config_to_source(cfg)
                stats_by_svc[sid] = _lc.compaction_stats(src)
            except Exception:
                stats_by_svc[sid] = None
        out["compaction"] = stats_by_svc
    except Exception:
        out["compaction"] = {}

    # ── DuckDB connection-pool wait stats (Phase 6 in-process sampler) ──
    # Backs the Pool Wait card in the admin SystemHealthCard. The same
    # samples also stream to the OTel ``app.thread_wait_ms`` histogram for
    # off-box analysis; this in-process projection is for the UI's 1s poll.
    try:
        from backend.core import duckdb_pool as _pool_mod

        out["pool_wait"] = _pool_mod.get_all_stats()
    except Exception:
        out["pool_wait"] = []

    # ── Scheduler liveness (SRE-06) ──────────────────────────────────
    # Age of the newest metric_snapshots row. That sampler is a global
    # 60s APScheduler job, so a large/None age witnesses a dead scheduler
    # — the signal that distinguishes "no crons running because idle" from
    # "no crons running because the scheduler thread died". None = fresh
    # boot with no samples yet (the card renders "unknown", not an alarm).
    try:
        from backend.core import metric_snapshots as _ms

        out["scheduler_last_tick_age_s"] = _ms.last_snapshot_age_s()
    except Exception:
        out["scheduler_last_tick_age_s"] = None

    # ── Recent cron failures (SRE-03) ────────────────────────────────
    # The cross-service aggregate ADR-09 §2.3's runbook tells the operator
    # to watch. For each configured service, take the latest *terminal* run
    # per task and surface the ones that ended in error. This catches the
    # non-`sync` crons (commit / optimize / rollup_compact / metadata_sync)
    # that the deep /api/health probe and the per-service Cron History both
    # miss as a single glance.
    try:
        from backend import config as _svcconfig
        from backend.core.metadata.cron_log import latest_cron_per_task

        failures: list[dict] = []
        for sid in _svcconfig.list_service_ids():
            try:
                for task, run in latest_cron_per_task(sid).items():
                    if run.get("status") == "error":
                        failures.append(
                            {
                                "service_id": sid,
                                "task": task,
                                "status": run.get("status"),
                                "started_at": run.get("started_at"),
                                "error_message": run.get("error_message"),
                            }
                        )
            except Exception:
                # One unreadable service's metadata.db must not sink the
                # whole aggregate — skip it (same per-probe isolation the
                # rest of this endpoint uses).
                continue
        # Newest-first so the card shows the freshest breakage at the top;
        # cap defensively so a many-service deploy can't bloat the payload.
        failures.sort(key=lambda f: f.get("started_at") or "", reverse=True)
        out["recent_cron_failures"] = failures[:50]
    except Exception:
        out["recent_cron_failures"] = []

    # ── Effective observability mode (SRE-20) ────────────────────────
    # The runtime truth (vs. the ADR's aspirational wiring): prod runs
    # ConsoleRenderer with OTEL off, so an incident `jq`/`grep` needs to
    # know it's matching bracketed text, not JSON, and that trace fields
    # are empty.
    try:
        from backend.core.request_telemetry import effective_exporter
        from backend.utils.structlog_config import effective_format

        out["observability"] = {
            "log_format": effective_format(),
            "otel_exporter": effective_exporter(),
        }
    except Exception:
        out["observability"] = None

    # ── Config-backup freshness (SRE-11 / ADR-13 §2.1) ───────────────
    # The service-config JSON is the one piece of VM-disk state that's not
    # recoverable from FOS. The backup runs from the operator's workstation
    # → GCS, so the backend can't see GCS directly; instead it reads a
    # VM-local marker the script writes on success. None = no backup ever
    # recorded (the honest "is my only irreplaceable state captured?" answer).
    try:
        import json as _json

        from backend import config as _cfg

        marker_path = os.environ.get("CONFIG_BACKUP_MARKER") or str(
            _cfg.DATA_DIR / "system" / "last_config_backup.json"
        )
        backup: dict | None = None
        if os.path.exists(marker_path):
            try:
                with open(marker_path) as _f:
                    raw = _json.load(_f)
                last_at = raw.get("at")
                age_s: float | None = None
                if last_at:
                    from backend.utils.date_utils import parse_iso_utc

                    dt = parse_iso_utc(str(last_at))
                    if dt is not None:
                        age_s = max(0.0, (datetime.now(UTC) - dt).total_seconds())
                backup = {"last_backup_at": last_at, "age_s": age_s, "source": raw.get("remote")}
            except Exception:
                # Marker present but unparseable — surface "unknown" rather
                # than a false-fresh; an empty dict maps to all-None fields.
                backup = {"last_backup_at": None, "age_s": None, "source": None}
        out["config_backup"] = backup
    except Exception:
        out["config_backup"] = None

    # ── FOS reachability probe (SRE-13, opt-in) ──────────────────────
    if probe_fos:
        try:
            from backend import config as _svcconfig
            from backend.core.duckdb import fos_reachable

            fos_out: dict = {}
            for cfg in _svcconfig.list_configs():
                sid = cfg.get("service_id") or cfg.get("name")
                try:
                    src = _svcconfig.config_to_source(cfg)
                    fos_out[sid] = fos_reachable(src)
                except Exception as e:
                    fos_out[sid] = {"reachable": False, "error": str(e)[:200]}
            out["fos"] = fos_out
        except Exception:
            out["fos"] = None

    return out


@router.get("/admin/vcl-health")
def api_vcl_health(service_id: str) -> dict[str, Any]:
    """Verify VCL state after migration — check for legacy/consolidated snippet presence.

    Returns a dict with:
    - legacy_snippets_found: count of pre-2.2 consolidated snippets
    - consolidated_snippets_found: count of new consolidated snippets
    - is_clean: True if no legacy snippets remain
    - recommendation: guidance for the operator

    Used by the admin UI post-deploy to show migration status.
    """
    try:
        from backend import config as svcconfig
        from backend.provision.declarative import fastly_integration

        cfg = svcconfig.load_config(service_id)
        if not cfg:
            return {
                "service_id": service_id,
                "error": f"Service {service_id} not configured",
                "is_clean": False,
            }

        token = cfg.get("admin_token")
        if not token:
            return {
                "service_id": service_id,
                "error": "No Fastly API token in service config",
                "is_clean": False,
            }

        active_version = fastly_integration.fetch_active_version(service_id, token)
        if active_version is None:
            return {
                "service_id": service_id,
                "error": "No active version found",
                "is_clean": False,
            }

        snippets = fastly_integration.fetch_snippets(service_id, active_version, token)

        # Count legacy vs consolidated
        from backend.provision.declarative.reconciler import _is_legacy_snippet

        legacy = [s for s in snippets if _is_legacy_snippet(s.name)]
        consolidated = [s for s in snippets if s.name.startswith("Fastly Log Analytics - ")]

        is_clean = len(legacy) == 0 and len(consolidated) > 0

        return {
            "service_id": service_id,
            "active_version": active_version,
            "legacy_snippets_found": len(legacy),
            "legacy_names": [s.name for s in legacy],
            "consolidated_snippets_found": len(consolidated),
            "consolidated_names": [s.name for s in consolidated],
            "is_clean": is_clean,
            "recommendation": (
                "✓ Clean state — VCL migration complete. Ready for production."
                if is_clean
                else (
                    f"⚠ Migration in progress: {len(legacy)} legacy snippet(s) remain. "
                    "Run reconciliation to complete the migration."
                    if legacy and consolidated
                    else (
                        f"⚠ Pre-migration state: {len(legacy)} legacy snippet(s) found. "
                        "Run reconciliation to migrate to consolidated VCL."
                        if legacy
                        else "⚠ Unexpected state: no snippets found. Service may not be configured."
                    )
                )
            ),
        }
    except Exception as e:
        import logging

        logger = logging.getLogger(__name__)
        logger.exception("VCL health check failed")
        return {
            "service_id": service_id,
            "error": str(e),
            "is_clean": False,
        }
