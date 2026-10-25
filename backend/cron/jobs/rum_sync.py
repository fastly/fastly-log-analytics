"""RUM sync cron job — ingest raw beacon logs from FOS.

Registered in scheduler as cron_rum_sync. Runs periodically (configurable interval).
"""

from __future__ import annotations

import logging
import sys as _sys
import time
from datetime import UTC, datetime, timedelta

from backend.core.rum_ingest import ingest_rum_logs
from backend.cron.decorators import cron_task
from backend.cron.lifecycle import cron_run

logger = logging.getLogger(__name__)


def _faro_purge_surrogate_key(logging_service_id: str, token: str) -> None:
    """Fire-and-forget surrogate-key purge on the logging service after a Faro bundle (re)upload.

    Purges on ``logging_service_id`` — NOT the CDN service fronting FOS for
    log downloads (F-1 audit finding). ``Surrogate-Key: rum-faro-sdk`` is
    set only in the RUM fetch-cache snippet, which is deployed to the
    instrumented *logging* service, never the CDN service — a purge
    targeting the CDN service can never match anything there. Mirrors the
    fire-and-forget convention of ``_purge_surrogate_key`` in
    ``backend.core.iceberg._core`` (which purges a different, unrelated key
    on the CDN service, where that IS the correct target): a missing
    service id/token or a purge failure must never surface — the FOS
    upload it follows already succeeded.
    """
    if not logging_service_id or not token:
        return
    try:
        from backend.core.fastly.client import fastly

        fastly("POST", f"/service/{logging_service_id}/purge/rum-faro-sdk", token=token, expect_empty=True)
    except Exception:
        logger.warning("Faro surrogate-key purge failed for %s (non-fatal)", logging_service_id, exc_info=True)


def _reconcile_faro_bundle(service_id: str, run_id: int | None) -> bool:
    """Keep the operator's pinned Faro bundle present and intact in FOS.

    Two deliberately different cadences: a cheap FOS HEAD every tick (catches
    a wiped bucket or an upload interrupted mid-write), and an upstream drift
    check throttled to once per ``faro_upstream_check_hours`` (catches unpkg
    re-releasing the same version string under the same tag) — the latter
    downloads a bundle from unpkg on every call, so running it every tick
    would mean an unpkg round trip per service per cron tick.

    This never bumps ``faro_version`` to a *different* version — upgrades are
    an explicit operator action. The one exception is adopting
    ``DEFAULT_FARO_VERSION`` for a service that is RUM-enabled but has no
    version pinned at all: since the tracker JS unconditionally loads the
    first-party ``/js/faro-sdk.js`` (no CDN fallback), an unpinned service has
    no bundle behind that path and would 404 for every visitor. That state
    only exists for services enabled before this default existed (self-heal,
    one-time — the next tick sees a pinned version and skips straight to the
    normal integrity check, so this never repeats or thrashes).

    That one-time self-heal also calls ``reconcile_vcl_state`` (F-4 audit
    finding): a service in this state was enabled before ``faro_version``
    existed, so its LIVE VCL was generated with ``faro_version=None`` — the
    ``/js/faro-sdk.js`` route, the SigV4 object-path rewrite, and the
    fetch-cache snippet are all absent from it. Uploading the bundle alone
    would leave it unreachable from any deployed route; the reconcile call
    is what actually makes ``/js/faro-sdk.js`` resolve. Every other branch
    below (integrity restore, upstream drift resync) re-uploads bytes for a
    version that was ALREADY pinned and reconciled when it was pinned, so
    those never need to touch VCL.

    Each ``download_and_upload_faro`` call below passes
    ``expected_current_version`` — a compare-and-set guard (F-5 audit
    finding) against an operator's concurrent ``upgrade_faro_version`` call
    moving the pin while this (possibly slow, unpkg-round-trip-bearing)
    call is in flight; see that function's docstring.

    Must never raise: wrapped end to end so a transient unpkg/FOS outage
    degrades to "bundle not refreshed this tick", never to "beacon ingest
    stopped." Not itself a cron run — it borrows the ingest run's run_id
    (if any) purely to surface progress messages; it starts no cron_runs
    row of its own.
    """
    import asyncio

    from backend import config as svcconfig
    from backend.core.faro_versions import DEFAULT_FARO_VERSION
    from backend.cron_progress import add_progress
    from backend.provision.rum_assets import (
        detect_faro_version_change,
        download_and_upload_faro,
        faro_bundle_intact,
    )

    def report(msg: str) -> None:
        logger.info(msg)
        if run_id:
            add_progress(run_id, {"type": "status", "message": msg})

    try:
        cfg = svcconfig.load_config(service_id)
        if not cfg:
            return True
        rum_cfg = cfg.get("rum")
        rum_cfg = rum_cfg if isinstance(rum_cfg, dict) else {}
        rum_enabled = svcconfig.is_rum_enabled(cfg)
        if not rum_enabled:
            return True
        token = cfg.get("fastly_api_key", "")

        pinned_version = rum_cfg.get("faro_version")
        if not pinned_version:
            # Self-heal: a service enabled before every enable path pinned a
            # version by default. Adopt the default rather than leave
            # /js/faro-sdk.js 404ing for every visitor.
            report(f"RUM enabled with no pinned Faro version; adopting default v{DEFAULT_FARO_VERSION}")
            try:
                asyncio.run(
                    download_and_upload_faro(service_id, DEFAULT_FARO_VERSION, token, expected_current_version=None)
                )
                _faro_purge_surrogate_key(service_id, token)
                report(f"Faro v{DEFAULT_FARO_VERSION} adopted and uploaded to FOS")
                pinned_version = DEFAULT_FARO_VERSION
                # Reload: download_and_upload_faro just persisted
                # faro_version/faro_content_hash/faro_fos_etag_md5 via its
                # own save_config call, which the stale in-memory cfg above
                # wouldn't reflect. Without this, the integrity check right
                # below would see no stored etag, treat the bundle it just
                # uploaded as "not intact", and re-upload it a second time
                # this same tick.
                cfg = svcconfig.load_config(service_id) or cfg
            except Exception as e:
                msg = str(e)
                if "403" in msg or "401" in msg or "unauthorized" in msg.lower():
                    logger.warning(
                        "Faro default-version adoption failed for %s (S3/FOS write credentials unauthorized: %s)",
                        service_id,
                        e,
                    )
                else:
                    logger.warning("Faro default-version adoption failed for %s", service_id, exc_info=True)
                return False

            # The bundle now exists in FOS, but the deployed VCL for this
            # service may still be missing the routes for it entirely (see
            # the docstring above) — reconcile now so /js/faro-sdk.js
            # actually resolves. Best-effort: a reconcile failure here must
            # not abort the bundle-ingest cron tick; the next tick retries
            # since pinned_version is now set but the live VCL wasn't
            # reconciled to match, so `rum_vcl_fingerprint`-based drift
            # detection (/rum/status) will keep reporting drift until an
            # operator-triggered reconcile (or a future tick, if this is
            # ever made retryable) succeeds.
            try:
                from backend.provision.declarative.reconciler import reconcile_vcl_state
                from backend.provision.rum_orchestrator_v2 import rum_vcl_fingerprint

                reconcile_vcl_state(service_id, token, dry_run=False, activate=True)
                report(f"VCL reconciled: /js/faro-sdk.js now routes to v{DEFAULT_FARO_VERSION}")
                # #2 audit finding: refresh the stored fingerprint now that the
                # live VCL actually matches what the generator produces for
                # this pin — without this, /rum/status's vcl_drift would keep
                # reporting drift forever after this self-heal, even though
                # it just successfully reconciled. Mirrors the refresh
                # upgrade_faro_version does after its own activation.
                svcconfig.mutate_config(
                    service_id, lambda c: c.__setitem__("rum_vcl_sha", rum_vcl_fingerprint(service_id, c))
                )
            except Exception:
                logger.warning(
                    "Faro default-version VCL reconcile failed for %s "
                    "(bundle uploaded to FOS, but /js/faro-sdk.js may still 404 until the next successful reconcile)",
                    service_id,
                    exc_info=True,
                )

        # 1. Cheap integrity check — every tick.
        if not faro_bundle_intact(cfg, pinned_version):
            report(f"Faro bundle v{pinned_version} missing/corrupt in FOS, restoring")
            try:
                asyncio.run(
                    download_and_upload_faro(service_id, pinned_version, token, expected_current_version=pinned_version)
                )
                _faro_purge_surrogate_key(service_id, token)
                report(f"Faro bundle v{pinned_version} restored to FOS")
            except Exception as e:
                msg = str(e)
                if "403" in msg or "401" in msg or "unauthorized" in msg.lower():
                    logger.warning(
                        "Faro bundle restore failed for %s (S3/FOS write credentials unauthorized: %s)",
                        service_id,
                        e,
                    )
                else:
                    logger.warning("Faro bundle restore failed for %s", service_id, exc_info=True)
                return False

        # 2. Throttled upstream drift check. Reload cfg first in case the
        # restore above just rewrote faro_content_hash, so the timestamp
        # write below doesn't clobber it with a stale in-memory copy.
        cfg = svcconfig.load_config(service_id) or cfg
        rum_cfg = dict(cfg.get("rum") or {})
        last_check = rum_cfg.get("faro_last_upstream_check") or 0
        check_interval_s = rum_cfg.get("faro_upstream_check_hours", 24) * 3600
        now = time.time()

        if now - last_check >= check_interval_s:
            drift = False
            try:
                drift = asyncio.run(detect_faro_version_change(service_id, pinned_version))
            except Exception:
                logger.warning("Faro upstream drift check failed for %s", service_id, exc_info=True)
            finally:
                # Write the attempt timestamp even on failure — otherwise a
                # persistently failing upstream turns into a per-tick
                # download storm instead of once per window.
                svcconfig.mutate_config(
                    service_id,
                    lambda c: c.__setitem__("rum", {**(c.get("rum") or {}), "faro_last_upstream_check": now}),
                )

            if drift:
                report(f"Faro upstream drift detected for v{pinned_version}, re-syncing")
                try:
                    asyncio.run(
                        download_and_upload_faro(
                            service_id, pinned_version, token, expected_current_version=pinned_version
                        )
                    )
                    _faro_purge_surrogate_key(service_id, token)
                    report(f"Faro bundle v{pinned_version} re-synced from upstream")
                except Exception:
                    logger.warning("Faro upstream re-sync failed for %s", service_id, exc_info=True)
                    return False
        return True
    except Exception:
        logger.warning("Faro reconcile failed for %s", service_id, exc_info=True)
        return False


@cron_task("cron_rum_sync", job_name="rum_sync")
def _run_rum_sync(service_id: str, **kwargs) -> None:
    """Sync RUM beacon logs from FOS raw/rum/ into local DuckDB tables.

    Calls ingest_rum_logs generator which handles orphan-row safety internally.
    """
    # No start-of-tick active-request gate here (mirroring 5a7541fd): a deferred
    # tick waits a full interval, and dashboard/SSE polling kept it tripped.
    logger.info(f"RUM sync starting for {service_id}")

    is_manual = kwargs.get("is_manual", False) or kwargs.get("run_id") is not None
    pass_max_seconds = 20 if not is_manual else 240

    from backend.core import metadata
    from backend.cron.lifecycle import cron_run
    from backend.cron_progress import add_progress

    had_warning = False
    # The wrapper owns the lease + progress lifecycle; ingest_rum_logs reuses the
    # leased run_id and still writes its own terminal cron_runs rows.
    # The backstop flips a row ingest left in ``running`` (e.g. the body raised before
    # ingest_rum_logs wrote its terminal row) instead of waiting for the next tick's reap.
    with cron_run(
        service_id,
        "rum_sync",
        run_id=kwargs.get("run_id"),
        include_log_output=False,
        finalize_running_backstop=True,
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id
        try:
            for event in ingest_rum_logs(service_id, max_seconds=pass_max_seconds, run_id=run_id):
                if event[0] == "started":
                    add_progress(run_id, {"type": "status", "message": f"RUM sync starting for {service_id}"})
                    if not _reconcile_faro_bundle(service_id, run_id):
                        had_warning = True
                elif event[0] == "file_done":
                    _, filename, count = event
                    msg = f"{filename}: {count} rows"
                    logger.debug(f"  {msg}")
                    add_progress(run_id, {"type": "status", "message": msg})
                elif event[0] == "error":
                    _, location, msg = event
                    logger.warning(f"  Error in {location}: {msg}")
                    add_progress(run_id, {"type": "error", "message": f"Error in {location}: {msg}"})
                elif event[0] == "cleanup_done":
                    _, cleanup_files, cleanup_bytes = event
                    msg = f"RUM cleanup deleted {cleanup_files} files, freed {cleanup_bytes / (1024 * 1024):.2f} MB"
                    logger.info(msg)
                    add_progress(run_id, {"type": "status", "message": msg})
                elif event[0] == "done":
                    _, total = event
                    msg = f"RUM sync complete: {total} total rows"
                    if had_warning:
                        msg += " (with warnings)"
                    logger.info(msg)
                    add_progress(run_id, {"type": "status", "message": msg})
                    if had_warning:
                        try:
                            con = metadata.get_con(service_id)
                            con.execute(
                                "UPDATE cron_runs SET status = 'warning' "
                                "WHERE id = ? AND service_id = ? AND status = 'success'",
                                (run_id, service_id),
                            )
                            con.commit()
                        except Exception:
                            logger.warning(
                                "Could not mark RUM cron run as warning",
                                extra={"service_id": service_id, "run_id": run_id},
                                exc_info=True,
                            )

                    if total > 0:
                        try:
                            from backend.cron.jobs.rum_commit import _run_rum_commit

                            logger.info(
                                "⚡ [rum_sync] %s: triggering post-sync RUM commit for %d new rows", service_id, total
                            )
                            _run_rum_commit(service_id)
                        except Exception as commit_err:
                            logger.warning("RUM post-sync commit failed for %s: %s", service_id, commit_err)

        except Exception as e:
            logger.error(f"RUM sync failed: {e}", exc_info=True)
            add_progress(run_id, {"type": "error", "message": f"RUM sync failed: {e}"})


# ── Celery/ledger-mode RUM ingest cron jobs (formerly backend.cron.jobs.rum_ledger) ──


@cron_task("cron_rum_discovery", job_name="rum_discovery")
def _run_rum_discovery_cron(service_id: str, run_id: int | None = None) -> None:
    """Discover new ``raw/rum/`` beacon files and dispatch batched
    ``convert_batch_rum_files`` calls covering them. Runs inline so the
    cron_runs row carries the real per-tick outcome instead of a fake instant
    success.
    """
    from backend.cron.scheduler import dev_mode_no_crons

    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — RUM discovery refused.", service_id)
        return

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return
    rum_cfg = cfg.get("rum")
    rum_cfg = rum_cfg if isinstance(rum_cfg, dict) else {}
    rum_enabled = svcconfig.is_rum_enabled(cfg)
    if not rum_enabled:
        return

    src = get_source_for_service(service_id)
    if src is None:
        return
    if src.get("access_level") == "read_only":
        return
    if not svcconfig.is_high_scale_mode(src):
        return

    with cron_run(
        service_id,
        "rum_discovery",
        source=src,
        run_id=run_id,
        error_summary="RUM discovery failed",
        finalize_running_backstop=True,
        include_log_output=False,
        skip_log_prefix="rum_discovery",
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id

        faro_ok = True
        faro_err = None
        try:
            faro_ok = _reconcile_faro_bundle(service_id, run_id)
        except Exception as fe:
            faro_ok = False
            faro_err = str(fe)
            logger.warning(
                "[rum_discovery] %s: Faro bundle reconcile failed (non-fatal): %s", service_id, fe, exc_info=True
            )

        if not svcconfig.CELERY_BROKER_URL:
            run.fail(
                "Celery RUM ingest misconfigured: no broker URL",
                error_message="DEPLOYMENT_MODE=high_scale requires CELERY_BROKER_URL",
            )
            return

        from backend.core.ingest import discover_rum_prefix
        from backend.provision.log_paths import rum_minute_list_prefix

        now = datetime.now(UTC)
        discovered = 0
        for i in range(5):
            prefix = rum_minute_list_prefix(now - timedelta(minutes=i))
            discovered += discover_rum_prefix(service_id, prefix_subpath=prefix, run_id=run_id)

        if not faro_ok:
            status = "warning"
            warn_msg = f"Faro bundle reconcile issue ({faro_err or 'drift/restore warning'})"
            summary = (
                f"Discovered {discovered} new RUM file(s); dispatched to ingest workers ({warn_msg})"
                if discovered
                else f"No new RUM files ({warn_msg})"
            )
            error_message = faro_err or "Faro bundle reconcile failed"
        else:
            status = "success"
            summary = (
                f"Discovered {discovered} new RUM file(s); dispatched to ingest workers"
                if discovered
                else "No new RUM files"
            )
            error_message = None

        run.record(status, summary=summary, error_message=error_message, files_downloaded=discovered)


def _run_ledger_sweep_impl(
    service_id: str,
    run_id: int | None,
    *,
    job_name: str,
    sweep_fn,
    label: str,
    require_rum: bool = False,
    finalize_running_backstop: bool = False,
    include_warning_error_message: bool = False,
) -> None:
    from backend.cron.scheduler import dev_mode_no_crons

    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — %s refused.", service_id, label)
        return

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return
    if require_rum and not svcconfig.is_rum_enabled(cfg):
        return
    src = get_source_for_service(service_id)
    if src is None or not svcconfig.is_high_scale_mode(src) or src.get("access_level") == "read_only":
        return

    row_prefix = "RUM " if require_rum else ""
    with cron_run(
        service_id,
        job_name,
        source=src,
        run_id=run_id,
        error_summary=f"{label[0].upper()}{label[1:]} failed",
        finalize_duration=True,
        finalize_running_backstop=finalize_running_backstop,
        include_log_output=False,
    ) as run:
        if not run.ok:
            return
        run.log_progress({"type": "status", "message": f"Starting {label} for {service_id}..."})

        summary = sweep_fn(service_id, run_id=run.run_id)
        warnings = []
        if not summary.get("broker_ok", True):
            warnings.append("Celery broker/queue depth probe failed")
        if summary.get("dead_letter", 0) > 0:
            warnings.append(f"{summary['dead_letter']} dead-letter/quarantined {row_prefix}row(s)")
        run_status = "warning" if warnings else "success"

        summary_msg = (
            f"reclaimed={summary.get('reclaimed', 0)} redispatched={summary.get('redispatched', 0)} "
            f"discovered={summary.get('discovered', 0)}"
        )
        if warnings:
            summary_msg += f" ({'; '.join(warnings)})"

        record_kwargs: dict = {
            "summary": summary_msg,
            "files_downloaded": summary.get("discovered", 0),
        }
        if include_warning_error_message:
            record_kwargs["error_message"] = "; ".join(warnings) if warnings else None
        run.record(run_status, **record_kwargs)
        run.log_progress({"type": "done", "message": summary_msg})


@cron_task("ledger_sweep", job_name="ledger_sweep")
def _run_ledger_sweep(service_id: str, run_id: int | None = None) -> None:
    """Celery-mode crash net: reclaim stale ledger claims, re-dispatch stuck
    rows, and diff a lookback FOS LIST against the ledger."""
    from backend.core.ingest import sweep_ledger_once

    _run_ledger_sweep_impl(
        service_id,
        run_id,
        job_name="ledger_sweep",
        sweep_fn=sweep_ledger_once,
        label="ledger sweep",
    )


@cron_task("ledger_rum_sweep", job_name="ledger_rum_sweep")
def _run_rum_ledger_sweep(service_id: str, run_id: int | None = None) -> None:
    """Celery-mode crash net for the RUM ledger pipeline."""
    from backend.core.ingest import sweep_rum_ledger_once

    _run_ledger_sweep_impl(
        service_id,
        run_id,
        job_name="ledger_rum_sweep",
        sweep_fn=sweep_rum_ledger_once,
        label="RUM ledger sweep",
        require_rum=True,
        finalize_running_backstop=True,
        include_warning_error_message=True,
    )


# ── RUM commit job (folded from backend.cron.jobs.rum_commit) ────────────────


@cron_task("cron_rum_commit", job_name="rum_commit")
def _run_rum_commit(service_id: str, force: bool = False, run_id: int | None = None, **kwargs) -> None:
    """Compact RUM tables from DuckDB cache to Iceberg/FOS."""
    from backend import config as svcconfig
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import get_source_for_service
    from backend.utils.telemetry_proxy import _BOTO3_CALLER_HINT

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    if src.get("access_level") == "read_only" and not force:
        return

    prov = cfg.get("provisioning", {})
    sync_cfg = prov.get("cron_sync", {})
    if not sync_cfg.get("enabled", True) and not force:
        return

    # No start-of-tick active-request gate here (mirroring 5a7541fd): a deferred
    # tick waits a full interval, and dashboard/SSE polling kept it tripped.
    from backend.core.duckdb import _cache_dir as _commit_cache_dir
    from backend.cron.lifecycle import cron_run
    from backend.cron.scheduler import _check_disk_space

    with cron_run(
        service_id,
        "rum_commit",
        source=src,
        run_id=run_id,
        error_summary="RUM commit failed",
        suppress_exceptions=False,
        finalize_running_backstop=True,
        include_log_output=False,
        skip_log_prefix="rum_commit",
    ) as run:
        if not run.ok:
            return

        ok, disk_msg = _check_disk_space(_commit_cache_dir(src), service_id, "rum_commit")
        if not ok:
            run.fail(f"RUM commit aborted: {disk_msg}", error_message=disk_msg)
            return

        boto3_token = _BOTO3_CALLER_HINT.set("rum_commit")
        try:
            logger.info(f"RUM commit starting for {service_id}")

            from backend.cron.jobs.sync import _buffer_result_error

            total_committed_vitals = 0
            total_committed_errors = 0
            vitals_err = None
            errors_err = None
            vitals_res = {}
            errors_res = {}

            # Commit client_vitals
            try:
                vitals_res = db_iceberg.commit_buffer(src, table_name="client_vitals")
                vitals_err = _buffer_result_error(vitals_res)
                if vitals_res.get("files_committed", 0) > 0:
                    total_committed_vitals = vitals_res.get("rows_committed", 0)
                    # Sync client_vitals view/metadata
                    db_iceberg.sync_data(src, table_name="client_vitals")
                if vitals_err:
                    logger.warning("[rum_commit] %s: client_vitals commit incomplete: %s", service_id, vitals_err)
            except Exception as e:
                vitals_err = "; ".join(part for part in (vitals_err, str(e)) if part)
                logger.warning("[rum_commit] %s: client_vitals commit failed: %s", service_id, e)

            # Commit client_errors
            try:
                errors_res = db_iceberg.commit_buffer(src, table_name="client_errors")
                errors_err = _buffer_result_error(errors_res)
                if errors_res.get("files_committed", 0) > 0:
                    total_committed_errors = errors_res.get("rows_committed", 0)
                    # Sync client_errors view/metadata
                    db_iceberg.sync_data(src, table_name="client_errors")
                if errors_err:
                    logger.warning("[rum_commit] %s: client_errors commit incomplete: %s", service_id, errors_err)
            except Exception as e:
                errors_err = "; ".join(part for part in (errors_err, str(e)) if part)
                logger.warning("[rum_commit] %s: client_errors commit failed: %s", service_id, e)

            # Raw RUM objects may only be deleted after both DuckLake commit paths
            # have completed successfully and their publication is durable.
            if vitals_err is None and errors_err is None:
                from backend.core.ingest import _mark_ledger_published

                _mark_ledger_published(service_id, rum=True)

                # Update/recompute precomputed RUM aggregates
                try:
                    from contextlib import closing
                    from datetime import UTC, datetime, timedelta

                    from backend.core.duckdb import get_connection, rum_source_for
                    from backend.core.iceberg import execute_with_stale_view_retry
                    from backend.core.iceberg._ducklake import _ducklake_attach
                    from backend.core.rollups.rum import (
                        backfill_missing_rum_aggregates,
                        recompute_rum_aggregates,
                    )

                    rum_src = rum_source_for(src)
                    with closing(get_connection(rum_src, read_only=False)) as rum_con:
                        # Attach standard lake catalog so standard client_vitals / client_errors views can resolve
                        try:
                            _ducklake_attach(rum_con, src, read_only=True)
                        except Exception as attach_err:
                            logger.warning(
                                "[rum_commit] %s: Failed to attach lake catalog to RUM connection: %s",
                                service_id,
                                attach_err,
                            )

                        # Find hours that had data in the last 2 hours to do a fast incremental recompute
                        # (48h on every 10s RUM commit caused 48 full client_vitals/client_errors scans per tick).
                        recent_hours = []
                        since = datetime.now(UTC) - timedelta(hours=2)
                        try:
                            res_v = rum_con.execute(
                                "SELECT DISTINCT DATE_TRUNC('hour', timestamp) FROM client_vitals WHERE timestamp >= ?",
                                [since],
                            ).fetchall()
                            recent_hours.extend([r[0] for r in res_v if r[0]])
                        except Exception:
                            pass

                        try:
                            res_e = rum_con.execute(
                                "SELECT DISTINCT DATE_TRUNC('hour', timestamp) FROM client_errors WHERE timestamp >= ?",
                                [since],
                            ).fetchall()
                            recent_hours.extend([r[0] for r in res_e if r[0]])
                        except Exception:
                            pass

                        target_hours = list(set(recent_hours)) if recent_hours else None
                        execute_with_stale_view_retry(
                            rum_con,
                            rum_src,
                            lambda c: recompute_rum_aggregates(c, service_id, hours=target_hours),
                            table_name="client_vitals",
                        )

                        # Also heal any historical hours (up to 30d) missing from rum_vitals_aggregates
                        # or lacking the synthetic 'total_beacons' row added in 3fcbcd27.
                        try:
                            execute_with_stale_view_retry(
                                rum_con,
                                rum_src,
                                lambda c: backfill_missing_rum_aggregates(c, service_id, lookback_days=30),
                                table_name="client_vitals",
                            )
                        except Exception as heal_err:
                            logger.warning(
                                "[rum_commit] %s: Failed to backfill missing RUM aggregate hours: %s",
                                service_id,
                                heal_err,
                            )

                        # Persist RUM freshness/count into the status doc so the
                        # request-ingest status refresh reads it cheaply instead of
                        # opening a nested RUM-lake connection (AGENTS.md Trap #39).
                        try:
                            from backend.core.rollups.rum import rum_count_and_latest

                            rum_cnt, rum_latest = execute_with_stale_view_retry(
                                rum_con, rum_src, rum_count_and_latest, table_name="client_vitals"
                            )
                            rum_latest_iso = (
                                rum_latest.isoformat()
                                if isinstance(rum_latest, datetime)
                                else (str(rum_latest) if rum_latest else None)
                            )
                            svcconfig.update_status(
                                service_id,
                                {
                                    "rum": {
                                        "latest_log_at": rum_latest_iso,
                                        "total_rows": int(rum_cnt or 0),
                                        "last_sync_at": datetime.now(UTC).isoformat(),
                                    }
                                },
                            )
                        except Exception as persist_err:
                            logger.warning("[rum_commit] %s: RUM freshness persist failed: %s", service_id, persist_err)
                except Exception as agg_err:
                    logger.warning(
                        "[rum_commit] %s: RUM aggregates update failed: %s", service_id, agg_err, exc_info=True
                    )

            # Also launch local compaction for BOTH tables with error logging
            try:
                import threading as _t

                from backend.core import local_compaction as _lc

                _t.Thread(
                    target=lambda: _lc.compact_local_partitions(src, table_name="client_vitals"),
                    name=f"local-compact-rum-vitals:{service_id}",
                    daemon=True,
                ).start()
                _t.Thread(
                    target=lambda: _lc.compact_local_partitions(src, table_name="client_errors"),
                    name=f"local-compact-rum-errors:{service_id}",
                    daemon=True,
                ).start()
            except Exception as lc_err:
                logger.warning("[rum_commit] %s: post-commit local compaction failed to launch: %s", service_id, lc_err)

            v_files = vitals_res.get("files_committed", 0)
            e_files = errors_res.get("files_committed", 0)
            total_rows = total_committed_vitals + total_committed_errors

            error_message: str | None = None
            if vitals_err and errors_err:
                status = "error"
                summary = f"RUM commit failed for both tables: vitals ({vitals_err}), errors ({errors_err})"
                error_message = summary
            elif vitals_err or errors_err:
                status = "warning"
                failed_tab = "vitals" if vitals_err else "errors"
                err_details = str(vitals_err or errors_err or "")
                summary = (
                    f"Partial RUM commit: {v_files} vitals ({total_committed_vitals} rows) "
                    f"and {e_files} errors ({total_committed_errors} rows); {failed_tab} failed: {err_details}"
                )
                error_message = err_details
            else:
                status = "success"
                summary = (
                    f"Committed {v_files} vitals files ({total_committed_vitals} rows) "
                    f"and {e_files} errors files ({total_committed_errors} rows)"
                )

            run.record(
                status,
                summary=summary,
                error_message=error_message,
                rows_ingested=total_rows,
            )
            logger.info("RUM commit complete: %s", summary)

            from backend.cron.jobs.sync import publish_sync_status_safe

            publish_sync_status_safe(service_id, "rum_commit")

        finally:
            _BOTO3_CALLER_HINT.reset(boto3_token)


_self_mod = _sys.modules[__name__]
for _alias in ("backend.cron.jobs.rum_ledger", "backend.cron.jobs.rum_commit"):
    _sys.modules[_alias] = _self_mod
_jobs_pkg = _sys.modules.get("backend.cron.jobs")
if _jobs_pkg is not None:
    setattr(_jobs_pkg, "rum_ledger", _self_mod)
    setattr(_jobs_pkg, "rum_commit", _self_mod)
_backend_pkg = _sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES["backend.cron.jobs.rum_commit"] = "backend.cron.jobs.rum_sync"
