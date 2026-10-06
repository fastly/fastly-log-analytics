"""RUM commit cron job — compact local DuckDB tables to Iceberg.

Registered in scheduler as cron_rum_commit.
"""

from __future__ import annotations

import logging
import time

from backend.cron.decorators import cron_task

logger = logging.getLogger(__name__)


@cron_task("cron_rum_commit", job_name="rum_commit")
def _run_rum_commit(service_id: str, force: bool = False, run_id: int | None = None, **kwargs) -> None:
    """Compact RUM tables from DuckDB cache to Iceberg/FOS."""
    from backend import config as svcconfig
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import (
        finalize_cron_run_if_running,
        get_source_for_service,
        log_cron_run,
        start_cron_run,
    )
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

    is_manual = kwargs.get("is_manual", False) or run_id is not None
    if not is_manual and not force:
        from backend.utils.active_requests import should_defer_cron

        if should_defer_cron("rum_commit", service_id):
            logger.info("⏸️ [rum_commit] %s: active queries running, deferring RUM commit tick", service_id)
            return

    try:
        if run_id is None:
            run_id = start_cron_run(src, "rum_commit")
    except RuntimeError as e:
        logger.info("[rum_commit] %s: skipping — %s", service_id, str(e))
        return

    from backend.core.duckdb import _cache_dir as _commit_cache_dir
    from backend.cron.scheduler import _check_disk_space

    ok, disk_msg = _check_disk_space(_commit_cache_dir(src), service_id, "rum_commit")
    if not ok:
        log_cron_run(
            src,
            "rum_commit",
            0.0,
            "error",
            run_id=run_id,
            error_message=disk_msg,
            summary=f"RUM commit aborted: {disk_msg}",
        )
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="rum_commit")

    start_time = time.time()
    boto3_token = _BOTO3_CALLER_HINT.set("rum_commit")

    try:
        logger.info(f"RUM commit starting for {service_id}")

        total_committed_vitals = 0
        total_committed_errors = 0
        vitals_err = None
        errors_err = None
        vitals_res = {}
        errors_res = {}

        def _buffer_result_error(result):
            errors = []
            if result.get("error"):
                errors.append(str(result["error"]))
            quarantined = int(result.get("quarantined_files", 0) or 0)
            if quarantined:
                errors.append(f"{quarantined} unreadable buffer file(s) quarantined")
            return "; ".join(errors) or None

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
                from backend.core.rollups.rum import recompute_rum_aggregates

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

                    # Find hours that had data in the last 48 hours to do a fast incremental recompute
                    recent_hours = []
                    since = datetime.now(UTC) - timedelta(hours=48)
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
                logger.warning("[rum_commit] %s: RUM aggregates update failed: %s", service_id, agg_err, exc_info=True)

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

        duration = time.time() - start_time
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

        log_cron_run(
            src,
            "rum_commit",
            duration,
            status,
            run_id=run_id,
            rows_ingested=total_rows,
            summary=summary,
            error_message=error_message,
        )
        logger.info("RUM commit complete: %s", summary)

    except Exception as e:
        logger.error(f"RUM commit failed: {e}", exc_info=True)
        duration = time.time() - start_time
        log_cron_run(
            src,
            "rum_commit",
            duration,
            "error",
            error_message=str(e),
            run_id=run_id,
        )
        raise
    finally:
        _BOTO3_CALLER_HINT.reset(boto3_token)
        end_progress(run_id)
        if run_id is not None:
            finalize_cron_run_if_running(src, "rum_commit", run_id)
