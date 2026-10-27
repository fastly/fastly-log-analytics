"""RUM DuckLake buffer commit cron job (`_run_rum_commit`)."""

from __future__ import annotations

import logging
import threading
from contextlib import closing
from datetime import UTC, datetime, timedelta

from backend import config as svcconfig
from backend.core.storage.backup import schedule_cloud_push
from backend.cron.jobs._common import (
    _buffer_result_error,
    _orig_publish_sync_status_safe,
    _resolve_sync_hook,
    publish_sync_status_safe,
)
from backend.cron.scheduler import _check_disk_space, cron_run, cron_task

logger = logging.getLogger("backend.scheduler")

_orig_check_disk_space = _check_disk_space
_orig_schedule_cloud_push = schedule_cloud_push


@cron_task("cron_rum_commit", job_name="rum_commit")
def _run_rum_commit(service_id: str, force: bool = False, run_id: int | None = None, **kwargs) -> None:
    """Compact RUM tables from DuckDB cache to DuckLake."""
    from backend.core import storage as db_storage
    from backend.core.duckdb import _cache_dir as _commit_cache_dir
    from backend.core.duckdb import get_source_for_service
    from backend.utils.telemetry_proxy import _BOTO3_CALLER_HINT

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return
    src = get_source_for_service(service_id)
    if src is None:
        return
    if not cfg.get("provisioning", {}).get("cron_sync", {}).get("enabled", True) and not force:
        return

    def _commit_rum_table(table_name: str) -> tuple[dict, int, str | None]:
        res: dict = {}
        committed_rows = 0
        err: str | None = None
        try:
            res = db_storage.commit_buffer(src, table_name=table_name)
            err = _buffer_result_error(res)
            if res.get("files_committed", 0) > 0:
                committed_rows = res.get("rows_committed", 0)
            if err:
                logger.warning("[rum_commit] %s: %s commit incomplete: %s", service_id, table_name, err)
        except Exception as e:
            err = "; ".join(part for part in (err, str(e)) if part)
            logger.warning("[rum_commit] %s: %s commit failed: %s", service_id, table_name, e)
        return res, committed_rows, err

    # No start-of-tick active-request gate here (mirroring 5a7541fd): a deferred
    # tick waits a full interval, and dashboard/SSE polling kept it tripped.
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

        disk_check_fn = _resolve_sync_hook("_check_disk_space", _check_disk_space, _orig_check_disk_space)
        ok, disk_msg = disk_check_fn(_commit_cache_dir(src), service_id, "rum_commit")
        if not ok:
            run.fail(f"RUM commit aborted: {disk_msg}", error_message=disk_msg)
            return

        boto3_token = _BOTO3_CALLER_HINT.set("rum_commit")
        try:
            logger.info(f"RUM commit starting for {service_id}")
            vitals_res, total_committed_vitals, vitals_err = _commit_rum_table("client_vitals")
            errors_res, total_committed_errors, errors_err = _commit_rum_table("client_errors")

            # Raw RUM objects may only be deleted after both DuckLake commit paths
            # have completed successfully and their publication is durable.
            if total_committed_vitals or total_committed_errors:
                push_fn = _resolve_sync_hook("schedule_cloud_push", schedule_cloud_push, _orig_schedule_cloud_push)
                push_fn(src, ("client_vitals", "client_errors"))
            if vitals_err is None and errors_err is None:
                from backend.core.ingest import _mark_ledger_published

                _mark_ledger_published(service_id, rum=True)

                # Update/recompute precomputed RUM aggregates
                try:
                    from backend.core.duckdb import get_connection, rum_source_for
                    from backend.core.rollups.rum import (
                        backfill_missing_rum_aggregates,
                        recompute_rum_aggregates,
                        rum_count_and_latest,
                    )
                    from backend.core.storage import execute_with_stale_view_retry
                    from backend.core.storage._ducklake import _ducklake_attach

                    rum_src = rum_source_for(src)
                    with closing(get_connection(rum_src, read_only=False)) as rum_con:
                        try:
                            _ducklake_attach(rum_con, src, read_only=True)
                        except Exception as attach_err:
                            logger.warning(
                                "[rum_commit] %s: Failed to attach lake catalog to RUM connection: %s",
                                service_id,
                                attach_err,
                            )

                        # Find hours with data in the last 2 hours for fast incremental recompute.
                        recent_hours = []
                        since = datetime.now(UTC) - timedelta(hours=2)
                        for tbl in ("client_vitals", "client_errors"):
                            try:
                                rows = rum_con.execute(
                                    f"SELECT DISTINCT DATE_TRUNC('hour', timestamp) FROM {tbl} WHERE timestamp >= ?",
                                    [since],
                                ).fetchall()
                                recent_hours.extend([r[0] for r in rows if r[0]])
                            except Exception:
                                pass

                        target_hours = list(set(recent_hours)) if recent_hours else None
                        execute_with_stale_view_retry(
                            rum_con,
                            rum_src,
                            lambda c: recompute_rum_aggregates(c, service_id, hours=target_hours),
                            table_name="client_vitals",
                        )

                        # Heal historical hours (up to 30d) missing from rum_vitals_aggregates.
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

                        # Persist RUM freshness/count into status doc (AGENTS.md Trap #39).
                        try:
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

            # Launch local compaction for BOTH tables with error logging
            try:
                from backend.core import local_compaction as _lc  # type: ignore[attr-defined]

                for short_name, tbl in (("vitals", "client_vitals"), ("errors", "client_errors")):
                    threading.Thread(
                        target=lambda t=tbl: _lc.compact_local_partitions(src, table_name=t),
                        name=f"local-compact-rum-{short_name}:{service_id}",
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

            run.record(status, summary=summary, error_message=error_message, rows_ingested=total_rows)
            logger.info("RUM commit complete: %s", summary)
            publish_fn = _resolve_sync_hook(
                "publish_sync_status_safe", publish_sync_status_safe, _orig_publish_sync_status_safe
            )
            publish_fn(service_id, "rum_commit")
        finally:
            _BOTO3_CALLER_HINT.reset(boto3_token)
