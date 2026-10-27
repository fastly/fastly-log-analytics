"""RUM beacon log sync and Faro bundle reconciliation cron job."""

from __future__ import annotations

import logging
import sys as _sys
import time

from backend import config as svcconfig
from backend.core.rum_ingest import ingest_rum_logs
from backend.cron.jobs._common import _resolve_sync_hook
from backend.cron.scheduler import cron_run, cron_task

logger = logging.getLogger("backend.scheduler")

_orig_ingest_rum_logs = ingest_rum_logs


def _faro_purge_surrogate_key(logging_service_id: str, token: str) -> None:
    """Fire-and-forget surrogate-key purge on the logging service after a Faro bundle (re)upload.

    Purges on ``logging_service_id`` — NOT the CDN service fronting FOS for
    log downloads (F-1 audit finding). ``Surrogate-Key: rum-faro-sdk`` is
    set only in the RUM fetch-cache snippet, which is deployed to the
    instrumented *logging* service, never the CDN service — a purge
    targeting the CDN service can never match anything there. Fire-and-forget:
    a missing service id/token or a purge failure must never surface — the
    FOS upload it follows already succeeded.
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

    def _upload_and_purge(
        ver: str, expected_ver: str | None, ok_msg: str, err_label: str, *, check_auth: bool = True
    ) -> bool:
        try:
            asyncio.run(download_and_upload_faro(service_id, ver, token, expected_current_version=expected_ver))
            _faro_purge_surrogate_key(service_id, token)
            report(ok_msg)
            return True
        except Exception as e:
            msg = str(e)
            if check_auth and ("403" in msg or "401" in msg or "unauthorized" in msg.lower()):
                logger.warning("%s failed for %s (S3/FOS write credentials unauthorized: %s)", err_label, service_id, e)
            else:
                logger.warning("%s failed for %s", err_label, service_id, exc_info=True)
            return False

    try:
        cfg = svcconfig.load_config(service_id)
        if not cfg or not svcconfig.is_rum_enabled(cfg):
            return True
        rum_cfg = cfg.get("rum")
        token = cfg.get("fastly_api_key", "")
        pinned_version = (rum_cfg if isinstance(rum_cfg, dict) else {}).get("faro_version")
        if not pinned_version:
            # Self-heal: adopt the default rather than leave /js/faro-sdk.js 404ing.
            report(f"RUM enabled with no pinned Faro version; adopting default v{DEFAULT_FARO_VERSION}")
            if not _upload_and_purge(
                DEFAULT_FARO_VERSION,
                None,
                f"Faro v{DEFAULT_FARO_VERSION} adopted and uploaded to FOS",
                "Faro default-version adoption",
            ):
                return False
            pinned_version = DEFAULT_FARO_VERSION
            # Reload so the integrity check below sees the newly persisted etag/hash.
            cfg = svcconfig.load_config(service_id) or cfg

            # Reconcile VCL so /js/faro-sdk.js routes to the newly adopted bundle (best-effort).
            try:
                from backend.provision.declarative.reconciler import reconcile_vcl_state
                from backend.provision.rum_orchestrator_v2 import rum_vcl_fingerprint

                reconcile_vcl_state(service_id, token, dry_run=False, activate=True)
                report(f"VCL reconciled: /js/faro-sdk.js now routes to v{DEFAULT_FARO_VERSION}")
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
            if not _upload_and_purge(
                pinned_version,
                pinned_version,
                f"Faro bundle v{pinned_version} restored to FOS",
                "Faro bundle restore",
            ):
                return False

        # 2. Throttled upstream drift check. Reload cfg first so the timestamp write
        # below doesn't clobber a freshly restored faro_content_hash.
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
                # Write attempt timestamp even on failure to avoid per-tick download storms.
                svcconfig.mutate_config(
                    service_id,
                    lambda c: c.__setitem__("rum", {**(c.get("rum") or {}), "faro_last_upstream_check": now}),
                )

            if drift:
                report(f"Faro upstream drift detected for v{pinned_version}, re-syncing")
                if not _upload_and_purge(
                    pinned_version,
                    pinned_version,
                    f"Faro bundle v{pinned_version} re-synced from upstream",
                    "Faro upstream re-sync",
                    check_auth=False,
                ):
                    return False
        return True
    except Exception:
        logger.warning("Faro reconcile failed for %s", service_id, exc_info=True)
        return False


_orig_reconcile_faro_bundle = _reconcile_faro_bundle


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
    from backend.cron_progress import add_progress

    ingest_fn = _resolve_sync_hook("ingest_rum_logs", ingest_rum_logs, _orig_ingest_rum_logs)
    reconcile_fn = _resolve_sync_hook("_reconcile_faro_bundle", _reconcile_faro_bundle, _orig_reconcile_faro_bundle)

    had_warning = False
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
            for event in ingest_fn(service_id, max_seconds=pass_max_seconds, run_id=run_id):
                kind = event[0]
                if kind == "started":
                    add_progress(run_id, {"type": "status", "message": f"RUM sync starting for {service_id}"})
                    if not reconcile_fn(service_id, run_id):
                        had_warning = True
                elif kind == "file_done":
                    msg = f"{event[1]}: {event[2]} rows"
                    logger.debug(f"  {msg}")
                    add_progress(run_id, {"type": "status", "message": msg})
                elif kind == "error":
                    err_msg = f"Error in {event[1]}: {event[2]}"
                    logger.warning(f"  {err_msg}")
                    add_progress(run_id, {"type": "error", "message": err_msg})
                elif kind == "cleanup_done":
                    msg = f"RUM cleanup deleted {event[1]} files, freed {event[2] / (1024 * 1024):.2f} MB"
                    logger.info(msg)
                    add_progress(run_id, {"type": "status", "message": msg})
                elif kind == "done":
                    total = event[1]
                    msg = f"RUM sync complete: {total} total rows" + (" (with warnings)" if had_warning else "")
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
                            logger.info(
                                "⚡ [rum_sync] %s: triggering post-sync RUM commit for %d new rows", service_id, total
                            )
                            from backend.cron.jobs import rum_commit as _rc_mod

                            commit_fn = _rc_mod._run_rum_commit
                            for mod_name in ("backend.cron.jobs.rum_sync", "backend.cron.jobs.sync"):
                                mod = _sys.modules.get(mod_name)
                                if mod is not None and "_run_rum_commit" in mod.__dict__:
                                    commit_fn = mod.__dict__["_run_rum_commit"]
                                    break
                            commit_fn(service_id)
                        except Exception as commit_err:
                            logger.warning("RUM post-sync commit failed for %s: %s", service_id, commit_err)
        except Exception as e:
            logger.error(f"RUM sync failed: {e}", exc_info=True)
            add_progress(run_id, {"type": "error", "message": f"RUM sync failed: {e}"})
