"""RUM sync cron job — ingest raw beacon logs from FOS.

Registered in scheduler as cron_rum_sync. Runs periodically (configurable interval).
"""

from __future__ import annotations

import logging
import time

from backend.core.rum_ingest import ingest_rum_logs
from backend.cron.decorators import cron_task

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


def _faro_bundle_intact(cfg: dict, pinned_version: str) -> bool:
    """Cheap SigV4 HEAD check: True only if FOS already holds the pinned
    bundle with an ETag matching the stored content hash.

    Any failure (missing FOS creds, no stored hash, 404/non-200, mismatched
    ETag, network error) returns False so the caller re-uploads. This is the
    every-tick integrity check — it must stay cheap (no unpkg traffic on the
    steady-state path) and must never raise.

    Compares against ``faro_fos_etag_md5`` (not ``faro_content_hash``): a
    single-part PUT's S3/FOS ``ETag`` is protocol-mandated MD5 of the
    object bytes, so this check needs an MD5 value specifically —
    independent of whatever algorithm ``faro_content_hash`` (the
    content-drift marker ``detect_faro_version_change`` reads) uses.
    """
    import certifi
    import httpx

    from backend.core.fastly.utils import region_endpoint
    from backend.provision.rum_assets import FARO_KEY_PREFIX, FARO_KEY_SUFFIX
    from backend.utils.fos_signing import sign_fos_request

    access_key = cfg.get("fos_access_key_id")
    secret_key = cfg.get("fos_secret_access_key")
    bucket = cfg.get("fos_bucket")
    region = cfg.get("fos_region", "us-east-1")
    stored_hash = (cfg.get("rum") or {}).get("faro_fos_etag_md5")

    if not all([access_key, secret_key, bucket]) or not stored_hash:
        return False

    assert access_key is not None and secret_key is not None

    fos_key = f"{FARO_KEY_PREFIX}{pinned_version}{FARO_KEY_SUFFIX}"
    fos_host = region_endpoint(region)
    fos_url = f"https://{fos_host}/{bucket}/{fos_key}"

    try:
        headers = sign_fos_request(
            method="HEAD",
            url=fos_url,
            headers={},
            body=b"",
            access_key_id=access_key,
            secret_access_key=secret_key,
            region=region,
        )
        with httpx.Client(verify=certifi.where()) as client:
            response = client.head(fos_url, headers=headers, timeout=10.0)
        if response.status_code != 200:
            return False
        etag = response.headers.get("ETag", "").strip('"')
        return etag == stored_hash
    except Exception:
        logger.warning("Faro FOS integrity HEAD check failed (treating as needs-restore)", exc_info=True)
        return False


def _reconcile_faro_bundle(service_id: str, run_id: int | None) -> None:
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
    from backend.provision.rum_assets import detect_faro_version_change, download_and_upload_faro

    def report(msg: str) -> None:
        logger.info(msg)
        if run_id:
            add_progress(run_id, {"type": "status", "message": msg})

    try:
        cfg = svcconfig.load_config(service_id)
        if not cfg:
            return
        rum_cfg = cfg.get("rum")
        rum_cfg = rum_cfg if isinstance(rum_cfg, dict) else {}
        # Mirrors the OR-pattern used elsewhere (e.g. routers/rum.py,
        # cron/scheduler.py): the declarative enable_rum() path only ever
        # sets the top-level rum_enabled flag, while the older imperative
        # provisioning path also sets rum.enabled — either one means RUM is
        # actually on for this service.
        if not (cfg.get("rum_enabled") or rum_cfg.get("enabled")):
            return
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
            except Exception:
                logger.warning("Faro default-version adoption failed for %s", service_id, exc_info=True)
                return

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
                cfg = svcconfig.load_config(service_id) or cfg
                cfg["rum_vcl_sha"] = rum_vcl_fingerprint(service_id, cfg)
                svcconfig.save_config(service_id, cfg)
            except Exception:
                logger.warning(
                    "Faro default-version VCL reconcile failed for %s "
                    "(bundle uploaded to FOS, but /js/faro-sdk.js may still 404 until the next successful reconcile)",
                    service_id,
                    exc_info=True,
                )

        # 1. Cheap integrity check — every tick.
        if not _faro_bundle_intact(cfg, pinned_version):
            report(f"Faro bundle v{pinned_version} missing/corrupt in FOS, restoring")
            try:
                asyncio.run(
                    download_and_upload_faro(service_id, pinned_version, token, expected_current_version=pinned_version)
                )
                _faro_purge_surrogate_key(service_id, token)
                report(f"Faro bundle v{pinned_version} restored to FOS")
            except Exception:
                logger.warning("Faro bundle restore failed for %s", service_id, exc_info=True)

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
                cfg = svcconfig.load_config(service_id) or cfg
                rum_cfg = dict(cfg.get("rum") or {})
                rum_cfg["faro_last_upstream_check"] = now
                cfg["rum"] = rum_cfg
                svcconfig.save_config(service_id, cfg)

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
    except Exception:
        logger.warning("Faro reconcile failed for %s", service_id, exc_info=True)


@cron_task("cron_rum_sync")
def _run_rum_sync(service_id: str, **kwargs) -> None:
    """Sync RUM beacon logs from FOS raw/rum/ into local DuckDB tables.

    Calls ingest_rum_logs generator which handles orphan-row safety internally.
    """
    logger.info(f"RUM sync starting for {service_id}")

    from backend.cron_progress import add_progress, cleanup_progress_and_reap, end_progress, start_progress

    run_id = None
    try:
        for event in ingest_rum_logs(service_id):
            if event[0] == "started":
                run_id = event[1]
                start_progress(run_id, service_id=service_id, task="rum_sync")
                add_progress(run_id, {"type": "status", "message": f"RUM sync starting for {service_id}"})
                _reconcile_faro_bundle(service_id, run_id)
            elif event[0] == "file_done":
                _, filename, count = event
                msg = f"{filename}: {count} rows"
                logger.debug(f"  {msg}")
                if run_id:
                    add_progress(run_id, {"type": "status", "message": msg})
            elif event[0] == "error":
                _, location, msg = event
                logger.warning(f"  Error in {location}: {msg}")
                if run_id:
                    add_progress(run_id, {"type": "error", "message": f"Error in {location}: {msg}"})
            elif event[0] == "cleanup_done":
                _, cleanup_files, cleanup_bytes = event
                msg = f"RUM cleanup deleted {cleanup_files} files, freed {cleanup_bytes / (1024 * 1024):.2f} MB"
                logger.info(msg)
                if run_id:
                    add_progress(run_id, {"type": "status", "message": msg})
            elif event[0] == "done":
                _, total = event
                msg = f"RUM sync complete: {total} total rows"
                logger.info(msg)
                if run_id:
                    add_progress(run_id, {"type": "status", "message": msg})
                    end_progress(run_id)

    except Exception as e:
        logger.error(f"RUM sync failed: {e}", exc_info=True)
        if run_id:
            add_progress(run_id, {"type": "error", "message": f"RUM sync failed: {e}"})
            end_progress(run_id, {"type": "error", "message": f"RUM sync failed: {e}"})
    finally:
        if run_id:
            cleanup_progress_and_reap()
