"""Provision CLI and print/prompt/lifecycle helpers.

Consolidates ``backend.provision.utils`` (plus ``log_paths`` and ``draft_version``)
and ``backend.provision.cli`` into a single module.
"""

from __future__ import annotations

import datetime
import logging
import os
import queue
import re
import secrets
import sys
import threading
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime as _datetime
from types import SimpleNamespace
from typing import Any

import typer
from rich.console import Console

# ── Rich Consoles & ANSI colour helpers (folded from utils.py) ───────────────
_stdout = Console(highlight=False, markup=False, soft_wrap=True)
_stderr = Console(stderr=True, highlight=False, markup=False, soft_wrap=True)

BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[91m"
GRN = "\033[92m"
YLW = "\033[93m"
BLU = "\033[94m"
MAG = "\033[95m"
CYN = "\033[96m"
RST = "\033[0m"


def _c(color, text):
    return f"{color}{text}{RST}"


def _highlight(msg):
    if "\033" in msg:
        return msg
    return re.sub(r"'([^']+)'", rf"'{_c(MAG, r'\1')}'", msg)


def _emit(console: Console, line: str) -> None:
    """Single write path through rich."""
    console.print(line, end="\n", soft_wrap=True)


def ok(msg):
    _emit(_stdout, f"  {_c(GRN + BOLD, '✓')}  {_highlight(msg)}")


def fail(msg):
    _emit(_stderr, f"  {_c(RED + BOLD, '✗')}  {_c(RED, msg)}")


def info(msg):
    _emit(_stdout, f"  {_c(BLU + BOLD, '→')}  {_highlight(msg)}")


def warn(msg):
    _emit(_stdout, f"  {_c(YLW + BOLD, '⚠')}  {_c(YLW, msg)}")


def blank():
    sys.stdout.write("\n")


def step(n, total, title):
    blank()
    _emit(_stdout, f"{_c(BOLD + MAG, f'[{n}/{total}]')} {_c(BOLD + CYN, title)}")


def banner(title):
    bar = "━" * 64
    blank()
    _emit(_stdout, _c(MAG + BOLD, bar))
    _emit(_stdout, _c(CYN + BOLD, f"  {title}"))
    _emit(_stdout, _c(MAG + BOLD, bar))


def ask(question, default=None):
    suffix = f" {_c(DIM, f'[{default}]')}" if default is not None else ""
    prompt_str = f"  {_c(CYN, '?')}  {question}{suffix}: "
    raw = input(prompt_str).strip()
    return raw if raw else (default if default is not None else "")


def ask_yes(question, default=True):
    hint = _c(DIM, "Y/n" if default else "y/N")
    raw = input(f"  {_c(CYN, '?')}  {question} [{hint}]: ").strip().lower()
    if not raw:
        return default
    return raw.startswith("y")


def ask_int(question, default, min_val=None, max_val=None):
    while True:
        raw = ask(question, default)
        try:
            val = int(raw)
            if min_val is not None and val < min_val:
                warn(f"Value must be at least {min_val}")
                continue
            if max_val is not None and val > max_val:
                warn(f"Value must be at most {max_val}")
                continue
            return val
        except ValueError:
            warn("Please enter a valid integer.")


def run_with_events(func, *args, raise_on_error: bool = True, **kwargs):
    q: queue.Queue[str | None] = queue.Queue()
    kwargs["status_cb"] = q.put
    res = []
    err = []

    def worker():
        try:
            res.append(func(*args, **kwargs))
        except Exception as e:
            err.append(e)
        finally:
            q.put(None)

    t = threading.Thread(target=worker)
    t.start()

    while True:
        msg = q.get()
        if msg is None:
            break
        yield {"type": "status", "message": msg}

    t.join()
    if err:
        if raise_on_error:
            raise err[0]
        yield {"type": "error", "message": str(err[0])}
        return None
    return res[0]


class VclBuilder:
    """Line- and section-joining helpers for VCL generators."""

    @staticmethod
    def join_lines(items: Iterable[str]) -> str:
        """``"\\n"``-join an iterable of already-rendered lines."""
        return "\n".join(items)

    @staticmethod
    def join_sections(sections: Iterable[str]) -> str:
        """``"\\n\\n"``-join sections, dropping blank/whitespace-only ones."""
        return "\n\n".join(s for s in sections if s.strip())


# ── Canonical FOS log object path templates (merged from log_paths.py) ────────

ANALYTICS_LOG_LAYOUT = "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
RUM_LOG_LAYOUT = "raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


def analytics_log_path() -> str:
    """Logging-endpoint ``path`` for the main analytics log stream."""
    return ANALYTICS_LOG_LAYOUT


def rum_log_path() -> str:
    """Logging-endpoint ``path`` for the RUM beacon log stream."""
    return RUM_LOG_LAYOUT


def minute_list_prefix(dt: _datetime) -> str:
    """S3 LIST ``Prefix`` bounding exactly one closed minute of analytics logs."""
    return dt.strftime("raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")


def rum_minute_list_prefix(dt: _datetime) -> str:
    """S3 LIST ``Prefix`` bounding exactly one closed minute of RUM beacon logs."""
    return dt.strftime("raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")


# ── Fastly service version lifecycle (merged from draft_version.py) ───────────

_draft_logger = logging.getLogger("backend.provision.draft_version")


def rollback_to_version(service_id: str, token: str, base_version: int, *, call: Callable[..., Any]) -> None:
    """Re-activate ``base_version``. A failed rollback is logged at WARNING, never raised."""
    try:
        call("PUT", f"/service/{service_id}/version/{base_version}/activate", token=token)
    except RuntimeError:
        _draft_logger.warning(
            "rollback activation of version %s failed for %s", base_version, service_id, exc_info=True
        )


@contextmanager
def draft_version(
    service_id: str,
    token: str,
    *,
    base_version: int,
    call: Callable[..., Any],
    comment: str | None = None,
    comment_before_activate: bool = False,
    status_cb: Callable[[str], None] | None = None,
    validate: bool = True,
    validate_msg: str = "Validating draft version {ver}...",
    activate_msg: str = "Activating version {ver}...",
    expect_empty_activate: bool = False,
    rollback: bool = True,
    on_failure: Callable[[Exception], None] | None = None,
) -> Iterator[int]:
    """Yield the new draft version number; validate + activate it when the body completes."""
    clone = call("PUT", f"/service/{service_id}/version/{base_version}/clone", token=token)
    new_ver = int(clone["number"])
    if comment is not None and not comment_before_activate:
        call("PUT", f"/service/{service_id}/version/{new_ver}", {"comment": comment}, token=token)

    try:
        yield new_ver

        if validate:
            if status_cb:
                status_cb(validate_msg.format(ver=new_ver))
            result = call("GET", f"/service/{service_id}/version/{new_ver}/validate", token=token)
            if result.get("status") != "ok":
                raise RuntimeError(f"Validation failed: {result.get('errors') or result}")

        if comment is not None and comment_before_activate:
            call("PUT", f"/service/{service_id}/version/{new_ver}", {"comment": comment}, token=token)

        if status_cb:
            status_cb(activate_msg.format(ver=new_ver))
        activate_kwargs: dict[str, Any] = {"expect_empty": True} if expect_empty_activate else {}
        call("PUT", f"/service/{service_id}/version/{new_ver}/activate", token=token, **activate_kwargs)
    except Exception as exc:
        if on_failure is not None:
            on_failure(exc)
        if rollback:
            rollback_to_version(service_id, token, base_version, call=call)
        raise


# Register module aliases BEFORE importing sibling provision modules that depend on utils
sys.modules["backend.provision.utils"] = sys.modules[__name__]
sys.modules["backend.provision.log_paths"] = sys.modules[__name__]
sys.modules["backend.provision.draft_version"] = sys.modules[__name__]

from backend.core.fastly.client import SHIELD_MAP, fastly, find_service_by_name, parse_period  # noqa: E402
from backend.provision.fastly_api import (  # noqa: E402
    account_has_rate_limiting,
    redeploy_cdn_vcl,
    update_logging_endpoint,
    validate_log_format,
)
from backend.provision.orchestrator import (  # noqa: E402
    _build_log_fields_config,
    cleanup_local_data,
    perform_teardown,
    write_service_config,
)

# ────────────────────────────────────────────────────────────────────────────
# Handler functions — driven directly by the unit tests; the typer
# commands at the bottom call into these via a SimpleNamespace.
# Patching contracts (the test file pokes ``backend.provision.cli.<name>``):
#   perform_teardown, cleanup_local_data,
#   update_logging_endpoint, write_service_config, validate_log_format,
#   redeploy_cdn_vcl, find_service_by_name, fastly, ask, ask_yes, ask_int
# ────────────────────────────────────────────────────────────────────────────


def wizard(args) -> dict:
    banner("Fastly Log Analysis — Guided Setup")
    _preflight_lf = _build_log_fields_config(args)
    fmt_errors = validate_log_format(_preflight_lf)
    if fmt_errors:
        for err in fmt_errors:
            print(f"  {_c(RED, 'ERROR:')} {err}")
        sys.exit(1)

    default_token = os.getenv("FASTLY_API_KEY")
    token = args.token or (default_token if args.yes else ask("Fastly API token", default=default_token))
    service_id = args.service_id or ask("Service ID")
    svc = fastly("GET", f"/service/{service_id}", token=token)
    endpoint_name = args.endpoint_name or (
        "Fastly Object Storage Logs" if args.yes else ask("Logging endpoint name", "Fastly Object Storage Logs")
    )
    region = (args.region or ("us-east-1" if args.yes else ask("FOS region", "us-east-1"))).lower()

    safe_id = re.sub(r"[^a-zA-Z0-9]", "-", service_id)
    safe_id = re.sub(r"-+", "-", safe_id).strip("-")
    safe_id_lower = safe_id.lower()
    bucket_name = args.bucket or (
        f"fos-{safe_id_lower}-logs" if args.yes else ask("FOS bucket name", f"fos-{safe_id_lower}-logs")
    )
    fos_prefix = ""
    sample_rate = max(
        1,
        min(
            100,
            int(args.sample_rate or (100 if args.yes else ask_int("Log sampling percentage (1-100)", "100", 1, 100))),
        ),
    )
    edge_only = (
        args.edge_only
        if args.edge_only is not None
        else (True if args.yes else ask_yes("Only log requests from edge?", default=True))
    )
    period = parse_period(args.period or ("1 minute" if args.yes else ask("Log rotation period", "1 minute")))

    cdn_name = args.cdn_name or (
        f"Log Analysis CDN Service for {service_id}"
        if args.yes
        else ask("CDN service name", f"Log Analysis CDN Service for {service_id}")
    )
    cdn_prefix = getattr(args, "cdn_prefix", None) or (safe_id if args.yes else ask("CDN domain prefix", safe_id))
    cdn_url = f"https://{cdn_prefix}.global.ssl.fastly.net"
    cdn_shield = getattr(args, "shield", None) or (
        SHIELD_MAP.get(region, "iad-va-us")
        if args.yes
        else ask("Fastly Shield POP for CDN", SHIELD_MAP.get(region, "iad-va-us"))
    )
    cdn_secret = secrets.token_urlsafe(24)

    delete_after = getattr(
        args, "delete_after", True if args.yes else ask_yes("Auto-delete raw logs (Recommended)?", default=True)
    )
    log_fields_cfg = _build_log_fields_config(args)

    cfg = {
        "admin_token": token,
        "logging_service_id": service_id,
        "service_name": svc.get("name", service_id),
        "endpoint_name": endpoint_name,
        "fos_region": region,
        "fos_bucket_name": bucket_name,
        "fos_prefix": fos_prefix,
        "sample_rate": sample_rate,
        "edge_only": edge_only,
        "log_period": period,
        "cdn_service_name": cdn_name,
        "cdn_url": cdn_url,
        "cdn_shield": cdn_shield,
        "cdn_secret": cdn_secret,
        "delete_after": not getattr(args, "disable_delete_after", False) if delete_after else False,
        "enable_cron_sync": not getattr(args, "disable_cron_sync", False),
        "commit_interval_mins": getattr(args, "commit_interval_mins", 5) or 5,
        "enable_cron_compact": not getattr(args, "disable_cron_compact", False),
        "log_retention_days": getattr(args, "log_retention_days", 30) or 30,
        "log_fields": log_fields_cfg,
    }

    if not args.yes:
        banner("Confirm — Resources to be provisioned")
        print(f"  {_c(BLU, 'Target Service:'):<24} {_c(BOLD + MAG, service_id)}  ({_c(DIM, svc.get('name', ''))})")
        print(f"  {_c(BLU, 'FOS Bucket:'):<24} {_c(BOLD + YLW, bucket_name)}")
        print(f"  {_c(BLU, 'CDN URL:'):<24} {_c(BOLD + CYN, cdn_url)}")
        if not ask_yes(_c(BOLD, "Proceed with provisioning?")):
            sys.exit(0)

    return cfg


def handle_teardown(args):
    banner("Fastly Log Analysis — Teardown")
    from backend import config as svcconfig

    service_id = getattr(args, "service_id", None)
    if not service_id:
        ids = svcconfig.list_service_ids()
        if ids:
            service_id = ids[0]

    state = {}
    if service_id:
        svc_cfg = svcconfig.load_config(service_id)
        if svc_cfg:
            prov_cfg = svc_cfg.get("provisioning", {})
            state = {
                "logging_service_id": service_id,
                "fos_bucket_name": svc_cfg.get("fos_bucket", ""),
                "fos_region": svc_cfg.get("fos_region", "us-east-1"),
                "fos_access_key": svc_cfg.get("fos_access_key_id", ""),
                "fos_secret_key": svc_cfg.get("fos_secret_access_key", ""),
                "fos_key_id": prov_cfg.get("fos_key_id", ""),
                "endpoint_name": prov_cfg.get("endpoint_name", "Fastly Log Analytics"),
                "cdn_service_id": prov_cfg.get("cdn_service_id", ""),
                "cdn_service_name": svc_cfg.get("name", service_id),
                "cdn_url": prov_cfg.get("cdn_url", ""),
                "cdn_secret": svc_cfg.get("cdn_secret", ""),
                "admin_token": svc_cfg.get("fastly_api_key", ""),
                "scoring": svc_cfg.get("scoring") or {},
            }

    if not state:
        if not getattr(args, "bucket", None):
            fail("No service config found — cannot determine what to remove.")
            sys.exit(1)
        state = {
            "logging_service_id": getattr(args, "service_id", None),
            "fos_bucket_name": args.bucket,
            "fos_region": getattr(args, "region", None) or "us-east-1",
            "endpoint_name": getattr(args, "endpoint_name", None) or "Fastly Log Analytics",
            "cdn_service_id": None,
            "fos_key_id": None,
        }

    token = args.token or os.getenv("FASTLY_API_KEY") or state.get("admin_token")
    if not token:
        token = ask("Admin API token")
    if not token:
        fail("Token is required.")
        sys.exit(1)

    if not args.yes:
        blank()
        print(f"  {_c(YLW + BOLD, 'The following will be permanently deleted:')}")
        print(f"  {_c(DIM, 'Logging endpoint:'):<30} {state.get('endpoint_name', '?')}")
        print(f"  {_c(DIM, 'FOS bucket + all data:'):<30} {_c(YLW + BOLD, state.get('fos_bucket_name', '?'))}")
        if (state.get("scoring") or {}).get("enabled") and not getattr(args, "no_remove_scoring", False):
            _scoring_svc = (state.get("scoring") or {}).get("scoring_service_id", "?")
            print(f"  {_c(DIM, 'Session scoring service:'):<30} {_c(YLW + BOLD, _scoring_svc)}")
        blank()
        if not ask_yes(_c(YLW + BOLD, "This cannot be undone. Continue?"), default=False):
            sys.exit(0)

    opts = {
        "remove_logging": not getattr(args, "no_remove_logging", False),
        "remove_cdn": not getattr(args, "no_remove_cdn", False),
        "remove_bucket": not getattr(args, "no_remove_bucket", False),
        "remove_scoring": not getattr(args, "no_remove_scoring", False),
    }

    try:
        for _ in perform_teardown(state, token, opts=opts):
            pass
        cleanup_local_data(
            service_id,
            bucket=state.get("fos_bucket_name"),
            remove_data=args.remove_data
            or (not args.yes and ask_yes("Remove local database and cache as well?", default=True)),
        )
        ok("Teardown completed.")
    except Exception as exc:
        fail(f"Teardown failed: {exc}")
        sys.exit(1)


def handle_reset_logs(args):
    banner("Fastly Log Analysis — Delete Data")
    from backend import config as svcconfig
    from backend.core.metadata.reconciliation import is_ingested_files_dedup_active
    from backend.core.reset import reset_service_logs
    from backend.cron.scheduler import get_scheduler

    service_id = getattr(args, "service_id", None)
    cfg = svcconfig.load_config(service_id) if service_id else None
    if not cfg:
        fail(f"No service config found for {service_id!r}.")
        sys.exit(1)

    if not args.yes:
        blank()
        print(f"  {_c(YLW + BOLD, 'The following will be permanently deleted:')}")
        print(f"  {_c(DIM, 'Cloud DuckLake log data (ducklake/ in FOS, High-Scale only)')}")
        print(f"  {_c(DIM, 'Quarantined-file copies (errors/)')}")
        print(f"  {_c(DIM, 'Local DuckLake data, DuckDB analytical database + cache')}")
        print(f"  {_c(DIM, 'Local ingestion dedup indexes')}")
        if args.delete_raw:
            print(f"  {_c(YLW + BOLD, 'Raw .gz request logs in cloud storage (raw/request/)')}")
        if args.wipe_usage:
            print(f"  {_c(DIM, 'Usage-log (Class A/B) billing history')}")
        blank()
        print(
            f"  {_c(BLU, 'Preserved:')} saved views, alerts, source registration, audit history, scoring labels/audit."
        )
        if args.delete_raw and not is_ingested_files_dedup_active(service_id):
            blank()
            warn(
                "This source keeps request logs after ingest (delete_after=False). Deleting raw/request/ now "
                "means the next full sync has no dedup record left and will re-ingest this "
                "service's ENTIRE history."
            )
        blank()
        if not ask_yes(_c(YLW + BOLD, "This cannot be undone. Continue?"), default=False):
            sys.exit(0)

    try:
        for event in reset_service_logs(
            service_id,
            delete_raw_logs=args.delete_raw,
            preserve_usage_history=not args.wipe_usage,
            actor="cli",
            reload_scheduler=lambda: get_scheduler().reload(),
        ):
            if event["type"] == "status":
                info(event["message"])
            elif event["type"] == "done":
                ok(event["message"])
            elif event["type"] == "error":
                fail(event["message"])
    except Exception as exc:
        fail(f"Log deletion failed: {exc}")
        sys.exit(1)


def _resolve_cli_service_and_token(args, *, require_token: bool = True) -> tuple[str, dict, str]:
    from backend import config as svcconfig

    service_id = args.service_id or (svcconfig.list_service_ids()[0] if svcconfig.list_service_ids() else None)
    if not service_id:
        fail("No services configured.")
        sys.exit(1)
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        fail(f"Config for {service_id} not found.")
        sys.exit(1)
    if not require_token:
        return service_id, cfg, ""
    token = args.token or cfg.get("fastly_api_key") or ask("Fastly API token")
    if not token:
        sys.exit(1)
    return service_id, cfg, token


def handle_update_logs(args):
    banner("Fastly Log Analysis — Update Logs")
    from backend.provision.orchestrator import (
        reconcile_system_custom_fields,
        system_feature_flags,
    )

    service_id, cfg, _ = _resolve_cli_service_and_token(args, require_token=False)

    from backend.core import field_registry as lf

    new_lf_config = (
        _build_log_fields_config(args)
        if any([getattr(args, k) for k in ["preset", "enable_group", "disable_group", "enable_field", "disable_field"]])
        else (cfg.get("log_fields") or _build_log_fields_config(args))
    )

    existing_lf = cfg.get("log_fields") or {}
    existing_custom = list(existing_lf.get("custom_fields") or [])
    scoring_enabled, cmcd_enabled = system_feature_flags(cfg)
    new_lf_config["custom_fields"] = reconcile_system_custom_fields(
        existing_custom,
        scoring_enabled=scoring_enabled,
        cmcd_enabled=cmcd_enabled,
    )

    if getattr(args, "dry_run", False):
        print(lf.generate_log_format(new_lf_config))
        return

    token = args.token or cfg.get("fastly_api_key") or ask("Fastly API token")
    if not token:
        sys.exit(1)

    prov_cfg = cfg.get("provisioning", {})
    cfg["log_fields"] = new_lf_config
    cfg["log_fields"]["format_hash"] = lf.format_hash(new_lf_config)
    cfg["log_fields"]["format_updated_at"] = datetime.datetime.now(datetime.UTC).isoformat()
    write_service_config(cfg)

    update_cfg = {
        "logging_service_id": service_id,
        "endpoint_name": args.endpoint_name or prov_cfg.get("endpoint_name", "Fastly Object Storage Logs"),
        "sample_rate": args.sample_rate or prov_cfg.get("sample_rate", 100),
        "edge_only": args.edge_only if args.edge_only is not None else prov_cfg.get("edge_only", False),
        "log_period": parse_period(args.period) if args.period else prov_cfg.get("log_period", 60),
        "log_fields": new_lf_config,
        "update_format": True,
    }

    try:
        for event in update_logging_endpoint(update_cfg, token):
            if event.get("type") == "status":
                info(event["message"])
            elif event.get("type") == "done":
                ok(f"Logs updated! Version: {event.get('version')}")
    except Exception as e:
        fail(f"Failed: {e}")
        sys.exit(1)


def handle_update_cdn(args):
    banner("Fastly Log Analysis — Update CDN Service")
    from backend import config as svcconfig

    service_id, cfg, token = _resolve_cli_service_and_token(args)

    prov_cfg = cfg.get("provisioning", {})
    cdn_service_id = prov_cfg.get("cdn_service_id") or cfg.get("cdn_service_id")
    if not cdn_service_id:
        found = find_service_by_name(cfg.get("cdn_service_name") or f"Log Analysis CDN Service for {service_id}", token)
        if found:
            cdn_service_id = found["id"]
    if not cdn_service_id:
        fail("CDN service ID not found.")
        sys.exit(1)

    try:
        persisted = cfg.get("provisioning", {}).get("rate_limiting", True)
        detected = account_has_rate_limiting(token, cdn_service_id, service_id)
        rate_limiting = detected if detected is not None else persisted
        if detected is not None and detected != persisted:
            prov = cfg.get("provisioning", {})
            prov["rate_limiting"] = detected
            cfg["provisioning"] = prov
            svcconfig.mutate_config(
                service_id, lambda c: c.setdefault("provisioning", {}).__setitem__("rate_limiting", detected)
            )
            if detected:
                ok("Edge rate limiting detected on this account — enabling it on the CDN service")
            else:
                warn("Edge rate limiting unavailable on this account — deploying CDN without it")
        new_ver = redeploy_cdn_vcl(cdn_service_id, token, rate_limiting=rate_limiting)
        ok(f"CDN service updated (version {new_ver})")
    except Exception as e:
        fail(f"Failed: {e}")
        sys.exit(1)


def handle_enable_scoring(args):
    """Enable — or redeploy — session scoring on a logging service."""
    banner("Fastly Log Analysis — Enable Session Scoring")
    service_id, _cfg, token = _resolve_cli_service_and_token(args)

    from backend.provision.session_scoring_orchestrator import enable_scoring

    try:
        result = enable_scoring(service_id, token)
        ok(
            f"Session scoring enabled / redeployed (logging active version {result.get('logging_service_active_version')})."
        )
    except Exception as e:
        fail(f"Failed to enable session scoring: {e}")
        sys.exit(1)


def handle_disable_scoring(args):
    """Disable session scoring on a logging service."""
    banner("Fastly Log Analysis — Disable Session Scoring")
    service_id, _cfg, token = _resolve_cli_service_and_token(args)

    if not getattr(args, "yes", False):
        blank()
        print(f"  {_c(YLW + BOLD, 'This will disable session scoring on')} {service_id}:")
        print(f"  {_c(DIM, '- removes the scoring VCL snippets + custom_fields')}")
        print(f"  {_c(DIM, '- tears down the scorer Compute service + ConfigStores')}")
        blank()
        if not ask_yes(_c(YLW + BOLD, "Continue?"), default=False):
            sys.exit(0)

    from backend.provision.session_scoring_orchestrator import disable_scoring

    try:
        disable_scoring(service_id, token)
        ok("Session scoring disabled.")
    except Exception as e:
        fail(f"Failed to disable session scoring: {e}")
        sys.exit(1)


def handle_list_groups(args):
    from backend import config as svcconfig
    from backend.core import field_registry as lf

    existing_cfg = (
        svcconfig.load_config(args.service_id).get("log_fields", {}) if getattr(args, "service_id", None) else {}
    )
    enabled_groups = set(existing_cfg.get("groups", lf.PRESETS["standard"]["groups"]))
    print(f"\n  {'Group':<7} {'Enabled':<9} {'Bytes':>6}    Fields")
    for gid, _info_dict in lf.GROUP_INFO.items():
        fields = [f for f in lf.LOG_FIELD_CATALOG if f["group"] == gid]
        print(
            f"  {('(core)' if gid is None else gid):<7} {('yes' if (gid is None or gid in enabled_groups) else 'no'):<9} {sum(f['typical_bytes'] for f in fields):>6}    {', '.join(f['id'] for f in fields)}"
        )


def handle_list_fields(args):
    from backend.core import field_registry as lf

    print(f"\n  {'Field':<20} {'Group':<6} {'Type':<12} {'Bytes':>6}    Description")
    for f in lf.LOG_FIELD_CATALOG:
        print(
            f"  {f['id']:<20} {(f['group'] or '(core)'):<6} {f['duckdb_type']:<12} {f['typical_bytes']:>6}    {f['description'][:60]}"
        )


# ────────────────────────────────────────────────────────────────────────────
# Typer surface — each subcommand collects typed options, packs them
# into a SimpleNamespace, and delegates to the matching handler.
# ────────────────────────────────────────────────────────────────────────────

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Fastly Log Analysis — provisioning CLI.",
    rich_markup_mode="rich",
)


@app.command("provision", help="Run the guided provisioning wizard.")
def cmd_provision(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip prompts, accept defaults."),
    token: str | None = typer.Option(None, "--token", help="Fastly API token."),
    service_id: str | None = typer.Option(None, "--service-id", help="Target Fastly service ID."),
    endpoint_name: str | None = typer.Option(None, "--endpoint-name"),
    region: str | None = typer.Option(None, "--region", help="FOS region (e.g. us-east-1)."),
    bucket: str | None = typer.Option(None, "--bucket", help="FOS bucket name."),
    sample_rate: int | None = typer.Option(None, "--sample-rate", min=1, max=100),
    edge_only: bool | None = typer.Option(None, "--edge-only/--no-edge-only"),
    period: str | None = typer.Option(None, "--period", help="Log rotation period (e.g. '1 minute')."),
    cdn_name: str | None = typer.Option(None, "--cdn-name"),
    cdn_prefix: str | None = typer.Option(None, "--cdn-prefix"),
    shield: str | None = typer.Option(None, "--shield"),
    disable_delete_after: bool = typer.Option(False, "--disable-delete-after"),
    disable_cron_sync: bool = typer.Option(False, "--disable-cron-sync"),
    commit_interval_mins: int = typer.Option(5, "--commit-interval-mins"),
    disable_cron_compact: bool = typer.Option(False, "--disable-cron-compact"),
    log_retention_days: int = typer.Option(30, "--log-retention-days"),
    preset: str | None = typer.Option(None, "--preset"),
    enable_group: list[str] | None = typer.Option(None, "--enable-group"),
    disable_group: list[str] | None = typer.Option(None, "--disable-group"),
    enable_field: list[str] | None = typer.Option(None, "--enable-field"),
    disable_field: list[str] | None = typer.Option(None, "--disable-field"),
):
    args = SimpleNamespace(
        yes=yes,
        token=token,
        service_id=service_id,
        endpoint_name=endpoint_name,
        region=region,
        bucket=bucket,
        prefix="",
        sample_rate=sample_rate,
        edge_only=edge_only,
        period=period,
        cdn_name=cdn_name,
        cdn_prefix=cdn_prefix,
        shield=shield,
        disable_delete_after=disable_delete_after,
        disable_cron_sync=disable_cron_sync,
        commit_interval_mins=commit_interval_mins,
        disable_cron_compact=disable_cron_compact,
        log_retention_days=log_retention_days,
        preset=preset,
        enable_group=enable_group,
        disable_group=disable_group,
        enable_field=enable_field,
        disable_field=disable_field,
    )
    cfg = wizard(args)
    from backend.provision.orchestrator import provision

    try:
        for event in provision(cfg):
            if event.get("type") == "status":
                info(event.get("message", ""))
            elif event.get("type") == "done":
                ok(event.get("message", ""))
    except Exception as e:
        fail(f"Provisioning failed: {e}")
        sys.exit(1)


@app.command("teardown", help="Tear down a provisioned service.")
def cmd_teardown(
    yes: bool = typer.Option(False, "--yes", "-y"),
    service_id: str | None = typer.Option(None, "--service-id"),
    token: str | None = typer.Option(None, "--token"),
    bucket: str | None = typer.Option(None, "--bucket"),
    region: str | None = typer.Option(None, "--region"),
    endpoint_name: str | None = typer.Option(None, "--endpoint-name"),
    remove_data: bool = typer.Option(False, "--remove-data"),
    no_remove_logging: bool = typer.Option(False, "--no-remove-logging"),
    no_remove_cdn: bool = typer.Option(False, "--no-remove-cdn"),
    no_remove_bucket: bool = typer.Option(False, "--no-remove-bucket"),
    no_remove_scoring: bool = typer.Option(False, "--no-remove-scoring"),
):
    args = SimpleNamespace(
        yes=yes,
        service_id=service_id,
        token=token,
        bucket=bucket,
        region=region,
        endpoint_name=endpoint_name,
        remove_data=remove_data,
        no_remove_logging=no_remove_logging,
        no_remove_cdn=no_remove_cdn,
        no_remove_bucket=no_remove_bucket,
        no_remove_scoring=no_remove_scoring,
    )
    handle_teardown(args)


@app.command("reset-logs", help="Delete a service's log data and reset it to a 0-state.")
def cmd_reset_logs(
    service_id: str = typer.Option(..., "--service-id", help="The Fastly service ID to reset."),
    delete_raw: bool = typer.Option(
        False,
        "--delete-raw/--no-delete-raw",
        help="Also delete raw .gz logs in cloud storage (default off; re-ingestion risk on delete_after=False sources).",
    ),
    wipe_usage: bool = typer.Option(False, "--wipe-usage/--preserve-usage", help="Delete Class A/B usage-log history."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the destructive-action confirmation prompt."),
):
    args = SimpleNamespace(service_id=service_id, delete_raw=delete_raw, wipe_usage=wipe_usage, yes=yes)
    handle_reset_logs(args)


@app.command("restore-data", help="Rebuild a service's local DuckLake data from its cloud copy in FOS.")
def cmd_restore_data(
    service_id: str = typer.Option(..., "--service-id", help="The Fastly service ID to restore."),
):
    from backend.core.duckdb import get_source_for_service
    from backend.core.storage.backup import restore_from_cloud

    source = get_source_for_service(service_id)
    if source is None:
        raise typer.BadParameter(f"No configured service {service_id!r}", param_hint="--service-id")
    result = restore_from_cloud(source, progress=info)
    ok(
        f"Restored {result['remote_files']} cloud file(s): {result['downloaded']} downloaded, {result['registered']} registered."
    )


@app.command("update-logs", help="Push a refreshed log_fields config.")
def cmd_update_logs(
    service_id: str | None = typer.Option(None, "--service-id"),
    token: str | None = typer.Option(None, "--token"),
    endpoint_name: str | None = typer.Option(None, "--endpoint-name"),
    sample_rate: int | None = typer.Option(None, "--sample-rate"),
    edge_only: bool | None = typer.Option(None, "--edge-only/--no-edge-only"),
    period: str | None = typer.Option(None, "--period"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    preset: str | None = typer.Option(None, "--preset"),
    enable_group: list[str] | None = typer.Option(None, "--enable-group"),
    disable_group: list[str] | None = typer.Option(None, "--disable-group"),
    enable_field: list[str] | None = typer.Option(None, "--enable-field"),
    disable_field: list[str] | None = typer.Option(None, "--disable-field"),
):
    args = SimpleNamespace(
        service_id=service_id,
        token=token,
        endpoint_name=endpoint_name,
        sample_rate=sample_rate,
        edge_only=edge_only,
        period=period,
        dry_run=dry_run,
        preset=preset,
        enable_group=enable_group,
        disable_group=disable_group,
        enable_field=enable_field,
        disable_field=disable_field,
    )
    handle_update_logs(args)


@app.command("update-cdn", help="Re-deploy the CDN VCL snippet.")
def cmd_update_cdn(
    service_id: str | None = typer.Option(None, "--service-id"),
    token: str | None = typer.Option(None, "--token"),
):
    args = SimpleNamespace(service_id=service_id, token=token)
    handle_update_cdn(args)


@app.command("enable-scoring", help="Enable (or redeploy) session scoring on a service.")
def cmd_enable_scoring(
    service_id: str | None = typer.Option(None, "--service-id"),
    token: str | None = typer.Option(None, "--token"),
):
    args = SimpleNamespace(service_id=service_id, token=token)
    handle_enable_scoring(args)


@app.command("disable-scoring", help="Disable session scoring on a service.")
def cmd_disable_scoring(
    yes: bool = typer.Option(False, "--yes", "-y"),
    service_id: str | None = typer.Option(None, "--service-id"),
    token: str | None = typer.Option(None, "--token"),
):
    args = SimpleNamespace(yes=yes, service_id=service_id, token=token)
    handle_disable_scoring(args)


@app.command("list-groups", help="List log-field groups (diagnostic).")
def cmd_list_groups(
    service_id: str | None = typer.Option(None, "--service-id"),
):
    args = SimpleNamespace(service_id=service_id)
    handle_list_groups(args)


@app.command("list-fields", help="List the log-field catalog (diagnostic).")
def cmd_list_fields():
    handle_list_fields(SimpleNamespace())


if __name__ == "__main__":
    app()
