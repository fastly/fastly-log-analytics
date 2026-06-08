import datetime
import os
import re
import secrets
import sys

from backend.core.fastly.client import fastly
from backend.core.fastly.service import find_service_by_name
from backend.core.fastly.utils import SHIELD_MAP, parse_period
from backend.provision.fastly_api import redeploy_cdn_vcl, update_logging_endpoint, validate_log_format
from backend.provision.orchestrator import (
    _build_log_fields_config,
    cleanup_local_data,
    generate_analyst_invite,
    perform_teardown,
    write_service_config,
)
from backend.provision.utils import (
    BLU,
    BOLD,
    CYN,
    DIM,
    MAG,
    YLW,
    _c,
    ask,
    ask_int,
    ask_yes,
    banner,
    blank,
    fail,
    info,
    ok,
    warn,
)


def wizard(args) -> dict:
    banner("Fastly Log Analysis — Guided Setup")
    _preflight_lf = _build_log_fields_config(args)
    fmt_errors = validate_log_format(_preflight_lf)
    if fmt_errors:
        from backend.provision.utils import RED

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
    bucket_name = args.bucket or (f"fos-{safe_id}-logs" if args.yes else ask("FOS bucket name", f"fos-{safe_id}-logs"))
    fos_prefix = (
        args.prefix if args.prefix is not None else ("" if args.yes else ask("Base log prefix inside bucket", ""))
    )
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
                "endpoint_name": prov_cfg.get("endpoint_name", "Fastly Object Storage Logs"),
                "cdn_service_id": prov_cfg.get("cdn_service_id", ""),
                "cdn_service_name": svc_cfg.get("name", service_id),
                "cdn_url": prov_cfg.get("cdn_url", ""),
                "cdn_secret": svc_cfg.get("cdn_secret", ""),
                "admin_token": svc_cfg.get("fastly_api_key", ""),
            }

    if not state:
        if not getattr(args, "bucket", None):
            fail("No service config found — cannot determine what to remove.")
            sys.exit(1)
        state = {
            "logging_service_id": getattr(args, "service_id", None),
            "fos_bucket_name": args.bucket,
            "fos_region": getattr(args, "region", None) or "us-east-1",
            "endpoint_name": getattr(args, "endpoint_name", None) or "Fastly Object Storage Logs",
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
        blank()
        if not ask_yes(_c(YLW + BOLD, "This cannot be undone. Continue?"), default=False):
            sys.exit(0)

    opts = {
        "remove_logging": not getattr(args, "no_remove_logging", False),
        "remove_cdn": not getattr(args, "no_remove_cdn", False),
        "remove_bucket": not getattr(args, "no_remove_bucket", False),
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


def handle_invite_analyst(args):
    banner("Fastly Log Analysis — Invite Analyst")
    from backend import config as svcconfig

    service_id = args.service_id
    if not service_id:
        ids = svcconfig.list_service_ids()
        if len(ids) == 1:
            service_id = ids[0]
        elif ids:
            for sid in ids:
                c = svcconfig.load_config(sid)
                info(f"  {sid}  ({c.get('name', '')})")
            service_id = ask("Service ID")
    if not service_id:
        fail("--service-id is required.")
        sys.exit(1)

    cfg = svcconfig.load_config(service_id)
    if not cfg or cfg.get("access_level") != "read_write":
        fail("Service not found or not read_write.")
        sys.exit(1)

    if not args.yes and not ask_yes("Proceed?"):
        return

    try:
        import json

        result = generate_analyst_invite(service_id)
        print(json.dumps({k: v for k, v in result.items() if v}, indent=2))
        warn("Save the secret_key now — it cannot be retrieved again.")
    except Exception as e:
        fail(str(e))
        sys.exit(1)


def handle_update_logs(args):
    banner("Fastly Log Analysis — Update Logs")
    from backend import config as svcconfig

    service_id = args.service_id or (svcconfig.list_service_ids()[0] if svcconfig.list_service_ids() else None)
    if not service_id:
        fail("No services configured.")
        sys.exit(1)
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        fail(f"Config for {service_id} not found.")
        sys.exit(1)

    from backend.core import log_fields as lf

    new_lf_config = (
        _build_log_fields_config(args)
        if any([getattr(args, k) for k in ["preset", "enable_group", "disable_group", "enable_field", "disable_field"]])
        else (cfg.get("log_fields") or _build_log_fields_config(args))
    )

    # MERGE GUARD (sibling of state_sync.import_admin_state fix from
    # 2026-06-02 incident): _build_log_fields_config(args) returns
    # {schema_version, preset, groups, field_overrides} — it has NO
    # custom_fields key. Assigning the result wholesale to
    # cfg["log_fields"] would strip the 6 scoring custom_fields the
    # orchestrator injected, the user's own custom_fields, and any
    # format_hash/updated_at metadata. Preserve custom_fields from the
    # on-disk cfg, then if scoring is enabled re-inject the canonical
    # _SCORING_CUSTOM_FIELDS from code as the source of truth.
    existing_lf = cfg.get("log_fields") or {}
    existing_custom = list(existing_lf.get("custom_fields") or [])
    if cfg.get("scoring", {}).get("enabled"):
        from backend.provision.session_scoring_orchestrator import (
            _SCORING_CUSTOM_FIELDS,
            _SCORING_FIELD_NAMES,
        )

        existing_custom = [cf for cf in existing_custom if cf.get("name") not in _SCORING_FIELD_NAMES]
        existing_custom.extend(dict(cf) for cf in _SCORING_CUSTOM_FIELDS)
    new_lf_config["custom_fields"] = existing_custom

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

    service_id = args.service_id or (svcconfig.list_service_ids()[0] if svcconfig.list_service_ids() else None)
    if not service_id:
        fail("No services configured.")
        sys.exit(1)
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        fail(f"Config for {service_id} not found.")
        sys.exit(1)

    token = args.token or cfg.get("fastly_api_key") or ask("Fastly API token")
    if not token:
        sys.exit(1)

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
        rate_limiting = cfg.get("provisioning", {}).get("rate_limiting", True)
        new_ver = redeploy_cdn_vcl(cdn_service_id, token, rate_limiting=rate_limiting)
        ok(f"CDN service updated (version {new_ver})")
    except Exception as e:
        fail(f"Failed: {e}")
        sys.exit(1)


def handle_list_groups(args):
    from backend import config as svcconfig
    from backend.core import log_fields as lf

    existing_cfg = (
        svcconfig.load_config(args.service_id).get("log_fields", {}) if getattr(args, "service_id", None) else {}
    )
    enabled_groups = set(existing_cfg.get("groups", lf.PRESETS["standard"]["groups"]))
    print(f"\n  {'Group':<7} {'Enabled':<9} {'Bytes':>6}    Fields")
    for gid, info_dict in lf.GROUP_INFO.items():
        fields = [f for f in lf.LOG_FIELD_CATALOG if f["group"] == gid]
        print(
            f"  {('(core)' if gid is None else gid):<7} {('yes' if (gid is None or gid in enabled_groups) else 'no'):<9} {sum(f['typical_bytes'] for f in fields):>6}    {', '.join(f['id'] for f in fields)}"
        )


def handle_list_fields(args):
    from backend.core import log_fields as lf

    print(f"\n  {'Field':<20} {'Group':<6} {'Type':<12} {'Bytes':>6}    Description")
    for f in lf.LOG_FIELD_CATALOG:
        print(
            f"  {f['id']:<20} {(f['group'] or '(core)'):<6} {f['duckdb_type']:<12} {f['typical_bytes']:>6}    {f['description'][:60]}"
        )
