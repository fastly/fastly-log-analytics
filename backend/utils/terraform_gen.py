"""Terraform .tf.json configuration generator for Fastly Log Analysis services."""

from __future__ import annotations

import json
import re
from typing import Any


def _terraform_template_escape(value: object) -> str:
    """Escape ``${`` / ``%{`` inside a string so Terraform treats them as literal characters."""
    s = "" if value is None else str(value)
    return s.replace("${", "$${").replace("%{", "%%{")


def _hcl_quoted_escape(value: object) -> str:
    """Escape a string for safe interpolation inside an HCL double-quoted literal (README snippets)."""
    s = "" if value is None else str(value)
    s = s.replace("\x00", "").replace("\r", "").replace("\n", "")
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    return _terraform_template_escape(s)


def _safe_vcl_filename_stem(name: str) -> str:
    """Sanitize an endpoint/snippet name into a safe relative filename stem (no quotes, ${, /, or ..)."""
    cleaned = re.sub(r"[^a-z0-9_]+", "_", (name or "").lower()).strip("_")
    return cleaned or "endpoint"


def _dump(obj: dict) -> str:
    """Canonical .tf.json serialisation: 2-space indent, keys sorted, trailing newline."""
    return json.dumps(obj, indent=2, sort_keys=True) + "\n"


from backend.config import (  # noqa: E402
    is_cmcd_enabled,
    is_logging_enabled,
    is_rum_enabled,
    is_scoring_enabled,
)
from backend.core.catalog import validate_vcl_custom_condition  # noqa: E402
from backend.core.fastly.client import (  # noqa: E402
    _assert_faro_version_safe,
    load_vcl,
    parse_period,
    validate_rum_custom_condition,
)
from backend.core.service_defaults import DEFAULT_LOG_PERIOD_SECONDS
from backend.core.vcl_generator import validate_quoted_literal, validate_url_exclusion_regex


def _tf_subdict(cfg: dict[str, Any], key: str) -> dict[str, Any]:
    val = cfg.get(key)
    if val is None:
        return {}
    if not isinstance(val, dict):
        raise ValueError(f"{key} must be a dict, got {type(val).__name__}")
    return val


def _tf_str(val: Any, field_name: str, default: str = "") -> str:
    if val is None:
        return default
    if not isinstance(val, str):
        raise ValueError(f"{field_name} must be a string, got {type(val).__name__}")
    return val


def _tf_int(val: Any, field_name: str) -> int:
    if isinstance(val, bool) or not isinstance(val, (int, str)):
        raise ValueError(f"{field_name} must be an integer, got {val!r}")
    try:
        return int(val.strip() if isinstance(val, str) else val)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer, got {val!r}") from exc


def generate_terraform(cfg: dict[str, Any], fos_access_key: str, fos_secret_key: str) -> dict[str, str]:
    """Generate Terraform .tf.json for the given provisioning configuration."""
    prov = _tf_subdict(cfg, "provisioning")
    rum_dict = _tf_subdict(cfg, "rum")
    cmcd_cfg = _tf_subdict(cfg, "cmcd")
    scoring_cfg = _tf_subdict(cfg, "scoring")
    fos_proxy = _tf_subdict(cfg, "fos_proxy")
    if cfg.get("log_fields") is not None and not isinstance(cfg.get("log_fields"), dict):
        raise ValueError("log_fields must be a dict")

    from backend.provision.opaque_names import (
        generate_opaque_bucket_name,
        generate_opaque_cdn_prefix,
        generate_opaque_label,
    )

    raw_logging_sid = _tf_str(cfg.get("logging_service_id"), "logging_service_id").replace("\r", "").replace("\n", "")
    raw_sid = _tf_str(cfg.get("service_id"), "service_id").replace("\r", "").replace("\n", "")
    service_id = raw_logging_sid or raw_sid or "YOUR_SERVICE_ID"
    endpoint_name = _tf_str(prov.get("endpoint_name"), "provisioning.endpoint_name", default="fastly_log_analysis")
    region = _tf_str(cfg.get("fos_region"), "fos_region", default="us-east-1")
    bucket = _tf_str(cfg.get("fos_bucket_name"), "fos_bucket_name", default="") or generate_opaque_bucket_name()
    prefix = _tf_str(cfg.get("fos_prefix"), "fos_prefix").strip("/")

    raw_lp = cfg.get("log_period")
    if raw_lp is None or raw_lp == "":
        period = DEFAULT_LOG_PERIOD_SECONDS
    elif isinstance(raw_lp, bool) or not isinstance(raw_lp, (int, str)):
        raise ValueError(f"Invalid log_period: {raw_lp!r}")
    elif isinstance(raw_lp, int):
        period = raw_lp
    else:
        try:
            period = parse_period(raw_lp)
        except ValueError:
            period = DEFAULT_LOG_PERIOD_SECONDS

    rum_period_val = rum_dict.get("log_period") if rum_dict.get("log_period") is not None else cfg.get("rum_log_period")
    if rum_period_val is None or rum_period_val == "":
        rum_period = None
    elif isinstance(rum_period_val, bool) or not isinstance(rum_period_val, (int, str)):
        raise ValueError(f"Invalid rum_log_period: {rum_period_val!r}")
    elif isinstance(rum_period_val, int):
        rum_period = rum_period_val
    else:
        rum_period = parse_period(rum_period_val)

    cdn_service_name = _tf_str(cfg.get("cdn_service_name"), "cdn_service_name", default="Fastly Log Analysis CDN Proxy")
    cdn_prefix = _tf_str(cfg.get("cdn_prefix"), "cdn_prefix", default="") or generate_opaque_cdn_prefix()
    cdn_domain = f"{cdn_prefix}.global.ssl.fastly.net"

    shield_aliases = (
        cfg.get("cdn_shield"),
        prov.get("cdn_shield"),
        fos_proxy.get("shield"),
    )
    for sh_val in shield_aliases:
        if sh_val is not None and (
            not isinstance(sh_val, str) or (sh_val and not re.fullmatch(r"^[A-Za-z0-9_-]{1,64}$", sh_val))
        ):
            raise ValueError(f"Invalid cdn_shield: {sh_val!r}")
    resolved_shield = next((s for s in shield_aliases if isinstance(s, str) and s), "")
    cdn_shield = resolved_shield if any(s is not None for s in shield_aliases) else "iad-va-us"
    cdn_secret = _tf_str(cfg.get("cdn_secret"), "cdn_secret", default="replace-with-secure-secret")

    prov_sr = _tf_int(prov["sample_rate"], "provisioning.sample_rate") if prov.get("sample_rate") is not None else None
    top_sr = _tf_int(cfg["sample_rate"], "sample_rate") if cfg.get("sample_rate") is not None else None
    sample_rate = prov_sr if prov_sr is not None else (top_sr if top_sr is not None else 100)
    edge_only = bool(prov.get("edge_only") if "edge_only" in prov else cfg.get("edge_only", False))

    raw_prov_cond = _tf_str(prov.get("custom_condition"), "provisioning.custom_condition")
    raw_top_cond = _tf_str(cfg.get("custom_condition"), "custom_condition")
    for cval in (raw_prov_cond, raw_top_cond):
        if cval and (cond_msg := validate_vcl_custom_condition(cval)):
            raise ValueError(f"Invalid custom_condition: {cond_msg}")
    prov_cond = raw_prov_cond.strip()
    top_cond = raw_top_cond.strip()
    custom_condition = prov_cond if prov.get("custom_condition") is not None else top_cond

    raw_top_rum_cond = _tf_str(cfg.get("rum_custom_condition"), "rum_custom_condition")
    raw_nested_rum_cond = _tf_str(rum_dict.get("custom_condition"), "rum.custom_condition")
    for rval in (raw_top_rum_cond, raw_nested_rum_cond):
        if rval and (rum_cond_msg := validate_rum_custom_condition(rval)):
            raise ValueError(f"Invalid rum_custom_condition: {rum_cond_msg}")
    top_rum_cond = raw_top_rum_cond.strip()
    nested_rum_cond = raw_nested_rum_cond.strip()
    rum_cond = nested_rum_cond if rum_dict.get("custom_condition") is not None else top_rum_cond

    nested_faro = _tf_str(rum_dict.get("faro_version"), "rum.faro_version")
    top_faro = _tf_str(cfg.get("faro_version"), "faro_version")
    for fv in (nested_faro, top_faro):
        if fv:
            _assert_faro_version_safe(fv)
    resolved_faro = nested_faro or top_faro or None

    top_cmcd_ver = _tf_int(cfg["cmcd_version"], "cmcd_version") if cfg.get("cmcd_version") is not None else None
    nested_cmcd_ver = _tf_int(cmcd_cfg["version"], "cmcd.version") if cmcd_cfg.get("version") is not None else None
    cmcd_version = top_cmcd_ver if top_cmcd_ver is not None else (nested_cmcd_ver if nested_cmcd_ver is not None else 1)
    top_cmcd_mode = _tf_str(cfg.get("cmcd_mode"), "cmcd_mode")
    nested_cmcd_mode = _tf_str(cmcd_cfg.get("mode"), "cmcd.mode")
    cmcd_mode = top_cmcd_mode or nested_cmcd_mode or "query_string"

    scoring_enabled = is_scoring_enabled(cfg)
    dom_aliases = (
        _tf_str(scoring_cfg.get("domain"), "scoring.domain"),
        _tf_str(scoring_cfg.get("scoring_domain"), "scoring.scoring_domain"),
        _tf_str(cfg.get("scoring_domain"), "scoring_domain"),
    )
    for dv in dom_aliases:
        if dv:
            validate_quoted_literal(dv, field="scoring.domain")
            if ".." in dv or not re.fullmatch(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$", dv):
                raise ValueError(f"scoring.domain must be a valid hostname, got {dv!r}")
    scoring_domain = dom_aliases[0] or dom_aliases[1] or dom_aliases[2] or ""

    nested_req_sec = _tf_str(scoring_cfg.get("request_secret"), "scoring.request_secret")
    top_req_sec = _tf_str(cfg.get("scoring_request_secret"), "scoring_request_secret")
    for sv in (nested_req_sec, top_req_sec):
        if sv:
            validate_quoted_literal(sv, allow_backslash=False, field="scoring.request_secret")
    resolved_req_sec = nested_req_sec if scoring_cfg.get("request_secret") is not None else top_req_sec

    top_excl_re = _tf_str(cfg.get("scoring_exclude_url_regex"), "scoring_exclude_url_regex")
    nested_excl_re = _tf_str(scoring_cfg.get("exclude_url_regex"), "scoring.exclude_url_regex")
    for ev in (top_excl_re, nested_excl_re):
        if ev:
            validate_url_exclusion_regex(ev)
    resolved_excl_re = top_excl_re if cfg.get("scoring_exclude_url_regex") is not None else nested_excl_re

    for enf_alias in (cfg.get("scoring_enforce_status_code"), scoring_cfg.get("enforce_status_code")):
        if enf_alias is not None and (not isinstance(enf_alias, int) or isinstance(enf_alias, bool)):
            raise ValueError(f"scoring.enforce_status_code must be an integer, got {enf_alias!r}")
    raw_enf_code = (
        cfg.get("scoring_enforce_status_code")
        if cfg.get("scoring_enforce_status_code") is not None
        else scoring_cfg.get("enforce_status_code")
    )

    rum_for_fs = {**rum_dict, "custom_condition": rum_cond}
    if resolved_faro is not None:
        rum_for_fs["faro_version"] = resolved_faro
    elif "faro_version" in rum_for_fs:
        rum_for_fs["faro_version"] = None

    fs_cfg: dict[str, Any] = {
        "service_id": service_id,
        "log_period": period,
        "rum_log_period": rum_period,
        "sample_rate": sample_rate,
        "edge_only": edge_only,
        "custom_condition": custom_condition,
        "fos_prefix": prefix,
        "cdn_shield": resolved_shield,
        "logging_enabled": is_logging_enabled(cfg),
        "rum_enabled": is_rum_enabled(cfg),
        "rum": rum_for_fs,
        "cmcd": {
            "enabled": is_cmcd_enabled(cfg),
            "mode": cmcd_mode,
            "version": cmcd_version,
        },
        "scoring": {
            "enabled": scoring_enabled,
            "domain": scoring_domain,
            "request_secret": resolved_req_sec,
            "exclude_url_regex": resolved_excl_re,
            "enforce_status_code": 429 if raw_enf_code is None else raw_enf_code,
        },
        "log_fields": cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []},
    }
    state = FeatureState.from_config(fs_cfg)
    desired_endpoints = desired_logging_endpoints(state)
    desired_backends_list = desired_backends(state)
    cdn_vcl = load_vcl(rate_limiting=True)
    cond_parts = ["!segmented_caching.is_inner_req"]
    if edge_only:
        if scoring_enabled:
            cond_parts.append("fastly.ff.visits_this_service == 0")
        else:
            cond_parts.append("(req.restarts == 0 && fastly.ff.visits_this_service == 0)")
    if sample_rate < 100:
        cond_parts.append(f"randombool({sample_rate}, 100)")
    if custom_condition:
        cond_parts.append(f"({custom_condition})")
    cond_stmt = " && ".join(cond_parts)
    fos_host = f"{region}.object.fastlystorage.app"
    files: dict[str, str] = {}
    # ── 1. Companion VCL files (not Terraform config) ──────────────────────────
    files["cdn_proxy.vcl"] = cdn_vcl
    # ── 2. versions.tf.json — provider pinning ─────────────────────────────────
    files["versions.tf.json"] = _dump(
        {
            "terraform": {
                "required_version": ">= 1.6",
                "required_providers": {
                    "aws": {"source": "hashicorp/aws", "version": "~> 5.0"},
                    "fastly": {"source": "fastly/fastly", "version": "~> 10.3"},
                },
            },
        }
    )
    # ── 3. fos.tf.json — Object Storage bucket ─────────────────────────────────
    files["fos.tf.json"] = _dump(
        {"resource": {"aws_s3_bucket": {"fos_bucket": {"bucket": _terraform_template_escape(bucket)}}}}
    )
    # ── 4. cdn_proxy.tf.json — CDN proxy service + dictionaries ────────────────
    cdn_snippets_blocks: list[dict] = []
    for name, type_, content, priority in _CDN_SNIPPETS:
        snip_filename = f"cdn_snippets/{name.replace('-', '_')}.vcl"
        files[snip_filename] = content
        cdn_snippets_blocks.append(
            {
                "name": name,
                "type": type_,
                "priority": priority,
                "content": f'${{file("${{path.module}}/{snip_filename}")}}',
            }
        )
    cdn_proxy_block: dict[str, Any] = {
        "name": _terraform_template_escape(cdn_service_name),
        "domain": [{"name": _terraform_template_escape(cdn_domain)}],
        "backend": [
            {
                "name": "fos_origin",
                "address": _terraform_template_escape(fos_host),
                "port": 443,
                "use_ssl": True,
                "ssl_check_cert": True,
                "override_host": _terraform_template_escape(fos_host),
                "ssl_cert_hostname": _terraform_template_escape(fos_host),
                "ssl_sni_hostname": _terraform_template_escape(fos_host),
                "connect_timeout": 5000,
                "first_byte_timeout": 60000,
                "between_bytes_timeout": 30000,
            }
        ],
        "vcl": [{"name": "main", "content": '${file("${path.module}/cdn_proxy.vcl")}', "main": True}],
        "dictionary": [{"name": "fos_credentials", "write_only": True}, {"name": "cdn_auth", "write_only": True}],
        "snippet": cdn_snippets_blocks,
    }
    if cdn_shield and cdn_shield.lower() != "none":
        cdn_proxy_block["backend"][0]["shield"] = _terraform_template_escape(cdn_shield)
    files["cdn_proxy.tf.json"] = _dump(
        {
            "resource": {
                "fastly_service_vcl": {"cdn_proxy": cdn_proxy_block},
                "fastly_service_dictionary_items": {
                    "fos_credentials": {
                        "service_id": "${fastly_service_vcl.cdn_proxy.id}",
                        "dictionary_id": '${{ for d in fastly_service_vcl.cdn_proxy.dictionary : d.name => d.dictionary_id }["fos_credentials"]}',
                        "items": {
                            "access_key": _terraform_template_escape(fos_access_key),
                            "secret_key": _terraform_template_escape(fos_secret_key),
                            "bucket": "${aws_s3_bucket.fos_bucket.bucket}",
                            "region": _terraform_template_escape(region),
                        },
                    },
                    "cdn_auth": {
                        "service_id": "${fastly_service_vcl.cdn_proxy.id}",
                        "dictionary_id": '${{ for d in fastly_service_vcl.cdn_proxy.dictionary : d.name => d.dictionary_id }["cdn_auth"]}',
                        "items": {"secret_key": _terraform_template_escape(cdn_secret)},
                    },
                },
            },
        }
    )
    # ── 5. logging_service.tf.json — Logging endpoint on existing service ──────
    logging_service_snips = logging_service_snippets(state)
    logging_snippet_blocks: list[dict] = []
    seen_filenames: set[str] = set()
    for snippet in logging_service_snips:
        subroutine = snippet.subroutine.replace("vcl_", "")
        if snippet.name == "Fastly Log Analysis Reset Client IP":
            snip_filename = "capture_snippets/recv_reset.vcl"
        else:
            snip_filename = f"capture_snippets/{subroutine}.vcl"
        if snip_filename not in seen_filenames:
            files[snip_filename] = snippet.body
            seen_filenames.add(snip_filename)
        logging_snippet_blocks.append(
            {
                "name": _terraform_template_escape(snippet.name),
                "type": subroutine,
                "priority": snippet.priority,
                "content": f'${{file("${{path.module}}/{snip_filename}")}}',
            }
        )
    logging_fos_blocks: list[dict] = []
    condition_blocks: list[dict] = []
    for endpoint in desired_endpoints:
        if endpoint.name == "Fastly Log Analytics":
            cond_name = f"Log Sampling - {_terraform_template_escape(endpoint_name)}"
            condition_blocks.append(
                {"name": cond_name, "statement": _terraform_template_escape(cond_stmt), "type": "RESPONSE"}
            )
            resp_cond = cond_name
            format_filename = "log_format.vcl"
        else:
            resp_cond = ""
            format_filename = f"{_safe_vcl_filename_stem(endpoint.name)}_format.vcl"
        files[format_filename] = endpoint.format_string
        block: dict[str, Any] = {
            "name": _terraform_template_escape(endpoint.name),
            "bucket_name": "${aws_s3_bucket.fos_bucket.bucket}",
            "region": _terraform_template_escape(region),
            "path": _terraform_template_escape(endpoint.path),
            "period": endpoint.period,
            "compression_codec": getattr(endpoint, "compression_codec", None) or "zstd",
            "message_type": "blank",
            "response_condition": resp_cond,
            "format_version": 2,
            "format": f'${{file("${{path.module}}/{format_filename}")}}',
        }
        logging_fos_blocks.append(block)
    logging_backends: list[dict[str, Any]] = []
    for backend in desired_backends_list:
        address = backend.address
        if address == "fos.example.com":
            address = fos_host
        cert_host = backend.ssl_cert_hostname or backend.ssl_hostname or address
        if cert_host == "fos.example.com":
            cert_host = fos_host
        sni_host = backend.ssl_sni_hostname or backend.ssl_hostname or cert_host
        if sni_host == "fos.example.com":
            sni_host = fos_host
        override_host = backend.override_host
        if override_host == "fos.example.com":
            override_host = fos_host
        b_block: dict[str, Any] = {
            "name": _terraform_template_escape(backend.name),
            "address": _terraform_template_escape(address),
            "port": backend.port,
            "use_ssl": backend.use_ssl,
            "ssl_check_cert": backend.ssl_check_cert,
            "ssl_cert_hostname": _terraform_template_escape(cert_host),
            "ssl_sni_hostname": _terraform_template_escape(sni_host),
            "connect_timeout": backend.connect_timeout,
            "first_byte_timeout": backend.first_byte_timeout,
            "between_bytes_timeout": backend.between_bytes_timeout,
            "auto_loadbalance": backend.auto_loadbalance,
        }
        if override_host:
            b_block["override_host"] = _terraform_template_escape(override_host)
        if backend.shield:
            b_block["shield"] = _terraform_template_escape(backend.shield)
        logging_backends.append(b_block)
    logging_service_block: dict[str, Any] = {
        "name": _terraform_template_escape(cfg.get("service_name", "Logging Service")),
        "domain": [{"name": "example.com"}],
        **({"logging_fos": logging_fos_blocks} if logging_fos_blocks else {}),
        "snippet": logging_snippet_blocks,
    }
    if condition_blocks:
        logging_service_block["condition"] = condition_blocks
    if logging_backends:
        logging_service_block["backend"] = logging_backends
    files["logging_service.tf.json"] = _dump(
        {"resource": {"fastly_service_vcl": {"logging_service": logging_service_block}}}
    )
    # ── 6. instructions — companion README explaining the layout ──────────────
    safe_region = _hcl_quoted_escape(region)
    safe_fos_host = _hcl_quoted_escape(fos_host)
    safe_service_id = _hcl_quoted_escape(service_id)
    files["instructions"] = f"""\
# Fastly Log Analysis Terraform Export

This directory contains the Terraform configuration to set up Fastly Object Storage logging and a CDN proxy for the Fastly Log Analysis tool.

Configuration is emitted as Terraform's JSON syntax (`.tf.json`). All Terraform commands (`init`, `fmt`, `validate`, `plan`, `apply`) accept it interchangeably with HCL.

## Files
- `versions.tf.json`: Terraform CLI floor + pinned versions for the `aws` and `fastly` providers (`fastly` 10.3 or later in 10.x, the first release with `logging_fos`).
- `fos.tf.json`: The Fastly Object Storage bucket resource (created via the AWS S3-compatible provider).
- `cdn_proxy.tf.json`: A NEW Fastly Delivery service that fronts the FOS bucket for fast dashboard access.
- `logging_service.tf.json`: The logging endpoint and capture snippets for your existing service. The endpoint is a native Fastly Object Storage endpoint (`logging_fos`): Fastly creates an access key limited to the bucket for it, so no keys are written here.
- `cdn_proxy.vcl`: The main VCL for the CDN proxy service (loaded by `cdn_proxy.tf.json`).
- `log_format.vcl`: The JSON log format string (loaded by `logging_service.tf.json`).
- `cdn_snippets/`: VCL snippets for the CDN proxy service (loaded by `cdn_proxy.tf.json`).
- `capture_snippets/`: VCL snippets for your existing logging service (loaded by `logging_service.tf.json`).

## AWS provider configuration

`fos.tf.json` declares an `aws_s3_bucket` resource. Fastly Object Storage is S3-compatible, so the AWS provider works against the Fastly FOS endpoint. Configure it in your root module (or alongside these files) like this:

```hcl
provider "aws" {{
  region                      = "{safe_region}"
  access_key                  = "<your fos access key>"
  secret_key                  = "<your fos secret key>"
  endpoints {{ s3 = "https://{safe_fos_host}" }}
  skip_credentials_validation = true
  skip_region_validation      = true
  skip_requesting_account_id  = true
}}
```

## Instructions
1. Review `fos.tf.json` and ensure the `aws` provider above is correctly configured.
2. `cdn_proxy.tf.json` creates a NEW Fastly service to accelerate your log reads.
3. `logging_service.tf.json` contains the configuration for your ACTIVE service. You should copy the `logging_fos`, `condition`, and `snippet` blocks into your existing `fastly_service_vcl` resource for service ID `{safe_service_id}`. Applying a `logging_fos` block needs a Fastly token with the Engineer role and access to all services, or Superuser.
4. Run `terraform init` and `terraform apply` to deploy the changes.
"""
    if cfg.get("deployment_mode") == "cloud":
        raw_install = str(cfg.get("install_name", "flp")).replace("\x00", "").replace("\r", "").replace("\n", "")
        install_name = _terraform_template_escape(re.sub(r"[^a-zA-Z0-9_-]", "-", raw_install).strip("-") or "flp")
        compute_domain = (
            _tf_str(cfg.get("cloud_compute_domain"), "cloud_compute_domain", default="").strip()
            or f"{generate_opaque_label('fla')}.edgecompute.app"
        )
        worker_domain = (
            _tf_str(cfg.get("cloud_worker_domain"), "cloud_worker_domain", default="").strip()
            or f"{generate_opaque_label('fla')}.edgecompute.app"
        )
        front_domain = (
            _tf_str(cfg.get("cloud_front_domain"), "cloud_front_domain", default="").strip()
            or _tf_str(cfg.get("share_domain"), "share_domain", default="").strip()
            or f"{generate_opaque_label('fla')}.global.ssl.fastly.net"
        )
        files["cloud_compute.tf.json"] = _dump(
            {
                "resource": {
                    "fastly_service_compute": {
                        "flp": {
                            "name": install_name,
                            "domain": [{"name": compute_domain}],
                        },
                        "flp_worker": {
                            "name": f"{install_name}-worker",
                            "domain": [{"name": worker_domain}],
                        },
                    },
                    "fastly_service_vcl": {
                        "flp_front": {
                            "name": f"{install_name}-front",
                            "domain": [{"name": front_domain}],
                        }
                    },
                }
            }
        )
    return files


def generate_cloud_terraform(cfg: dict[str, Any], fos_access_key: str, fos_secret_key: str) -> dict[str, str]:
    """Generate Terraform .tf.json including Cloud flp-front + flp/flp-worker resources."""
    return generate_terraform({**cfg, "deployment_mode": "cloud"}, fos_access_key, fos_secret_key)


from backend.provision.declarative.reconciler import (  # noqa: E402
    FeatureState,
    desired_backends,
    desired_logging_endpoints,
    logging_service_snippets,
)
from backend.provision.fastly_api import _CDN_SNIPPETS  # noqa: E402
