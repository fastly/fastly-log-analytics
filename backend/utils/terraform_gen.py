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


from backend.config import is_rum_enabled  # noqa: E402
from backend.core.catalog import validate_vcl_custom_condition  # noqa: E402
from backend.core.fastly.client import load_vcl  # noqa: E402
from backend.core.service_defaults import DEFAULT_LOG_PERIOD_SECONDS


def generate_terraform(cfg: dict[str, Any], fos_access_key: str, fos_secret_key: str) -> dict[str, str]:
    """Generate Terraform .tf.json for the given provisioning configuration."""
    service_id = str(cfg.get("logging_service_id", "YOUR_SERVICE_ID")).replace("\r", "").replace("\n", "")
    endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "fastly_log_analysis")
    region = cfg.get("fos_region", "us-east-1")
    bucket = cfg.get("fos_bucket_name", "your-bucket-name")
    prefix = cfg.get("fos_prefix", "").strip("/")
    try:
        period = int(cfg.get("log_period", DEFAULT_LOG_PERIOD_SECONDS))
    except (TypeError, ValueError):
        period = DEFAULT_LOG_PERIOD_SECONDS
    try:
        rum_period_val = cfg.get("rum_log_period") or (cfg.get("rum") or {}).get("log_period")
        rum_period = int(rum_period_val) if rum_period_val is not None else None
    except (TypeError, ValueError):
        rum_period = None
    cdn_service_name = cfg.get("cdn_service_name", "Fastly Log Analysis CDN Proxy")
    cdn_prefix = cfg.get("cdn_prefix", bucket)
    cdn_domain = f"{cdn_prefix}.global.ssl.fastly.net"
    cdn_shield = cfg.get("cdn_shield", "iad-va-us")
    cdn_secret = cfg.get("cdn_secret", "replace-with-secure-secret")
    prov = cfg.get("provisioning", {})
    sample_rate = prov.get("sample_rate") if "sample_rate" in prov else cfg.get("sample_rate", 100)
    sample_rate = int(sample_rate) if sample_rate is not None else 100
    edge_only = prov.get("edge_only") if "edge_only" in prov else cfg.get("edge_only", False)
    edge_only = bool(edge_only)
    custom_condition = prov.get("custom_condition") if "custom_condition" in prov else cfg.get("custom_condition", "")
    custom_condition = (custom_condition or "").strip()
    if custom_condition:
        cond_msg = validate_vcl_custom_condition(custom_condition)
        if cond_msg:
            raise ValueError(f"Invalid custom_condition: {cond_msg}")
    raw_rum = cfg.get("rum")
    rum_dict: dict[str, Any] = raw_rum if isinstance(raw_rum, dict) else {}
    rum_cond_raw = (
        cfg.get("rum_custom_condition")
        if cfg.get("rum_custom_condition") is not None
        else rum_dict.get("custom_condition", "")
    )
    rum_cond = (rum_cond_raw or "").strip()
    if rum_cond:
        rum_cond_msg = validate_vcl_custom_condition(rum_cond)
        if rum_cond_msg:
            raise ValueError(f"Invalid rum_custom_condition: {rum_cond_msg}")
    cmcd_cfg = cfg.get("cmcd", {})
    scoring_cfg = cfg.get("scoring", {})
    scoring_enabled = bool(scoring_cfg.get("enabled", cfg.get("scoring_enabled", False)))
    scoring_domain = str(
        scoring_cfg.get("domain") or scoring_cfg.get("scoring_domain") or cfg.get("scoring_domain") or ""
    )
    fs_cfg: dict[str, Any] = {
        "service_id": service_id,
        "log_period": period,
        "rum_log_period": rum_period,
        "sample_rate": sample_rate,
        "edge_only": edge_only,
        "custom_condition": custom_condition,
        "fos_prefix": prefix,
        "logging_enabled": bool(cfg.get("logging_enabled", True)),
        "rum_enabled": is_rum_enabled(cfg),
        "rum": {**rum_dict, **({"custom_condition": rum_cond} if rum_cond else {})},
        "cmcd": {
            "enabled": bool(cmcd_cfg.get("enabled", False)),
            "mode": cmcd_cfg.get("mode", "query_string"),
            "version": int(cmcd_cfg.get("version", 1)),
        },
        "scoring": {
            "enabled": scoring_enabled,
            "domain": scoring_domain,
            "request_secret": scoring_cfg.get("request_secret", cfg.get("scoring_request_secret", "")),
            "exclude_url_regex": scoring_cfg.get("exclude_url_regex", ""),
            "enforce_status_code": int(scoring_cfg.get("enforce_status_code", 429)),
        },
        "log_fields": cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []},
    }
    state = FeatureState.from_config(fs_cfg)
    desired_endpoints = adopt_live_endpoint_types(desired_logging_endpoints(state), _live_logging_endpoints(cfg))
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
    logging_blocks: dict[str, list[dict]] = {"logging_fos": [], "logging_s3": []}
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
            "path": _terraform_template_escape(endpoint.path),
            "period": endpoint.period,
            "compression_codec": getattr(endpoint, "compression_codec", None) or "zstd",
            "message_type": "blank",
            "response_condition": resp_cond,
            "format_version": 2,
            "format": f'${{file("${{path.module}}/{format_filename}")}}',
        }
        if endpoint.endpoint_type == "s3":
            block.update(
                {
                    "domain": _terraform_template_escape(fos_host),
                    "timestamp_format": "%Y-%m-%dT%H:%M:%S.000",
                    "s3_access_key": _terraform_template_escape(fos_access_key),
                    "s3_secret_key": _terraform_template_escape(fos_secret_key),
                }
            )
            logging_blocks["logging_s3"].append(block)
        else:
            block["region"] = _terraform_template_escape(region)
            logging_blocks["logging_fos"].append(block)
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
        **{kind: blocks for kind, blocks in logging_blocks.items() if blocks},
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
- `logging_service.tf.json`: The logging endpoint and capture snippets for your existing service. The endpoint is a native Fastly Object Storage endpoint (`logging_fos`): Fastly creates an access key limited to the bucket for it, so no keys are written here. A service that already logs through the S3-compatible endpoint keeps it as `logging_s3`, signed with your FOS access key.
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
3. `logging_service.tf.json` contains the configuration for your ACTIVE service. You should copy the `logging_fos` (or `logging_s3`), `condition`, and `snippet` blocks into your existing `fastly_service_vcl` resource for service ID `{safe_service_id}`. Applying a `logging_fos` block needs a Fastly token with the Engineer role and access to all services, or Superuser.
4. Run `terraform init` and `terraform apply` to deploy the changes.
"""
    if cfg.get("deployment_mode") == "cloud":
        raw_install = str(cfg.get("install_name", "flp")).replace("\x00", "").replace("\r", "").replace("\n", "")
        install_name = _terraform_template_escape(re.sub(r"[^a-zA-Z0-9_-]", "-", raw_install).strip("-") or "flp")
        files["cloud_compute.tf.json"] = _dump(
            {
                "resource": {
                    "fastly_service_compute": {
                        "flp": {
                            "name": install_name,
                            "domain": [{"name": f"{install_name}.edgecompute.app"}],
                        },
                        "flp_worker": {
                            "name": f"{install_name}-worker",
                            "domain": [{"name": f"{install_name}-worker.edgecompute.app"}],
                        },
                    },
                    "fastly_service_vcl": {
                        "flp_front": {
                            "name": f"{install_name}-front",
                            "domain": [{"name": f"{install_name}.global.ssl.fastly.net"}],
                        }
                    },
                }
            }
        )
    return files


def _live_logging_endpoints(cfg: dict[str, Any]) -> list[LoggingEndpoint]:
    """The service's live logging endpoints, so an existing legacy S3 endpoint is exported as S3, as reconcile keeps it."""
    token = cfg.get("token")
    service_id = cfg.get("logging_service_id")
    if not token or not service_id:
        return []
    return _fetch_logging_endpoints(str(service_id), str(token))


def generate_cloud_terraform(cfg: dict[str, Any], fos_access_key: str, fos_secret_key: str) -> dict[str, str]:
    """Generate Terraform .tf.json including Cloud flp-front + flp/flp-worker resources."""
    return generate_terraform({**cfg, "deployment_mode": "cloud"}, fos_access_key, fos_secret_key)


from backend.provision.declarative.models import LoggingEndpoint  # noqa: E402
from backend.provision.declarative.reconciler import (  # noqa: E402
    FeatureState,
    _fetch_logging_endpoints,
    desired_backends,
    desired_logging_endpoints,
    logging_service_snippets,
)
from backend.provision.declarative.vcl_builder import adopt_live_endpoint_types  # noqa: E402
from backend.provision.fastly_api import _CDN_SNIPPETS  # noqa: E402
