"""Terraform generation for Fastly Object Storage log analysis.

Emits Terraform configuration as ``.tf.json`` files. The JSON shape is
Terraform's official `JSON configuration syntax
<https://developer.hashicorp.com/terraform/language/syntax/json>`_ —
``terraform init / fmt / validate / plan / apply`` all accept it
interchangeably with HCL.

**Why JSON and not HCL.** The prior HCL implementation built each file
with f-strings and a custom ``_hcl_escape`` regex helper that handled
backslashes, quotes, newlines, tabs, and Terraform-template syntax. Any
field that escaped through unsplit was an injection vector (an attacker-
supplied bucket name with a stray quote could close the HCL string and
splice arbitrary HCL — see the audit comments at the prior commit). The
JSON path replaces the entire escaping primitive with :func:`json.dumps`
(stdlib, audited, fuzzed to death) and a 4-line
:func:`_terraform_template_escape` helper that only handles the one
Terraform-specific concern JSON doesn't own — the ``${…}`` / ``%{…}``
template syntax that Terraform still interprets inside JSON string
values.

What :func:`json.dumps` owns:
  - ``\\`` (backslashes) escaped as ``\\\\``
  - ``"`` (quotes) escaped as ``\\"``
  - control bytes (newline, tab, CR, etc.) escaped as ``\\n``/``\\t``/``\\r``
  - Unicode handled correctly
  - Output guaranteed parseable JSON (no half-formed strings, no
    open-brace mismatches, no missing commas)

What we still escape:
  - ``${`` → ``$${`` (Terraform interpolation opener)
  - ``%{`` → ``%%{`` (Terraform template-directive opener)

If a future Terraform release adds a third template-prefix character, the
escape list grows by one line. The whole-string regex sweep is gone.
"""

from __future__ import annotations

import json
from typing import Any

from backend.core.fastly.utils import load_vcl
from backend.provision.declarative.generators import (
    desired_backends,
    desired_logging_endpoints,
    desired_snippets,
    logging_service_snippets,
)
from backend.provision.declarative.state import FeatureState
from backend.provision.fastly_api import _CDN_SNIPPETS


def _terraform_template_escape(value: object) -> str:
    """Escape ``${`` / ``%{`` inside a string so Terraform treats them as
    literal characters rather than template-syntax openers.

    JSON-level escaping (backslashes, quotes, control bytes) is handled by
    :func:`json.dumps` at serialise time — this function intentionally does
    NOT touch those characters. The ONLY thing it owns is the Terraform-
    interpreter-level template prefix that survives JSON encoding."""
    s = "" if value is None else str(value)
    return s.replace("${", "$${").replace("%{", "%%{")


def _dump(obj: dict) -> str:
    """Canonical .tf.json serialisation: 2-space indent, keys sorted for
    determinism, trailing newline so ``terraform fmt -check`` is happy.

    Sorted keys are load-bearing for the idempotency contract — Python
    dict insertion order is preserved but the test suite (and human diffs)
    are easier to read when keys are in the same order across runs and
    across machines."""
    return json.dumps(obj, indent=2, sort_keys=True) + "\n"


def generate_terraform(cfg: dict[str, Any], fos_access_key: str, fos_secret_key: str) -> dict[str, str]:
    """Generate Terraform .tf.json for the given provisioning configuration.

    Returns a ``{filename: content}`` map. Filenames have ``.tf.json``
    extensions (Terraform recognises this suffix as JSON config). The VCL
    snippet files keep their ``.vcl`` extension — they're referenced from
    the JSON via ``file("${path.module}/X")`` and aren't Terraform config
    themselves.
    """
    # 023: service_id ends up inside a JSON string. JSON encoding handles
    # quotes/newlines, but the value also appears in the rendered
    # instructions README (plain markdown) where a stray CR/LF would break
    # the surrounding sentence. Strip both before any use.
    service_id = str(cfg.get("logging_service_id", "YOUR_SERVICE_ID")).replace("\r", "").replace("\n", "")
    endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "fastly_log_analysis")
    region = cfg.get("fos_region", "us-east-1")
    bucket = cfg.get("fos_bucket_name", "your-bucket-name")
    prefix = cfg.get("fos_prefix", "").strip("/")
    # 022: log_period flows into a numeric position in the JSON. Cast to
    # int with a safe fallback so attacker-supplied non-numerics can't
    # smuggle a string where Terraform expects an int.
    try:
        period = int(cfg.get("log_period", 3600))
    except (TypeError, ValueError):
        period = 3600
    cdn_service_name = cfg.get("cdn_service_name", "Fastly Log Analysis CDN Proxy")
    cdn_prefix = cfg.get("cdn_prefix", bucket)
    cdn_domain = f"{cdn_prefix}.global.ssl.fastly.net"
    cdn_shield = cfg.get("cdn_shield", "iad-va-us")
    cdn_secret = cfg.get("cdn_secret", "replace-with-secure-secret")

    # Prefer nested provisioning.* fields, fall back to legacy flat fields for backward compat
    prov = cfg.get("provisioning", {})
    sample_rate = prov.get("sample_rate") if "sample_rate" in prov else cfg.get("sample_rate", 100)
    sample_rate = int(sample_rate) if sample_rate is not None else 100
    edge_only = prov.get("edge_only") if "edge_only" in prov else cfg.get("edge_only", False)
    edge_only = bool(edge_only)
    custom_condition = prov.get("custom_condition") if "custom_condition" in prov else cfg.get("custom_condition", "")
    custom_condition = (custom_condition or "").strip()

    # Extract nested CMCD and Scoring config
    cmcd_cfg = cfg.get("cmcd", {})
    scoring_cfg = cfg.get("scoring", {})

    # Construct robust declarative FeatureState to reuse all canonical VCL/endpoint rules
    fs_cfg = {
        "service_id": service_id,
        "log_period": period,
        "sample_rate": sample_rate,
        "edge_only": edge_only,
        "custom_condition": custom_condition,
        "fos_prefix": prefix,
        "logging_enabled": bool(cfg.get("logging_enabled", True)),
        "rum_enabled": bool(cfg.get("rum_enabled", False)),
        "cmcd": {
            "enabled": bool(cmcd_cfg.get("enabled", False)),
            "mode": cmcd_cfg.get("mode", "query_string"),
            "version": int(cmcd_cfg.get("version", 1)),
        },
        "scoring": {
            "enabled": bool(scoring_cfg.get("enabled", False)),
            "domain": scoring_cfg.get("domain", ""),
            "request_secret": scoring_cfg.get("request_secret", ""),
            "exclude_url_regex": scoring_cfg.get("exclude_url_regex", ""),
            "enforce_status_code": int(scoring_cfg.get("enforce_status_code", 429)),
        },
        "log_fields": cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []},
    }
    state = FeatureState.from_config(fs_cfg)

    # Use the same code to generate snippets, endpoints, and formats
    desired_snippets_list = desired_snippets(state)
    desired_endpoints = desired_logging_endpoints(state)
    desired_backends_list = desired_backends(state)

    cdn_vcl = load_vcl(rate_limiting=True)

    cond_parts = ["!segmented_caching.is_inner_req"]
    if edge_only:
        # Restart-tolerant when scoring is enabled: scoring restarts the request
        # (req.restarts 0→1), so gating on req.restarts == 0 would drop every
        # scored request from the logs. Mirrors fastly_api._log_sampling_edge_clause.
        if scoring_cfg.get("enabled", False):
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
    # Pin major versions so an upstream bump (Fastly v6, AWS v7) doesn't
    # silently break customer ``terraform apply``. Pessimistic operator
    # ``~> X.0`` allows minor/patch upgrades. If you bump these, also update
    # tests/utils/test_terraform_gen.py.
    files["versions.tf.json"] = _dump(
        {
            "terraform": {
                "required_version": ">= 1.6",
                "required_providers": {
                    "aws": {"source": "hashicorp/aws", "version": "~> 5.0"},
                    "fastly": {"source": "fastly/fastly", "version": "~> 5.0"},
                },
            },
        }
    )

    # ── 3. fos.tf.json — Object Storage bucket ─────────────────────────────────
    files["fos.tf.json"] = _dump(
        {
            "resource": {
                "aws_s3_bucket": {
                    "fos_bucket": {
                        "bucket": _terraform_template_escape(bucket),
                    },
                },
            },
        }
    )

    # ── 4. cdn_proxy.tf.json — CDN proxy service + dictionaries ────────────────
    # Multi-occurrence sub-blocks like ``snippet`` must be JSON arrays. The
    # ``${…}`` patterns inside string values are Terraform references that we
    # want preserved verbatim — we do NOT pass these through
    # ``_terraform_template_escape`` because we authored them, they're not
    # user input. The escape is for fields whose values come from ``cfg``.
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
                "ssl_cert_hostname": _terraform_template_escape(fos_host),
                "ssl_sni_hostname": _terraform_template_escape(fos_host),
                "connect_timeout": 5000,
                "first_byte_timeout": 60000,
                "between_bytes_timeout": 30000,
            }
        ],
        "vcl": [
            {
                "name": "main",
                "content": '${file("${path.module}/cdn_proxy.vcl")}',
                "main": True,
            }
        ],
        "dictionary": [
            {"name": "fos_credentials", "write_only": True},
            {"name": "cdn_auth", "write_only": True},
        ],
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
                        "items": {"secret": _terraform_template_escape(cdn_secret)},
                    },
                },
            },
        }
    )

    # ── 5. logging_service.tf.json — Logging endpoint on existing service ──────
    # Use logging-service-specific snippets (capture VCL for all stages)
    logging_service_snips = logging_service_snippets(state)
    logging_snippet_blocks: list[dict] = []

    # Track which subroutines we've already written to avoid conflicts when
    # multiple snippets target the same subroutine (e.g., recv + recv_reset)
    seen_filenames: set[str] = set()

    for snippet in logging_service_snips:
        subroutine = snippet.subroutine.replace("vcl_", "")
        # Handle recv_reset specially (it's a recv snippet but needs a different filename)
        if snippet.name == "Fastly Log Analysis Reset Client IP":
            snip_filename = "capture_snippets/recv_reset.vcl"
        else:
            snip_filename = f"capture_snippets/{subroutine}.vcl"

        # Avoid duplicate filenames (recv and recv_reset both target recv subroutine)
        if snip_filename not in seen_filenames:
            files[snip_filename] = snippet.body
            seen_filenames.add(snip_filename)

        logging_snippet_blocks.append(
            {
                "name": snippet.name,
                "type": subroutine,
                "priority": snippet.priority,
                "content": f'${{file("${{path.module}}/{snip_filename}")}}',
            }
        )

    logging_s3_blocks: list[dict] = []
    condition_blocks: list[dict] = []

    for endpoint in desired_endpoints:
        if endpoint.name == "Fastly Log Analytics":
            cond_name = f"Log Sampling - {_terraform_template_escape(endpoint_name)}"
            condition_blocks.append(
                {
                    "name": cond_name,
                    "statement": _terraform_template_escape(cond_stmt),
                    "type": "RESPONSE",
                }
            )
            resp_cond = cond_name
            format_filename = "log_format.vcl"
        else:
            resp_cond = ""
            format_filename = f"{endpoint.name.lower().replace(' ', '_')}_format.vcl"

        files[format_filename] = endpoint.format_string

        logging_s3_blocks.append(
            {
                "name": _terraform_template_escape(endpoint.name),
                "bucket_name": "${aws_s3_bucket.fos_bucket.bucket}",
                "domain": _terraform_template_escape(fos_host),
                "path": _terraform_template_escape(endpoint.path),
                "period": endpoint.period,
                "gzip_level": 9,
                "message_type": "blank",
                "timestamp_format": "%Y-%m-%dT%H:%M:%S.000",
                "response_condition": resp_cond,
                "format_version": 2,
                "format": f'${{file("${{path.module}}/{format_filename}")}}',
                "s3_access_key": _terraform_template_escape(fos_access_key),
                "s3_secret_key": _terraform_template_escape(fos_secret_key),
            }
        )

    logging_backends: list[dict] = []
    for backend in desired_backends_list:
        address = backend.address
        if address == "fos.example.com":
            address = fos_host
        logging_backends.append(
            {
                "name": backend.name,
                "address": _terraform_template_escape(address),
                "port": backend.port,
                "use_ssl": True,
                "ssl_cert_hostname": _terraform_template_escape(address),
                "ssl_sni_hostname": _terraform_template_escape(address),
                "connect_timeout": backend.connect_timeout,
                "first_byte_timeout": backend.first_byte_timeout,
                "between_bytes_timeout": backend.between_bytes_timeout,
                "auto_loadbalance": backend.auto_loadbalance,
            }
        )

    logging_service_block: dict[str, Any] = {
        "name": _terraform_template_escape(cfg.get("service_name", "Logging Service")),
        "domain": [{"name": "example.com"}],
        "logging_s3": logging_s3_blocks,
        "snippet": logging_snippet_blocks,
    }
    if condition_blocks:
        logging_service_block["condition"] = condition_blocks
    if logging_backends:
        logging_service_block["backend"] = logging_backends

    files["logging_service.tf.json"] = _dump(
        {
            "resource": {
                "fastly_service_vcl": {"logging_service": logging_service_block},
            },
        }
    )

    # ── 6. instructions — companion README explaining the layout ──────────────
    # JSON can't carry comments; the explanatory text that used to live as
    # HCL banner comments now lives here. Customers read this before
    # running ``terraform apply``.
    files["instructions"] = f"""\
# Fastly Log Analysis Terraform Export

This directory contains the Terraform configuration to set up Fastly Object Storage logging and a CDN proxy for the Fastly Log Analysis tool.

Configuration is emitted as Terraform's JSON syntax (`.tf.json`). All Terraform commands (`init`, `fmt`, `validate`, `plan`, `apply`) accept it interchangeably with HCL.

## Files
- `versions.tf.json`: Terraform CLI floor + pinned major versions for the `aws` and `fastly` providers.
- `fos.tf.json`: The Fastly Object Storage bucket resource (created via the AWS S3-compatible provider).
- `cdn_proxy.tf.json`: A NEW Fastly Delivery service that fronts the FOS bucket for fast dashboard access.
- `logging_service.tf.json`: The logging endpoint and capture snippets for your existing service.
- `cdn_proxy.vcl`: The main VCL for the CDN proxy service (loaded by `cdn_proxy.tf.json`).
- `log_format.vcl`: The JSON log format string (loaded by `logging_service.tf.json`).
- `cdn_snippets/`: VCL snippets for the CDN proxy service (loaded by `cdn_proxy.tf.json`).
- `capture_snippets/`: VCL snippets for your existing logging service (loaded by `logging_service.tf.json`).

## AWS provider configuration

`fos.tf.json` declares an `aws_s3_bucket` resource. Fastly Object Storage is S3-compatible, so the AWS provider works against the Fastly FOS endpoint. Configure it in your root module (or alongside these files) like this:

```hcl
provider "aws" {{
  region                      = "{region}"
  access_key                  = "<your fos access key>"
  secret_key                  = "<your fos secret key>"
  endpoints {{ s3 = "https://{fos_host}" }}
  skip_credentials_validation = true
  skip_region_validation      = true
  skip_requesting_account_id  = true
}}
```

## Instructions
1. Review `fos.tf.json` and ensure the `aws` provider above is correctly configured.
2. `cdn_proxy.tf.json` creates a NEW Fastly service to accelerate your log reads.
3. `logging_service.tf.json` contains the configuration for your ACTIVE service. You should copy the `logging_s3`, `condition`, and `snippet` blocks into your existing `fastly_service_vcl` resource for service ID `{service_id}`.
4. Run `terraform init` and `terraform apply` to deploy the changes.
"""

    return files
