"""Terraform generation for Fastly Object Storage log analysis."""

from typing import Any

from backend.core.fastly.utils import load_vcl
from backend.provision import generate_capture_vcl, load_log_format
from backend.provision.fastly_api import _CDN_SNIPPETS


def _hcl_escape(value: object) -> str:
    """Escape ``value`` for safe inclusion *inside* an HCL string literal.

    Returns the escaped contents *without* surrounding quotes — call sites
    already supply the quotes (``"{x}"``). HCL string literals follow JSON
    escaping rules for ``\\`` and ``"``; ``${`` must be escaped as ``$${`` so
    user input can't be interpolated as an HCL template expression.

    Without this, every f-string splice in ``generate_terraform`` is a
    classic injection target: a bucket name like ``b"; rm -rf /; #`` closes
    the HCL string and pivots into arbitrary HCL, breaking ``terraform
    apply`` (and worse: in tools that exec the generated HCL, allowing
    arbitrary resource declarations).
    """
    s = "" if value is None else str(value)
    return (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("${", "$${")
        .replace("%{", "%%{")
    )


def generate_terraform(cfg: dict[str, Any], fos_access_key: str, fos_secret_key: str) -> dict[str, str]:
    """Generate Terraform HCL for the given provisioning configuration."""
    # Escape every user-supplied string used inside HCL string literals.
    # The raw values are kept around for non-HCL contexts (e.g. comments,
    # path construction, derived domain names) where they're safe.
    # 023: service_id ends up inside HCL comments verbatim. A newline or
    # carriage return would terminate the comment early and let attacker-
    # supplied text inject arbitrary HCL. Strip both before any use.
    service_id = str(cfg.get("logging_service_id", "YOUR_SERVICE_ID")).replace("\r", "").replace("\n", "")
    endpoint_name = cfg.get("endpoint_name", "fastly_log_analysis")
    region = cfg.get("fos_region", "us-east-1")
    bucket = cfg.get("fos_bucket_name", "your-bucket-name")
    prefix = cfg.get("fos_prefix", "").strip("/")
    # 022: log_period flows into the HCL ``period = {period}`` numeric
    # literal. An attacker who sets ``log_period = "1; resource ..."``
    # would otherwise break out of the literal and inject HCL. Cast to
    # int (with a safe fallback) so the rendered value is always a
    # numeric token regardless of what was on the wire.
    try:
        period = int(cfg.get("log_period", 3600))
    except (TypeError, ValueError):
        period = 3600
    cdn_service_name = cfg.get("cdn_service_name", "Fastly Log Analysis CDN Proxy")
    cdn_prefix = cfg.get("cdn_prefix", bucket)
    cdn_domain = f"{cdn_prefix}.global.ssl.fastly.net"
    cdn_shield = cfg.get("cdn_shield", "iad-va-us")
    cdn_secret = cfg.get("cdn_secret", "replace-with-secure-secret")

    sample_rate = int(cfg.get("sample_rate", 100))
    edge_only = bool(cfg.get("edge_only", False))
    custom_condition = cfg.get("custom_condition", "").strip()

    # HCL-escaped versions for splicing into "..." literals. Numeric and
    # bool fields are safe to render directly. region/prefix derive into
    # paths and other non-HCL contexts, so we keep both raw + escaped.
    bucket_h = _hcl_escape(bucket)
    endpoint_name_h = _hcl_escape(endpoint_name)
    cdn_service_name_h = _hcl_escape(cdn_service_name)
    cdn_domain_h = _hcl_escape(cdn_domain)
    cdn_shield_h = _hcl_escape(cdn_shield)
    cdn_secret_h = _hcl_escape(cdn_secret)
    fos_access_key_h = _hcl_escape(fos_access_key)
    fos_secret_key_h = _hcl_escape(fos_secret_key)

    log_format = load_log_format(cfg.get("log_fields"))
    vcl_snippets = generate_capture_vcl(cfg.get("log_fields"))

    # Check if rate limiting should be enabled in the exported VCL
    # We default to true but try to match what the user might have seen
    cdn_vcl = load_vcl(rate_limiting=True)

    path = f"/{prefix}/raw/%Y-%m-%d/%H/" if prefix else "/raw/%Y-%m-%d/%H/"

    cond_parts = ["!segmented_caching.is_inner_req"]
    if edge_only:
        cond_parts.append("(req.restarts == 0 && fastly.ff.visits_this_service == 0)")
    if sample_rate < 100:
        cond_parts.append(f"randombool({sample_rate}, 100)")
    if custom_condition:
        cond_parts.append(f"({custom_condition})")
    cond_stmt = " && ".join(cond_parts)
    # cond_stmt mixes constants we control with user-supplied custom_condition;
    # the whole thing flows into an HCL `statement = "..."` literal and so
    # must be escaped — quotes inside a VCL expression like `req.url ~ "x"`
    # would otherwise close the HCL string.
    cond_stmt_h = _hcl_escape(cond_stmt)

    fos_host = f"{region}.object.fastlystorage.app"
    fos_host_h = _hcl_escape(fos_host)
    shield_line = (
        f'    shield                = "{cdn_shield_h}"\n' if cdn_shield and cdn_shield.lower() != "none" else ""
    )

    files = {}

    # 1. Store the main CDN VCL and log format in their own files
    files["cdn_proxy.vcl"] = cdn_vcl
    files["log_format.vcl"] = log_format

    # 1b. Pin provider versions so an upstream major-version bump (Fastly v6,
    # AWS v7) doesn't silently break customer `terraform apply`. Use the
    # pessimistic operator `~> X.0` so minor/patch upgrades are still
    # allowed; only majors are gated. If you bump these, also update
    # tests/utils/test_terraform_gen.py's `_versions.tf` scaffold so
    # `terraform validate` runs against the same constraint.
    files["versions.tf"] = """\
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.0" }
    fastly = { source = "fastly/fastly", version = "~> 5.0" }
  }
}
"""

    # 2. FOS Bucket configuration
    fos_hcl = f"""\
# ==============================================================================
# FASTLY OBJECT STORAGE BUCKET
# Note: You need the AWS provider configured for the Fastly FOS endpoint.
# provider "aws" {{
#   region     = "{region}"
#   access_key = "{fos_access_key}"
#   secret_key = "{fos_secret_key}"
#   endpoints {{ s3 = "https://{fos_host}" }}
#   skip_credentials_validation = true
#   skip_region_validation      = true
#   skip_requesting_account_id  = true
# }}
# ==============================================================================

resource "aws_s3_bucket" "fos_bucket" {{
  bucket = "{bucket_h}"
}}
"""
    files["fos.tf"] = fos_hcl

    # 3. CDN Proxy Service configuration
    cdn_hcl = []
    cdn_hcl.append(f"""\
# ==============================================================================
# CDN PROXY SERVICE
# This service fronts the FOS bucket for secure, fast dashboard access.
# ==============================================================================

resource "fastly_service_vcl" "cdn_proxy" {{
  name = "{cdn_service_name_h}"

  domain {{
    name = "{cdn_domain_h}"
  }}

  backend {{
    name                  = "fos_origin"
    address               = "{fos_host_h}"
    port                  = 443
    use_ssl               = true
    ssl_cert_hostname     = "{fos_host_h}"
    ssl_sni_hostname      = "{fos_host_h}"
    connect_timeout       = 5000
    first_byte_timeout    = 60000
    between_bytes_timeout = 30000
{shield_line}  }}

  vcl {{
    name    = "main"
    content = file("${{path.module}}/cdn_proxy.vcl")
    main    = true
  }}

  dictionary {{
    name       = "fos_credentials"
    write_only = true
  }}

  dictionary {{
    name       = "cdn_auth"
    write_only = true
  }}
""")

    for name, type_, content, priority in _CDN_SNIPPETS:
        snip_filename = f"cdn_snippets/{name.replace('-', '_')}.vcl"
        files[snip_filename] = content
        cdn_hcl.append(f"""
  snippet {{
    name     = "{name}"
    type     = "{type_}"
    priority = {priority}
    content  = file("${{path.module}}/{snip_filename}")
  }}""")

    cdn_hcl.append(f"""
}}

resource "fastly_service_dictionary_items" "fos_credentials" {{
  service_id    = fastly_service_vcl.cdn_proxy.id
  dictionary_id = {{ for d in fastly_service_vcl.cdn_proxy.dictionary : d.name => d.dictionary_id }}["fos_credentials"]
  items = {{
    access_key = "{fos_access_key_h}"
    secret_key = "{fos_secret_key_h}"
    bucket     = aws_s3_bucket.fos_bucket.bucket
    region     = "{_hcl_escape(region)}"
  }}
}}

resource "fastly_service_dictionary_items" "cdn_auth" {{
  service_id    = fastly_service_vcl.cdn_proxy.id
  dictionary_id = {{ for d in fastly_service_vcl.cdn_proxy.dictionary : d.name => d.dictionary_id }}["cdn_auth"]
  items = {{
    secret = "{cdn_secret_h}"
  }}
}}
""")
    files["cdn_proxy.tf"] = "".join(cdn_hcl)

    # 4. Logging configuration for existing service.
    # Top-level blocks are emitted at column 0 so the file passes
    # `terraform fmt -check` — leading indentation in earlier versions broke
    # validation in module consumers' Terraform tooling.
    log_hcl = []
    log_hcl.append(f"""\
# ==============================================================================
# LOGGING CONFIGURATION FOR YOUR EXISTING SERVICE (ID: {service_id})
# Note: You should merge these resources into your existing Terraform or
# use a `fastly_service_vcl` resource block if starting from scratch.
# ==============================================================================

# --- LOGGING ENDPOINT ---

resource "fastly_service_vcl" "logging_service" {{
  name = "{_hcl_escape(cfg.get("service_name", "Logging Service"))}"

  domain {{
    name = "example.com" # Placeholder, update to your actual domain
  }}

  condition {{
    name      = "Log Sampling - {endpoint_name_h}"
    statement = "{cond_stmt_h}"
    type      = "RESPONSE"
  }}

  logging_s3 {{
    name               = "{endpoint_name_h}"
    bucket_name        = aws_s3_bucket.fos_bucket.bucket
    domain             = "{fos_host_h}"
    path               = "{_hcl_escape(path)}"
    period             = {period}
    gzip_level         = 9
    message_type       = "blank"
    timestamp_format   = "%Y-%m-%dT%H:%M:%S.000"
    response_condition = "Log Sampling - {endpoint_name_h}"
    format_version     = 2
    format             = file("${{path.module}}/log_format.vcl")

    s3_access_key = "{fos_access_key_h}"
    s3_secret_key = "{fos_secret_key_h}"
  }}
""")

    snippets_map = {
        "recv": "Fastly Log Analysis Capture",
        "miss": "Fastly Log Analysis Miss",
        "pass": "Fastly Log Analysis Pass",
        "fetch": "Fastly Log Analysis Origin Fetch",
        "error": "Fastly Log Analysis Origin Error",
        "deliver": "Fastly Log Analysis Origin Deliver",
    }

    for phase, snip_vcl in vcl_snippets.items():
        snip_name = snippets_map.get(phase)
        if snip_name:
            snip_filename = f"capture_snippets/{phase}.vcl"
            files[snip_filename] = snip_vcl
            log_hcl.append(f"""
  # --- {snip_name} ---
  snippet {{
    name     = "{snip_name}"
    type     = "{phase}"
    priority = {1 if phase == "recv" else 100}
    content  = file("${{path.module}}/{snip_filename}")
  }}""")

    log_hcl.append("\n}\n")
    files["logging_service.tf"] = "".join(log_hcl)

    files["instructions"] = f"""\
# Fastly Log Analysis Terraform Export

This directory contains the Terraform configuration to set up Fastly Object Storage logging and a CDN proxy for the Fastly Log Analysis tool.

## Files
- `fos.tf`: The Fastly Object Storage bucket resource.
- `cdn_proxy.tf`: The Fastly Delivery service that fronts the bucket.
- `logging_service.tf`: The logging endpoint and capture snippets for your existing service.
- `cdn_proxy.vcl`: The main VCL for the CDN proxy service.
- `log_format.vcl`: The JSON log format string.
- `cdn_snippets/`: VCL snippets for the CDN proxy service.
- `capture_snippets/`: VCL snippets for your logging service.

## Instructions
1. Review `fos.tf` and ensure the `aws` provider is correctly configured.
2. `cdn_proxy.tf` creates a NEW Fastly service to accelerate your log reads.
3. `logging_service.tf` contains the configuration for your ACTIVE service. You should copy the `logging_s3`, `condition`, and `snippet` blocks into your existing `fastly_service_vcl` resource for service ID `{service_id}`.
4. Run `terraform init` and `terraform apply` to deploy the changes.
"""

    return files
