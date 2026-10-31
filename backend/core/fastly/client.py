from __future__ import annotations

import json
import logging
import re
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator
from typing import Annotated, Any, Literal

import tenacity
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator

from backend.utils.runtime_modes import mock_mode_enabled

logger = logging.getLogger(__name__)


# ── Fastly constants, SigV4 VCL & CDN VCL helpers (folded from utils.py) ─────

FASTLY_LOG_FIELDS = ("log", "log_records", "log_entries", "logging_requests")

SHIELD_MAP = {
    "us-east-1": "iad-va-us",  # Ashburn, VA
    "us-west": "bfi-wa-us",  # Seattle, WA (BFI)
    "us-central-1": "chi-il-us",  # Chicago, IL (CHI)
    "eu-central-1": "frankfurt-de",  # Frankfurt, Germany
    "eu-south-1": "mxp-milan-it",  # Milan, Italy
    "eu-west-1": "mxp-paris-fr",  # Paris, France
    "uk-east-1": "london-uk",  # London, UK
    "jp-central-1": "nrt-tokyo-jp",  # Tokyo, Japan (NRT)
    "au-east-1": "sydney-au",  # Sydney, Australia
}


def region_endpoint(region: str) -> str:
    return f"{region}.object.fastlystorage.app"


def parse_period(s: str) -> int:
    """Parse '60', '1 minute', '5m', '5 minutes' → integer seconds."""
    s = s.strip().lower()
    m = re.fullmatch(r"(\d+)\s*(m(?:in(?:utes?)?)?)?", s)
    if m:
        val = int(m.group(1))
        return val * 60 if m.group(2) else val
    raise ValueError(f"Cannot parse period: {s!r}  (try '60' or '5 minutes')")


def render_vcl_sigv4_signing_block(
    *,
    condition: str,
    outer_indent: str = "",
    pre_bucket_lines: tuple[str, ...] = (),
    unset_client_ip: bool = False,
    header_comment: str = "# FOS SigV4 signing",
    detailed_comments: bool = False,
) -> str:
    """Render an AWS SigV4 signing VCL ``if (...) { ... }`` block for Fastly Object Storage."""
    body_indent = outer_indent + "    "
    extra_pre_bucket = ""
    if pre_bucket_lines:
        extra_pre_bucket = "\n\n" + "\n".join(f"{body_indent}{line}" if line else "" for line in pre_bucket_lines)
    client_ip_line = f"\n{body_indent}unset bereq.http.Fastly-Client-IP;" if unset_client_ip else ""
    date_comment_lines = (
        [
            f"{body_indent}# Round x-amz-date down to the minute (finding 006). The SigV4",
            f"{body_indent}# signature is computed over this header — making it per-request",
            f"{body_indent}# invalidates Fastly's per-(URL, method) collapsed-forwarding /",
            f"{body_indent}# request coalescing: every concurrent request gets its own unique",
            f"{body_indent}# signed Authorization, so they all forward to FOS in parallel",
            f"{body_indent}# instead of riding on a single in-flight fetch. Pinning the seconds",
            f"{body_indent}# to ``00`` collapses all requests within a minute to the SAME",
            f"{body_indent}# signature, restoring coalescing and capping the per-minute origin",
            f"{body_indent}# spend regardless of incoming RPS. Still well inside AWS SigV4's",
            f"{body_indent}# 15-minute validity window. (Audit limited VCL arithmetic — no %",
            f"{body_indent}# or / operator — so per-minute granularity via strftime format",
            f"{body_indent}# is the cleanest expression of the rounding.)",
        ]
        if detailed_comments
        else []
    )
    bucket_comment = (
        "# Only prepend bucket if not already present (handles direct S3-style paths from DuckDB/Boto3)"
        if detailed_comments
        else "# Prepend bucket if not already present"
    )
    bucket_inner_indent = "     " if detailed_comments else "    "
    qs_lines = (
        [
            f"{body_indent}# Keep query parameters for S3 API calls (ListObjectsV2, etc)",
            f"{body_indent}# BUT remove our auth 'key' parameter so it doesn't leak to FOS or mess with signing",
            f'{body_indent}set bereq.url = querystring.filter(bereq.url, "key");',
            "",
            f"{body_indent}# Sort for canonical request",
            f"{body_indent}set var.canonicalQuery = querystring.sort(bereq.url.qs);",
        ]
        if detailed_comments
        else [
            f"{body_indent}# Remove auth params and sort query string",
            f'{body_indent}set bereq.url = querystring.filter(bereq.url, "key");',
            f"{body_indent}set var.canonicalQuery = querystring.sort(bereq.url.qs);",
        ]
    )
    norm_comment = (
        "# Normalize path for signing (S3 expects encoded path)"
        if detailed_comments
        else "# Normalize path for signing"
    )
    lines = [
        f"{outer_indent}{header_comment}",
        f"{outer_indent}if ({condition}) {{",
        f"{body_indent}declare local var.fosAccessKey STRING;",
        f"{body_indent}declare local var.fosSecretKey STRING;",
        f"{body_indent}declare local var.fosBucket STRING;",
        f"{body_indent}declare local var.fosRegion STRING;",
        f"{body_indent}declare local var.fosHost STRING;",
        f"{body_indent}declare local var.canonicalHeaders STRING;",
        f"{body_indent}declare local var.signedHeaders STRING;",
        f"{body_indent}declare local var.canonicalRequest STRING;",
        f"{body_indent}declare local var.canonicalQuery STRING;",
        f"{body_indent}declare local var.stringToSign STRING;",
        f"{body_indent}declare local var.dateStamp STRING;",
        f"{body_indent}declare local var.signature STRING;",
        f"{body_indent}declare local var.scope STRING;",
        "",
        f'{body_indent}set var.fosAccessKey = table.lookup(fos_credentials, "access_key", "missing");',
        f'{body_indent}set var.fosSecretKey = table.lookup(fos_credentials, "secret_key", "missing");',
        f'{body_indent}set var.fosBucket = table.lookup(fos_credentials, "bucket", "missing");',
        f'{body_indent}set var.fosRegion = table.lookup(fos_credentials, "region", "missing");',
        f'{body_indent}set var.fosHost = var.fosRegion ".object.fastlystorage.app";',
        "",
        f'{body_indent}set bereq.http.x-amz-content-sha256 = digest.hash_sha256("");',
        *date_comment_lines,
        f'{body_indent}set bereq.http.x-amz-date = strftime({{"%Y%m%dT%H%M00Z"}}, now);',
        f"{body_indent}set bereq.http.host = var.fosHost;{extra_pre_bucket}",
        "",
        f"{body_indent}{bucket_comment}",
        f'{body_indent}if (regsub(bereq.url.path, "^/([^/]+)/.*$", "\\1") != var.fosBucket) {{',
        f'{body_indent}{bucket_inner_indent}set bereq.url = "/" var.fosBucket bereq.url;',
        f"{body_indent}}}",
        "",
        *qs_lines,
        "",
        f"{body_indent}{norm_comment}",
        f'{body_indent}set bereq.url = regsuball(urlencode(urldecode(bereq.url.path)), {{"%2F"}}, "/") + if(bereq.url.qs != "", "?" + bereq.url.qs, "");',
        "",
        f'{body_indent}set var.dateStamp = strftime({{"%Y%m%d"}}, now);',
        "",
        f'{body_indent}set var.canonicalHeaders = ""',
        f'{body_indent}    "host:" bereq.http.host LF',
        f'{body_indent}    "x-amz-content-sha256:" bereq.http.x-amz-content-sha256 LF',
        f'{body_indent}    "x-amz-date:" bereq.http.x-amz-date LF',
        f"{body_indent};",
        f'{body_indent}set var.signedHeaders = "host;x-amz-content-sha256;x-amz-date";',
        f'{body_indent}set var.canonicalRequest = ""',
        f"{body_indent}    req.method LF",
        f"{body_indent}    bereq.url.path LF",
        f"{body_indent}    var.canonicalQuery LF",
        f"{body_indent}    var.canonicalHeaders LF",
        f"{body_indent}    var.signedHeaders LF",
        f'{body_indent}    digest.hash_sha256("")',
        f"{body_indent};",
        "",
        f'{body_indent}set var.scope = var.dateStamp "/" var.fosRegion "/s3/aws4_request";',
        "",
        f'{body_indent}set var.stringToSign = ""',
        f'{body_indent}    "AWS4-HMAC-SHA256" LF',
        f"{body_indent}    bereq.http.x-amz-date LF",
        f"{body_indent}    var.scope LF",
        f'{body_indent}    regsub(digest.hash_sha256(var.canonicalRequest),"^0x", "")',
        f"{body_indent};",
        "",
        f"{body_indent}set var.signature = digest.awsv4_hmac(",
        f"{body_indent}    var.fosSecretKey,",
        f"{body_indent}    var.dateStamp,",
        f"{body_indent}    var.fosRegion,",
        f'{body_indent}    "s3",',
        f"{body_indent}    var.stringToSign",
        f"{body_indent});",
        "",
        f'{body_indent}set bereq.http.Authorization = "AWS4-HMAC-SHA256 "',
        f'{body_indent}    "Credential=" var.fosAccessKey "/" var.scope ", "',
        f'{body_indent}    "SignedHeaders=" var.signedHeaders ", "',
        f'{body_indent}    "Signature=" + regsub(var.signature,"^0x", "")',
        f"{body_indent};",
        f"{body_indent}unset bereq.http.Accept;",
        f"{body_indent}unset bereq.http.Accept-Language;",
        f"{body_indent}unset bereq.http.User-Agent;{client_ip_line}",
        f"{outer_indent}}}",
    ]
    return "\n".join(lines)


def load_vcl(rate_limiting: bool = True) -> str:
    sigv4_block = render_vcl_sigv4_signing_block(
        condition='(req.method == "GET" || req.method == "HEAD") && !req.backend.is_shield',
        outer_indent="    ",
        unset_client_ip=True,
        header_comment="# Fastly Object Storage signing https://www.fastly.com/documentation/guides/integrations/non-fastly-services/amazon-s3/",
        detailed_comments=True,
    )
    vcl = (
        """#RATELIMIT_BEGIN
ratecounter auth_fail_rc {}
penaltybox auth_fail_pb {}
#RATELIMIT_END

sub miss_pass {
"""
        + sigv4_block
        + """
}

sub vcl_recv {
  # Authenticate and take authority over the client IP exactly once, on
  # the first Fastly POP to handle the request (the edge).
  # ``fastly.ff.visits_this_service`` is 0 only on that first touch; any
  # request arriving with it > 0 has already transited — and been
  # authenticated by — our own edge, so re-running the gate here would
  # only 401 a legitimately-forwarded request (the ``key`` param is
  # stripped before forwarding). Unauthenticated requests ``error 401``
  # at the edge and are never forwarded to the shield. A client cannot
  # forge ``visits_this_service`` > 0: each ``Fastly-FF`` entry is a
  # salted hash that only genuine Fastly hops can produce.
  if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {
    # A client may supply Fastly-Client-IP; overwrite it with the real
    # connection IP so downstream logging / rate-limiting can't be spoofed.
    set req.http.Fastly-Client-IP = client.ip;

    # Block requests that do not provide the correct secret key.
    # NOTE on the auth fallback: the third argument to ``table.lookup`` is
    # returned when ``cdn_auth.secret`` is absent from the edge dictionary.
    # Defaulting to ``""`` is fail-open — an attacker who sends an empty
    # ``key`` query param trivially matches. The literal fallback string
    # below is substituted in load_vcl() with an unguessable
    # ``unprovisioned-fallback-`` + ``secrets.token_hex(32)`` value, which
    # is never knowable to an attacker and therefore fails closed when the
    # dictionary is unprovisioned. The human-readable prefix makes clear in
    # the rendered VCL that this is a throwaway placeholder, NOT the real
    # ``cdn_auth.secret`` (which lives only in the write-only edge dict).
    if (subfield(req.url.qs, "key", "&") != table.lookup(cdn_auth, "secret", "REPLACE_AT_LOAD_VCL_FALLBACK_SECRET") && req.http.x-fastly-key != table.lookup(cdn_auth, "secret", "REPLACE_AT_LOAD_VCL_FALLBACK_SECRET")) {
#RATELIMIT_BEGIN
      declare local var.last_minute INTEGER;
      set var.last_minute = ratelimit.ratecounter_increment(auth_fail_rc, req.http.Fastly-Client-IP, 1);
      if (var.last_minute >= 2) {
        ratelimit.penaltybox_add(auth_fail_pb, req.http.Fastly-Client-IP, 1m);
      }
#RATELIMIT_END
      error 401 "Unauthorized";
    }
#RATELIMIT_BEGIN
    if (req.method != "FASTLYPURGE" && ratelimit.penaltybox_has(auth_fail_pb, req.http.Fastly-Client-IP)) {
      error 401 "Unauthorized";
    }
#RATELIMIT_END
  }

  # Enable segmented caching for potentially large log or parquet files
  set req.enable_segmented_caching = true;
  set segmented_caching.block_size = 20971520; # 20 MB, the maximum

  # Cache-key hardening (post-auth — auth check above still reads the
  # `key` qs param from the original req.url):
  #   1. querystring.filter_except keeps ONLY the S3-API parameters the
  #      FOS origin actually understands and strips everything else
  #      (including our auth `key` secret, any caller-injected tracking
  #      params, marketing UTM params, session IDs, etc.). Unexpected
  #      params no longer fracture the cache or leak into req.hash.
  #   2. querystring.sort canonicalises the remaining param order so
  #      `?prefix=foo&max-keys=10` and `?max-keys=10&prefix=foo` resolve
  #      to one cache entry instead of two.
  # Allow-list rationale (S3 API surface FOS exposes):
  #   - List objects v2: list-type, prefix, delimiter, continuation-token,
  #                       start-after, max-keys, encoding-type, fetch-owner
  #   - List objects v1: marker
  #   - Get object:      versionId, partNumber, response-content-type,
  #                       response-content-disposition, response-cache-control
  # Anything else is silently dropped. If a legitimate S3 param needs to
  # pass through later, add it to this list and re-deploy.
  set req.url = querystring.filter_except(req.url, "list-type,prefix,delimiter,continuation-token,start-after,max-keys,encoding-type,fetch-owner,marker,versionId,partNumber,response-content-type,response-content-disposition,response-cache-control");
  set req.url = querystring.sort(req.url);

  # Never cache admin_state.json — it changes on every mutation
  if (req.url ~ "/meta/admin_state\\.json$") {
    return(pass);
  }

  # Race condition handling: disable SWR on shield nodes so only edge serves stale
  if (fastly.ff.visits_this_service > 1) {
    set req.max_stale_while_revalidate = 0s;
  }

#FASTLY recv
  # Normally, you should consider requests other than GET and HEAD to be uncacheable
  # (to this we add the special FASTLYPURGE method)
  if (req.method != "HEAD" && req.method != "GET" && req.method != "FASTLYPURGE") {
    return(pass);
  }
  # If you are using image optimization, insert the code to enable it here
  # See https://www.fastly.com/documentation/reference/io/ for more information.
  return(lookup);
}
sub vcl_hash {
  # Security: hash on the full URL (path + query string), not just
  # req.url.path. Before this fix, two requests that differed only in
  # query parameters (e.g. ListObjectsV2 with different ?prefix= values,
  # or ?versionId= variants) shared a single cache entry — the second
  # caller would receive the first caller's object listing. The CDN
  # auth `key` querystring has already been stripped from req.url by
  # the querystring.filter_except in vcl_recv, AND remaining params are
  # sorted by querystring.sort, so the cache key (a) does NOT include
  # the secret and (b) is normalised across param-order variants.
  # Expect a one-time cache-hit-rate dip + origin egress spike on
  # rollout while prior entries are stranded; the canary monitors
  # those signals and auto-rolls back if they exceed v6 §6 thresholds.
  set req.hash += req.url;
  set req.hash += req.http.host;
#FASTLY hash
  return(hash);
}
sub vcl_hit {
#FASTLY hit
  return(deliver);
}
sub vcl_miss {
#FASTLY miss
    call miss_pass;
  return(fetch);
}
sub vcl_pass {
#FASTLY pass
    call miss_pass;
  return(pass);
}
sub vcl_fetch {
#FASTLY fetch

# Unset Fastly Object Storage headers https://www.fastly.com/documentation/guides/integrations/non-fastly-services/amazon-s3/
unset beresp.http.x-amz-id-2;
unset beresp.http.x-amz-request-id;
unset beresp.http.x-amz-delete-marker;
unset beresp.http.x-amz-version-id;

  # Unset headers that reduce cacheability for images processed using the Fastly image optimizer
  if (req.http.X-Fastly-Imageopto-Api) {
    unset beresp.http.Set-Cookie;
    unset beresp.http.Vary;
  }
  # Log the number of restarts for debugging purposes
  if (req.restarts > 0) {
    set beresp.http.Fastly-Restarts = req.restarts;
  }
  # If the response is setting a cookie, make sure it is not cached
  if (beresp.http.Set-Cookie) {
    return(pass);
  }
  # By default we set a TTL based on the `Cache-Control` header but we don't parse additional directives
  # like `private` and `no-store`. Private in particular should be respected at the edge:
  if (beresp.http.Cache-Control ~ "(?:private|no-store)") {
    return(pass);
  }
  # If no TTL has been provided in the response headers, set a default
  if (!beresp.http.Expires && !beresp.http.Surrogate-Control ~ "max-age" && !beresp.http.Cache-Control ~ "(?:s-maxage|max-age)") {
    set beresp.ttl = 3600s;
    # Apply a longer default TTL for images processed using Image Optimizer
    if (req.http.X-Fastly-Imageopto-Api) {
      set beresp.ttl = 2592000s; # 30 days
      set beresp.http.Cache-Control = "max-age=2592000, public";
    }
  }
  return(deliver);
}
sub vcl_error {
#FASTLY error
  return(deliver);
}
sub vcl_deliver {
#FASTLY deliver

  # Remove AWS headers returned from Fastly Object Storage https://www.fastly.com/documentation/solutions/examples/using-s3-compatible-buckets-as-private-origins/
  unset resp.http.x-amz-id-2;
  unset resp.http.x-amz-request-id;
  unset resp.http.server;

  return(deliver);
}
sub vcl_log {
#FASTLY log
}"""
    )
    if not rate_limiting:
        vcl = re.sub(r"\s*#RATELIMIT_BEGIN.*?#RATELIMIT_END", "", vcl, flags=re.DOTALL)
    vcl = vcl.replace("REPLACE_AT_LOAD_VCL_FALLBACK_SECRET", "unprovisioned-fallback-" + secrets.token_hex(32))
    return vcl


# ── RUM VCL snippet generation and provisioning helpers (folded from rum_provisioning.py) ──

RUM_RECV_NAME = "RUM - Recv"
RUM_DELIVER_SET_COOKIE_NAME = "RUM - Set cookies"
RUM_ASSET_FETCH_NAME = "RUM - Asset fetch FOS"
RUM_SIGV4_SIGN_NAME = "RUM - Asset fetch SigV4 signing"
RUM_FARO_FETCH_NAME = "RUM - Faro SDK fetch caching"
RUM_SNIPPET_NAMES = frozenset(
    {RUM_RECV_NAME, RUM_DELIVER_SET_COOKIE_NAME, RUM_ASSET_FETCH_NAME, RUM_SIGV4_SIGN_NAME, RUM_FARO_FETCH_NAME}
)

_FARO_VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
_RUM_PATH_PREDICATE = re.compile(r"\breq\.url(\.path)?\b")


def validate_rum_custom_condition(condition: str) -> str | None:
    """Return an error message if ``condition`` can't work, else None."""
    cond = (condition or "").strip()
    if not cond:
        return None
    if _RUM_PATH_PREDICATE.search(cond):
        return (
            "A condition on req.url / req.url.path cannot work here: it is AND-ed onto "
            'req.url.path == "/rum-beacon", so the path is always "/rum-beacon". Such a '
            "condition is either a no-op or silently disables all RUM logging. To exclude "
            "beacons by page URL, filter in the tracker — the page URL is in the beacon's "
            "POST body, which VCL cannot read."
        )
    if "'" in cond:
        return "VCL string literals use double quotes, not single quotes."
    return None


def _assert_faro_version_safe(version: str) -> str:
    """Raise ``ValueError`` unless ``version`` is a plain ``X.Y.Z`` string."""
    if not isinstance(version, str) or not _FARO_VERSION_RE.fullmatch(version):
        raise ValueError(f"faro_version must be a plain X.Y.Z version string; got {version!r}")
    return version


def generate_rum_vcl(logging_service_id: str, faro_version: str | None = None) -> dict[str, str]:
    """Generate Phase 1 RUM VCL snippets for the logging service."""
    if faro_version is not None:
        _assert_faro_version_safe(faro_version)
    return {
        RUM_RECV_NAME: _generate_recv_vcl(),
        RUM_DELIVER_SET_COOKIE_NAME: _generate_deliver_set_cookie_vcl(logging_service_id),
    }


def generate_rum_recv_parts() -> tuple[str, str]:
    """Return the beacon field extraction and response blocks separately."""
    return _generate_beacon_field_extraction_vcl(), _generate_beacon_response_vcl()


def _generate_recv_vcl() -> str:
    """Recv stage: handle RUM beacons."""
    return (
        """# Edge-only (first hop, no restarts) block for RUM
if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {
    # Handle RUM beacon POST to /rum-beacon
    if (req.url.path == "/rum-beacon") {
"""
        + _generate_beacon_field_extraction_vcl()
        + """
"""
        + _generate_beacon_response_vcl()
        + """
    }
}"""
    )


def _generate_beacon_field_extraction_vcl() -> str:
    """Populate RUM headers before the generic capture block runs."""
    return """# Extract RUM fields before generic log capture promotes them.
set req.http.x-fos-edge-data:rum_cid = querystring.get(req.url, "cid");
set req.http.x-fos-edge-data:fastly_req_id = querystring.get(req.url, "req");
if (req.http.x-fos-edge-data:fastly_req_id == "") {
    set req.http.x-fos-edge-data:fastly_req_id = req.http.Fastly-Request-ID;
}
set req.http.x-fos-edge-data:rum_raw_query = req.url;
set req.http.x-fos-edge-data:rum_metric_name = querystring.get(req.url, "rum_metric_name");
set req.http.x-fos-edge-data:rum_metric_value = querystring.get(req.url, "rum_metric_value");
set req.http.x-fos-edge-data:rum_metric_rating = querystring.get(req.url, "rum_metric_rating");
set req.http.x-fos-edge-data:rum_pathname = querystring.get(req.url, "rum_pathname");
set req.http.x-fos-edge-data:rum_error_message = querystring.get(req.url, "rum_error_message");
set req.http.x-fos-edge-data:rum_error_file = querystring.get(req.url, "rum_error_file");
set req.http.x-fos-edge-data:rum_body = req.body;"""


def _generate_beacon_response_vcl() -> str:
    """Mark and terminate a RUM beacon after its fields were captured."""
    return """# Mark beacon to skip S3 logging (already logged separately to metadata DB)
set req.http.x-skip-rum-logging = "1";

# Synthetic 204 response (no origin round-trip needed)
error 611 "No Content";"""


def _generate_asset_fetch_vcl(shield_pop: str = "iad-va-us", faro_version: str | None = None) -> str:
    """Fetch /js/rum.js (and optionally /js/faro-sdk.js) from FOS with SigV4 signing."""
    if shield_pop and shield_pop.lower() != "none":
        shield_var = f"ssl_shield_{shield_pop.replace('-', '_')}"
        backend_str = f"fastly.try_select_shield({shield_var}, F_fos_origin)"
    else:
        backend_str = "F_fos_origin"

    if faro_version is None:
        path_condition = 'req.url.path == "/js/rum.js"'
    else:
        _assert_faro_version_safe(faro_version)
        path_condition = '(req.url.path == "/js/rum.js" || req.url.path == "/js/faro-sdk.js")'

    return f"""# Fetch RUM tracker JS from FOS with SigV4 signing
if ({path_condition} && req.method == "GET") {{
    # Backend points to FOS endpoint (shared with logging)
    set req.backend = {backend_str};
    # Flag for SigV4 signing in miss/pass (req.backend.name not available there)
    set req.http.X-FOS-Request = "1";
    return(lookup);
}}"""


def _generate_sigv4_sign_vcl(faro_version: str | None = None) -> str:
    """SigV4 signing for FOS requests in miss/pass stages."""
    body = render_vcl_sigv4_signing_block(
        condition='req.http.X-FOS-Request == "1" && !req.backend.is_shield',
        pre_bucket_lines=(
            "# If the user-facing path is /js/rum.js, point to the actual FOS object path",
            'if (bereq.url.path == "/js/rum.js") {',
            '    set bereq.url = "/rum/rum-tracker.js";',
            "}",
        ),
    )

    if faro_version is None:
        return body

    _assert_faro_version_safe(faro_version)
    anchor = """    if (bereq.url.path == "/js/rum.js") {
        set bereq.url = "/rum/rum-tracker.js";
    }"""
    faro_rewrite = (
        anchor
        + f"""

    # If the user-facing path is /js/faro-sdk.js, point to the pinned Faro Web SDK object in FOS
    if (bereq.url.path == "/js/faro-sdk.js") {{
        set bereq.url = "/rum/faro-web-sdk-v{faro_version}.iife.js";
    }}"""
    )
    return body.replace(anchor, faro_rewrite, 1)


def _generate_faro_fetch_vcl() -> str:
    """Cache + purge policy for the self-hosted Faro Web SDK bundle (vcl_fetch)."""
    return """# Cache the self-hosted Faro Web SDK bundle at the edge; only a short
# browser TTL, since this stable path serves mutable (per-upgrade) content.
if (req.url.path == "/js/faro-sdk.js" && req.http.X-FOS-Request == "1") {
    if (beresp.status == 200) {
        set beresp.ttl = 604800s;
        set beresp.cacheable = true;
        set beresp.http.Surrogate-Key = "rum-faro-sdk rum-js";
        set beresp.http.Surrogate-Control = "max-age=604800";
        set beresp.http.Cache-Control = "public, max-age=300";
    } else {
        set beresp.ttl = 0s;
        set beresp.cacheable = false;
    }
}"""


def _generate_deliver_set_cookie_vcl(logging_service_id: str) -> str:
    """Set the rum_cid cookie in deliver stage (post-scoring if enabled)."""
    del logging_service_id
    return """# Set rum_cid cookie in deliver (after scoring, if enabled)
# rum_cid is derived from the edge session scorer's sid (if scoring is enabled).
# This snippet runs at deliver priority 101 (after the scoring deliver snippet),
# so it can read the sid that scoring set.
if (req.restarts == 1 && fastly.ff.visits_this_service == 0 && req.url.path != "/rum-beacon") {
    # Only set on the pass-2 deliver (after potential scorer restart), excluding beacons
    # Read rum_cid from the edge scorer's sid (set by scoring's deliver snippet)
    if (req.http.x-edge-score:sid) {
        set resp.http.Set-Cookie = "rum_cid=" + req.http.x-edge-score:sid + "; Path=/; SameSite=Lax";
    }
}"""


def _generate_vcl_error_handler() -> str:
    """Error handler for synthetic 611 (RUM beacon 204 response)."""
    return """# vcl_error handler for RUM beacon synthetic 204 response
if (obj.status == 611) {
    set obj.status = 204;
    set obj.response = "No Content";
    set obj.http.Cache-Control = "no-cache, no-store, must-revalidate";
    set obj.http.Access-Control-Allow-Origin = "*";
    set obj.http.Access-Control-Allow-Methods = "POST, OPTIONS";
    set obj.http.Access-Control-Allow-Headers = "Content-Type";
    synthetic "";
    return (deliver);
}"""


def generate_rum_asset_fetch_vcl(shield_pop: str = "iad-va-us", faro_version: str | None = None) -> dict[str, str]:
    """Generate Phase 3 RUM asset fetch VCL snippets."""
    if faro_version is not None:
        _assert_faro_version_safe(faro_version)

    snippets = {
        RUM_ASSET_FETCH_NAME: _generate_asset_fetch_vcl(shield_pop, faro_version),
        RUM_SIGV4_SIGN_NAME: _generate_sigv4_sign_vcl(faro_version),
    }
    if faro_version is not None:
        snippets[RUM_FARO_FETCH_NAME] = _generate_faro_fetch_vcl()
    return snippets


# ── Canned Fastly API responses for E2E (formerly backend.core.fastly.mock_fixtures) ──


def is_mock_mode() -> bool:
    """Single-source check the gated callers read on every request."""
    return mock_mode_enabled("FASTLY_MOCK_MODE") or mock_mode_enabled("FASTLY_API_MOCK")


def api_key_or_mock(key: str) -> str:
    """``key`` unchanged, or a placeholder in FASTLY_MOCK_MODE when it is empty."""
    return key or ("mock-api-key" if is_mock_mode() else key)


def _service_id_from_path(path: str) -> str:
    m = re.match(r"^/service/([^/]+)", path)
    return m.group(1) if m else "mock-svc-id"


def _version_from_path(path: str) -> int:
    m = re.search(r"/version/(\d+)", path)
    return int(m.group(1)) if m else 1


def mock_response(method: str, path: str, body: dict | None = None) -> Any:
    """Return a canned response for the given Fastly endpoint."""
    if path == "/tokens/self" and method == "GET":
        return {
            "id": "mock-token-id",
            "user_id": "mock-user",
            "scope": "global",
            "customer_id": "mock-customer",
            "services": [],
        }

    if path == "/service" and method == "POST":
        return {"id": "mock-svc-id", "name": (body or {}).get("name", "mock"), "version": 1}

    if path == "/service" and method == "GET":
        return []

    if re.fullmatch(r"/service/[^/]+", path) and method in ("GET", "PUT", "DELETE"):
        return {
            "id": _service_id_from_path(path),
            "version": 1,
            "customer_id": "mock-customer",
        }

    if method == "GET" and re.search(r"^/service/[^/]+/version$", path):
        return [{"number": 1, "active": True}]

    if method == "GET" and re.search(r"^/service/[^/]+/version/active$", path):
        return {"number": 1, "active": True}

    if method == "GET" and "/version/" in path:
        if path.endswith(("/logging/s3", "/snippet", "/condition", "/domain", "/backend", "/vcl")):
            return []

    if "/version/" in path and "/activate" in path:
        return {"number": _version_from_path(path), "active": True}

    if "/version/" in path and "/validate" in path:
        return {"status": "ok", "errors": []}

    if "/version/" in path and path.endswith("/clone"):
        return {"number": _version_from_path(path) + 1}

    if "/domain" in path:
        return {"name": (body or {}).get("name", "mock.example"), "service_id": _service_id_from_path(path)}

    if "/backend" in path:
        return {"name": (body or {}).get("name", "fos_origin"), "address": "mock.example"}

    if "/snippet" in path:
        return {"id": "mock-snippet-id", "name": (body or {}).get("name", "mock_snippet")}

    if "/logging/" in path:
        return {"name": (body or {}).get("name", "mock-logger"), "version": _version_from_path(path)}

    if "/dictionary" in path:
        return {"id": "mock-dict-id", "name": (body or {}).get("name", "mock_dict")}

    if "/acl" in path:
        return {"id": "mock-acl-id", "name": (body or {}).get("name", "mock_acl")}

    if "/condition" in path:
        return {"name": (body or {}).get("name", "mock_condition"), "statement": (body or {}).get("statement", "")}

    if "/vcl" in path:
        return {"name": (body or {}).get("name", "main"), "content": (body or {}).get("content", "")}

    if re.search(r"/version(?:/\d+)?$", path) and method == "POST":
        return {"number": _version_from_path(path)}

    if "/object-storage/access-keys" in path:
        if method == "POST":
            return {
                "access_key": "AKIA_MOCK",
                "secret_key": "SECRET_MOCK",  # gitleaks:allow — canned mock secret, not real
                "description": (body or {}).get("description", "fos-log-analysis-mock"),
            }
        if method == "DELETE":
            return {}
        if method == "GET":
            return {"data": []}

    if "/object-storage/buckets" in path:
        if method == "POST":
            return {"data": {"id": "mock-bucket-id", "attributes": {"name": (body or {}).get("name", "mock-bucket")}}}
        if method == "GET":
            return {"data": []}

    return {"ok": True}


API_BASE = "https://api.fastly.com"
RT_BASE = "https://rt.fastly.com"

_RETRYABLE_HTTP_CODES = (429, 500, 502, 503, 504)
RETRY_AFTER_CAP_S = 30.0


def _is_retryable_fastly_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in _RETRYABLE_HTTP_CODES
    return isinstance(exc, (urllib.error.URLError, ConnectionError, TimeoutError))


def parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header given as delay-seconds, clamped to ``[0, RETRY_AFTER_CAP_S]``."""
    if not value:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        return None
    if seconds != seconds or seconds < 0:
        return None
    return min(seconds, RETRY_AFTER_CAP_S)


def _wait_strategy(method: str) -> Callable[[tenacity.RetryCallState], float]:
    """Default jittered backoff, except that GET retries honor a 429/503 ``Retry-After``."""
    default = tenacity.wait_exponential(multiplier=1, min=1, max=8) + tenacity.wait_random(min=0, max=2)

    def _wait(retry_state: tenacity.RetryCallState) -> float:
        if method.upper() == "GET" and retry_state.outcome is not None:
            exc = retry_state.outcome.exception()
            if isinstance(exc, urllib.error.HTTPError) and exc.code in (429, 503):
                hinted = parse_retry_after(exc.headers.get("Retry-After") if exc.headers else None)
                if hinted is not None:
                    return hinted
        return default(retry_state)

    return _wait


def _request(method, path, *, data, headers, expect_empty, max_retries, timeout, base=API_BASE):
    """Shared transport for ``fastly()`` and ``fastly_raw()``."""
    try:
        from backend.utils.telemetry import tracked_call

        telemetry_context = tracked_call(method, path, service="Fastly API")
    except ImportError:
        telemetry_context = None

    def _do_call():
        url = base + path

        try:
            for attempt in tenacity.Retrying(
                retry=tenacity.retry_if_exception(_is_retryable_fastly_error),
                stop=tenacity.stop_after_attempt(max_retries + 1),
                wait=_wait_strategy(method),
                sleep=lambda s: time.sleep(s),
                reraise=True,
            ):
                with attempt:
                    req = urllib.request.Request(url, data=data, headers=headers, method=method)
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        raw = resp.read().decode()
                        if expect_empty or not raw.strip():
                            return {}
                        return json.loads(raw)
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode(errors="replace")
            raise RuntimeError(f"HTTP {exc.code} {method} {path}\n    {body_text}") from exc
        except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
            raise RuntimeError(f"Network error on {method} {path}: {exc}") from exc

    if telemetry_context:
        with telemetry_context:
            return _do_call()
    return _do_call()


def fastly(method, path, body=None, *, token, expect_empty=False, max_retries=3, timeout=30):
    """Make a Fastly API request and return parsed JSON."""
    if is_mock_mode():
        return mock_response(method, path, body)

    data = json.dumps(body).encode() if body is not None else None
    hdrs = {"Fastly-Key": token, "Accept": "application/json"}
    if data:
        hdrs["Content-Type"] = "application/json"

    return _request(
        method,
        path,
        data=data,
        headers=hdrs,
        expect_empty=expect_empty,
        max_retries=max_retries,
        timeout=timeout,
    )


def fastly_raw(
    method,
    path,
    data: bytes,
    *,
    content_type: str,
    token,
    expect_empty=False,
    max_retries=3,
    timeout=60,
):
    """Make a Fastly API request with a RAW bytes body (not JSON)."""
    if is_mock_mode():
        try:
            body = json.loads(data.decode()) if data else None
        except Exception:
            body = None
        return mock_response(method, path, body)

    hdrs = {"Fastly-Key": token, "Accept": "application/json", "Content-Type": content_type}

    return _request(
        method,
        path,
        data=data,
        headers=hdrs,
        expect_empty=expect_empty,
        max_retries=max_retries,
        timeout=timeout,
    )


def fastly_rt(path, *, token, max_retries=0, timeout=10):
    """GET from the real-time analytics host (``rt.fastly.com``) and return parsed JSON."""
    if is_mock_mode():
        return mock_response("GET", path, None)

    return _request(
        "GET",
        path,
        data=None,
        headers={"Fastly-Key": token, "Accept": "application/json"},
        expect_empty=False,
        max_retries=max_retries,
        timeout=timeout,
        base=RT_BASE,
    )


PageStyle = Literal["cursor", "page", "link"]


def _next_path_from_link(link: str) -> str | None:
    """Reduce a ``links.next`` value to an API path, or ``None`` if it points off-host."""
    if link.startswith("/"):
        return link
    parsed = urllib.parse.urlsplit(link)
    if f"{parsed.scheme}://{parsed.netloc}" != API_BASE:
        return None
    return parsed.path + (f"?{parsed.query}" if parsed.query else "")


def fastly_paginate(
    path: str,
    *,
    token: str,
    style: PageStyle = "cursor",
    items_key: str = "data",
    params: dict[str, Any] | None = None,
    cursor_param: str = "cursor",
    next_cursor_path: tuple[str, ...] = ("meta", "next_cursor"),
    page_param: str = "page",
    per_page_param: str = "per_page",
    per_page: int = 100,
    max_pages: int = 1000,
    page_delay: float = 0.0,
    timeout: float = 30,
) -> Iterator[dict]:
    """GET every page of a list endpoint, yielding each parsed page body (reads only)."""
    base_params = dict(params or {})
    sep = "&" if "?" in path else "?"
    seen: set[str] = set()
    cursor: str | None = None
    page_no = 1
    next_path: str | None = None

    for _ in range(max_pages):
        if style == "link" and next_path is not None:
            request_path = next_path
        else:
            q = dict(base_params)
            if style == "cursor" and cursor:
                q[cursor_param] = cursor
            elif style == "page":
                q[page_param] = page_no
                q[per_page_param] = per_page
            query = urllib.parse.urlencode(q, quote_via=urllib.parse.quote, safe="")
            request_path = path + (sep + query if query else "")

        page = fastly("GET", request_path, token=token, timeout=timeout)
        yield page

        if style == "cursor":
            node: Any = page
            for key in next_cursor_path:
                node = node.get(key) if isinstance(node, dict) else None
            if not node or not isinstance(node, str) or node in seen:
                return
            seen.add(node)
            cursor = node
        elif style == "page":
            items = page.get(items_key) if isinstance(page, dict) else None
            if not items or len(items) < per_page:
                return
            page_no += 1
        else:
            link = (page.get("links") or {}).get("next") if isinstance(page, dict) else None
            if not link or not isinstance(link, str):
                return
            next_path = _next_path_from_link(link)
            if next_path is None or next_path in seen:
                return
            seen.add(next_path)

        if page_delay:
            time.sleep(page_delay)


def _count(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _opt_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


Count = Annotated[int, BeforeValidator(_count)]
OptInt = Annotated[int | None, BeforeValidator(_opt_int)]


class _Tolerant(BaseModel):
    model_config = ConfigDict(extra="allow")


class StatsRecord(_Tolerant):
    """One bucket of ``/stats/service/{id}`` or ``/stats/aggregate``."""

    start_time: OptInt = None
    requests: Count = 0
    edge_requests: Count = 0
    object_storage_class_a_operations_count: Count = 0
    object_storage_class_b_operations_count: Count = 0
    object_storage: Any = None

    def fos_ops(self) -> tuple[int, int]:
        a = self.object_storage_class_a_operations_count
        b = self.object_storage_class_b_operations_count
        if a or b:
            return a, b
        sub = self.object_storage
        if isinstance(sub, dict):
            return _count(sub.get("class_a_operations_count")), _count(sub.get("class_b_operations_count"))
        return 0, 0


class StatsResponse(_Tolerant):
    data: list[StatsRecord] = Field(default_factory=list)

    @field_validator("data", mode="before")
    @classmethod
    def _null_data_is_empty(cls, v: Any) -> Any:
        return [] if v is None else v


class NgwafWorkspace(_Tolerant):
    id: str | None = None
    name: str | None = None
    attributes: dict[str, Any] | None = None

    def display_name(self) -> str | None:
        return self.name or (self.attributes or {}).get("name") or self.id


class NgwafWorkspacesResponse(_Tolerant):
    """``GET /ngwaf/v1/workspaces``: list under ``data`` (current) or ``workspaces`` (legacy)."""

    data: list[NgwafWorkspace] | None = None
    workspaces: list[NgwafWorkspace] | None = None

    def workspaces_list(self) -> list[dict[str, str | None]]:
        items = self.data if self.data is not None else (self.workspaces or [])
        return [{"id": w.id, "name": w.display_name()} for w in items]


def get_active_version(service_id: str, token: str) -> int | None:
    info = get_active_version_info(service_id, token, timeout=None, max_retries=None)
    return info["number"] if info else None


def get_active_version_info(
    service_id: str, token: str, *, timeout: int | None = 8, max_retries: int | None = 1
) -> dict | None:
    """Return the active version's number + timestamps for a service."""
    extra_kwargs: dict[str, int] = {}
    if timeout is not None:
        extra_kwargs["timeout"] = timeout
    if max_retries is not None:
        extra_kwargs["max_retries"] = max_retries
    for path in (f"/service/{service_id}/version/active", f"/service/{service_id}/version"):
        try:
            res = fastly("GET", path, token=token, **extra_kwargs)
            candidates = [res] if isinstance(res, dict) else (res if isinstance(res, list) else [])
            for v in candidates:
                if isinstance(v, dict) and v.get("active"):
                    return {
                        "number": int(v["number"]),
                        "updated_at": v.get("updated_at"),
                        "created_at": v.get("created_at"),
                    }
        except RuntimeError:
            pass
    return None


def get_generated_vcl(service_id: str, version: int, token: str) -> str | None:
    """Return the fully-compiled (generated) VCL for a service version, or None."""
    try:
        result = fastly("GET", f"/service/{service_id}/version/{version}/generated_vcl", token=token)
    except RuntimeError:
        return None
    content = result.get("content") if isinstance(result, dict) else None
    return content or None


def find_service_by_name(name: str, token: str) -> dict | None:
    try:
        return next((s for s in fastly("GET", "/service", token=token) if s.get("name") == name), None)
    except RuntimeError:
        return None


def find_condition(name: str, service_id: str, version: int, token: str) -> dict | None:
    try:
        conditions = fastly("GET", f"/service/{service_id}/version/{version}/condition", token=token)
        return next((c for c in conditions if c.get("name") == name), None)
    except RuntimeError:
        return None


def ensure_condition(name: str, statement: str, type: str, service_id: str, version: int, token: str) -> dict:
    payload = {"name": name, "statement": statement, "type": type}
    existing = find_condition(name, service_id, version, token)
    if existing:
        if existing.get("statement") == statement and existing.get("type") == type:
            return existing
        encoded_name = urllib.parse.quote(name, safe="")
        return fastly("PUT", f"/service/{service_id}/version/{version}/condition/{encoded_name}", payload, token=token)
    return fastly("POST", f"/service/{service_id}/version/{version}/condition", payload, token=token)


def list_s3_endpoints(service_id: str, version: int, token: str) -> list[str]:
    try:
        return [r["name"] for r in fastly("GET", f"/service/{service_id}/version/{version}/logging/s3", token=token)]
    except RuntimeError:
        return []


def list_vcl_snippets(service_id: str, version: int, token: str) -> list[str]:
    try:
        return [r["name"] for r in fastly("GET", f"/service/{service_id}/version/{version}/snippet", token=token)]
    except RuntimeError:
        return []


def ensure_vcl_snippet(
    name: str, type: str, content: str, priority: int, service_id: str, version: int, token: str
) -> dict:
    payload = {"name": name, "type": type, "content": content, "priority": priority, "dynamic": 0}
    try:
        for s in fastly("GET", f"/service/{service_id}/version/{version}/snippet", token=token):
            if s.get("name") == name:
                if s.get("type") == type and s.get("content") == content and int(s.get("priority", 100)) == priority:
                    return s
                encoded_name = urllib.parse.quote(name, safe="")
                return fastly(
                    "PUT", f"/service/{service_id}/version/{version}/snippet/{encoded_name}", payload, token=token
                )
    except RuntimeError:
        pass
    return fastly("POST", f"/service/{service_id}/version/{version}/snippet", payload, token=token)
