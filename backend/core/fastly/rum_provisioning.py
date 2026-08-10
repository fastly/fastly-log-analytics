"""RUM (Real User Monitoring) VCL snippet generation and provisioning helpers.

Phase 1 snippets:
  - Request ID minting (recv, priority 10)
  - Beacon route (/rum-beacon) handling: sets x-skip-rum-logging flag + error 611 (recv, priority 20)
  - Session ID cookie setting (deliver, priority 101)

Phase 3 (deferred):
  - Asset fetch (/js/rum.js from FOS with SigV4) — see generate_rum_asset_fetch_vcl()
  - Self-hosted Faro Web SDK bundle (/js/faro-sdk.js), optional via faro_version — same
    FOS backend + SigV4 signing as /js/rum.js, plus a purgeable cache snippet keyed off
    Surrogate-Key "rum-faro-sdk" (matches the cron purge in backend/cron/jobs/rum_sync.py)
"""

from __future__ import annotations

import re

# Phase 1 snippet name constants — must match what the orchestrator expects.
RUM_RECV_NAME = "RUM - Recv"
RUM_DELIVER_SET_COOKIE_NAME = "RUM - Set cookies"

# Phase 3 snippet names (deferred) — not installed in Phase 1
RUM_ASSET_FETCH_NAME = "RUM - Asset fetch FOS"
RUM_SIGV4_SIGN_NAME = "RUM - Asset fetch SigV4 signing"
RUM_FARO_FETCH_NAME = "RUM - Faro SDK fetch caching"

# faro_version originates from a user-facing version picker (Task 7/8) and is
# interpolated into both a VCL string literal (the object-path rewrite in
# _generate_sigv4_sign_vcl) and an FOS object path (backend/provision/rum_assets.py's
# FARO_KEY_PREFIX). It must never carry a quote, newline, or path-traversal segment.
# Stable Faro releases are always plain X.Y.Z (see _is_stable_numeric in
# backend/core/faro_versions.py) so this is deliberately narrower than a general
# VCL-string-safety check (like _assert_vcl_string_safe in session_scoring_vcl.py) —
# digits and dots only, nothing else is a legitimate version.
#
# NOTE: \d matches any Unicode decimal-digit codepoint (category Nd), not just
# 0-9 — e.g. fullwidth "１２３" or Arabic-Indic "١٢٣" would pass a \d-based
# pattern and then be interpolated verbatim into VCL and an FOS object path.
# Use an explicit [0-9] class (not re.ASCII + \d, which is easy to lose on a
# future re.compile edit) so the character class itself is the source of truth.
_FARO_VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


def _assert_faro_version_safe(version: str) -> str:
    """Raise ``ValueError`` unless ``version`` is a plain ``X.Y.Z`` string.

    Returns ``version`` unchanged on success. This is the sole gate between a
    user-facing version picker and VCL/object-path interpolation for the Faro
    bundle — see the module note above.
    """
    if not isinstance(version, str) or not _FARO_VERSION_RE.fullmatch(version):
        raise ValueError(f"faro_version must be a plain X.Y.Z version string; got {version!r}")
    return version


def generate_rum_vcl(logging_service_id: str, faro_version: str | None = None) -> dict[str, str]:
    """Generate Phase 1 RUM VCL snippets for the logging service.

    Returns a dict mapping snippet names to VCL source code (Phase 1 only):
      {
        "RUM - Recv": "vcl ...",
        "RUM - Set cookies": "vcl ...",
      }

    ``faro_version``, when provided, is validated (see ``_assert_faro_version_safe``)
    but does not change Phase 1 output — the Faro asset-serving snippets live in
    ``generate_rum_asset_fetch_vcl()``. Validating here too means a caller that
    threads an operator-controlled version through this function fails loudly
    instead of silently dropping it.

    Phase 3 asset fetch snippets: see generate_rum_asset_fetch_vcl()
    """
    if faro_version is not None:
        _assert_faro_version_safe(faro_version)
    return {
        RUM_RECV_NAME: _generate_recv_vcl(),
        RUM_DELIVER_SET_COOKIE_NAME: _generate_deliver_set_cookie_vcl(logging_service_id),
    }


def _generate_recv_vcl() -> str:
    """Recv stage: handle RUM beacons."""
    return """# Edge-only (first hop, no restarts) block for RUM
if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {
    # Handle RUM beacon POST to /rum-beacon
    if (req.url.path == "/rum-beacon") {
        # Extract the essential fields from querystring:
        # - cid: session ID from rum_cid cookie (set in deliver)
        # - req: per-request ID (minted in recv)
        # - raw query: complete set of event_N_* params, parsed during ingest
        set req.http.x-fos-edge-data:rum_cid = querystring.get(req.url, "cid");
        set req.http.x-fos-edge-data:fastly_req_id = querystring.get(req.url, "req");
        set req.http.x-fos-edge-data:rum_raw_query = req.url;

        # Mark beacon to skip S3 logging (already logged separately to metadata DB)
        set req.http.x-skip-rum-logging = "1";

        # Synthetic 204 response (no origin round-trip needed)
        error 611 "No Content";
    }
}"""


def _generate_asset_fetch_vcl(shield_pop: str = "iad-va-us", faro_version: str | None = None) -> str:
    """Fetch /js/rum.js (and optionally /js/faro-sdk.js) from FOS with SigV4 signing.

    Routes GET /js/rum.js to FOS origin. Signing is handled in miss/pass
    VCL snippet (same pattern as CDN service). When ``faro_version`` is
    given, an identical GET route is added for /js/faro-sdk.js — same
    backend selection, same X-FOS-Request flag; the actual FOS object path
    rewrite happens in ``_generate_sigv4_sign_vcl``.

    This is the single source of truth for the recv-stage asset-fetch
    routing block: ``backend.provision.declarative.generators.
    generate_consolidated_snippet``'s ``vcl_recv`` branch calls
    ``generate_rum_asset_fetch_vcl`` (which wraps this function) directly
    rather than carrying its own copy, so editing this function is what
    actually changes shipped VCL (previously it did not — see the F-7 audit
    finding, now resolved).
    """
    if shield_pop and shield_pop.lower() != "none":
        shield_var = f"ssl_shield_{shield_pop.replace('-', '_')}"
        backend_str = f"fastly.try_select_shield({shield_var}, F_fos_origin)"
    else:
        backend_str = "F_fos_origin"

    base = f"""# Fetch RUM tracker JS from FOS with SigV4 signing
if (req.url.path == "/js/rum.js" && req.method == "GET") {{
    # Backend points to FOS endpoint (shared with logging)
    set req.backend = {backend_str};
    # Flag for SigV4 signing in miss/pass (req.backend.name not available there)
    set req.http.X-FOS-Request = "1";
    return(lookup);
}}"""

    if faro_version is None:
        return base

    # faro_version is never interpolated into this function's output (only its
    # presence gates whether the /js/faro-sdk.js block below is emitted) — this
    # call exists to fail fast rather than silently add the route for a value
    # that will later be rejected. The actual interpolation site (the object-path
    # rewrite that DOES need this guard) is _generate_sigv4_sign_vcl below.
    _assert_faro_version_safe(faro_version)
    faro_block = f"""# Fetch the self-hosted Faro Web SDK bundle from FOS (same backend + signing as rum.js)
if (req.url.path == "/js/faro-sdk.js" && req.method == "GET") {{
    set req.backend = {backend_str};
    set req.http.X-FOS-Request = "1";
    return(lookup);
}}"""
    return base + "\n\n" + faro_block


def _generate_sigv4_sign_vcl(faro_version: str | None = None) -> str:
    """SigV4 signing for FOS requests in miss/pass stages.

    Reuses the pattern from the CDN log-fronting service.
    Signs GET/HEAD requests to FOS with AWS SigV4 using credentials from edge dictionary.

    When ``faro_version`` is given, an extra rewrite is spliced in right after
    the existing /js/rum.js one so /js/faro-sdk.js resolves to the pinned FOS
    object path (``rum/faro-web-sdk-v{version}.iife.js`` — must match
    ``FARO_KEY_PREFIX``/``FARO_KEY_SUFFIX`` in backend/provision/rum_assets.py).
    Splicing via ``str.replace`` on the existing anchor (rather than turning
    this whole body into an f-string) keeps the faro_version=None output
    byte-for-byte identical to before this parameter existed.
    """
    body = """# FOS SigV4 signing
if (req.http.X-FOS-Request == "1" && !req.backend.is_shield) {
    declare local var.fosAccessKey STRING;
    declare local var.fosSecretKey STRING;
    declare local var.fosBucket STRING;
    declare local var.fosRegion STRING;
    declare local var.fosHost STRING;
    declare local var.canonicalHeaders STRING;
    declare local var.signedHeaders STRING;
    declare local var.canonicalRequest STRING;
    declare local var.canonicalQuery STRING;
    declare local var.stringToSign STRING;
    declare local var.dateStamp STRING;
    declare local var.signature STRING;
    declare local var.scope STRING;

    set var.fosAccessKey = table.lookup(fos_credentials, "access_key", "missing");
    set var.fosSecretKey = table.lookup(fos_credentials, "secret_key", "missing");
    set var.fosBucket = table.lookup(fos_credentials, "bucket", "missing");
    set var.fosRegion = table.lookup(fos_credentials, "region", "missing");
    set var.fosHost = var.fosRegion ".object.fastlystorage.app";

    set bereq.http.x-amz-content-sha256 = digest.hash_sha256("");
    set bereq.http.x-amz-date = strftime({"%Y%m%dT%H%M00Z"}, now);
    set bereq.http.host = var.fosHost;

    # If the user-facing path is /js/rum.js, point to the actual FOS object path
    if (bereq.url.path == "/js/rum.js") {
        set bereq.url = "/rum/rum-tracker.js";
    }

    # Prepend bucket if not already present
    if (regsub(bereq.url.path, "^/([^/]+)/.*$", "\\1") != var.fosBucket) {
        set bereq.url = "/" var.fosBucket bereq.url;
    }

    # Remove auth params and sort query string
    set bereq.url = querystring.filter(bereq.url, "key");
    set var.canonicalQuery = querystring.sort(bereq.url.qs);

    # Normalize path for signing
    set bereq.url = regsuball(urlencode(urldecode(bereq.url.path)), {"%2F"}, "/") + if(bereq.url.qs != "", "?" + bereq.url.qs, "");

    set var.dateStamp = strftime({"%Y%m%d"}, now);

    set var.canonicalHeaders = ""
        "host:" bereq.http.host LF
        "x-amz-content-sha256:" bereq.http.x-amz-content-sha256 LF
        "x-amz-date:" bereq.http.x-amz-date LF
    ;
    set var.signedHeaders = "host;x-amz-content-sha256;x-amz-date";
    set var.canonicalRequest = ""
        req.method LF
        bereq.url.path LF
        var.canonicalQuery LF
        var.canonicalHeaders LF
        var.signedHeaders LF
        digest.hash_sha256("")
    ;

    set var.scope = var.dateStamp "/" var.fosRegion "/s3/aws4_request";

    set var.stringToSign = ""
        "AWS4-HMAC-SHA256" LF
        bereq.http.x-amz-date LF
        var.scope LF
        regsub(digest.hash_sha256(var.canonicalRequest),"^0x", "")
    ;

    set var.signature = digest.awsv4_hmac(
        var.fosSecretKey,
        var.dateStamp,
        var.fosRegion,
        "s3",
        var.stringToSign
    );

    set bereq.http.Authorization = "AWS4-HMAC-SHA256 "
        "Credential=" var.fosAccessKey "/" var.scope ", "
        "SignedHeaders=" var.signedHeaders ", "
        "Signature=" + regsub(var.signature,"^0x", "")
    ;
    unset bereq.http.Accept;
    unset bereq.http.Accept-Language;
    unset bereq.http.User-Agent;
}"""

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
    """Cache + purge policy for the self-hosted Faro Web SDK bundle (vcl_fetch).

    Gated on ``beresp.status == 200`` (F-3 audit finding): without this
    check, a transient FOS 403/404 (e.g. mid-upload, or a bucket-policy
    blip) would be cached at the edge for 7 days AND handed to every
    browser tagged ``immutable`` — unpurgeable by browsers, and only
    fixable at the edge by waiting out the TTL or bumping the version.

    Edge TTL and browser TTL are deliberately decoupled via
    ``Surrogate-Control`` (edge-only) vs ``Cache-Control`` (browser-visible):
    ``/js/faro-sdk.js`` is a STABLE path serving MUTABLE content (an
    upgrade repoints the same path at a new pinned version), so a long
    browser ``max-age`` + ``immutable`` would mean already-issued browser
    copies can never be invalidated by an upgrade — purging only clears the
    edge cache, not any browser that already cached the old bytes. The edge
    keeps a long TTL (purged explicitly via Surrogate-Key on every upload/
    upgrade), while the browser gets a short one so a stale client re-checks
    soon after an upgrade even if a purge is missed.

    Surrogate-Key MUST be exactly "rum-faro-sdk" — the FOS-sync cron
    (backend/cron/jobs/rum_sync.py::_faro_purge_surrogate_key) and
    upgrade_faro_version (backend/provision/rum_orchestrator_v2.py::
    _purge_faro_surrogate_key) both purge this exact key after
    uploading/re-uploading a version, so the cache and the purge path stay
    pinned together.

    Follow-up (not this pass): a version-bearing public path (e.g.
    ``/js/faro-sdk-v{version}.js``) would let the browser cache
    immutably again, since each version would be a distinct URL — a larger
    design change than this fix, deliberately deferred.
    """
    return """# Cache the self-hosted Faro Web SDK bundle at the edge; only a short
# browser TTL, since this stable path serves mutable (per-upgrade) content.
if (req.url.path == "/js/faro-sdk.js" && req.http.X-FOS-Request == "1") {
    if (beresp.status == 200) {
        set beresp.ttl = 604800s;
        set beresp.cacheable = true;
        set beresp.http.Surrogate-Key = "rum-faro-sdk";
        set beresp.http.Surrogate-Control = "max-age=604800";
        set beresp.http.Cache-Control = "public, max-age=300";
    } else {
        set beresp.ttl = 0s;
        set beresp.cacheable = false;
    }
}"""


def _generate_deliver_set_cookie_vcl(logging_service_id: str) -> str:
    """Set the rum_cid cookie in deliver stage (post-scoring if enabled)."""
    del logging_service_id  # Not needed for basic version
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
    synthetic "";
    return (deliver);
}"""


def generate_rum_asset_fetch_vcl(shield_pop: str = "iad-va-us", faro_version: str | None = None) -> dict[str, str]:
    """Generate Phase 3 RUM asset fetch VCL snippets (deferred, not yet deployed).

    Returns a dict mapping snippet names to VCL source code:
      {
        "RUM - Asset fetch FOS": "vcl ...",
        "RUM - Asset fetch SigV4 signing": "vcl ...",
      }

    These snippets are NOT part of Phase 1 deployment. Phase 1 includes only:
    - Request ID minting + beacon handling (recv)
    - Session ID cookie setting (deliver)

    ``faro_version``, when given (validated via ``_assert_faro_version_safe``),
    adds the self-hosted Faro Web SDK bundle route to the two existing snippets
    plus a third, purgeable, vcl_fetch-stage caching snippet:
      {
        ...,
        "RUM - Faro SDK fetch caching": "vcl ...",
      }
    When ``faro_version`` is None (the default), the returned dict and both
    existing snippet bodies are byte-for-byte identical to before this
    parameter existed — services that haven't pinned a Faro version are
    unaffected.
    """
    if faro_version is not None:
        _assert_faro_version_safe(faro_version)

    snippets = {
        RUM_ASSET_FETCH_NAME: _generate_asset_fetch_vcl(shield_pop, faro_version),
        RUM_SIGV4_SIGN_NAME: _generate_sigv4_sign_vcl(faro_version),
    }
    if faro_version is not None:
        snippets[RUM_FARO_FETCH_NAME] = _generate_faro_fetch_vcl()
    return snippets
