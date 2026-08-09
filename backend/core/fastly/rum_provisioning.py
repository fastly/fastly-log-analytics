"""RUM (Real User Monitoring) VCL snippet generation and provisioning helpers.

Phase 1 snippets:
  - Request ID minting (recv, priority 10)
  - Beacon route (/rum-beacon) handling: sets x-skip-rum-logging flag + error 611 (recv, priority 20)
  - Session ID cookie setting (deliver, priority 101)

Phase 3 (deferred):
  - Asset fetch (/js/rum.js from FOS with SigV4) — see generate_rum_asset_fetch_vcl()
"""

from __future__ import annotations

# Phase 1 snippet name constants — must match what the orchestrator expects.
RUM_RECV_NAME = "RUM - Recv"
RUM_DELIVER_SET_COOKIE_NAME = "RUM - Set cookies"

# Phase 3 snippet names (deferred) — not installed in Phase 1
RUM_ASSET_FETCH_NAME = "RUM - Asset fetch FOS"
RUM_SIGV4_SIGN_NAME = "RUM - Asset fetch SigV4 signing"


def generate_rum_vcl(logging_service_id: str) -> dict[str, str]:
    """Generate Phase 1 RUM VCL snippets for the logging service.

    Returns a dict mapping snippet names to VCL source code (Phase 1 only):
      {
        "RUM - Recv": "vcl ...",
        "RUM - Set cookies": "vcl ...",
      }

    Phase 3 asset fetch snippets: see generate_rum_asset_fetch_vcl()
    """
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


def _generate_asset_fetch_vcl(shield_pop: str = "iad-va-us") -> str:
    """Fetch /js/rum.js from FOS with SigV4 signing.

    Routes GET /js/rum.js to FOS origin. Signing is handled in miss/pass
    VCL snippet (same pattern as CDN service).
    """
    if shield_pop and shield_pop.lower() != "none":
        shield_var = f"ssl_shield_{shield_pop.replace('-', '_')}"
        backend_str = f"fastly.try_select_shield({shield_var}, F_fos_origin)"
    else:
        backend_str = "F_fos_origin"

    return f"""# Fetch RUM tracker JS from FOS with SigV4 signing
if (req.url.path == "/js/rum.js" && req.method == "GET") {{
    # Backend points to FOS endpoint (shared with logging)
    set req.backend = {backend_str};
    # Flag for SigV4 signing in miss/pass (req.backend.name not available there)
    set req.http.X-FOS-Request = "1";
    return(lookup);
}}"""


def _generate_sigv4_sign_vcl() -> str:
    """SigV4 signing for FOS requests in miss/pass stages.

    Reuses the pattern from the CDN log-fronting service.
    Signs GET/HEAD requests to FOS with AWS SigV4 using credentials from edge dictionary.
    """
    return """# FOS SigV4 signing
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


def generate_rum_asset_fetch_vcl(shield_pop: str = "iad-va-us") -> dict[str, str]:
    """Generate Phase 3 RUM asset fetch VCL snippets (deferred, not yet deployed).

    Returns a dict mapping snippet names to VCL source code:
      {
        "RUM - Asset fetch FOS": "vcl ...",
        "RUM - Asset fetch SigV4 signing": "vcl ...",
      }

    These snippets are NOT part of Phase 1 deployment. Phase 1 includes only:
    - Request ID minting + beacon handling (recv)
    - Session ID cookie setting (deliver)
    """
    return {
        RUM_ASSET_FETCH_NAME: _generate_asset_fetch_vcl(shield_pop),
        RUM_SIGV4_SIGN_NAME: _generate_sigv4_sign_vcl(),
    }
