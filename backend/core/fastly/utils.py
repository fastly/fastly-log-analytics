import argparse
import re
import secrets

# Candidate field names on Fastly's /stats/service response that carry the
# "log lines emitted" counter. Ordered: most-likely first. If all four miss
# we log once per request and fall back to 0 so the panel still renders.
FASTLY_LOG_FIELDS = ("log", "log_records", "log_entries", "logging_requests")

# Mapping from Fastly Object Storage region to Fastly Shield POP
# Source: https://www.fastly.com/documentation/guides/platform/object-storage/working-with-object-storage/#managing-object-storage-buckets-and-objects
SHIELD_MAP = {
    "us-east-1": "iad-va-us",  # Ashburn, VA
    "us-west": "bfi-wa-us",  # Seattle, WA (BFI)
    "us-central-1": "chi-il-us",  # Chicago, IL (CHI)
    "eu-central": "frankfurt-de",  # Frankfurt, Germany
    "eu-south-1": "mxp-milan-it",  # Milan, Italy
    "uk-east-1": "london-uk",  # London, UK
    "jp-central-1": "nrt-tokyo-jp",  # Tokyo, Japan (NRT)
    "au-east-1": "sydney-au",  # Sydney, Australia
}


def region_endpoint(region: str) -> str:
    return f"{region}.object.fastlystorage.app"


def parse_period(s: str) -> int:
    """Parse '60', '1 minute', '5m', '5 minutes' → integer seconds."""
    s = s.strip().lower()
    m = re.match(r"^(\d+)\s*(m(?:in(?:utes?)?)?)?$", s)
    if m:
        val = int(m.group(1))
        return val * 60 if m.group(2) else val
    raise ValueError(f"Cannot parse period: {s!r}  (try '60' or '5 minutes')")


def int_range(mini, maxi):
    """Return a type checker for argparse to enforce numeric ranges."""

    def checker(arg):
        try:
            val = int(arg)
        except ValueError:
            raise argparse.ArgumentTypeError("Must be an integer")
        if val < mini or val > maxi:
            raise argparse.ArgumentTypeError(f"Must be between {mini} and {maxi}")
        return val

    return checker


def load_vcl(rate_limiting: bool = True) -> str:
    vcl = """#RATELIMIT_BEGIN
ratecounter auth_fail_rc {}
penaltybox auth_fail_pb {}
#RATELIMIT_END

sub miss_pass {
    # Fastly Object Storage signing https://www.fastly.com/documentation/guides/integrations/non-fastly-services/amazon-s3/
    if ((req.method == "GET" || req.method == "HEAD") && !req.backend.is_shield) {
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
        # Round x-amz-date down to the minute (finding 006). The SigV4
        # signature is computed over this header — making it per-request
        # invalidates Fastly's per-(URL, method) collapsed-forwarding /
        # request coalescing: every concurrent request gets its own unique
        # signed Authorization, so they all forward to FOS in parallel
        # instead of riding on a single in-flight fetch. Pinning the seconds
        # to ``00`` collapses all requests within a minute to the SAME
        # signature, restoring coalescing and capping the per-minute origin
        # spend regardless of incoming RPS. Still well inside AWS SigV4's
        # 15-minute validity window. (Audit limited VCL arithmetic — no %
        # or / operator — so per-minute granularity via strftime format
        # is the cleanest expression of the rounding.)
        set bereq.http.x-amz-date = strftime({"%Y%m%dT%H%M00Z"}, now);
        set bereq.http.host = var.fosHost;

        # Only prepend bucket if not already present (handles direct S3-style paths from DuckDB/Boto3)
        if (regsub(bereq.url.path, "^/([^/]+)/.*$", "\\1") != var.fosBucket) {
             set bereq.url = "/" var.fosBucket bereq.url;
        }

        # Keep query parameters for S3 API calls (ListObjectsV2, etc)
        # BUT remove our auth 'key' parameter so it doesn't leak to FOS or mess with signing
        set bereq.url = querystring.filter(bereq.url, "key");

        # Sort for canonical request
        set var.canonicalQuery = querystring.sort(bereq.url.qs);

        # Normalize path for signing (S3 expects encoded path)
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
        unset bereq.http.Fastly-Client-IP;
    }
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
  if (req.url ~ "/iceberg/meta/admin_state\\.json$") {
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
    if not rate_limiting:
        vcl = re.sub(r"\s*#RATELIMIT_BEGIN.*?#RATELIMIT_END", "", vcl, flags=re.DOTALL)
    # Substitute the fallback-secret placeholder with a fresh random
    # value so that when ``cdn_auth.secret`` is missing from the edge
    # dictionary, the lookup returns an unguessable value and the auth
    # check fails closed instead of allowing empty-key requests through.
    # A new secret per load_vcl() call is fine: real auth uses the
    # dictionary value (this fallback is never matched in steady state).
    # The ``unprovisioned-fallback-`` prefix is purely cosmetic — it
    # self-documents the rendered VCL so the value reads as a placeholder
    # rather than a leaked secret, while the token_hex tail keeps it
    # unguessable.
    vcl = vcl.replace("REPLACE_AT_LOAD_VCL_FALLBACK_SECRET", "unprovisioned-fallback-" + secrets.token_hex(32))
    return vcl
