import argparse
import re

# Candidate field names on Fastly's /stats/service response that carry the
# "log lines emitted" counter. Ordered: most-likely first. If all four miss
# we log once per request and fall back to 0 so the panel still renders.
FASTLY_LOG_FIELDS = ("log", "log_records", "log_entries", "logging_requests")

# Mapping from Fastly Object Storage region to Fastly Shield POP
# Source: https://www.fastly.com/documentation/guides/platform/object-storage/working-with-object-storage/#managing-object-storage-buckets-and-objects
SHIELD_MAP = {
    "us-east-1": "iad-va-us",  # Ashburn, VA
    "us-west": "sea-wa-us",  # Seattle, WA
    "us-central-1": "mdw-il-us",  # Chicago, IL
    "eu-central": "fra-de-eu",  # Frankfurt, Germany
    "eu-south-1": "mxp-it-eu",  # Milan, Italy
    "uk-east-1": "lcy-gb-eu",  # London, UK
    "jp-central-1": "tyo-jp-asia",  # Tokyo, Japan
    "au-east-1": "syd-au-aus",  # Sydney, Australia
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
        set bereq.http.x-amz-date = strftime({"%Y%m%dT%H%M%SZ"}, now);
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
  if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {
    set req.http.Fastly-Client-IP = client.ip;
  }

  # Block requests that do not provide the correct secret key (purges are exempt)
  if (req.method != "FASTLYPURGE" && req.restarts == 0 && fastly.ff.visits_this_service == 0 && subfield(req.url.qs, "key", "&") != table.lookup(cdn_auth, "secret", "") && req.http.x-fastly-key != table.lookup(cdn_auth, "secret", "")) {
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
  if (req.method != "FASTLYPURGE" && req.restarts == 0 && fastly.ff.visits_this_service == 0) {
    if (ratelimit.penaltybox_has(auth_fail_pb, req.http.Fastly-Client-IP)) {
      error 401 "Unauthorized";
    }
  }
#RATELIMIT_END

  # Enable segmented caching for potentially large log or parquet files
  set req.enable_segmented_caching = true;
  set segmented_caching.block_size = 20971520; # 20 MB, the maximum

  # Strip only the key from the URL before forwarding to Fastly Object Storage
  set req.url = querystring.filter(req.url, "key");

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
  set req.hash += req.url.path;
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
    return vcl
