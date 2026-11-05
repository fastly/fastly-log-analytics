#RATELIMIT_BEGIN
ratecounter auth_fail_rc {}


penaltybox auth_fail_pb {}


ratecounter auth_challenge_rc {}


#RATELIMIT_END

sub vcl_recv {
  if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {
    set req.http.Fastly-Client-IP = client.ip;
    unset req.http.x-flp-front-key;
    unset req.http.x-flp-front-rl;
    unset req.http.x-flp-clock-secret;
    unset req.http.x-flp-worker-key;
    unset req.http.x-flp-tick;
    unset req.http.x-flp-auth-fail;
    unset req.http.x-flp-api-file;
    unset req.http.x-flp-fos-service;
    unset req.http.x-flp-fos-bucket;
    unset req.http.x-flp-fos-region;
    set req.http.x-flp-front-key = table.lookup(flp_front_auth, "front_key", "");
    if (req.http.x-flp-front-key == "") {
      error 620 "Front not configured";
    }
    set req.http.x-flp-front-rl = "1";
    #RATELIMIT_BEGIN
    if (req.url.path ~ "^/(setup|share-login|auth|api)(\.(html|txt)|/|$)") {
      if (ratelimit.penaltybox_has(auth_fail_pb, req.http.Fastly-Client-IP)) {
        error 618 "Too Many Requests";
      }
    }
    if (req.url.path ~ "^/(api/)?auth/.*(challenge|options|begin)$") {
      declare local var.challenges INTEGER;
      set var.challenges = ratelimit.ratecounter_increment(auth_challenge_rc, req.http.Fastly-Client-IP, 1);
      if (var.challenges > 30) {
        error 619 "Too Many Requests";
      }
    }
    #RATELIMIT_END
    if (!req.http.Cookie:flp_cdn && req.http.Cookie:__Host-flp_cdn) {
      set req.http.Cookie:flp_cdn = req.http.Cookie:__Host-flp_cdn;
    }
    declare local var.browser_file_key STRING;
    set var.browser_file_key = "";
    if ((req.method == "GET" || req.method == "HEAD") && (req.http.Cookie:flp_cdn || req.http.Cookie:flp_session)) {
      if (req.url.path == "/api/file") {
        set var.browser_file_key = regsuball(subfield(req.url.qs, "key", "&"), "\+", "%20");
      } else if ((!req.http.x-fastly-key || req.http.x-fastly-key == "") && req.url.path ~ "^/fos/pq/((?:[A-Za-z0-9._-]+/)?processed/(?:request|rum|ops|summary|summary_bundle)/.+\.parquet)$") {
        set var.browser_file_key = re.group.1;
      } else if ((!req.http.x-fastly-key || req.http.x-fastly-key == "") && req.url.path ~ "^/fos/((?:[A-Za-z0-9._-]+/)?processed/(?:request|rum|ops|summary|summary_bundle)/.+\.parquet)$") {
        set var.browser_file_key = re.group.1;
      } else if (req.url.path ~ "^/([A-Za-z0-9._-]+/processed/(?:request|rum|ops|summary|summary_bundle)/.+\.parquet)$") {
        set var.browser_file_key = re.group.1;
      }
    }
    if ((req.url.path == "/api/file" || var.browser_file_key != "") && (req.method == "GET" || req.method == "HEAD") && req.http.Cookie:flp_cdn && req.http.Cookie:flp_session) {
      declare local var.cdn_secret STRING;
      set var.cdn_secret = table.lookup(flp_front_auth, "cdn_key", "");
      if (var.cdn_secret != "" && req.http.Cookie:flp_cdn ~ "^(([A-Za-z0-9]{22})~(admin|analyst|no_pii)~([0-9]{10})~([0-9a-f]{16})~([A-Za-z0-9._/-]*))~([0-9a-f]{64})$") {
        declare local var.cdn_payload STRING;
        declare local var.cdn_sid STRING;
        declare local var.cdn_role STRING;
        declare local var.cdn_exp STRING;
        declare local var.cdn_shash STRING;
        declare local var.cdn_scope STRING;
        declare local var.cdn_sig STRING;
        set var.cdn_payload = re.group.1;
        set var.cdn_sid = re.group.2;
        set var.cdn_role = re.group.3;
        set var.cdn_exp = re.group.4;
        set var.cdn_shash = re.group.5;
        set var.cdn_scope = re.group.6;
        set var.cdn_sig = re.group.7;
        if (
          var.cdn_shash == substr(digest.hash_sha256(req.http.Cookie:flp_session), 0, 16) &&
          digest.secure_is_equal(digest.hmac_sha256(var.cdn_secret, "flp-cdn-v1~" + var.cdn_payload), "0x" + var.cdn_sig) &&
          std.atoi(var.cdn_exp) > std.atoi(strftime({"%s"}, now)) &&
          table.lookup(flp_fos_buckets, var.cdn_sid, "") != "" &&
          table.lookup(flp_fos_credentials, var.cdn_sid + ".ak", "") != "" &&
          table.lookup(flp_fos_credentials, var.cdn_sid + ".sk", "") != ""
        ) {
          if (var.cdn_role == "no_pii") {
            error 624 "Forbidden";
          }
          if (var.browser_file_key == "") {
            error 625 "Bad Request";
          }
          declare local var.file_key STRING;
          declare local var.rel_key STRING;
          set var.file_key = urldecode(var.browser_file_key);
          if (
            std.strlen(var.file_key) > 512 ||
            std.strlen(var.file_key) <= std.strlen(var.cdn_scope) ||
            substr(var.file_key, 0, std.strlen(var.cdn_scope)) != var.cdn_scope
          ) {
            error 626 "Bad Request";
          }
          set var.rel_key = substr(var.file_key, std.strlen(var.cdn_scope));
          if (
            var.rel_key !~ "^processed/(request|rum|ops|summary|summary_bundle)/" ||
            var.file_key !~ "\.parquet$" ||
            var.file_key ~ "(\.\.|//|[^A-Za-z0-9._=/\-])"
          ) {
            error 626 "Bad Request";
          }
          if (var.rel_key ~ "^processed/ops/" && var.cdn_role != "admin") {
            error 624 "Forbidden";
          }
          set req.http.x-flp-api-file = "1";
          set req.http.x-fastly-key = var.cdn_secret;
          set req.url = "/fos/" + var.cdn_sid + "/" + var.file_key;
        }
      }
    }
    if (!req.http.x-flp-api-file && req.url.path != "/api/file" && var.browser_file_key != "") {
      set req.url = "/api/file?key=" + urlencode(urldecode(var.browser_file_key));
    }
    if (req.url.path ~ "^/fos(/|$)") {
      declare local var.cdn_key STRING;
      declare local var.registry STRING;
      set var.cdn_key = table.lookup(flp_front_auth, "cdn_key", "");
      if (var.cdn_key == "" || req.http.x-fastly-key != var.cdn_key) {
        error 622 "Forbidden";
      }
      if (req.method != "GET" && req.method != "HEAD") {
        error 405 "Method Not Allowed";
      }
      if (req.url.path ~ "^/fos/([A-Za-z0-9]{22})(/.*)$") {
        set req.http.x-flp-fos-service = re.group.1;
      } else {
        error 621 "Not Found";
      }
      set var.registry = table.lookup(flp_fos_buckets, req.http.x-flp-fos-service, "");
      if (var.registry ~ "^([a-z0-9][a-z0-9-]{1,61}[a-z0-9])\|([a-z0-9-]+)$") {
        set req.http.x-flp-fos-bucket = re.group.1;
        set req.http.x-flp-fos-region = re.group.2;
      } else {
        error 621 "Not Found";
      }
      if (req.http.x-flp-fos-region !~ "^(us-east-1|us-west|us-central-1|eu-central-1|eu-south-1|eu-west-1|uk-east-1|jp-central-1|au-east-1)$") {
        error 621 "Not Found";
      }
      if (
        table.lookup(flp_fos_credentials, req.http.x-flp-fos-service ".ak", "") == "" ||
        table.lookup(flp_fos_credentials, req.http.x-flp-fos-service ".sk", "") == ""
      ) {
        error 621 "Not Found";
      }
      unset req.http.Cookie;
      unset req.http.Authorization;
      unset req.http.x-fastly-key;
      unset req.http.x-flp-front-key;
      unset req.http.x-flp-front-rl;
      set req.url = querystring.filter_except(req.url, "list-type" + querystring.filtersep() + "prefix" + querystring.filtersep() + "delimiter" + querystring.filtersep() + "continuation-token" + querystring.filtersep() + "start-after" + querystring.filtersep() + "max-keys" + querystring.filtersep() + "encoding-type" + querystring.filtersep() + "fetch-owner" + querystring.filtersep() + "marker" + querystring.filtersep() + "versionId" + querystring.filtersep() + "partNumber" + querystring.filtersep() + "response-content-type" + querystring.filtersep() + "response-content-disposition" + querystring.filtersep() + "response-cache-control");
      set req.url = querystring.sort(req.url);
    }
  }

  if (req.http.x-flp-fos-service) {
    set req.enable_segmented_caching = true;
    set segmented_caching.block_size = 20971520;
    if (fastly.ff.visits_this_service > 1) {
      set req.max_stale_while_revalidate = 0s;
    }
  }
  if (req.url.path == "/_tick" || req.url.path == "/_flush") {
    if (req.method != "GET") {
      error 405 "Method Not Allowed";
    }
    # Edge and shield cache across seconds for the sleep TTL Compute returns in x-flp-tick-ttl.
    set req.url = req.url.path;
    set req.http.x-flp-tick = "1";
    unset req.http.Cookie;
  }
#FASTLY recv
  if (!req.http.x-flp-fos-service) {
    # Fastly normalizes to gzip unless the service enables Brotli; Compute holds br variants.
    declare local var.br STRING;
    declare local var.gzip STRING;
    set var.br = accept.encoding_lookup("br", "identity", req.http.Fastly-Orig-Accept-Encoding);
    set var.gzip = accept.encoding_lookup("gzip", "identity", req.http.Fastly-Orig-Accept-Encoding);
    # Keep gzip beside br: some assets (the DuckDB wasm) are stored gzip-only.
    if (var.br == "br" && var.gzip == "gzip") {
      set req.http.Accept-Encoding = "br, gzip";
    } else if (var.br == "br") {
      set req.http.Accept-Encoding = "br";
    } else if (var.gzip == "gzip") {
      set req.http.Accept-Encoding = "gzip";
    } else {
      unset req.http.Accept-Encoding;
    }
    # The shield only sees the edge-normalized value, so carry the client's gzip support.
    if (fastly.ff.visits_this_service == 0) {
      if (var.gzip == "gzip") {
        set req.http.x-flp-gzip = "1";
      } else {
        unset req.http.x-flp-gzip;
      }
    }
  }
  if (req.http.x-flp-fos-service) {
    # Every FOS object key is written once; only bucket listings change underneath us.
    if (req.url.path ~ "^/fos/[A-Za-z0-9]{22}/?$" || req.url.qs ~ "(^|&)(list-type|prefix|delimiter|continuation-token|start-after|marker|max-keys|encoding-type|fetch-owner)=") {
      return(pass);
    }
    return(lookup);
  }
  if (req.method != "GET" && req.method != "HEAD") {
    return(pass);
  }
  if (req.url.path ~ "^/(api|auth|setup|share-login|admin|dashboard|origin|performance|security|network|fastly-value|assets-shield|rum|streaming|sessions|query|insights|logs|provision|log-fields|services|cloud-files)(\.(html|txt)|/|$)") {
    return(pass);
  }
  if (req.url.path ~ "^/(_next/static|duckdb|geo)/" || req.url.path ~ "^/maplibre-gl-") {
    unset req.http.Cookie;
  }
  return(lookup);
}


sub vcl_hash {
#FASTLY hash
  set req.hash += req.url;
  set req.hash += req.http.host;
  if (req.http.x-flp-fos-service) {
    set req.hash += req.http.x-flp-fos-bucket;
    set req.hash += req.http.x-flp-fos-region;
  }
  return(hash);
}


sub vcl_hit {
#FASTLY hit
  return(deliver);
}


sub vcl_miss {
#FASTLY miss
  call flp_fos_sign;
  return(fetch);
}


sub vcl_pass {
#FASTLY pass
  call flp_fos_sign;
  return(pass);
}


sub vcl_fetch {
  if (req.http.x-flp-fos-service) {
    unset beresp.http.x-amz-id-2;
    unset beresp.http.x-amz-request-id;
    unset beresp.http.x-amz-delete-marker;
    unset beresp.http.x-amz-version-id;
    if (req.http.x-flp-api-file == "1") {
      if (beresp.status == 404) {
        error 627 "Not Found";
      }
      if (beresp.status != 200 && beresp.status != 206 && beresp.status != 304 && beresp.status != 416) {
        error 628 "Bad Gateway";
      }
    }
    if (
      (beresp.status != 200 && beresp.status != 206) || beresp.http.Set-Cookie ||
      (beresp.http.Cache-Control ~ "(?i)(private|no-store)" && req.http.x-flp-api-file != "1") ||
      req.url.path ~ "^/fos/[A-Za-z0-9]{22}/?$" ||
      req.url.qs ~ "(^|&)(list-type|prefix|delimiter|continuation-token|start-after|marker|max-keys|encoding-type|fetch-owner)="
    ) {
      set beresp.cacheable = false;
      return(pass);
    }
    unset beresp.http.Vary;
    set beresp.cacheable = true;
    declare local var.sid STRING;
    set var.sid = req.http.x-flp-fos-service;
    if (req.url.path ~ "^/fos/[A-Za-z0-9]{22}/(.+/)?processed/ops/([A-Za-z0-9_]+)/") {
      set beresp.http.Surrogate-Key = "flp-fos flp-fos-" var.sid " flp-fos-ops-" re.group.2;
    } else if (req.url.path ~ "^/fos/[A-Za-z0-9]{22}/(.+/)?processed/request/") {
      set beresp.http.Surrogate-Key = "flp-fos flp-fos-" var.sid " flp-fos-request";
    } else {
      set beresp.http.Surrogate-Key = "flp-fos flp-fos-" var.sid;
    }
    # One day never exceeds the shortest configurable retention; the retention sweep also
    # purges flp-fos-<sid> after deleting, and nothing serves stale past that.
    set beresp.ttl = 86400s;
    set beresp.stale_if_error = 0s;
    set beresp.stale_while_revalidate = 0s;
    return(deliver);
  }
  if (req.http.x-flp-tick == "1") {
    unset beresp.http.Set-Cookie;
    unset beresp.http.Vary;
    unset beresp.http.content-security-policy;
    unset beresp.http.strict-transport-security;
    unset beresp.http.x-content-type-options;
    unset beresp.http.referrer-policy;
    set beresp.grace = 0s;
    set beresp.stale_if_error = 0s;
    set beresp.stale_while_revalidate = 0s;
    set beresp.cacheable = true;
    set beresp.http.Surrogate-Key = "flp-clock";
    set beresp.ttl = 1s;
    if (beresp.status == 200 && beresp.http.x-flp-tick-ttl) {
      declare local var.ttl INTEGER;
      set var.ttl = std.atoi(beresp.http.x-flp-tick-ttl);
      if (beresp.http.Age) {
        set var.ttl -= std.atoi(beresp.http.Age);
      }
      if (var.ttl > 1) {
        set beresp.ttl *= var.ttl;
      }
    }
    if (fastly.ff.visits_this_service == 0) {
      unset beresp.http.x-flp-tick-ttl;
    }
    return(deliver);
  }
  # Compute marks unencoded JSON of at least 1 KiB; the first node to see it compresses.
  if (beresp.http.x-compress-hint == "on") {
    if (
      req.http.x-flp-gzip == "1" && !beresp.http.Content-Encoding &&
      beresp.http.Content-Type ~ "^application/json"
    ) {
      set beresp.gzip = true;
      if (beresp.http.Vary) {
        set beresp.http.Vary = beresp.http.Vary ", Accept-Encoding";
      } else {
        set beresp.http.Vary = "Accept-Encoding";
      }
    }
    unset beresp.http.x-compress-hint;
  }
  if (
    req.url.path ~ "^/(api|auth|setup|share-login|admin|dashboard|origin|performance|security|network|fastly-value|assets-shield|rum|streaming|sessions|query|insights|logs|provision|log-fields|services|cloud-files)(\.(html|txt)|/|$)" ||
    beresp.http.Cache-Control ~ "(?i)(private|no-store)"
  ) {
    set beresp.cacheable = false;
    return(pass);
  }
  if ((req.url.path ~ "^/(_next/static|duckdb|geo)/" || req.url.path ~ "^/maplibre-gl-") && beresp.status == 200) {
    unset beresp.http.Set-Cookie;
    set beresp.cacheable = true;
    set beresp.ttl = 31536000s;
    set beresp.http.Cache-Control = "public, max-age=31536000, immutable";
    set beresp.http.Surrogate-Key = "flp-static";
    return(deliver);
  }
  if (beresp.http.Set-Cookie) {
    set beresp.cacheable = false;
    return(pass);
  }
  if (beresp.status == 200 && beresp.http.Content-Type ~ "^text/html") {
    set beresp.cacheable = true;
    set beresp.ttl = 60s;
    set beresp.http.Cache-Control = "public, max-age=60";
    set beresp.http.Surrogate-Key = "flp-html";
    return(deliver);
  }
  # Compute marks every shared static-export file (RSC .txt, icons, the 404 page) public.
  if ((beresp.status == 200 || beresp.status == 404) && beresp.http.Cache-Control ~ "(?i)\bpublic\b") {
    set beresp.cacheable = true;
    set beresp.ttl = 60s;
    set beresp.http.Cache-Control = "public, max-age=60";
    if (!beresp.http.Surrogate-Key) {
      set beresp.http.Surrogate-Key = "flp-html";
    }
    return(deliver);
  }
#FASTLY fetch
  set beresp.cacheable = false;
  return(pass);
}


sub vcl_deliver {
#FASTLY deliver
  #RATELIMIT_BEGIN
  if (fastly.ff.visits_this_service == 0 && resp.http.x-flp-auth-fail == "1") {
    declare local var.failures INTEGER;
    set var.failures = ratelimit.ratecounter_increment(auth_fail_rc, req.http.Fastly-Client-IP, 1);
    if (var.failures >= 10) {
      ratelimit.penaltybox_add(auth_fail_pb, req.http.Fastly-Client-IP, 15m);
    }
  }
  #RATELIMIT_END
  if (fastly.ff.visits_this_service == 0) {
    unset resp.http.x-flp-auth-fail;
  }
  unset resp.http.x-flp-front-key;
  unset resp.http.x-flp-front-rl;
  if (req.http.x-flp-fos-service) {
    declare local var.shield_fos_cache STRING;
    set var.shield_fos_cache = resp.http.x-flp-fos-cache;
    set resp.http.x-flp-fos-cache = if(fastly_info.state ~ "^HIT" && fastly_info.state !~ "(SYNTH|PASS)", "HIT", "MISS");
    if (var.shield_fos_cache == "HIT") {
      set resp.http.x-flp-fos-cache = "HIT";
    }
    unset resp.http.x-amz-id-2;
    unset resp.http.x-amz-request-id;
    unset resp.http.server;
    if (req.http.x-flp-api-file == "1") {
      if (req.method == "HEAD" && req.http.Range == "bytes=0-" && resp.status == 200) {
        declare local var.len INTEGER;
        declare local var.last INTEGER;
        set var.len = std.atoi(resp.http.Content-Length);
        if (var.len > 0) {
          set var.last = var.len;
          set var.last -= 1;
          set resp.status = 206;
          set resp.response = "Partial Content";
          set resp.http.Accept-Ranges = "bytes";
          set resp.http.Content-Range = "bytes 0-" + var.last + "/" + var.len;
        }
      }
      set resp.http.Content-Type = "application/vnd.apache.parquet";
      set resp.http.Cache-Control = "private, max-age=31536000, immutable";
      set resp.http.Vary = "Cookie";
      set resp.http.x-flp-cdn-cache = resp.http.x-flp-fos-cache;
      set resp.http.Strict-Transport-Security = "max-age=31536000; includeSubDomains";
      set resp.http.X-Content-Type-Options = "nosniff";
      set resp.http.Referrer-Policy = "same-origin";
    } else if (fastly.ff.visits_this_service == 0 && (resp.status == 200 || resp.status == 206)) {
      if (resp.http.x-flp-fos-cache == "HIT") {
        set resp.http.Cache-Control = "public, max-age=86400";
      } else {
        set resp.http.Cache-Control = "no-cache";
      }
    }
  }
  return(deliver);
}


sub vcl_error {
  if (obj.status >= 624 && obj.status <= 628) {
    set obj.http.Content-Type = "application/json";
    set obj.http.Cache-Control = "private, no-store";
    set obj.http.Vary = "Cookie";
    set obj.http.Strict-Transport-Security = "max-age=31536000; includeSubDomains";
    set obj.http.X-Content-Type-Options = "nosniff";
    set obj.http.Referrer-Policy = "same-origin";
    if (obj.status == 624) {
      set obj.status = 403;
      set obj.response = "Forbidden";
      synthetic {"{"error":"forbidden","detail":"Insufficient role." }"};
    } else if (obj.status == 625) {
      set obj.status = 400;
      set obj.response = "Bad Request";
      synthetic {"{"error":"bad_request","detail":"key is required" }"};
    } else if (obj.status == 626) {
      set obj.status = 400;
      set obj.response = "Bad Request";
      synthetic {"{"error":"bad_request","detail":"key is outside this service's processed files" }"};
    } else if (obj.status == 627) {
      set obj.status = 404;
      set obj.response = "Not Found";
      synthetic {"{"error":"not_found","detail":"File not found" }"};
    } else {
      set obj.status = 502;
      set obj.response = "Bad Gateway";
      synthetic {"{"error":"bad_gateway","detail":"File read failed" }"};
    }
    return(deliver);
  }
  if (obj.status == 621 || obj.status == 622) {
    if (obj.status == 621) {
      set obj.status = 404;
      set obj.response = "Not Found";
    } else {
      set obj.status = 403;
      set obj.response = "Forbidden";
    }
    set obj.http.Cache-Control = "private, no-store";
    synthetic {"Storage request denied"};
    return(deliver);
  }
  if (obj.status == 618 || obj.status == 619) {
    if (obj.status == 618) {
      set obj.http.Retry-After = "900";
    } else {
      set obj.http.Retry-After = "60";
    }
    set obj.status = 429;
    set obj.response = "Too Many Requests";
    set obj.http.Cache-Control = "private, no-store";
    synthetic {"Too many authentication attempts"};
    return(deliver);
  }
  if (obj.status == 620) {
    set obj.status = 503;
    set obj.response = "Service Unavailable";
    set obj.http.Cache-Control = "private, no-store";
    synthetic {"Front not configured"};
    return(deliver);
  }
#FASTLY error
}


sub vcl_log {
#FASTLY log
}


// @scope: miss, pass
// Regional FOS backends (e.g. F_fos_us_east / us-east-1, eu-central-1) sign S3 requests
// at the shield hop; Cross-Origin-Opener-Policy: same-origin and Cross-Origin-Embedder-Policy:
// require-corp are preserved on cross-origin isolated responses.
sub flp_fos_sign {
  if (req.http.x-flp-fos-service &&!req.backend.is_shield) {
    declare local var.fosAccessKey STRING;
    declare local var.fosSecretKey STRING;
    declare local var.fosRegion STRING;
    declare local var.canonicalHeaders STRING;
    declare local var.signedHeaders STRING;
    declare local var.canonicalRequest STRING;
    declare local var.canonicalQuery STRING;
    declare local var.canonicalPath STRING;
    declare local var.stringToSign STRING;
    declare local var.dateStamp STRING;
    declare local var.signature STRING;
    declare local var.scope STRING;

    set var.fosAccessKey = table.lookup(flp_fos_credentials, req.http.x-flp-fos-service ".ak", "");
    set var.fosSecretKey = table.lookup(flp_fos_credentials, req.http.x-flp-fos-service ".sk", "");
    set var.fosRegion = req.http.x-flp-fos-region;
    set bereq.http.host = var.fosRegion ".object.fastlystorage.app";
    set bereq.url = "/" req.http.x-flp-fos-bucket regsub(bereq.url, "^/fos/[A-Za-z0-9]{22}", "");
    set bereq.http.x-amz-content-sha256 = digest.hash_sha256("");
    set bereq.http.x-amz-date = strftime({"%Y%m%dT%H%M00Z"}, now);
    set var.canonicalQuery = querystring.sort(bereq.url.qs);
    set var.canonicalPath = regsuball(urlencode(urldecode(bereq.url.path)), {"%2F"}, "/");
    set bereq.url = var.canonicalPath+if(bereq.url.qs != "", "?" + bereq.url.qs, "");
    set var.dateStamp = strftime({"%Y%m%d"}, now);
    set var.canonicalHeaders = "" "host:" bereq.http.host LF "x-amz-content-sha256:" bereq.http.x-amz-content-sha256 LF
                               "x-amz-date:" bereq.http.x-amz-date LF;
    set var.signedHeaders = "host;x-amz-content-sha256;x-amz-date";
    set var.canonicalRequest = "" req.method LF var.canonicalPath LF var.canonicalQuery LF var.canonicalHeaders LF
                               var.signedHeaders LF digest.hash_sha256("");
    set var.scope = var.dateStamp "/" var.fosRegion "/s3/aws4_request";
    set var.stringToSign = "" "AWS4-HMAC-SHA256" LF bereq.http.x-amz-date LF var.scope LF
                           regsub(digest.hash_sha256(var.canonicalRequest), "^0x", "");
    set var.signature = digest.awsv4_hmac(var.fosSecretKey, var.dateStamp, var.fosRegion, "s3", var.stringToSign);
    set bereq.http.Authorization = "AWS4-HMAC-SHA256 " "Credential=" var.fosAccessKey "/" var.scope ", "
                                   "SignedHeaders=" var.signedHeaders ", " "Signature=" regsub(var.signature, "^0x", "");
    unset bereq.http.Accept;
    unset bereq.http.Accept-Language;
    unset bereq.http.User-Agent;
    unset bereq.http.Fastly-Client-IP;
    unset bereq.http.Cookie;
    unset bereq.http.x-fastly-key;
    unset bereq.http.x-flp-front-key;
    unset bereq.http.x-flp-front-rl;
    unset bereq.http.x-flp-api-file;
    unset bereq.http.x-flp-fos-service;
    unset bereq.http.x-flp-fos-bucket;
    unset bereq.http.x-flp-fos-region;
  }
}
