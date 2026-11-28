// @scope: recv
sub http_requests_redirect_to_https {
  testing.inject_variable("req.is_ssl", false);
  set req.url = "/usage?service=fixture";
  set req.http.Host = "flp.example";
  testing.call_subroutine("vcl_recv");
  assert.error(801);
}


// @scope: error
sub https_redirect_preserves_host_path_and_query {
  set obj.status = 801;
  set req.url = "/usage?service=fixture";
  set req.http.Host = "flp.example";
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 308);
  assert.equal(obj.http.Location, "https://flp.example/usage?service=fixture");
  assert.state(deliver);
}


// @scope: error
sub https_redirect_normalizes_leading_slashes_and_rejects_malformed_host {
  set obj.status = 801;
  set req.url = "///evil.example/path";
  set req.http.Host = "logs.example.com";
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 308);
  assert.equal(obj.http.Location, "https://logs.example.com/evil.example/path");
  assert.state(deliver);

  unset obj.http.Location;
  set obj.status = 801;
  set req.url = "/dashboard?q=1";
  set req.http.Host = "legit.example@evil.example";
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 400);
  assert.equal(obj.response, "Bad Request");
  assert.is_notset(obj.http.Location);
  assert.state(deliver);

  unset obj.http.Location;
  set obj.status = 801;
  set req.url = "/dashboard?q=1";
  unset req.http.Host;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 400);
  assert.equal(obj.response, "Bad Request");
  assert.is_notset(obj.http.Location);
  assert.state(deliver);

  set obj.status = 801;
  set req.url = "/dashboard?q=1";
  set req.http.Host = "logs.example.com";
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 308);
  assert.equal(obj.http.Location, "https://logs.example.com/dashboard?q=1");
  assert.state(deliver);

  unset obj.http.Location;
  set obj.status = 801;
  set req.url = "/\evil.example/path";
  set req.http.Host = "logs.example.com";
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 308);
  assert.equal(obj.http.Location, "https://logs.example.com/evil.example/path");
  assert.state(deliver);

  unset obj.http.Location;
  set obj.status = 801;
  set req.url = "///\evil.example/path";
  set req.http.Host = "logs.example.com";
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 308);
  assert.equal(obj.http.Location, "https://logs.example.com/evil.example/path");
  assert.state(deliver);

  unset obj.http.Location;
  set obj.status = 801;
  set req.url = "@evil.example/path";
  set req.http.Host = "logs.example.com";
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 400);
  assert.equal(obj.response, "Bad Request");
  assert.is_notset(obj.http.Location);
  assert.state(deliver);

  unset obj.http.Location;
  set obj.status = 801;
  set req.url = "*";
  set req.http.Host = "logs.example.com";
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 400);
  assert.equal(obj.response, "Bad Request");
  assert.is_notset(obj.http.Location);
  assert.state(deliver);

  unset obj.http.Location;
  set obj.status = 801;
  set req.url = "/dashboard";
  set req.http.Host = "logs.example.com";
  set req.http.x-flp-fos-service = "spoofed";
  set req.http.x-flp-api-file = "1";
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 308);
  assert.is_notset(req.http.x-flp-fos-service);
  assert.is_notset(req.http.x-flp-api-file);
  assert.state(deliver);
}


// @scope: recv
sub http_redirect_scrubs_spoofed_fos_headers_before_error_801 {
  testing.inject_variable("req.is_ssl", false);
  set req.url = "/usage?service=fixture";
  set req.http.Host = "flp.example";
  set req.http.x-flp-fos-service = "spoofed";
  set req.http.x-flp-api-file = "1";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-fos-service);
  assert.is_notset(req.http.x-flp-api-file);
  assert.error(801);
}


// @scope: recv
sub public_shell_queries_share_a_cache_key {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/usage?campaign=one";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/usage");
  assert.state(lookup);
}


// @scope: recv
sub public_rsc_and_static_asset_queries_share_a_cache_key {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/usage/__next._tree.txt?__rsc=abc";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/usage/__next._tree.txt");
  assert.state(lookup);

  set req.url = "/_next/static/chunks/app.js?v=attacker";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/_next/static/chunks/app.js");
  assert.state(lookup);

  set req.url = "/duckdb/duckdb-eh.wasm?v=attacker";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/duckdb/duckdb-eh.wasm");
  assert.state(lookup);
}


// @scope: recv
sub private_api_queries_keep_the_complete_cache_key {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/api/files?from=a&to=b&service=fixture";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/api/files?from=a&to=b&service=fixture");
  assert.state(pass);
}


// @scope: recv
sub overwrites_untrusted_front_key_and_client_ip {
  testing.inject_variable("req.is_ssl", true);
  set req.http.x-flp-front-key = "attacker";
  set req.http.x-flp-front-rl = "attacker";
  set req.http.Fastly-Client-IP = "192.0.2.99";
  testing.inject_variable("client.ip", "192.0.2.10");
  set req.url = "/api/admin/users";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.x-flp-front-key, "test-front-key-not-a-live-secret");
  assert.equal(req.http.x-flp-front-rl, "1");
  assert.equal(req.http.Fastly-Client-IP, "192.0.2.10");
  assert.state(pass);
}


// @scope: recv
sub static_assets_drop_cookies {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/_next/static/chunks/abc123.js";
  set req.http.Cookie = "flp_session=private";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.Cookie);
  assert.state(lookup);
}


// @scope: recv
sub setup_is_never_cached {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/setup?next=/admin";
  testing.call_subroutine("vcl_recv");
  assert.state(pass);
}


// @scope: recv
sub clock_normalizes_query_string_for_sleep_ttl_caching {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/_tick?b=7";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/_tick");
  assert.equal(req.http.x-flp-tick, "1");
  assert.state(lookup);
}


// @scope: recv
sub clock_continue_with_the_cdn_key_skips_the_cache {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/_tick";
  set req.http.x-flp-continue = "1";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.x-flp-tick, "1");
  assert.state(pass);
}


// @scope: recv
sub clock_continue_without_the_cdn_key_stays_cached {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/_tick";
  set req.http.x-flp-continue = "1";
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");
  assert.state(lookup);
}


// @scope: recv
sub clock_ignores_a_client_sent_pass_marker {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/_tick";
  set req.http.x-flp-tick-pass = "1";
  testing.call_subroutine("vcl_recv");
  assert.state(lookup);
}


// @scope: recv
sub flush_clock_normalizes_query_string_for_sleep_ttl_caching {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/_flush?b=7";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/_flush");
  assert.equal(req.http.x-flp-tick, "1");
  assert.state(lookup);
}


// @scope: recv
sub flush_clock_rejects_other_methods {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/_flush";
  set req.method = "POST";
  testing.call_subroutine("vcl_recv");
  assert.error(405);
}


// @scope: fetch
sub flush_success_is_cached_for_the_one_second_bucket_with_zero_grace {
  set req.url = "/_flush?b=1";
  set req.http.x-flp-tick = "1";
  set beresp.status = 200;
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 1s);
  assert.equal(beresp.grace, 0s);
  assert.state(deliver);
}


// @scope: fetch
sub static_has_year_ttl_and_surrogate_key {
  set req.url = "/_next/static/chunks/abc123.js";
  set beresp.status = 200;
  set beresp.http.Set-Cookie = "must-not-cache=1";
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.ttl, 31536000s);
  assert.equal(beresp.http.Cache-Control, "public, max-age=31536000, immutable");
  assert.is_notset(beresp.http.Set-Cookie);
  assert.equal(beresp.http.Surrogate-Key, "flp-static");
}


// @scope: fetch
sub private_html_cannot_be_cached {
  set req.url = "/custom-role-view";
  set beresp.status = 200;
  set beresp.http.Content-Type = "text/html";
  set beresp.http.Cache-Control = "PrIvAtE, max-age=60";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);
}


// @scope: fetch
sub public_html_has_minute_ttl_and_surrogate_key {
  set req.url = "/sign-in";
  set beresp.status = 200;
  set beresp.http.Content-Type = "text/html; charset=utf-8";
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.ttl, 60s);
  assert.equal(beresp.http.Surrogate-Key, "flp-html");
}


// @scope: fetch
sub public_static_assets_are_cached_like_html {
  set req.url = "/usage.txt";
  set beresp.status = 200;
  set beresp.http.Content-Type = "text/x-component";
  set beresp.http.Cache-Control = "public, max-age=60";
  set beresp.http.Surrogate-Key = "flp-html";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 60s);
  assert.equal(beresp.http.Surrogate-Key, "flp-html");
  assert.state(deliver);
}


// @scope: fetch
sub public_static_not_found_page_is_cached {
  set req.url = "/no-such-page";
  set beresp.status = 404;
  set beresp.http.Content-Type = "text/html";
  set beresp.http.Cache-Control = "public, max-age=60";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 60s);
  assert.state(deliver);
}


// @scope: fetch
sub undeclared_non_html_responses_are_not_cached {
  set req.url = "/no-such-page";
  set beresp.status = 404;
  set beresp.http.Content-Type = "application/json";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);
}


// @scope: deliver
sub strips_internal_auth_failure_marker {
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");
  assert.is_notset(resp.http.x-flp-auth-fail);
}


// @scope: error
sub boxed_auth_returns_retry_after {
  set obj.status = 618;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 429);
  assert.equal(obj.http.Retry-After, "900");
  assert.equal(obj.http.Cache-Control, "private, no-store");
}


// @scope: recv
sub data_api_has_no_blanket_rate_cap {
  testing.inject_variable("req.is_ssl", true);
  testing.inject_variable("client.ip", "192.0.2.55");
  declare local var.count INTEGER;
  set var.count = ratelimit.ratecounter_increment(auth_challenge_rc, "192.0.2.55", 1000);
  set req.url = "/api/log-extents";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(pass);
}


// @scope: recv
sub thirty_challenges_are_allowed {
  testing.inject_variable("req.is_ssl", true);
  testing.inject_variable("client.ip", "192.0.2.55");
  declare local var.count INTEGER;
  set var.count = ratelimit.ratecounter_increment(auth_challenge_rc, "192.0.2.55", 29);
  set req.url = "/api/auth/login/options";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
}


// @scope: recv
sub thirty_first_challenge_is_rejected {
  testing.inject_variable("req.is_ssl", true);
  testing.inject_variable("client.ip", "192.0.2.55");
  declare local var.count INTEGER;
  set var.count = ratelimit.ratecounter_increment(auth_challenge_rc, "192.0.2.55", 30);
  set req.url = "/api/auth/login/options";
  testing.call_subroutine("vcl_recv");
  assert.error(619);
}


// @scope: deliver
sub edge_hop_strips_marker_and_counts_failure() {
  set req.http.Fastly-Client-IP = "192.0.2.57";
  testing.inject_variable("fastly.ff.visits_this_service", "0");
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");
  assert.is_notset(resp.http.x-flp-auth-fail);
  declare local var.count INTEGER;
  set var.count = ratelimit.ratecounter_increment(auth_fail_rc, "192.0.2.57", 0);
  assert.equal(var.count, 1);
}


// @scope: deliver
sub nine_failures_do_not_box_client {
  set req.http.Fastly-Client-IP = "192.0.2.55";
  declare local var.count INTEGER;
  set var.count = ratelimit.ratecounter_increment(auth_fail_rc, "192.0.2.55", 8);
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");
  assert.false(ratelimit.penaltybox_has(auth_fail_pb, "192.0.2.55"));
}


// @scope: deliver
sub tenth_failure_boxes_client {
  set req.http.Fastly-Client-IP = "192.0.2.55";
  declare local var.count INTEGER;
  set var.count = ratelimit.ratecounter_increment(auth_fail_rc, "192.0.2.55", 9);
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");
  assert.true(ratelimit.penaltybox_has(auth_fail_pb, "192.0.2.55"));
}


// @scope: deliver
sub successful_auth_does_not_increment_failure_counter {
  set req.http.Fastly-Client-IP = "192.0.2.55";
  declare local var.count INTEGER;
  set var.count = ratelimit.ratecounter_increment(auth_fail_rc, "192.0.2.55", 9);
  testing.call_subroutine("vcl_deliver");
  assert.false(ratelimit.penaltybox_has(auth_fail_pb, "192.0.2.55"));
}


// @scope: recv
sub absent_dictionary_key_fails_closed {
  testing.inject_variable("req.is_ssl", true);
  testing.table_set(flp_front_auth, "front_key", "");
  set req.http.x-flp-front-key = "attacker";
  testing.call_subroutine("vcl_recv");
  assert.error(620);
}


// @scope: recv
sub boxed_client_never_reaches_compute {
  testing.inject_variable("req.is_ssl", true);
  testing.inject_variable("client.ip", "192.0.2.55");
  ratelimit.penaltybox_add(auth_fail_pb, "192.0.2.55", 15m);
  set req.url = "/api/admin/users";
  testing.call_subroutine("vcl_recv");
  assert.error(618);
}


// @scope: fetch
sub cookie_setting_html_never_becomes_public {
  set req.url = "/sign-in";
  set beresp.status = 200;
  set beresp.http.Content-Type = "text/html";
  set beresp.http.Set-Cookie = "flp_session=private";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);
}


// @scope: fetch
sub tick_success_is_cached_for_the_one_second_bucket_with_zero_grace {
  set req.url = "/_tick?b=1";
  set req.http.x-flp-tick = "1";
  set beresp.status = 200;
  set beresp.http.Set-Cookie = "flp_session=unexpected";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 1s);
  assert.equal(beresp.grace, 0s);
  assert.is_notset(beresp.http.Set-Cookie);
  assert.state(deliver);
}


// @scope: fetch
sub tick_honors_compute_sleep_ttl_and_strips_internal_and_security_headers {
  set req.url = "/_tick";
  set req.http.x-flp-tick = "1";
  set beresp.status = 200;
  set beresp.http.x-flp-tick-ttl = "47";
  set beresp.http.Vary = "Cookie";
  set beresp.http.content-security-policy = "default-src 'self'";
  set beresp.http.strict-transport-security = "max-age=31536000; includeSubDomains";
  set beresp.http.x-content-type-options = "nosniff";
  set beresp.http.referrer-policy = "same-origin";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 47s);
  assert.equal(beresp.grace, 0s);
  assert.equal(beresp.http.Surrogate-Key, "flp-clock");
  // Falco 2.0.1 hardcodes fastly.ff.visits_this_service = 1 in vcl_fetch (shield hop),
  // where x-flp-tick-ttl is preserved for the edge POP; visits_this_service == 0 edge
  // stripping is verified in test_shield_safe_vcl_contracts_falco_cannot_inject.
  assert.equal(beresp.http.x-flp-tick-ttl, "47");
  assert.is_notset(beresp.http.Vary);
  assert.is_notset(beresp.http.content-security-policy);
  assert.is_notset(beresp.http.strict-transport-security);
  assert.is_notset(beresp.http.x-content-type-options);
  assert.is_notset(beresp.http.referrer-policy);
  assert.state(deliver);
}


// @scope: fetch
sub tick_subtracts_shield_age_from_edge_sleep_ttl {
  set req.url = "/_tick";
  set req.http.x-flp-tick = "1";
  set beresp.status = 200;
  set beresp.http.x-flp-tick-ttl = "47";
  set beresp.http.Age = "12";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 35s);
  assert.equal(beresp.http.x-flp-tick-ttl, "47");
  assert.state(deliver);
}


// @scope: fetch
sub tick_clamps_nonpositive_sleep_ttl_to_one_second {
  set req.url = "/_tick";
  set req.http.x-flp-tick = "1";
  set beresp.status = 200;
  set beresp.http.x-flp-tick-ttl = "0";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 1s);
  assert.equal(beresp.grace, 0s);
  assert.equal(beresp.http.x-flp-tick-ttl, "0");
  assert.state(deliver);
}


// @scope: fetch
sub tick_failures_are_collapsed_for_the_bucket {
  set req.url = "/_tick?b=1";
  set req.http.x-flp-tick = "1";
  set beresp.status = 500;
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 1s);
  assert.equal(beresp.grace, 0s);
}


// @scope: recv
sub static_export_txt_and_html_variants_pass_and_enforce_penalty_box {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/admin.txt";
  testing.call_subroutine("vcl_recv");
  assert.state(pass);
  testing.inject_variable("client.ip", "192.0.2.55");
  ratelimit.penaltybox_add(auth_fail_pb, "192.0.2.55", 15m);
  set req.url = "/setup.txt";
  testing.call_subroutine("vcl_recv");
  assert.error(618);
}


// @scope: fetch
sub static_export_protected_variants_are_never_cached {
  set req.url = "/setup.html";
  set beresp.status = 200;
  set beresp.http.Content-Type = "text/html";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);
}


// @scope: recv
sub duckdb_assets_drop_cookies_and_are_looked_up {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/duckdb/1.33.1-dev57.0/duckdb-eh.wasm";
  set req.http.Cookie = "flp_session=private";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.Cookie);
  assert.state(lookup);
}


// @scope: recv
sub accept_encoding_prefers_brotli_then_gzip_for_precompressed_assets {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/_next/static/chunks/abc123.js";
  set req.http.Fastly-Orig-Accept-Encoding = "gzip, deflate, br, zstd";
  set req.http.Accept-Encoding = "gzip";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.Accept-Encoding, "br, gzip");
  assert.equal(req.http.x-flp-gzip, "1");
  set req.http.Fastly-Orig-Accept-Encoding = "br";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.Accept-Encoding, "br");
  set req.http.Fastly-Orig-Accept-Encoding = "gzip, deflate";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.Accept-Encoding, "gzip");
  set req.http.Fastly-Orig-Accept-Encoding = "deflate";
  set req.http.x-flp-gzip = "1";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.Accept-Encoding);
  assert.is_notset(req.http.x-flp-gzip);
}


// @scope: recv
sub fos_reads_keep_the_platform_accept_encoding {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/processed/request/a.parquet";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  set req.http.Fastly-Orig-Accept-Encoding = "gzip, br";
  set req.http.Accept-Encoding = "gzip";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.Accept-Encoding, "gzip");
}


// @scope: fetch
sub duckdb_versioned_core_is_immutable_for_a_year {
  set req.url = "/duckdb/1.33.1-dev57.0/duckdb-eh.wasm";
  set beresp.status = 200;
  set beresp.http.Content-Type = "application/wasm";
  set beresp.http.Set-Cookie = "must-not-cache=1";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 31536000s);
  assert.equal(beresp.http.Cache-Control, "public, max-age=31536000, immutable");
  assert.equal(beresp.http.Content-Type, "application/wasm");
  assert.is_notset(beresp.http.Set-Cookie);
  assert.equal(beresp.http.Surrogate-Key, "flp-static");
  assert.state(deliver);

  set req.url = "/duckdb/1.33.1-dev57.0/duckdb-browser-eh.worker.js";
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.http.Cache-Control, "public, max-age=31536000, immutable");
}


// @scope: recv
sub duckdb_wasm_requests_prefer_brotli {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/duckdb/1.33.1-dev57.0/duckdb-eh.wasm";
  set req.http.Fastly-Orig-Accept-Encoding = "gzip, deflate, br, zstd";
  set req.http.Accept-Encoding = "gzip";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.Accept-Encoding, "br, gzip");
}


// @scope: fetch
sub duckdb_has_day_ttl_and_revalidates {
  set req.url = "/duckdb/1.33.1-dev57.0/extensions/v1.5.4/wasm_eh/parquet.duckdb_extension.wasm";
  set beresp.status = 200;
  set beresp.http.Set-Cookie = "must-not-cache=1";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 86400s);
  assert.equal(
    beresp.http.Cache-Control,
    "public, max-age=86400, must-revalidate"
  );
  assert.is_notset(beresp.http.Set-Cookie);
  assert.equal(beresp.http.Surrogate-Key, "flp-static");
  assert.state(deliver);
}


// @scope: fetch
sub geo_has_day_ttl_and_revalidates {
  set req.url = "/geo/world.topo.json";
  set beresp.status = 200;
  set beresp.http.Set-Cookie = "must-not-cache=1";
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.ttl, 86400s);
  assert.equal(
    beresp.http.Cache-Control,
    "public, max-age=86400, must-revalidate"
  );
  assert.is_notset(beresp.http.Set-Cookie);
  assert.equal(beresp.http.Surrogate-Key, "flp-static");
}


// @scope: recv
sub geo_asn_names_strips_cookie_query_and_normalizes_encoding_for_lookup {
  testing.inject_variable("req.is_ssl", true);
  set req.http.Cookie = "flp_session=user-session";
  set req.http.Fastly-Orig-Accept-Encoding = "gzip, deflate, br";
  set req.url = "/geo/asn-names.tsv?v=1";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/geo/asn-names.tsv");
  assert.is_notset(req.http.Cookie);
  assert.equal(req.http.Accept-Encoding, "br, gzip");
  assert.state(lookup);
}


// @scope: fetch
sub geo_asn_names_from_compute_is_cached_in_vcl {
  set req.url = "/geo/asn-names.tsv";
  set beresp.status = 200;
  set beresp.http.Content-Type = "text/tab-separated-values; charset=utf-8";
  set beresp.http.Content-Encoding = "br";
  set beresp.http.Vary = "Accept-Encoding";
  set beresp.http.Set-Cookie = "must-not-cache=1";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 86400s);
  assert.equal(
    beresp.http.Cache-Control,
    "public, max-age=86400, must-revalidate"
  );
  assert.equal(beresp.http.Vary, "Accept-Encoding");
  assert.is_notset(beresp.http.Set-Cookie);
  assert.equal(beresp.http.Surrogate-Key, "flp-static");
  assert.state(deliver);
}


// @scope: fetch
sub maplibre_worker_has_day_ttl_and_revalidates {
  set req.url = "/maplibre-gl-worker.mjs";
  set beresp.status = 200;
  set beresp.http.Set-Cookie = "must-not-cache=1";
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.ttl, 86400s);
  assert.equal(
    beresp.http.Cache-Control,
    "public, max-age=86400, must-revalidate"
  );
  assert.is_notset(beresp.http.Set-Cookie);
  assert.equal(beresp.http.Surrogate-Key, "flp-static");
}


// @scope: fetch
sub large_json_api_responses_are_gzipped_but_still_passed {
  set req.url = "/api/dashboard/summary";
  set req.http.x-flp-gzip = "1";
  set beresp.status = 200;
  set beresp.http.Content-Type = "application/json";
  set beresp.http.x-compress-hint = "on";
  set beresp.http.Vary = "Cookie";
  testing.call_subroutine("vcl_fetch");
  assert.is_notset(beresp.http.x-compress-hint);
  assert.equal(beresp.http.Vary, "Cookie, Accept-Encoding");
  assert.true(beresp.gzip);
  assert.false(beresp.cacheable);
  assert.state(pass);
}


// @scope: fetch
sub unhinted_json_and_non_gzip_clients_are_not_compressed {
  set req.url = "/api/me";
  set req.http.x-flp-gzip = "1";
  set beresp.status = 200;
  set beresp.http.Content-Type = "application/json";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.gzip);
  set beresp.http.x-compress-hint = "on";
  unset req.http.x-flp-gzip;
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.gzip);
}


// @scope: fetch
sub parquet_and_encoded_responses_are_never_compressed {
  set req.url = "/api/query/file";
  set req.http.x-flp-gzip = "1";
  set beresp.status = 200;
  set beresp.http.Content-Type = "application/vnd.apache.parquet";
  set beresp.http.x-compress-hint = "on";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.gzip);
  set beresp.http.Content-Type = "application/json";
  set beresp.http.Content-Encoding = "gzip";
  set beresp.http.x-compress-hint = "on";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.gzip);
}


// @scope: fetch
sub fos_parquet_is_never_compressed {
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/processed/request/a.parquet";
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.http.x-flp-gzip = "1";
  set beresp.status = 200;
  set beresp.http.Content-Type = "application/octet-stream";
  set beresp.http.x-compress-hint = "on";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.gzip);
}


// @scope: deliver
sub ipv6_same_64_subnet_shares_failure_counter_and_penalty_box {
  testing.inject_variable("fastly.ff.visits_this_service", "0");
  declare local var.i INTEGER;
  set req.http.Fastly-Client-IP = "2001:db8:abcd:1234::1";
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");
  set var.i = ratelimit.ratecounter_increment(auth_fail_rc, "v6:536939960:2882343476", 8);
  assert.false(ratelimit.penaltybox_has(auth_fail_pb, "v6:536939960:2882343476"));

  set req.http.Fastly-Client-IP = "2001:0db8:abcd:1234:ffff:eeee:dddd:cccc";
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");
  assert.true(ratelimit.penaltybox_has(auth_fail_pb, "v6:536939960:2882343476"));
  assert.false(ratelimit.penaltybox_has(auth_fail_pb, "v6:536939960:2882343477"));
}


// @scope: recv
sub penalty_box_blocks_fos_parquet_and_signin_before_rewrites_while_keeping_public_and_clock_safe {
  testing.inject_variable("req.is_ssl", true);
  ratelimit.penaltybox_add(auth_fail_pb, "v6:536939960:2882343476", 15m);
  testing.inject_variable("client.ip", "2001:0db8:abcd:1234:dead:beef:cafe:99");

  set req.url = "/sign-in";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  set req.url = "/sign-in.txt";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  set req.url = "/invite";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/processed/request/a.parquet";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  unset req.http.x-fastly-key;
  set req.http.Cookie = "flp_session=test-session";
  set req.url = "/fos/pq/goal2-live-20261029/processed/request/a.parquet";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  set req.url = "/goal2-live-20261029/processed/request/a.parquet";
  testing.call_subroutine("vcl_recv");
  assert.error(618);
  assert.equal(req.url, "/goal2-live-20261029/processed/request/a.parquet");

  // Multi-slash paths (in falco `set req.url`, //// yields a multi-slash req.url/req.url.path)
  set req.url = "////fos/AAAAAAAAAAAAAAAAAAAAAA/processed/request/a.parquet";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  set req.url = "////api/file?key=processed%2Frequest%2Fa.parquet";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  set req.url = "////sign-in";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  set req.url = "////tenant/logs/processed/request/a.parquet";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  unset req.http.Cookie;
  set req.url = "/";
  testing.call_subroutine("vcl_recv");
  assert.not_error();

  set req.url = "/version";
  testing.call_subroutine("vcl_recv");
  assert.not_error();

  set req.url = "/_next/static/chunks/app.js";
  testing.call_subroutine("vcl_recv");
  assert.not_error();

  set req.url = "/_tick";
  testing.call_subroutine("vcl_recv");
  assert.not_error();

  set req.url = "/_flush";
  testing.call_subroutine("vcl_recv");
  assert.not_error();

  testing.inject_variable("client.ip", "2001:db8:abcd:1235::1");
  set req.url = "/sign-in";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
}


// @scope: recv
sub ipv6_same_64_subnet_shares_challenge_rate_limit {
  testing.inject_variable("req.is_ssl", true);
  declare local var.count INTEGER;
  set var.count = ratelimit.ratecounter_increment(auth_challenge_rc, "v6:536939960:2882343476", 30);
  testing.inject_variable("client.ip", "2001:0db8:abcd:1234:1111:2222:3333:4444");
  set req.url = "/api/auth/login/options";
  testing.call_subroutine("vcl_recv");
  assert.error(619);
}


// @scope: error
sub edge_auth_denials_emit_auth_fail_marker_and_clear_bad_cdn_cookies {
  set obj.status = 621;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 404);
  assert.equal(obj.http.x-flp-auth-fail, "1");

  unset obj.http.x-flp-auth-fail;
  set obj.status = 622;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 403);
  assert.equal(obj.http.x-flp-auth-fail, "1");

  unset obj.http.x-flp-auth-fail;
  set obj.status = 624;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 403);
  assert.equal(obj.http.x-flp-auth-fail, "1");

  unset obj.http.x-flp-auth-fail;
  set obj.status = 629;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 403);
  assert.equal(obj.http.x-flp-auth-fail, "1");
  assert.true(obj.http.Set-Cookie ~ "Max-Age=0");

  unset obj.http.x-flp-auth-fail;
  unset obj.http.Set-Cookie;
  set obj.status = 626;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 400);
  assert.is_notset(obj.http.x-flp-auth-fail);
}


// @scope: recv
sub sec_coex_vcl_31a_strips_unauthenticated_clock_continue_on_tick_and_flush {
  // Unauthenticated HTTPS /_tick with spoofed x-flp-continue: 20
  testing.inject_variable("req.is_ssl", true);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.url = "/_tick";
  set req.http.x-flp-continue = "20";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-continue);
  assert.is_notset(req.http.x-flp-tick-pass);
  assert.state(lookup);

  // Unauthenticated HTTPS /_flush with spoofed x-flp-continue: 20
  set req.url = "/_flush";
  set req.http.x-flp-continue = "20";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-continue);
  assert.is_notset(req.http.x-flp-tick-pass);
  assert.state(lookup);

  // Wrong x-fastly-key on /_tick and /_flush over HTTPS
  set req.url = "/_tick";
  set req.http.x-flp-continue = "20";
  set req.http.x-fastly-key = "wrong-cdn-key";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-continue);
  assert.is_notset(req.http.x-flp-tick-pass);
  assert.state(lookup);

  set req.url = "/_flush";
  set req.http.x-flp-continue = "20";
  set req.http.x-fastly-key = "wrong-cdn-key";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-continue);
  assert.is_notset(req.http.x-flp-tick-pass);
  assert.state(lookup);

  // Plain HTTP (!req.is_ssl) must NEVER honor x-flp-continue even with valid cdn_key
  testing.inject_variable("req.is_ssl", false);
  set req.url = "/_tick";
  set req.http.x-flp-continue = "20";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-continue);
  assert.is_notset(req.http.x-flp-tick-pass);
  assert.state(lookup);

  set req.url = "/_flush";
  set req.http.x-flp-continue = "20";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-continue);
  assert.is_notset(req.http.x-flp-tick-pass);
  assert.state(lookup);

  // Non-clock GET / must strip x-flp-continue even over HTTPS with valid cdn_key
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/";
  set req.http.x-flp-continue = "20";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-continue);
  assert.is_notset(req.http.x-flp-tick-pass);
  assert.state(lookup);
}


// @scope: recv
sub sec_coex_vcl_31a_shield_hop_strips_unauth_continue_and_preserves_auth_continue {
  // Shield hop (visits_this_service == 1) without valid x-fastly-key must strip x-flp-continue & x-flp-tick-pass
  testing.inject_variable("req.is_ssl", true);
  testing.inject_variable("fastly.ff.visits_this_service", 1);
  set req.url = "/_tick";
  set req.http.x-flp-continue = "20";
  set req.http.x-flp-tick-pass = "1";
  unset req.http.x-fastly-key;
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-continue);
  assert.is_notset(req.http.x-flp-tick-pass);
  assert.state(lookup);

  // Shield hop over plain HTTP must strip x-flp-continue even with valid x-fastly-key
  testing.inject_variable("req.is_ssl", false);
  set req.url = "/_flush";
  set req.http.x-flp-continue = "20";
  set req.http.x-flp-tick-pass = "1";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-continue);
  assert.is_notset(req.http.x-flp-tick-pass);
  assert.state(lookup);

  // Authenticated HTTPS control at edge (visits_this_service == 0) preserves x-flp-continue & sets x-flp-tick-pass
  testing.inject_variable("req.is_ssl", true);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.url = "/_tick";
  set req.http.x-flp-continue = "2";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.x-flp-continue, "2");
  assert.equal(req.http.x-flp-tick-pass, "1");
  assert.state(pass);

  // Authenticated HTTPS control at shield (visits_this_service == 1) on /_flush preserves x-flp-continue & x-flp-tick-pass
  testing.inject_variable("fastly.ff.visits_this_service", 1);
  set req.url = "/_flush";
  set req.http.x-flp-continue = "2";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.x-flp-continue, "2");
  assert.equal(req.http.x-flp-tick-pass, "1");
  assert.state(pass);
}


// @scope: deliver
sub sec22_edge_deliver_strips_internal_headers_and_surrogate_key {
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set resp.status = 200;
  set resp.http.Surrogate-Key = "flp-html svc-123";
  set resp.http.x-flp-auth-fail = "1";
  set resp.http.x-flp-fos-bucket = "fla-0123456789abcdef01234567";
  set resp.http.x-flp-fos-region = "us-east-1";
  set resp.http.x-flp-fos-service = "svc123";
  set resp.http.x-flp-tick-ttl = "10";
  set resp.http.x-flp-worker-key = "internal-key";
  set resp.http.x-flp-clock-secret = "internal-clock";
  set resp.http.x-flp-shell = "admin";
  set resp.http.x-flp-log-dropped = "3";
  set resp.http.x-compress-hint = "on";
  testing.call_subroutine("vcl_deliver");
  assert.is_notset(resp.http.Surrogate-Key);
  assert.is_notset(resp.http.x-flp-auth-fail);
  assert.is_notset(resp.http.x-flp-fos-bucket);
  assert.is_notset(resp.http.x-flp-fos-region);
  assert.is_notset(resp.http.x-flp-fos-service);
  assert.is_notset(resp.http.x-flp-tick-ttl);
  assert.is_notset(resp.http.x-flp-worker-key);
  assert.is_notset(resp.http.x-flp-clock-secret);
  assert.is_notset(resp.http.x-flp-shell);
  assert.is_notset(resp.http.x-flp-log-dropped);
  assert.is_notset(resp.http.x-compress-hint);
}


// @scope: deliver
sub idempotent_read_api_deliver_restarts_once_on_5xx_only {
  set req.method = "GET";
  unset req.http.x-flp-fos-service;
  set req.url = "/api/files?from=a&to=b&view=summary&fork=1";
  set resp.status = 503;
  testing.call_subroutine("vcl_deliver");
  assert.state(restart);

  set req.method = "HEAD";
  set req.url = "/api/sync-status";
  set resp.status = 502;
  testing.call_subroutine("vcl_deliver");
  assert.state(restart);

  set req.method = "GET";
  set req.url = "/api/files?from=a&to=b&view=summary&fork=1";
  set resp.status = 200;
  testing.call_subroutine("vcl_deliver");
  assert.state(deliver);

  set req.method = "POST";
  set req.url = "/api/files";
  set resp.status = 503;
  testing.call_subroutine("vcl_deliver");
  assert.state(deliver);

  set req.method = "GET";
  set req.url = "/api/admin/jobs/1/step";
  set resp.status = 503;
  testing.call_subroutine("vcl_deliver");
  assert.state(deliver);
}


// @scope: error
sub idempotent_read_api_error_restarts_once_on_5xx_excluding_620 {
  set req.method = "GET";
  unset req.http.x-flp-fos-service;
  set req.url = "/api/files?from=a&to=b&view=summary&fork=1";
  set obj.status = 503;
  testing.call_subroutine("vcl_error");
  assert.state(restart);

  set req.method = "POST";
  set obj.status = 503;
  testing.call_subroutine("vcl_error");
  assert.not_restart();

  set req.method = "GET";
  set req.url = "/api/admin/jobs/1/step";
  set obj.status = 503;
  testing.call_subroutine("vcl_error");
  assert.not_restart();

  set req.url = "/api/files";
  set obj.status = 620;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 503);
  assert.not_restart();
}


// @scope: fetch
sub static_asset_non_200_responses_are_not_cached {
  set req.method = "GET";
  set req.url = "/_next/static/chunks/main.js";
  set beresp.status = 404;
  set beresp.http.Cache-Control = "public, max-age=60";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);

  set req.url = "/duckdb/1.33.1/duckdb-eh.wasm";
  set beresp.status = 404;
  set beresp.http.Cache-Control = "public, max-age=60";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);
}
