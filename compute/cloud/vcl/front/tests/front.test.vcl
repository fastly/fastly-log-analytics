// @scope: recv
sub overwrites_untrusted_front_key_and_client_ip {
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
  set req.url = "/_next/static/chunks/abc123.js";
  set req.http.Cookie = "flp_session=private";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.Cookie);
  assert.state(lookup);
}


// @scope: recv
sub setup_is_never_cached {
  set req.url = "/setup?next=/admin";
  testing.call_subroutine("vcl_recv");
  assert.state(pass);
}


// @scope: recv
sub clock_normalizes_query_string_for_sleep_ttl_caching {
  set req.url = "/_tick?b=7";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/_tick");
  assert.equal(req.http.x-flp-tick, "1");
  assert.state(lookup);
}


// @scope: recv
sub flush_clock_normalizes_query_string_for_sleep_ttl_caching {
  set req.url = "/_flush?b=7";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/_flush");
  assert.equal(req.http.x-flp-tick, "1");
  assert.state(lookup);
}


// @scope: recv
sub flush_clock_rejects_other_methods {
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
  testing.inject_variable("client.ip", "192.0.2.55");
  declare local var.count INTEGER;
  set var.count = ratelimit.ratecounter_increment(auth_challenge_rc, "192.0.2.55", 29);
  set req.url = "/api/auth/login/options";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
}


// @scope: recv
sub thirty_first_challenge_is_rejected {
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
  testing.table_set(flp_front_auth, "front_key", "");
  set req.http.x-flp-front-key = "attacker";
  testing.call_subroutine("vcl_recv");
  assert.error(620);
}


// @scope: recv
sub boxed_client_never_reaches_compute {
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
  set req.url = "/duckdb/1.33.1-dev57.0/duckdb-eh.wasm";
  set req.http.Cookie = "flp_session=private";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.Cookie);
  assert.state(lookup);
}


// @scope: recv
sub accept_encoding_prefers_brotli_then_gzip_for_precompressed_assets {
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
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/processed/request/a.parquet";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  set req.http.Fastly-Orig-Accept-Encoding = "gzip, br";
  set req.http.Accept-Encoding = "gzip";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.Accept-Encoding, "gzip");
}


// @scope: fetch
sub duckdb_has_year_ttl_and_surrogate_key {
  set req.url = "/duckdb/1.33.1-dev57.0/duckdb-eh.wasm";
  set beresp.status = 200;
  set beresp.http.Set-Cookie = "must-not-cache=1";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 31536000s);
  assert.equal(beresp.http.Cache-Control, "public, max-age=31536000, immutable");
  assert.is_notset(beresp.http.Set-Cookie);
  assert.equal(beresp.http.Surrogate-Key, "flp-static");
  assert.state(deliver);
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
