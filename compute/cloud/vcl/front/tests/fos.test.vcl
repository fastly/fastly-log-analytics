// @scope: recv
sub fos_requires_header_auth_even_for_cached_objects {
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/raw/request/a.gz?key=test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.error(622);
}


// @scope: recv
sub fos_unknown_service_is_not_an_app_request {
  set req.url = "/fos/BBBBBBBBBBBBBBBBBBBBBB/raw/a.gz";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.error(621);
}


// @scope: recv
sub fos_auth_and_registry_fail_closed {
  testing.table_set(flp_front_auth, "cdn_key", "");
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/raw/a.gz";
  set req.http.x-fastly-key = "";
  testing.call_subroutine("vcl_recv");
  assert.error(622);
}


// @scope: recv
sub fos_drops_secrets_and_canonicalizes_cache_keys {
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/raw/a.gz?key=private&max-keys=10&prefix=a&unexpected=secret";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  set req.http.Cookie = "flp_session=private";
  set req.http.x-flp-fos-region = "attacker";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.equal(req.url, "/fos/AAAAAAAAAAAAAAAAAAAAAA/raw/a.gz?max-keys=10&prefix=a");
  assert.equal(req.http.x-flp-fos-region, "us-east-1");
  assert.equal(req.http.x-flp-fos-bucket, "fos-test-logs");
  assert.is_notset(req.http.Cookie);
  assert.is_notset(req.http.x-fastly-key);
  assert.is_notset(req.http.x-flp-front-key);
  assert.true(req.enable_segmented_caching);
  assert.equal(segmented_caching.block_size, 20971520);
  assert.state(pass);
}


// @scope: recv
sub client_cannot_route_app_requests_to_storage {
  set req.url = "/sign-in";
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.http.x-flp-fos-bucket = "private";
  set req.http.x-flp-fos-region = "us-east-1";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-fos-service);
  assert.is_notset(req.http.x-flp-fos-bucket);
  assert.is_notset(req.http.x-flp-fos-region);
}


// @scope: recv
sub fos_does_not_expose_writes {
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/raw/a.gz";
  set req.method = "DELETE";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.error(405);
}


// @scope: miss
sub fos_signing_replaces_authorization_and_strips_app_credentials {
  testing.fixed_time("2026-10-29 12:34:56");
  set req.method = "GET";
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.http.x-flp-fos-bucket = "fos-test-logs";
  set req.http.x-flp-fos-region = "us-east-1";
  set bereq.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/raw/request/a b.gz?prefix=a";
  set bereq.http.Authorization = "Bearer must-not-reach-fos";
  set bereq.http.x-flp-front-key = "must-not-reach-fos";
  set bereq.http.Cookie = "flp_session=private";
  testing.call_subroutine("vcl_miss");
  assert.equal(bereq.url, "/fos-test-logs/raw/request/a b.gz?prefix=a");
  assert.equal(bereq.http.host, "us-east-1.object.fastlystorage.app");
  assert.equal(bereq.http.x-amz-date, "20261029T123400Z");
  assert.equal(bereq.http.Authorization, "AWS4-HMAC-SHA256 Credential=fixture-access/20261029/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=7b52115043fcf7e92968a9510f1172fdde5039d665715e47823fbb2c23802efb");
  assert.is_notset(bereq.http.x-flp-front-key);
  assert.is_notset(bereq.http.Cookie);
  assert.is_notset(bereq.http.x-flp-fos-bucket);
}


// @scope: fetch
sub fos_errors_and_listings_are_never_cached {
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/?list-type=2";
  set beresp.status = 200;
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);
}


// @scope: recv
sub immutable_fos_objects_can_use_segmented_cache {
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/processed/request/year=2026/month=10/day=30/hour=01/window=a.parquet";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.equal(req.http.x-flp-fos-service, "AAAAAAAAAAAAAAAAAAAAAA");
}


// @scope: recv
sub fos_write_once_meta_objects_are_looked_up {
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/meta/cron-runs/dt=2026-10-29/tick-1.json";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
}


// @scope: recv
sub fos_listings_always_bypass_cache {
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/?list-type=2&prefix=raw%2F";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(pass);
}


// @scope: recv
sub fos_versioned_object_reads_are_looked_up {
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/processed/request/a.parquet?versionId=v1";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
}


// @scope: fetch
sub fos_versioned_object_fetch_is_cacheable {
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/processed/request/a.parquet?versionId=v1";
  set beresp.status = 200;
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.state(deliver);
}


// @scope: recv
sub corrupted_registry_cannot_select_an_arbitrary_origin {
  testing.table_set(flp_fos_buckets, "AAAAAAAAAAAAAAAAAAAAAA", "fos-test-logs|attacker.example");
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/raw/a.gz";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.error(621);
}


// @scope: hash
sub reprovisioned_bucket_does_not_reuse_previous_object_cache {
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/raw/a.gz";
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.http.x-flp-fos-bucket = "fos-first";
  set req.http.x-flp-fos-region = "us-east-1";
  testing.call_subroutine("vcl_hash");
  assert.contains(req.hash, "fos-first");
  assert.contains(req.hash, "us-east-1");
}


// @scope: deliver
sub fos_delivery_reports_synthetic_hits_as_misses() {
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  testing.inject_variable("fastly_info.state", "HIT-SYNTH");
  testing.call_subroutine("vcl_deliver");
  assert.equal(resp.http.x-flp-fos-cache, "MISS");
}


// @scope: deliver
sub fos_delivery_reports_pass_as_miss() {
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  testing.inject_variable("fastly_info.state", "PASS");
  testing.call_subroutine("vcl_deliver");
  assert.equal(resp.http.x-flp-fos-cache, "MISS");
}


// @scope: deliver
sub non_fos_delivery_has_no_cache_header() {
  testing.inject_variable("fastly_info.state", "HIT");
  testing.call_subroutine("vcl_deliver");
  assert.is_notset(resp.http.x-flp-fos-cache);
}


// @scope: fetch
sub fos_ops_keys_carry_family_surrogate_keys() {
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/processed/ops/cron_runs/dt=2026-10-30/a.parquet";
  set beresp.status = 200;
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.http.Surrogate-Key, "flp-fos flp-fos-AAAAAAAAAAAAAAAAAAAAAA flp-fos-ops-cron_runs");
}


// @scope: fetch
sub fos_request_keys_carry_request_surrogate_key() {
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/processed/request/dt=2026-10-30/a.parquet";
  set beresp.status = 200;
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.http.Surrogate-Key, "flp-fos flp-fos-AAAAAAAAAAAAAAAAAAAAAA flp-fos-request");
}


// @scope: recv
sub fos_immutable_raw_objects_are_looked_up() {
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/raw/request/a.gz";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
}


// @scope: recv
sub fos_prefixed_raw_objects_are_looked_up() {
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/logs/edge/raw/rum/a.gz";
  set req.http.x-fastly-key = "test-cdn-key-not-a-live-secret";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
}


// @scope: fetch
sub fos_raw_fetch_is_cached_under_the_service_purge_key() {
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/raw/request/a.gz";
  set beresp.status = 200;
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 86400s);
  assert.equal(beresp.http.Surrogate-Key, "flp-fos flp-fos-AAAAAAAAAAAAAAAAAAAAAA");
  assert.state(deliver);
}


// @scope: fetch
sub fos_processed_ttl_never_exceeds_shortest_retention() {
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/processed/request/year=2026/month=10/day=30/hour=01/window=a.parquet";
  set beresp.status = 200;
  set beresp.http.Cache-Control = "public, max-age=31536000, stale-if-error=604800";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 86400s);
  assert.equal(beresp.stale_if_error, 0s);
  assert.equal(beresp.stale_while_revalidate, 0s);
  assert.state(deliver);
}


// @scope: fetch
sub fos_bundles_and_prefixed_keys_carry_the_service_purge_key() {
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/logs/edge/processed/summary_bundle/request/year=2026/month=10/day=30/day_bundle=c1-n5-h0.parquet";
  set beresp.status = 200;
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.http.Surrogate-Key, "flp-fos flp-fos-AAAAAAAAAAAAAAAAAAAAAA");
  assert.equal(beresp.ttl, 86400s);
}


// @scope: fetch
sub fos_prefixed_ops_keys_carry_family_surrogate_keys() {
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.url = "/fos/AAAAAAAAAAAAAAAAAAAAAA/logs/edge/processed/ops/cron_runs/year=2026/month=10/day=30/hour=01/a.parquet";
  set beresp.status = 200;
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.http.Surrogate-Key, "flp-fos flp-fos-AAAAAAAAAAAAAAAAAAAAAA flp-fos-ops-cron_runs");
}


// @scope: recv
sub api_file_with_valid_cdn_cookie_rewrites_directly_to_fos_cache_lookup() {
  testing.fixed_time("2026-10-29 12:34:56");
  declare local var.shash STRING;
  declare local var.payload STRING;
  declare local var.sig STRING;
  set var.shash = substr(digest.hash_sha256("test-session"), 0, 16);
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~admin~1893456000~" + var.shash + "~logs/edge/";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.url = "/api/file?key=logs%2Fedge%2Fprocessed%2Fops%2Fcron_runs%2Fyear%3D2026%2Fmonth%3D10%2Fday%3D31%2Fcompact%3D29891370.parquet";
  set req.http.Cookie = "flp_session=test-session; flp_cdn=" + var.payload + "~" + var.sig;
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.equal(req.url, "/fos/AAAAAAAAAAAAAAAAAAAAAA/logs/edge/processed/ops/cron_runs/year=2026/month=10/day=31/compact=29891370.parquet");
  assert.equal(req.http.x-flp-fos-service, "AAAAAAAAAAAAAAAAAAAAAA");
  assert.equal(req.http.x-flp-fos-bucket, "fos-test-logs");
  assert.equal(req.http.x-flp-fos-region, "us-east-1");
  assert.equal(req.http.x-flp-api-file, "1");
  assert.is_notset(req.http.Cookie);
  assert.true(req.enable_segmented_caching);
}


// @scope: recv
sub api_file_denies_analyst_on_ops_and_allows_request_dataset() {
  testing.fixed_time("2026-10-29 12:34:56");
  declare local var.shash STRING;
  declare local var.payload STRING;
  declare local var.sig STRING;
  set var.shash = substr(digest.hash_sha256("analyst-session"), 0, 16);
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~analyst~1893456000~" + var.shash + "~";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.url = "/api/file?key=processed%2Frequest%2Fyear%3D2026%2Fmonth%3D10%2Fday%3D31%2Fwindow%3D1.parquet";
  set req.http.Cookie = "flp_session=analyst-session; flp_cdn=" + var.payload + "~" + var.sig;
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.equal(req.url, "/fos/AAAAAAAAAAAAAAAAAAAAAA/processed/request/year=2026/month=10/day=31/window=1.parquet");

  set req.url = "/api/file?key=processed%2Fops%2Fcron_runs%2Fyear%3D2026%2Fmonth%3D10%2Fday%3D31%2Fwindow%3D1.parquet";
  set req.http.Cookie = "flp_session=analyst-session; flp_cdn=" + var.payload + "~" + var.sig;
  testing.call_subroutine("vcl_recv");
  assert.error(624);
}


// @scope: recv
sub api_file_denies_no_pii_and_invalid_keys() {
  testing.fixed_time("2026-10-29 12:34:56");
  declare local var.shash STRING;
  declare local var.payload STRING;
  declare local var.sig STRING;
  set var.shash = substr(digest.hash_sha256("nopii-session"), 0, 16);
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~no_pii~1893456000~" + var.shash + "~";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.url = "/api/file?key=processed%2Frequest%2Fa.parquet";
  set req.http.Cookie = "flp_session=nopii-session; flp_cdn=" + var.payload + "~" + var.sig;
  testing.call_subroutine("vcl_recv");
  assert.error(624);

  set var.shash = substr(digest.hash_sha256("admin-session"), 0, 16);
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~admin~1893456000~" + var.shash + "~scope-a/";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.url = "/api/file?key=scope-b%2Fprocessed%2Frequest%2Fa.parquet";
  set req.http.Cookie = "flp_session=admin-session; flp_cdn=" + var.payload + "~" + var.sig;
  testing.call_subroutine("vcl_recv");
  assert.error(626);
}


// @scope: recv
sub api_file_falls_through_when_session_cookie_changes() {
  testing.fixed_time("2026-10-29 12:34:56");
  declare local var.shash STRING;
  declare local var.payload STRING;
  declare local var.sig STRING;
  set var.shash = substr(digest.hash_sha256("old-session"), 0, 16);
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~admin~1893456000~" + var.shash + "~";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.url = "/api/file?key=processed%2Frequest%2Fa.parquet";
  set req.http.Cookie = "flp_session=new-session; flp_cdn=" + var.payload + "~" + var.sig;
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(pass);
  assert.is_notset(req.http.x-flp-fos-service);
}


// @scope: deliver
sub api_file_delivery_sets_parquet_headers_and_cdn_cache_hit() {
  set req.method = "GET";
  set req.http.x-flp-fos-service = "AAAAAAAAAAAAAAAAAAAAAA";
  set req.http.x-flp-api-file = "1";
  set resp.status = 200;
  testing.call_subroutine("vcl_deliver");
  assert.equal(resp.status, 200);
  assert.equal(resp.http.Content-Type, "application/vnd.apache.parquet");
  assert.equal(resp.http.Cache-Control, "private, max-age=31536000, immutable");
  assert.equal(resp.http.Vary, "Cookie");
  assert.equal(resp.http.x-flp-cdn-cache, "MISS");

  set resp.http.x-flp-fos-cache = "HIT";
  testing.call_subroutine("vcl_deliver");
  assert.equal(resp.http.x-flp-fos-cache, "HIT");
  assert.equal(resp.http.x-flp-cdn-cache, "HIT");
}


// @scope: recv
sub direct_fos_parquet_path_with_valid_cdn_cookie_rewrites_to_service_fos_lookup() {
  testing.fixed_time("2026-10-29 12:34:56");
  declare local var.shash STRING;
  declare local var.payload STRING;
  declare local var.sig STRING;
  set var.shash = substr(digest.hash_sha256("test-session"), 0, 16);
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~admin~1893456000~" + var.shash + "~goal2-live-20261029/";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.url = "/fos/goal2-live-20261029/processed/summary/request/year=2026/month=11/day=04/hour=19/summary=1793819640-1793819760-b1fb986e0bd3s0-n14.parquet";
  set req.http.Cookie = "flp_session=test-session; flp_cdn=" + var.payload + "~" + var.sig;
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.equal(req.url, "/fos/AAAAAAAAAAAAAAAAAAAAAA/goal2-live-20261029/processed/summary/request/year=2026/month=11/day=04/hour=19/summary=1793819640-1793819760-b1fb986e0bd3s0-n14.parquet");
  assert.equal(req.http.x-flp-fos-service, "AAAAAAAAAAAAAAAAAAAAAA");
  assert.equal(req.http.x-flp-api-file, "1");
}


// @scope: recv
sub direct_fos_parquet_path_falls_back_to_api_file_when_session_cookie_changes() {
  testing.fixed_time("2026-10-29 12:34:56");
  declare local var.shash STRING;
  declare local var.payload STRING;
  declare local var.sig STRING;
  set var.shash = substr(digest.hash_sha256("old-session"), 0, 16);
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~admin~1893456000~" + var.shash + "~goal2-live-20261029/";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.url = "/fos/goal2-live-20261029/processed/request/a.parquet";
  set req.http.Cookie = "flp_session=new-session; flp_cdn=" + var.payload + "~" + var.sig;
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(pass);
  assert.equal(req.url, {"/api/file?key=goal2-live-20261029%2Fprocessed%2Frequest%2Fa.parquet"});
}
