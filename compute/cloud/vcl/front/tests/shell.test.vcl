// Test helper: falco requires mock targets in the testing VCL.
sub mock_flp_build_hash {
  set req.http.x-test-build-hash = "called";
}


// @scope: recv
sub analytics_shells_are_looked_up_on_a_path_only_key {
  testing.inject_variable("req.is_ssl", true);
  set req.http.Cookie = "flp_session=any-session";
  set req.url = "/dashboard?service=AAAAAAAAAAAAAAAAAAAAAA&metric=requests";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/dashboard");
  assert.state(lookup);

  set req.url = "/sessions/detail?token=abc";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/sessions/detail");
  assert.state(lookup);

  set req.url = "/origin.txt?_rsc=xyz";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/origin.txt");
  assert.state(lookup);

  set req.url = "/security/__next.security.__PAGE__.txt?_rsc=xyz";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/security/__next.security.__PAGE__.txt");
  assert.state(lookup);
}


// @scope: recv
sub analytics_shell_writes_still_reach_compute {
  testing.inject_variable("req.is_ssl", true);
  set req.method = "POST";
  set req.url = "/dashboard?service=x";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/dashboard?service=x");
  assert.state(pass);
}


// @scope: recv
sub auth_and_setup_routes_keep_passing_with_their_query {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/share-login?token=invite";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/share-login?token=invite");
  assert.state(pass);

  set req.url = "/setup.txt?_rsc=1";
  testing.call_subroutine("vcl_recv");
  assert.state(pass);

  set req.url = "/auth/callback?code=1";
  testing.call_subroutine("vcl_recv");
  assert.state(pass);
}


// @scope: recv
sub admin_shells_without_a_cdn_credential_reach_compute {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/admin?service=x";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-shell);
  assert.equal(req.url, "/admin?service=x");
  assert.state(pass);

  set req.http.Cookie = "flp_session=admin-session";
  set req.url = "/provision.txt?_rsc=1";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-shell);
  assert.state(pass);
}


// @scope: recv
sub client_cannot_forge_the_admin_shell_marker {
  testing.inject_variable("req.is_ssl", true);
  set req.http.x-flp-shell = "admin";
  set req.http.Cookie = "flp_session=admin-session";
  set req.url = "/admin/share";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-shell);
  assert.state(pass);
}


// @scope: recv
sub admin_shell_with_a_signed_admin_cdn_cookie_is_looked_up {
  testing.inject_variable("req.is_ssl", true);
  testing.fixed_time("2026-10-29 12:34:56");
  declare local var.payload STRING;
  declare local var.sig STRING;
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~admin~1893456000~" + substr(digest.hash_sha256("admin-session"), 0, 16) + "~logs/edge/";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.http.Cookie = "flp_session=admin-session; __Host-flp_cdn=" + var.payload + "~" + var.sig;
  set req.url = "/admin/session-scoring?service=x";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.equal(req.http.x-flp-shell, "admin");
  assert.equal(req.url, "/admin/session-scoring");
  assert.equal(req.http.Cookie:flp_session, "admin-session");
  assert.state(lookup);
}


// @scope: recv
sub admin_shell_honors_the_current_auth_epoch {
  testing.inject_variable("req.is_ssl", true);
  testing.fixed_time("2026-10-29 12:34:56");
  testing.table_set(flp_front_auth, "auth_epoch", "3");
  declare local var.payload STRING;
  declare local var.sig STRING;
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~admin~1893456000~" + substr(digest.hash_sha256("admin-session"), 0, 16) + "~3~logs/edge/";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v2~" + var.payload), 2, 64);
  set req.http.Cookie = "flp_session=admin-session; flp_cdn=" + var.payload + "~" + var.sig;
  set req.url = "/logs";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.x-flp-shell, "admin");
  assert.state(lookup);

  // A role change or revocation bumps the epoch; the old credential falls back to Compute.
  testing.table_set(flp_front_auth, "auth_epoch", "4");
  set req.url = "/logs";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-shell);
  assert.state(pass);
}


// @scope: recv
sub admin_shell_rejects_non_admin_forged_expired_and_rebound_credentials {
  testing.inject_variable("req.is_ssl", true);
  testing.fixed_time("2026-10-29 12:34:56");
  declare local var.shash STRING;
  declare local var.payload STRING;
  declare local var.sig STRING;
  set var.shash = substr(digest.hash_sha256("admin-session"), 0, 16);

  // Analyst credential.
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~analyst~1893456000~" + var.shash + "~logs/edge/";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.http.Cookie = "flp_session=admin-session; flp_cdn=" + var.payload + "~" + var.sig;
  set req.url = "/admin";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.is_notset(req.http.x-flp-shell);
  assert.state(pass);

  // Role rewritten to admin without re-signing.
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~admin~1893456000~" + var.shash + "~logs/edge/";
  set req.http.Cookie = "flp_session=admin-session; flp_cdn=" + var.payload + "~" + var.sig;
  set req.url = "/admin";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.is_notset(req.http.x-flp-shell);
  assert.state(pass);

  // Expired.
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~admin~1761000000~" + var.shash + "~logs/edge/";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.http.Cookie = "flp_session=admin-session; flp_cdn=" + var.payload + "~" + var.sig;
  set req.url = "/admin";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-shell);
  assert.state(pass);

  // Valid credential presented with a different session cookie.
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~admin~1893456000~" + var.shash + "~logs/edge/";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.http.Cookie = "flp_session=other-session; flp_cdn=" + var.payload + "~" + var.sig;
  set req.url = "/admin";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-shell);
  assert.state(pass);

  // Valid credential without any session cookie.
  set req.http.Cookie = "flp_cdn=" + var.payload + "~" + var.sig;
  set req.url = "/admin";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-shell);
  assert.state(pass);
}


// @scope: recv
sub admin_credential_never_marks_non_admin_paths {
  testing.inject_variable("req.is_ssl", true);
  testing.fixed_time("2026-10-29 12:34:56");
  declare local var.payload STRING;
  declare local var.sig STRING;
  set var.payload = "AAAAAAAAAAAAAAAAAAAAAA~admin~1893456000~" + substr(digest.hash_sha256("admin-session"), 0, 16) + "~logs/edge/";
  set var.sig = substr(digest.hmac_sha256("test-cdn-key-not-a-live-secret", "flp-cdn-v1~" + var.payload), 2, 64);
  set req.http.Cookie = "flp_session=admin-session; flp_cdn=" + var.payload + "~" + var.sig;
  set req.url = "/api/admin/users";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-shell);
  assert.state(pass);

  set req.url = "/dashboard";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-flp-shell);
  assert.state(lookup);
}


// @scope: fetch
sub admin_shell_is_cached_without_changing_its_private_headers {
  set req.url = "/admin";
  set req.http.x-flp-shell = "admin";
  set beresp.status = 200;
  set beresp.http.Content-Type = "text/html";
  set beresp.http.Content-Encoding = "br";
  set beresp.http.Cache-Control = "private, no-store";
  set beresp.http.Vary = "Cookie";
  set beresp.http.Content-Security-Policy = "default-src 'self'";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 60s);
  assert.equal(beresp.http.Cache-Control, "private, no-store");
  assert.equal(beresp.http.Vary, "Accept-Encoding");
  assert.equal(beresp.http.Surrogate-Key, "flp-html");
  assert.equal(beresp.http.Content-Security-Policy, "default-src 'self'");
  assert.state(deliver);
}


// @scope: fetch
sub admin_rsc_payload_is_cached_like_its_shell {
  set req.url = "/admin.txt";
  set req.http.x-flp-shell = "admin";
  set beresp.status = 200;
  set beresp.http.Content-Type = "text/plain";
  set beresp.http.Cache-Control = "private, no-store";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 60s);
  assert.state(deliver);
}


// @scope: fetch
sub admin_shell_redirects_denials_and_cookies_are_never_cached {
  set req.url = "/admin";
  set req.http.x-flp-shell = "admin";
  set beresp.status = 303;
  set beresp.http.Location = "/sign-in";
  set beresp.http.Cache-Control = "private, no-store";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);

  set beresp.status = 403;
  set beresp.http.Content-Type = "application/json";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);

  set beresp.status = 200;
  set beresp.http.Content-Type = "text/html";
  set beresp.http.Set-Cookie = "flp_session=; Max-Age=0";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);
}


// @scope: fetch
sub admin_shell_without_the_marker_is_never_cached {
  set req.url = "/services";
  set beresp.status = 200;
  set beresp.http.Content-Type = "text/html";
  set beresp.http.Cache-Control = "private, no-store";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);

  // Even a public-looking admin response stays uncached without a verified credential.
  set beresp.http.Cache-Control = "public, max-age=60";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);
}


// @scope: fetch
sub analytics_shell_is_cached_like_the_home_page {
  set req.url = "/dashboard";
  set beresp.status = 200;
  set beresp.http.Content-Type = "text/html";
  set beresp.http.Cache-Control = "public, max-age=60";
  set beresp.http.Surrogate-Key = "flp-html";
  set beresp.http.Vary = "Accept-Encoding";
  testing.call_subroutine("vcl_fetch");
  assert.true(beresp.cacheable);
  assert.equal(beresp.ttl, 60s);
  assert.equal(beresp.http.Cache-Control, "public, max-age=60");
  assert.equal(beresp.http.Surrogate-Key, "flp-html");
  assert.equal(beresp.http.Vary, "Accept-Encoding");
  assert.state(deliver);
}


// @scope: fetch
sub analytics_shell_redirects_and_cookies_are_never_cached {
  set req.url = "/dashboard";
  set beresp.status = 303;
  set beresp.http.Location = "/setup";
  set beresp.http.Cache-Control = "private, no-store";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);

  set beresp.status = 200;
  set beresp.http.Content-Type = "text/html";
  set beresp.http.Cache-Control = "public, max-age=60";
  set beresp.http.Set-Cookie = "flp_session=; Max-Age=0";
  testing.call_subroutine("vcl_fetch");
  assert.false(beresp.cacheable);
  assert.state(pass);
}


// @scope: deliver
sub admin_shell_hit_restores_the_compute_vary {
  set req.url = "/admin";
  set req.http.x-flp-shell = "admin";
  set resp.status = 200;
  set resp.http.Content-Type = "text/html";
  set resp.http.Vary = "Accept-Encoding";
  testing.call_subroutine("vcl_deliver");
  assert.equal(resp.http.Vary, "Cookie");
}


// @scope: hash
sub shells_are_keyed_by_build_but_hashed_assets_are_not {
  testing.mock("flp_build_hash", "mock_flp_build_hash");
  set req.url = "/dashboard";
  testing.call_subroutine("vcl_hash");
  assert.equal(req.http.x-test-build-hash, "called");

  unset req.http.x-test-build-hash;
  set req.url = "/";
  testing.call_subroutine("vcl_hash");
  assert.equal(req.http.x-test-build-hash, "called");

  unset req.http.x-test-build-hash;
  set req.url = "/_next/static/chunks/app.js";
  testing.call_subroutine("vcl_hash");
  assert.is_notset(req.http.x-test-build-hash);

  set req.url = "/duckdb/1.33.1-dev57.0/duckdb-eh.wasm";
  testing.call_subroutine("vcl_hash");
  assert.is_notset(req.http.x-test-build-hash);

  set req.url = "/_tick";
  set req.http.x-flp-tick = "1";
  testing.call_subroutine("vcl_hash");
  assert.is_notset(req.http.x-test-build-hash);
}
