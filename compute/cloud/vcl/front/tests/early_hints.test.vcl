// Test helper: falco requires mock targets in the testing VCL, so it also runs as a trivial case.
sub mock_flp_early_hints {
  set req.http.x-test-early-hints = "called";
}


// @scope: deliver
sub mock_flp_hint_links {
  set resp.http.Link = "mocked";
}


// @scope: recv
sub early_hints_reached_for_h2_document_navigation {
  testing.mock("flp_early_hints", "mock_flp_early_hints");
  testing.inject_variable("req.is_ssl", true);
  testing.inject_variable("fastly_info.is_h2", true);
  set req.url = "/dashboard?service=fixture";
  set req.http.Sec-Fetch-Dest = "document";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.x-test-early-hints, "called");
}


// @scope: recv
sub early_hints_reached_for_h3_html_navigation_without_fetch_metadata {
  testing.mock("flp_early_hints", "mock_flp_early_hints");
  testing.inject_variable("req.is_ssl", true);
  testing.inject_variable("fastly_info.is_h3", true);
  set req.url = "/admin/";
  set req.http.Accept = "text/html,application/xhtml+xml";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.x-test-early-hints, "called");
}


// @scope: recv
sub early_hints_skipped_for_http1 {
  testing.mock("flp_early_hints", "mock_flp_early_hints");
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/dashboard";
  set req.http.Sec-Fetch-Dest = "document";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-test-early-hints);
}


// @scope: recv
sub early_hints_skipped_for_subresources_and_non_get {
  testing.mock("flp_early_hints", "mock_flp_early_hints");
  testing.inject_variable("req.is_ssl", true);
  testing.inject_variable("fastly_info.is_h2", true);
  set req.url = "/dashboard";
  set req.http.Sec-Fetch-Dest = "empty";
  set req.http.Accept = "text/html";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-test-early-hints);

  set req.http.Sec-Fetch-Dest = "document";
  set req.method = "POST";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-test-early-hints);
}


// @scope: recv
sub early_hints_skipped_for_api_and_data_paths {
  testing.mock("flp_early_hints", "mock_flp_early_hints");
  testing.inject_variable("req.is_ssl", true);
  testing.inject_variable("fastly_info.is_h2", true);
  set req.http.Sec-Fetch-Dest = "document";
  set req.url = "/api/files";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-test-early-hints);

  set req.url = "/auth/callback";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-test-early-hints);

  set req.url = "/logs/day.parquet";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.x-test-early-hints);
}


// @scope: recv
sub early_hints_skipped_for_plain_http_redirect {
  testing.mock("flp_early_hints", "mock_flp_early_hints");
  testing.inject_variable("req.is_ssl", false);
  testing.inject_variable("fastly_info.is_h2", true);
  set req.url = "/dashboard";
  set req.http.Sec-Fetch-Dest = "document";
  testing.call_subroutine("vcl_recv");
  assert.error(801);
  assert.is_notset(req.http.x-test-early-hints);
}


// @scope: deliver
sub final_html_repeats_hints_as_a_link_header {
  testing.mock("flp_hint_links", "mock_flp_hint_links");
  set req.url = "/dashboard";
  set resp.status = 200;
  set resp.http.Content-Type = "text/html; charset=utf-8";
  testing.call_subroutine("vcl_deliver");
  assert.equal(resp.http.Link, "mocked");
}


// @scope: deliver
sub link_header_skipped_for_redirects_non_html_and_storage {
  testing.mock("flp_hint_links", "mock_flp_hint_links");
  set req.url = "/dashboard";
  set resp.status = 303;
  set resp.http.Content-Type = "text/html";
  testing.call_subroutine("vcl_deliver");
  assert.is_notset(resp.http.Link);

  set resp.status = 200;
  set resp.http.Content-Type = "text/plain";
  testing.call_subroutine("vcl_deliver");
  assert.is_notset(resp.http.Link);

  set resp.http.Content-Type = "text/html";
  set req.method = "POST";
  testing.call_subroutine("vcl_deliver");
  assert.is_notset(resp.http.Link);
}
