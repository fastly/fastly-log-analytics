// @scope: recv
sub fallback_is_authenticated_and_declared_to_compute {
  set req.http.x-flp-front-key = "attacker";
  set req.http.x-flp-front-rl = "1";
  set req.url = "/api/auth/login/options";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.http.x-flp-front-key, "test-front-key-not-a-live-secret");
  assert.equal(req.http.x-flp-front-rl, "0");
  assert.state(pass);
}


// @scope: deliver
sub fallback_also_strips_internal_markers {
  set resp.http.x-flp-auth-fail = "1";
  set resp.http.x-flp-front-key = "must-not-leak";
  testing.call_subroutine("vcl_deliver");
  assert.is_notset(resp.http.x-flp-auth-fail);
  assert.is_notset(resp.http.x-flp-front-key);
}
