// Wrapper VCL for falco-driven semantic tests.
//
// The capture-VCL generator (backend.provision.generate_capture_vcl) emits
// snippets meant for vcl_recv / vcl_fetch / vcl_deliver. Falco (the
// open-source Fastly VCL linter / test runner) needs those snippets inside
// a full, syntactically-valid VCL file with a backend declared and the
// wrapping subroutines present. This file is that wrapper.
//
// The marker comments inside the subroutines below are substituted by the
// test runner (str.replace) with the generated snippets. They render as
// no-op comments to any VCL parser, so this file remains valid VCL on its
// own (the IDE doesn't complain about template placeholders).
//
// Why no vcl_recv wrapper here:
// The generated recv snippet references proprietary Fastly variables
// (fastly.ff.visits_this_service among others) that the open-source falco
// parser cannot resolve at parse time. Wrapping recv in this template would
// break every fetch/deliver test. When we want to exercise recv semantics
// via falco, add a separate template that pre-binds the missing variables
// via testing.inject_variable(...) rather than fighting Falco globally.
//
// Same caveat for the currently-skipped test_falco_origin_field_miss_pass_only
// (fastly_info.state binding via !~ operator is broken in falco) — adding an
// inject_variable stub here would let us re-enable that test.

backend F_origin {
    .connect_timeout = 1s;
    .dynamic = true;
    .port = "80";
    .host = "localhost";
}

sub vcl_fetch {
    set req.backend = F_origin;
    //<INJECT_FETCH_SNIPPET>
}

sub vcl_deliver {
    //<INJECT_DELIVER_SNIPPET>
}
