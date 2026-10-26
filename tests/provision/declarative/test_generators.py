"""Unit tests for VCL generators: variable duplication, field presence, deterministic order."""

from __future__ import annotations

import re

import pytest
from backend.provision.declarative.generators import (
    desired_backends,
    desired_logging_endpoints,
    desired_snippets,
    generate_consolidated_snippet,
    generate_log_format,
    logging_service_snippets,
)
from backend.provision.declarative.state import FeatureState


class TestGeneratorVCLStructure:
    """Test VCL generation correctness."""

    def test_generator_no_duplicate_var_declarations(self):
        """Verify no VCL variable declared twice in consolidated snippet."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "scoring": {"enabled": True, "domain": "scorer.example.com"},
                "cmcd": {"enabled": True},
            }
        )

        for subroutine in ["vcl_recv", "vcl_fetch", "vcl_deliver", "vcl_error"]:
            vcl = generate_consolidated_snippet(state, subroutine)
            var_decls = re.findall(r"declare\s+local\s+var\.(\w+)", vcl)
            duplicates = [v for v in set(var_decls) if var_decls.count(v) > 1]
            assert not duplicates, f"{subroutine}: duplicate var declarations {duplicates}"

    def test_rum_beacon_fields_are_extracted_before_capture(self):
        """RUM fields must be populated before the log capture promotes them."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
            }
        )

        vcl = generate_consolidated_snippet(state, "vcl_recv")

        extraction = vcl.index(
            'set req.http.x-fos-edge-data:rum_metric_name = querystring.get(req.url, "rum_metric_name");'
        )
        capture = vcl.index("# Capture edge data for logging")
        assert extraction < capture
        assert 'querystring.get(req.url, "rum_metric_value")' in vcl
        assert 'querystring.get(req.url, "rum_error_message")' in vcl

    def test_generator_builds_5_consolidated_snippets(self):
        """Verify desired_snippets returns 5 consolidated snippets."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
            }
        )
        snippets = desired_snippets(state)
        assert len(snippets) == 6, f"Should have 6 snippets, got {len(snippets)}"
        names = {s.name for s in snippets}
        expected = {
            "Fastly Log Analytics - vcl_recv",
            "Fastly Log Analytics - vcl_miss",
            "Fastly Log Analytics - vcl_pass",
            "Fastly Log Analytics - vcl_fetch",
            "Fastly Log Analytics - vcl_deliver",
            "Fastly Log Analytics - vcl_error",
        }
        assert names == expected, f"Unexpected snippet names: {names}"

    def test_generator_snippet_priority_is_10(self):
        """Verify consolidated snippets have priority=10."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
            }
        )
        snippets = desired_snippets(state)
        for snippet in snippets:
            assert snippet.priority == 10, f"{snippet.name} has priority {snippet.priority}, expected 10"

    def test_generator_includes_all_custom_fields_in_format(self):
        """Verify every enabled custom field appears in log format."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "log_fields": {
                    "custom_fields": [
                        {"name": "custom_field_1", "expression": "req.http.Custom-1", "enabled": True},
                        {"name": "custom_field_2", "expression": "req.http.Custom-2", "enabled": True},
                    ]
                },
            }
        )

        log_format = generate_log_format(state)
        for field in state.log_fields.custom_fields:
            if field.get("enabled", True):
                assert field["name"] in log_format, f"Field {field['name']} missing from log format"

    def test_generator_rum_fetch_ttl(self):
        """Verify RUM vcl_fetch snippet sets cache TTL of 1 day."""
        # 1. RUM enabled
        state_enabled = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
            }
        )
        vcl_enabled = generate_consolidated_snippet(state_enabled, "vcl_fetch")
        assert "set beresp.ttl = 86400s;" in vcl_enabled
        assert "set beresp.cacheable = true;" in vcl_enabled
        assert 'set beresp.http.Surrogate-Key = "rum-js";' in vcl_enabled
        assert 'set beresp.http.Surrogate-Control = "max-age=86400";' in vcl_enabled
        assert 'set beresp.http.Cache-Control = "public, max-age=300";' in vcl_enabled
        assert "immutable" not in vcl_enabled
        assert 'req.url.path == "/js/rum.js"' in vcl_enabled

        # 2. RUM disabled
        state_disabled = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": False,
            }
        )
        vcl_disabled = generate_consolidated_snippet(state_disabled, "vcl_fetch")
        assert "set beresp.ttl = 86400s;" not in vcl_disabled

    def test_rum_js_caching_gates_on_200_status(self):
        """Same F-3 class of bug the Faro bundle was fixed for: without a
        beresp.status check, a transient FOS 403/404 (mid-upload, or a
        bucket-policy blip) gets cached at the edge for a full day, so every
        visitor loads a broken tracker until the TTL expires."""
        state = FeatureState.from_config(
            {"service_id": "srv_test", "log_period": 60, "sample_rate": 100, "rum_enabled": True}
        )
        vcl = generate_consolidated_snippet(state, "vcl_fetch")

        rum_block = vcl.split('if (req.url.path == "/js/rum.js") {', 1)[1]
        status_idx = rum_block.index("if (beresp.status == 200)")
        # Every caching directive must sit inside the 200 branch, not above it.
        for directive in (
            "set beresp.ttl = 86400s;",
            "set beresp.cacheable = true;",
            'set beresp.http.Surrogate-Key = "rum-js";',
        ):
            assert status_idx < rum_block.index(directive)
        # And the non-200 branch must explicitly refuse to cache.
        assert "set beresp.cacheable = false;" in rum_block


class TestScoringExcludesRumAssets:
    """Section 4 must never route our own hosted RUM scripts to the scorer."""

    def _state(self, rum_enabled, exclude_url_regex=None):
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "rum_enabled": rum_enabled,
            "scoring": {"enabled": True, "request_secret": "s3cret", "domain": "scorer.example.com"},
        }
        if exclude_url_regex is not None:
            cfg["scoring"]["exclude_url_regex"] = exclude_url_regex
        return FeatureState.from_config(cfg)

    def test_rum_assets_excluded_from_scoring_when_rum_enabled(self):
        vcl = generate_consolidated_snippet(self._state(rum_enabled=True), "vcl_recv")
        assert 'req.url.path != "/js/rum.js"' in vcl
        assert 'req.url.path != "/js/faro-sdk.js"' in vcl

    def test_no_rum_guard_when_rum_disabled(self):
        """The guard is dead weight on a service with no RUM routes."""
        vcl = generate_consolidated_snippet(self._state(rum_enabled=False), "vcl_recv")
        assert 'req.url.path != "/js/rum.js"' not in vcl

    def test_guard_survives_a_narrow_operator_exclude_regex(self):
        """The load-bearing case. DEFAULT_ASSET_EXT_REGEX happens to match
        '.js', so with the default in place the exclusion looks fine either
        way. But exclude_url_regex is operator-overridable — a narrower
        override must not silently start routing our own asset requests
        through the scorer, so the guard cannot depend on that config."""
        vcl = generate_consolidated_snippet(self._state(rum_enabled=True, exclude_url_regex=r"^/static/"), "vcl_recv")
        assert "^/static/" in vcl
        assert 'req.url.path != "/js/rum.js"' in vcl
        assert 'req.url.path != "/js/faro-sdk.js"' in vcl

    def test_guard_precedes_the_scoring_return_pass(self):
        """The scoring block ends in return(pass), which exits vcl_recv before
        the RUM asset routing below it runs. The guard therefore has to be
        part of the same condition — not a separate check after it — or a GET
        /js/rum.js still pays a scorer round-trip before reaching FOS."""
        vcl = generate_consolidated_snippet(self._state(rum_enabled=True), "vcl_recv")

        guard_idx = vcl.index('req.url.path != "/js/rum.js"')
        pass_idx = vcl.index("return(pass);")
        assert guard_idx < pass_idx
        # Same VCL statement: no ')' closing the if-condition between them.
        assert "{" in vcl[guard_idx:pass_idx]


class TestLoggingEndpointGeneration:
    """Test logging endpoint generation."""

    def test_generator_main_endpoint_created(self):
        """Verify main logging endpoint is created by default."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "fos_prefix": "raw",
            }
        )
        endpoints = desired_logging_endpoints(state)
        main = [e for e in endpoints if e.name == "Fastly Log Analytics"]
        assert len(main) == 1, "Should have exactly one main endpoint"
        assert "year=%Y" in main[0].path

    def test_generator_endpoints_use_null_placement(self):
        """Verify that S3 logging endpoints use None/null placement for Format Version Default."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "fos_prefix": "raw",
            }
        )
        endpoints = desired_logging_endpoints(state)
        main = [e for e in endpoints if e.name == "Fastly Log Analytics"]
        rum = [e for e in endpoints if e.name == "Fastly RUM Logs"]
        assert len(main) == 1
        assert len(rum) == 1
        assert main[0].placement is None, f"Expected main endpoint placement to be None, got {main[0].placement}"
        assert rum[0].placement is None, f"Expected RUM endpoint placement to be None, got {rum[0].placement}"

    def test_generator_main_endpoint_not_created_when_logging_disabled(self):
        """Verify main logging endpoint is not created when logging_enabled=False."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "fos_prefix": "raw",
                "logging_enabled": False,
            }
        )
        endpoints = desired_logging_endpoints(state)
        main = [e for e in endpoints if e.name == "Fastly Log Analytics"]
        assert len(main) == 0, "Should not have main endpoint when logging is disabled"

    def test_generator_rum_endpoint_created_when_enabled(self):
        """Verify RUM endpoint is created when rum_enabled=True."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "fos_prefix": "raw",
            }
        )
        endpoints = desired_logging_endpoints(state)
        rum = [e for e in endpoints if e.name == "Fastly RUM Logs"]
        assert len(rum) == 1, "Should have RUM endpoint when RUM enabled"
        assert "raw/rum/" in rum[0].path
        assert rum[0].response_condition == "rum_log_condition"
        assert rum[0].period == 60  # falls back to log_period when rum_log_period is None

    def test_generator_rum_endpoint_respects_rum_log_period(self):
        """Verify RUM endpoint uses rum_log_period when configured."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "rum_log_period": 300,
                "fos_prefix": "raw",
            }
        )
        endpoints = desired_logging_endpoints(state)
        rum = [e for e in endpoints if e.name == "Fastly RUM Logs"]
        assert len(rum) == 1
        assert rum[0].period == 300

    def test_generator_rum_endpoint_not_created_when_disabled(self):
        """Verify RUM endpoint is not created when rum_enabled=False."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": False,
            }
        )
        endpoints = desired_logging_endpoints(state)
        rum = [e for e in endpoints if "RUM" in e.name]
        assert len(rum) == 0, "Should not have RUM endpoint when RUM disabled"


class TestBackendGeneration:
    """Test backend generation."""

    def test_generator_scoring_backend_created_when_enabled(self):
        """Verify session_scorer backend is created when scoring_enabled=True."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "scoring": {"enabled": True, "domain": "scorer.example.com"},
            }
        )
        backends = desired_backends(state)
        scorer = [b for b in backends if b.name == "session_scorer"]
        assert len(scorer) == 1, "Should have session_scorer backend"
        assert scorer[0].address == "scorer.example.com"
        assert scorer[0].port == 443
        assert scorer[0].ssl_hostname == "scorer.example.com"

    def test_generator_rum_backend_created_when_enabled(self):
        """Verify fos_origin backend is created when rum_enabled=True."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
            }
        )
        backends = desired_backends(state)
        fos = [b for b in backends if b.name == "fos_origin"]
        assert len(fos) == 1, "Should have fos_origin backend for RUM"
        assert fos[0].port == 443

    def test_generator_no_backends_when_features_disabled(self):
        """Verify no backends created when all features disabled."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": False,
                "scoring": {"enabled": False},
            }
        )
        backends = desired_backends(state)
        assert len(backends) == 0, "Should have no backends when features disabled"


class TestRUMAssetFetchGeneration:
    """Test Phase 3 RUM asset-fetch VCL generation."""

    def test_rum_asset_fetch_included_in_vcl_recv_when_enabled(self):
        """Verify RUM asset-fetch FOS logic is included in vcl_recv when rum_enabled=True."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
            }
        )
        snippets = desired_snippets(state)
        recv_snippet = next(s for s in snippets if s.subroutine == "vcl_recv")

        # Verify asset-fetch logic is present
        assert "/js/rum.js" in recv_snippet.body, "Should include /js/rum.js route check"
        assert "F_fos_origin" in recv_snippet.body, "Should set fos_origin backend"
        assert "X-FOS-Request" in recv_snippet.body, "Should set X-FOS-Request flag"

    def test_rum_sigv4_signing_included_in_vcl_miss_when_enabled(self):
        """Verify RUM SigV4 signing logic is included in vcl_miss when rum_enabled=True."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
            }
        )
        snippets = desired_snippets(state)
        miss_snippet = next(s for s in snippets if s.subroutine == "vcl_miss")

        # Verify SigV4 signing logic is present
        assert "X-FOS-Request" in miss_snippet.body, "Should check X-FOS-Request flag"
        assert 'bereq.url = "/rum/rum-tracker.js";' in miss_snippet.body, "Should rewrite request path in vcl_miss"
        assert "fosAccessKey" in miss_snippet.body, "Should declare fosAccessKey variable"
        assert "AWS4-HMAC-SHA256" in miss_snippet.body, "Should use AWS4-HMAC-SHA256 signing"
        assert "x-amz-date" in miss_snippet.body, "Should set x-amz-date header"

    def test_rum_error_handler_included_in_vcl_error_when_enabled(self):
        """Verify RUM error handler is included in vcl_error when rum_enabled=True."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
            }
        )
        snippets = desired_snippets(state)
        error_snippet = next(s for s in snippets if s.subroutine == "vcl_error")

        # Verify error handler for synthetic 611 is present
        assert "obj.status == 611" in error_snippet.body, "Should handle error 611"
        assert "set obj.status = 204" in error_snippet.body, "Should convert 611 to 204 No Content"
        assert "synthetic" in error_snippet.body, "Should return synthetic response"

    def test_rum_asset_fetch_not_included_when_disabled(self):
        """Verify RUM asset-fetch is not included when rum_enabled=False."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": False,
            }
        )
        snippets = desired_snippets(state)
        recv_snippet = next(s for s in snippets if s.subroutine == "vcl_recv")
        miss_snippet = next(s for s in snippets if s.subroutine == "vcl_miss")

        # Verify asset-fetch logic is NOT present
        assert "/js/rum.js" not in recv_snippet.body, "Should not include /js/rum.js route when rum disabled"
        assert "fosAccessKey" not in miss_snippet.body, "Should not include SigV4 signing when rum disabled"

    def test_rum_recv_and_asset_fetch_both_in_vcl_recv(self):
        """Verify both RUM recv logic and asset-fetch are in consolidated vcl_recv."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
            }
        )
        snippets = desired_snippets(state)
        recv_snippet = next(s for s in snippets if s.subroutine == "vcl_recv")

        # Verify both Phase 1 (recv) and Phase 3 (asset-fetch) are present
        assert "x-rum-req-id" in recv_snippet.body, "Should include Phase 1 recv logic (request ID minting)"
        assert "/rum-beacon" in recv_snippet.body, "Should include Phase 1 beacon handling"
        assert "/js/rum.js" in recv_snippet.body, "Should include Phase 3 asset-fetch logic"

    def test_rum_request_id_minting_moved_to_section_6(self):
        """Verify x-rum-req-id minting is moved to Section 6 and Section 6 is at the top."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
            }
        )
        snippets = desired_snippets(state)
        recv_snippet = next(s for s in snippets if s.subroutine == "vcl_recv")
        body = recv_snippet.body

        # Verify x-rum-req-id is in the Log Field Capture (Section 6) block and NOT in the RUM (Section 5) block
        assert "# Section 6: Log Field Capture (vcl_recv)" in body
        assert "# Section 5: RUM (vcl_recv)" in body

        # Find section offsets
        idx_6 = body.index("# Section 6: Log Field Capture (vcl_recv)")
        idx_5 = body.index("# Section 5: RUM (vcl_recv)")

        # Section 6 should be at the top, before Section 5
        assert idx_6 < idx_5, "Section 6 should be at the top, before Section 5"

        # Verify minting line is within the Section 6 block, not Section 5
        sec_6_block = body[idx_6:idx_5]
        sec_5_block = body[idx_5:]

        assert "set req.http.x-rum-req-id = randomstr(12);" in sec_6_block, "Minting should be in Section 6"
        assert "set req.http.x-rum-req-id = randomstr(12);" not in sec_5_block, "Minting should NOT be in Section 5"

    def test_backend_shield_mapping(self):
        """Verify FOS origin gets assigned the correct shield POP based on region or custom configuration."""
        # 1. Default shield POP mapping
        state1 = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "fos_region": "us-east-1",
                "cdn_shield": "",
            }
        )
        backends1 = desired_backends(state1)
        fos1 = next(b for b in backends1 if b.name == "fos_origin")
        assert fos1.shield == "iad-va-us"

        # 2. Another region mapping
        state2 = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "fos_region": "us-central-1",
                "cdn_shield": "",
            }
        )
        backends2 = desired_backends(state2)
        fos2 = next(b for b in backends2 if b.name == "fos_origin")
        assert fos2.shield == "chi-il-us"

        # 3. Custom shield override
        state3 = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "fos_region": "us-east-1",
                "cdn_shield": "frankfurt-de",
            }
        )
        backends3 = desired_backends(state3)
        fos3 = next(b for b in backends3 if b.name == "fos_origin")
        assert fos3.shield == "frankfurt-de"

        # 4. Explicitly disabling shield
        state4 = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "fos_region": "us-east-1",
                "cdn_shield": "none",
            }
        )
        backends4 = desired_backends(state4)
        fos4 = next(b for b in backends4 if b.name == "fos_origin")
        assert fos4.shield == ""


class TestLoggingServiceSnippets:
    """Test logging service snippet generation."""

    def test_logging_service_snippets_creates_capture_snippet(self):
        """Verify logging_service_snippets creates 'Fastly Log Analysis Capture' recv snippet."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "logging_enabled": True,
            }
        )
        snippets = logging_service_snippets(state)
        capture = [s for s in snippets if s.name == "Fastly Log Analysis Capture"]
        assert len(capture) == 1, "Should have Fastly Log Analysis Capture snippet"
        assert capture[0].subroutine == "vcl_recv"
        assert capture[0].priority == 1
        assert "fastly.ff.visits_this_service" in capture[0].body

    def test_logging_service_snippets_empty_when_disabled(self):
        """Verify logging_service_snippets returns empty list when logging disabled."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "logging_enabled": False,
            }
        )
        snippets = logging_service_snippets(state)
        assert len(snippets) == 0, "Should have no snippets when logging disabled"

    def test_logging_service_snippets_includes_miss_pass_when_group_l(self):
        """Verify logging_service_snippets includes miss/pass/fetch/deliver/error for Group L."""
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "logging_enabled": True,
                "log_fields": {"groups": ["L"], "custom_fields": []},
            }
        )
        snippets = logging_service_snippets(state)
        names = {s.name for s in snippets}
        # Should have recv (Capture), miss, pass, and optionally fetch/deliver/error for Group L
        assert "Fastly Log Analysis Capture" in names
        assert "Fastly Log Analysis Miss" in names or len(snippets) >= 1


class TestFaroVersionThreading:
    """Test that cfg["rum"]["faro_version"] threads through FeatureState into generated VCL."""

    def _rum_state(self, faro_version=None):
        cfg = {
            "service_id": "srv_test",
            "log_period": 60,
            "sample_rate": 100,
            "rum_enabled": True,
        }
        if faro_version is not None:
            cfg["rum"] = {"faro_version": faro_version}
        return FeatureState.from_config(cfg)

    def test_from_config_defaults_faro_version_to_none(self):
        """No cfg["rum"] block at all -> faro_version stays None."""
        state = FeatureState.from_config(
            {"service_id": "srv_test", "log_period": 60, "sample_rate": 100, "rum_enabled": True}
        )
        assert state.faro_version is None

    def test_from_config_reads_pinned_faro_version(self):
        """cfg["rum"]["faro_version"] is threaded onto FeatureState.faro_version."""
        state = self._rum_state(faro_version="2.9.0")
        assert state.faro_version == "2.9.0"

    def test_faro_version_none_output_is_byte_identical_to_baseline(self):
        """Regression guard: a service with no pinned Faro version must see
        no change whatsoever in generated VCL — the exact snippet bodies
        that existed before faro_version was threaded through."""
        state = self._rum_state(faro_version=None)

        vcl_recv = generate_consolidated_snippet(state, "vcl_recv")
        assert "/js/faro-sdk.js" not in vcl_recv
        assert (
            """# Fetch RUM tracker JS from FOS with SigV4 signing
if (req.url.path == "/js/rum.js" && req.method == "GET") {
    # Backend points to FOS endpoint (shared with logging)
    set req.backend = fastly.try_select_shield(ssl_shield_iad_va_us, F_fos_origin);
    # Flag for SigV4 signing in miss/pass (req.backend.name not available there)
    set req.http.X-FOS-Request = "1";
    return(lookup);
}"""
            in vcl_recv
        )

        vcl_fetch = generate_consolidated_snippet(state, "vcl_fetch")
        assert "faro-sdk" not in vcl_fetch
        assert "rum-faro-sdk" not in vcl_fetch
        assert (
            vcl_fetch
            == """# Section 5: RUM (vcl_fetch)
# Cache RUM tracker JS at the edge; browsers get a short TTL so a
# Surrogate-Key purge of "rum-js" becomes visible to clients quickly.
if (req.url.path == "/js/rum.js") {
    if (beresp.status == 200) {
        set beresp.ttl = 86400s;
        set beresp.cacheable = true;
        set beresp.http.Surrogate-Key = "rum-js";
        set beresp.http.Surrogate-Control = "max-age=86400";
        set beresp.http.Cache-Control = "public, max-age=300";
    } else {
        set beresp.ttl = 0s;
        set beresp.cacheable = false;
    }
}"""
        )

        vcl_miss = generate_consolidated_snippet(state, "vcl_miss")
        assert "faro-sdk" not in vcl_miss
        assert "/js/rum.js" in vcl_miss

    def test_faro_version_pinned_adds_recv_routing(self):
        """A pinned faro_version puts GET /js/faro-sdk.js on the SAME route as
        /js/rum.js — one or-ed condition, not a second copied block, so the
        backend selection and X-FOS-Request flag cannot drift apart."""
        state = self._rum_state(faro_version="2.9.0")
        vcl_recv = generate_consolidated_snippet(state, "vcl_recv")
        assert (
            'if ((req.url.path == "/js/rum.js" || req.url.path == "/js/faro-sdk.js") && req.method == "GET")'
            in vcl_recv
        )

    def test_faro_version_pinned_rewrites_object_path_in_miss(self):
        """A pinned faro_version rewrites bereq.url to the versioned FOS object path."""
        state = self._rum_state(faro_version="2.9.0")
        vcl_miss = generate_consolidated_snippet(state, "vcl_miss")
        assert '"/rum/faro-web-sdk-v2.9.0.iife.js"' in vcl_miss
        # Existing rum.js rewrite must remain untouched.
        assert '"/rum/rum-tracker.js"' in vcl_miss

    def test_faro_version_pinned_adds_purgeable_fetch_caching(self):
        """A pinned faro_version adds the RUM_FARO_FETCH_NAME caching snippet to vcl_fetch,
        keyed off the exact surrogate key the cron purges."""
        state = self._rum_state(faro_version="2.9.0")
        vcl_fetch = generate_consolidated_snippet(state, "vcl_fetch")
        assert '"/js/faro-sdk.js" && req.http.X-FOS-Request == "1"' in vcl_fetch
        assert 'set beresp.http.Surrogate-Key = "rum-faro-sdk rum-js";' in vcl_fetch
        assert "set beresp.ttl = 604800s;" in vcl_fetch
        # Existing rum.js caching must remain untouched.
        assert "set beresp.ttl = 86400s;" in vcl_fetch
        # Both hosted scripts carry the shared key reconciliation purges.
        keys = re.findall(r'Surrogate-Key = "([^"]+)"', vcl_fetch)
        assert [set(k.split()) for k in keys] == [{"rum-js"}, {"rum-faro-sdk", "rum-js"}]

    def test_faro_version_invalid_rejected_at_generation(self):
        """An invalid faro_version string is rejected, not silently interpolated."""
        state = self._rum_state(faro_version="not-a-version")
        with pytest.raises(ValueError, match="faro_version must be a plain"):
            generate_consolidated_snippet(state, "vcl_recv")
        with pytest.raises(ValueError, match="faro_version must be a plain"):
            generate_consolidated_snippet(state, "vcl_miss")


# ── Merged from tests/provision/declarative/test_fastly_integration.py ──────

from typing import Any
from unittest.mock import patch

from backend.provision.declarative.diff import Backend, LoggingEndpoint, ServiceDictionary, VCLSnippet

from backend.provision.declarative import fastly_integration as fi


def _fastly_mock(responses: dict[tuple[str, str], Any]) -> Any:
    def _fake(method, path, body=None, *, token, **kwargs):
        key = (method, path)
        if key not in responses:
            raise AssertionError(f"unexpected fastly() call: {key}")
        value = responses[key]
        if isinstance(value, Exception):
            raise value
        return value

    return _fake


@pytest.mark.parametrize(
    ("responses", "expected"),
    [
        ({("GET", "/service/svc1/version/active"): {"active": True, "number": 7}}, 7),
        (
            {
                ("GET", "/service/svc1/version/active"): RuntimeError("HTTP 404"),
                ("GET", "/service/svc1/version"): [{"number": 1, "active": False}, {"number": 2, "active": True}],
            },
            2,
        ),
        (
            {
                ("GET", "/service/svc1/version/active"): RuntimeError("HTTP 404"),
                ("GET", "/service/svc1/version"): {"items": [{"number": 3, "active": True}]},
            },
            3,
        ),
        (
            {
                ("GET", "/service/svc1/version/active"): {"active": False},
                ("GET", "/service/svc1/version"): [{"number": 1, "active": False}],
            },
            None,
        ),
        (
            {
                ("GET", "/service/svc1/version/active"): RuntimeError("HTTP 404"),
                ("GET", "/service/svc1/version"): RuntimeError("HTTP 500"),
            },
            None,
        ),
    ],
)
def test_fetch_active_version_variants(responses, expected):
    with patch.object(fi, "fastly", _fastly_mock(responses)):
        assert fi.fetch_active_version("svc1", "tok") == expected


def test_fetch_snippets_logging_backends_and_dictionaries():
    with patch.object(
        fi,
        "fastly",
        _fastly_mock(
            {
                ("GET", "/service/svc1/version/3/snippet"): [
                    {"name": "recv1", "priority": 50, "snippet": "# vcl", "type": "vcl_recv"}
                ]
            }
        ),
    ):
        assert fi.fetch_snippets("svc1", 3, "tok") == [
            VCLSnippet(name="recv1", priority=50, body="# vcl", subroutine="vcl_recv")
        ]

    with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/snippet"): {"items": [{}]}})):
        assert fi.fetch_snippets("svc1", 3, "tok") == [
            VCLSnippet(name="", priority=100, body="", subroutine="vcl_recv")
        ]

    for bad_snip in (RuntimeError("HTTP 500"), {"not_items": []}):
        with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/snippet"): bad_snip})):
            assert fi.fetch_snippets("svc1", 3, "tok") == []

    ep_raw = {
        "name": "ep1",
        "path": "/raw/",
        "period": 60,
        "response_condition": "cond1",
        "format": "%h",
        "placement": "none",
        "response_object_name": "",
    }
    with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/logging/s3"): [ep_raw]})):
        assert fi.fetch_logging_endpoints("svc1", 3, "tok") == [
            LoggingEndpoint(
                name="ep1",
                endpoint_type="s3",
                path="/raw/",
                period=60,
                response_condition="cond1",
                format_string="%h",
                placement="none",
                response_object_name="",
            )
        ]
    with patch.object(
        fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/logging/s3"): RuntimeError("boom")})
    ):
        assert fi.fetch_logging_endpoints("svc1", 3, "tok") == []

    with patch.object(
        fi,
        "fastly",
        _fastly_mock(
            {("GET", "/service/svc1/version/3/backend"): [{"name": "origin1", "address": "origin.example.test"}]}
        ),
    ):
        assert fi.fetch_backends("svc1", 3, "tok") == [Backend(name="origin1", address="origin.example.test", port=443)]
    with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/backend"): RuntimeError("boom")})):
        assert fi.fetch_backends("svc1", 3, "tok") == []

    # Dictionaries
    with patch.object(
        fi,
        "fastly",
        _fastly_mock(
            {
                ("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": "did1", "write_only": False}],
                ("GET", "/service/svc1/dictionary/did1/items"): [
                    {"item_key": "k1", "item_value": "v1"},
                    {"key": "k2", "value": "v2"},
                ],
            }
        ),
    ):
        assert fi.fetch_dictionaries("svc1", 3, "tok") == [
            ServiceDictionary(name="d1", write_only=False, items={"k1": "v1", "k2": "v2"})
        ]

    with patch.object(
        fi,
        "fastly",
        _fastly_mock(
            {
                ("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": "did1"}],
                ("GET", "/service/svc1/dictionary/did1/items"): {"item_key": "solo", "item_value": "only"},
            }
        ),
    ):
        assert fi.fetch_dictionaries("svc1", 3, "tok")[0].items == {"solo": "only"}

    with patch.object(
        fi,
        "fastly",
        _fastly_mock(
            {
                ("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": "did1"}],
                ("GET", "/service/svc1/dictionary/did1/items"): RuntimeError("boom"),
            }
        ),
    ):
        assert fi.fetch_dictionaries("svc1", 3, "tok") == [ServiceDictionary(name="d1", write_only=False, items={})]

    with patch.object(
        fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": ""}]})
    ):
        assert fi.fetch_dictionaries("svc1", 3, "tok") == [ServiceDictionary(name="d1", items={})]

    with patch.object(
        fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/dictionary"): RuntimeError("boom")})
    ):
        assert fi.fetch_dictionaries("svc1", 3, "tok") == []


def test_clone_delete_validate_and_activate_mutations():
    with patch.object(fi, "fastly", _fastly_mock({("PUT", "/service/svc1/version/3/clone"): {"number": 4}})):
        assert fi.clone_version("svc1", 3, "tok") == 4

    calls: list[tuple[str, str, Any]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, b=None, **kw: (calls.append((m, p, b)), {"number": 9} if p.endswith("/clone") else {})[1],
    ):
        assert fi.clone_version("svc1", 3, "tok", comment="reconciler pass") == 9
    assert ("PUT", "/service/svc1/version/9", {"comment": "reconciler pass"}) in calls

    del_calls: list[tuple[str, str, bool]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, b=None, *, expect_empty=False, **kw: (del_calls.append((m, p, expect_empty)), {})[1],
    ):
        fi.delete_snippet("svc1", 3, "weird name/slash", "tok")
        fi.delete_logging_endpoint("svc1", 3, "My Endpoint", "tok")
        fi.delete_backend("svc1", 3, "origin/1", "tok")
        fi.delete_dictionary("svc1", 3, "my dict", "tok")
    assert del_calls == [
        ("DELETE", "/service/svc1/version/3/snippet/weird%20name%2Fslash", True),
        ("DELETE", "/service/svc1/version/3/logging/s3/My%20Endpoint", True),
        ("DELETE", "/service/svc1/version/3/backend/origin%2F1", True),
        ("DELETE", "/service/svc1/version/3/dictionary/my%20dict", True),
    ]

    with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/validate"): {"status": "ok"}})):
        assert fi.validate_version("svc1", 3, "tok") is True
    with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/validate"): {"status": "error"}})):
        assert fi.validate_version("svc1", 3, "tok") is False
    with patch.object(
        fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/validate"): RuntimeError("bad VCL")})
    ):
        with pytest.raises(RuntimeError, match="VCL validation failed"):
            fi.validate_version("svc1", 3, "tok")

    act_calls: list[tuple[str, str]] = []
    with patch.object(fi, "fastly", lambda m, p, **kw: (act_calls.append((m, p)), {})[1]):
        fi.activate_version("svc1", 3, "tok")
    assert act_calls == [("PUT", "/service/svc1/version/3/activate")]


def test_create_or_update_snippet_logging_backend_and_dictionary():
    # Snippet update vs create
    for exists, subr, exp_method, exp_type in (
        (True, "vcl_recv", "PUT", "recv"),
        (False, "deliver", "POST", "deliver"),
    ):
        calls: list[tuple[str, str, Any]] = []

        def _snip_fake(m, p, b=None, _exists=exists, _calls=calls, **kw):
            _calls.append((m, p, b))
            if m == "GET":
                if not _exists:
                    raise RuntimeError("HTTP 404")
                return {"name": "s"}
            return {}

        with patch.object(fi, "fastly", _snip_fake):
            fi.create_or_update_snippet(
                "svc1", 3, VCLSnippet(name="s", priority=50, body="# vcl", subroutine=subr), "tok"
            )
        assert [c[0] for c in calls] == ["GET", exp_method] and calls[1][2]["type"] == exp_type

    # Logging endpoint create vs update
    ep_calls: list[tuple[str, str, Any]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, b=None, **kw: (
            ep_calls.append((m, p, b)),
            (_ for _ in ()).throw(RuntimeError("404")) if m == "GET" else {},
        )[1],
    ):
        fi.create_or_update_logging_endpoint(
            "svc1",
            3,
            LoggingEndpoint("ep1", "s3", "/raw/", 60, "", "%h", "none", ""),
            "tok",
            bucket_name="my-bucket",
            domain="s3.example.test",
            access_key="AKIA",
            secret_key="shh",
        )
    assert [c[0] for c in ep_calls] == ["GET", "POST"]
    assert ep_calls[1][2]["bucket_name"] == "my-bucket" and "response_condition" not in ep_calls[1][2]

    ep_upd: list[tuple[str, str, Any]] = []
    with patch.object(fi, "fastly", lambda m, p, b=None, **kw: (ep_upd.append((m, p, b)), {"name": "ep1"})[1]):
        fi.create_or_update_logging_endpoint(
            "svc1", 3, LoggingEndpoint("ep1", "s3", "/raw/", 60, "always_true", "%h", "none", ""), "tok"
        )
    assert [c[0] for c in ep_upd] == ["GET", "PUT"] and ep_upd[1][2]["response_condition"] == "always_true"

    # Backend create vs update
    be_calls: list[tuple[str, str, Any]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, b=None, **kw: (
            be_calls.append((m, p, b)),
            (_ for _ in ()).throw(RuntimeError("404")) if m == "GET" else {},
        )[1],
    ):
        fi.create_or_update_backend(
            "svc1",
            3,
            Backend("origin1", "origin.example.test", 443, override_host="custom.example.test", shield="dca-dc-us"),
            "tok",
        )
    assert [c[0] for c in be_calls] == ["GET", "POST"] and be_calls[1][2]["shield"] == "dca-dc-us"

    be_upd: list[tuple[str, str, Any]] = []
    with patch.object(fi, "fastly", lambda m, p, b=None, **kw: (be_upd.append((m, p, b)), {"name": "origin1"})[1]):
        fi.create_or_update_backend("svc1", 3, Backend("origin1", "origin.example.test", 443), "tok")
    assert [c[0] for c in be_upd] == ["GET", "PUT"] and be_upd[1][2]["shield"] is None

    # Dictionary create + item upsert, update + fallback POST, missing id, empty items
    d_calls: list[tuple[str, str, Any]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, b=None, **kw: (
            d_calls.append((m, p, b)),
            (_ for _ in ()).throw(RuntimeError("404"))
            if m == "GET"
            else ({"id": "dict-id-1"} if p.endswith("/dictionary") else {}),
        )[1],
    ):
        fi.create_or_update_dictionary(
            "svc1", 3, ServiceDictionary(name="my_dict", write_only=True, items={"key1": "val1"}), "tok"
        )
    assert [(c[0], c[1].split("/")[-1]) for c in d_calls] == [
        ("GET", "my_dict"),
        ("POST", "dictionary"),
        ("PUT", "key1"),
    ]

    d_upd: list[str] = []

    def _dict_fallback(m, p, b=None, **kw):
        d_upd.append(m)
        if m == "GET":
            return {"name": "my_dict"}
        if m == "PUT" and "/dictionary/my_dict" in p:
            return {"id": "dict-id-1"}
        if m == "PUT" and "/item/new_key" in p:
            raise RuntimeError("HTTP 500")
        return {}

    with patch.object(fi, "fastly", _dict_fallback):
        fi.create_or_update_dictionary(
            "svc1", 3, ServiceDictionary(name="my_dict", items={"new_key": "new_val"}), "tok"
        )
    assert d_upd == ["GET", "PUT", "PUT", "POST"]

    with patch.object(
        fi,
        "fastly",
        lambda m, p, *a, **kw: (_ for _ in ()).throw(RuntimeError("404")) if m == "GET" else {},
    ):
        with pytest.raises(RuntimeError, match="Failed to get dictionary ID"):
            fi.create_or_update_dictionary("svc1", 3, ServiceDictionary(name="my_dict", items={"k": "v"}), "tok")

    d_empty: list[tuple[str, str]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, *a, **kw: (
            d_empty.append((m, p)),
            (_ for _ in ()).throw(RuntimeError("404")) if m == "GET" else {"id": "dict-id-1"},
        )[1],
    ):
        fi.create_or_update_dictionary("svc1", 3, ServiceDictionary(name="empty_dict", items={}), "tok")
    assert [c[0] for c in d_empty] == ["GET", "POST"]


# ── Merged from tests/provision/declarative/test_fastly_integration.py ──

"""Unit tests for backend.provision.declarative.fastly_integration.

These are thin wrappers around ``backend.core.fastly.client.fastly`` that
shape Fastly API responses into the diff module's dataclasses (or issue
mutations). The reconciler integration tests patch this whole module out,
so its own response-shaping and error-swallowing branches were otherwise
untested. No test here makes a real network call — ``fastly`` itself is
replaced with a plain callable, which is the same boundary
test_session_scoring_orchestrator.py already patches
(``backend.provision.declarative.fastly_integration.fastly``).
"""


import pytest


def _fastly_mock_fastly_integration(responses: dict[tuple[str, str], Any]) -> Any:
    """Build a fake ``fastly(method, path, body=None, *, token, ...)`` that
    dispatches on (method, path), raising RuntimeError for unmapped calls
    (matching the real client's translate-everything-to-RuntimeError
    contract) or re-raising a mapped exception instance."""

    def _fake(method, path, body=None, *, token, **kwargs):
        key = (method, path)
        if key not in responses:
            raise AssertionError(f"unexpected fastly() call: {key}")
        value = responses[key]
        if isinstance(value, Exception):
            raise value
        return value

    return _fake


# ── fetch_active_version ────────────────────────────────────────────────


def test_fetch_active_version_from_active_endpoint():
    fake = _fastly_mock_fastly_integration({("GET", "/service/svc1/version/active"): {"active": True, "number": 7}})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_active_version("svc1", "tok") == 7


def test_fetch_active_version_falls_back_to_list_when_active_endpoint_errors():
    fake = _fastly_mock_fastly_integration(
        {
            ("GET", "/service/svc1/version/active"): RuntimeError("HTTP 404"),
            ("GET", "/service/svc1/version"): [
                {"number": 1, "active": False},
                {"number": 2, "active": True},
            ],
        }
    )
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_active_version("svc1", "tok") == 2


def test_fetch_active_version_falls_back_to_items_dict_shape():
    fake = _fastly_mock_fastly_integration(
        {
            ("GET", "/service/svc1/version/active"): RuntimeError("HTTP 404"),
            ("GET", "/service/svc1/version"): {"items": [{"number": 3, "active": True}]},
        }
    )
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_active_version("svc1", "tok") == 3


def test_fetch_active_version_returns_none_when_nothing_active():
    fake = _fastly_mock_fastly_integration(
        {
            ("GET", "/service/svc1/version/active"): {"active": False},
            ("GET", "/service/svc1/version"): [{"number": 1, "active": False}],
        }
    )
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_active_version("svc1", "tok") is None


def test_fetch_active_version_returns_none_when_fallback_list_also_errors():
    def _raise_second(method, path, body=None, *, token, **kwargs):
        if path.endswith("/active"):
            raise RuntimeError("HTTP 404")
        raise RuntimeError("HTTP 500")

    with patch.object(fi, "fastly", _raise_second):
        assert fi.fetch_active_version("svc1", "tok") is None


# ── fetch_snippets / fetch_logging_endpoints / fetch_backends ───────────


def test_fetch_snippets_parses_list_response():
    fake = _fastly_mock_fastly_integration(
        {
            ("GET", "/service/svc1/version/3/snippet"): [
                {"name": "recv1", "priority": 50, "snippet": "# vcl", "type": "vcl_recv"},
            ]
        }
    )
    with patch.object(fi, "fastly", fake):
        snippets = fi.fetch_snippets("svc1", 3, "tok")
    assert snippets == [VCLSnippet(name="recv1", priority=50, body="# vcl", subroutine="vcl_recv")]


def test_fetch_snippets_parses_items_dict_response_and_defaults_missing_fields():
    fake = _fastly_mock_fastly_integration({("GET", "/service/svc1/version/3/snippet"): {"items": [{}]}})
    with patch.object(fi, "fastly", fake):
        snippets = fi.fetch_snippets("svc1", 3, "tok")
    assert snippets == [VCLSnippet(name="", priority=100, body="", subroutine="vcl_recv")]


def test_fetch_snippets_swallows_errors_and_returns_empty_list():
    fake = _fastly_mock_fastly_integration({("GET", "/service/svc1/version/3/snippet"): RuntimeError("HTTP 500")})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_snippets("svc1", 3, "tok") == []


def test_fetch_snippets_treats_unrecognized_shape_as_empty():
    fake = _fastly_mock_fastly_integration({("GET", "/service/svc1/version/3/snippet"): {"not_items": []}})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_snippets("svc1", 3, "tok") == []


def test_fetch_logging_endpoints_parses_list_response():
    fake = _fastly_mock_fastly_integration(
        {
            ("GET", "/service/svc1/version/3/logging/s3"): [
                {
                    "name": "ep1",
                    "path": "/raw/",
                    "period": 60,
                    "response_condition": "cond1",
                    "format": "%h",
                    "placement": "none",
                    "response_object_name": "",
                }
            ]
        }
    )
    with patch.object(fi, "fastly", fake):
        endpoints = fi.fetch_logging_endpoints("svc1", 3, "tok")
    assert endpoints == [
        LoggingEndpoint(
            name="ep1",
            endpoint_type="s3",
            path="/raw/",
            period=60,
            response_condition="cond1",
            format_string="%h",
            placement="none",
            response_object_name="",
        )
    ]


def test_fetch_logging_endpoints_swallows_errors():
    fake = _fastly_mock_fastly_integration({("GET", "/service/svc1/version/3/logging/s3"): RuntimeError("boom")})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_logging_endpoints("svc1", 3, "tok") == []


def test_fetch_backends_parses_list_response_with_defaults():
    fake = _fastly_mock_fastly_integration(
        {
            ("GET", "/service/svc1/version/3/backend"): [
                {"name": "origin1", "address": "origin.example.test"},
            ]
        }
    )
    with patch.object(fi, "fastly", fake):
        backends = fi.fetch_backends("svc1", 3, "tok")
    assert backends == [Backend(name="origin1", address="origin.example.test", port=443)]


def test_fetch_backends_swallows_errors():
    fake = _fastly_mock_fastly_integration({("GET", "/service/svc1/version/3/backend"): RuntimeError("boom")})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_backends("svc1", 3, "tok") == []


# ── fetch_dictionaries ───────────────────────────────────────────────────


def test_fetch_dictionaries_fetches_items_for_each_dict():
    fake = _fastly_mock_fastly_integration(
        {
            ("GET", "/service/svc1/version/3/dictionary"): [
                {"name": "d1", "id": "did1", "write_only": False},
            ],
            ("GET", "/service/svc1/dictionary/did1/items"): [
                {"item_key": "k1", "item_value": "v1"},
                {"key": "k2", "value": "v2"},
            ],
        }
    )
    with patch.object(fi, "fastly", fake):
        dicts = fi.fetch_dictionaries("svc1", 3, "tok")
    assert dicts == [ServiceDictionary(name="d1", write_only=False, items={"k1": "v1", "k2": "v2"})]


def test_fetch_dictionaries_items_dict_shape_is_wrapped_as_single_entry():
    fake = _fastly_mock_fastly_integration(
        {
            ("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": "did1"}],
            ("GET", "/service/svc1/dictionary/did1/items"): {"item_key": "solo", "item_value": "only"},
        }
    )
    with patch.object(fi, "fastly", fake):
        dicts = fi.fetch_dictionaries("svc1", 3, "tok")
    assert dicts[0].items == {"solo": "only"}


def test_fetch_dictionaries_swallows_item_fetch_errors_but_keeps_dict_with_no_items():
    fake = _fastly_mock_fastly_integration(
        {
            ("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": "did1"}],
            ("GET", "/service/svc1/dictionary/did1/items"): RuntimeError("boom"),
        }
    )
    with patch.object(fi, "fastly", fake):
        dicts = fi.fetch_dictionaries("svc1", 3, "tok")
    assert dicts == [ServiceDictionary(name="d1", write_only=False, items={})]


def test_fetch_dictionaries_skips_item_fetch_when_no_dict_id():
    fake = _fastly_mock_fastly_integration({("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": ""}]})
    with patch.object(fi, "fastly", fake):
        dicts = fi.fetch_dictionaries("svc1", 3, "tok")
    assert dicts == [ServiceDictionary(name="d1", items={})]


def test_fetch_dictionaries_outer_swallow_returns_empty_list():
    fake = _fastly_mock_fastly_integration({("GET", "/service/svc1/version/3/dictionary"): RuntimeError("boom")})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_dictionaries("svc1", 3, "tok") == []


# ── clone_version ────────────────────────────────────────────────────────


def test_clone_version_without_comment():
    fake = _fastly_mock_fastly_integration({("PUT", "/service/svc1/version/3/clone"): {"number": 4}})
    with patch.object(fi, "fastly", fake):
        assert fi.clone_version("svc1", 3, "tok") == 4


def test_clone_version_sets_comment_on_new_version():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if path.endswith("/clone"):
            return {"number": 9}
        return {}

    with patch.object(fi, "fastly", _fake):
        new_version = fi.clone_version("svc1", 3, "tok", comment="reconciler pass")

    assert new_version == 9
    assert ("PUT", "/service/svc1/version/9", {"comment": "reconciler pass"}) in calls


# ── delete_* mutations ───────────────────────────────────────────────────


def test_delete_snippet_encodes_name_and_expects_empty():
    calls = []

    def _fake(method, path, body=None, *, token, expect_empty=False, **kwargs):
        calls.append((method, path, expect_empty))
        return {}

    with patch.object(fi, "fastly", _fake):
        fi.delete_snippet("svc1", 3, "weird name/slash", "tok")

    assert calls == [("DELETE", "/service/svc1/version/3/snippet/weird%20name%2Fslash", True)]


def test_delete_logging_endpoint_encodes_name():
    calls = []

    def _fake(method, path, body=None, *, token, expect_empty=False, **kwargs):
        calls.append((method, path))
        return {}

    with patch.object(fi, "fastly", _fake):
        fi.delete_logging_endpoint("svc1", 3, "My Endpoint", "tok")

    assert calls == [("DELETE", "/service/svc1/version/3/logging/s3/My%20Endpoint")]


def test_delete_backend_encodes_name():
    calls = []

    def _fake(method, path, body=None, *, token, expect_empty=False, **kwargs):
        calls.append((method, path))
        return {}

    with patch.object(fi, "fastly", _fake):
        fi.delete_backend("svc1", 3, "origin/1", "tok")

    assert calls == [("DELETE", "/service/svc1/version/3/backend/origin%2F1")]


def test_delete_dictionary_encodes_name():
    calls = []

    def _fake(method, path, body=None, *, token, expect_empty=False, **kwargs):
        calls.append((method, path))
        return {}

    with patch.object(fi, "fastly", _fake):
        fi.delete_dictionary("svc1", 3, "my dict", "tok")

    assert calls == [("DELETE", "/service/svc1/version/3/dictionary/my%20dict")]


# ── create_or_update_snippet ─────────────────────────────────────────────


def test_create_or_update_snippet_updates_when_snippet_exists():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            return {"name": "recv1"}
        return {}

    snippet = VCLSnippet(name="recv1", priority=50, body="# vcl", subroutine="vcl_recv")
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_snippet("svc1", 3, snippet, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "PUT"]
    put_body = calls[1][2]
    assert put_body["type"] == "recv"  # vcl_ prefix stripped
    assert put_body["content"] == "# vcl"


def test_create_or_update_snippet_creates_when_snippet_missing():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            raise RuntimeError("HTTP 404 not found")
        return {}

    snippet = VCLSnippet(name="new_snip", priority=50, body="# vcl", subroutine="deliver")
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_snippet("svc1", 3, snippet, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "POST"]
    post_body = calls[1][2]
    # subroutine without "vcl_" prefix passes through unchanged
    assert post_body["type"] == "deliver"


# ── create_or_update_logging_endpoint ────────────────────────────────────


def test_create_or_update_logging_endpoint_creates_with_s3_params():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            raise RuntimeError("HTTP 404")
        return {}

    endpoint = LoggingEndpoint(
        name="ep1",
        endpoint_type="s3",
        path="/raw/",
        period=60,
        response_condition="",
        format_string="%h",
        placement="none",
        response_object_name="",
    )
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_logging_endpoint(
            "svc1",
            3,
            endpoint,
            "tok",
            bucket_name="my-bucket",
            domain="s3.example.test",
            access_key="AKIA",
            secret_key="shh",
        )

    methods = [c[0] for c in calls]
    assert methods == ["GET", "POST"]
    post_body = calls[1][2]
    assert post_body["bucket_name"] == "my-bucket"
    assert post_body["domain"] == "s3.example.test"
    assert post_body["access_key"] == "AKIA"
    assert post_body["secret_key"] == "shh"
    # empty response_condition is omitted, not sent as ""
    assert "response_condition" not in post_body


def test_create_or_update_logging_endpoint_updates_when_it_exists():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            return {"name": "ep1"}
        return {}

    endpoint = LoggingEndpoint(
        name="ep1",
        endpoint_type="s3",
        path="/raw/",
        period=60,
        response_condition="always_true",
        format_string="%h",
        placement="none",
        response_object_name="",
    )
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_logging_endpoint("svc1", 3, endpoint, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "PUT"]
    put_body = calls[1][2]
    assert put_body["response_condition"] == "always_true"
    assert "bucket_name" not in put_body


# ── create_or_update_backend ─────────────────────────────────────────────


def test_create_or_update_backend_creates_with_shield_and_override_host():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            raise RuntimeError("HTTP 404")
        return {}

    backend = Backend(
        name="origin1",
        address="origin.example.test",
        port=443,
        override_host="custom.example.test",
        shield="dca-dc-us",
    )
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_backend("svc1", 3, backend, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "POST"]
    post_body = calls[1][2]
    assert post_body["override_host"] == "custom.example.test"
    assert post_body["shield"] == "dca-dc-us"


def test_create_or_update_backend_updates_and_nulls_shield_when_absent():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            return {"name": "origin1"}
        return {}

    backend = Backend(name="origin1", address="origin.example.test", port=443)
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_backend("svc1", 3, backend, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "PUT"]
    put_body = calls[1][2]
    assert put_body["shield"] is None
    assert "override_host" not in put_body


# ── create_or_update_dictionary ───────────────────────────────────────────


def test_create_or_update_dictionary_creates_and_upserts_items_via_put():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            raise RuntimeError("HTTP 404")
        if method == "POST" and path.endswith("/dictionary"):
            return {"id": "dict-id-1"}
        return {}

    dictionary = ServiceDictionary(name="my_dict", write_only=True, items={"key1": "val1"})
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_dictionary("svc1", 3, dictionary, "tok")

    # GET (miss) -> POST create dict -> PUT item upsert
    kinds = [(c[0], c[1].split("/")[-1]) for c in calls]
    assert ("GET", "my_dict") in kinds
    assert ("POST", "dictionary") in kinds
    assert ("PUT", "key1") in kinds


def test_create_or_update_dictionary_updates_existing_and_falls_back_to_post_for_new_items():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET" and path.endswith("/dictionary/my_dict"):
            return {"name": "my_dict"}
        if method == "PUT" and "/dictionary/my_dict" in path:
            return {"id": "dict-id-1"}
        if method == "PUT" and "/item/new_key" in path:
            raise RuntimeError("HTTP 500 upsert failed")
        if method == "POST" and path.endswith("/item"):
            return {}
        raise AssertionError(f"unexpected call {method} {path}")

    dictionary = ServiceDictionary(name="my_dict", items={"new_key": "new_val"})
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_dictionary("svc1", 3, dictionary, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "PUT", "PUT", "POST"]


def test_create_or_update_dictionary_raises_when_response_missing_id():
    def _fake(method, path, body=None, *, token, **kwargs):
        if method == "GET":
            raise RuntimeError("HTTP 404")
        return {}  # POST create response has no "id"

    dictionary = ServiceDictionary(name="my_dict", items={"k": "v"})
    with patch.object(fi, "fastly", _fake):
        with pytest.raises(RuntimeError, match="Failed to get dictionary ID"):
            fi.create_or_update_dictionary("svc1", 3, dictionary, "tok")


def test_create_or_update_dictionary_skips_item_upsert_when_no_items():
    calls: list[tuple[str, str]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path))
        if method == "GET":
            raise RuntimeError("HTTP 404")
        return {"id": "dict-id-1"}

    dictionary = ServiceDictionary(name="empty_dict", items={})
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_dictionary("svc1", 3, dictionary, "tok")

    assert all(c[0] != "PATCH" for c in calls)


# ── validate_version / activate_version ──────────────────────────────────


def test_validate_version_true_on_ok_status():
    fake = _fastly_mock_fastly_integration({("GET", "/service/svc1/version/3/validate"): {"status": "ok"}})
    with patch.object(fi, "fastly", fake):
        assert fi.validate_version("svc1", 3, "tok") is True


def test_validate_version_false_on_non_ok_status():
    fake = _fastly_mock_fastly_integration({("GET", "/service/svc1/version/3/validate"): {"status": "error"}})
    with patch.object(fi, "fastly", fake):
        assert fi.validate_version("svc1", 3, "tok") is False


def test_validate_version_reraises_with_context_on_error():
    fake = _fastly_mock_fastly_integration(
        {("GET", "/service/svc1/version/3/validate"): RuntimeError("bad VCL: undefined var")}
    )
    with patch.object(fi, "fastly", fake):
        with pytest.raises(RuntimeError, match="VCL validation failed"):
            fi.validate_version("svc1", 3, "tok")


def test_activate_version_calls_activate_endpoint():
    calls = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path))
        return {}

    with patch.object(fi, "fastly", _fake):
        fi.activate_version("svc1", 3, "tok")

    assert calls == [("PUT", "/service/svc1/version/3/activate")]


class TestBuildRumLogCondition:
    """RUM beacon log-emission condition must fire only at the true edge."""

    def test_edge_guards_present_without_custom(self):
        from backend.provision.declarative.reconciler import build_rum_log_condition

        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "fos_prefix": "raw",
                "rum": {},
            }
        )
        stmt = build_rum_log_condition(state)

        assert 'req.url.path == "/rum-beacon"' in stmt
        assert 'req.method == "POST"' in stmt
        assert "fastly.ff.visits_this_service == 0" in stmt
        assert "req.restarts == 0" in stmt
        # Extraneous path negation must not be reintroduced.
        assert 'req.url.path !~ "^/api/"' not in stmt
        # No trailing custom group when none is configured.
        assert not re.search(r" && \((?!req\.restarts == 0)(.+)\)$", stmt)

    def test_custom_condition_appended_and_extractable(self):
        from backend.provision.declarative.reconciler import build_rum_log_condition

        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "fos_prefix": "raw",
                "rum": {"custom_condition": 'req.http.host == "example.com"'},
            }
        )
        stmt = build_rum_log_condition(state)

        assert "fastly.ff.visits_this_service == 0" in stmt
        assert "req.restarts == 0" in stmt
        assert stmt.endswith('&& (req.http.host == "example.com")')
        # Mirrors the readback validator in routers/services/core.py.
        m = re.search(r" && \((?!req\.restarts == 0)(.+)\)$", stmt)
        assert m is not None
        assert m.group(1) == 'req.http.host == "example.com"'
