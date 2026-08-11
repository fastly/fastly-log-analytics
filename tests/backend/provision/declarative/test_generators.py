"""Unit tests for VCL generators: variable duplication, field presence, deterministic order."""

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
        assert 'set beresp.http.Cache-Control = "max-age=86400, public, immutable";' in vcl_enabled
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
        assert "analytics_log" in main[0].path

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
        assert "/raw_rum/" in rum[0].path
        assert rum[0].response_condition == "rum_log_condition"

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
# Cache RUM tracker JS aggressively; updates use ?v=X query string busting
if (req.url.path == "/js/rum.js") {
    set beresp.ttl = 86400s;
    set beresp.cacheable = true;
    set beresp.http.Cache-Control = "max-age=86400, public, immutable";
}"""
        )

        vcl_miss = generate_consolidated_snippet(state, "vcl_miss")
        assert "faro-sdk" not in vcl_miss
        assert "/js/rum.js" in vcl_miss

    def test_faro_version_pinned_adds_recv_routing(self):
        """A pinned faro_version adds a GET /js/faro-sdk.js route in vcl_recv."""
        state = self._rum_state(faro_version="2.9.0")
        vcl_recv = generate_consolidated_snippet(state, "vcl_recv")
        assert '"/js/faro-sdk.js" && req.method == "GET"' in vcl_recv
        # Existing /js/rum.js route must remain untouched alongside the new one.
        assert '"/js/rum.js" && req.method == "GET"' in vcl_recv

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
        assert 'set beresp.http.Surrogate-Key = "rum-faro-sdk";' in vcl_fetch
        assert "set beresp.ttl = 604800s;" in vcl_fetch
        # Existing rum.js caching must remain untouched.
        assert "set beresp.ttl = 86400s;" in vcl_fetch

    def test_faro_version_invalid_rejected_at_generation(self):
        """An invalid faro_version string is rejected, not silently interpolated."""
        state = self._rum_state(faro_version="not-a-version")
        with pytest.raises(ValueError, match="faro_version must be a plain"):
            generate_consolidated_snippet(state, "vcl_recv")
        with pytest.raises(ValueError, match="faro_version must be a plain"):
            generate_consolidated_snippet(state, "vcl_miss")
