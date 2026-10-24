"""Tests for the VclBuilder VCL-assembly primitives (B15 consolidation)."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime

from backend.core.fastly.rum_provisioning import RUM_ASSET_FETCH_NAME, generate_rum_asset_fetch_vcl
from backend.provision import cmcd_orchestrator as orch
from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES, reconcile_cmcd_custom_fields
from backend.provision.declarative.generators import generate_consolidated_snippet
from backend.provision.declarative.state import FeatureState
from backend.provision.log_paths import (
    ANALYTICS_LOG_LAYOUT,
    analytics_log_path,
    minute_list_prefix,
    rum_log_path,
    rum_minute_list_prefix,
)
from backend.provision.vcl_builder import VclBuilder


class TestLineMode:
    def test_raw_and_build(self) -> None:
        b = VclBuilder()
        b.raw("set req.http.A = 1;").raw("set req.http.B = 2;")
        assert b.build() == "set req.http.A = 1;\nset req.http.B = 2;"

    def test_line_indent(self) -> None:
        b = VclBuilder()
        b.line("outer").line("inner", indent=1).line("deeper", indent=2)
        assert b.build() == "outer\n  inner\n    deeper"

    def test_blank_line_not_indented(self) -> None:
        b = VclBuilder()
        b.line("x").blank().line("y", indent=1)
        assert b.build() == "x\n\n  y"

    def test_lines_bulk(self) -> None:
        b = VclBuilder()
        b.lines(["a", "b"], indent=1)
        assert b.build() == "  a\n  b"

    def test_join_lines_static(self) -> None:
        assert VclBuilder.join_lines(["a", "b", "c"]) == "a\nb\nc"


class TestSectionMode:
    def test_join_sections_static_matches_legacy_idiom(self) -> None:
        sections = ["a", "", "  ", "b"]
        assert VclBuilder.join_sections(sections) == "\n\n".join(s for s in sections if s.strip())


class TestIndentStatic:
    def test_indents_each_content_line(self) -> None:
        assert VclBuilder.indent("a\nb") == "  a\n  b"

    def test_preserves_blank_lines(self) -> None:
        assert VclBuilder.indent("a\n\nb") == "  a\n\n  b"

    def test_levels(self) -> None:
        assert VclBuilder.indent("x", levels=2) == "    x"


# ── CMCD custom-field cleanup ────────────────────────────────────────────────


def test_remove_custom_fields_strips_cmcd_fields_only():
    cfg = {
        "log_fields": {
            "custom_fields": reconcile_cmcd_custom_fields([{"name": "mine"}], enabled=True),
        }
    }
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert _CMCD_FIELD_NAMES <= names and "mine" in names

    orch._remove_cmcd_custom_fields(cfg)
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert not (_CMCD_FIELD_NAMES & names)
    assert "mine" in names


def test_remove_custom_fields_tolerates_missing_blocks():
    for cfg in ({}, {"log_fields": {}}, {"log_fields": {"custom_fields": []}}):
        orch._remove_cmcd_custom_fields(cfg)  # must not raise


# ── FOS log path templates ───────────────────────────────────────────────────


class TestAnalyticsLogPath:
    def test_fixed_root(self):
        assert analytics_log_path() == "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


class TestRumLogPath:
    def test_fixed_root(self):
        assert rum_log_path() == "raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


class TestMinuteListPrefix:
    def test_no_prefix(self):
        dt = datetime(2026, 8, 27, 16, 53, tzinfo=UTC)
        assert minute_list_prefix(dt) == "raw/request/year=2026/month=08/day=27/hour=16/minute=53/"


class TestRumMinuteListPrefix:
    def test_no_prefix(self):
        dt = datetime(2026, 8, 27, 16, 53, tzinfo=UTC)
        assert rum_minute_list_prefix(dt) == "raw/rum/year=2026/month=08/day=27/hour=16/minute=53/"


class TestProducersUseCanonicalLayout:
    def test_declarative_generator_matches(self):
        import backend.provision.declarative.generators as gen

        src = inspect.getsource(gen)
        assert "raw/%Y-%m-%d" not in src, "declarative generator must not use the dash layout"
        assert "log_paths" in src, "declarative generator must build paths via log_paths"

    def test_fastly_api_has_no_dash_layout_literal(self):
        import backend.provision.fastly_api as fa

        src = inspect.getsource(fa)
        assert "raw/%Y-%m-%d" not in src, "add_logging_endpoint must use log_paths, not the dash layout"

    def test_services_router_has_no_dash_layout_literal(self):
        import backend.routers.services.core as sc

        src = inspect.getsource(sc)
        assert "raw/%Y-%m-%d" not in src, "logging-settings update must use log_paths, not the dash layout"

    def test_layout_constant_matches_live_bucket_shape(self):
        assert ANALYTICS_LOG_LAYOUT == "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


# ── RUM recv VCL single source of truth ──────────────────────────────────────


def _minimal_rum_state(*, faro_version: str | None) -> FeatureState:
    return FeatureState(
        service_id="svc_test",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="",
        fos_endpoint="fos.example.com",
        logging_enabled=False,
        rum_enabled=True,
        faro_version=faro_version,
    )


def test_vcl_recv_asset_routing_sourced_from_rum_provisioning_without_faro():
    state = _minimal_rum_state(faro_version=None)
    shipped_recv = generate_consolidated_snippet(state, "vcl_recv")
    shared_copy = generate_rum_asset_fetch_vcl("iad-va-us")[RUM_ASSET_FETCH_NAME]
    assert shared_copy in shipped_recv


def test_vcl_recv_asset_routing_sourced_from_rum_provisioning_with_faro():
    state = _minimal_rum_state(faro_version="2.9.0")
    shipped_recv = generate_consolidated_snippet(state, "vcl_recv")
    shared_copy = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")[RUM_ASSET_FETCH_NAME]
    assert shared_copy in shipped_recv
