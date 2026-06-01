"""Tests for record_cdn_call and X-Cache MISS → synthetic FOS op detection.

When a request to the CDN-fronted bucket is a full MISS (no edge HIT, no shield
HIT), the CDN had to fetch from FOS — that's a billable Class B GET/HEAD that
we never observed directly. We synthesize an FOS row alongside the CDN row so
the usage log reflects real cost.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def fresh_call_tracking():
    from backend.utils.telemetry import _CALLS, start_call_tracking

    start_call_tracking()
    yield
    _CALLS.set([])


class TestIsFullMiss:
    def test_single_hit(self):
        from backend.utils.telemetry import _is_full_miss

        assert _is_full_miss("HIT") is False

    def test_chained_hit_hit(self):
        from backend.utils.telemetry import _is_full_miss

        assert _is_full_miss("HIT, HIT") is False

    def test_edge_miss_shield_hit(self):
        """MISS at edge but shield served it — no FOS read happened."""
        from backend.utils.telemetry import _is_full_miss

        assert _is_full_miss("MISS, HIT") is False

    def test_full_miss(self):
        from backend.utils.telemetry import _is_full_miss

        assert _is_full_miss("MISS, MISS") is True

    def test_pass(self):
        """PASS = uncacheable request, always goes to FOS."""
        from backend.utils.telemetry import _is_full_miss

        assert _is_full_miss("PASS") is True

    def test_pass_pass(self):
        from backend.utils.telemetry import _is_full_miss

        assert _is_full_miss("PASS, PASS") is True

    def test_mixed_pass_miss(self):
        from backend.utils.telemetry import _is_full_miss

        assert _is_full_miss("MISS, PASS") is True

    def test_empty(self):
        from backend.utils.telemetry import _is_full_miss

        assert _is_full_miss("") is False
        assert _is_full_miss(None) is False

    def test_case_insensitive(self):
        from backend.utils.telemetry import _is_full_miss

        assert _is_full_miss("miss, miss") is True
        assert _is_full_miss("Hit, Hit") is False


class TestRecordCdnCall:
    def test_hit_records_only_cdn_op(self):
        from backend.utils.telemetry import get_tracked_calls, record_cdn_call

        record_cdn_call("GET", "key.parquet", 12.3, headers={"X-Cache": "HIT, HIT"}, bytes_count=1024)

        calls = get_tracked_calls()
        assert len(calls) == 1
        assert calls[0]["service"] == "CDN"
        assert calls[0]["method"] == "GET"
        assert calls[0]["bytes"] == 1024

    def test_full_miss_records_cdn_plus_fos(self):
        from backend.utils.telemetry import get_tracked_calls, record_cdn_call

        record_cdn_call("GET", "key.parquet", 99.0, headers={"X-Cache": "MISS, MISS"}, bytes_count=2048)

        calls = get_tracked_calls()
        assert len(calls) == 2
        services = [c["service"] for c in calls]
        assert services == ["CDN", "FOS"]
        assert calls[1]["method"] == "GET_OBJECT"
        assert calls[1]["bytes"] == 2048
        assert "synthesized from CDN MISS" in (calls[1]["details"] or "")

    def test_head_miss_synthesizes_get_object_not_head_object(self):
        """Fastly fetches the full body on origin MISS regardless of client
        method (to populate cache for subsequent reads), so the real FOS-side
        op is always GET_OBJECT — even when the client sent HEAD. Field-
        confirmed by tracing single-file ingest paths: client HEAD MISS
        warmed Fastly's cache, and every CDN GET that followed was a HIT,
        meaning the FOS read happened exactly once on the HEAD path."""
        from backend.utils.telemetry import get_tracked_calls, record_cdn_call

        record_cdn_call("HEAD", "key.parquet", 50.0, headers={"X-Cache": "MISS, MISS"}, bytes_count=None)

        calls = get_tracked_calls()
        assert len(calls) == 2
        assert calls[1]["method"] == "GET_OBJECT"
        assert calls[1]["service"] == "FOS"
        assert "synthesized from CDN MISS" in (calls[1]["details"] or "")

    def test_edge_miss_shield_hit_no_fos_op(self):
        """The shield served it — no FOS read, just the CDN op."""
        from backend.utils.telemetry import get_tracked_calls, record_cdn_call

        record_cdn_call("GET", "key", 30.0, headers={"X-Cache": "MISS, HIT"}, bytes_count=512)

        calls = get_tracked_calls()
        assert len(calls) == 1
        assert calls[0]["service"] == "CDN"

    def test_no_headers_records_only_cdn(self):
        """When we don't have headers we can't tell — don't guess the FOS op."""
        from backend.utils.telemetry import get_tracked_calls, record_cdn_call

        record_cdn_call("GET", "key", 10.0, headers=None, bytes_count=100)

        calls = get_tracked_calls()
        assert len(calls) == 1
        assert calls[0]["service"] == "CDN"

    def test_pass_synthesizes_fos_op(self):
        from backend.utils.telemetry import get_tracked_calls, record_cdn_call

        record_cdn_call("GET", "uncacheable", 80.0, headers={"X-Cache": "PASS"}, bytes_count=4096)

        calls = get_tracked_calls()
        assert len(calls) == 2
        assert calls[1]["service"] == "FOS"
        assert calls[1]["method"] == "GET_OBJECT"
