"""Tests for backend.utils.pop_utils.

The POP cache backs the dashboard's choropleth + POP latency table. Stale
or malformed data here surfaces as missing dots on the map or broken
distance calculations. Three thin functions to cover:
- ``fetch_pop_locations`` — hits the Fastly API and writes the cache
- ``get_pop_locations`` — reads the cache (empty list on miss)
- ``get_pop_lat_lon_map`` — derived view; skips entries with bad shape
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from backend.utils import pop_utils


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    cache = tmp_path / "pop_locations.json"
    monkeypatch.setattr(pop_utils, "CACHE_FILE", str(cache))
    return cache


# ── fetch_pop_locations ──────────────────────────────────────────────────────


def test_fetch_returns_false_for_empty_api_key(isolated_cache):
    assert pop_utils.fetch_pop_locations("") is False
    assert pop_utils.fetch_pop_locations(None) is False  # type: ignore[arg-type]
    # Nothing was written
    assert not isolated_cache.exists()


def test_fetch_writes_cache_on_success(isolated_cache):
    sample = [{"code": "JFK", "coordinates": {"latitude": 40.6, "longitude": -73.8}}]

    with patch("backend.core.fastly.client.fastly", return_value=sample):
        ok = pop_utils.fetch_pop_locations("api-key")

    assert ok is True
    assert isolated_cache.exists()
    assert json.loads(isolated_cache.read_text()) == sample


def test_fetch_returns_false_on_network_error(isolated_cache):
    with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("boom")):
        ok = pop_utils.fetch_pop_locations("api-key")
    assert ok is False
    assert not isolated_cache.exists()


def test_fetch_passes_api_key_to_fastly_client(isolated_cache):
    """The Fastly-Key header is what the API authenticates on; fastly()
    sets it from the token kwarg. Verify the kwarg is threaded through —
    a missing/wrong token returns the API's public dataset (different
    shape) and silently breaks the cache.
    """
    with patch("backend.core.fastly.client.fastly", return_value=[]) as mock_fastly:
        pop_utils.fetch_pop_locations("my-secret-token")

    mock_fastly.assert_called_once_with("GET", "/datacenters", token="my-secret-token")


# ── get_pop_locations ────────────────────────────────────────────────────────


def test_get_returns_empty_when_cache_missing(isolated_cache):
    assert pop_utils.get_pop_locations() == []


def test_get_returns_cached_data(isolated_cache):
    data = [{"code": "LHR", "coordinates": {"latitude": 51.5, "longitude": -0.1}}]
    isolated_cache.write_text(json.dumps(data))
    assert pop_utils.get_pop_locations() == data


def test_get_returns_empty_for_corrupt_cache(isolated_cache):
    """A truncated or non-JSON file must not raise — just return empty."""
    isolated_cache.write_text("{ not valid json")
    assert pop_utils.get_pop_locations() == []


# ── get_pop_lat_lon_map ──────────────────────────────────────────────────────


def test_lat_lon_map_extracts_coords_per_code(isolated_cache):
    data = [
        {"code": "JFK", "coordinates": {"latitude": 40.6, "longitude": -73.8}},
        {"code": "LHR", "coordinates": {"latitude": 51.5, "longitude": -0.1}},
    ]
    isolated_cache.write_text(json.dumps(data))
    got = pop_utils.get_pop_lat_lon_map()
    assert got == {"JFK": (40.6, -73.8), "LHR": (51.5, -0.1)}


def test_lat_lon_map_skips_entries_missing_required_fields(isolated_cache):
    """A POP without coordinates (or with missing latitude/longitude) must
    be silently skipped — the alternative is a crash in the consumer."""
    data = [
        {"code": "GOOD", "coordinates": {"latitude": 1.0, "longitude": 2.0}},
        {"code": "NO_COORDS"},  # no coordinates key
        {"code": "PARTIAL", "coordinates": {"latitude": 3.0}},  # missing longitude
        {"coordinates": {"latitude": 4.0, "longitude": 5.0}},  # no code
    ]
    isolated_cache.write_text(json.dumps(data))
    got = pop_utils.get_pop_lat_lon_map()
    assert got == {"GOOD": (1.0, 2.0)}


def test_lat_lon_map_returns_empty_for_non_list_cache(isolated_cache):
    """Defensive: if the cache ever contains a dict instead of a list
    (legacy format, manual edit, etc.), don't crash."""
    isolated_cache.write_text(json.dumps({"some": "object"}))
    assert pop_utils.get_pop_lat_lon_map() == {}
