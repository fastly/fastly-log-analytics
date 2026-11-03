from __future__ import annotations

import numpy as np
import pytest

from scripts.lib.log_factory import generate_columns

HOUR_MS = 3_600_000
ORIGIN_FETCH = {"MISS", "PASS", "ERROR"}


def _cols(scenario: str, n: int = 4000) -> dict:
    return generate_columns(n, 0, 24 * HOUR_MS, scenario, np.random.default_rng(7))


@pytest.mark.parametrize("scenario", ["diurnal", "origin-5xx-outage", "bot-scrape", "ddos-spike", "slow-network"])
def test_origin_fields_are_set_on_origin_fetches_and_null_on_cache_hits(scenario: str) -> None:
    cols = _cols(scenario)
    cache = np.asarray(cols["cache"])
    fetched = np.isin(cache, list(ORIGIN_FETCH))
    ost = np.asarray(cols["ost"], dtype=object)
    oip = np.asarray(cols["oip"], dtype=object)
    assert fetched.any() and (~fetched).any()
    assert all(v is not None for v in ost[fetched]) and all(v is not None for v in oip[fetched])
    assert all(v is None for v in ost[~fetched]) and all(v is None for v in oip[~fetched])


def test_origin_ips_come_from_a_small_pool_so_ip_health_has_repeat_origins() -> None:
    oip = {v for v in np.asarray(_cols("diurnal")["oip"], dtype=object) if v is not None}
    assert 2 <= len(oip) <= 16


def test_outage_window_carries_origin_5xx() -> None:
    cols = _cols("origin-5xx-outage", n=20000)
    cache = np.asarray(cols["cache"])
    ost = np.asarray(cols["ost"], dtype=object)
    five_xx = [v for v, c in zip(ost, cache, strict=True) if c in ORIGIN_FETCH and v is not None and v >= 500]
    assert len(five_xx) > 100


def test_request_header_size_and_connection_reuse_are_populated_for_security_rollups() -> None:
    cols = _cols("diurnal")
    hdr = np.asarray(cols["req_header_bytes"])
    reqs = np.asarray(cols["conn_requests"])
    assert hdr.min() >= 120 and len(np.unique(hdr)) > 50
    assert reqs.min() >= 1 and reqs.max() > 20 and (reqs == 1).any()
