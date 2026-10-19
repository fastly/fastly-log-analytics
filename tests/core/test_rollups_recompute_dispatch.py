"""Pins which per-hour writers ``recompute_touched_hours`` fires, in what order,
and that one writer's failure neither stops the rest nor propagates."""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import patch

import pytest

from tests._helpers.dates import past_hour as _past_hour

pytestmark = pytest.mark.usefixtures("frozen_clock")

EXPECTED_ORDER = (
    "build_time_series_bundles",
    "build_session_bundles",
    "build_slow_urls_bundles",
    "build_origin_summary_bundles",
    "build_network_rtt_bundles",
    "build_network_speed_bundles",
    "build_network_heatmap_bundles",
    "build_network_geo_bundles",
    "build_verified_bots_ts_bundles",
    "build_perf_latency_bundles",
    "build_origin_dims_bundles",
    "build_origin_latency_ts_bundles",
    "build_security_dims_bundles",
    "build_perf_dims_bundles",
    "build_ngwaf_bots_bundles",
    "build_overview_bundles",
    "build_network_summary_bundles",
)


def _stub_core(stack: ExitStack) -> None:
    stack.enter_context(patch("backend.core.rollups.recompute._safe_table_for", return_value="logs_x"))
    stack.enter_context(patch("backend.core.rollups.recompute._get_fields", return_value=["ip"]))
    stack.enter_context(patch("backend.core.rollups.recompute._run_per_field_copy"))
    stack.enter_context(patch("backend.core.rollups.recompute._run_ip_spread_per_field"))
    stack.enter_context(patch("backend.core.rollups.recompute.bundle_hours"))
    stack.enter_context(patch("backend.core.rollups.recompute.bundle_hours_ip_spread"))


def test_recompute_fires_every_hour_writer_in_order_with_touched_hours():
    from backend.core.rollups import recompute

    h, _ = _past_hour(2)
    calls: list[tuple[str, str, list[str]]] = []
    with ExitStack() as stack:
        _stub_core(stack)
        for name in EXPECTED_ORDER:
            stack.enter_context(
                patch(
                    f"backend.core.rollups.recompute.{name}",
                    side_effect=lambda sid, src, hours, _n=name: calls.append((_n, sid, list(hours))),
                )
            )
        recompute.recompute_touched_hours("svc", {"name": "svc"}, {h})

    assert [c[0] for c in calls] == list(EXPECTED_ORDER)
    assert all(c[1] == "svc" and c[2] == [h] for c in calls)


@pytest.mark.parametrize("failing", EXPECTED_ORDER)
def test_one_failing_writer_does_not_stop_the_others(failing, caplog):
    from backend.core.rollups import recompute

    h, _ = _past_hour(2)
    called: list[str] = []
    with ExitStack() as stack:
        _stub_core(stack)
        for name in EXPECTED_ORDER:
            if name == failing:
                effect = RuntimeError("boom")
            else:

                def effect(sid, src, hours, _n=name):
                    called.append(_n)

            stack.enter_context(patch(f"backend.core.rollups.recompute.{name}", side_effect=effect))
        with caplog.at_level("WARNING"):
            recompute.recompute_touched_hours("svc", {"name": "svc"}, {h})

    assert called == [n for n in EXPECTED_ORDER if n != failing]
    assert any("bundle failed" in r.getMessage() and "boom" in r.getMessage() for r in caplog.records)


def test_registry_matches_expected_order_and_resolves_on_recompute():
    from backend.core.rollups import recompute
    from backend.core.rollups.spec import HOUR_WRITERS

    assert tuple(s.builder for s in HOUR_WRITERS) == EXPECTED_ORDER
    assert len({s.label for s in HOUR_WRITERS}) == len(HOUR_WRITERS)
    assert all(callable(getattr(recompute, s.builder)) for s in HOUR_WRITERS)
