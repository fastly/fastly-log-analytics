"""Regression coverage for ``rum_custom_condition`` validation.

``rum_custom_condition`` is AND-ed onto ``req.url.path == "/rum-beacon"`` when
building the RUM response condition, so it is evaluated on the beacon request
and ``req.url.path`` is pinned to ``/rum-beacon``. Any predicate on the path is
therefore either:

  - a tautology  — ``!~ "^/api/"`` is always true, a silent no-op; or
  - a contradiction — ``~ "^/api/"`` makes the whole condition identically
    false, disabling ALL RUM logging with no error anywhere.

Both are valid VCL, so Fastly's ``validate`` accepts them. The SE-demo service
shipped the no-op form for a month, copied from what the settings dialog used to
offer as its example.
"""

from __future__ import annotations

import pytest

from backend.core.fastly.rum_provisioning import validate_rum_custom_condition


@pytest.mark.parametrize("cond", ["", "   ", None])
def test_empty_condition_is_allowed(cond):
    assert validate_rum_custom_condition(cond) is None


@pytest.mark.parametrize(
    "cond",
    [
        'req.url.path !~ "^/api/"',  # the exact value the SE-demo service ran
        'req.url.path ~ "^/api/"',  # the kill-switch inverse
        'req.url ~ "^/api/"',
        'client.ip != "203.0.113.5" && req.url.path !~ "^/api/"',
    ],
)
def test_path_predicates_are_rejected(cond):
    err = validate_rum_custom_condition(cond)
    assert err is not None
    assert "req.url" in err
    # The message must tell the operator where page filtering actually belongs,
    # otherwise they'll just try another unworkable variant.
    assert "tracker" in err


def test_single_quotes_rejected():
    """VCL string literals are double-quoted; the old placeholder used singles."""
    err = validate_rum_custom_condition("client.ip != '192.168.1.1'")
    assert err is not None
    assert "double quotes" in err


@pytest.mark.parametrize(
    "cond",
    [
        'req.http.User-Agent !~ "(bot|crawler)"',
        'client.geo.country_code != "US"',
        'req.http.Host == "example.com"',
    ],
)
def test_workable_conditions_pass(cond):
    assert validate_rum_custom_condition(cond) is None
