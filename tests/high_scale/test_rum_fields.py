"""RUM beacon environment/geography extraction shared by the fact writers."""

from __future__ import annotations

import pytest

from backend.high_scale._rum_fields import rum_environment, rum_error_position


def test_flat_beacon_keys_are_extracted_verbatim() -> None:
    env = rum_environment(
        {
            "browser": "Firefox",
            "os": "Linux",
            "device": "Desktop",
            "country": "DE",
            "city": "Berlin",
            "region": "BE",
            "pop": "FRA",
            "tls": "1.3",
            "ttfb": 0.042,
        }
    )

    assert env == {
        "country": "DE",
        "browser": "Firefox",
        "os": "Linux",
        "device": "Desktop",
        "city": "Berlin",
        "region": "BE",
        "pop": "FRA",
        "tls": "1.3",
        "ttfb": 0.042,
    }


def test_edge_log_aliases_are_accepted_for_geography_and_connection_fields() -> None:
    env = rum_environment(
        {
            "geo_country_code": "FR",
            "geo_city": "Paris",
            "geo_region": "IDF",
            "server_pop": "CDG",
            "tls_version": "1.2",
            "time_to_first_byte": "88",
        }
    )

    assert env["country"] == "FR" and env["city"] == "Paris" and env["region"] == "IDF"
    assert env["pop"] == "CDG" and env["tls"] == "1.2" and env["ttfb"] == 88.0


def test_missing_values_stay_empty_and_are_never_defaulted_to_a_browser() -> None:
    env = rum_environment({"browser": "  ", "os": None, "ttfb": "not-a-number"})

    assert env["browser"] == "" and env["os"] == "" and env["device"] == ""
    assert env["country"] == "" and env["pop"] == "" and env["tls"] == ""
    assert env["ttfb"] is None


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), True, ""])
def test_non_finite_or_non_numeric_ttfb_is_dropped(bad) -> None:
    assert rum_environment({"ttfb": bad})["ttfb"] is None


def test_numeric_strings_in_text_fields_are_kept_as_text() -> None:
    assert rum_environment({"tls": 1.3, "region": 13})["tls"] == "1.3"
    assert rum_environment({"region": 13})["region"] == "13"


def test_error_position_reads_the_rum_prefixed_or_plain_keys() -> None:
    assert rum_error_position({"rum_error_line": "42", "rum_error_col": 7}) == (42, 7)
    assert rum_error_position({"error_line": 5, "error_col": "9"}) == (5, 9)


@pytest.mark.parametrize("row", [{}, {"rum_error_line": "abc"}, {"rum_error_line": -3, "rum_error_col": None}])
def test_unknown_or_invalid_error_positions_are_zero(row) -> None:
    assert rum_error_position(row) == (0, 0)
