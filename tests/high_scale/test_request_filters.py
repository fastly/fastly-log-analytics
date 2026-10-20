"""Request-filter translation shared by the high-scale raw readers."""

from __future__ import annotations

import pytest

from backend.high_scale._filters import request_filter_clauses
from backend.models.common import FilterSpec


def test_include_and_exclude_become_typed_in_predicates() -> None:
    clauses, params = request_filter_clauses(
        {
            "country": FilterSpec(mode="include", values=["US", "CA"]),
            "status": {"mode": "exclude", "values": [404, "500"]},
        }
    )

    assert clauses == [
        "(country IN ({flt_0_0:String}, {flt_0_1:String}))",
        "(custom_fields[{flt_k1:String}] NOT IN ({flt_1_0:String}, {flt_1_1:String}))",
    ]
    assert params == {
        "flt_0_0": "US",
        "flt_0_1": "CA",
        "flt_k1": "status",
        "flt_1_0": "404",
        "flt_1_1": "500",
    }


def test_ip_aliases_resolve_to_the_client_ip_column() -> None:
    for key in ("ip", "client_ip", "IP", "xfilter_ip_2"):
        clauses, _ = request_filter_clauses({key: {"mode": "include", "values": ["203.0.113.7"]}})
        assert clauses == ["(client_ip IN ({flt_0_0:String}))"]


def test_hostile_keys_never_reach_the_sql_text() -> None:
    clauses, params = request_filter_clauses({"status'] OR 1=1 --": {"mode": "include", "values": ["x"]}})

    sql = " ".join(clauses)
    assert "OR 1=1" not in sql and "'" not in sql
    assert params["flt_k0"] == "statusor11"


def test_blank_values_and_empty_filters_are_skipped() -> None:
    clauses, params = request_filter_clauses(
        {"country": {"mode": "include", "values": ["", "  "]}, "pop": {"mode": "include", "values": []}}
    )

    assert clauses == [] and params == {}


def test_null_values_mean_the_map_field_is_absent() -> None:
    include, _ = request_filter_clauses({"pop": {"mode": "include", "values": [None, "SJC"]}})
    exclude, _ = request_filter_clauses({"pop": {"mode": "exclude", "values": [None]}})

    assert include == ["(custom_fields[{flt_k0:String}] IN ({flt_0_0:String}) OR custom_fields[{flt_k0:String}] = '')"]
    assert exclude == ["(custom_fields[{flt_k0:String}] != '')"]


def test_float_fields_filter_on_their_rounded_integer() -> None:
    clauses, params = request_filter_clauses({"ttl": {"mode": "include", "values": [3600]}})

    assert clauses == [
        "(toString(toInt32(round(toFloat64OrZero(custom_fields[{flt_k0:String}])))) IN ({flt_0_0:String}))"
    ]
    assert params["flt_k0"] == "ttl"


@pytest.mark.parametrize("key", ["_bot_name", "_ngwaf_bot_name", "_tunnel_requests", "waf_sig_ind"])
def test_filters_the_fact_table_cannot_express_raise(key: str) -> None:
    with pytest.raises(ValueError, match="do not support filtering"):
        request_filter_clauses({key: {"mode": "include", "values": ["x"]}})


def test_no_filters_yield_no_clauses() -> None:
    assert request_filter_clauses(None) == ([], {})
    assert request_filter_clauses({}) == ([], {})
