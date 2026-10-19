"""``/api/query`` typed response keeps the pre-typing ``jsonable_encoder`` wire bytes."""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from unittest.mock import patch

from fastapi.encoders import jsonable_encoder

from tests.conftest import MOCK_SERVICE_ID


def _result() -> dict:
    return {
        "columns": ["ts", "tz_ts", "amount", "n"],
        "data": [
            {
                "ts": dt.datetime(2026, 1, 2, 3, 4, 5),
                "tz_ts": dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
                "amount": Decimal("12.50"),
                "n": 7,
            }
        ],
        "row_count": 1,
        "total_rows": 1,
        "truncated": False,
        "elapsed_ms": 3,
        "explain_plan": "plan",
        "debug_queries": [],
        "debug_calls": [],
        "section_timings": [],
    }


def test_query_rows_encode_like_jsonable_encoder(client):
    result = _result()
    expected_rows = json.loads(json.dumps(jsonable_encoder(result["data"])))
    with patch("backend.repositories.query.execute_query", return_value=result):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"] == expected_rows
    assert body["data"][0]["tz_ts"].endswith("+00:00")
    assert body["data"][0]["amount"] == 12.5
    assert {k: body[k] for k in ("columns", "row_count", "total_rows", "truncated", "elapsed_ms", "explain_plan")} == {
        k: result[k] for k in ("columns", "row_count", "total_rows", "truncated", "elapsed_ms", "explain_plan")
    }


def test_query_omits_explain_plan_when_producer_did_not_set_it(client):
    result = _result()
    del result["explain_plan"]
    with patch("backend.repositories.query.execute_query", return_value=result):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )
    assert resp.status_code == 200
    assert "explain_plan" not in resp.json()
