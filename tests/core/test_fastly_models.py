"""Typed Fastly stats + NGWAF response models: consumed fields only, unknown fields tolerated."""

from __future__ import annotations

import pytest

from backend.core.fastly.models import NgwafWorkspacesResponse, StatsRecord, StatsResponse


def test_stats_record_flattened_fos_ops():
    rec = StatsRecord.model_validate(
        {
            "start_time": 1760000000,
            "object_storage_class_a_operations_count": 10,
            "object_storage_class_b_operations_count": "20",
            "some_future_field": {"x": 1},
        }
    )
    assert rec.fos_ops() == (10, 20)
    assert rec.start_time == 1760000000
    assert rec.model_extra["some_future_field"] == {"x": 1}


def test_stats_record_nested_fos_ops_fallback():
    rec = StatsRecord.model_validate({"object_storage": {"class_a_operations_count": 3, "class_b_operations_count": 4}})
    assert rec.fos_ops() == (3, 4)


@pytest.mark.parametrize("payload", [{}, {"object_storage": None}, {"object_storage": "bad"}])
def test_stats_record_no_fos_data_is_zero(payload):
    assert StatsRecord.model_validate(payload).fos_ops() == (0, 0)


def test_stats_record_null_and_garbage_counts_coerce_to_zero():
    rec = StatsRecord.model_validate({"requests": None, "edge_requests": "n/a", "start_time": None})
    assert rec.requests == 0
    assert rec.edge_requests == 0
    assert rec.start_time is None


def test_stats_response_tolerates_missing_or_null_data():
    assert StatsResponse.model_validate({}).data == []
    assert StatsResponse.model_validate({"data": None}).data == []
    resp = StatsResponse.model_validate({"data": [{"requests": 5}], "meta": {"to": 1}})
    assert resp.data[0].requests == 5


def test_ngwaf_workspaces_data_shape_and_name_fallbacks():
    resp = NgwafWorkspacesResponse.model_validate(
        {
            "data": [
                {"id": "w1", "name": "Prod"},
                {"id": "w2", "attributes": {"name": "Attr Name"}},
                {"id": "w3"},
            ],
            "meta": {"limit": 100},
        }
    )
    assert resp.workspaces_list() == [
        {"id": "w1", "name": "Prod"},
        {"id": "w2", "name": "Attr Name"},
        {"id": "w3", "name": "w3"},
    ]


def test_ngwaf_workspaces_legacy_workspaces_key_and_empty_list():
    assert NgwafWorkspacesResponse.model_validate({"workspaces": [{"id": "a", "name": "A"}]}).workspaces_list() == [
        {"id": "a", "name": "A"}
    ]
    # present-but-empty "data" must not fall through to "workspaces"
    assert NgwafWorkspacesResponse.model_validate({"data": [], "workspaces": [{"id": "a"}]}).workspaces_list() == []
