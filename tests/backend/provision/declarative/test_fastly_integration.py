"""Unit tests for backend.provision.declarative.fastly_integration.

These are thin wrappers around ``backend.core.fastly.client.fastly`` that
shape Fastly API responses into the diff module's dataclasses (or issue
mutations). The reconciler integration tests patch this whole module out,
so its own response-shaping and error-swallowing branches were otherwise
untested. No test here makes a real network call — ``fastly`` itself is
replaced with a plain callable, which is the same boundary
test_session_scoring_orchestrator.py already patches
(``backend.provision.declarative.fastly_integration.fastly``).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from backend.provision.declarative import fastly_integration as fi
from backend.provision.declarative.diff import Backend, LoggingEndpoint, ServiceDictionary, VCLSnippet


def _fastly_mock(responses: dict[tuple[str, str], Any]) -> Any:
    """Build a fake ``fastly(method, path, body=None, *, token, ...)`` that
    dispatches on (method, path), raising RuntimeError for unmapped calls
    (matching the real client's translate-everything-to-RuntimeError
    contract) or re-raising a mapped exception instance."""

    def _fake(method, path, body=None, *, token, **kwargs):
        key = (method, path)
        if key not in responses:
            raise AssertionError(f"unexpected fastly() call: {key}")
        value = responses[key]
        if isinstance(value, Exception):
            raise value
        return value

    return _fake


# ── fetch_active_version ────────────────────────────────────────────────


def test_fetch_active_version_from_active_endpoint():
    fake = _fastly_mock({("GET", "/service/svc1/version/active"): {"active": True, "number": 7}})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_active_version("svc1", "tok") == 7


def test_fetch_active_version_falls_back_to_list_when_active_endpoint_errors():
    fake = _fastly_mock(
        {
            ("GET", "/service/svc1/version/active"): RuntimeError("HTTP 404"),
            ("GET", "/service/svc1/version"): [
                {"number": 1, "active": False},
                {"number": 2, "active": True},
            ],
        }
    )
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_active_version("svc1", "tok") == 2


def test_fetch_active_version_falls_back_to_items_dict_shape():
    fake = _fastly_mock(
        {
            ("GET", "/service/svc1/version/active"): RuntimeError("HTTP 404"),
            ("GET", "/service/svc1/version"): {"items": [{"number": 3, "active": True}]},
        }
    )
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_active_version("svc1", "tok") == 3


def test_fetch_active_version_returns_none_when_nothing_active():
    fake = _fastly_mock(
        {
            ("GET", "/service/svc1/version/active"): {"active": False},
            ("GET", "/service/svc1/version"): [{"number": 1, "active": False}],
        }
    )
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_active_version("svc1", "tok") is None


def test_fetch_active_version_returns_none_when_fallback_list_also_errors():
    def _raise_second(method, path, body=None, *, token, **kwargs):
        if path.endswith("/active"):
            raise RuntimeError("HTTP 404")
        raise RuntimeError("HTTP 500")

    with patch.object(fi, "fastly", _raise_second):
        assert fi.fetch_active_version("svc1", "tok") is None


# ── fetch_snippets / fetch_logging_endpoints / fetch_backends ───────────


def test_fetch_snippets_parses_list_response():
    fake = _fastly_mock(
        {
            ("GET", "/service/svc1/version/3/snippet"): [
                {"name": "recv1", "priority": 50, "snippet": "# vcl", "type": "vcl_recv"},
            ]
        }
    )
    with patch.object(fi, "fastly", fake):
        snippets = fi.fetch_snippets("svc1", 3, "tok")
    assert snippets == [VCLSnippet(name="recv1", priority=50, body="# vcl", subroutine="vcl_recv")]


def test_fetch_snippets_parses_items_dict_response_and_defaults_missing_fields():
    fake = _fastly_mock({("GET", "/service/svc1/version/3/snippet"): {"items": [{}]}})
    with patch.object(fi, "fastly", fake):
        snippets = fi.fetch_snippets("svc1", 3, "tok")
    assert snippets == [VCLSnippet(name="", priority=100, body="", subroutine="vcl_recv")]


def test_fetch_snippets_swallows_errors_and_returns_empty_list():
    fake = _fastly_mock({("GET", "/service/svc1/version/3/snippet"): RuntimeError("HTTP 500")})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_snippets("svc1", 3, "tok") == []


def test_fetch_snippets_treats_unrecognized_shape_as_empty():
    fake = _fastly_mock({("GET", "/service/svc1/version/3/snippet"): {"not_items": []}})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_snippets("svc1", 3, "tok") == []


def test_fetch_logging_endpoints_parses_list_response():
    fake = _fastly_mock(
        {
            ("GET", "/service/svc1/version/3/logging/s3"): [
                {
                    "name": "ep1",
                    "path": "/raw/",
                    "period": 60,
                    "response_condition": "cond1",
                    "format": "%h",
                    "placement": "none",
                    "response_object_name": "",
                }
            ]
        }
    )
    with patch.object(fi, "fastly", fake):
        endpoints = fi.fetch_logging_endpoints("svc1", 3, "tok")
    assert endpoints == [
        LoggingEndpoint(
            name="ep1",
            endpoint_type="s3",
            path="/raw/",
            period=60,
            response_condition="cond1",
            format_string="%h",
            placement="none",
            response_object_name="",
        )
    ]


def test_fetch_logging_endpoints_swallows_errors():
    fake = _fastly_mock({("GET", "/service/svc1/version/3/logging/s3"): RuntimeError("boom")})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_logging_endpoints("svc1", 3, "tok") == []


def test_fetch_backends_parses_list_response_with_defaults():
    fake = _fastly_mock(
        {
            ("GET", "/service/svc1/version/3/backend"): [
                {"name": "origin1", "address": "origin.example.test"},
            ]
        }
    )
    with patch.object(fi, "fastly", fake):
        backends = fi.fetch_backends("svc1", 3, "tok")
    assert backends == [Backend(name="origin1", address="origin.example.test", port=443)]


def test_fetch_backends_swallows_errors():
    fake = _fastly_mock({("GET", "/service/svc1/version/3/backend"): RuntimeError("boom")})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_backends("svc1", 3, "tok") == []


# ── fetch_dictionaries ───────────────────────────────────────────────────


def test_fetch_dictionaries_fetches_items_for_each_dict():
    fake = _fastly_mock(
        {
            ("GET", "/service/svc1/version/3/dictionary"): [
                {"name": "d1", "id": "did1", "write_only": False},
            ],
            ("GET", "/service/svc1/dictionary/did1/items"): [
                {"item_key": "k1", "item_value": "v1"},
                {"key": "k2", "value": "v2"},
            ],
        }
    )
    with patch.object(fi, "fastly", fake):
        dicts = fi.fetch_dictionaries("svc1", 3, "tok")
    assert dicts == [ServiceDictionary(name="d1", write_only=False, items={"k1": "v1", "k2": "v2"})]


def test_fetch_dictionaries_items_dict_shape_is_wrapped_as_single_entry():
    fake = _fastly_mock(
        {
            ("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": "did1"}],
            ("GET", "/service/svc1/dictionary/did1/items"): {"item_key": "solo", "item_value": "only"},
        }
    )
    with patch.object(fi, "fastly", fake):
        dicts = fi.fetch_dictionaries("svc1", 3, "tok")
    assert dicts[0].items == {"solo": "only"}


def test_fetch_dictionaries_swallows_item_fetch_errors_but_keeps_dict_with_no_items():
    fake = _fastly_mock(
        {
            ("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": "did1"}],
            ("GET", "/service/svc1/dictionary/did1/items"): RuntimeError("boom"),
        }
    )
    with patch.object(fi, "fastly", fake):
        dicts = fi.fetch_dictionaries("svc1", 3, "tok")
    assert dicts == [ServiceDictionary(name="d1", write_only=False, items={})]


def test_fetch_dictionaries_skips_item_fetch_when_no_dict_id():
    fake = _fastly_mock({("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": ""}]})
    with patch.object(fi, "fastly", fake):
        dicts = fi.fetch_dictionaries("svc1", 3, "tok")
    assert dicts == [ServiceDictionary(name="d1", items={})]


def test_fetch_dictionaries_outer_swallow_returns_empty_list():
    fake = _fastly_mock({("GET", "/service/svc1/version/3/dictionary"): RuntimeError("boom")})
    with patch.object(fi, "fastly", fake):
        assert fi.fetch_dictionaries("svc1", 3, "tok") == []


# ── clone_version ────────────────────────────────────────────────────────


def test_clone_version_without_comment():
    fake = _fastly_mock({("PUT", "/service/svc1/version/3/clone"): {"number": 4}})
    with patch.object(fi, "fastly", fake):
        assert fi.clone_version("svc1", 3, "tok") == 4


def test_clone_version_sets_comment_on_new_version():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if path.endswith("/clone"):
            return {"number": 9}
        return {}

    with patch.object(fi, "fastly", _fake):
        new_version = fi.clone_version("svc1", 3, "tok", comment="reconciler pass")

    assert new_version == 9
    assert ("PUT", "/service/svc1/version/9", {"comment": "reconciler pass"}) in calls


# ── delete_* mutations ───────────────────────────────────────────────────


def test_delete_snippet_encodes_name_and_expects_empty():
    calls = []

    def _fake(method, path, body=None, *, token, expect_empty=False, **kwargs):
        calls.append((method, path, expect_empty))
        return {}

    with patch.object(fi, "fastly", _fake):
        fi.delete_snippet("svc1", 3, "weird name/slash", "tok")

    assert calls == [("DELETE", "/service/svc1/version/3/snippet/weird%20name%2Fslash", True)]


def test_delete_logging_endpoint_encodes_name():
    calls = []

    def _fake(method, path, body=None, *, token, expect_empty=False, **kwargs):
        calls.append((method, path))
        return {}

    with patch.object(fi, "fastly", _fake):
        fi.delete_logging_endpoint("svc1", 3, "My Endpoint", "tok")

    assert calls == [("DELETE", "/service/svc1/version/3/logging/s3/My%20Endpoint")]


def test_delete_backend_encodes_name():
    calls = []

    def _fake(method, path, body=None, *, token, expect_empty=False, **kwargs):
        calls.append((method, path))
        return {}

    with patch.object(fi, "fastly", _fake):
        fi.delete_backend("svc1", 3, "origin/1", "tok")

    assert calls == [("DELETE", "/service/svc1/version/3/backend/origin%2F1")]


def test_delete_dictionary_encodes_name():
    calls = []

    def _fake(method, path, body=None, *, token, expect_empty=False, **kwargs):
        calls.append((method, path))
        return {}

    with patch.object(fi, "fastly", _fake):
        fi.delete_dictionary("svc1", 3, "my dict", "tok")

    assert calls == [("DELETE", "/service/svc1/version/3/dictionary/my%20dict")]


# ── create_or_update_snippet ─────────────────────────────────────────────


def test_create_or_update_snippet_updates_when_snippet_exists():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            return {"name": "recv1"}
        return {}

    snippet = VCLSnippet(name="recv1", priority=50, body="# vcl", subroutine="vcl_recv")
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_snippet("svc1", 3, snippet, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "PUT"]
    put_body = calls[1][2]
    assert put_body["type"] == "recv"  # vcl_ prefix stripped
    assert put_body["content"] == "# vcl"


def test_create_or_update_snippet_creates_when_snippet_missing():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            raise RuntimeError("HTTP 404 not found")
        return {}

    snippet = VCLSnippet(name="new_snip", priority=50, body="# vcl", subroutine="deliver")
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_snippet("svc1", 3, snippet, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "POST"]
    post_body = calls[1][2]
    # subroutine without "vcl_" prefix passes through unchanged
    assert post_body["type"] == "deliver"


# ── create_or_update_logging_endpoint ────────────────────────────────────


def test_create_or_update_logging_endpoint_creates_with_s3_params():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            raise RuntimeError("HTTP 404")
        return {}

    endpoint = LoggingEndpoint(
        name="ep1",
        endpoint_type="s3",
        path="/raw/",
        period=60,
        response_condition="",
        format_string="%h",
        placement="none",
        response_object_name="",
    )
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_logging_endpoint(
            "svc1",
            3,
            endpoint,
            "tok",
            bucket_name="my-bucket",
            domain="s3.example.test",
            access_key="AKIA",
            secret_key="shh",
        )

    methods = [c[0] for c in calls]
    assert methods == ["GET", "POST"]
    post_body = calls[1][2]
    assert post_body["bucket_name"] == "my-bucket"
    assert post_body["domain"] == "s3.example.test"
    assert post_body["access_key"] == "AKIA"
    assert post_body["secret_key"] == "shh"
    # empty response_condition is omitted, not sent as ""
    assert "response_condition" not in post_body


def test_create_or_update_logging_endpoint_updates_when_it_exists():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            return {"name": "ep1"}
        return {}

    endpoint = LoggingEndpoint(
        name="ep1",
        endpoint_type="s3",
        path="/raw/",
        period=60,
        response_condition="always_true",
        format_string="%h",
        placement="none",
        response_object_name="",
    )
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_logging_endpoint("svc1", 3, endpoint, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "PUT"]
    put_body = calls[1][2]
    assert put_body["response_condition"] == "always_true"
    assert "bucket_name" not in put_body


# ── create_or_update_backend ─────────────────────────────────────────────


def test_create_or_update_backend_creates_with_shield_and_override_host():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            raise RuntimeError("HTTP 404")
        return {}

    backend = Backend(
        name="origin1",
        address="origin.example.test",
        port=443,
        override_host="custom.example.test",
        shield="dca-dc-us",
    )
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_backend("svc1", 3, backend, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "POST"]
    post_body = calls[1][2]
    assert post_body["override_host"] == "custom.example.test"
    assert post_body["shield"] == "dca-dc-us"


def test_create_or_update_backend_updates_and_nulls_shield_when_absent():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            return {"name": "origin1"}
        return {}

    backend = Backend(name="origin1", address="origin.example.test", port=443)
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_backend("svc1", 3, backend, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "PUT"]
    put_body = calls[1][2]
    assert put_body["shield"] is None
    assert "override_host" not in put_body


# ── create_or_update_dictionary ───────────────────────────────────────────


def test_create_or_update_dictionary_creates_and_upserts_items_via_patch():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            raise RuntimeError("HTTP 404")
        if method == "POST" and path.endswith("/dictionary"):
            return {"id": "dict-id-1"}
        return {}

    dictionary = ServiceDictionary(name="my_dict", write_only=True, items={"key1": "val1"})
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_dictionary("svc1", 3, dictionary, "tok")

    # GET (miss) -> POST create dict -> PATCH item upsert
    kinds = [(c[0], c[1].split("/")[-1]) for c in calls]
    assert ("GET", "my_dict") in kinds
    assert ("POST", "dictionary") in kinds
    assert ("PATCH", "key1") in kinds


def test_create_or_update_dictionary_updates_existing_and_falls_back_to_post_for_new_items():
    calls: list[tuple[str, str, Any]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        if method == "GET" and path.endswith("/dictionary/my_dict"):
            return {"name": "my_dict"}
        if method == "PUT" and "/dictionary/my_dict" in path:
            return {"id": "dict-id-1"}
        if method == "PATCH":
            raise RuntimeError("HTTP 404 item not found")
        if method == "POST" and path.endswith("/items"):
            return {}
        raise AssertionError(f"unexpected call {method} {path}")

    dictionary = ServiceDictionary(name="my_dict", items={"new_key": "new_val"})
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_dictionary("svc1", 3, dictionary, "tok")

    methods = [c[0] for c in calls]
    assert methods == ["GET", "PUT", "PATCH", "POST"]


def test_create_or_update_dictionary_raises_when_response_missing_id():
    def _fake(method, path, body=None, *, token, **kwargs):
        if method == "GET":
            raise RuntimeError("HTTP 404")
        return {}  # POST create response has no "id"

    dictionary = ServiceDictionary(name="my_dict", items={"k": "v"})
    with patch.object(fi, "fastly", _fake):
        with pytest.raises(RuntimeError, match="Failed to get dictionary ID"):
            fi.create_or_update_dictionary("svc1", 3, dictionary, "tok")


def test_create_or_update_dictionary_skips_item_upsert_when_no_items():
    calls: list[tuple[str, str]] = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path))
        if method == "GET":
            raise RuntimeError("HTTP 404")
        return {"id": "dict-id-1"}

    dictionary = ServiceDictionary(name="empty_dict", items={})
    with patch.object(fi, "fastly", _fake):
        fi.create_or_update_dictionary("svc1", 3, dictionary, "tok")

    assert all(c[0] != "PATCH" for c in calls)


# ── validate_version / activate_version ──────────────────────────────────


def test_validate_version_true_on_ok_status():
    fake = _fastly_mock({("GET", "/service/svc1/version/3/validate"): {"status": "ok"}})
    with patch.object(fi, "fastly", fake):
        assert fi.validate_version("svc1", 3, "tok") is True


def test_validate_version_false_on_non_ok_status():
    fake = _fastly_mock({("GET", "/service/svc1/version/3/validate"): {"status": "error"}})
    with patch.object(fi, "fastly", fake):
        assert fi.validate_version("svc1", 3, "tok") is False


def test_validate_version_reraises_with_context_on_error():
    fake = _fastly_mock({("GET", "/service/svc1/version/3/validate"): RuntimeError("bad VCL: undefined var")})
    with patch.object(fi, "fastly", fake):
        with pytest.raises(RuntimeError, match="VCL validation failed"):
            fi.validate_version("svc1", 3, "tok")


def test_activate_version_calls_activate_endpoint():
    calls = []

    def _fake(method, path, body=None, *, token, **kwargs):
        calls.append((method, path))
        return {}

    with patch.object(fi, "fastly", _fake):
        fi.activate_version("svc1", 3, "tok")

    assert calls == [("PUT", "/service/svc1/version/3/activate")]
