"""Tests for ``backend.core.fastly.service`` — Fastly service-config CRUD.

Thin wrappers around ``fastly()`` for the half-dozen Fastly resources
the provisioning wizard touches (versions, dictionaries, conditions,
S3 logging endpoints, VCL snippets). The wrappers do four things
worth pinning:

  - lookup-by-name iteration (``find_*``)
  - "ensure" idempotency: update-when-differs, no-op-when-same, create-when-absent
  - RuntimeError → safe-empty fallback (so failed lookups don't crash the
    wizard)
  - URL-encoding of names that contain ``/`` or ``%`` (otherwise the PUT
    URL gets mis-routed)
"""

from __future__ import annotations

from unittest.mock import patch

from backend.core.fastly import service

# ── get_active_version ─────────────────────────────────────────────────────


def test_get_active_version_returns_active_number():
    fake_versions = [
        {"number": 1, "active": False},
        {"number": 7, "active": True},
        {"number": 3, "active": False},
    ]
    with patch("backend.core.fastly.service.fastly", return_value=fake_versions):
        assert service.get_active_version("svc-1", "tkn") == 7


def test_get_active_version_returns_none_when_no_active():
    """Service exists but no version is active (between deploys) →
    None. Pinned because raising would block re-provisioning and the
    wizard relies on this distinction to render a "configure first"
    state."""
    with patch("backend.core.fastly.service.fastly", return_value=[{"number": 1, "active": False}]):
        assert service.get_active_version("svc-1", "tkn") is None


def test_get_active_version_returns_none_on_api_error():
    """RuntimeError (401, 404, network) → None. Pinned because the
    teardown path calls this on best-effort basis; raising would
    leave half-provisioned services dangling."""
    with patch("backend.core.fastly.service.fastly", side_effect=RuntimeError("401")):
        assert service.get_active_version("svc-1", "tkn") is None


def test_get_active_version_returns_first_active_when_multiple():
    """If two versions are flagged active (data corruption), the
    first wins. Pinned to lock the precedence — flipping it would
    cause the wizard to read from the wrong version."""
    fake = [{"number": 5, "active": True}, {"number": 9, "active": True}]
    with patch("backend.core.fastly.service.fastly", return_value=fake):
        assert service.get_active_version("svc-1", "tkn") == 5


# ── find_service_by_name ───────────────────────────────────────────────────


def test_find_service_by_name_returns_match():
    services = [
        {"id": "svc-a", "name": "Production"},
        {"id": "svc-b", "name": "Staging"},
    ]
    with patch("backend.core.fastly.service.fastly", return_value=services):
        assert service.find_service_by_name("Staging", "tkn")["id"] == "svc-b"


def test_find_service_by_name_returns_none_for_unknown():
    with patch("backend.core.fastly.service.fastly", return_value=[]):
        assert service.find_service_by_name("ghost", "tkn") is None


def test_find_service_by_name_returns_none_on_error():
    with patch("backend.core.fastly.service.fastly", side_effect=RuntimeError("403")):
        assert service.find_service_by_name("anything", "tkn") is None


# ── find_dictionary_by_name ───────────────────────────────────────────────


def test_find_dictionary_by_name_returns_match():
    dicts = [{"id": "d1", "name": "allowlist"}, {"id": "d2", "name": "blocklist"}]
    with patch("backend.core.fastly.service.fastly", return_value=dicts):
        out = service.find_dictionary_by_name("svc-1", 5, "blocklist", "tkn")
    assert out["id"] == "d2"


def test_find_dictionary_by_name_returns_none_on_error():
    with patch("backend.core.fastly.service.fastly", side_effect=RuntimeError("404")):
        assert service.find_dictionary_by_name("svc-1", 5, "any", "tkn") is None


# ── upsert_dictionary_items ───────────────────────────────────────────────


def test_upsert_dictionary_items_builds_patch_payload():
    """Items dict is flattened to the Fastly API's
    ``[{item_key, item_value}]`` array shape. Pinned because the API
    rejects nested dict bodies with 400."""
    with patch("backend.core.fastly.service.fastly") as mock_fastly:
        service.upsert_dictionary_items("svc-1", "dict-1", {"k1": "v1", "k2": "v2"}, "tkn")

    args, kwargs = mock_fastly.call_args[0], mock_fastly.call_args[1]
    assert args[0] == "PATCH"
    assert "/dictionary/dict-1/items" in args[1]
    payload = args[2]
    assert payload == {"items": [{"item_key": "k1", "item_value": "v1"}, {"item_key": "k2", "item_value": "v2"}]}


# ── find_condition + ensure_condition ─────────────────────────────────────


def test_find_condition_iterates_and_matches_by_name():
    conditions = [
        {"name": "Cond-A", "statement": 'req.url == "/"'},
        {"name": "Cond-B", "statement": 'req.method == "POST"'},
    ]
    with patch("backend.core.fastly.service.fastly", return_value=conditions):
        out = service.find_condition("Cond-B", "svc-1", 5, "tkn")
    assert out["statement"] == 'req.method == "POST"'


def test_find_condition_returns_none_when_no_match():
    with patch("backend.core.fastly.service.fastly", return_value=[]):
        assert service.find_condition("absent", "svc-1", 5, "tkn") is None


def test_ensure_condition_creates_when_absent():
    """No existing condition → POST. Pinned because a regression that
    sent PUT on a missing condition would 404 instead of creating."""
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path, payload))
        if method == "GET":
            return []  # no existing
        return {"id": "new"}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_condition("My Cond", 'req.url ~ "/"', "REQUEST", "svc-1", 5, "tkn")

    methods = [c[0] for c in calls]
    assert "POST" in methods
    assert "PUT" not in methods


def test_ensure_condition_noops_when_statement_and_type_match():
    """If the existing condition has the same statement AND type, no
    write. Pinned because re-saving an unchanged condition increments
    the Fastly version number for nothing."""
    existing = {"name": "X", "statement": "S", "type": "REQUEST"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append(method)
        return [existing]  # GET returns the existing match

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        out = service.ensure_condition("X", "S", "REQUEST", "svc-1", 5, "tkn")

    assert out == existing
    # Only the GET happened; no POST/PUT
    assert calls.count("POST") == 0
    assert calls.count("PUT") == 0


def test_ensure_condition_updates_when_statement_changes():
    """Existing condition + different statement → PUT to update.
    Pinned because re-creating (POST) instead would fail with a
    name-conflict 400."""
    existing = {"name": "X", "statement": "OLD", "type": "REQUEST"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path))
        if method == "GET":
            return [existing]
        return {"id": "updated"}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_condition("X", "NEW", "REQUEST", "svc-1", 5, "tkn")

    put_calls = [c for c in calls if c[0] == "PUT"]
    assert len(put_calls) == 1


def test_ensure_condition_url_encodes_condition_name():
    """Condition names can contain ``/`` and ``%`` — those must be
    URL-encoded in the PUT path or the request routes to the wrong
    resource (or 404s). Pinned to prevent naming-edge regressions."""
    existing = {"name": "Path / With % Spaces", "statement": "S", "type": "REQ"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path))
        if method == "GET":
            return [existing]
        return {}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_condition("Path / With % Spaces", "NEW", "REQ", "svc-1", 5, "tkn")

    put_path = next(c[1] for c in calls if c[0] == "PUT")
    # ``/`` → %2F, space → %20, ``%`` → %25
    assert "Path%20%2F%20With%20%25%20Spaces" in put_path
    # Crucially: no literal slash from the name leaks into the path
    # (which would break the route)
    assert put_path.endswith("Path%20%2F%20With%20%25%20Spaces")


# ── list_s3_endpoints + list_vcl_snippets ─────────────────────────────────


def test_list_s3_endpoints_returns_names_list():
    fake = [{"name": "logs-fos"}, {"name": "audit-logs"}]
    with patch("backend.core.fastly.service.fastly", return_value=fake):
        assert service.list_s3_endpoints("svc", 5, "tkn") == ["logs-fos", "audit-logs"]


def test_list_s3_endpoints_returns_empty_on_error():
    """RuntimeError → empty list. Pinned because the cron's
    teardown-detector reads this to know what to remove; a raise
    would abort the whole teardown."""
    with patch("backend.core.fastly.service.fastly", side_effect=RuntimeError("404")):
        assert service.list_s3_endpoints("svc", 5, "tkn") == []


def test_list_vcl_snippets_returns_names_list():
    fake = [{"name": "init"}, {"name": "recv"}]
    with patch("backend.core.fastly.service.fastly", return_value=fake):
        assert service.list_vcl_snippets("svc", 5, "tkn") == ["init", "recv"]


def test_list_vcl_snippets_returns_empty_on_error():
    with patch("backend.core.fastly.service.fastly", side_effect=RuntimeError("403")):
        assert service.list_vcl_snippets("svc", 5, "tkn") == []


# ── ensure_vcl_snippet: 3-way idempotency ──────────────────────────────────


def test_ensure_vcl_snippet_creates_when_absent():
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path, payload))
        if method == "GET":
            return []
        return {"id": "snip-new"}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_vcl_snippet("my-snip", "init", "set req.x = 1;", 100, "svc", 5, "tkn")

    methods = [c[0] for c in calls]
    assert "POST" in methods
    # Payload must include the dynamic=0 flag (Fastly distinguishes static vs dynamic)
    post_payload = next(c[2] for c in calls if c[0] == "POST")
    assert post_payload["dynamic"] == 0


def test_ensure_vcl_snippet_noops_when_all_fields_match():
    """A snippet that already exists with the same type, content,
    AND priority → return existing, no write. Pinned because
    re-saving bumps the Fastly service version, polluting the
    change history with no-op deploys."""
    existing = {"name": "x", "type": "init", "content": "C", "priority": 100, "id": "old"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append(method)
        return [existing]

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        out = service.ensure_vcl_snippet("x", "init", "C", 100, "svc", 5, "tkn")

    assert out == existing
    assert calls.count("POST") == 0
    assert calls.count("PUT") == 0


def test_ensure_vcl_snippet_updates_when_content_changes():
    """Differing content → PUT update. Pinned because the most common
    re-provision case is "VCL changed" and POST instead of PUT would
    fail with a name-conflict 400."""
    existing = {"name": "x", "type": "init", "content": "OLD", "priority": 100, "id": "old"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path))
        if method == "GET":
            return [existing]
        return {}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_vcl_snippet("x", "init", "NEW", 100, "svc", 5, "tkn")

    put_calls = [c for c in calls if c[0] == "PUT"]
    assert len(put_calls) == 1


def test_ensure_vcl_snippet_updates_when_priority_changes():
    existing = {"name": "x", "type": "init", "content": "C", "priority": 100, "id": "old"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append(method)
        if method == "GET":
            return [existing]
        return {}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_vcl_snippet("x", "init", "C", 200, "svc", 5, "tkn")

    assert "PUT" in calls


def test_ensure_vcl_snippet_url_encodes_snippet_name():
    """Same URL-encoding requirement as ensure_condition."""
    existing = {"name": "edge / case", "type": "init", "content": "OLD", "priority": 100, "id": "x"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path))
        if method == "GET":
            return [existing]
        return {}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_vcl_snippet("edge / case", "init", "NEW", 100, "svc", 5, "tkn")

    put_path = next(c[1] for c in calls if c[0] == "PUT")
    assert "edge%20%2F%20case" in put_path


def test_ensure_vcl_snippet_creates_on_get_failure():
    """If the GET fails (404, network), fall through to the POST
    create. Pinned because a transient GET error shouldn't lock us
    out of provisioning."""
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append(method)
        if method == "GET":
            raise RuntimeError("404")
        return {"id": "new"}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_vcl_snippet("x", "init", "C", 100, "svc", 5, "tkn")

    assert "POST" in calls
