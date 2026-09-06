"""Regression coverage for teardown: dereference before delete, and actually dereference.

Two 2026-08-13 defects, both found from a real teardown of a live service.

1. ``remove_logging_endpoint`` blind-DELETEd a HARDCODED list of snippet names
   and swallowed every 404. When the names on the service didn't match the list
   it logged "removed 0 active snippets" and moved on, leaving the entire
   capture VCL live on a service the operator believed was torn down. Endpoints,
   conditions and dictionaries were removed correctly because those three
   already enumerated-then-matched. The list was also missing
   ``Fastly Log Analytics - vcl_pass``.

2. ``teardown_scoring_resources`` treated the VCL strip as best-effort and
   deleted the Compute service regardless. A strip failure (Fastly 500s on these
   calls are real) then left the customer's ACTIVE version pre-flighting to a
   deleted host — an outage caused by our teardown.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from backend.provision import fastly_api
from backend.provision.cmcd_vcl import cmcd_snippet_names
from backend.provision.session_scoring_vcl import scoring_snippet_names

# What a real service looks like: ours, plus customer snippets that must survive.
CUSTOMER_SNIPPETS = ["Session Tracking - Recv", "Session Tracking - Deliver", "force_ssl", "Cache Overrides"]
OUR_SNIPPETS = [
    "Fastly Log Analytics - vcl_recv",
    "Fastly Log Analytics - vcl_miss",
    "Fastly Log Analytics - vcl_pass",
    "Fastly Log Analytics - vcl_fetch",
    "Fastly Log Analytics - vcl_deliver",
    "Fastly Log Analytics - vcl_error",
]


def _run_remove(snippet_names, deleted_sink):
    """Drive remove_logging_endpoint against a faked Fastly."""

    def fake_fastly(method, path, body=None, **kwargs):
        if method == "GET" and path.endswith("/snippet"):
            return [{"name": n} for n in snippet_names]
        if method == "DELETE" and "/snippet/" in path:
            import urllib.parse

            name = urllib.parse.unquote(path.rsplit("/snippet/", 1)[1])
            if name not in snippet_names:
                raise RuntimeError("HTTP 404 not found")
            deleted_sink.append(name)
            return {}
        if "/clone" in path:
            return {"number": 99}
        if "/validate" in path:
            return {"status": "ok"}
        if method == "GET" and path.endswith(("/backend", "/dictionary", "/condition")):
            return []
        return {}

    with (
        patch.object(fastly_api, "fastly", side_effect=fake_fastly),
        patch.object(fastly_api, "get_active_version", return_value=98),
        patch.object(fastly_api, "list_s3_endpoints", return_value=["Fastly Object Storage Logs"]),
    ):
        fastly_api.remove_logging_endpoint("svc-td", "Fastly Object Storage Logs", "tok")


def test_all_capture_snippets_removed_including_vcl_pass():
    """THE REGRESSION: every capture snippet goes, vcl_pass included."""
    deleted: list[str] = []
    _run_remove(OUR_SNIPPETS + CUSTOMER_SNIPPETS, deleted)
    for name in OUR_SNIPPETS:
        assert name in deleted, f"{name} was left on the service"


def test_customer_snippets_are_never_touched():
    """'Session Tracking - *' is the CUSTOMER's; ours is 'Session Scoring - *'.

    A prefix sweep on "Session " would delete their VCL. Pinned deliberately.
    """
    deleted: list[str] = []
    _run_remove(OUR_SNIPPETS + CUSTOMER_SNIPPETS, deleted)
    for name in CUSTOMER_SNIPPETS:
        assert name not in deleted, f"deleted customer-owned snippet {name!r}"


def test_legacy_capture_names_still_removed():
    """Older generations used 'Fastly Log Analytics Capture' etc."""
    legacy = ["Fastly Log Analytics Capture", "Fastly Log Analytics Origin Deliver"]
    deleted: list[str] = []
    _run_remove(legacy + CUSTOMER_SNIPPETS, deleted)
    assert set(legacy) <= set(deleted)


def test_feature_snippets_removed_from_canonical_lists():
    """Scoring / CMCD / RUM names come from the generators, not a copy."""
    feature = [*scoring_snippet_names(), *cmcd_snippet_names(), "RUM - Recv", "RUM - Set cookies"]
    deleted: list[str] = []
    _run_remove(feature + CUSTOMER_SNIPPETS, deleted)
    for name in feature:
        assert name in deleted, f"{name} was left on the service"


def test_unmatched_snippets_do_not_report_silent_success():
    """If nothing of ours matched, that must be surfaced, not logged as success.

    This is the exact 2026-08-13 shape: names on the service didn't match, so
    zero were removed and teardown reported success.
    """
    deleted: list[str] = []
    with patch.object(fastly_api, "warn") as mock_warn:
        _run_remove(CUSTOMER_SNIPPETS, deleted)
    assert deleted == []
    assert mock_warn.called, "removing zero analytics snippets must warn"
    assert any("teardown left VCL behind" in str(c) or "still on draft" in str(c) for c in mock_warn.call_args_list)


# ── scoring: never delete the Compute service while it may still be referenced ──


def test_scoring_teardown_aborts_when_vcl_strip_fails():
    """A strip failure must NOT proceed to delete the Compute service."""
    from backend.provision import session_scoring_orchestrator as sso

    with (
        patch.object(sso, "get_active_version", side_effect=RuntimeError("HTTP 500 clone failed")),
        patch.object(sso, "delete_scoring_service") as mock_delete,
    ):
        with pytest.raises(RuntimeError, match="Aborting scoring teardown"):
            sso.teardown_scoring_resources("svc-td", {"scoring_service_id": "compute-1"}, "tok")

    mock_delete.assert_not_called()


def test_scoring_teardown_proceeds_when_service_already_gone():
    """No active version means nothing left to dereference — not a failure."""
    from backend.provision import session_scoring_orchestrator as sso

    with (
        patch.object(sso, "get_active_version", return_value=None),
        patch.object(sso, "delete_scoring_service", return_value=[]) as mock_delete,
    ):
        sso.teardown_scoring_resources("svc-td", {"scoring_service_id": "compute-1"}, "tok")

    mock_delete.assert_called_once()
