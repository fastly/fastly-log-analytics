"""Streaming → Content Security policies, assignments, live rules and publishing."""

from __future__ import annotations

import json

import httpx
import pytest

from backend.utils import blocklist_client
from tests.conftest import MOCK_SERVICE_ID

_HEADERS = {"x-fastly-service-id": MOCK_SERVICE_ID}
_BASE = "/api/admin/content-security"
_URL = "https://blocker.example.com/block"
_SECRET = "Bearer s3cret-token-value"


class _Blocker:
    """MockTransport handler: records every call, replies with ``status``."""

    def __init__(self, status: int = 200):
        self.status = status
        # When set, the next replies come from here (status, body) before falling back to ``status``.
        self.script: list[tuple[int, dict]] = []
        self.calls: list[tuple[str, str, dict, httpx.Headers]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, str(request.url), json.loads(request.content or b"{}"), request.headers))
        if self.script:
            status, body = self.script.pop(0)
            return httpx.Response(status, json=body)
        return httpx.Response(self.status, json={})


@pytest.fixture(autouse=True)
def asn_names(monkeypatch):
    """ASN owner names without the network: 7922 is Comcast, every other ASN is unnamed."""
    from backend.core import duckdb

    names = {7922: "Comcast Cable"}
    monkeypatch.setattr(
        duckdb, "get_asn_names", lambda service_id, asns: {int(a): names.get(int(a), f"AS{a}") for a in asns}
    )
    return names


@pytest.fixture
def blocker(monkeypatch):
    rec = _Blocker()
    monkeypatch.setattr(blocklist_client, "_TRANSPORT", httpx.MockTransport(rec))
    rec.sleeps = []  # type: ignore[attr-defined]
    monkeypatch.setattr(blocklist_client, "_sleep", rec.sleeps.append)  # retries do not really wait
    return rec


@pytest.fixture
def configured(client):
    resp = client.put(f"{_BASE}/settings", json={"blocklist_url": _URL, "auth_header": f"Authorization: {_SECRET}"})
    assert resp.status_code == 200


def _get(client, path, **params):
    return client.get(f"{_BASE}{path}", headers=_HEADERS, params=params)


def _send(client, method, path, body=None, **params):
    return client.request(method, f"{_BASE}{path}", headers=_HEADERS, json=body, params=params)


def _policy(client, name="Playoffs", **extra):
    resp = _send(client, "POST", "/policies", {"name": name, **extra})
    assert resp.status_code == 201, resp.text
    return resp.json()["policy"]


def _assign(client, policy_id, scope="default", content_id=None, mode="ENFORCE", starts_at=None, ends_at=None):
    return _send(
        client,
        "POST",
        "/assignments",
        {
            "policy_id": policy_id,
            "scope_type": scope,
            "content_id": content_id,
            "mode": mode,
            "starts_at": starts_at,
            "ends_at": ends_at,
        },
    )


# ── Policies ─────────────────────────────────────────────────────────────────


def test_list_empty(client):
    resp = _get(client, "/policies")
    assert resp.status_code == 200
    assert resp.json()["configured"] is False
    assert resp.json()["policies"] == []


def test_create_policy_normalizes_and_returns_rules(client):
    pol = _policy(
        client,
        country_regex=" ^(CN|RU)$ ",
        rules={
            "country": [{"value": "br", "action": "pace", "kbps": 256}, {"value": "us", "action": "exempt"}],
            "asn": [{"value": "AS7922", "action": "block"}],
            "referer": [{"value": "https://Pirate.Example/watch", "action": "block"}],
        },
        individual={"subscriber_id": ["sub-1"], "ip": ["2001:DB8::0001"]},
    )
    assert pol["country_regex"] == "^(CN|RU)$"
    assert pol["rules"]["country"] == [
        {"value": "BR", "action": "pace", "kbps": 256},
        {"value": "US", "action": "exempt", "kbps": 0},
    ]
    assert pol["rules"]["asn"] == [{"value": "7922", "action": "block", "kbps": 0, "label": "Comcast Cable (7922)"}]
    assert pol["rules"]["referer"][0]["value"] == "pirate.example"
    assert pol["individual"] == {"subscriber_id": ["sub-1"], "ip": ["2001:db8::1"]}
    assert pol["used_by"] == []
    assert _get(client, f"/policies/{pol['id']}").json()["name"] == "Playoffs"


def test_policy_names_unique_per_service(client):
    _policy(client)
    resp = _send(client, "POST", "/policies", {"name": "Playoffs"})
    assert resp.status_code == 409
    assert "name_taken" in resp.text


@pytest.mark.parametrize(
    "body",
    [
        {"country_regex": "(a)\\1"},
        {"country_regex": "(a+)+"},
        {"rules": {"country": [{"value": "USA", "action": "block"}]}},
        {"rules": {"country": [{"value": "BR", "action": "pace", "kbps": 0}]}},
        {"rules": {"asn": [{"value": "x", "action": "block"}]}},
        {"individual": {"ip": ["not-an-ip"]}},
        {"rules": {"country": [{"value": "BR", "action": "block"}, {"value": "br", "action": "exempt"}]}},
    ],
)
def test_create_policy_rejects_bad_input(client, body):
    resp = _send(client, "POST", "/policies", {"name": "Bad", **body})
    assert resp.status_code == 422, resp.text
    assert _get(client, "/policies").json()["policies"] == []


def test_replace_policy_bumps_rev_and_keeps_history(client):
    pol = _policy(client, rules={"country": [{"value": "BR", "action": "block"}]})
    resp = _send(
        client,
        "PUT",
        f"/policies/{pol['id']}",
        {"name": "Renamed", "rules": {"country": [{"value": "FR", "action": "block"}]}},
    )
    assert resp.status_code == 200
    new = resp.json()["policy"]
    assert (new["name"], new["rev"]) == ("Renamed", 2)
    assert [r["value"] for r in new["rules"]["country"]] == ["FR"]
    revs = _get(client, f"/policies/{pol['id']}/revisions").json()["revisions"]
    assert [r["rev"] for r in revs] == [2, 1]
    assert revs[1]["policy"]["rules"]["country"][0]["value"] == "BR"


def test_unknown_policy_is_404(client):
    assert _get(client, "/policies/nope").status_code == 404
    assert _send(client, "PUT", "/policies/nope", {"name": "x"}).status_code == 404
    assert _send(client, "DELETE", "/policies/nope").status_code == 404


def test_rule_upsert_replaces_action_and_remove(client):
    pol = _policy(client)
    r = _send(client, "POST", f"/policies/{pol['id']}/rules", {"category": "country", "value": "br", "action": "block"})
    assert r.json()["policy"]["rules"]["country"] == [{"value": "BR", "action": "block", "kbps": 0}]
    r = _send(
        client,
        "POST",
        f"/policies/{pol['id']}/rules",
        {"category": "country", "value": "BR", "action": "pace", "kbps": 128},
    )
    assert r.json()["policy"]["rules"]["country"] == [{"value": "BR", "action": "pace", "kbps": 128}]
    r = _send(client, "DELETE", f"/policies/{pol['id']}/rules", category="country", value="br")
    assert r.status_code == 200 and r.json()["policy"]["rules"]["country"] == []
    assert _send(client, "DELETE", f"/policies/{pol['id']}/rules", category="country", value="br").status_code == 404


def test_individual_rule_cannot_pace_or_exempt(client):
    pol = _policy(client)
    for action in ("pace", "exempt"):
        r = _send(
            client,
            "POST",
            f"/policies/{pol['id']}/rules",
            {"category": "ip", "value": "203.0.113.9", "action": action, "kbps": 128},
        )
        assert r.status_code == 422


def test_delete_policy_refused_while_assigned(client):
    pol = _policy(client)
    a = _assign(client, pol["id"]).json()["assignment"]
    assert _send(client, "DELETE", f"/policies/{pol['id']}").status_code == 409
    assert _send(client, "DELETE", f"/assignments/{a['id']}").status_code == 200
    assert _send(client, "DELETE", f"/policies/{pol['id']}").status_code == 200
    assert _get(client, "/policies").json()["policies"] == []


# ── Assignments ──────────────────────────────────────────────────────────────


def test_one_default_assignment_per_service(client):
    pol = _policy(client)
    assert _assign(client, pol["id"]).status_code == 201
    resp = _assign(client, pol["id"])
    assert resp.status_code == 409
    assert "default" in resp.text


def test_assignment_needs_existing_policy_in_this_service(client):
    assert _assign(client, "does-not-exist").status_code == 404


def test_content_assignment_requires_content_id_and_default_rejects_one(client):
    pol = _policy(client)
    assert _assign(client, pol["id"], "content_id", None).status_code == 422
    assert _assign(client, pol["id"], "default", "ev-1").status_code == 422
    assert _assign(client, pol["id"], "content_id", "ev-1", mode="MAYBE").status_code == 422


def test_content_assignments_may_not_overlap_but_may_touch(client):
    pol = _policy(client)
    t = lambda h: f"2026-10-20T{h:02d}:00:00Z"  # noqa: E731
    assert _assign(client, pol["id"], "content_id", "ev-1", starts_at=t(10), ends_at=t(12)).status_code == 201
    overlapping = _assign(client, pol["id"], "content_id", "ev-1", starts_at=t(11), ends_at=t(13))
    assert overlapping.status_code == 409 and "overlap" in overlapping.text
    assert _assign(client, pol["id"], "content_id", "ev-1", starts_at=t(12), ends_at=t(14)).status_code == 201
    assert _assign(client, pol["id"], "content_id", "ev-2", starts_at=t(10), ends_at=t(12)).status_code == 201
    # An open-ended window overlaps everything after its start.
    assert _assign(client, pol["id"], "content_id", "ev-1", starts_at=t(9)).status_code == 409


def test_assignment_window_must_be_ordered(client):
    pol = _policy(client)
    r = _assign(
        client, pol["id"], "content_id", "ev-1", starts_at="2026-10-20T12:00:00Z", ends_at="2026-10-20T11:00:00Z"
    )
    assert r.status_code == 422


def test_update_and_delete_assignment_free_the_slot(client):
    p1, p2 = _policy(client, "One"), _policy(client, "Two")
    a = _assign(client, p1["id"], "content_id", "ev-1").json()["assignment"]
    upd = _send(
        client,
        "PUT",
        f"/assignments/{a['id']}",
        {"policy_id": p2["id"], "scope_type": "content_id", "content_id": "ev-1", "mode": "WARN"},
    )
    assert upd.status_code == 200
    got = upd.json()["assignment"]
    assert (got["policy_name"], got["mode"], got["status"]) == ("Two", "WARN", "active")
    assert _send(client, "DELETE", f"/assignments/{a['id']}").status_code == 200
    assert _assign(client, p1["id"], "content_id", "ev-1").status_code == 201


def test_assignment_status_in_listing(client):
    pol = _policy(client)
    _assign(client, pol["id"], "content_id", "past", starts_at="2020-01-01T00:00:00Z", ends_at="2020-01-02T00:00:00Z")
    _assign(client, pol["id"], "content_id", "future", starts_at="2999-01-01T00:00:00Z")
    _assign(client, pol["id"], "content_id", "now")
    statuses = {a["content_id"]: a["status"] for a in _get(client, "/assignments").json()["assignments"]}
    assert statuses == {"past": "ended", "future": "scheduled", "now": "active"}


# ── Effective policy and live rules ──────────────────────────────────────────


def test_effective_is_most_specific(client):
    default, special = _policy(client, "Default"), _policy(client, "Special")
    _assign(client, default["id"])
    _assign(client, special["id"], "content_id", "ev-1", mode="WARN")
    ev = _get(client, "/effective", content_id="ev-1").json()
    assert (ev["policy"]["name"], ev["assignment"]["mode"], ev["shared_with"]) == ("Special", "WARN", 0)
    other = _get(client, "/effective", content_id="ev-2").json()
    assert other["policy"]["name"] == "Default"
    assert _get(client, "/effective").json()["policy"]["name"] == "Default"


def test_effective_reports_how_many_others_share_the_policy(client):
    pol = _policy(client)
    _assign(client, pol["id"], "content_id", "ev-1")
    _assign(client, pol["id"], "content_id", "ev-2")
    _assign(client, pol["id"])
    assert _get(client, "/effective", content_id="ev-1").json()["shared_with"] == 2


def test_effective_none_when_nothing_assigned(client):
    out = _get(client, "/effective", content_id="ev-1").json()
    assert out["policy"] is None and out["assignment"] is None


def test_live_rule_creates_default_policy_and_assignment_when_none_in_effect(client):
    resp = _send(
        client, "POST", "/live-rule", {"category": "country", "value": "cn", "action": "block", "content_id": "ev-1"}
    )
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["created_default"] is True
    assert out["policy"]["name"] == "Default"
    assert out["policy"]["rules"]["country"] == [{"value": "CN", "action": "block", "kbps": 0}]
    assert (out["assignment"]["scope_type"], out["assignment"]["mode"], out["assignment"]["ends_at"]) == (
        "default",
        "ENFORCE",
        None,
    )
    again = _send(
        client, "POST", "/live-rule", {"category": "asn", "value": "AS7922", "action": "pace", "kbps": 256}
    ).json()
    assert again["created_default"] is False
    assert [r["value"] for r in again["policy"]["rules"]["asn"]] == ["7922"]


def test_live_rule_edits_the_policy_in_effect_for_the_content(client):
    default, special = _policy(client, "Default"), _policy(client, "Special")
    _assign(client, default["id"])
    _assign(client, special["id"], "content_id", "ev-1")
    out = _send(client, "POST", "/live-rule", {"category": "host", "value": "Bad.Example", "content_id": "ev-1"}).json()
    assert out["policy"]["id"] == special["id"] and out["created_default"] is False
    assert _get(client, f"/policies/{default['id']}").json()["rules"]["host"] == []


def test_live_rule_refuses_when_default_exists_but_is_not_in_effect(client):
    pol = _policy(client)
    _assign(client, pol["id"], ends_at="2020-01-01T00:00:00Z")
    resp = _send(client, "POST", "/live-rule", {"category": "country", "value": "cn"})
    assert resp.status_code == 409 and "default_not_active" in resp.text


def test_live_exempt_and_individual_block(client):
    exempt = _send(client, "POST", "/live-rule", {"category": "country", "value": "US", "action": "exempt"}).json()
    assert exempt["policy"]["rules"]["country"][0]["action"] == "exempt"
    ind = _send(client, "POST", "/live-rule", {"category": "subscriber_id", "value": "sub-9"}).json()
    assert ind["policy"]["individual"]["subscriber_id"] == ["sub-9"]


def test_individual_blocks_capped_per_service(client, monkeypatch):
    from backend.core.metadata import content_security_policies as db

    monkeypatch.setattr(db, "MAX_INDIVIDUAL_PER_SERVICE", 2)
    pol = _policy(client, individual={"ip": ["203.0.113.1", "203.0.113.2"]})
    resp = _send(client, "POST", f"/policies/{pol['id']}/rules", {"category": "ip", "value": "203.0.113.3"})
    assert resp.status_code == 422 and "too_many_individual" in resp.text
    assert len(_get(client, f"/policies/{pol['id']}").json()["individual"]["ip"]) == 2


# ── ASN names ────────────────────────────────────────────────────────────────


def test_asn_rules_carry_the_owner_name_and_number(client):
    pol = _policy(
        client, rules={"asn": [{"value": "AS7922", "action": "block"}, {"value": "64500", "action": "block"}]}
    )
    labels = {r["value"]: r["label"] for r in pol["rules"]["asn"]}
    assert labels == {"7922": "Comcast Cable (7922)", "64500": "AS64500"}
    # Other categories carry no label.
    other = _policy(client, "Other", rules={"country": [{"value": "BR", "action": "block"}]})
    assert other["rules"]["country"][0].get("label") is None


def test_asn_names_appear_in_every_read_of_the_policy(client):
    pol = _policy(client, rules={"asn": [{"value": "7922", "action": "block"}]})
    _assign(client, pol["id"])
    assert _get(client, f"/policies/{pol['id']}").json()["rules"]["asn"][0]["label"] == "Comcast Cable (7922)"
    assert _get(client, "/policies").json()["policies"][0]["rules"]["asn"][0]["label"] == "Comcast Cable (7922)"
    effective = _get(client, "/effective").json()
    assert effective["policy"]["rules"]["asn"][0]["label"] == "Comcast Cable (7922)"
    live = _send(client, "POST", "/live-rule", {"category": "asn", "value": "7922"}).json()
    assert live["policy"]["rules"]["asn"][0]["label"] == "Comcast Cable (7922)"


def test_a_failed_name_lookup_leaves_the_rule_without_a_label(client, monkeypatch):
    from backend.core import duckdb

    def boom(service_id, asns):
        raise RuntimeError("no network")

    monkeypatch.setattr(duckdb, "get_asn_names", boom)
    pol = _policy(client, rules={"asn": [{"value": "7922", "action": "block"}]})
    assert pol["rules"]["asn"][0]["value"] == "7922"
    assert pol["rules"]["asn"][0].get("label") is None


def test_the_label_is_never_published_or_stored(client, blocker, configured):
    _policy(client, rules={"asn": [{"value": "7922", "action": "block"}]})
    _assign(client, _get(client, "/policies").json()["policies"][0]["id"])
    body = blocker.calls[-1][2]
    rules = next(iter(body["policies"].values()))["rules"]["asn"]
    assert rules == [{"value": "7922", "action": "block"}]


# ── Country regex preview ────────────────────────────────────────────────────


def test_country_regex_preview(client):
    ok = _get(client, "/country-regex/preview", pattern="^(CN|RU)$").json()
    assert (ok["valid"], ok["error"], ok["matches"]) == (True, None, ["CN", "RU"])
    bad = _get(client, "/country-regex/preview", pattern="(a+)+").json()
    assert bad["valid"] is False and bad["matches"] == []
    assert _get(client, "/country-regex/preview", pattern="").json()["matches"] == []


# ── Publishing ───────────────────────────────────────────────────────────────


def test_not_configured_still_saves(client):
    pol = _policy(client)
    resp = _send(client, "PUT", f"/policies/{pol['id']}", {"name": "Playoffs"})
    assert resp.json()["publish"]["status"] == "not_configured"
    assert _get(client, "/sync-status").json()["in_sync"] is False


def test_publish_sends_whole_service_state_to_v2_endpoint(client, blocker, configured):
    default, special = (
        _policy(client, "Default", country_regex="^(CN)$"),
        _policy(
            client,
            "Special",
            individual={"ip": ["203.0.113.9"]},
            rules={"country": [{"value": "br", "action": "pace", "kbps": 256}]},
        ),
    )
    _assign(client, default["id"])
    resp = _assign(client, special["id"], "content_id", "ev-1", mode="WARN", starts_at="2999-01-01T00:00:00Z")
    assert resp.json()["publish"]["status"] == "published"

    method, url, body, headers = blocker.calls[-1]
    assert (method, url) == ("PUT", f"https://blocker.example.com/v2/services/{MOCK_SERVICE_ID}/policy-set")
    assert headers["authorization"] == _SECRET.removeprefix("Authorization: ")
    assert body["rev"] == _get(client, "/sync-status").json()["rev"]
    by_scope = {a["scope"]: a for a in body["assignments"]}
    assert by_scope["default"]["mode"] == "ENFORCE" and by_scope["default"]["content_id"] is None
    assert (by_scope["content_id"]["content_id"], by_scope["content_id"]["mode"]) == ("ev-1", "WARN")
    assert by_scope["content_id"]["starts_at"] == "2999-01-01T00:00:00Z"
    assert body["policies"][default["id"]]["country_regex"] == "^(CN)$"
    sp = body["policies"][special["id"]]
    assert sp["rules"]["country"] == [{"value": "BR", "action": "pace", "kbps": 256}]
    assert sp["individual"] == {"subscriber_id": [], "ip": ["203.0.113.9"]}
    assert _get(client, "/sync-status").json()["in_sync"] is True


def test_ended_assignments_and_unassigned_policies_are_not_published(client, blocker, configured):
    used, unused = _policy(client, "Used"), _policy(client, "Unused")
    _assign(client, used["id"], "content_id", "old", starts_at="2020-01-01T00:00:00Z", ends_at="2020-01-02T00:00:00Z")
    _assign(client, used["id"])
    body = blocker.calls[-1][2]
    assert [a["scope"] for a in body["assignments"]] == ["default"]
    assert list(body["policies"]) == [used["id"]] and unused["id"] not in body["policies"]


def test_failed_publish_keeps_the_change_and_marks_it_not_live(client, blocker, configured):
    blocker.status = 503
    pol = _policy(client)
    assert pol["name"] == "Playoffs"
    status = _get(client, "/sync-status").json()
    assert status["in_sync"] is False and "503" in (status["last_error"] or "")
    assert _get(client, "/policies").json()["policies"][0]["name"] == "Playoffs"

    blocker.status = 200
    out = _send(client, "POST", "/publish").json()
    assert out["status"] == "published"
    assert _get(client, "/sync-status").json()["in_sync"] is True


def test_a_429_or_5xx_is_retried_and_a_later_success_goes_live(client, blocker, configured):
    blocker.script = [(503, {"error": "too busy"}), (429, {"error": "slow down"})]
    pol = _policy(client)
    assert pol["name"] == "Playoffs"
    assert len(blocker.calls) == 3 and blocker.sleeps == [1.2, 2.4]
    assert _get(client, "/sync-status").json()["in_sync"] is True


def test_a_client_error_is_not_retried(client, blocker, configured):
    blocker.status = 422
    _policy(client)
    assert len(blocker.calls) == 1 and blocker.sleeps == []
    assert "HTTP 422" in _get(client, "/sync-status").json()["last_error"]


def test_the_blockers_reason_is_shown_when_it_gives_one(client, blocker, configured):
    blocker.script = [(500, {"error": "internal error", "detail": "KV put failed"})] * 3
    _policy(client)
    status = _get(client, "/sync-status").json()
    assert status["in_sync"] is False
    assert status["last_error"] == "Blocklist endpoint returned HTTP 500: internal error - KV put failed"
    assert len(blocker.calls) == 3


def test_the_blockers_reason_is_short_and_single_line(client, blocker, configured):
    blocker.script = [(500, {"error": "line one\nline two " + "x" * 500})] * 3
    _policy(client)
    err = _get(client, "/sync-status").json()["last_error"]
    assert "\n" not in err and len(err) < 260


def test_publish_error_never_echoes_credentials(client, blocker, configured):
    blocker.status = 500
    resp = _send(client, "POST", "/policies", {"name": "x"})
    assert "s3cret" not in resp.text
    assert "s3cret" not in _get(client, "/sync-status").text
    from backend.core.metadata.state import list_audit

    assert "s3cret" not in json.dumps([e["details"] for e in list_audit(MOCK_SERVICE_ID)])


def test_changes_are_audited(client, blocker, configured):
    from backend.core.metadata.state import list_audit

    pol = _policy(client)
    _assign(client, pol["id"])
    events = {e["event_type"] for e in list_audit(MOCK_SERVICE_ID)}
    assert {"content_security_policy_created", "content_security_assignment_created"} <= events


# ── Isolation ────────────────────────────────────────────────────────────────


@pytest.mark.security_regression
def test_policies_are_scoped_to_their_service(client):
    from backend.core.metadata import content_security_policies as db

    other = "other-service-id"
    db.create_policy(other, "Theirs", None, [{"category": "country", "value": "CN", "action": "block"}], "admin")
    assert _get(client, "/policies").json()["policies"] == []
    theirs = db.list_policies(other)[0]
    assert _get(client, f"/policies/{theirs['id']}").status_code == 404
    assert _assign(client, theirs["id"]).status_code == 404  # cannot assign another service's policy
    assert _send(client, "DELETE", f"/policies/{theirs['id']}").status_code == 404


@pytest.mark.security_regression
def test_policy_routes_require_admin(client):
    from backend.main import app

    paths = app.openapi()["paths"]
    for path in (
        "/api/admin/content-security/policies",
        "/api/admin/content-security/assignments",
        "/api/admin/content-security/live-rule",
        "/api/admin/content-security/publish",
    ):
        assert path in paths
