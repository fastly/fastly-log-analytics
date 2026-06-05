"""Tests for backend.scoring.labels — CRUD over the scoring_labels table.

The isolate_metadata_db fixture in tests/conftest.py points
backend.core.metadata_db at a per-test sandbox SQLite DB; the schema
is initialized on first get_con() call so each test starts clean."""

from __future__ import annotations

import pytest

from backend.scoring import labels as _labels

SVC = "svc1"


def test_save_label_basic_round_trip():
    saved = _labels.save_label(
        SVC,
        sid="abc123def456",
        label="bad",
        notes="probable scraper",
        flagged_by="alice",
        sample_ip="1.2.3.4",
        sample_ua="curl/8.4",
        sample_url="/login",
    )
    assert saved["sid"] == "abc123def456"
    assert saved["label"] == "bad"
    assert saved["notes"] == "probable scraper"
    assert saved["sample_ip"] == "1.2.3.4"
    assert saved["created_at"]

    fetched = _labels.get_label(SVC, "abc123def456")
    assert fetched is not None
    assert fetched["id"] == saved["id"]
    assert fetched["label"] == "bad"


def test_save_label_upserts_on_sid():
    """Re-labeling the same sid must UPDATE in place — same id, new label."""
    first = _labels.save_label(SVC, sid="sid001", label="bad")
    assert first["label"] == "bad"

    second = _labels.save_label(SVC, sid="sid001", label="good", notes="actually a real user")
    # Same row, updated label + notes
    assert second["id"] == first["id"]
    assert second["label"] == "good"
    assert second["notes"] == "actually a real user"

    # And only one row exists
    rows = _labels.list_labels(SVC)
    assert len([r for r in rows if r["sid"] == "sid001"]) == 1


def test_save_label_rejects_unknown_label():
    with pytest.raises(ValueError, match="label must be one of"):
        _labels.save_label(SVC, sid="x", label="maybe")


def test_save_label_requires_sid():
    with pytest.raises(ValueError, match="sid is required"):
        _labels.save_label(SVC, sid="", label="good")


def test_list_labels_most_recent_first():
    _labels.save_label(SVC, sid="aaa", label="good")
    _labels.save_label(SVC, sid="bbb", label="bad")
    _labels.save_label(SVC, sid="ccc", label="neutral")

    rows = _labels.list_labels(SVC)
    # All three present, most-recent first
    assert [r["sid"] for r in rows[:3]] == ["ccc", "bbb", "aaa"]


def test_get_label_returns_none_when_missing():
    assert _labels.get_label(SVC, "never-labeled") is None


def test_update_label_patch_semantics():
    saved = _labels.save_label(SVC, sid="u1", label="bad", notes="initial")

    # Update only notes
    after_notes = _labels.update_label(SVC, saved["id"], notes="revised note")
    assert after_notes["label"] == "bad"
    assert after_notes["notes"] == "revised note"

    # Update only label
    after_label = _labels.update_label(SVC, saved["id"], label="neutral")
    assert after_label["label"] == "neutral"
    assert after_label["notes"] == "revised note"  # unchanged


def test_update_label_rejects_bad_label():
    saved = _labels.save_label(SVC, sid="u2", label="good")
    with pytest.raises(ValueError, match="label must be one of"):
        _labels.update_label(SVC, saved["id"], label="ugly")


def test_update_label_noop_when_no_fields_passed():
    saved = _labels.save_label(SVC, sid="u3", label="good")
    # Calling without any patchable fields returns current state, no-op.
    result = _labels.update_label(SVC, saved["id"])
    assert result["id"] == saved["id"]
    assert result["label"] == "good"


def test_delete_label_is_idempotent():
    saved = _labels.save_label(SVC, sid="d1", label="bad")
    r1 = _labels.delete_label(SVC, saved["id"])
    assert r1["status"] == "success"

    r2 = _labels.delete_label(SVC, saved["id"])
    assert r2["status"] == "success"  # second delete no-ops cleanly

    assert _labels.get_label(SVC, "d1") is None


def test_counts_by_label_always_returns_three_keys():
    # No labels yet
    counts = _labels.counts_by_label(SVC)
    assert counts == {"good": 0, "bad": 0, "neutral": 0}

    _labels.save_label(SVC, sid="g1", label="good")
    _labels.save_label(SVC, sid="g2", label="good")
    _labels.save_label(SVC, sid="b1", label="bad")
    counts = _labels.counts_by_label(SVC)
    assert counts == {"good": 2, "bad": 1, "neutral": 0}


def test_labels_isolated_per_service():
    _labels.save_label("svcA", sid="shared", label="good")
    _labels.save_label("svcB", sid="shared", label="bad")
    # Same sid in two services → two independent labels.
    a = _labels.get_label("svcA", "shared")
    b = _labels.get_label("svcB", "shared")
    assert a["label"] == "good"
    assert b["label"] == "bad"
    assert a["id"] != b["id"]
