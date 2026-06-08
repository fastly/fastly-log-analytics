"""Tests for backend.provision.session_scoring_setup.rotate_aes_key.

The rotator's contract: pull current_key_hex out of the scoring_keys
ConfigStore, slot it into previous_key_hex, generate a fresh 32-byte hex
key, write it as the new current_key_hex. Each test pins one axis of
that contract — first-rotation cold start, second-rotation steady state,
upsert fallback when PATCH 404s, input validation, and HTTP error
propagation. Mocking pattern mirrors test_session_scoring_setup.py:
patch the module-level ``fastly`` symbol with a MagicMock whose
``side_effect`` is keyed off ``(method, path)``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.provision import session_scoring_setup as sss
from backend.provision.session_scoring_setup import (
    CURRENT_KEY_HEX,
    PREVIOUS_KEY_HEX,
    rotate_aes_key,
)

KEYS_STORE_ID = "KEYS_STORE_XYZ"
TOKEN = "FAKE_TOKEN"

CURRENT_ITEM_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item/{CURRENT_KEY_HEX}"
PREVIOUS_ITEM_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item/{PREVIOUS_KEY_HEX}"
ITEM_COLLECTION_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item"


def _hex64(byte: int) -> str:
    """Helper: produce a deterministic 64-char hex string for fixtures."""
    return f"{byte:02x}" * 32


# ── Happy paths ──────────────────────────────────────────────────────────────


def test_first_rotation_moves_current_into_previous_and_writes_fresh_current():
    """Cold start: scoring_keys has current_key_hex only (no previous_key_hex
    yet — that slot is created on first rotation). The old current must
    be PATCHed/POSTed into the previous slot, and a brand-new 32-byte
    key written as the new current."""
    old_current = _hex64(0xAB)
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": old_current}
        # PATCH on previous_key_hex 404s — slot doesn't exist yet.
        if method == "PATCH" and path == PREVIOUS_ITEM_PATH:
            raise RuntimeError("HTTP 404 PATCH /.../previous_key_hex\n    not found")
        # POST upsert fallback succeeds, as does PATCH on the existing
        # current_key_hex slot.
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    # Returned dict: new 64-char hex current, old value preserved as
    # previous, ISO-8601 rotation timestamp.
    assert len(result["current_key_hex"]) == 64
    int(result["current_key_hex"], 16)  # valid hex
    assert result["previous_key_hex"] == old_current
    assert result["current_key_hex"] != old_current
    assert "T" in result["rotated_at"]  # ISO 8601

    # The previous slot was written with the OLD current value (PATCH 404
    # → POST upsert).
    prev_writes = [
        c
        for c in calls_in_order
        if c[1] == PREVIOUS_ITEM_PATH
        or c[1] == ITEM_COLLECTION_PATH
        and c[2]
        and c[2].get("item_key") == PREVIOUS_KEY_HEX
    ]
    # Either form of write (PATCH or POST-collection) should carry the old_current value.
    prev_values = [
        (c[2] or {}).get("item_value")
        for c in calls_in_order
        if (c[1] == PREVIOUS_ITEM_PATH and c[0] == "PATCH")
        or (c[1] == ITEM_COLLECTION_PATH and c[0] == "POST" and (c[2] or {}).get("item_key") == PREVIOUS_KEY_HEX)
    ]
    assert old_current in prev_values, f"previous slot was never written with old current: {calls_in_order}"

    # The current slot was overwritten with the NEW key.
    cur_writes = [
        (c[2] or {}).get("item_value")
        for c in calls_in_order
        if (c[1] == CURRENT_ITEM_PATH and c[0] == "PATCH")
        or (c[1] == ITEM_COLLECTION_PATH and c[0] == "POST" and (c[2] or {}).get("item_key") == CURRENT_KEY_HEX)
    ]
    assert result["current_key_hex"] in cur_writes

    # Both keys must be 64 hex chars AND distinct.
    assert len(old_current) == 64
    assert len(result["current_key_hex"]) == 64
    assert old_current != result["current_key_hex"]


def test_second_rotation_overwrites_previous_with_old_current():
    """Steady state: both current_key_hex and previous_key_hex exist in the
    store. After rotation: previous = old-current; current = brand-new
    key. The old previous is dropped (one grace level by design)."""
    old_current = _hex64(0xCD)
    old_previous = _hex64(0xEF)  # this value should be DISCARDED
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": old_current}
        # Both slots already exist — PATCH succeeds on both.
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    assert result["previous_key_hex"] == old_current
    assert result["current_key_hex"] != old_current
    assert result["current_key_hex"] != old_previous  # not somehow recycled

    # The previous slot was PATCHed with old_current (not the prior previous).
    prev_patches = [c for c in calls_in_order if c[0] == "PATCH" and c[1] == PREVIOUS_ITEM_PATH]
    assert len(prev_patches) == 1
    assert prev_patches[0][2] == {"item_value": old_current}

    # The current slot was PATCHed with the new key.
    cur_patches = [c for c in calls_in_order if c[0] == "PATCH" and c[1] == CURRENT_ITEM_PATH]
    assert len(cur_patches) == 1
    assert cur_patches[0][2] == {"item_value": result["current_key_hex"]}

    # The dropped old_previous value MUST NOT appear in any write body —
    # it should be irretrievable after this rotation.
    written_values = [(c[2] or {}).get("item_value") for c in calls_in_order if c[0] in ("PATCH", "POST") and c[2]]
    assert old_previous not in written_values


# ── PATCH-then-POST upsert fallback ──────────────────────────────────────────


def test_patch_404_falls_back_to_post_for_current_key_creation():
    """Edge case: the current_key_hex slot itself is missing (extreme
    cold start, e.g. store was wiped). PATCH on current 404s → POST
    upsert is tried with item_key=current_key_hex."""
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            # No current key in the store at all.
            raise RuntimeError("HTTP 404 GET /.../current_key_hex\n    not found")
        if method == "PATCH":
            raise RuntimeError("HTTP 404 PATCH\n    item does not exist")
        # POST always succeeds.
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    # prev_value defaults to "" when GET failed, so previous slot is not
    # written at all on this code path.
    assert result["previous_key_hex"] == ""

    # The current key was POSTed (not PATCHed — that 404'd).
    post_creates = [c for c in calls_in_order if c[0] == "POST" and c[1] == ITEM_COLLECTION_PATH]
    assert any(c[2] == {"item_key": CURRENT_KEY_HEX, "item_value": result["current_key_hex"]} for c in post_creates), (
        f"no POST upsert for current_key_hex: {calls_in_order}"
    )

    # And a PATCH was attempted FIRST (the fallback contract).
    patch_attempts = [c for c in calls_in_order if c[0] == "PATCH" and c[1] == CURRENT_ITEM_PATH]
    assert len(patch_attempts) == 1
    assert patch_attempts[0][2] == {"item_value": result["current_key_hex"]}


# ── Input validation ────────────────────────────────────────────────────────


def test_empty_store_id_raises_value_error():
    """Caller bug: rotate without a store id. Must fail fast before any
    Fastly API call — calling the API with an empty path would build a
    malformed URL and waste a round trip."""
    fastly_mock = MagicMock()
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        with pytest.raises(ValueError, match="scoring_keys_store_id is required"):
            rotate_aes_key("", token=TOKEN)
    fastly_mock.assert_not_called()


def test_empty_token_propagates_http_401_as_runtime_error():
    """Empty token reaches the Fastly client, which sends an empty
    Fastly-Key header → 401. The GET is wrapped in try/except so prev_value
    falls back to ""; the subsequent PATCH-then-POST attempts on
    current_key_hex both 401 and the POST surfaces as RuntimeError
    (no inner handler swallows it)."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        # Every call comes back as 401 because the token is empty.
        raise RuntimeError("HTTP 401 unauthorized\n    invalid token")

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        with pytest.raises(RuntimeError, match="401"):
            rotate_aes_key(KEYS_STORE_ID, token="")


def test_401_from_fastly_surfaces_as_runtime_error():
    """An explicit auth failure (bad-but-non-empty token) must NOT be
    silently swallowed by the PATCH→POST fallback. The first PATCH-then-POST
    pair on the current slot should bubble the RuntimeError up to the
    caller — losing the rotation silently would leave the cookies-issued-now
    cohort un-decodable on the next deploy."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "GET":
            # GET is in a try/except — return a value so we get past it.
            return {"item_key": CURRENT_KEY_HEX, "item_value": _hex64(0x11)}
        # All writes (PATCH, POST) 401.
        raise RuntimeError("HTTP 401 unauthorized\n    bad token")

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        with pytest.raises(RuntimeError, match="401"):
            rotate_aes_key(KEYS_STORE_ID, token="bad-token")


# ── Cryptographic-quality keys ──────────────────────────────────────────────


def test_two_successive_rotations_produce_different_current_keys():
    """``secrets.token_hex(32)`` must be the source, not anything
    predictable. Two back-to-back rotations should produce two distinct
    64-char hex strings. (Birthday-paradox collision probability at
    256 bits of entropy is ~2^-128 — effectively impossible.)"""

    state = {"current": _hex64(0x01)}

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": state["current"]}
        if method == "PATCH" and path == CURRENT_ITEM_PATH and body:
            # Track what the rotator wrote so the next GET reflects it.
            state["current"] = body["item_value"]
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        first = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)
        second = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    # Distinct keys, each 64 hex chars (full 32 bytes of entropy).
    assert len(first["current_key_hex"]) == 64
    assert len(second["current_key_hex"]) == 64
    assert first["current_key_hex"] != second["current_key_hex"], (
        "two successive rotations produced identical keys — RNG is broken or stubbed"
    )

    # The second rotation's previous_key_hex should equal the first's
    # current_key_hex (proper key-history chaining).
    assert second["previous_key_hex"] == first["current_key_hex"]


# ── Module-level reference smoke check ──────────────────────────────────────


def test_rotate_aes_key_uses_secrets_token_hex():
    """Belt-and-suspenders: patch secrets.token_hex inside the module
    namespace and verify the rotator actually consults it (so a future
    refactor to a weaker RNG would fail this test loudly)."""
    sentinel = "ff" * 32
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": _hex64(0x22)}
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    # The function imports ``secrets`` locally inside its body, so we
    # patch the module attribute that the import resolves against.
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("secrets.token_hex", return_value=sentinel) as token_hex_mock,
    ):
        result = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    assert result["current_key_hex"] == sentinel
    token_hex_mock.assert_called_with(32)
    # Sanity: module reference still intact after the patch.
    assert sss.CURRENT_KEY_HEX == "current_key_hex"
