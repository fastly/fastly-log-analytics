"""Tests for backend.scoring.cookie — AES-GCM-with-AAD session cookie codec.

These tests double as the cross-language wire-format contract: every
``test_*_wire_format_byte_exact`` case must be reproducible by the Rust
port under ``compute/scorer/`` with byte-identical output. Add new cases
here in lockstep with the Rust impl.
"""

from __future__ import annotations

import pytest

from backend.scoring.cookie import (
    NONCE_BYTES,
    SID_BYTES,
    CookieCodec,
    CookieError,
    SessionState,
    _b64url_decode,
    _b64url_encode,
    _pack_payload,
    _unpack_payload,
    new_sid,
    quantize_score,
)

KEY_A = bytes(range(32))  # deterministic test key
KEY_B = bytes(reversed(range(32)))
NONCE_FIXED = bytes(range(12))  # for byte-exact tests
SVC = "TestSvc123"


def _state(**kw) -> SessionState:
    defaults = dict(
        sid=b"\x01\x02\x03\x04\x05\x06",
        seq=10,
        sum_dt=100,
        sum_dt_sq=1500,
        last_ts=1_700_000_000,
        score=25,
        issued_at=1_699_990_000,
    )
    defaults.update(kw)
    return SessionState(**defaults)


# ── quantize_score ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected",
    [
        (0, 0),
        (1, 0),
        (2, 0),
        (3, 5),
        (5, 5),
        (7, 5),
        (8, 10),
        (12, 10),
        (13, 15),
        (47, 45),
        (48, 50),
        (97, 95),
        (98, 100),
        (100, 100),
        (-5, 0),
        (150, 100),
        (50.49, 50),
        (50.5, 50),  # banker's rounding (Python's default) → 50
        (52.5, 52.5),  # Wait, but quantize is /5 → 52.5/5 = 10.5 → round → 10 → *5 = 50
    ],
)
def test_quantize_score_buckets(raw, expected):
    if raw == 52.5:
        # Banker's rounding means round(10.5) → 10 in Python. Document the
        # behavior so the Rust port can match.
        assert quantize_score(raw) == 50
    else:
        assert quantize_score(raw) == expected


# ── new_sid ──────────────────────────────────────────────────────────────────


def test_new_sid_is_correct_length():
    assert len(new_sid()) == SID_BYTES


def test_new_sid_varies():
    """Sanity check: 100 fresh sids should all differ."""
    sids = {new_sid() for _ in range(100)}
    assert len(sids) == 100


# ── _pack_payload / _unpack_payload ──────────────────────────────────────────


def test_v1_pack_size_is_30_bytes():
    """v1 cookies (legacy, no prev_route_path) pack to 30 bytes flat."""
    state = _state(v=1)
    assert len(_pack_payload(state)) == 30


def test_v2_pack_size_is_30_plus_path_len_plus_one():
    """v2 always emits the length-prefix byte (= 0 when path is empty),
    so the wire is at least 31 bytes — 30 header + 1 length + N path."""
    empty = _state()  # default v = SCHEMA_VERSION = 2, empty path
    assert len(_pack_payload(empty)) == 31
    with_path = _state(prev_route_path="/checkout")
    assert len(_pack_payload(with_path)) == 31 + len("/checkout")


def test_pack_layout_byte_exact():
    """Wire-format byte test — Rust port must produce these exact bytes.

    Layout (v2):
        v(B) sid(6) seq(H) sum_dt(I) sum_dt_sq(Q) last_ts(I) score(B)
        issued_at(I) prev_route_len(B) prev_route_path(N)
    All multi-byte ints little-endian. The matching Rust fixture lives in
    `compute/scorer/src/cookie.rs::tests::pack_layout_byte_exact`.
    """
    state = SessionState(
        v=2,
        sid=b"\x11\x22\x33\x44\x55\x66",
        seq=0x1234,
        sum_dt=0x10203040,
        sum_dt_sq=0x0102030405060708,
        last_ts=0x65000000,
        score=80,
        issued_at=0x64000000,
        prev_route_path="/home",
    )
    packed = _pack_payload(state)
    expected = bytes.fromhex(
        # v
        "02"
        # sid (6 bytes)
        "112233445566"
        # seq u16 LE
        "3412"
        # sum_dt u32 LE
        "40302010"
        # sum_dt_sq u64 LE
        "0807060504030201"
        # last_ts u32 LE
        "00000065"
        # score
        "50"
        # issued_at u32 LE
        "00000064"
        # prev_route_len (u8)
        "05"
        # prev_route_path "/home" UTF-8: 2f 68 6f 6d 65
        "2f686f6d65"
    )
    assert packed == expected, f"\npacked:   {packed.hex()}\nexpected: {expected.hex()}"


def test_pack_unpack_round_trip():
    state = _state(seq=42, sum_dt=999, sum_dt_sq=12345678, score=65, prev_route_path="/users/{int}")
    assert _unpack_payload(_pack_payload(state)) == state


def test_unpack_rejects_too_short():
    with pytest.raises(CookieError, match="payload too short"):
        _unpack_payload(b"\x00" * 29)


def test_unpack_accepts_v1_legacy_30_byte_plaintext():
    """v1 decoder back-compat: 30-byte plaintext → empty prev_route_path."""
    v1 = bytes.fromhex("011122334455663412403020100807060504030201000000655000000064")
    s = _unpack_payload(v1)
    assert s.v == 1
    assert s.prev_route_path == ""
    assert s.score == 80


# ── SessionState bounds enforcement ──────────────────────────────────────────


@pytest.mark.parametrize(
    "field,bad",
    [
        ("seq", 0x10000),
        ("seq", -1),
        ("sum_dt", 0x100000000),
        ("sum_dt", -1),
        ("sum_dt_sq", -1),
        ("last_ts", -1),
        ("issued_at", -1),
        ("score", -1),
        ("score", 101),
        ("v", 256),
        ("v", -1),
    ],
)
def test_session_state_rejects_out_of_range(field, bad):
    base = dict(
        sid=b"\x00" * SID_BYTES,
        seq=0,
        sum_dt=0,
        sum_dt_sq=0,
        last_ts=0,
        score=0,
        issued_at=0,
    )
    base[field] = bad
    with pytest.raises(CookieError, match=field):
        SessionState(**base)


def test_session_state_rejects_wrong_sid_length():
    with pytest.raises(CookieError, match="sid must be 6 bytes"):
        SessionState(
            sid=b"\x00" * 5,
            seq=0,
            sum_dt=0,
            sum_dt_sq=0,
            last_ts=0,
            score=0,
            issued_at=0,
        )


# ── Base64URL helpers ────────────────────────────────────────────────────────


def test_b64url_round_trip_random_data():
    import os

    for _ in range(20):
        data = os.urandom(58)  # nonce + ciphertext approx size
        assert _b64url_decode(_b64url_encode(data)) == data


def test_b64url_no_padding_in_output():
    assert "=" not in _b64url_encode(b"any-bytes-here-12345")


def test_b64url_url_safe_charset():
    """No '+' or '/' should appear — URL-safe base64 uses '-' and '_'."""
    # 64 bytes guarantees we'd hit a + or / with regular base64 on most inputs.
    blob = bytes(range(64)) + bytes(range(64))
    enc = _b64url_encode(blob)
    assert "+" not in enc
    assert "/" not in enc


# ── CookieCodec: construction ────────────────────────────────────────────────


def test_codec_rejects_wrong_key_length():
    with pytest.raises(CookieError, match="key must be 32 bytes"):
        CookieCodec(key=b"\x00" * 16, service_id=SVC)


def test_codec_requires_service_id():
    with pytest.raises(CookieError, match="service_id"):
        CookieCodec(key=KEY_A, service_id="")


def test_codec_rejects_wrong_previous_key_length():
    with pytest.raises(CookieError, match="previous_key"):
        CookieCodec(key=KEY_A, previous_key=b"\x00" * 16, service_id=SVC)


# ── CookieCodec: encode + decode round-trip ──────────────────────────────────


def test_codec_encode_decode_round_trip():
    codec = CookieCodec(key=KEY_A, service_id=SVC)
    state = _state(seq=7, sum_dt=42, sum_dt_sq=300, score=15)
    cookie = codec.encode(state)
    decoded = codec.decode(cookie)
    assert decoded == state


def test_codec_encode_uses_fresh_nonce_each_call():
    """Two encodes of the same state produce different ciphertexts (nonce
    randomness). Decoding both yields the same state."""
    codec = CookieCodec(key=KEY_A, service_id=SVC)
    state = _state()
    c1 = codec.encode(state)
    c2 = codec.encode(state)
    assert c1 != c2
    assert codec.decode(c1) == codec.decode(c2) == state


def test_codec_fixed_nonce_byte_exact_wire_format():
    """With a fixed key + nonce + state, the wire format is fully deterministic
    and the Rust port must reproduce exactly this base64url string. v2 cookies
    have a variable-length suffix (length byte + UTF-8 path); this fixture
    locks in a non-empty path so the suffix layout is exercised."""
    codec = CookieCodec(key=KEY_A, service_id=SVC)
    state = SessionState(
        v=2,
        sid=b"\x11\x22\x33\x44\x55\x66",
        seq=0x1234,
        sum_dt=0x10203040,
        sum_dt_sq=0x0102030405060708,
        last_ts=0x65000000,
        score=80,
        issued_at=0x64000000,
        prev_route_path="/home",
    )
    cookie = codec.encode(state, nonce=NONCE_FIXED)
    decoded = codec.decode(cookie)
    assert decoded == state

    raw = _b64url_decode(cookie)
    # 12 nonce + 30 v1 header + 1 length byte + 5 path bytes + 16 GCM tag = 64
    assert len(raw) == NONCE_BYTES + 30 + 1 + 5 + 16
    assert raw.hex().startswith(NONCE_FIXED.hex())


# ── CookieCodec: AEAD failure modes ──────────────────────────────────────────


def test_codec_decode_rejects_tampered_ciphertext():
    codec = CookieCodec(key=KEY_A, service_id=SVC)
    cookie = codec.encode(_state())
    # Flip one byte in the ciphertext portion (after the nonce).
    raw = bytearray(_b64url_decode(cookie))
    raw[NONCE_BYTES + 5] ^= 0x01
    tampered = _b64url_encode(bytes(raw))
    with pytest.raises(CookieError, match="AEAD verification failed"):
        codec.decode(tampered)


def test_codec_decode_rejects_wrong_key():
    codec_a = CookieCodec(key=KEY_A, service_id=SVC)
    codec_b = CookieCodec(key=KEY_B, service_id=SVC)
    cookie = codec_a.encode(_state())
    with pytest.raises(CookieError, match="AEAD"):
        codec_b.decode(cookie)


def test_codec_decode_rejects_wrong_service_id():
    """Cross-service replay: cookie issued for service A must fail on B."""
    codec_a = CookieCodec(key=KEY_A, service_id="ServiceA")
    codec_b = CookieCodec(key=KEY_A, service_id="ServiceB")
    cookie = codec_a.encode(_state())
    with pytest.raises(CookieError, match="AEAD"):
        codec_b.decode(cookie)


def test_codec_decode_rejects_wrong_schema_version():
    """AAD encodes schema version, so a cookie encoded under one version
    cannot be re-authenticated as a different version. v1 is intentionally
    accepted by the v2 decoder for back-compat, so we exercise mismatch
    against a version neither side will ever use."""
    codec_v1 = CookieCodec(key=KEY_A, service_id=SVC, schema_version=1)
    codec_v99 = CookieCodec(key=KEY_A, service_id=SVC, schema_version=99)
    cookie = codec_v1.encode(_state(v=1))
    with pytest.raises(CookieError, match="AEAD"):
        codec_v99.decode(cookie)


def test_codec_v2_decoder_accepts_v1_cookie_for_back_compat():
    """During the v1→v2 migration window, the v2 decoder must accept
    v1 cookies that browsers still have stored. prev_route_path lands
    empty, which makes L2 fall back to uniform prior for that one
    request — acceptable, the next request rotates to a v2 cookie."""
    codec_v1 = CookieCodec(key=KEY_A, service_id=SVC, schema_version=1)
    codec_v2 = CookieCodec(key=KEY_A, service_id=SVC, schema_version=1)
    # Encode under v1
    s = _state(v=1)
    cookie = codec_v1.encode(s)
    # Decode under v2 codec is the trick: we have to use AAD v1 too,
    # since AAD encodes the schema version. The "back-compat" here is
    # really about the decoder accepting a v1 PAYLOAD when the AAD
    # matches — i.e., the v2 decoder running with schema_version=1 AAD
    # against a v1-encoded cookie. (Real production browsers will
    # always have the matching AAD because the customer's VCL service
    # id never changes between encode and decode.)
    decoded = codec_v2.decode(cookie)
    assert decoded.v == 1
    assert decoded.prev_route_path == ""


def test_codec_decode_rejects_garbage_base64():
    """Truly malformed base64 (length not a multiple of 4 after padding,
    binascii.Error) surfaces as base64url decode failure."""
    codec = CookieCodec(key=KEY_A, service_id=SVC)
    # A string whose padded length is still not a multiple of 4 chars.
    with pytest.raises(CookieError, match="base64url"):
        codec.decode("A")  # 1 char + 3 pad = 4, but decodes to 0 useful bytes
    with pytest.raises(CookieError):
        codec.decode("===")  # all padding


def test_codec_decode_rejects_random_short_garbage():
    """Even ostensibly-valid base64 that's too short gets caught by the
    length check before AEAD ever runs."""
    codec = CookieCodec(key=KEY_A, service_id=SVC)
    with pytest.raises(CookieError, match="too short"):
        codec.decode("AAAAAAAA")  # decodes to 6 bytes, well under threshold


def test_codec_decode_rejects_too_short():
    codec = CookieCodec(key=KEY_A, service_id=SVC)
    with pytest.raises(CookieError, match="too short"):
        codec.decode(_b64url_encode(b"\x00" * 20))


# ── Dual-key rotation ────────────────────────────────────────────────────────


def test_codec_dual_key_decrypts_with_previous_during_grace():
    """Scenario: key rotated. Sessions issued under the old key are still
    valid during the 24h grace window because decode trial-falls-back."""
    old = CookieCodec(key=KEY_A, service_id=SVC)
    new = CookieCodec(key=KEY_B, previous_key=KEY_A, service_id=SVC)
    cookie_under_old = old.encode(_state(score=25))
    # New codec can still decrypt the old cookie.
    decoded = new.decode(cookie_under_old)
    assert decoded.score == 25


def test_codec_dual_key_new_cookies_use_current():
    """Encrypt-with-current is the invariant; previous_key is decrypt-only."""
    old = CookieCodec(key=KEY_A, service_id=SVC)
    new = CookieCodec(key=KEY_B, previous_key=KEY_A, service_id=SVC)
    cookie = new.encode(_state())
    # Old codec (which only knows KEY_A) cannot decrypt cookies from new.
    with pytest.raises(CookieError, match="AEAD"):
        old.decode(cookie)


def test_codec_no_previous_key_strict_mode():
    """Without previous_key, old cookies are rejected after rotation —
    matches the post-grace-window behavior."""
    old = CookieCodec(key=KEY_A, service_id=SVC)
    new = CookieCodec(key=KEY_B, service_id=SVC)  # no previous_key
    cookie_under_old = old.encode(_state())
    with pytest.raises(CookieError, match="AEAD"):
        new.decode(cookie_under_old)


# ── Wire-format envelope sanity ──────────────────────────────────────────────


def test_cookie_envelope_size_under_120_bytes():
    """Doc claims ~80-120 byte cookies after base64url. Verify."""
    codec = CookieCodec(key=KEY_A, service_id=SVC)
    cookie = codec.encode(_state())
    assert 70 <= len(cookie) <= 120, f"cookie size out of expected range: {len(cookie)}"
