"""AES-GCM-with-AAD session cookie codec (reference implementation).

Authenticated-encrypted session state for the edge scorer. The Rust/Wasm
port under ``compute/scorer/`` must round-trip these bytes 1:1 — every
fixture in ``tests/scoring/fixtures/cookies/`` is used by both impls.

Per the research doc §3.1 / §3.3 the payload carries 8 fields totalling
30 plaintext bytes. The doc nominally specifies CBOR but we use a packed
little-endian struct instead — same wire size, no cross-language
canonical-ordering footguns. The schema version byte (``v``) is the first
field of the plaintext, so future format changes can be version-dispatched
on decode without changing the framing.

Wire format (after AES-GCM and Base64URL):

    base64url(   nonce (12 B)  ||  AES-GCM(plaintext, aad) || tag (16 B)   )

    plaintext (variable, little-endian):
        v                u8     schema version       ← first byte for dispatch
        sid              6 B    raw session id bytes
        seq              u16    sequence count (cap 65535)
        sum_dt           u32    Σ Δt seconds
        sum_dt_sq        u64    Σ Δt² seconds² (widened per §3.3)
        last_ts          u32    last-request unix epoch
        score            u8     quantized 0-100 (rounded to nearest 5)
        issued_at        u32    cookie creation unix epoch  ← end of v1 (30 B)
        prev_route_len   u8     length of prev_route_path (v2 only, 0-255)
        prev_route_path  N B    normalized path of last-scored URL (UTF-8)

    prev_route_path carries the session's most-recently-scored route so the
    scorer can compute the L2 transition probability without VCL having to
    pass prev_route as a header — req.http doesn't persist across separate
    client requests, so a header-based mechanism never worked.

    Decoder accepts v1 (30-byte plaintext, no prev_route_path) for the
    migration window. Encoder always emits the current SCHEMA_VERSION.

    aad: ascii(f"{service_id}|v{schema_version}")
         — binds the cookie to one customer service AND one schema
           version, blocking cross-service replay and version downgrade.

Key rotation: pass a previous key alongside the current one to
``CookieCodec``; ``decode`` trial-decrypts with the current key first and
falls back to the previous key on AEAD failure. Encrypt always uses the
current key. This is the 24h dual-key grace described in §3.1.
"""

from __future__ import annotations

import base64
import os
import secrets
import struct
from dataclasses import dataclass
from typing import Final

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

SCHEMA_VERSION: Final[int] = 2
SID_BYTES: Final[int] = 6
NONCE_BYTES: Final[int] = 12
KEY_BYTES: Final[int] = 32  # AES-256
SCORE_BUCKET: Final[int] = 5  # quantize to nearest 5 per §3.3
# v2 adds a length-prefixed UTF-8 path suffix. The length byte (u8)
# caps the path at 255 bytes; encoder truncates longer paths silently.
PREV_ROUTE_MAX_BYTES: Final[int] = 255

# v1 plaintext: 30 bytes fixed. v2 plaintext: 30 + 1 (length) + N (path).
_V1_PACK_FMT: Final[str] = "<B 6s H I Q I B I"
_V1_PACK_SIZE: Final[int] = struct.calcsize(_V1_PACK_FMT)
assert _V1_PACK_SIZE == 30, f"v1 pack size drifted: {_V1_PACK_SIZE} != 30"

# Sliding caps per §3.3. Going over u16 for seq triggers a fresh sid (see
# encode); the u32 / u64 widths leave plenty of headroom for any realistic
# session (24h of requests at 1Hz = 86400 events, well under both ceilings).
SEQ_MAX: Final[int] = 0xFFFF
SUM_DT_MAX: Final[int] = 0xFFFFFFFF
SUM_DT_SQ_MAX: Final[int] = 0xFFFFFFFFFFFFFFFF
TS_MAX: Final[int] = 0xFFFFFFFF  # year 2106; we re-window before then


class CookieError(Exception):
    """Raised when a cookie is malformed, mis-keyed, tampered with, or for the
    wrong service / schema version. The caller should treat any of these the
    same way: discard the cookie, issue a fresh sid."""


@dataclass(frozen=True)
class SessionState:
    """Plaintext payload of one session cookie. Immutable so call sites can't
    accidentally mutate the version stored in the request-scoped cache."""

    sid: bytes  # SID_BYTES raw bytes
    seq: int
    sum_dt: int  # seconds
    sum_dt_sq: int  # seconds²
    last_ts: int  # unix epoch
    score: int  # 0-100, quantized to nearest 5
    issued_at: int  # unix epoch
    v: int = SCHEMA_VERSION
    # v2: normalized path of the most-recently-scored URL for this session.
    # Empty on v1 cookies and on first-request-in-session. Truncated to
    # PREV_ROUTE_MAX_BYTES at encode time; the failure mode of truncation
    # is "L2 falls back to uniform-prior probability for this request",
    # not "crash".
    prev_route_path: str = ""

    def __post_init__(self) -> None:
        # Bounds enforcement is part of the contract — callers building a
        # state to encode must hand us values that fit the wire format.
        # Trapping here gives a much better stack than struct.pack's cryptic
        # "argument out of range".
        if len(self.sid) != SID_BYTES:
            raise CookieError(f"sid must be {SID_BYTES} bytes, got {len(self.sid)}")
        if not 0 <= self.seq <= SEQ_MAX:
            raise CookieError(f"seq out of range: {self.seq}")
        if not 0 <= self.sum_dt <= SUM_DT_MAX:
            raise CookieError(f"sum_dt out of range: {self.sum_dt}")
        if not 0 <= self.sum_dt_sq <= SUM_DT_SQ_MAX:
            raise CookieError(f"sum_dt_sq out of range: {self.sum_dt_sq}")
        if not 0 <= self.last_ts <= TS_MAX:
            raise CookieError(f"last_ts out of range: {self.last_ts}")
        if not 0 <= self.issued_at <= TS_MAX:
            raise CookieError(f"issued_at out of range: {self.issued_at}")
        if not 0 <= self.score <= 100:
            raise CookieError(f"score out of range: {self.score}")
        if not 0 <= self.v <= 0xFF:
            raise CookieError(f"v out of range: {self.v}")


def quantize_score(raw: float | int) -> int:
    """Round to nearest SCORE_BUCKET (default 5), clamp to [0, 100].

    Per §1.3 this is the information-leak countermeasure if the cookie is
    ever decrypted: an attacker who reads a quantized 65 doesn't know
    whether they're at 63 or 67 — losing fine-grained gradient information
    they could use to titrate against the threshold."""
    if raw < 0:
        return 0
    if raw > 100:
        return 100
    bucket = SCORE_BUCKET
    return int(round(float(raw) / bucket)) * bucket


def new_sid() -> bytes:
    """6 cryptographically-random bytes. 2^48 ≈ 281 trillion unique sids."""
    return secrets.token_bytes(SID_BYTES)


def _pack_payload(state: SessionState) -> bytes:
    """Pack a state into wire format. v1 (legacy) is the 30-byte fixed
    header. v2 adds a length-prefixed UTF-8 path suffix; always emit the
    length byte even when path is empty so the decoder can dispatch on
    plaintext length unambiguously (== 30 → v1, > 30 → v2)."""
    head = struct.pack(
        _V1_PACK_FMT,
        state.v,
        state.sid,
        state.seq,
        state.sum_dt,
        state.sum_dt_sq,
        state.last_ts,
        state.score,
        state.issued_at,
    )
    if state.v == 1:
        return head
    path_bytes = state.prev_route_path.encode("utf-8")[:PREV_ROUTE_MAX_BYTES]
    path_bytes = path_bytes.decode("utf-8", errors="ignore").encode("utf-8")
    return head + bytes([len(path_bytes)]) + path_bytes


def _unpack_payload(buf: bytes) -> SessionState:
    if len(buf) < _V1_PACK_SIZE:
        raise CookieError(f"payload too short: {len(buf)} < {_V1_PACK_SIZE}")
    v, sid, seq, sum_dt, sum_dt_sq, last_ts, score, issued_at = struct.unpack(_V1_PACK_FMT, buf[:_V1_PACK_SIZE])
    prev_route_path = ""
    if len(buf) > _V1_PACK_SIZE:
        path_len = buf[_V1_PACK_SIZE]
        end = _V1_PACK_SIZE + 1 + path_len
        if len(buf) != end:
            raise CookieError(
                f"prev_route_path length mismatch: payload {len(buf)} bytes, "
                f"declared len {path_len}, expected end {end}"
            )
        try:
            prev_route_path = buf[_V1_PACK_SIZE + 1 : end].decode("utf-8")
        except UnicodeDecodeError as e:
            raise CookieError(f"prev_route_path utf-8 decode failed: {e}") from e
    return SessionState(
        sid=sid,
        seq=seq,
        sum_dt=sum_dt,
        sum_dt_sq=sum_dt_sq,
        last_ts=last_ts,
        score=score,
        issued_at=issued_at,
        v=v,
        prev_route_path=prev_route_path,
    )


def _aad(service_id: str, schema_version: int) -> bytes:
    """AAD ties the cookie to one customer service AND one schema version.

    Format chosen to be trivially reproducible in any language: ASCII
    ``{service_id}|v{N}`` with no padding, no length prefix, no JSON.
    """
    return f"{service_id}|v{schema_version}".encode("ascii")


def _b64url_encode(data: bytes) -> str:
    """Base64URL without padding — cookie-safe, RFC 4648 §5."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    # Re-pad to a multiple of 4 before standard decode.
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


@dataclass
class CookieCodec:
    """Encode and decode session cookies.

    Construct with the current 32-byte AES key. Pass ``previous_key`` during
    the 24h grace window after a key rotation; ``decode`` then trial-
    decrypts with the current key first and falls back to the previous on
    AEAD failure (the right move because AEAD verification is constant-time
    relative to the key, so the fallback adds at most one AES-GCM verify
    on the unhappy path).
    """

    key: bytes
    previous_key: bytes | None = None
    service_id: str = ""
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if len(self.key) != KEY_BYTES:
            raise CookieError(f"key must be {KEY_BYTES} bytes (AES-256), got {len(self.key)}")
        if self.previous_key is not None and len(self.previous_key) != KEY_BYTES:
            raise CookieError(f"previous_key must be {KEY_BYTES} bytes (AES-256), got {len(self.previous_key)}")
        if not self.service_id:
            raise CookieError("service_id is required (AAD binding)")
        self._aad = _aad(self.service_id, self.schema_version)
        self._aead = AESGCM(self.key)
        self._aead_prev = AESGCM(self.previous_key) if self.previous_key else None

    def encode(self, state: SessionState, *, nonce: bytes | None = None) -> str:
        """Encrypt and base64url-encode a state. ``nonce`` is for tests only;
        production calls let it default to a fresh random 96-bit nonce."""
        if state.v != self.schema_version:
            raise CookieError(f"state schema version {state.v} != codec schema version {self.schema_version}")
        if nonce is None:
            nonce = os.urandom(NONCE_BYTES)
        elif len(nonce) != NONCE_BYTES:
            raise CookieError(f"nonce must be {NONCE_BYTES} bytes, got {len(nonce)}")

        plaintext = _pack_payload(state)
        ciphertext = self._aead.encrypt(nonce, plaintext, self._aad)
        return _b64url_encode(nonce + ciphertext)

    def decode(self, cookie_value: str) -> SessionState:
        """Decrypt, verify, and unpack. Raises ``CookieError`` on any failure
        (bad base64, wrong length, tampered ciphertext, wrong key, wrong
        service id, wrong schema version)."""
        try:
            raw = _b64url_decode(cookie_value)
        except Exception as e:
            raise CookieError(f"base64url decode failed: {e}") from e

        if len(raw) < NONCE_BYTES + _V1_PACK_SIZE + 16:  # 16-byte GCM tag
            raise CookieError(f"cookie too short: {len(raw)} bytes")

        nonce, ciphertext = raw[:NONCE_BYTES], raw[NONCE_BYTES:]

        plaintext: bytes | None = None
        last_err: Exception | None = None
        for aead in (self._aead, self._aead_prev):
            if aead is None:
                continue
            try:
                plaintext = aead.decrypt(nonce, ciphertext, self._aad)
                break
            except InvalidTag as e:
                last_err = e
                continue
        if plaintext is None:
            raise CookieError(f"AEAD verification failed: {last_err}") from last_err

        state = _unpack_payload(plaintext)
        # Accept v1 cookies during the migration window — they carry no
        # prev_route_path, so L2 falls back to uniform prior for that one
        # request but the request still serves. The decoder is the only
        # place we accept old schemas; the encoder always emits the
        # current SCHEMA_VERSION.
        if state.v != self.schema_version and state.v != 1:
            raise CookieError(f"payload schema version {state.v} != codec schema version {self.schema_version}")
        return state
