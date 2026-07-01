"""Server-side ``(range_token, anchor) -> (start, end)`` window resolver.

The relative-range wire contract (``pending-docs/wire-token-relative-range-
anchor-spec.md``): time-windowed analytics requests may carry an optional
``range_token`` (a relative selection like ``"24h"`` / ``"7d"`` / ``"30d"`` /
``"auto"``) plus a ``anchor`` (a single reference instant). When present, the
server resolves them DETERMINISTICALLY into absolute ``(start, end)`` bounds —
those bounds then drive the analyst clamp + the scan, and the ``(range_token,
quantized_anchor)`` pair is what stabilizes the response-memo cache key.

Why this exists (the network 30d analyst cliff): the network response memo is
anchor-faithful — it keys on the rolling minute-bucketed RESOLVED bounds, so an
analyst loading across rolling minutes gets a fresh key every minute and
recomputes the full ~26s 30d pipeline. A relative token + a quantized anchor is
server-reproducible and STABLE within the anchor quantum, so the key holds
still long enough for the memo to actually serve.

SECURITY (analyst-adversary boundary — see the spec + ``_response_cache_key``):
  * This module ONLY computes a window from a token + anchor. It does NOT read
    the analyst clamp and does NOT widen anything — the caller still runs the
    resolved bounds through ``ctx.clamp`` (``TimeBounds.clamp``) so the invite
    ceiling is enforced regardless of which token was supplied. An analyst can
    never widen past their invite by choosing ``"30d"``.
  * The anchor is QUANTIZED (floored to the quantum, default 60s) so a crafted
    sub-minute anchor can't mint unbounded distinct cache entries, and so the
    SSR seed + client first-paint land on the same key.
  * ``invite_clamp_fingerprint`` partitions the cache key by the analyst's
    own invite-clamp shape so an open invite, a date-restricted invite, and
    admin (None) never share a token+anchor entry.

This is the Python port of the frontend ``lib/insights-defaults.ts`` adaptive
default + the ``filterStore.relativeRange`` token vocabulary, kept here so the
backend can resolve the SAME default the frontend would pick for ``"auto"``.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

from backend.utils.date_utils import iso_z, parse_iso_utc

# Anchor quantization granularity. 60s aligns with the 30s response-memo TTL —
# an anchor that's stable for 60s keeps a memo entry reachable across the
# rolling-minute reloads that were causing the cliff. The SSR seed and the
# client first-paint both floor to this same quantum so their keys byte-match.
ANCHOR_QUANTUM_SECONDS = 60

# Fixed relative tokens → lookback delta. Mirrors filterStore.relativeRange
# quick-presets; extend here (and the FE) in lockstep if a new preset is added.
# ``"auto"`` is resolved adaptively from the service's log extents (see
# ``_pick_auto_token``) — it is NOT in this map because it has no fixed delta.
_FIXED_TOKEN_DELTAS: dict[str, timedelta] = {
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}

# The full accepted token vocabulary (fixed deltas + the adaptive sentinel).
VALID_RANGE_TOKENS: frozenset[str] = frozenset(_FIXED_TOKEN_DELTAS) | {"auto"}


def is_valid_range_token(token: str | None) -> bool:
    """True when ``token`` is a recognized relative-range token.

    A None / empty / unknown token means "no keyed path" — the caller falls
    back to the existing anchor-faithful absolute-bounds branch. We deliberately
    do NOT raise on an unknown token: an additive wire field must degrade to the
    legacy path, never 400 a request the old client could make.
    """
    return token in VALID_RANGE_TOKENS


def quantize_anchor(anchor: str | None, *, now: datetime | None = None) -> str:
    """Floor an anchor instant to the ``ANCHOR_QUANTUM_SECONDS`` grid → ISO-Z.

    A missing / unparseable anchor falls back to the server's wall-clock ``now``
    (so the keyed path is reachable even if the client omits the anchor). The
    floor makes the value stable within the quantum: two requests within the
    same 60s window produce the identical quantized anchor (and thus the
    identical cache key), which is exactly what lets the memo serve across the
    rolling-minute reloads.
    """
    dt = parse_iso_utc(anchor) if anchor else None
    if dt is None:
        dt = now if now is not None else datetime.now(UTC)
    epoch = int(dt.timestamp())
    floored = epoch - (epoch % ANCHOR_QUANTUM_SECONDS)
    return iso_z(datetime.fromtimestamp(floored, tz=UTC))


def _history_hours_from_extents(earliest_log_at: str | None, *, now: datetime) -> float | None:
    """Hours of history available = ``now - earliest_log_at``.

    Port of ``historyHoursFromExtents`` in ``frontend/lib/insights-defaults.ts``:
    a date-only extent (``"2026-06-15"``) is widened to UTC start-of-day before
    parsing so a non-UTC machine doesn't mis-bucket the span. Returns ``None``
    for a missing / unparseable extent (→ the static ``"auto"`` default).
    """
    if not earliest_log_at:
        return None
    dt = parse_iso_utc(earliest_log_at)
    if dt is None:
        return None
    return max(0.0, (now - dt).total_seconds() / 3600.0)


def _pick_auto_token(earliest_log_at: str | None, *, now: datetime) -> str:
    """Resolve ``"auto"`` to a concrete fixed token from the service's history.

    Mirrors the SPIRIT of ``pickInsightsDefault`` (adaptive default sized to how
    much history a service actually has) projected onto the network range
    vocabulary: a brand-new service shouldn't default to a 30d window that's
    almost entirely empty (and pays the full 30d scan for nothing), and a
    mature service shouldn't default to 24h and hide the trend. Half-open bands,
    higher bucket on the boundary:

        <7d  history  → "24h"   (new service: small, fast window)
        <30d history  → "7d"    (a week or two of data: week view)
        >=30d history → "30d"   (mature service: full month)

    Falls back to ``"7d"`` when there's no usable extent — a safe middle default
    that matches the frontend's "we don't know yet" behavior without paying the
    30d cost on a cold/unknown service.
    """
    history_hours = _history_hours_from_extents(earliest_log_at, now=now)
    if history_hours is None:
        return "7d"
    if history_hours < 7 * 24:
        return "24h"
    if history_hours < 30 * 24:
        return "7d"
    return "30d"


def resolve_window(
    range_token: str | None,
    anchor: str | None,
    *,
    earliest_log_at: str | None = None,
    now: datetime | None = None,
) -> tuple[str, str]:
    """Resolve ``(range_token, anchor) -> (start_iso, end_iso)`` deterministically.

    Fixed tokens (``"24h"`` / ``"7d"`` / ``"30d"``) → ``[anchor - delta, anchor]``.
    ``"auto"`` → the adaptive default from ``earliest_log_at`` (see
    ``_pick_auto_token``), then the same ``[anchor - delta, anchor]`` shape.

    The anchor used for the math is the QUANTIZED anchor (floored to the
    quantum) so the resolved bounds are stable within the quantum — the same
    property that stabilizes the cache key. Both returned strings are ISO-Z.

    Raises ``ValueError`` for an unrecognized token so callers that have already
    gated on ``is_valid_range_token`` get a clear contract violation rather than
    a silent wrong window. (The router gates first, so this only fires on a
    programming error.)

    SECURITY: the returned bounds are the SCAN intent BEFORE the analyst clamp.
    The caller MUST still pass them through ``ctx.clamp`` — this function never
    sees the invite ceiling and must not be trusted to enforce it.
    """
    if not is_valid_range_token(range_token):
        raise ValueError(f"unrecognized range_token: {range_token!r}")

    now_dt = now if now is not None else datetime.now(UTC)
    quantized = quantize_anchor(anchor, now=now_dt)
    anchor_dt = parse_iso_utc(quantized)
    assert anchor_dt is not None  # quantize_anchor always returns a parseable ISO-Z

    # ``range_token`` is non-None here (is_valid_range_token gated above), and
    # ``_pick_auto_token`` always returns a fixed token, so ``token`` is a known
    # key of _FIXED_TOKEN_DELTAS by this point.
    if range_token == "auto":
        token = _pick_auto_token(earliest_log_at, now=now_dt)
    else:
        assert range_token is not None  # narrowed by is_valid_range_token above
        token = range_token

    delta = _FIXED_TOKEN_DELTAS[token]
    start_dt = anchor_dt - delta
    return iso_z(start_dt), iso_z(anchor_dt)


def invite_clamp_fingerprint(analyst_session: object | None) -> str | None:
    """Stable cache-key fragment identifying the analyst's invite-clamp shape.

    ``None`` for admin (no analyst session) — admin entries never share with
    any analyst entry. For an analyst, an 8-hex digest of the invite's window
    PARAMETERS (``query_start_time``, ``query_end_time``, ``query_window_hours``)
    so an open invite, a date-restricted invite, and a windowed invite each get
    a distinct fragment.

    SECURITY (the spec's central point): we MUST partition the cache key by this
    fingerprint. The resolved bounds are clamped to the invite ceiling before
    the scan, so two invites with DIFFERENT ceilings scanning the SAME token
    would otherwise alias onto one key and one could serve rows the other's
    ceiling forbids. Keying on the invite-clamp shape keeps each ceiling's
    results in its own partition. We deliberately do NOT key on the resolved
    clamp BOUNDS (which roll with ``now``) — that's the cliff we're fixing; the
    invite PARAMETERS are static per invite, which is what makes the key stable.

    The digest folds the same three params as
    ``remote_access.analyst_clamp_cache_key`` but is hashed (not the raw "a|b|c"
    string) so the fragment is fixed-width and carries no invite timestamps into
    the key bytes.
    """
    if analyst_session is None:
        return None
    qs = getattr(analyst_session, "query_start_time", None)
    qe = getattr(analyst_session, "query_end_time", None)
    qw = getattr(analyst_session, "query_window_hours", None)
    raw = f"{qs or ''}|{qe or ''}|{qw or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
