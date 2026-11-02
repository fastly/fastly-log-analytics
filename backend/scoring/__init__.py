"""URL → canonical route ID normalization, Layer 2 (route transition) scoring,
admin session labels CRUD, and DuckDB log-to-sessionized-JSONL trace fixtures.

Per research doc §5.3: collapse high-cardinality path segments (numeric ids,
UUIDs, slugs that look like keys) into ``*`` placeholders so the transition
matrix doesn't blow up to one row per unique URL. Query strings are
discarded entirely (the scorer keys on path topology, not query
parameters).

The Rust port under ``compute/scorer/`` must produce identical output for
the same URL — these are the lookup keys for the matrix.

**ASCII-only cross-language parity contract (EC-02).** The train/score parity
guarantee holds for the ASCII subset only. This module lowercases with
``str.lower()`` and collapses ids with ``\\d`` (both full-Unicode), while the
Rust scorer uses ``to_ascii_lowercase`` / ``is_ascii_digit``; on NON-ASCII input
the two therefore diverge (``/CAF%C3%89`` → ``/café`` here vs ``/cafÉ`` at the
edge; Arabic-Indic digits collapse here but not there). This is a deliberate,
documented limit — the worst case is the edge treating an i18n route as novel
(an L2 accuracy loss, never a category-evasion bypass), and porting Unicode
folding into the Wasm would pull in unicode crates. The divergence classes are
pinned by ``tests/scoring/test_scorer_parity_and_vectors.py``; raw C0 control
chars (``\\t \\n \\r``) are NOT divergent — ``urlsplit`` strips them here and the
Rust ``normalize`` strips them too.
"""

from __future__ import annotations

import hashlib
import logging
import math
import posixpath
import re
import time
import uuid
from collections.abc import Iterable, Iterator
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal
from urllib.parse import unquote, urlsplit

from backend.core.metadata import get_con
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry

logger = logging.getLogger(__name__)

# ── URL Normalization ─────────────────────────────────────────────────────────

_NUMERIC_ID = re.compile(r"^\d+$")
_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_HEX_HASH = re.compile(r"^[0-9a-fA-F]{24,}$")
_PREFIXED_ID = re.compile(r"^[A-Z]{2,5}[-_][A-Za-z0-9_-]+$")
_LONG_OPAQUE = re.compile(r"^[A-Za-z0-9_-]{20,}$")

_ID_PATTERNS = (_NUMERIC_ID, _UUID, _HEX_HASH, _PREFIXED_ID, _LONG_OPAQUE)

_CATEGORY_MAP: Final[dict[str, str]] = {
    "": "home",
    "api": "api",
    "graphql": "api",
    "products": "product",
    "product": "product",
    "items": "product",
    "p": "product",
    "categories": "browse",
    "category": "browse",
    "search": "browse",
    "browse": "browse",
    "cart": "cart",
    "basket": "cart",
    "checkout": "checkout",
    "pay": "checkout",
    "order": "checkout",
    "orders": "checkout",
    "account": "account",
    "user": "account",
    "users": "account",
    "profile": "account",
    "settings": "account",
    "auth": "auth",
    "login": "auth",
    "signin": "auth",
    "signup": "auth",
    "register": "auth",
    "logout": "auth",
    "admin": "admin",
    "static": "asset",
    "assets": "asset",
    "blog": "content",
    "news": "content",
    "about": "content",
    "help": "content",
    "support": "content",
    "privacy": "content",
    "terms": "content",
    "faq": "content",
}


@dataclass(frozen=True)
class Route:
    """Canonical normalized route plus its category tag.

    ``path`` is the lookup key for the transition matrix.
    ``category`` is the L2 category-backoff key (§4.2)."""

    path: str
    category: str


def _strip_query(url: str) -> str:
    """Return just the path component of a URL. Handles both relative
    (``/foo/bar?x=1``) and absolute (``https://h/foo/bar?x=1``) inputs."""
    while url.startswith("//"):
        url = url[1:]
    try:
        parts = urlsplit(url)
        return parts.path or "/"
    except ValueError:
        return "/"


def _looks_like_id(segment: str) -> bool:
    if not segment:
        return False
    return any(pat.fullmatch(segment) for pat in _ID_PATTERNS)


def _category_for(first_segment: str) -> str:
    return _CATEGORY_MAP.get(first_segment.lower(), "other")


_SLASH_SPLIT_RE = re.compile(r"(%2f|%2F)")


def unquote_except_slash(s: str) -> str:
    """Decode all percent-encoded sequences in the string EXCEPT for encoded slashes
    (%2f / %2F). This ensures that encoded directory traversals (like %2e%2e)
    can be resolved by normpath, while encoded slashes are preserved as data."""
    parts = _SLASH_SPLIT_RE.split(s)
    return "".join(unquote(p) if i % 2 == 0 else p for i, p in enumerate(parts))


def _unquote_until_stable(s: str, max_iter: int = 4) -> str:
    """Iteratively :func:`unquote_except_slash` until the string stops changing."""
    for _ in range(max_iter):
        decoded = unquote_except_slash(s)
        if decoded == s:
            return decoded
        s = decoded
    return s


def normalize(url: str) -> Route:
    """Convert a raw URL into a canonical (route, category) pair."""
    path = posixpath.normpath(_unquote_until_stable(_strip_query(url)))
    if path in ("", "/"):
        return Route(path="/", category="home")

    raw_segments = [unquote(s) for s in path.split("/") if s != ""]
    if not raw_segments:
        return Route(path="/", category="home")

    normalized = ["*" if _looks_like_id(seg) else seg.lower() for seg in raw_segments]
    canonical = "/" + "/".join(normalized)
    category = _category_for(raw_segments[0].lower())
    return Route(path=canonical, category=category)


# ── Layer 2 tuning constants & Route Transition Shield (research doc §4.2) ────

L2_LAPLACE_ALPHA: Final[float] = 0.5
L2_SKIPGRAM_BETA: Final[float] = 0.7


def _l2_score_from_trans_prob(p: float) -> int:
    """Map a TransScore probability to a Layer 2 anomaly score [0, 100]."""
    if p >= 1.0:
        return 0
    if p <= 1e-12:
        return 100
    raw = -math.log10(p) * (100.0 / 6.0)
    return max(0, min(100, int(round(raw))))


def _transition_prob(matrix: dict, prev_route: str, current_route: str, vocab_size: int) -> float:
    """Laplace-smoothed P(current | prev) lookup from a serialized matrix."""
    counts = matrix.get("counts", {})
    row_totals = matrix.get("row_totals", {})
    if row_totals and row_totals.get(prev_route, 0) <= 0:
        return 1e-12
    prev_row = counts.get(prev_route, {})
    numerator = prev_row.get(current_route, 0) + L2_LAPLACE_ALPHA
    denominator = row_totals.get(prev_route, 0) + L2_LAPLACE_ALPHA * vocab_size
    if denominator <= 0:
        return 1.0 / max(vocab_size, 1)
    return numerator / denominator


def score_layer2(
    matrix: dict | None,
    prev_route: Route | None,
    prev_anchor_route: Route | None,
    current_route: Route,
) -> tuple[int, list[str], float]:
    """Apply the L2 rules. Returns (score_contribution, reasons, trans_prob)."""
    if matrix is None or prev_route is None:
        return 0, [], 1.0

    vocab_size = int(matrix.get("vocab_size", 0))
    if vocab_size <= 0:
        return 0, [], 1.0

    direct_p = _transition_prob(matrix, prev_route.path, current_route.path, vocab_size)
    if (
        prev_anchor_route is not None
        and prev_anchor_route.path != prev_route.path
        and prev_anchor_route.path in matrix.get("counts", {})
    ):
        anchor_p = _transition_prob(matrix, prev_anchor_route.path, current_route.path, vocab_size) * L2_SKIPGRAM_BETA
        trans_prob = max(direct_p, anchor_p)
    else:
        trans_prob = direct_p

    score = _l2_score_from_trans_prob(trans_prob)
    reasons = ["low-transition-prob"] if score >= 50 else []
    return score, reasons, trans_prob


# ── Admin Session Labels (folded from backend.scoring.labels) ─────────────────

Label = Literal["good", "bad", "neutral"]
ALLOWED_LABELS: frozenset[str] = frozenset({"good", "bad", "neutral"})

_LABEL_COUNTS_TTL_S = 30.0
_LABEL_COUNTS_CACHE: BoundedTTLCache[str, tuple[float, dict[str, int]]] = BoundedTTLCache(
    maxsize=128, ttl_seconds=_LABEL_COUNTS_TTL_S
)
CacheRegistry.register("scoring.labels._LABEL_COUNTS_CACHE", _LABEL_COUNTS_CACHE)

_REQUEST_LABELS_CACHE: ContextVar[dict | None] = ContextVar("_REQUEST_LABELS_CACHE", default=None)


def init_request_cache() -> None:
    """Open a per-request memoization scope; called from middleware on entry."""
    _REQUEST_LABELS_CACHE.set({})


def clear_request_cache() -> None:
    """Close the per-request memoization scope; called from middleware on exit."""
    _REQUEST_LABELS_CACHE.set(None)


def _bust_cache(service_id: str) -> None:
    """Drop cached entries for this service so a same-request read after write returns new state."""
    _LABEL_COUNTS_CACHE.pop(service_id, None)
    cache = _REQUEST_LABELS_CACHE.get()
    if cache is not None:
        for k in [k for k in cache if isinstance(k, tuple) and len(k) >= 2 and k[1] == service_id]:
            cache.pop(k, None)


def _row_to_dict(r) -> dict:
    return {
        "id": r["id"],
        "service_id": r["service_id"],
        "sid": r["sid"],
        "label": r["label"],
        "notes": r["notes"] or "",
        "flagged_by": r["flagged_by"] or "",
        "sample_ip": r["sample_ip"] or "",
        "sample_ua": r["sample_ua"] or "",
        "sample_url": r["sample_url"] or "",
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


def save_label(
    service_id: str,
    sid: str,
    label: str,
    *,
    notes: str = "",
    flagged_by: str = "admin",
    sample_ip: str = "",
    sample_ua: str = "",
    sample_url: str = "",
) -> dict:
    """Upsert a label keyed on (service_id, sid)."""
    if label not in ALLOWED_LABELS:
        raise ValueError(f"label must be one of {sorted(ALLOWED_LABELS)}, got {label!r}")
    if not sid:
        raise ValueError("sid is required")

    con = get_con(service_id)
    new_id = str(uuid.uuid4())
    con.execute(
        """
        INSERT INTO scoring_labels (
            id, service_id, sid, label, notes, flagged_by,
            sample_ip, sample_ua, sample_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(service_id, sid) DO UPDATE SET
            label = excluded.label,
            notes = excluded.notes,
            flagged_by = excluded.flagged_by,
            sample_ip = excluded.sample_ip,
            sample_ua = excluded.sample_ua,
            sample_url = excluded.sample_url,
            updated_at = datetime('now')
        """,
        (new_id, service_id, sid, label, notes, flagged_by, sample_ip, sample_ua, sample_url),
    )
    con.commit()
    _bust_cache(service_id)
    row = con.execute(
        "SELECT * FROM scoring_labels WHERE service_id = ? AND sid = ?",
        (service_id, sid),
    ).fetchone()
    return _row_to_dict(row) if row else {"id": new_id, "service_id": service_id, "sid": sid, "label": label}


def list_labels(service_id: str, limit: int = 500) -> list[dict]:
    """Most-recent first. Limit is a safety cap."""
    cache = _REQUEST_LABELS_CACHE.get()
    if cache is not None:
        cached = cache.get(("list", service_id, int(limit)))
        if cached is not None:
            return cached

    con = get_con(service_id)
    rows = con.execute(
        "SELECT * FROM scoring_labels WHERE service_id = ? ORDER BY updated_at DESC, id DESC LIMIT ?",
        (service_id, int(limit)),
    ).fetchall()
    result = [_row_to_dict(r) for r in rows]
    if cache is not None:
        cache[("list", service_id, int(limit))] = result
    return result


def get_label_by_id(service_id: str, label_id: str) -> dict | None:
    con = get_con(service_id)
    row = con.execute(
        "SELECT * FROM scoring_labels WHERE id = ? AND service_id = ?",
        (label_id, service_id),
    ).fetchone()
    return _row_to_dict(row) if row else None


def update_label(
    service_id: str,
    label_id: str,
    *,
    label: str | None = None,
    notes: str | None = None,
) -> dict:
    """PATCH semantics — only update the fields that were passed."""
    con = get_con(service_id)
    sets: list[str] = []
    params: list = []
    if label is not None:
        if label not in ALLOWED_LABELS:
            raise ValueError(f"label must be one of {sorted(ALLOWED_LABELS)}, got {label!r}")
        sets.append("label = ?")
        params.append(label)
    if notes is not None:
        sets.append("notes = ?")
        params.append(notes)
    if not sets:
        return get_label_by_id(service_id, label_id) or {}
    sets.append("updated_at = datetime('now')")
    params.extend([label_id, service_id])
    con.execute(f"UPDATE scoring_labels SET {', '.join(sets)} WHERE id = ? AND service_id = ?", params)
    con.commit()
    _bust_cache(service_id)
    return get_label_by_id(service_id, label_id) or {}


def delete_label(service_id: str, label_id: str) -> dict:
    """Hard delete. Idempotent."""
    con = get_con(service_id)
    con.execute("DELETE FROM scoring_labels WHERE id = ? AND service_id = ?", (label_id, service_id))
    con.commit()
    _bust_cache(service_id)
    return {"status": "success", "id": label_id}


def counts_by_label(service_id: str) -> dict[str, int]:
    """{label: count}. Includes 'good', 'bad', 'neutral' keys with 0 for missing."""
    cache = _REQUEST_LABELS_CACHE.get()
    if cache is not None:
        cached = cache.get(("counts", service_id))
        if cached is not None:
            return dict(cached)

    now = time.monotonic()
    ttl_entry = _LABEL_COUNTS_CACHE.get(service_id)
    if ttl_entry is not None and (now - ttl_entry[0]) < _LABEL_COUNTS_TTL_S:
        out_copy = dict(ttl_entry[1])
        if cache is not None:
            cache[("counts", service_id)] = out_copy
        return dict(out_copy)

    con = get_con(service_id)
    rows = con.execute(
        "SELECT label, COUNT(*) AS n FROM scoring_labels WHERE service_id = ? GROUP BY label",
        (service_id,),
    ).fetchall()
    out = {"good": 0, "bad": 0, "neutral": 0}
    for r in rows:
        out[r["label"]] = int(r["n"])
    _LABEL_COUNTS_CACHE[service_id] = (now, dict(out))
    if cache is not None:
        cache[("counts", service_id)] = out
    return out


# ── Sessionized JSONL Trace Fixtures (folded from backend.scoring.fixtures) ───

SESSION_GAP_SECONDS = 30 * 60
_TRACE_COLUMNS = (
    "timestamp",
    "ip",
    "ua",
    "url",
    "method",
    "status",
    "referer",
    "ttfb",
    "country",
    "asn",
)


@dataclass
class Event:
    """A single request as seen at the edge."""

    ts: str
    url: str
    method: str
    status: int
    referer: str
    ttfb_ms: float
    country: str
    asn: int | None


@dataclass
class Session:
    session_id: str
    client_ip: str
    user_agent: str
    events: list[Event] = field(default_factory=list)

    @property
    def start_ts(self) -> str:
        return self.events[0].ts if self.events else ""

    @property
    def end_ts(self) -> str:
        return self.events[-1].ts if self.events else ""

    @property
    def event_count(self) -> int:
        return len(self.events)

    def to_jsonl_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "client_ip": self.client_ip,
            "user_agent": self.user_agent,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "event_count": self.event_count,
            "events": [
                {
                    "ts": e.ts,
                    "url": e.url,
                    "method": e.method,
                    "status": e.status,
                    "referer": e.referer,
                    "ttfb_ms": e.ttfb_ms,
                    "country": e.country,
                    "asn": e.asn,
                }
                for e in self.events
            ],
        }


def _synth_session_id(client_ip: str, user_agent: str, start_ts: str) -> str:
    """Stable 12-hex-char session id from the (ip, ua, start_ts) tuple."""
    h = hashlib.sha1(f"{client_ip}|{user_agent}|{start_ts}".encode(), usedforsecurity=False).hexdigest()
    return f"ip_{h[:12]}"


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _ts_iso(value: Any) -> str:
    """ISO-8601 with second precision, UTC-suffixed."""
    return _parse_ts(value).isoformat(timespec="seconds")


def rows_to_events(rows: Iterable[tuple[Any, ...]]) -> Iterator[tuple[str, str, Event]]:
    """Convert raw DuckDB row tuples (in ``_TRACE_COLUMNS`` order) into ``(client_ip, user_agent, Event)`` triples."""
    for ts, ip, ua, url, method, status, referer, ttfb, country, asn in rows:
        yield (
            ip or "",
            ua or "",
            Event(
                ts=_ts_iso(ts),
                url=url or "",
                method=(method or "").upper(),
                status=int(status) if status is not None else 0,
                referer=referer or "",
                ttfb_ms=round(float(ttfb) * 1000.0, 3) if ttfb is not None else 0.0,
                country=country or "",
                asn=int(asn) if asn is not None else None,
            ),
        )


def sessionize(
    events: Iterable[tuple[str, str, Event]],
    *,
    gap_seconds: int = SESSION_GAP_SECONDS,
) -> Iterator[Session]:
    """Group ``(ip, ua, Event)`` triples into sessions."""
    current: Session | None = None
    last_ts: datetime | None = None
    threshold = timedelta(seconds=gap_seconds)

    for ip, ua, ev in events:
        ts = _parse_ts(ev.ts)
        new_session = (
            current is None
            or current.client_ip != ip
            or current.user_agent != ua
            or (last_ts is not None and ts - last_ts > threshold)
        )
        if new_session:
            if current is not None and current.events:
                yield current
            current = Session(
                session_id=_synth_session_id(ip, ua, ev.ts),
                client_ip=ip,
                user_agent=ua,
            )
        if current is None:
            raise RuntimeError("current is unexpectedly None")
        current.events.append(ev)
        last_ts = ts

    if current is not None and current.events:
        yield current


def extract_traces(
    con,
    *,
    service_id: str,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int | None = None,
    gap_seconds: int = SESSION_GAP_SECONDS,
) -> Iterator[Session]:
    """Stream sessions from the per-service DuckDB logs view."""
    view = f"logs_{service_id.lower()}"
    where_clauses = []
    if start is not None:
        where_clauses.append(f"timestamp >= TIMESTAMP '{start.isoformat()}'")
    if end is not None:
        where_clauses.append(f"timestamp < TIMESTAMP '{end.isoformat()}'")
    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    limit_sql = f"LIMIT {int(limit)}" if limit else ""

    sql = f"""
        SELECT {", ".join(_TRACE_COLUMNS)}
        FROM {view}
        {where_sql}
        ORDER BY ip, ua, timestamp
        {limit_sql}
    """

    logger.info("[scoring.fixtures] streaming events from %s", view)
    rows = con.execute(sql).fetchall()
    logger.info("[scoring.fixtures] fetched %d rows, sessionizing …", len(rows))

    yield from sessionize(rows_to_events(rows), gap_seconds=gap_seconds)


# ── Transition-Matrix Builder & FSM1 Encoding (folded from backend.scoring.matrix) ──

MIN_EVENTS_PER_SESSION = 2
MIN_MEAN_DWELL_S = 0.2


@dataclass
class MatrixStats:
    """Counters surfaced for the training CLI's summary log."""

    sessions_in: int = 0
    sessions_dropped_short: int = 0
    sessions_dropped_fast: int = 0
    sessions_kept: int = 0
    transitions: int = 0
    routes_seen: int = 0


@dataclass
class TransitionMatrix:
    counts: dict[str, dict[str, int]] = field(default_factory=dict)
    row_totals: dict[str, int] = field(default_factory=dict)
    categories: dict[str, str] = field(default_factory=dict)
    vocab: set[str] = field(default_factory=set)
    session_count: int = 0
    transition_count: int = 0
    anchors: list[str] = field(default_factory=list)

    def add_transition(self, prev: Route, curr: Route) -> None:
        self.counts.setdefault(prev.path, {})
        self.counts[prev.path][curr.path] = self.counts[prev.path].get(curr.path, 0) + 1
        self.row_totals[prev.path] = self.row_totals.get(prev.path, 0) + 1
        self.transition_count += 1
        if prev.path not in self.categories:
            self.categories[prev.path] = prev.category
        if curr.path not in self.categories:
            self.categories[curr.path] = curr.category
        self.vocab.add(prev.path)
        self.vocab.add(curr.path)

    def to_json_dict(self, version: str) -> dict[str, Any]:
        return {
            "version": version,
            "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "vocab_size": len(self.vocab),
            "session_count": self.session_count,
            "transition_count": self.transition_count,
            "counts": self.counts,
            "row_totals": self.row_totals,
            "categories": self.categories,
            "anchors": self.anchors,
        }


def _session_mean_dwell_seconds(events: list[dict]) -> float:
    """Mean of inter-event time deltas, in seconds."""
    if len(events) < 2:
        return 0.0
    first = datetime.fromisoformat(events[0]["ts"])
    last = datetime.fromisoformat(events[-1]["ts"])
    return (last - first).total_seconds() / (len(events) - 1)


def build_matrix(
    sessions: Iterable[dict],
    *,
    min_events: int = MIN_EVENTS_PER_SESSION,
    min_mean_dwell_s: float = MIN_MEAN_DWELL_S,
) -> tuple[TransitionMatrix, MatrixStats]:
    """Build a transition matrix from sessionized traces."""
    matrix = TransitionMatrix()
    stats = MatrixStats()

    for session in sessions:
        stats.sessions_in += 1
        events = session.get("events", [])

        if len(events) < min_events:
            stats.sessions_dropped_short += 1
            continue

        if _session_mean_dwell_seconds(events) < min_mean_dwell_s:
            stats.sessions_dropped_fast += 1
            continue

        prev_route: Route | None = None
        for ev in events:
            curr = normalize(ev.get("url", "/"))
            if prev_route is not None:
                matrix.add_transition(prev_route, curr)
            prev_route = curr
        stats.sessions_kept += 1
        matrix.session_count += 1

    stats.transitions = matrix.transition_count
    stats.routes_seen = len(matrix.vocab)
    return matrix, stats


FSM1_MAGIC = b"FSM1"
FSM1_VERSION = 1


def _uvarint(value: int) -> bytes:
    """LEB128 unsigned varint. Mirrors the Rust reader's `uvarint`."""
    if value < 0:
        raise ValueError(f"uvarint cannot encode negative value {value}")
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def serialize_kv(matrix: dict[str, Any]) -> bytes:
    """Encode a matrix JSON dict into the FSM1 binary KV payload."""
    version = str(matrix.get("version", ""))
    vocab_size = int(matrix.get("vocab_size", 0))
    counts: dict[str, dict[str, int]] = matrix.get("counts", {}) or {}
    row_totals: dict[str, int] = matrix.get("row_totals", {}) or {}

    routes: set[str] = set(counts.keys()) | set(row_totals.keys())
    for row in counts.values():
        routes.update(row.keys())
    sorted_routes = sorted(routes, key=lambda s: s.encode())
    rid = {r: i for i, r in enumerate(sorted_routes)}
    n_routes = len(sorted_routes)

    out = bytearray()
    out += FSM1_MAGIC
    out.append(FSM1_VERSION)
    out += vocab_size.to_bytes(4, "little")
    out += n_routes.to_bytes(4, "little")
    ver_bytes = version.encode()
    out += len(ver_bytes).to_bytes(2, "little")
    out += ver_bytes

    blob = bytearray()
    offsets = [0]
    for r in sorted_routes:
        blob += r.encode()
        offsets.append(len(blob))
    for off in offsets:
        out += off.to_bytes(4, "little")
    out += blob

    for r in sorted_routes:
        out += _uvarint(int(row_totals.get(r, 0)))

    row_off = [0]
    pair_bytes = bytearray()
    for r in sorted_routes:
        row = counts.get(r, {})
        cols = sorted((rid[c], int(cnt)) for c, cnt in row.items())
        for col_id, cnt in cols:
            pair_bytes += _uvarint(col_id)
            pair_bytes += _uvarint(cnt)
        row_off.append(row_off[-1] + len(cols))
    for off in row_off:
        out += _uvarint(off)
    out += pair_bytes

    return bytes(out)


def default_version() -> str:
    """Auto-assigned matrix version, ``YYYY-MM-DD-HHMMSS`` (UTC, second resolution)."""
    return datetime.now(UTC).strftime("%Y-%m-%d-%H%M%S")


# ── ROC-AUC Evaluation (folded from backend.scoring.evaluate) ─────────────────

DEFAULT_MIN_AUC: Final[float] = 0.85


@dataclass
class EvaluatedSession:
    session_id: str
    label: str  # "good" | "bad"
    l2_score: int  # 0-100 from the L2 scorer
    transition_count: int


@dataclass
class EvaluationResult:
    auc: float
    pass_threshold: float
    passed: bool
    n_good: int
    n_bad: int
    per_session: list[EvaluatedSession] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"AUC={self.auc:.3f} (threshold {self.pass_threshold:.2f}) — "
            f"{'PASS' if self.passed else 'FAIL'} "
            f"(n_good={self.n_good}, n_bad={self.n_bad})"
        )


def _session_l2_score(session: dict, matrix: dict) -> tuple[int, int]:
    """Compute the maximum L2 score across all transitions in a session."""
    events = session.get("events", [])
    if len(events) < 2:
        return 0, 0
    prev = normalize(events[0].get("url", "/"))
    max_score = 0
    n_trans = 0
    for ev in events[1:]:
        curr = normalize(ev.get("url", "/"))
        score, _, _ = score_layer2(matrix, prev, None, curr)
        max_score = max(max_score, score)
        prev = curr
        n_trans += 1
    return max_score, n_trans


def _compute_auc(scores: list[tuple[int, str]]) -> float:
    """Area under the ROC curve via the Mann-Whitney U formulation."""
    good_scores = [s for s, lbl in scores if lbl == "good"]
    bad_scores = [s for s, lbl in scores if lbl == "bad"]
    n_good = len(good_scores)
    n_bad = len(bad_scores)
    if n_good == 0 or n_bad == 0:
        return 0.5

    wins = 0.0
    for g in good_scores:
        for b in bad_scores:
            if g < b:
                wins += 1.0
            elif g == b:
                wins += 0.5
    return wins / (n_good * n_bad)


def _evaluate_sessions_with_extractor(
    labeled_sessions: Iterable[tuple[dict, str]],
    score_extractor: Any,
    *,
    min_auc: float = DEFAULT_MIN_AUC,
) -> EvaluationResult:
    per_session: list[EvaluatedSession] = []
    scores_for_auc: list[tuple[int, str]] = []
    n_good = n_bad = 0

    for session, label in labeled_sessions:
        if label not in ("good", "bad", "neutral"):
            raise ValueError(f"unexpected label {label!r}; want 'good', 'bad', or 'neutral'")
        extracted = score_extractor(session)
        if extracted is None:
            continue
        score, n_trans = extracted
        per_session.append(
            EvaluatedSession(
                session_id=session.get("session_id", "?"),
                label=label,
                l2_score=score,
                transition_count=n_trans,
            )
        )
        if label == "neutral":
            continue
        scores_for_auc.append((score, label))
        if label == "good":
            n_good += 1
        else:
            n_bad += 1

    auc = _compute_auc(scores_for_auc)
    return EvaluationResult(
        auc=auc,
        pass_threshold=min_auc,
        passed=auc >= min_auc,
        n_good=n_good,
        n_bad=n_bad,
        per_session=per_session,
    )


def evaluate(
    matrix: dict,
    labeled_sessions: Iterable[tuple[dict, str]],
    *,
    min_auc: float = DEFAULT_MIN_AUC,
) -> EvaluationResult:
    """Score every labeled session against ``matrix`` and report AUC."""
    return _evaluate_sessions_with_extractor(
        labeled_sessions,
        lambda session: _session_l2_score(session, matrix),
        min_auc=min_auc,
    )


def evaluate_from_persisted_scores(
    labeled_sessions: Iterable[tuple[dict, str]],
    *,
    min_auc: float = DEFAULT_MIN_AUC,
) -> EvaluationResult:
    """AUC using the score that was ALREADY persisted in DuckDB."""

    def _extract_persisted(session: dict) -> tuple[int, int] | None:
        max_score = session.get("max_edge_score")
        if max_score is None:
            return None
        n_events = len(session.get("events", []))
        return int(max_score), max(0, n_events - 1)

    return _evaluate_sessions_with_extractor(
        labeled_sessions,
        _extract_persisted,
        min_auc=min_auc,
    )


_KNOWN_REASON_ATOMS = (
    "cookie-missing",
    "cookie-tampered",
    "cookie-expired",
    "cookie-replayed",
    "impossibly-fast",
    "robotic-consistency",
    "low-transition-prob",
)


def evaluate_per_reason(
    labeled_sessions: Iterable[tuple[dict, str]],
    *,
    min_auc: float = DEFAULT_MIN_AUC,
    min_per_class: int = 3,
) -> dict:
    """AUC broken down by which L1/L2 rule fired in each session."""
    sessions_list = list(labeled_sessions)

    buckets: list[dict] = []
    for reason in _KNOWN_REASON_ATOMS:
        filtered: list[tuple[dict, str]] = []
        for session, label in sessions_list:
            if label not in ("good", "bad", "neutral"):
                continue
            events = session.get("events") or []
            if any(
                reason in {atom.strip() for atom in (ev.get("edge_score_reason") or "").split(",") if atom.strip()}
                for ev in events
            ):
                filtered.append((session, label))

        n_good = sum(1 for _, lbl in filtered if lbl == "good")
        n_bad = sum(1 for _, lbl in filtered if lbl == "bad")
        bucket: dict = {
            "reason": reason,
            "n_good": n_good,
            "n_bad": n_bad,
            "min_per_class": min_per_class,
            "has_min_samples": n_good >= min_per_class and n_bad >= min_per_class,
        }
        if bucket["has_min_samples"]:
            result = evaluate_from_persisted_scores(filtered, min_auc=min_auc)
            bucket["auc"] = round(float(result.auc), 4)
            bucket["passed"] = bool(result.passed)
            bucket["threshold"] = float(result.pass_threshold)
        buckets.append(bucket)
    return {"buckets": buckets, "min_per_class": min_per_class, "known_reasons": list(_KNOWN_REASON_ATOMS)}
