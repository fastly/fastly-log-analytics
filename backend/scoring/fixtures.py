"""Convert real prod log rows from DuckDB into sessionized JSONL traces.

The output is the canonical input format for both the training pipeline
(matrix builder, PageRank) and the scorer test fixtures. One JSONL line per
session, ordered by start time.

Session boundary heuristic (pre-cookie deployment): group rows by
(client_ip, user_agent), then split into separate sessions whenever the gap
between consecutive events exceeds ``SESSION_GAP_SECONDS`` (default 30 min,
industry standard). Once the AES-GCM session cookie is deployed at the edge,
this fallback will be replaced by SID-based grouping.

Output schema (one JSONL line per session):

    {
      "session_id": "ip_<sha1-prefix>",      # synthetic until cookie ships
      "client_ip": "1.2.3.4",
      "user_agent": "...",
      "start_ts": "2026-05-15T23:30:00+00:00",
      "end_ts":   "2026-05-15T23:35:12+00:00",
      "event_count": 7,
      "events": [
        {"ts": "...", "url": "/", "method": "GET", "status": 200,
         "referer": "", "ttfb_ms": 50, "country": "US", "asn": 7922}
      ]
    }
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import IO, Any

logger = logging.getLogger(__name__)

# Industry-standard 30-minute inactivity boundary. Tuned later from production
# session-gap distributions; for now matches GA / Adobe conventions so the
# trained model approximates "what a normal session looks like" in the
# analytics sense.
SESSION_GAP_SECONDS = 30 * 60

# Columns the scorer cares about. Kept short on purpose — every extra column
# is bytes-per-event in the JSONL output and we extract millions of events.
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
    """A single request as seen at the edge. Field names match the JSONL
    schema exactly so dataclasses.asdict round-trips cleanly."""

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
    """Stable 12-hex-char session id from the (ip, ua, start_ts) tuple.

    Start-time-anchored so that re-running extraction on the same data
    produces the same ids — useful for reproducible test fixtures."""
    h = hashlib.sha1(f"{client_ip}|{user_agent}|{start_ts}".encode()).hexdigest()
    return f"ip_{h[:12]}"


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    # DuckDB returns timestamps as datetime when fetched via Python API; this
    # branch is the safety net for cases where they come back as strings
    # (e.g. when stitched via CSV).
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _ts_iso(value: Any) -> str:
    """ISO-8601 with second precision, UTC-suffixed. Matches what the scorer
    consumes; truncating sub-second avoids burning bytes on JSON timestamps
    we wouldn't use anyway."""
    dt = _parse_ts(value)
    return dt.isoformat(timespec="seconds")


def rows_to_events(rows: Iterable[tuple[Any, ...]]) -> Iterator[tuple[str, str, Event]]:
    """Convert raw DuckDB row tuples (in ``_TRACE_COLUMNS`` order) into
    ``(client_ip, user_agent, Event)`` triples. Used as the sessionizer's
    input stream. Generators throughout so we never materialize the full
    1.8M-row set in memory."""
    for row in rows:
        ts, ip, ua, url, method, status, referer, ttfb, country, asn = row
        yield (
            ip or "",
            ua or "",
            Event(
                ts=_ts_iso(ts),
                url=url or "",
                method=(method or "").upper(),
                status=int(status) if status is not None else 0,
                referer=referer or "",
                # ttfb is stored in seconds in the source schema; the scorer
                # works in ms. Round to 3 decimals — sub-ms precision is noise
                # at this layer.
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
    """Group ``(ip, ua, Event)`` triples into sessions.

    REQUIRES the input to be sorted by ``(ip, ua, ts)`` ascending — this is
    enforced at the SQL layer with ``ORDER BY ip, ua, timestamp`` so we can
    sessionize in a single streaming pass without buffering. Caller is
    responsible for the sort.

    Within an (ip, ua) bucket, starts a new session whenever the gap from
    the previous event exceeds ``gap_seconds``.
    """
    current: Session | None = None
    last_ts: datetime | None = None
    threshold = timedelta(seconds=gap_seconds)

    for ip, ua, ev in events:
        ts = _parse_ts(ev.ts)

        # New session iff we crossed an (ip, ua) boundary or exceeded the gap.
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

        assert current is not None
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
    """Stream sessions from the per-service DuckDB logs view.

    ``con`` is a DuckDB connection (the per-service one returned by
    ``backend.deps.get_con``). ``service_id`` is used to resolve the view
    name (``logs_<lower(service_id)>``).
    """
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
    rows = con.execute(sql).fetchall()  # DuckDB streams internally; we get a list back
    logger.info("[scoring.fixtures] fetched %d rows, sessionizing …", len(rows))

    yield from sessionize(rows_to_events(rows), gap_seconds=gap_seconds)


def write_jsonl(sessions: Iterable[Session], out: IO[str]) -> int:
    """Write sessions as JSONL to an open text file handle. Returns count.

    The text mode wrapping (vs. binary + manual encode) is deliberate — the
    output is canonical JSON UTF-8, the file is typically small enough that
    OS buffering is fine, and text mode keeps line termination portable.
    """
    n = 0
    for s in sessions:
        out.write(json.dumps(s.to_jsonl_dict(), separators=(",", ":")) + "\n")
        n += 1
    return n


def write_jsonl_path(sessions: Iterable[Session], path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        return write_jsonl(sessions, f)
