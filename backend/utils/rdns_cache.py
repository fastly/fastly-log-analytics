"""Reverse DNS cache with FCrDNS validation.

Stores IP→hostname mappings in a SQLite DB (WAL mode). A background enrichment
job populates the cache; query-time reads are non-blocking — unknown IPs return
'pending' immediately and are enqueued for the next enrichment run.

Thread safety
-------------
WAL mode allows concurrent readers, but SQLite serialises writers. All writes
go through _write_lock so only one thread writes at a time. Read-only calls
open an independent :memory:-free connection with check_same_thread=False and
uri=True mode (file:...?mode=ro) so they never block writers.
"""

from __future__ import annotations

import ipaddress
import logging
import socket
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

_DB_PATH = Path("data/cache/rdns_cache.db")
_write_lock = threading.Lock()
_last_enrichment_at: str | None = None


def _is_ip_in_cidrs(ip: str, cidrs: list[str]) -> bool:
    """Return True if the IP address falls within any of the provided CIDR ranges."""
    if not cidrs:
        return False
    try:
        ip_obj = ipaddress.ip_address(ip)
        for cidr in cidrs:
            try:
                if ip_obj in ipaddress.ip_network(cidr):
                    return True
            except ValueError:
                continue
    except Exception:
        pass
    return False


# ── Init ──────────────────────────────────────────────────────────────────────


def _write_con() -> sqlite3.Connection:
    """Open a write connection (creates DB + schema on first call)."""
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(_DB_PATH), check_same_thread=False, timeout=10)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=10000")
    con.execute("PRAGMA cache_size=-16000")  # 16MB — rDNS hit/miss lookups
    con.execute("""
        CREATE TABLE IF NOT EXISTS rdns (
            ip              TEXT PRIMARY KEY,
            hostname        TEXT,
            status          TEXT,
            fcrdns_verified INTEGER DEFAULT 0,
            looked_up_at    TEXT
        )
    """)
    con.commit()
    return con


def _read_con() -> sqlite3.Connection:
    """Open a read-only connection; does not create the DB."""
    if not _DB_PATH.exists():
        return None  # type: ignore[return-value]
    con = sqlite3.connect(
        f"file:{_DB_PATH}?mode=ro",
        uri=True,
        check_same_thread=False,
    )
    con.row_factory = sqlite3.Row
    return con


# Ensure schema exists at import time (fast, idempotent)
def _init() -> None:
    with _write_lock:
        con = _write_con()
        con.close()


_init()


# ── Public API ────────────────────────────────────────────────────────────────


def get_hostname(ip: str) -> tuple[str | None, str, bool]:
    """Return (hostname, status, fcrdns_verified) for an IP.

    If the IP is not in the cache, it is enqueued as 'pending' and
    (None, 'pending', False) is returned immediately.
    """
    con = _read_con()
    if con is None:
        enqueue([ip])
        return None, "pending", False
    try:
        row = con.execute("SELECT hostname, status, fcrdns_verified FROM rdns WHERE ip = ?", (ip,)).fetchone()
    finally:
        con.close()

    if row is None:
        enqueue([ip])
        return None, "pending", False
    return row["hostname"], row["status"], bool(row["fcrdns_verified"])


def get_hostnames(ips: list[str]) -> dict[str, tuple[str | None, str, bool]]:
    """Batched ``get_hostname`` for an arbitrary list of IPs.

    Returns ``{ip: (hostname, status, fcrdns_verified)}`` for every IP found in
    the cache. Missing IPs are batch-enqueued as 'pending' and omitted from the
    result so callers can treat them as ``(None, 'pending', False)`` via a
    single dict lookup.

    Avoids the per-IP connection-open/close + SELECT that a Python loop around
    ``get_hostname`` would cost — the security panel can fold thousands of rows
    into a single read against ``rdns``.
    """
    if not ips:
        return {}
    unique = list({ip for ip in ips if ip})
    if not unique:
        return {}
    con = _read_con()
    if con is None:
        enqueue(unique)
        return {}
    try:
        out: dict[str, tuple[str | None, str, bool]] = {}
        # SQLite's default SQLITE_LIMIT_VARIABLE_NUMBER is 999 — chunk to stay
        # well under it even with the few extra bind-spots we pass below.
        chunk_size = 900
        for i in range(0, len(unique), chunk_size):
            chunk = unique[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            cur = con.execute(
                f"SELECT ip, hostname, status, fcrdns_verified FROM rdns WHERE ip IN ({placeholders})",
                chunk,
            )
            for row in cur.fetchall():
                out[row["ip"]] = (row["hostname"], row["status"], bool(row["fcrdns_verified"]))
    finally:
        con.close()

    missing = [ip for ip in unique if ip not in out]
    if missing:
        enqueue(missing)
    return out


def enqueue(ips: list[str]) -> int:
    """Insert unseen IPs as 'pending'. Skip IPs already in the cache.

    Returns the number of newly inserted rows.
    """
    if not ips:
        return 0
    with _write_lock:
        con = _write_con()
        try:
            before = con.total_changes
            con.executemany(
                "INSERT OR IGNORE INTO rdns (ip, status, fcrdns_verified) VALUES (?, 'pending', 0)",
                [(ip,) for ip in ips],
            )
            con.commit()
            return con.total_changes - before
        finally:
            con.close()


def enrich_batch_gen(limit: int = 200):
    """Resolve pending IPs with FCrDNS validation, then discover new IPs from
    DuckDB sources. Yields progress events.
    """
    global _last_enrichment_at

    resolved = 0
    errors = 0
    discovered_count = 0

    # ── Pass 1: resolve pending IPs ──────────────────────────────────────────
    with _write_lock:
        con = _write_con()
        try:
            pending = con.execute("SELECT ip FROM rdns WHERE status = 'pending' LIMIT ?", (limit,)).fetchall()
        finally:
            con.close()

    if not pending:
        yield {"type": "status", "message": "No pending IPs to resolve."}
    else:
        yield {"type": "status", "message": f"Resolving {len(pending)} pending IPs..."}

    for (ip,) in pending:
        hostname, status, fcrdns = _do_lookup(ip)
        with _write_lock:
            con = _write_con()
            try:
                con.execute(
                    """UPDATE rdns SET hostname=?, status=?, fcrdns_verified=?,
                       looked_up_at=? WHERE ip=?""",
                    (hostname, status, int(fcrdns), _now(), ip),
                )
                con.commit()
            finally:
                con.close()
        if status == "resolved":
            resolved += 1
            yield {"type": "log", "message": f"Resolved {ip} -> {hostname}"}
        else:
            errors += 1
            yield {"type": "log", "message": f"Failed to resolve {ip}: {status}"}

    # ── Pass 1b: refresh stale entries (>48h old) ────────────────────────────
    with _write_lock:
        con = _write_con()
        try:
            stale = con.execute(
                """SELECT ip FROM rdns
                   WHERE status != 'pending'
                     AND looked_up_at < datetime('now', '-48 hours')
                   LIMIT ?""",
                (max(1, limit // 4),),
            ).fetchall()
        finally:
            con.close()

    if stale:
        yield {"type": "status", "message": f"Refreshing {len(stale)} stale cache entries..."}
        for (ip,) in stale:
            hostname, status, fcrdns = _do_lookup(ip)
            with _write_lock:
                con = _write_con()
                try:
                    con.execute(
                        """UPDATE rdns SET hostname=?, status=?, fcrdns_verified=?,
                           looked_up_at=? WHERE ip=?""",
                        (hostname, status, int(fcrdns), _now(), ip),
                    )
                    con.commit()
                finally:
                    con.close()
            yield {"type": "log", "message": f"Refreshed {ip} -> {hostname}"}

    # ── Pass 2: discovery — find new IPs from DuckDB sources ─────────────────
    yield {"type": "status", "message": "Discovering new IPs from log sources..."}
    try:
        for event in _discover_new_ips_gen(max_new=500):
            if event["type"] == "count":
                discovered_count = event["count"]
                yield {"type": "status", "message": f"Discovered {discovered_count} new IPs."}
            else:
                yield event
    except Exception as e:
        logger.error("[rdns_cache] Discovery pass failed: %s", e)
        yield {"type": "error", "message": f"Discovery failed: {e}"}

    _last_enrichment_at = _now()
    yield {
        "type": "done",
        "message": f"Enrichment complete. Resolved: {resolved}, Errors: {errors}, New IPs found: {discovered_count}",
    }


def enrich_batch(limit: int = 200) -> dict:
    """Resolve pending IPs with FCrDNS validation, then discover new IPs from
    DuckDB sources.

    Returns a summary dict with counts for monitoring/logging.
    """
    global _last_enrichment_at

    resolved = 0
    errors = 0
    discovered = 0

    # ── Pass 1: resolve pending IPs ──────────────────────────────────────────
    with _write_lock:
        con = _write_con()
        try:
            pending = con.execute("SELECT ip FROM rdns WHERE status = 'pending' LIMIT ?", (limit,)).fetchall()
        finally:
            con.close()

    for (ip,) in pending:
        hostname, status, fcrdns = _do_lookup(ip)
        with _write_lock:
            con = _write_con()
            try:
                con.execute(
                    """UPDATE rdns SET hostname=?, status=?, fcrdns_verified=?,
                       looked_up_at=? WHERE ip=?""",
                    (hostname, status, int(fcrdns), _now(), ip),
                )
                con.commit()
            finally:
                con.close()
        if status == "resolved":
            resolved += 1
        else:
            errors += 1

    # ── Pass 1b: refresh stale entries (>48h old) ────────────────────────────
    with _write_lock:
        con = _write_con()
        try:
            stale = con.execute(
                """SELECT ip FROM rdns
                   WHERE status != 'pending'
                     AND looked_up_at < datetime('now', '-48 hours')
                   LIMIT ?""",
                (max(1, limit // 4),),
            ).fetchall()
        finally:
            con.close()

    for (ip,) in stale:
        hostname, status, fcrdns = _do_lookup(ip)
        with _write_lock:
            con = _write_con()
            try:
                con.execute(
                    """UPDATE rdns SET hostname=?, status=?, fcrdns_verified=?,
                       looked_up_at=? WHERE ip=?""",
                    (hostname, status, int(fcrdns), _now(), ip),
                )
                con.commit()
            finally:
                con.close()

    # ── Pass 2: discovery — find new IPs from DuckDB sources ─────────────────
    try:
        discovered = _discover_new_ips(max_new=500)
    except Exception as e:
        logger.error("[rdns_cache] Discovery pass failed: %s", e)

    _last_enrichment_at = _now()
    summary = {"resolved": resolved, "errors": errors, "discovered": discovered}
    if resolved > 0 or errors > 0 or discovered > 0:
        logger.info("🌐 \x1b[34m[rdns]\x1b[0m enrich_batch complete: %s", summary)
    else:
        logger.debug("🌐 \x1b[34m[rdns]\x1b[0m enrich_batch complete (no activity)")
    return summary


def get_stats() -> dict:
    """Return cache stats for the admin UI."""
    con = _read_con()
    if con is None:
        return {"total": 0, "pending": 0, "last_enrichment_at": None}
    try:
        total = con.execute("SELECT count(*) FROM rdns").fetchone()[0]
        pending = con.execute("SELECT count(*) FROM rdns WHERE status='pending'").fetchone()[0]
    finally:
        con.close()
    return {
        "total": total,
        "pending": pending,
        "last_enrichment_at": _last_enrichment_at,
    }


def backfill_from_sources_gen(max_ips: int = 50_000) -> int:
    """One-time seed: scan all DuckDB sources for IPs from the last 30 days.
    Yields progress events.
    """
    count = 0
    for event in _discover_new_ips_gen(max_new=max_ips, days=30):
        if event["type"] == "count":
            count = event["count"]
            yield {"type": "done", "message": f"Backfill complete. Enqueued {count} IPs."}
        else:
            yield event


def backfill_from_sources(max_ips: int = 50_000) -> int:
    """One-time seed: scan all DuckDB sources for IPs from the last 30 days.

    Returns count of newly enqueued IPs.
    """
    return _discover_new_ips(max_new=max_ips, days=30)


# ── Verification logic (pure, no DB) ─────────────────────────────────────────


def classify(
    ip: str,
    hostname: str | None,
    status: str,
    fcrdns_verified: bool,
    verification_domains: list[str],
    verification_cidrs: list[str] | None = None,
) -> str:
    """Classify a bot hit into verified / impersonator / unverified.

    Tries CIDR match first (instant trust), then falls back to FCrDNS validation.
    """
    # 1. CIDR match is authoritative if provided
    if verification_cidrs and _is_ip_in_cidrs(ip, verification_cidrs):
        return "verified"

    # 2. Fallback to FCrDNS
    if not verification_domains:
        return "unverified"

    if status == "pending":
        return "unverified_pending"

    if status in ("nxdomain", "error") or hostname is None or not fcrdns_verified:
        return "impersonator"

    if any(hostname == d or hostname.endswith("." + d) for d in verification_domains):
        return "verified"

    return "impersonator"


# ── Internal helpers ──────────────────────────────────────────────────────────


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _do_lookup(ip: str) -> tuple[str | None, str, bool]:
    """Perform reverse + forward DNS lookup for FCrDNS validation."""
    try:
        hostname = socket.gethostbyaddr(ip)[0]
    except socket.herror:
        return None, "nxdomain", False
    except Exception:
        return None, "error", False

    # FCrDNS: forward-lookup the hostname and check if original IP is in result
    try:
        forward_ips = {info[4][0] for info in socket.getaddrinfo(hostname, None)}
        fcrdns_verified = ip in forward_ips
    except Exception:
        fcrdns_verified = False

    return hostname, "resolved", fcrdns_verified


def _discover_new_ips_gen(max_new: int = 500, days: int = 30):
    """Query all DuckDB sources for IPs not yet in the rDNS cache.
    Yields progress events.
    """
    try:
        from backend import config as svcconfig
        from backend.core.duckdb import get_connection, get_source_for_service
    except ImportError:
        yield {"type": "count", "count": 0}
        return

    try:
        # Load fully hydrated source dictionaries (including credentials)
        sources = []
        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if service_id:
                src = get_source_for_service(service_id)
                if src:
                    sources.append(src)
    except Exception as e:
        logger.error("[rdns_cache] Could not load sources for discovery: %s", e)
        yield {"type": "count", "count": 0}
        return

    # Collect IPs not already in cache across all sources
    new_ips: set[str] = set()
    remaining = max_new

    for src in sources:
        if remaining <= 0:
            break
        yield {"type": "status", "message": f"Scanning {src.get('name')}..."}
        try:
            # read_only: SELECT DISTINCT against the view, no writes.
            con = get_connection(src, read_only=True)
            try:
                from backend.core.duckdb import _safe_table_name

                # Check if this source has an ip column
                table_name = _safe_table_name(src["name"])
                cols = [
                    r[0]
                    for r in con.execute(
                        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
                    ).fetchall()
                ]
                if "ip" not in cols:
                    continue

                rows = con.execute(
                    f"""SELECT DISTINCT ip FROM "{table_name}"
                        WHERE ip IS NOT NULL
                          AND timestamp >= now() - INTERVAL '{days} days'
                        LIMIT {remaining * 2}"""
                ).fetchall()
            finally:
                con.close()

            # Filter to IPs not already cached
            candidate_ips = [r[0] for r in rows if r[0]]
            if not candidate_ips:
                continue

            with _write_lock:
                con2 = _write_con()
                try:
                    placeholders = ",".join("?" * len(candidate_ips))
                    already = {
                        r[0]
                        for r in con2.execute(
                            f"SELECT ip FROM rdns WHERE ip IN ({placeholders})",
                            candidate_ips,
                        ).fetchall()
                    }
                finally:
                    con2.close()

            found_this_src = 0
            for ip in candidate_ips:
                if ip not in already and ip not in new_ips:
                    new_ips.add(ip)
                    found_this_src += 1
                    remaining -= 1
                    if remaining <= 0:
                        break
            if found_this_src > 0:
                yield {"type": "log", "message": f"Found {found_this_src} new IPs in {src.get('name')}"}

        except Exception as e:
            logger.error("[rdns_cache] Discovery error for source %s: %s", src.get("name"), e)
            yield {"type": "log", "message": f"Error scanning {src.get('name')}: {e}"}

    if new_ips:
        enqueue(list(new_ips))
        logger.info("🌐 \x1b[34m[rdns]\x1b[0m Discovered %d new IPs for enrichment", len(new_ips))

    yield {"type": "count", "count": len(new_ips)}


def _discover_new_ips(max_new: int = 500, days: int = 30) -> int:
    """Query all DuckDB sources for IPs not yet in the rDNS cache.

    Returns count of newly enqueued IPs.
    """
    count = 0
    for event in _discover_new_ips_gen(max_new, days):
        if event["type"] == "count":
            count = event["count"]
    return count
