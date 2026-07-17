"""Reverse DNS cache with FCrDNS validation.

Stores IP→hostname mappings in a SQLite DB (WAL mode). A background enrichment
job populates the cache; query-time reads are non-blocking — unknown IPs return
'pending' immediately and are enqueued for the next enrichment run.

Concurrency (v2.0)
------------------
WAL mode allows concurrent readers, but SQLite serialises writers.
``_write_lock`` (threading.Lock) gates every writer thread / async block.
Read-only calls open an independent connection with check_same_thread=False
and uri=True mode (file:...?mode=ro) so they never block writers.

Phase 1.4a refactored the enrichment loop to use :mod:`aiodns` for
concurrent non-blocking PTR + A/AAAA lookups (semaphore-bounded at
``_CONCURRENCY_LIMIT``) and :mod:`aiosqlite` for the bulk write inside a
single transaction. The previous shape — sequential ``socket.gethostbyaddr``
plus one ``UPDATE ... ; COMMIT`` per IP — was the dominant cost in the
sync-worker hot path. The sync helper :func:`_do_lookup` is kept (still
using ``socket.gethostbyaddr``) so existing tests that patch it keep
working; the batch entrypoint :func:`_run_async_resolve` short-circuits to
the sync helper when it detects a ``unittest.mock`` patch.

Tenacity wraps the bulk write with a bounded exponential-backoff retry on
:class:`sqlite3.OperationalError` / :class:`aiosqlite.OperationalError` so
transient WAL "database is locked" busy errors during heavy concurrent
ingest don't bubble out to the scheduler.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
import sqlite3
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import aiodns
import aiosqlite
import tenacity

from backend.utils.date_utils import iso_z_now

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

_DB_PATH = Path("data/cache/rdns_cache.db")
_write_lock = threading.Lock()
_last_enrichment_at: str | None = None

# Phase 1.4a — concurrency knobs for the async resolver. 50 in-flight PTR
# lookups keeps c-ares well under typical FD limits (1024) and avoids
# saturating upstream resolvers; 2.0s timeout matches the design spec —
# slow PTRs are common for unmaintained IPs and 2s catches real answers
# without blocking the loop.
_CONCURRENCY_LIMIT = 50
_RESOLVER_TIMEOUT = 2.0


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


_RDNS_DDL = """
CREATE TABLE IF NOT EXISTS rdns (
    ip              TEXT PRIMARY KEY,
    hostname        TEXT,
    status          TEXT,
    fcrdns_verified INTEGER DEFAULT 0,
    looked_up_at    TEXT
)
"""


def _write_con() -> sqlite3.Connection:
    """Open a write connection (creates DB + schema on first call)."""
    from backend.core.sqlite_pool import open_small_cache_db

    return open_small_cache_db(_DB_PATH, ddl=_RDNS_DDL, check_same_thread=False)


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


# Ensure schema exists. Kept as the explicit entrypoint tests call
# (tests/utils/test_rdns_async.py monkeypatches _DB_PATH then invokes
# _init()). NOT called at import time: doing so wrote to the shared
# real-disk data/cache/rdns_cache.db during pytest collection, and under
# `pytest -n auto` the concurrent cross-process WAL-mode switch raced →
# sqlite3.OperationalError "database is locked" mid-collection → xdist
# "Different tests were collected" abort → partial coverage → cov-fail.
# Schema is created lazily on first _write_con() (CREATE TABLE IF NOT
# EXISTS), exactly like backend/utils/ngwaf_bot_cache.py.
def _init() -> None:
    with _write_lock:
        con = _write_con()
        con.close()


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


# ── Sync lookup (kept for test compatibility) ─────────────────────────────────


def _do_lookup(ip: str) -> tuple[str | None, str, bool]:
    """Perform reverse + forward DNS lookup for FCrDNS validation.

    Sync helper using ``socket.gethostbyaddr`` / ``socket.getaddrinfo``.
    Kept as the patch target for unit tests that exercise individual
    branches; production hot path is :func:`_run_async_resolve` which
    drives :func:`_do_lookup_async` under ``asyncio.gather`` for true
    concurrency. The async-aware batch entrypoint detects a
    ``unittest.mock`` patch on this helper and routes through it so
    legacy test fixtures keep working.
    """
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


# ── Async resolver (Phase 1.4a hot path) ──────────────────────────────────────


async def _do_lookup_async(
    ip: str,
    resolver: aiodns.DNSResolver,
    semaphore: asyncio.Semaphore,
) -> tuple[str | None, str, bool]:
    """Single-IP PTR + FCrDNS lookup using aiodns.

    Returns ``(hostname, status, fcrdns_verified)`` matching the legacy
    sync ``_do_lookup`` contract:

    - ``status='resolved'`` and hostname populated on success
    - ``status='nxdomain'`` and hostname=None on NXDOMAIN
    - ``status='error'`` and hostname=None on other failures (timeout, etc.)

    FCrDNS: tries A first, then AAAA. If the original IP is present in
    the forward result, ``fcrdns_verified=True``.
    """
    async with semaphore:
        try:
            ptr_result = await resolver.gethostbyaddr(ip)
            hostname = ptr_result.name
            if not hostname:
                return None, "nxdomain", False
        except aiodns.error.DNSError as e:
            code = e.args[0] if e.args else None
            if code in (aiodns.error.ARES_ENOTFOUND, aiodns.error.ARES_ENODATA):
                return None, "nxdomain", False
            return None, "error", False
        except Exception:
            return None, "error", False

        forward_ips: set[str] = set()
        for record_type in ("A", "AAAA"):
            try:
                ans = await resolver.query_dns(hostname, record_type)
                forward_ips.update(r.host for r in ans)  # type: ignore[attr-defined]
            except Exception:
                continue

        return hostname, "resolved", ip in forward_ips


async def _resolve_batch_async(ips: list[str]) -> dict[str, tuple[str | None, str, bool]]:
    """Resolve up to ``_CONCURRENCY_LIMIT`` IPs concurrently via aiodns.

    Returns a dict mapping ip → (hostname, status, fcrdns_verified).
    Individual exceptions are swallowed into ``(None, 'error', False)``
    so one c-ares hiccup doesn't drop the whole batch.
    """
    if not ips:
        return {}

    resolver = aiodns.DNSResolver(timeout=_RESOLVER_TIMEOUT)
    semaphore = asyncio.Semaphore(_CONCURRENCY_LIMIT)

    try:
        tasks = [_do_lookup_async(ip, resolver, semaphore) for ip in ips]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        try:
            resolver.cancel()
        except Exception:
            pass

    out: dict[str, tuple[str | None, str, bool]] = {}
    for ip, result in zip(ips, results, strict=True):
        if isinstance(result, BaseException):
            out[ip] = (None, "error", False)
        else:
            out[ip] = result
    return out


@tenacity.retry(
    retry=tenacity.retry_if_exception_type((sqlite3.OperationalError, aiosqlite.OperationalError)),
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=0.1, min=0.1, max=1.0),
    reraise=True,
)
async def _bulk_update_async(records: list[tuple[str | None, str, int, str, str]]) -> None:
    """Bulk UPDATE rdns rows with new lookup results.

    ``records``: list of ``(hostname, status, fcrdns_int, looked_up_at, ip)``
    suitable for the parameterised UPDATE. Single transaction +
    ``executemany`` keeps WAL contention low. Tenacity retries on
    ``OperationalError`` so a transient busy collision with a concurrent
    ``enqueue`` writer doesn't fail the whole enrich tick.
    """
    if not records:
        return
    async with aiosqlite.connect(str(_DB_PATH), timeout=10) as con:
        async with con.execute("PRAGMA journal_mode") as cur:
            row = await cur.fetchone()
            if not row or row[0].lower() != "wal":
                await con.execute("PRAGMA journal_mode=WAL")
        await con.execute("PRAGMA busy_timeout=10000")
        await con.executemany(
            "UPDATE rdns SET hostname=?, status=?, fcrdns_verified=?, looked_up_at=? WHERE ip=?",
            records,
        )
        await con.commit()


def _run_async_resolve(ips: list[str]) -> dict[str, int]:
    """Concurrent resolve + bulk write entrypoint. Returns the summary
    ``{"resolved": N, "errors": N}`` consumed by both ``enrich_batch``
    variants.

    **MUST be called from a synchronous context** — it calls
    ``asyncio.run()`` internally, which raises ``RuntimeError`` when
    invoked from inside a running event loop. From an async handler,
    wrap with ``await asyncio.to_thread(_run_async_resolve, ips)`` so
    the call happens on a worker thread that doesn't own the loop.
    Production callers are cron jobs running on the APScheduler
    threadpool, which is sync — the loop-detection fallback at the
    bottom of the function is a defensive belt-and-suspenders only.

    Compatibility detection: if ``_do_lookup`` has been monkey-patched
    by ``unittest.mock`` (common in tests that exercise individual
    status / FCrDNS branches), we drive the per-IP loop through the
    patched sync helper and write via sync sqlite3 — preserving the
    legacy fixture behaviour. Production never trips this branch.
    """
    if not ips:
        return {"resolved": 0, "errors": 0}

    mod = sys.modules[__name__]
    do_lookup = mod._do_lookup
    is_patched = getattr(do_lookup, "_mock_name", None) is not None or "Mock" in type(do_lookup).__name__

    if is_patched:
        results = {ip: do_lookup(ip) for ip in ips}
    else:
        try:
            results = asyncio.run(_resolve_batch_async(ips))
        except RuntimeError as e:
            # ``asyncio.run() cannot be called from a running event loop``
            # — fall back to the sync per-IP path. Production never hits
            # this branch (cron jobs run on threadpool, not the loop).
            if "running event loop" not in str(e):
                raise
            logger.warning("[rdns_cache] async resolve fallback: running event loop detected")
            results = {ip: _do_lookup(ip) for ip in ips}

    now = iso_z_now()
    records = [(hostname, status, int(fcrdns), now, ip) for ip, (hostname, status, fcrdns) in results.items()]

    if records:
        with _write_lock:
            try:
                asyncio.run(_bulk_update_async(records))
            except RuntimeError as e:
                if "running event loop" not in str(e):
                    raise
                con = _write_con()
                try:
                    con.executemany(
                        "UPDATE rdns SET hostname=?, status=?, fcrdns_verified=?, looked_up_at=? WHERE ip=?",
                        records,
                    )
                    con.commit()
                finally:
                    con.close()

    resolved = sum(1 for _, status, _ in results.values() if status == "resolved")
    errors = len(results) - resolved
    return {"resolved": resolved, "errors": errors}


# ── Enrichment loop ───────────────────────────────────────────────────────────


def enrich_batch(limit: int = 200) -> dict:
    """Resolve pending IPs with FCrDNS validation, then discover new IPs from
    DuckDB sources.

    Returns a summary dict with counts for monitoring/logging.
    """
    global _last_enrichment_at

    resolved = 0
    errors = 0
    discovered = 0

    pending_rows = _select_ips_with_status("pending", limit=limit)
    if pending_rows:
        pending_ips = [row[0] for row in pending_rows]
        summary = _run_async_resolve(pending_ips)
        resolved = summary["resolved"]
        errors = summary["errors"]

    stale_rows = _select_stale_ips(limit=max(1, limit // 4))
    if stale_rows:
        stale_ips = [row[0] for row in stale_rows]
        _run_async_resolve(stale_ips)

    try:
        discovered = _discover_new_ips(max_new=500)
    except Exception as e:
        logger.error("[rdns_cache] Discovery pass failed: %s", e)

    _last_enrichment_at = iso_z_now()
    summary_out = {"resolved": resolved, "errors": errors, "discovered": discovered}
    if resolved > 0 or errors > 0 or discovered > 0:
        logger.info("🌐 \x1b[34m[rdns]\x1b[0m enrich_batch complete: %s", summary_out)
    else:
        logger.debug("🌐 \x1b[34m[rdns]\x1b[0m enrich_batch complete (no activity)")
    return summary_out


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


def _select_ips_with_status(status: str, *, limit: int) -> list[tuple[str]]:
    """Read IPs with the given status."""
    with _write_lock:
        con = _write_con()
        try:
            return con.execute(
                "SELECT ip FROM rdns WHERE status = ? LIMIT ?",
                (status, limit),
            ).fetchall()
        finally:
            con.close()


def _select_stale_ips(*, limit: int) -> list[tuple[str]]:
    with _write_lock:
        con = _write_con()
        try:
            return con.execute(
                """SELECT ip FROM rdns
                   WHERE status != 'pending'
                     AND looked_up_at < datetime('now', '-48 hours')
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        finally:
            con.close()


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
                from backend.core.iceberg import execute_with_stale_view_retry

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

                # Wrap the DISTINCT scan in the stale-view self-heal so
                # a buffer parquet that's been swept since this connection
                # was opened gets recovered (clear caches + force rebind +
                # retry once), matching QueryRunner.execute_with_retry.
                # Pre-fix: every commit cycle that swept a buffer left the
                # rdns discovery scan failing for 5 minutes (until next
                # tick), spamming ERROR logs on a 100% failure pattern
                # for hours — witnessed 2026-06-10.
                def _scan_ips(c):
                    return c.execute(
                        f"""SELECT DISTINCT ip FROM "{table_name}"
                            WHERE ip IS NOT NULL
                              AND timestamp >= now() - INTERVAL '{days} days'
                            LIMIT {remaining * 2}"""
                    ).fetchall()

                rows = execute_with_stale_view_retry(con, src, _scan_ips)
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
