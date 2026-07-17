"""Wellknown-bots rollup writer + reader.

Pre-materialises the regex-prefiltered (ua, ip, count) rows that the
/api/security/aggregates "wellknown_bots" block reads. The 500-pattern
RE2 prefilter against every UA in the temp_table is the dominant cost
in that endpoint (~155 ms / 12% of wall time per the 2026-06-11 perf
audit). Amortising it at ingest time means request-path workers do a
cheap parquet read of an already-narrowed list instead of re-running
the regex over the full window on every dashboard load.

Schema: ``cache/<svc>/rollups/wellknown_bots/hour=YYYY-MM-DD-HH/
compacted_<uuid>.parquet`` with columns ``(ua, ip, request_count,
pattern_set_version)``. The bot_id and FCrDNS classification stay in
Python — they're cheap given pre-filtered input and avoid having to
rewrite the rollup when classify() / matcher semantics change.

Version invalidation: every row carries the
:func:`backend.utils.bot_sources.get_pattern_set_version` value
captured at write time. The reader compares against the current
version and falls back to the live SQL path for any hour whose
rollup is stale or missing — correctness over speed.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime, timedelta

from ._common import parse_hour_token, quote_path_list

logger = logging.getLogger(__name__)


def _wellknown_bots_root(source: dict) -> str:
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "wellknown_bots")


def _hour_has_parquets(d: str) -> bool:
    """True iff directory ``d`` holds at least one published parquet
    (ignoring in-flight ``.tmp_`` writes). Cheaper than glob.glob."""
    try:
        for f in os.listdir(d):
            if f.endswith(".parquet") and not f.startswith(".tmp_"):
                return True
    except OSError:
        pass
    return False


def recompute_wellknown_bots_rollup(
    service_id: str,
    source: dict,
    hours: set[str] | list[str],
) -> int:
    """Pre-materialise (ua, ip, count) rows for the wellknown_bots
    request-path query for each touched hour.

    Reads each CLOSED hour's committed Iceberg data partition directly —
    ``cache/<svc>/data/timestamp_hour=<H>/*.parquet`` — via a private
    ``:memory:`` DuckDB connection, rather than the per-service iceberg
    *view*. The view UNIONs the in-progress ``buffer/`` parquets by
    absolute path; a connection that binds the view once and loops for
    minutes fails every COPY after a concurrent ingest flushes one of
    those buffer files (observed 2026-06-27: a full historical backfill
    of ~800 hours took ~27 min and every per-hour COPY after the first
    buffer commit raised ``IO Error: Cannot open buffer/batch_*.parquet``,
    so 0 hours migrated). A closed hour's rows are all committed under
    ``data/timestamp_hour=<H>/`` — the buffer holds only the active hour —
    so reading that partition is complete, and the glob is re-resolved
    per hour, so a file rewritten by compaction mid-loop fails only that
    one hour's COPY (the next backfill tick self-heals it) instead of the
    whole run.

    Skips the active hour — its data is still in flight in the buffer and
    the security reader serves it live. Returns the number of hour
    partitions written.

    Best-effort: any failure logs and returns the count written so far.
    The sync caller should NOT raise on a rollup failure (the reader's
    live-SQL fallback covers any missing hour).
    """
    if not hours:
        return 0
    import duckdb

    from backend.core.duckdb import _cache_dir
    from backend.core.iceberg.view import _get_service_lock
    from backend.utils.bot_sources import get_bot_regex_pattern, get_pattern_set_version

    version = get_pattern_set_version()
    if not version:
        # No source files cached yet — nothing to materialise. The
        # reader's live-SQL fallback handles this correctly.
        return 0
    pattern = get_bot_regex_pattern(500)
    if not pattern:
        return 0

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    parsed: list[str] = []
    for h in hours:
        if h == active_hour:
            continue
        if parse_hour_token(h) is None:
            continue
        parsed.append(h)
    if not parsed:
        return 0

    try:
        cache_dir = _cache_dir(source)
    except Exception:
        return 0
    data_root = os.path.join(cache_dir, "data")

    # Restrict to hours whose committed data partition exists on disk.
    # An hour with no partition is either genuinely empty or not yet
    # flushed; the reader's coverage floor + live fallback handles it.
    eligible: list[str] = [h for h in parsed if _hour_has_parquets(os.path.join(data_root, f"timestamp_hour={h}"))]
    if not eligible:
        return 0

    bots_root = _wellknown_bots_root(source)
    os.makedirs(bots_root, exist_ok=True)
    lock_key = source.get("name", "default")
    pattern_sql = pattern.replace("'", "''")
    version_sql = version.replace("'", "''")

    rebuilt = 0
    # :memory: connection reads only static, already-committed parquet
    # files — never the buffer-including per-service view. Matches the
    # isolation bundle_hours / compact_closed_days / the rollup readers use.
    con = duckdb.connect(":memory:")
    try:
        # Validate the source has the columns we need before per-hour
        # work — saves N×(failed COPY) on services without UA/IP fields.
        # Probe the first eligible hour's partition (closed hours share a
        # schema); a later hour missing the column falls through to the
        # per-hour COPY's own try/except.
        probe_glob = os.path.join(data_root, f"timestamp_hour={eligible[0]}", "*.parquet").replace("'", "''")
        try:
            desc = con.execute(f"SELECT * FROM read_parquet('{probe_glob}', union_by_name=true) LIMIT 0").description
            cols = {d[0] for d in (desc or [])}
        except duckdb.Error as e:
            logger.warning("[rollups] %s: bot-rollup column probe failed: %s", service_id, e)
            return 0
        if "ua" not in cols or "ip" not in cols:
            return 0

        for hour in eligible:
            # COPY ... TO '<path>' targets a SINGLE FILE when no
            # PARTITION_BY clause is present (a directory target only
            # works alongside PARTITION_BY — observed 2026-06-12: an
            # earlier draft used a tmp directory and DuckDB raised
            # "Cannot open file: Is a directory"). Write to a unique
            # tmp file under the final hour-partition dir, then rename
            # to the canonical compacted_ name under the iceberg lock.
            hour_glob = os.path.join(data_root, f"timestamp_hour={hour}", "*.parquet").replace("'", "''")
            # Defense-in-depth on top of the partition glob: a half-open
            # TIMESTAMPTZ range (absolute instants) is TZ-agnostic, unlike
            # strftime(timestamp,...) which renders in the connection's
            # session TZ. Matches build_per_hour_bundles / security_dims.
            hour_dt = datetime.strptime(hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)
            start_iso = hour_dt.isoformat()
            end_iso = (hour_dt + timedelta(hours=1)).isoformat()
            hour_dir = os.path.join(bots_root, f"hour={hour}")
            os.makedirs(hour_dir, exist_ok=True)
            tmp_path = os.path.join(hour_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
            tmp_path_sql = tmp_path.replace("'", "''")
            try:
                con.execute(
                    f"COPY ("
                    f"  SELECT ua, ip, count(*) AS request_count, "
                    f"         '{version_sql}' AS pattern_set_version "
                    f"  FROM read_parquet('{hour_glob}', union_by_name=true) "
                    f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
                    f"    AND timestamp <  TIMESTAMPTZ '{end_iso}' "
                    f"    AND ua IS NOT NULL AND ip IS NOT NULL "
                    f"    AND regexp_matches(ua, '{pattern_sql}') "
                    f"  GROUP BY ua, ip "
                    f"  ORDER BY request_count DESC "
                    f"  LIMIT 50000"
                    f") TO '{tmp_path_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
            except duckdb.Error as e:
                logger.warning("[rollups] %s: bot-rollup COPY failed for hour=%s: %s", service_id, hour, e)
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                continue

            # Atomic publish under the per-service iceberg lock.
            # Serializes against concurrent rebuilds (backfill +
            # post-sync overlap). Sweep any pre-existing parquets
            # for the hour FIRST so a reader scanning the dir can't
            # see a stale-version row alongside the freshly-written
            # one (the version check on read would catch it, but
            # eliminating the window is cheaper).
            with _get_service_lock(lock_key):
                try:
                    for fname in os.listdir(hour_dir):
                        if fname.endswith(".parquet") and not fname.startswith(".tmp_"):
                            try:
                                os.remove(os.path.join(hour_dir, fname))
                            except OSError:
                                pass
                except OSError:
                    pass
                final_path = os.path.join(hour_dir, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
                try:
                    os.replace(tmp_path, final_path)
                except OSError as e:
                    logger.warning(
                        "[rollups] %s: bot-rollup publish failed for hour=%s: %s",
                        service_id,
                        hour,
                        e,
                    )
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
                    continue
            rebuilt += 1
    finally:
        con.close()

    return rebuilt


def backfill_wellknown_bots_rollup(service_id: str, source: dict) -> int:
    """Self-heal the wellknown_bots rollup tree against the CURRENT
    ``pattern_set_version``: rebuild what it can, drop what it can't.

    Two passes, both keyed on the committed Iceberg data partitions
    (``data/timestamp_hour=<H>/``) — the universe the writer can actually
    read from now that :func:`recompute_wellknown_bots_rollup` reads data
    dirs directly:

    1. **Rebuild** — every closed hour that HAS a data partition but lacks a
       current-version wellknown partition (missing entirely, or stamped
       under an older version, e.g. the historical mtime-format versions or
       a since-superseded content hash). :func:`recompute_wellknown_bots_rollup`
       sweeps the hour dir before writing, so a rebuild replaces the stale file.

    2. **Stale-sweep** — every closed hour whose wellknown partition is on a
       NON-current version but whose raw data partition is GONE (compacted out
       of the shorter raw-retention window) — so it can never be rebuilt onto
       the current version. Its files are deleted. This is the load-bearing
       half: the reader bails the WHOLE window to the live path when the
       window mixes versions, and raw data is retained for far fewer days than
       the rollup tree, so without this a single unrebuildable stale hour
       pins a 30 d window on the live scan (and its all-rows temp) forever.
       Deleting it lets the reader see a uniform window; the now-absent hour
       is treated as empty — a sub-coverage-floor (<2 %) leaderboard effect,
       vs. forcing the entire window live. Empty stale partitions (0 rows)
       are left alone: they carry no version row, so they don't poison the
       reader's version check.

    Idempotent. Returns the number of hour partitions (re)written; logs the
    stale-sweep count separately.
    """
    from backend.core.duckdb import _cache_dir
    from backend.core.iceberg.view import _get_service_lock
    from backend.utils.bot_sources import get_pattern_set_version

    version = get_pattern_set_version()
    if not version:
        return 0

    try:
        cache_dir = _cache_dir(source)
    except Exception:
        return 0
    data_root = os.path.join(cache_dir, "data")
    bots_root = _wellknown_bots_root(source)
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    # Closed hours with a committed raw data partition — the rebuildable set.
    data_hours: set[str] = set()
    try:
        for entry in os.listdir(data_root):
            if not entry.startswith("timestamp_hour="):
                continue
            h = entry[len("timestamp_hour=") :]
            if h < active_hour and _hour_has_parquets(os.path.join(data_root, entry)):
                data_hours.add(h)
    except OSError:
        data_hours = set()

    # Existing wellknown partitions: per-hour file paths + per-hour versions.
    hour_paths: dict[str, list[str]] = {}
    if os.path.isdir(bots_root):
        try:
            for entry in os.listdir(bots_root):
                if not entry.startswith("hour="):
                    continue
                hour = entry[len("hour=") :]
                hd = os.path.join(bots_root, entry)
                files = [
                    os.path.join(hd, f) for f in os.listdir(hd) if f.endswith(".parquet") and not f.startswith(".tmp_")
                ]
                if files:
                    hour_paths[hour] = files
        except OSError:
            hour_paths = {}

    current_hours: set[str] = set()
    stale_hours: set[str] = set()
    if hour_paths:
        import duckdb

        all_paths = [p for paths in hour_paths.values() for p in paths]
        con = duckdb.connect(":memory:")
        try:
            paths_sql = quote_path_list(all_paths)
            version_sql = version.replace("'", "''")
            # One batched read of (hour, version) pairs across the tree.
            # Empty partitions contribute no row → invisible here (a current
            # empty re-builds harmlessly if it has data; a stale empty has no
            # version row so it can't poison the reader — leave it).
            rows = con.execute(
                f"SELECT DISTINCT regexp_extract(filename, 'hour=([0-9-]+)', 1) AS hr, "
                f"  (pattern_set_version = '{version_sql}') AS is_current "
                f"FROM read_parquet([{paths_sql}], filename=true)"
            ).fetchall()
            for hr, is_current in rows:
                if not hr:
                    continue
                if is_current:
                    current_hours.add(hr)
                else:
                    stale_hours.add(hr)
        except duckdb.Error:
            current_hours = set()
            stale_hours = set()
        finally:
            con.close()

    # Pass 1 — rebuild data-backed hours lacking a current-version partition.
    missing = sorted(h for h in data_hours if h not in current_hours)
    n_written = recompute_wellknown_bots_rollup(service_id, source, missing) if missing else 0

    # Pass 2 — delete stale partitions that can't be rebuilt (raw data gone),
    # so they stop pinning the window on the live path. An hour that also has
    # current-version files (mixed on disk) or a data partition (rebuilt in
    # pass 1) is left to the writer's own sweep.
    lock_key = source.get("name", "default")
    n_deleted = 0
    for hour in sorted(stale_hours):
        if hour in current_hours or hour in data_hours or hour >= active_hour:
            continue
        with _get_service_lock(lock_key):
            removed_any = False
            for p in hour_paths.get(hour, []):
                try:
                    os.remove(p)
                    removed_any = True
                except OSError:
                    pass
            # Drop the now-empty partition dir so it doesn't linger as a
            # directory the reader has to stat. Best-effort: skip if a
            # concurrent writer repopulated it (rmdir fails on non-empty).
            try:
                os.rmdir(os.path.join(bots_root, f"hour={hour}"))
            except OSError:
                pass
        if removed_any:
            n_deleted += 1
    if n_deleted:
        logger.info(
            "[rollups] %s: swept %d unrebuildable stale wellknown hour(s) (raw data aged out)",
            service_id,
            n_deleted,
        )

    return n_written


def read_wellknown_bots_rollup(
    source: dict,
    start_time: str,
    end_time: str,
) -> list[tuple[str, str, int]] | None:
    """Return ``[(ua, ip, request_count), ...]`` for the request window
    by reading the wellknown_bots rollup parquet partitions.

    Serves CLOSED hours only on windows >= 48 h, dropping the in-progress
    active hour (a negligible slice on a wide window — the same
    closed-hours-only posture slow_urls / security_dims use). This is what
    lets the security endpoint skip the all-rows live regex + its temp.

    Returns ``None`` (callers should fall back to the live SQL path) when
    ANY of:
    - The window is < 48 h (live serves narrow windows, incl. the active hour).
    - The rollup directory doesn't exist (writer hasn't run yet).
    - Fewer than 50 % of the window's closed hours have a parquet partition
      (a half-built rollup falls back rather than badly undercount).
    - The cached parquet was written under a different
      ``pattern_set_version`` than the currently-loaded bot sources
      (a sources refresh has happened since the last rollup).

    The closed-hours read is single-path (no half-rollup-half-live union):
    hours with a partition are read from the rollup, hours without one
    contribute nothing — no overlap with a live path, so no double-count.
    """
    from backend.utils.bot_sources import get_pattern_set_version

    current_version = get_pattern_set_version()
    if not current_version:
        return None

    bots_root = _wellknown_bots_root(source)
    if not os.path.isdir(bots_root):
        return None

    # Enumerate every closed hour in the request window. The window
    # comes in as ISO timestamps; we round start DOWN to the hour and
    # end UP so a 24h window of [2026-06-11T00:00, 2026-06-12T00:00)
    # asks for 24 hour-partitions.
    try:
        start_dt = _parse_iso_to_hour(start_time)
        end_dt = _parse_iso_to_hour(end_time)
    except Exception:
        return None
    if start_dt is None or end_dt is None:
        return None
    if end_dt - start_dt > timedelta(days=366):
        return None
    # Only serve wide (>= 48 h) windows from the rollup. Narrow windows
    # (e.g. 24 h) stay on the live SQL path: the closed-hour read cost
    # doesn't amortise there, and the live path also covers the in-progress
    # active hour the rollup drops. Matches the >= 48 h gate the slow_urls /
    # security_dims rollups use (_SLOW_URLS_ROLLUP_MIN_HOURS).
    if end_dt - start_dt < timedelta(hours=48):
        return None

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    # Closed hours only — drop the in-progress active hour (no partition is
    # written for it). On a >= 48 h window the dropped slice is negligible;
    # same closed-hours-only posture as slow_urls / security_dims (neither
    # merges the active hour). Previously the reader BAILED to live whenever
    # the window touched the active hour, which — since dashboard requests
    # always end "now" — meant the rollup never served, leaving the live
    # regex (and its all-rows temp) on the request path.
    closed_hours: list[str] = []
    cur = start_dt
    while cur <= end_dt:
        h = cur.strftime("%Y-%m-%d-%H")
        if h < active_hour:
            closed_hours.append(h)
        cur += timedelta(hours=1)
    if not closed_hours:
        return None

    # Collect partitions for the hours that HAVE one, requiring >= 50 %
    # closed-hour coverage (mirrors _collect_rollup_paths' floor). Hours
    # with no partition contribute nothing — correct for genuinely empty
    # hours; backfill_wellknown_bots_rollup fills writer-lagged ones. Below
    # the floor we fall back to live so a half-built rollup can't badly
    # undercount the leaderboard.
    paths: list[str] = []
    covered = 0
    for h in closed_hours:
        hour_dir = os.path.join(bots_root, f"hour={h}")
        if not os.path.isdir(hour_dir):
            continue
        files = [f for f in os.listdir(hour_dir) if f.endswith(".parquet") and not f.startswith(".tmp_")]
        if not files:
            continue
        covered += 1
        paths.extend(os.path.join(hour_dir, f) for f in files)
    if not paths or covered < (len(closed_hours) + 1) // 2:
        return None

    # Single read_parquet across the whole window; DuckDB's :memory:
    # connection avoids contending with the per-service writer pool.
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        paths_sql = quote_path_list(paths)
        try:
            # First check: is the rollup version current? Pull a single
            # distinct version value — if any row's version mismatches
            # the current set, fall back (the writer guarantees one
            # version per partition, but a stale partition from before
            # a source refresh could still be on disk).
            version_row = con.execute(
                f"SELECT DISTINCT pattern_set_version FROM read_parquet([{paths_sql}]) LIMIT 2"
            ).fetchall()
        except duckdb.Error:
            return None
        if not version_row:
            # Empty rollup — no bot traffic in window. Safe to return
            # an empty list (the reader handles that the same as a
            # live SQL returning zero rows).
            return []
        versions = {r[0] for r in version_row}
        if len(versions) > 1 or current_version not in versions:
            return None

        # Aggregate (ua, ip) ACROSS hours before the LIMIT. Each hour's
        # partition holds one row per (ua, ip); a 30 d window has ~700 hour
        # partitions but typically only ~1k distinct (ua, ip) pairs, so a raw
        # per-hour read returns ~200k rows that collapse to ~1k. Two reasons
        # the GROUP BY is load-bearing, not cosmetic:
        #   1. Correctness — the old per-hour ``LIMIT 10000`` capped on
        #      per-hour counts, so a bot with a small count in each of many
        #      hours (large total) could be dropped, and surviving bots had
        #      only their top-10k per-hour slices summed (undercount). SUM
        #      first, cap second → exact totals + no dropped long tail.
        #   2. Cost — the caller runs a 500-pattern UA matcher + an rDNS-cache
        #      lookup PER returned row; collapsing ~200k → ~1k cut that
        #      request-path enrichment from seconds to sub-second.
        rows = con.execute(
            f"SELECT ua, ip, SUM(request_count) AS request_count FROM read_parquet([{paths_sql}]) "
            f"GROUP BY ua, ip ORDER BY request_count DESC LIMIT 10000"
        ).fetchall()
        return [(r[0], r[1], int(r[2])) for r in rows]
    finally:
        con.close()


def _parse_iso_to_hour(iso: str) -> datetime | None:
    """Parse an ISO-ish timestamp string into a UTC datetime truncated
    to the hour. Accepts both ``2026-06-11T00:00:00Z`` and
    ``2026-06-11T00:00:00+00:00``. Returns ``None`` on parse failure
    (caller falls back to the live SQL path).
    """
    if not iso:
        return None
    try:
        s = iso.rstrip("Z")
        if "+" not in s and len(s) > 10 and s[-3] == ":":
            # Hand-formatted offset like "+00:00" → already handled
            pass
        dt = datetime.fromisoformat(s).replace(tzinfo=UTC)
    except ValueError:
        return None
    return dt.replace(minute=0, second=0, microsecond=0)
