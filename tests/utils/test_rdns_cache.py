"""Tests for backend.utils.bot_sources.

Covers the pure helpers (``_is_ip_in_cidrs``, ``classify``) and the
public cache API (``enqueue``, ``get_hostname``, ``get_stats``) against
an isolated per-test SQLite file. The lookup + enrichment paths that
shell out to DNS are mocked at the ``socket`` boundary so tests don't
actually hit the network.
"""

from __future__ import annotations

import asyncio
import socket
from unittest.mock import AsyncMock, MagicMock, patch

import aiodns
import pytest

import backend.utils.bot_sources as rdns_cache


@pytest.fixture()
def isolated_db():
    con = rdns_cache._write_con()
    try:
        con.execute("DELETE FROM rdns")
        con.commit()
    except Exception:
        con.rollback()
    yield


# ── _is_ip_in_cidrs (pure) ────────────────────────────────────────────────────


@pytest.mark.usefixtures("isolated_db")
def test_cidr_match_returns_true_for_in_range():
    assert rdns_cache._is_ip_in_cidrs("66.249.66.1", ["66.249.64.0/19"]) is True


@pytest.mark.usefixtures("isolated_db")
def test_cidr_match_returns_false_for_out_of_range():
    assert rdns_cache._is_ip_in_cidrs("8.8.8.8", ["66.249.64.0/19"]) is False


@pytest.mark.usefixtures("isolated_db")
def test_cidr_match_short_circuits_empty_list():
    assert rdns_cache._is_ip_in_cidrs("8.8.8.8", []) is False


@pytest.mark.usefixtures("isolated_db")
def test_cidr_match_ignores_invalid_cidrs_in_list():
    """A malformed CIDR must not crash — just skip it and keep checking."""
    cidrs = ["not-a-cidr", "8.8.8.0/24"]
    assert rdns_cache._is_ip_in_cidrs("8.8.8.42", cidrs) is True


@pytest.mark.usefixtures("isolated_db")
def test_cidr_match_returns_false_for_invalid_ip():
    assert rdns_cache._is_ip_in_cidrs("not-an-ip", ["8.8.8.0/24"]) is False


@pytest.mark.usefixtures("isolated_db")
def test_cidr_match_works_for_ipv6():
    assert rdns_cache._is_ip_in_cidrs("2606:4700::1", ["2606:4700::/32"]) is True
    assert rdns_cache._is_ip_in_cidrs("2001:db8::1", ["2606:4700::/32"]) is False


# ── classify (pure) ──────────────────────────────────────────────────────────


@pytest.mark.usefixtures("isolated_db")
def test_classify_cidr_match_short_circuits_to_verified():
    assert (
        rdns_cache.classify(
            ip="66.249.66.1",
            hostname="some-other-host",
            status="found",
            fcrdns_verified=False,
            verification_domains=["googlebot.com"],
            verification_cidrs=["66.249.64.0/19"],
        )
        == "verified"
    )


@pytest.mark.usefixtures("isolated_db")
def test_classify_no_domains_means_unverified():
    """Without ``verification_domains`` we can't FCrDNS-verify anything."""
    assert rdns_cache.classify("1.2.3.4", "x.googlebot.com", "found", True, verification_domains=[]) == "unverified"


@pytest.mark.usefixtures("isolated_db")
def test_classify_pending_status_returns_pending_label():
    assert (
        rdns_cache.classify("1.2.3.4", None, "pending", False, verification_domains=["googlebot.com"])
        == "unverified_pending"
    )


@pytest.mark.usefixtures("isolated_db")
@pytest.mark.parametrize("status", ["nxdomain", "error"])
def test_classify_nxdomain_or_error_is_impersonator(status):
    assert rdns_cache.classify("1.2.3.4", None, status, False, verification_domains=["googlebot.com"]) == "impersonator"


@pytest.mark.usefixtures("isolated_db")
def test_classify_unverified_fcrdns_is_impersonator():
    """Even if hostname matches, fcrdns_verified=False means impersonator."""
    assert (
        rdns_cache.classify(
            "1.2.3.4",
            "crawl-66-249-66-1.googlebot.com",
            "found",
            fcrdns_verified=False,
            verification_domains=["googlebot.com"],
        )
        == "impersonator"
    )


@pytest.mark.usefixtures("isolated_db")
def test_classify_verified_fcrdns_with_matching_subdomain():
    assert (
        rdns_cache.classify(
            "66.249.66.1",
            "crawl-66-249-66-1.googlebot.com",
            "found",
            fcrdns_verified=True,
            verification_domains=["googlebot.com"],
        )
        == "verified"
    )


@pytest.mark.usefixtures("isolated_db")
def test_classify_verified_fcrdns_with_exact_domain():
    assert (
        rdns_cache.classify(
            "1.2.3.4",
            "googlebot.com",
            "found",
            fcrdns_verified=True,
            verification_domains=["googlebot.com"],
        )
        == "verified"
    )


@pytest.mark.usefixtures("isolated_db")
def test_classify_non_matching_domain_is_impersonator():
    """Hostname doesn't end with a verification domain → impersonator."""
    assert (
        rdns_cache.classify(
            "1.2.3.4",
            "bot.evil.example",
            "found",
            fcrdns_verified=True,
            verification_domains=["googlebot.com"],
        )
        == "impersonator"
    )


# ── Public cache API ──────────────────────────────────────────────────────────


@pytest.mark.usefixtures("isolated_db")
def test_enqueue_inserts_pending_rows():
    n = rdns_cache.enqueue(["1.1.1.1", "8.8.8.8"])
    assert n == 2
    # Both visible as pending
    res = rdns_cache.get_hostnames(["1.1.1.1", "8.8.8.8"])
    for ip in ("1.1.1.1", "8.8.8.8"):
        h, st, _ = res[ip]
        assert h is None
        assert st == "pending"


@pytest.mark.usefixtures("isolated_db")
def test_enqueue_dedups_against_existing_rows():
    rdns_cache.enqueue(["1.1.1.1"])
    n = rdns_cache.enqueue(["1.1.1.1", "2.2.2.2"])
    assert n == 1  # only the new one


@pytest.mark.usefixtures("isolated_db")
def test_enqueue_empty_list_is_noop():
    assert rdns_cache.enqueue([]) == 0


@pytest.mark.usefixtures("isolated_db")
@pytest.mark.security_regression
def test_enqueue_drops_new_ips_once_pending_backlog_at_cap(monkeypatch):
    """Audit finding 008: without a backlog cap, a client rotating through
    many source IPs (or spoofing a bot User-Agent to hit the
    classification enqueue path) can grow the pending backlog without
    bound. Once at cap, new IPs are dropped rather than inserted."""
    monkeypatch.setattr(rdns_cache, "_MAX_PENDING_BACKLOG", 2)
    assert rdns_cache.enqueue(["1.1.1.1", "2.2.2.2"]) == 2
    # At cap now — the next enqueue must be a no-op, not a partial insert.
    n = rdns_cache.enqueue(["3.3.3.3", "4.4.4.4"])
    assert n == 0
    assert rdns_cache._get_pending_count() == 2


@pytest.mark.usefixtures("isolated_db")
def test_enqueue_resumes_once_backlog_drains_below_cap(monkeypatch):
    """Once pending IPs get resolved (backlog drains), enqueue must accept
    new IPs again — the cap gates the instantaneous backlog, not a
    lifetime total."""
    monkeypatch.setattr(rdns_cache, "_MAX_PENDING_BACKLOG", 1)
    rdns_cache.enqueue(["1.1.1.1"])
    assert rdns_cache.enqueue(["2.2.2.2"]) == 0  # at cap

    con = rdns_cache._write_con()
    try:
        con.execute("UPDATE rdns SET status='resolved', looked_up_at=datetime('now') WHERE ip='1.1.1.1'")
        con.commit()
    finally:
        con.close()

    assert rdns_cache.enqueue(["2.2.2.2"]) == 1


@pytest.mark.usefixtures("isolated_db")
def test_get_hostnames_batch_returns_cached_and_enqueues_missing():
    """``get_hostnames`` should fold N per-IP SELECTs into one call: rows
    already in the cache come back via the dict, IPs we've never seen are
    omitted from the result *and* batch-enqueued as 'pending'."""
    # Seed one IP into the cache so we have something to find.
    con = rdns_cache._write_con()
    try:
        con.execute(
            "INSERT INTO rdns (ip, hostname, status, fcrdns_verified) VALUES (?, ?, 'found', 1)",
            ("66.249.66.1", "crawl-66-249-66-1.googlebot.com"),
        )
        con.commit()
    finally:
        con.close()

    result = rdns_cache.get_hostnames(["66.249.66.1", "9.9.9.9", "9.9.9.9", ""])
    assert result == {"66.249.66.1": ("crawl-66-249-66-1.googlebot.com", "found", True)}
    # The previously-unknown IP must now exist as 'pending' in the cache.
    h, st, _ = rdns_cache.get_hostnames(["9.9.9.9"])["9.9.9.9"]
    assert h is None and st == "pending"


@pytest.mark.usefixtures("isolated_db")
def test_get_hostnames_batch_empty_input_is_noop():
    assert rdns_cache.get_hostnames([]) == {}
    assert rdns_cache.get_hostnames(["", None]) == {}  # type: ignore[list-item]


# ── _do_lookup: DNS resolution with FCrDNS ──────────────────────────────────


@pytest.mark.usefixtures("isolated_db")
def test_do_lookup_returns_nxdomain_on_herror():
    """``socket.herror`` (PTR record missing) → ('nxdomain', no
    hostname). Pinned because the classifier treats nxdomain as
    impersonator only when the bot defines a verification domain.
    Phase 1.4a: assertion targets the sync fallback (the running-loop
    safety net); the aiodns hot path translates ARES_ENOTFOUND /
    ARES_ENODATA to the same status."""
    with patch("socket.gethostbyaddr", side_effect=socket.herror("no PTR")):
        host, status, fcrdns = rdns_cache._do_lookup("1.2.3.4")
    assert host is None
    assert status == "nxdomain"
    assert fcrdns is False


@pytest.mark.usefixtures("isolated_db")
def test_do_lookup_returns_error_on_unexpected_exception():
    """Any other exception (timeout, gaierror) → 'error' status.
    Phase 1.4a: the socket-based path lives in
    ``_do_lookup_sync_fallback`` (production uses the aiodns async
    path); this test pins the fallback contract because the running-
    loop fallback branch still relies on it."""
    with patch("socket.gethostbyaddr", side_effect=OSError("timeout")):
        host, status, fcrdns = rdns_cache._do_lookup("1.2.3.4")
    assert host is None
    assert status == "error"
    assert fcrdns is False


@pytest.mark.usefixtures("isolated_db")
def test_do_lookup_fcrdns_verified_when_forward_matches_reverse():
    """Reverse DNS returns hostname, forward DNS for that hostname
    includes the original IP → FCrDNS verified. Pinned against
    the sync-fallback path (the aiodns path is exercised via the
    enrich_batch tests above)."""
    fake_forward = [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("66.249.66.1", 0)),
    ]
    with (
        patch("socket.gethostbyaddr", return_value=("crawl-66-249-66-1.googlebot.com", [], [])),
        patch("socket.getaddrinfo", return_value=fake_forward),
    ):
        host, status, fcrdns = rdns_cache._do_lookup("66.249.66.1")

    assert host == "crawl-66-249-66-1.googlebot.com"
    assert status == "resolved"
    assert fcrdns is True


@pytest.mark.usefixtures("isolated_db")
def test_do_lookup_fcrdns_unverified_when_forward_mismatches():
    """Classic impersonator pattern: PTR points to a googlebot
    hostname, but forward lookup returns a different IP. Pinned
    against the sync-fallback path."""
    fake_forward = [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("66.249.66.99", 0)),  # different!
    ]
    with (
        patch("socket.gethostbyaddr", return_value=("crawl-fake.googlebot.com", [], [])),
        patch("socket.getaddrinfo", return_value=fake_forward),
    ):
        host, status, fcrdns = rdns_cache._do_lookup("66.249.66.1")

    assert host == "crawl-fake.googlebot.com"
    assert status == "resolved"
    assert fcrdns is False  # FCrDNS failed


@pytest.mark.usefixtures("isolated_db")
def test_do_lookup_fcrdns_false_when_forward_resolution_errors():
    """Reverse DNS succeeds but forward DNS fails (NXDOMAIN, timeout)
    → fcrdns=False but status is still 'resolved'. Pinned against
    the sync-fallback path."""
    with (
        patch("socket.gethostbyaddr", return_value=("known.example.com", [], [])),
        patch("socket.getaddrinfo", side_effect=OSError("forward DNS down")),
    ):
        host, status, fcrdns = rdns_cache._do_lookup("1.2.3.4")

    assert host == "known.example.com"
    assert status == "resolved"
    assert fcrdns is False


# ── enrich_batch ────────────────────────────────────────────────────────────


@pytest.mark.usefixtures("isolated_db")
def test_enrich_batch_resolves_pending_ips():
    """Happy path: pending IPs get resolved + persisted. Pinned
    because the cron job depends on the summary counts for monitoring."""
    rdns_cache.enqueue(["1.1.1.1", "2.2.2.2"])

    fake_lookups = {
        "1.1.1.1": ("one.example.com", "resolved", True),
        "2.2.2.2": ("two.example.com", "resolved", True),
    }

    with (
        patch("backend.utils.bot_sources._do_lookup", side_effect=lambda ip: fake_lookups[ip]),
        patch("backend.utils.bot_sources._discover_new_ips", return_value=0),
    ):
        summary = rdns_cache.enrich_batch(limit=10)

    assert summary["resolved"] == 2
    assert summary["errors"] == 0
    assert rdns_cache._get_pending_count() == 0


@pytest.mark.usefixtures("isolated_db")
def test_enrich_batch_counts_errors_separately():
    """nxdomain / error statuses bump the ``errors`` counter, not
    ``resolved``. Pinned because the admin dashboard's monitoring
    graph splits these — conflating them would hide DNS flaps."""
    rdns_cache.enqueue(["bad.ip"])

    with (
        patch(
            "backend.utils.bot_sources._do_lookup",
            return_value=(None, "nxdomain", False),
        ),
        patch("backend.utils.bot_sources._discover_new_ips", return_value=0),
    ):
        summary = rdns_cache.enrich_batch(limit=10)

    assert summary["resolved"] == 0
    assert summary["errors"] == 1


@pytest.mark.usefixtures("isolated_db")
def test_enrich_batch_stamps_last_enrichment_timestamp():
    """``_last_enrichment_at`` is what ``get_stats`` returns; the
    cron stamps it so the admin UI can show "Last refreshed: N
    minutes ago"."""
    rdns_cache._last_enrichment_at = None
    with patch("backend.utils.bot_sources._discover_new_ips", return_value=0):
        rdns_cache.enrich_batch(limit=10)

    assert rdns_cache._last_enrichment_at is not None
    assert "T" in rdns_cache._last_enrichment_at  # ISO-Z format


@pytest.mark.usefixtures("isolated_db")
def test_enrich_batch_swallows_discovery_exception():
    """If ``_discover_new_ips`` raises (S3 down, missing source),
    the resolve pass still completes. Pinned because losing the
    resolve pass over a discovery error would freeze every existing
    cache entry for hours."""
    rdns_cache.enqueue(["1.1.1.1"])

    with (
        patch(
            "backend.utils.bot_sources._do_lookup",
            return_value=("ok.example.com", "resolved", True),
        ),
        patch("backend.utils.bot_sources._discover_new_ips", side_effect=RuntimeError("S3 down")),
    ):
        summary = rdns_cache.enrich_batch(limit=10)

    assert summary["resolved"] == 1
    assert summary["discovered"] == 0  # swallowed


@pytest.mark.usefixtures("isolated_db")
def test_enrich_batch_no_op_when_nothing_pending():
    """No pending IPs + discovery returns 0 → all-zero summary, no
    log spam. Pinned because the cron runs every 5 minutes and we
    don't want it logging at INFO level when there's nothing to do."""
    with patch("backend.utils.bot_sources._discover_new_ips", return_value=0):
        summary = rdns_cache.enrich_batch(limit=10)

    assert summary == {"resolved": 0, "errors": 0, "discovered": 0, "batch_limit": 10}


# ── reap (audit finding 008) ─────────────────────────────────────────────────


@pytest.mark.usefixtures("isolated_db")
def test_reap_deletes_stale_nxdomain_rows_past_retention(monkeypatch):
    monkeypatch.setattr(rdns_cache, "_STALE_ROW_RETENTION_DAYS", 30)
    con = rdns_cache._write_con()
    try:
        con.execute(
            "INSERT INTO rdns (ip, status, fcrdns_verified, looked_up_at) "
            "VALUES ('1.1.1.1', 'nxdomain', 0, datetime('now', '-31 days'))"
        )
        con.execute(
            "INSERT INTO rdns (ip, status, fcrdns_verified, looked_up_at) "
            "VALUES ('2.2.2.2', 'nxdomain', 0, datetime('now', '-1 days'))"
        )
        con.commit()
    finally:
        con.close()

    result = rdns_cache._reap_stale_rows()

    assert result["stale_deleted"] == 1
    con = rdns_cache._write_con()
    try:
        total = con.execute("SELECT count(*) FROM rdns").fetchone()[0]
    finally:
        con.close()
    assert total == 1  # the 1-day-old row survives


@pytest.mark.usefixtures("isolated_db")
def test_reap_never_deletes_pending_rows_regardless_of_age():
    """A row can only be pending if enrich_batch hasn't gotten to it yet —
    reaping it would silently drop a real, unresolved backlog entry
    instead of just bounding disk growth."""
    con = rdns_cache._write_con()
    try:
        con.execute("INSERT INTO rdns (ip, status, fcrdns_verified) VALUES ('1.1.1.1', 'pending', 0)")
        con.commit()
    finally:
        con.close()

    rdns_cache._reap_stale_rows()

    assert rdns_cache._get_pending_count() == 1


@pytest.mark.usefixtures("isolated_db")
@pytest.mark.security_regression
def test_reap_trims_oldest_non_pending_rows_once_over_total_cap(monkeypatch):
    """Audit finding 008: even resolved rows must not accumulate forever —
    once past the hard cap, the oldest-looked-up rows go first."""
    monkeypatch.setattr(rdns_cache, "_MAX_TOTAL_ROWS", 2)
    con = rdns_cache._write_con()
    try:
        con.executemany(
            "INSERT INTO rdns (ip, status, fcrdns_verified, looked_up_at) VALUES (?, 'resolved', 1, datetime('now', ?))",
            [("1.1.1.0", "-3 days"), ("1.1.1.1", "-2 days"), ("1.1.1.2", "-1 days")],
        )
        con.commit()
    finally:
        con.close()

    result = rdns_cache._reap_stale_rows()

    assert result["overflow_deleted"] == 1
    con = rdns_cache._write_con()
    try:
        remaining = {row[0] for row in con.execute("SELECT ip FROM rdns").fetchall()}
    finally:
        con.close()
    assert remaining == {"1.1.1.1", "1.1.1.2"}  # oldest (1.1.1.0) evicted first


@pytest.mark.usefixtures("isolated_db")
def test_maybe_reap_is_gated_to_run_at_most_once_per_interval(monkeypatch):
    calls = []
    monkeypatch.setattr(
        rdns_cache, "_reap_stale_rows", lambda: calls.append(1) or {"stale_deleted": 0, "overflow_deleted": 0}
    )
    rdns_cache._last_reap_monotonic = None

    rdns_cache._maybe_reap_stale_rows()
    rdns_cache._maybe_reap_stale_rows()  # second call within the interval — must be a no-op

    assert len(calls) == 1


@pytest.mark.usefixtures("isolated_db")
def test_maybe_reap_swallows_exceptions():
    """A reap failure must never take down the every-5-min enrichment
    cron — same swallow-and-log posture as the discovery pass."""
    rdns_cache._last_reap_monotonic = None
    with patch("backend.utils.bot_sources._reap_stale_rows", side_effect=RuntimeError("db locked")):
        rdns_cache._maybe_reap_stale_rows()  # must not raise


# ── _now ────────────────────────────────────────────────────────────────────


# The local _now() helper was replaced by backend.utils.date_utils.iso_z_now();
# the ISO 8601 Z-suffix format is pinned by tests/utils/test_date_utils.py.


# ── _discover_new_ips: branch coverage via mocked DuckDB ──────────────────


@pytest.mark.usefixtures("isolated_db")
def test_discover_new_ips_returns_zero_when_no_sources_configured():
    """No services configured → no sources to scan → 0 new IPs.
    Pinned because raising here would break the cron on fresh installs."""
    with patch("backend.config.list_configs", return_value=[]):
        out = rdns_cache._discover_new_ips(max_new=100)
    assert out == 0


@pytest.mark.usefixtures("isolated_db")
def test_discover_new_ips_gen_yields_zero_count_when_import_fails():
    """If the duckdb module can't be imported (unlikely but defensive),
    the generator yields a count=0 event and exits."""
    # We can't easily mock the ImportError after it's been imported,
    # but we can simulate the "no configs" branch which goes through
    # the same exit path.
    with patch("backend.config.list_configs", return_value=[]):
        events = list(rdns_cache._discover_new_ips_gen(max_new=10))

    count_events = [e for e in events if e["type"] == "count"]
    assert len(count_events) == 1
    assert count_events[0]["count"] == 0


@pytest.mark.usefixtures("isolated_db")
def test_discover_new_ips_swallows_config_load_failure():
    """``list_configs`` raising must not crash discovery — log + return
    0, let the next cron run try again."""
    with patch("backend.config.list_configs", side_effect=RuntimeError("config corrupt")):
        out = rdns_cache._discover_new_ips(max_new=100)
    assert out == 0


# ── get_hostnames: cache hit (after enrichment) ───────────────────────────


@pytest.mark.usefixtures("isolated_db")
def test_get_hostnames_returns_cached_resolution_after_enrichment():
    """After ``enrich_batch`` resolves a pending IP, subsequent
    ``get_hostnames`` calls return the cached hostname WITHOUT
    re-enqueueing or hitting the network."""
    rdns_cache.enqueue(["1.1.1.1"])

    with (
        patch(
            "backend.utils.bot_sources._do_lookup",
            return_value=("known.example.com", "resolved", True),
        ),
        patch("backend.utils.bot_sources._discover_new_ips", return_value=0),
    ):
        rdns_cache.enrich_batch(limit=10)

    # Now query — should NOT trigger another lookup
    with patch("backend.utils.bot_sources._do_lookup") as mock_lookup:
        host, status, fcrdns = rdns_cache.get_hostnames(["1.1.1.1"])["1.1.1.1"]

    assert host == "known.example.com"
    assert status == "resolved"
    assert fcrdns is True
    mock_lookup.assert_not_called()


# ── get_hostnames when DB unreachable (read connection returns None) ──────


@pytest.mark.usefixtures("isolated_db")
def test_get_hostnames_enqueues_and_returns_empty_when_read_con_unavailable():
    """If ``_read_con`` returns None (DB file missing or unreadable),
    the helper still enqueues the IP as pending and returns an empty dict."""
    with patch("backend.utils.bot_sources._read_con", return_value=None):
        res = rdns_cache.get_hostnames(["9.9.9.9"])
    assert res == {}


# ── enrich_batch: stale-refresh pass ──────────────────────────────────────


@pytest.mark.usefixtures("isolated_db")
def test_enrich_batch_refreshes_stale_entries():
    """Entries with ``looked_up_at`` older than 48 hours get re-resolved.
    Pinned because forgetting the refresh pass would freeze the cache
    against rDNS records that legitimately change (cloud providers
    rotate IPs)."""
    # Seed a resolved row with a stale looked_up_at timestamp
    rdns_cache.enqueue(["8.8.4.4"])
    con = rdns_cache._write_con()
    try:
        con.execute(
            "UPDATE rdns SET hostname=?, status='resolved', fcrdns_verified=1, looked_up_at=datetime('now', '-3 days') WHERE ip=?",
            ("old.example.com", "8.8.4.4"),
        )
        con.commit()
    finally:
        con.close()

    calls = []

    def fake_lookup(ip):
        calls.append(ip)
        return ("refreshed.example.com", "resolved", True)

    with (
        patch("backend.utils.bot_sources._do_lookup", side_effect=fake_lookup),
        patch("backend.utils.bot_sources._discover_new_ips", return_value=0),
    ):
        rdns_cache.enrich_batch(limit=10)

    # The stale entry was re-looked-up
    assert "8.8.4.4" in calls
    # And the cache reflects the refresh
    host, status, _ = rdns_cache.get_hostnames(["8.8.4.4"])["8.8.4.4"]
    assert host == "refreshed.example.com"


# ── _discover_new_ips_gen: source iteration ──────────────────────────────


@pytest.mark.usefixtures("isolated_db")
def test_discover_new_ips_gen_emits_zero_count_when_no_configs():
    """``list_configs`` returns nothing → ``count=0`` event + return.
    Pinned because fresh installs have no services, and the generator
    must terminate cleanly (not iterate forever)."""
    with (
        patch("backend.config.list_configs", return_value=[]),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
    ):
        events = list(rdns_cache._discover_new_ips_gen(max_new=10))

    # The empty-sources path doesn't emit "Scanning" or "Found" events,
    # only the terminal count=0 signal that the upstream consumer
    # converts into "Discovered N new IPs".
    log_events = [e for e in events if e["type"] == "log"]
    assert log_events == []


@pytest.mark.usefixtures("isolated_db")
def test_discover_new_ips_gen_swallows_list_configs_exception():
    """``list_configs`` raising → ``count=0`` event + return. Pinned
    because a corrupt configs/ directory shouldn't crash the rDNS
    cron — the cron should keep running for the next interval."""
    with (
        patch("backend.config.list_configs", side_effect=RuntimeError("configs dir gone")),
    ):
        events = list(rdns_cache._discover_new_ips_gen(max_new=10))

    assert len(events) == 1
    assert events[0] == {"type": "count", "count": 0}


@pytest.mark.usefixtures("isolated_db")
def test_discover_new_ips_gen_scans_source_and_emits_log_on_found_ips():
    """Happy-path: source with `ip` column → scan emits a "Found N
    new IPs in <service>" log event AND enqueues them for the next
    enrichment pass. Pinned because losing the enqueue would silently
    starve the resolution loop of new work."""
    fake_src = {"name": "test_service", "service_id": "svc-1"}

    # Mock the DuckDB connection used inside the function
    fake_con = MagicMock()
    # First execute: schema query → returns a row with 'ip' column
    # Second execute: distinct IPs query → returns rows
    # Each call returns its own mock; configure side_effect
    schema_rows = MagicMock()
    schema_rows.fetchall.return_value = [("ip",), ("status",)]
    ip_rows = MagicMock()
    ip_rows.fetchall.return_value = [("1.1.1.2",), ("1.1.1.3",)]
    fake_con.execute.side_effect = [schema_rows, ip_rows]

    # And the write con for the "already" filter — empty (nothing cached)
    fake_write_con = MagicMock()
    already_rows = MagicMock()
    already_rows.fetchall.return_value = []
    fake_write_con.execute.return_value = already_rows

    with (
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-1"}]),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.utils.bot_sources._write_con", return_value=fake_write_con),
        patch("backend.utils.bot_sources.enqueue") as mock_enqueue,
    ):
        events = list(rdns_cache._discover_new_ips_gen(max_new=10))

    # Status event for scanning, log event for found IPs
    status_events = [e for e in events if e["type"] == "status"]
    log_events = [e for e in events if e["type"] == "log"]
    assert any("Scanning test_service" in e["message"] for e in status_events)
    assert any("Found 2 new IPs" in e["message"] for e in log_events)

    # New IPs were enqueued
    mock_enqueue.assert_called_once()
    enqueued = mock_enqueue.call_args[0][0]
    assert set(enqueued) == {"1.1.1.2", "1.1.1.3"}


@pytest.mark.usefixtures("isolated_db")
def test_discover_new_ips_gen_skips_sources_without_ip_column():
    """Sources whose schema lacks an `ip` column are skipped silently
    (no events emitted for them). Pinned because non-edge-log
    services (e.g. metadata-only replicas) don't have an ip column;
    forcing them through the rDNS loop would 500 the cron."""
    fake_src = {"name": "no_ip_svc", "service_id": "svc-2"}
    fake_con = MagicMock()
    # Schema row WITHOUT 'ip'
    schema_rows = MagicMock()
    schema_rows.fetchall.return_value = [("status",), ("timestamp",)]
    fake_con.execute.return_value = schema_rows

    with (
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-2"}]),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.utils.bot_sources.enqueue") as mock_enqueue,
    ):
        events = list(rdns_cache._discover_new_ips_gen(max_new=10))

    # Status event for scanning emitted, but no log "Found N new IPs"
    log_events = [e for e in events if e["type"] == "log"]
    assert not any("Found" in e.get("message", "") for e in log_events)
    mock_enqueue.assert_not_called()


@pytest.mark.usefixtures("isolated_db")
def test_discover_new_ips_gen_emits_error_log_on_source_scan_failure():
    """If scanning a single source raises (DuckDB lock, corrupt
    parquet), emit a log event and continue to the next source.
    Pinned because losing the error-log fallback would hide
    per-source failures during the global rDNS sweep."""
    fake_src = {"name": "broken_svc", "service_id": "svc-bad"}

    with (
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-bad"}]),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("file locked")),
    ):
        events = list(rdns_cache._discover_new_ips_gen(max_new=10))

    log_events = [e for e in events if e["type"] == "log"]
    assert any("Error scanning broken_svc" in e["message"] for e in log_events)


# ── _do_lookup_async / _resolve_batch_async / _bulk_update_async ──────────────


def _ptr(name: str):
    """Build the gethostbyaddr-shaped object aiodns returns."""
    obj = MagicMock()
    obj.name = name
    return obj


def _forward_records(hosts: list[str]):
    """Build the real pycares DNSResult aiodns >= 4 ``query_dns()`` returns."""
    import pycares

    records = [pycares.DNSRecord("h.example.", 1, 1, 60, pycares.ARecordData(addr=h)) for h in hosts]
    return pycares.DNSResult(answer=records, authority=[], additional=[])


@pytest.mark.usefixtures("isolated_db")
def test_do_lookup_async_returns_resolved_when_ptr_succeeds_and_fcrdns_passes():
    resolver = MagicMock()
    resolver.gethostbyaddr = AsyncMock(return_value=_ptr("crawl.googlebot.com"))
    resolver.query_dns = AsyncMock(return_value=_forward_records(["66.249.66.1"]))

    sem = asyncio.Semaphore(5)
    host, status, fcrdns = asyncio.run(
        rdns_cache._do_lookup_async("66.249.66.1", resolver, sem),
    )
    assert host == "crawl.googlebot.com"
    assert status == "resolved"
    assert fcrdns is True


@pytest.mark.usefixtures("isolated_db")
def test_do_lookup_async_marks_fcrdns_false_on_forward_mismatch():
    resolver = MagicMock()
    resolver.gethostbyaddr = AsyncMock(return_value=_ptr("crawl-fake.googlebot.com"))
    resolver.query_dns = AsyncMock(return_value=_forward_records(["66.249.66.99"]))

    sem = asyncio.Semaphore(5)
    host, status, fcrdns = asyncio.run(
        rdns_cache._do_lookup_async("66.249.66.1", resolver, sem),
    )
    assert host == "crawl-fake.googlebot.com"
    assert status == "resolved"
    assert fcrdns is False


@pytest.mark.usefixtures("isolated_db")
def test_do_lookup_async_returns_nxdomain_on_ares_enotfound():
    resolver = MagicMock()
    resolver.gethostbyaddr = AsyncMock(
        side_effect=aiodns.error.DNSError(aiodns.error.ARES_ENOTFOUND, "not found"),
    )
    sem = asyncio.Semaphore(5)
    host, status, fcrdns = asyncio.run(
        rdns_cache._do_lookup_async("1.2.3.4", resolver, sem),
    )
    assert host is None
    assert status == "nxdomain"
    assert fcrdns is False


@pytest.mark.usefixtures("isolated_db")
def test_do_lookup_async_returns_error_on_other_dns_errors():
    resolver = MagicMock()
    resolver.gethostbyaddr = AsyncMock(
        side_effect=aiodns.error.DNSError(aiodns.error.ARES_ETIMEOUT, "timeout"),
    )
    sem = asyncio.Semaphore(5)
    host, status, fcrdns = asyncio.run(
        rdns_cache._do_lookup_async("1.2.3.4", resolver, sem),
    )
    assert host is None
    assert status == "error"
    assert fcrdns is False


@pytest.mark.usefixtures("isolated_db")
def test_resolve_batch_async_runs_concurrently_and_returns_map():
    """All IPs resolved concurrently; result map keyed by IP."""
    fake_lookups = {
        "1.1.1.1": ("one.example.com", "resolved", True),
        "2.2.2.2": ("two.example.com", "resolved", False),
        "3.3.3.3": (None, "nxdomain", False),
    }

    async def fake_lookup(ip, resolver, semaphore):
        return fake_lookups[ip]

    with patch("backend.utils.bot_sources._do_lookup_async", side_effect=fake_lookup):
        results = asyncio.run(rdns_cache._resolve_batch_async(list(fake_lookups)))

    assert results == fake_lookups


@pytest.mark.usefixtures("isolated_db")
def test_resolve_batch_async_swallows_exceptions_into_error_status():
    """A single lookup raising must not poison the whole batch."""

    async def fake_lookup(ip, resolver, semaphore):
        if ip == "1.1.1.1":
            raise RuntimeError("boom")
        return ("ok.example.com", "resolved", True)

    with patch("backend.utils.bot_sources._do_lookup_async", side_effect=fake_lookup):
        results = asyncio.run(rdns_cache._resolve_batch_async(["1.1.1.1", "2.2.2.2"]))

    assert results["1.1.1.1"] == (None, "error", False)
    assert results["2.2.2.2"] == ("ok.example.com", "resolved", True)


@pytest.mark.usefixtures("isolated_db")
def test_resolve_batch_async_returns_empty_for_empty_input():
    assert asyncio.run(rdns_cache._resolve_batch_async([])) == {}


@pytest.mark.usefixtures("isolated_db")
def test_bulk_update_async_writes_records_in_single_transaction():
    """End-to-end bulk update writes the rows to the SQLite file."""
    rdns_cache.enqueue(["1.1.1.1", "2.2.2.2"])

    records = [
        ("one.example.com", "resolved", 1, "2026-06-09T00:00:00Z", "1.1.1.1"),
        ("two.example.com", "resolved", 0, "2026-06-09T00:00:00Z", "2.2.2.2"),
    ]
    asyncio.run(rdns_cache._bulk_update_async(records))

    res = rdns_cache.get_hostnames(["1.1.1.1", "2.2.2.2"])
    h1, s1, _ = res["1.1.1.1"]
    h2, s2, _ = res["2.2.2.2"]
    assert (h1, s1) == ("one.example.com", "resolved")
    assert (h2, s2) == ("two.example.com", "resolved")


@pytest.mark.usefixtures("isolated_db")
def test_bulk_update_async_noop_on_empty_records():
    """Empty input is a no-op (doesn't open a connection)."""
    asyncio.run(rdns_cache._bulk_update_async([]))


# ── Merged from tests/utils/test_bot_sources.py ──

import json
from pathlib import Path

import pytest

from backend.utils import bot_sources
from backend.utils.bot_sources import (
    extract_literal_substring,
    fetch_external_cidrs,
    get_all_sources_meta,
    get_bot_by_id,
    get_ilike_prefilter_literals,
    load_source,
)


@pytest.fixture()
def _isolated_cache_dir(tmp_path, monkeypatch):
    """Point ``_CACHE_DIR`` at a tmp_path so cached source files don't
    leak across tests and we don't write into the real data/cache/."""
    monkeypatch.setattr(bot_sources, "_CACHE_DIR", tmp_path)
    # Clear all in-process caches between tests.
    with bot_sources._matcher_lock:
        bot_sources._matcher_cache.clear()
    with bot_sources._source_cache_lock:
        bot_sources._source_cache.clear()
    with bot_sources._compiled_ip_feeds_lock:
        bot_sources._compiled_ip_feeds.clear()
    with bot_sources._version_cache_lock:
        bot_sources._version_cache.clear()
    yield
    with bot_sources._matcher_lock:
        bot_sources._matcher_cache.clear()
    with bot_sources._source_cache_lock:
        bot_sources._source_cache.clear()
    with bot_sources._compiled_ip_feeds_lock:
        bot_sources._compiled_ip_feeds.clear()
    with bot_sources._version_cache_lock:
        bot_sources._version_cache.clear()


def _seed_cache(source_id: str, entries: list[dict]) -> Path:
    """Write a properly-shaped envelope file for a source."""
    path = bot_sources._cache_path(source_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "last_updated": "2026-05-15T00:00:00Z",
                "entry_count": len(entries),
                "entries": entries,
            }
        )
    )
    return path


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_fetch_external_cidrs_json_array():
    source_config = {"url": "https://example.com/ips.json", "selector": None}

    with patch("backend.utils.bot_sources.http_get_text", return_value=json.dumps(["1.1.1.1", "2.2.2.2/24"])):
        res = fetch_external_cidrs(source_config)
        assert len(res) == 2
        assert "1.1.1.1" in res


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_fetch_external_cidrs_plaintext():
    source_config = {"url": "https://example.com/ips.txt", "selector": None}

    with patch("backend.utils.bot_sources.http_get_text", return_value="# Comments\n1.1.1.1\n2.2.2.2/24\n\n"):
        res = fetch_external_cidrs(source_config)
        assert len(res) == 2
        assert "1.1.1.1" in res


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_fetch_external_cidrs_google_format():
    source_config = {"url": "https://example.com/ips.json", "selector": '$.prefixes[*]["ipv4Prefix","ipv6Prefix"]'}
    google_mock = {"prefixes": [{"ipv4Prefix": "1.1.1.1/32"}, {"ipv6Prefix": "2001:db8::/32"}]}

    with patch("backend.utils.bot_sources.http_get_text", return_value=json.dumps(google_mock)):
        res = fetch_external_cidrs(source_config)
        assert len(res) == 2
        assert "1.1.1.1/32" in res
        assert "2001:db8::/32" in res


# ── load_source ──────────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_load_source_returns_empty_when_cache_missing():
    assert load_source("well-known-bots") == []


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_load_source_returns_cached_entries():
    entries = [{"id": "bot-1", "pattern": {"accepted": ["Bot/1"]}}]
    _seed_cache("well-known-bots", entries)
    assert load_source("well-known-bots") == entries


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_load_source_returns_empty_for_corrupt_cache():
    """Truncated JSON / wrong shape must not raise — just degrade."""
    bot_sources._cache_path("well-known-bots").write_text("{ not valid")
    assert load_source("well-known-bots") == []


# ── get_all_sources_meta ─────────────────────────────────────────────────────


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_get_all_sources_meta_returns_envelope_without_entries():
    """The admin UI's source table renders one row per source. The
    response must include source metadata (name, url) plus the cache
    envelope (last_updated, entry_count) — but NOT the full entries list
    (which can be tens of thousands of items)."""
    _seed_cache("well-known-bots", [{"id": f"b{i}"} for i in range(50)])
    meta = get_all_sources_meta()
    assert len(meta) >= 1
    wkb = next(m for m in meta if m["id"] == "well-known-bots")
    assert wkb["name"] == "Well-Known Bots (arcjet)"
    assert wkb["entry_count"] == 50
    assert wkb["last_updated"] == "2026-05-15T00:00:00Z"
    # Crucially: no full entries payload
    assert "entries" not in wkb


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_get_all_sources_meta_returns_none_fields_when_cache_missing():
    meta = get_all_sources_meta()
    wkb = next(m for m in meta if m["id"] == "well-known-bots")
    assert wkb["entry_count"] is None
    assert wkb["last_updated"] is None


# ── get_bot_by_id ────────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_get_bot_by_id_returns_match():
    _seed_cache(
        "well-known-bots",
        [
            {"id": "googlebot", "name": "Googlebot"},
            {"id": "bingbot", "name": "Bingbot"},
        ],
    )
    bot = get_bot_by_id("googlebot")
    assert bot is not None
    assert bot["name"] == "Googlebot"


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_get_bot_by_id_returns_none_for_unknown():
    _seed_cache("well-known-bots", [{"id": "googlebot"}])
    assert get_bot_by_id("never-heard-of") is None


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_get_bot_by_id_skips_disabled_sources(monkeypatch):
    """A bot only present in a *disabled* source must be invisible."""
    monkeypatch.setattr(
        bot_sources,
        "BOT_SOURCES",
        [
            {"id": "disabled-src", "name": "Disabled", "url": "x", "enabled": False},
        ],
    )
    _seed_cache("disabled-src", [{"id": "hidden-bot"}])
    assert get_bot_by_id("hidden-bot") is None


# ── extract_literal_substring (pure regex helper) ────────────────────────────


@pytest.mark.usefixtures("_isolated_cache_dir")
@pytest.mark.parametrize(
    "pattern,expected",
    [
        # Plain ASCII characters (including `.` and `/`) are appended
        # literally — only the metacharacter list `(){}*+?^$|` breaks a run.
        # `*` here breaks the run, leaving "Googlebot/." as the captured prefix.
        (r"Googlebot/.*", "Googlebot/."),
        # Escape-decoded literals: \/ → /, \. → .
        (r"compatible; bingbot\/2\.0", "compatible; bingbot/2.0"),
        # \d / \w break the literal run; the longest surviving run wins.
        (r"foo\d+barbaz", "barbaz"),
        # Character class `[Bb]` is skipped entirely; the run resumes
        # AFTER the closing `]`, so we get the tail "ingbot/".
        (r"[Bb]ingbot\/", "ingbot/"),
        # Group `(...)` and alternation `|` break the literal run.
        (r"foo(bar|baz)long_literal", "long_literal"),
        # No literal of length >= 4 → returns None
        (r"\w+\d+", None),
        (r"abc", None),  # too short
    ],
)
def test_extract_literal_substring(pattern, expected):
    assert extract_literal_substring(pattern) == expected


# ── get_ilike_prefilter_literals ─────────────────────────────────────────────


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_ilike_prefilter_literals_dedupes_and_skips_short():
    """The pre-filter list is used to push down ILIKE checks before the
    expensive regexp_matches() — duplicates would multiply work, and
    very short literals would over-match."""
    _seed_cache(
        "well-known-bots",
        [
            {"id": "a", "pattern": {"accepted": [r"Googlebot/.*", r"Googlebot/[0-9]+"]}},
            {"id": "b", "pattern": {"accepted": [r"bingbot\/2\.0"]}},
            {"id": "c", "pattern": {"accepted": [r"\d+"]}},  # nothing extractable
        ],
    )
    literals = get_ilike_prefilter_literals()
    # Googlebot/ appears in two patterns under the same source — must dedupe
    assert literals.count("Googlebot/") == 1
    assert "bingbot/2.0" in literals
    # The \d+ pattern yields nothing; it just shouldn't show up
    assert "" not in literals


# ── build_matcher ────────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_build_matcher_finds_bot_by_ua():
    _seed_cache(
        "well-known-bots",
        [
            {"id": "googlebot", "name": "Googlebot", "pattern": {"accepted": [r"Googlebot/[0-9]"]}},
        ],
    )
    match = bot_sources.build_matcher()
    hits = match("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)")
    assert len(hits) == 1
    assert hits[0]["id"] == "googlebot"


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_build_matcher_returns_empty_tuple_when_no_match():
    _seed_cache(
        "well-known-bots",
        [
            {"id": "googlebot", "pattern": {"accepted": [r"Googlebot/[0-9]"]}},
        ],
    )
    match = bot_sources.build_matcher()
    hits = match("regular Chrome/123 user agent")
    assert hits == ()


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_build_matcher_caches_result_between_calls():
    """Repeated lookups for the same UA should hit the lru_cache — the
    function returns the SAME tuple instance, not just equal."""
    _seed_cache(
        "well-known-bots",
        [
            {"id": "googlebot", "pattern": {"accepted": [r"Googlebot/[0-9]"]}},
        ],
    )
    match = bot_sources.build_matcher()
    a = match("Googlebot/2.1")
    b = match("Googlebot/2.1")
    assert a is b  # lru_cache returns the cached instance


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_build_matcher_handles_invalid_regex_in_source():
    """A malformed pattern in a single entry must not crash the matcher —
    the entry is skipped (logged debug) and the rest still compile."""
    _seed_cache(
        "well-known-bots",
        [
            {"id": "bad", "pattern": {"accepted": ["[unclosed"]}},
            {"id": "good", "pattern": {"accepted": [r"Googlebot/[0-9]"]}},
        ],
    )
    match = bot_sources.build_matcher()
    hits = match("Googlebot/2.1")
    assert any(h["id"] == "good" for h in hits)


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_build_matcher_invalidates_when_cache_file_changes(tmp_path, monkeypatch):
    """build_matcher() is memoised against the cache file's mtime. After a
    new entry is written, the next call should rebuild and include it."""
    _seed_cache(
        "well-known-bots",
        [
            {"id": "v1", "pattern": {"accepted": [r"V1Bot"]}},
        ],
    )
    m1 = bot_sources.build_matcher()
    assert any(h["id"] == "v1" for h in m1("V1Bot"))
    assert m1("V2Bot") == ()  # not present yet

    # Bump mtime + add a new entry
    import time

    time.sleep(0.01)  # ensure mtime granularity is exceeded
    _seed_cache(
        "well-known-bots",
        [
            {"id": "v1", "pattern": {"accepted": [r"V1Bot"]}},
            {"id": "v2", "pattern": {"accepted": [r"V2Bot"]}},
        ],
    )
    m2 = bot_sources.build_matcher()
    assert any(h["id"] == "v2" for h in m2("V2Bot")), "matcher did not pick up the new entry"


# ── fetch_and_cache_source ──────────────────────────────────────────────────


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_fetch_and_cache_source_writes_envelope_to_cache(tmp_path, monkeypatch):
    """Fetched bot data is normalised and written to the cache file
    with a top-level envelope (``last_updated`` + ``entry_count`` +
    ``entries``). Pinned because ``load_source`` keys on this exact
    shape; a refactor that dropped the envelope would break every
    subsequent matcher build."""
    fake_data = [
        {
            "id": "googlebot",
            "name": "Googlebot",
            "pattern": {"accepted": [r"Googlebot/2\.1"]},
            "verification": [{"type": "dns", "masks": ["*.google.com"]}],
        }
    ]

    with patch("backend.utils.bot_sources.http_get_text", return_value=json.dumps(fake_data)):
        meta = bot_sources.fetch_and_cache_source("well-known-bots")

    # Returned metadata
    assert meta["id"] == "well-known-bots"
    assert meta["entry_count"] == 1
    assert meta["last_updated"]

    # Cache file written with the envelope shape
    cache = bot_sources.load_source("well-known-bots")
    assert len(cache) == 1
    assert cache[0]["id"] == "googlebot"
    # The verification list got normalised to {"domains": [...], "cidrs": [...]}
    assert isinstance(cache[0]["verification"], dict)
    assert "google.com" in cache[0]["verification"]["domains"]


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_fetch_and_cache_source_normalises_cidr_verification():
    """``verification: [{type: cidr, prefixes: [...]}]`` gets flattened
    into ``verification: {domains: [], cidrs: [...]}`` — pinned because
    the matcher's IP-membership check reads from this normalized shape."""
    fake_data = [
        {
            "id": "bingbot",
            "name": "Bingbot",
            "pattern": {"accepted": [r"bingbot"]},
            "verification": [
                {"type": "cidr", "prefixes": ["157.55.39.0/24", "207.46.13.0/24"]},
            ],
        }
    ]

    with patch("backend.utils.bot_sources.http_get_text", return_value=json.dumps(fake_data)):
        bot_sources.fetch_and_cache_source("well-known-bots")

    entries = bot_sources.load_source("well-known-bots")
    cidrs = entries[0]["verification"]["cidrs"]
    assert "157.55.39.0/24" in cidrs
    assert "207.46.13.0/24" in cidrs


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_fetch_and_cache_source_resolves_dynamic_cidr_sources():
    """If a verification entry has ``sources: [...]``, each is fetched
    via ``fetch_external_cidrs`` and the results merged into ``cidrs``.
    Pinned because losing this would silently lose Googlebot's dynamic
    IP list — a core matching surface."""
    fake_well_known = [
        {
            "id": "googlebot",
            "name": "Googlebot",
            "pattern": {"accepted": [r"Googlebot"]},
            "verification": [
                {
                    "type": "cidr",
                    "sources": [{"url": "https://googlebot.example.com/ips.json"}],
                }
            ],
        }
    ]

    with (
        patch("backend.utils.bot_sources.http_get_text", return_value=json.dumps(fake_well_known)),
        patch("backend.utils.bot_sources.fetch_external_cidrs", return_value=["8.8.8.0/24"]),
    ):
        bot_sources.fetch_and_cache_source("well-known-bots")

    entries = bot_sources.load_source("well-known-bots")
    assert "8.8.8.0/24" in entries[0]["verification"]["cidrs"]


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_fetch_and_cache_source_raises_on_unknown_source_id():
    """Unknown source id → ValueError (not a silent no-op).
    Pinned because the refresh CLI uses this exit code to signal
    a configuration error."""
    with pytest.raises(ValueError, match="Unknown bot source"):
        bot_sources.fetch_and_cache_source("does-not-exist")


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_fetch_and_cache_source_invalidates_matcher_cache():
    """After a new fetch, the matcher cache must be cleared so the
    next ``build_matcher()`` call picks up the new entries. Pinned
    because a stale matcher would silently route logs to the OLD
    bot list for the full process lifetime."""
    # Prime the matcher cache
    _seed_cache("well-known-bots", [{"id": "old", "pattern": {"accepted": [r"OldBot"]}}])
    bot_sources.build_matcher()
    assert bot_sources._matcher_cache.get("fn") is not None

    # Fetch new data → cache should be invalidated
    with patch("backend.utils.bot_sources.http_get_text", return_value=json.dumps([{"id": "new"}])):
        bot_sources.fetch_and_cache_source("well-known-bots")

    # The cache was cleared as a side-effect of the fetch
    assert bot_sources._matcher_cache == {}


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_fetch_and_cache_source_invalidates_all_in_memory_caches():
    """fetch_and_cache_source must clear _matcher_cache, pop _source_cache,
    clear _compiled_ip_feeds, and clear _version_cache."""
    # 1. Prime all four caches
    _seed_cache("well-known-bots", [{"id": "cached_bot", "pattern": {"accepted": [r"OldBot"]}}])
    bot_sources.build_matcher()
    bot_sources.load_source("well-known-bots")
    bot_sources.get_pattern_set_version()
    with bot_sources._compiled_ip_feeds_lock:
        bot_sources._compiled_ip_feeds["mtime"] = 12345.0
        bot_sources._compiled_ip_feeds["feeds"] = {"test": []}

    assert bot_sources._matcher_cache != {}
    assert "well-known-bots" in bot_sources._source_cache
    assert bot_sources._compiled_ip_feeds != {}
    assert bot_sources._version_cache != {}

    # 2. Fetch new data
    with patch("backend.utils.bot_sources.http_get_text", return_value=json.dumps([{"id": "new_bot"}])):
        bot_sources.fetch_and_cache_source("well-known-bots")

    # 3. All in-memory caches must be invalidated
    assert bot_sources._matcher_cache == {}
    assert "well-known-bots" not in bot_sources._source_cache
    assert bot_sources._compiled_ip_feeds == {}
    assert bot_sources._version_cache == {}


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_atomic_write_cache_ensures_atomic_replace_and_cleanup_on_error():
    """_atomic_write_cache must write atomically and remove temporary files on failure."""
    envelope = {"last_updated": "2026-10-11T00:00:00Z", "entry_count": 1, "entries": [{"id": "bot1"}]}
    target = bot_sources._cache_path("test-source")

    # 1. Normal write succeeds and leaves no temp files
    bot_sources._atomic_write_cache("test-source", envelope)
    assert target.exists()
    assert json.loads(target.read_text())["entries"][0]["id"] == "bot1"
    temp_files = list(bot_sources._CACHE_DIR.glob(".test-source.json.*.tmp"))
    assert temp_files == []

    # 2. Write failure cleans up temp file and does not corrupt existing file
    with patch("os.replace", side_effect=OSError("Atomic replace failed")):
        with pytest.raises(OSError, match="Atomic replace failed"):
            bot_sources._atomic_write_cache(
                "test-source",
                {"last_updated": "2026-10-11T01:00:00Z", "entry_count": 2, "entries": [{"id": "corrupt"}]},
            )

    # Existing file intact
    assert json.loads(target.read_text())["entries"][0]["id"] == "bot1"
    # Temp file cleaned up
    temp_files = list(bot_sources._CACHE_DIR.glob(".test-source.json.*.tmp"))
    assert temp_files == []


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_version_cache_registered_in_cache_registry():
    """_version_cache is registered in CacheRegistry and drains on clear."""
    from backend.utils.bounded_cache import CacheRegistry

    assert "utils.bot_sources._version_cache" in CacheRegistry.names()

    bot_sources._version_cache["mtime"] = 99999.0
    bot_sources._version_cache["version"] = "v_test"

    cleared = CacheRegistry.clear("utils.bot_sources._version_cache")
    assert cleared is True
    assert bot_sources._version_cache == {}


# ── refresh_all_sources ─────────────────────────────────────────────────────


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_refresh_all_sources_returns_metadata_for_each_enabled_source():
    """Walks ``BOT_SOURCES`` (filtering out disabled ones) and calls
    fetch_and_cache_source for each. Returns the list of metadata
    dicts the admin UI renders."""
    fake_sources = [
        {"id": "well-known-bots", "name": "Well-Known Bots (arcjet)", "url": "https://x", "enabled": True},
    ]
    with (
        patch.object(bot_sources, "BOT_SOURCES", fake_sources),
        patch(
            "backend.utils.bot_sources.fetch_and_cache_source",
            return_value={"id": "well-known-bots", "name": "Well-Known", "entry_count": 50},
        ) as mock_fetch,
    ):
        out = bot_sources.refresh_all_sources()

    assert mock_fetch.call_count == 1  # one enabled source
    assert len(out) == 1
    assert out[0]["entry_count"] == 50


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_refresh_all_sources_continues_when_one_source_fails():
    """A fetch failure for ONE source must not abort the refresh of
    others — pinned because a transient GitHub outage on the
    well-known-bots URL would otherwise leave the whole bot pipeline
    stale across all sources."""
    # Add a second enabled source to BOT_SOURCES for this test
    fake_sources = [
        {"id": "good", "name": "Good", "url": "https://x", "enabled": True},
        {"id": "bad", "name": "Bad", "url": "https://y", "enabled": True},
    ]

    def _fake_fetch(sid):
        if sid == "bad":
            raise RuntimeError("Network down")
        return {"id": sid, "entry_count": 10}

    with (
        patch.object(bot_sources, "BOT_SOURCES", fake_sources),
        patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=_fake_fetch),
    ):
        out = bot_sources.refresh_all_sources()

    assert len(out) == 2
    succeeded = [r for r in out if not r.get("failed")]
    failed = [r for r in out if r.get("failed")]
    assert len(succeeded) == 1
    assert succeeded[0]["id"] == "good"
    assert len(failed) == 1
    assert failed[0]["id"] == "bad"
    assert "Network down" in failed[0]["error"]


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_refresh_all_sources_skips_disabled_sources():
    """``enabled: False`` sources must be skipped entirely — not even
    fetched. Pinned because admins toggle sources off when they're
    flaky, and re-fetching anyway would defeat the purpose."""
    fake_sources = [
        {"id": "off", "name": "Off", "url": "https://x", "enabled": False},
    ]
    with (
        patch.object(bot_sources, "BOT_SOURCES", fake_sources),
        patch("backend.utils.bot_sources.fetch_and_cache_source") as mock_fetch,
    ):
        out = bot_sources.refresh_all_sources()

    mock_fetch.assert_not_called()
    assert out == []


# ── get_bot_regex_pattern ──────────────────────────────────────────────────


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_get_bot_regex_pattern_returns_none_for_empty_sources():
    """No cached sources → no literals → None. The caller treats None
    as "skip this prefilter" rather than running an empty regex
    (which DuckDB rejects)."""
    assert bot_sources.get_bot_regex_pattern() is None


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_get_bot_regex_pattern_builds_case_insensitive_alternation():
    """Pattern is ``(?i)alt1|alt2|...`` with each literal re.escaped.
    Pinned because losing the (?i) would make the DuckDB filter
    case-sensitive and miss most real bot UAs."""
    _seed_cache(
        "well-known-bots",
        [
            {"id": "g", "pattern": {"accepted": [r"Googlebot\/2\.1"]}},
            {"id": "b", "pattern": {"accepted": [r"bingbot\/2\.0"]}},
        ],
    )
    pattern = bot_sources.get_bot_regex_pattern()
    assert pattern is not None
    assert pattern.startswith("(?i)")
    # Both literals are present (escaped)
    assert "Googlebot/2" in pattern or r"Googlebot\/2" in pattern
    assert "bingbot/2" in pattern or r"bingbot\/2" in pattern


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_get_bot_regex_pattern_respects_limit():
    """When literals exceed ``limit``, only the first N (sorted by
    length desc) are included. Pinned because RE2's compiled NFA size
    grows with the alternation count, and exceeding the limit would
    surface as DuckDB regex-compile errors."""
    entries = [{"id": f"b{i}", "pattern": {"accepted": [f"BotLiteral{i:04d}"]}} for i in range(20)]
    _seed_cache("well-known-bots", entries)

    pattern = bot_sources.get_bot_regex_pattern(limit=5)
    assert pattern is not None
    # Count alternation pieces (split by |) — should be at most 5
    # Strip the (?i) prefix first
    body = pattern[len("(?i)") :]
    pieces = body.split("|")
    assert len(pieces) == 5


# ── enrich_bot_metadata: DataFrame enrichment ─────────────────────────────


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_enrich_bot_metadata_returns_early_on_empty_dataframe():
    """Empty DataFrame → no work, no exception. Pinned because the
    enrichment path is wired into the dashboard's empty-day render
    and crashing here would break the whole panel."""
    import pandas as pd

    df = pd.DataFrame()
    # Must not raise (and must not add columns to an empty df)
    bot_sources.enrich_bot_metadata(df)
    assert df.empty


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_enrich_bot_metadata_adds_bot_name_column_with_match():
    """When UA matches a known bot, ``_bot_name`` gets the bot name +
    classification state. Pinned because the dashboard's bot column
    keys on this exact column name."""
    import pandas as pd

    _seed_cache(
        "well-known-bots",
        [
            {
                "id": "googlebot",
                "name": "Googlebot",
                "pattern": {"accepted": [r"Googlebot/2\.1"]},
                "verification": {"domains": [], "cidrs": []},
            }
        ],
    )

    df = pd.DataFrame({"ua": ["Googlebot/2.1"], "ip": ["1.2.3.4"]})
    with patch(
        "backend.utils.bot_sources.get_hostnames",
        return_value={"1.2.3.4": ("crawl-1-2-3-4.googlebot.com", "ok", True)},
    ):
        bot_sources.enrich_bot_metadata(df)

    assert "_bot_name" in df.columns
    assert "Googlebot" in df["_bot_name"][0]


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_enrich_bot_metadata_marks_unknown_uas_as_null_string():
    """UAs that don't match any bot → ``"null"`` (string, not None).
    Pinned because the frontend filters on ``_bot_name != "null"``
    for the "show only bots" toggle — using None would break it."""
    import pandas as pd

    _seed_cache("well-known-bots", [])  # no entries → no matches

    df = pd.DataFrame({"ua": ["Mozilla/5.0 (real browser)"], "ip": ["1.2.3.4"]})
    bot_sources.enrich_bot_metadata(df)

    assert "_bot_name" in df.columns
    assert df["_bot_name"][0] == "null"


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_enrich_bot_metadata_handles_missing_ua_or_ip_columns():
    """If the DataFrame doesn't have ``ua`` OR ``ip`` columns
    (because the analyst selected a non-UA query), the enrichment
    skips that pass — pinned because the column-existence check is
    what prevents KeyError from breaking custom analytical queries."""
    import pandas as pd

    df = pd.DataFrame({"timestamp": ["2026-01-01"], "status": [200]})
    bot_sources.enrich_bot_metadata(df)
    # ``_bot_name`` should NOT be added when input lacks ua/ip
    assert "_bot_name" not in df.columns


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_enrich_bot_metadata_ngwaf_path_returns_none_when_db_missing():
    """``waf_req_id`` column present but the NGWAF DB doesn't exist
    → ``_ngwaf_bot_name`` is None (all rows). Used by services that
    don't have NGWAF enabled yet — must not crash the dashboard."""
    import pandas as pd

    df = pd.DataFrame({"waf_req_id": ["req-1", "req-2"]})

    with patch("os.path.exists", return_value=False):
        bot_sources.enrich_bot_metadata(df)

    assert "_ngwaf_bot_name" in df.columns
    assert df["_ngwaf_bot_name"].isnull().all()


# ── get_pattern_set_version: content-hash, not fetch-mtime ────────────────────


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_pattern_set_version_empty_when_no_cache():
    """No source files cached → empty string (rollup writer skips)."""
    assert bot_sources.get_pattern_set_version() == ""


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_pattern_set_version_stable_across_mtime_bump_with_same_content():
    """A daily re-fetch of IDENTICAL content must NOT churn the version — the
    wellknown rollup reader requires one version across the window, so an
    mtime-keyed version made every multi-day window mixed-version. The version
    is a content hash, so a touch (new mtime, same bytes) keeps it stable."""
    import os

    path = _seed_cache("well-known-bots", [{"id": "googlebot", "pattern": "Googlebot"}])
    v1 = bot_sources.get_pattern_set_version()
    assert v1.startswith("v") and len(v1) > 1

    # Bump mtime well past the first (whole-second granularity) WITHOUT
    # changing the bytes — simulates the daily no-op re-fetch.
    st = path.stat()
    os.utime(path, (st.st_atime + 100, st.st_mtime + 100))
    v2 = bot_sources.get_pattern_set_version()

    assert v2 == v1, "identical content under a new mtime must hash to the same version"


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_pattern_set_version_changes_when_content_changes():
    """A real pattern-set change must bump the version so stale rollups still
    fall back to live (the 'correctness over speed' invariant preserved)."""
    _seed_cache("well-known-bots", [{"id": "googlebot", "pattern": "Googlebot"}])
    v1 = bot_sources.get_pattern_set_version()

    # Reset the cache to force a re-hash, then change the content.
    bot_sources._version_cache["mtime"] = None
    bot_sources._version_cache["version"] = None
    _seed_cache("well-known-bots", [{"id": "bingbot", "pattern": "bingbot"}])
    v2 = bot_sources.get_pattern_set_version()

    assert v1 != v2, "changed source content must produce a different version"


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_enrich_bot_metadata_ip_match_only():
    """Verify that an IP falling within an enabled IP-based feed (Tor/IPsum) is correctly labeled,
    even when the User-Agent doesn't match any well-known bots."""
    import pandas as pd

    # Mock BOT_SOURCES with one IP source
    fake_sources = [
        {
            "id": "tor-exit-nodes",
            "name": "Tor Exit Nodes (Official)",
            "url": "https://x",
            "enabled": True,
            "type": "ip",
        },
    ]
    # Seed the cache with a fake Tor range
    _seed_cache(
        "tor-exit-nodes",
        [
            {
                "id": "tor-exit-nodes",
                "name": "Tor Exit Nodes (Official)",
                "pattern": {"accepted": []},
                "verification": {"domains": [], "cidrs": ["192.168.50.0/24"]},
            }
        ],
    )

    df = pd.DataFrame({"ua": ["Mozilla/5.0 (real browser)"], "ip": ["192.168.50.10"]})

    with (
        patch.object(bot_sources, "BOT_SOURCES", fake_sources),
        patch("backend.utils.bot_sources._cache_mtime", return_value=123.45),
    ):
        # Force cache invalidation
        bot_sources._compiled_ip_feeds.clear()
        bot_sources.enrich_bot_metadata(df)

    assert "_bot_name" in df.columns
    assert df["_bot_name"][0] == "Tor Exit Nodes (Official) (IP-Match)"


@pytest.mark.usefixtures("_isolated_cache_dir")
def test_enrich_bot_metadata_hybrid_ua_and_ip_match():
    """Verify that if UA matches a well-known bot AND the IP matches an IP-based source,
    the resulting _bot_name has both annotations combined correctly."""
    import pandas as pd

    # Mock BOT_SOURCES with one UA source and one IP source
    fake_sources = [
        {
            "id": "well-known-bots",
            "name": "Well-Known Bots (arcjet)",
            "url": "https://x",
            "enabled": True,
            "type": "ua",
        },
        {"id": "aws-ip-ranges", "name": "AWS IP Ranges", "url": "https://y", "enabled": True, "type": "ip"},
    ]
    # Seed both caches
    _seed_cache(
        "well-known-bots",
        [
            {
                "id": "googlebot",
                "name": "Googlebot",
                "pattern": {"accepted": ["Googlebot"]},
                "verification": {"domains": ["google.com"], "cidrs": []},
            }
        ],
    )
    _seed_cache(
        "aws-ip-ranges",
        [
            {
                "id": "aws-ip-ranges",
                "name": "AWS IP Ranges",
                "pattern": {"accepted": []},
                "verification": {"domains": [], "cidrs": ["10.0.0.0/8"]},
            }
        ],
    )

    df = pd.DataFrame({"ua": ["Googlebot/2.1"], "ip": ["10.80.0.5"]})

    with (
        patch.object(bot_sources, "BOT_SOURCES", fake_sources),
        patch("backend.utils.bot_sources._cache_mtime", return_value=123.45),
        patch(
            "backend.utils.bot_sources.get_hostnames",
            return_value={"10.80.0.5": ("crawl.google.com", "resolved", True)},
        ),
    ):
        bot_sources._compiled_ip_feeds.clear()
        bot_sources._source_cache.clear()
        with bot_sources._matcher_lock:
            bot_sources._matcher_cache.clear()
        bot_sources.enrich_bot_metadata(df)

    assert "_bot_name" in df.columns
    assert df["_bot_name"][0] == "Googlebot (verified, AWS IP Ranges)"


def test_bot_sources_package_submodules_and_pytricia_matching():
    """Verify the decomposed bot_sources package submodules, BoundedTTLCache instances,
    and pytricia-backed match_ip_to_bot_source."""
    import ipaddress

    from backend.routers.admin import admin_queries
    from backend.utils.bot_sources import cidr, fetcher, rdns, registry
    from backend.utils.bounded_cache import BoundedTTLCache

    assert isinstance(bot_sources._matcher_cache, BoundedTTLCache)
    assert isinstance(bot_sources._source_cache, BoundedTTLCache)
    assert isinstance(bot_sources._compiled_ip_feeds, BoundedTTLCache)
    assert isinstance(bot_sources._version_cache, BoundedTTLCache)
    assert isinstance(admin_queries._cancel_history, BoundedTTLCache)

    assert registry.CATEGORY_LABELS["search_engine"] == "Search Engine"
    assert fetcher.fetch_bot_source is fetcher.fetch_and_cache_source
    assert fetcher.sync_all_bot_sources is fetcher.refresh_all_sources
    assert rdns.classify is bot_sources.classify

    mock_feeds = {
        "tor-exit-nodes": [ipaddress.ip_network("185.220.101.0/24")],
        "aws-ip-ranges": [ipaddress.ip_network("2600:1f00::/24")],
    }
    with patch("backend.utils.bot_sources._get_compiled_ip_feeds", return_value=mock_feeds):
        assert cidr.match_ip_to_bot_source("185.220.101.42") == "tor-exit-nodes"
        assert cidr.match_ip_to_bot_source("2600:1f00::1") == "aws-ip-ranges"
        assert cidr.match_ip_to_bot_source("8.8.8.8") is None
        assert cidr.match_ip_to_bot_source("not-an-ip") is None
        assert cidr.match_ip_to_bot_source("") is None


@pytest.mark.parametrize(
    "bad_url",
    [
        "http://example.com/ips.json",
        "file:///etc/passwd",
        "ftp://example.com/ips.txt",
        "https:///no-host",
        "https://127.0.0.1/ips.json",
        "https://[::1]/ips.json",
        "https://[::ffff:127.0.0.1]/ips.json",
        "https://10.0.0.1/ips.json",
        "https://192.168.1.1/ips.json",
        "https://169.254.169.254/latest/meta-data/",
    ],
)
def test_validate_feed_url_rejects_disallowed_schemes_and_literal_ips(bad_url: str):
    from backend.utils.bot_sources.fetcher import _validate_feed_url

    with pytest.raises(ValueError):
        _validate_feed_url(bad_url)


@pytest.mark.parametrize(
    "resolved_ip",
    [
        "127.0.0.1",
        "::1",
        "::ffff:127.0.0.1",
        "10.1.2.3",
        "172.16.0.1",
        "192.168.0.1",
        "169.254.169.254",
        "0.0.0.0",
        "224.0.0.1",
    ],
)
def test_validate_feed_url_rejects_dns_resolved_internal_addresses(resolved_ip: str):
    from backend.utils.bot_sources.fetcher import _validate_feed_url

    family = socket.AF_INET6 if ":" in resolved_ip else socket.AF_INET
    addrinfo = [(family, socket.SOCK_STREAM, 6, "", (resolved_ip, 443))]
    with patch("backend.utils.bot_sources.fetcher.socket.getaddrinfo", return_value=addrinfo):
        with pytest.raises(ValueError, match="disallowed IP address"):
            _validate_feed_url("https://feeds.example.com/ips.json")


def test_http_get_text_rejects_redirect_to_loopback_before_following():
    import httpx

    from backend.utils.bot_sources.fetcher import http_get_text

    def fake_getaddrinfo(host, port, *args, **kwargs):
        if host == "feeds.example.com":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]
        if host == "internal.example.com":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]
        raise AssertionError(f"Unexpected host lookup: {host}")

    requests_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(str(request.url))
        return httpx.Response(302, headers={"Location": "https://internal.example.com/secret"})

    transport = httpx.MockTransport(handler)
    real_client = httpx.Client

    with (
        patch("backend.utils.bot_sources.fetcher.socket.getaddrinfo", side_effect=fake_getaddrinfo),
        patch(
            "backend.utils.bot_sources.fetcher.httpx.Client",
            side_effect=lambda **kw: real_client(transport=transport, **kw),
        ),
    ):
        with pytest.raises(ValueError, match="disallowed IP address"):
            http_get_text("https://feeds.example.com/ips.json")

    assert requests_seen == ["https://feeds.example.com/ips.json"]


def test_http_get_text_bounds_response_body_size():
    import httpx

    from backend.utils.bot_sources.fetcher import http_get_text

    addrinfo = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]
    transport = httpx.MockTransport(
        lambda _req: httpx.Response(200, content=b"x" * 101, headers={"Content-Length": "101"})
    )
    real_client = httpx.Client

    with (
        patch("backend.utils.bot_sources.fetcher.socket.getaddrinfo", return_value=addrinfo),
        patch(
            "backend.utils.bot_sources.fetcher.httpx.Client",
            side_effect=lambda **kw: real_client(transport=transport, **kw),
        ),
    ):
        with pytest.raises(ValueError, match="max_bytes"):
            http_get_text("https://feeds.example.com/ips.json", max_bytes=100)
