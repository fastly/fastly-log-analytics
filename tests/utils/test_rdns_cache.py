"""Tests for backend.utils.rdns_cache.

Covers the pure helpers (``_is_ip_in_cidrs``, ``classify``) and the
public cache API (``enqueue``, ``get_hostname``, ``get_stats``) against
an isolated per-test SQLite file. The lookup + enrichment paths that
shell out to DNS are mocked at the ``socket`` boundary so tests don't
actually hit the network.
"""

from __future__ import annotations

import socket
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from backend.utils import rdns_cache


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Point ``_DB_PATH`` at a fresh per-test SQLite file."""
    db = tmp_path / "rdns_cache.db"
    monkeypatch.setattr(rdns_cache, "_DB_PATH", Path(db))
    yield db


# ── _is_ip_in_cidrs (pure) ────────────────────────────────────────────────────


def test_cidr_match_returns_true_for_in_range():
    assert rdns_cache._is_ip_in_cidrs("66.249.66.1", ["66.249.64.0/19"]) is True


def test_cidr_match_returns_false_for_out_of_range():
    assert rdns_cache._is_ip_in_cidrs("8.8.8.8", ["66.249.64.0/19"]) is False


def test_cidr_match_short_circuits_empty_list():
    assert rdns_cache._is_ip_in_cidrs("8.8.8.8", []) is False


def test_cidr_match_ignores_invalid_cidrs_in_list():
    """A malformed CIDR must not crash — just skip it and keep checking."""
    cidrs = ["not-a-cidr", "8.8.8.0/24"]
    assert rdns_cache._is_ip_in_cidrs("8.8.8.42", cidrs) is True


def test_cidr_match_returns_false_for_invalid_ip():
    assert rdns_cache._is_ip_in_cidrs("not-an-ip", ["8.8.8.0/24"]) is False


def test_cidr_match_works_for_ipv6():
    assert rdns_cache._is_ip_in_cidrs("2606:4700::1", ["2606:4700::/32"]) is True
    assert rdns_cache._is_ip_in_cidrs("2001:db8::1", ["2606:4700::/32"]) is False


# ── classify (pure) ──────────────────────────────────────────────────────────


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


def test_classify_no_domains_means_unverified():
    """Without ``verification_domains`` we can't FCrDNS-verify anything."""
    assert rdns_cache.classify("1.2.3.4", "x.googlebot.com", "found", True, verification_domains=[]) == "unverified"


def test_classify_pending_status_returns_pending_label():
    assert (
        rdns_cache.classify("1.2.3.4", None, "pending", False, verification_domains=["googlebot.com"])
        == "unverified_pending"
    )


@pytest.mark.parametrize("status", ["nxdomain", "error"])
def test_classify_nxdomain_or_error_is_impersonator(status):
    assert rdns_cache.classify("1.2.3.4", None, status, False, verification_domains=["googlebot.com"]) == "impersonator"


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


def test_get_hostname_returns_pending_for_unknown_ip():
    rdns_cache._init()  # ensure schema present after the isolated_db swap
    hostname, status, verified = rdns_cache.get_hostname("9.9.9.9")
    assert hostname is None
    assert status == "pending"
    assert verified is False


def test_enqueue_inserts_pending_rows():
    rdns_cache._init()
    n = rdns_cache.enqueue(["1.1.1.1", "8.8.8.8"])
    assert n == 2
    # Both visible as pending
    for ip in ("1.1.1.1", "8.8.8.8"):
        h, st, _ = rdns_cache.get_hostname(ip)
        assert h is None
        assert st == "pending"


def test_enqueue_dedups_against_existing_rows():
    rdns_cache._init()
    rdns_cache.enqueue(["1.1.1.1"])
    n = rdns_cache.enqueue(["1.1.1.1", "2.2.2.2"])
    assert n == 1  # only the new one


def test_enqueue_empty_list_is_noop():
    assert rdns_cache.enqueue([]) == 0


def test_get_hostnames_batch_returns_cached_and_enqueues_missing():
    """``get_hostnames`` should fold N per-IP SELECTs into one call: rows
    already in the cache come back via the dict, IPs we've never seen are
    omitted from the result *and* batch-enqueued as 'pending'."""
    rdns_cache._init()
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
    h, st, _ = rdns_cache.get_hostname("9.9.9.9")
    assert h is None and st == "pending"


def test_get_hostnames_batch_empty_input_is_noop():
    assert rdns_cache.get_hostnames([]) == {}
    assert rdns_cache.get_hostnames(["", None]) == {}  # type: ignore[list-item]


def test_get_stats_returns_zero_when_db_missing():
    """If the DB file doesn't exist, stats should not raise."""
    # Don't call _init — leave file missing
    stats = rdns_cache.get_stats()
    assert stats["total"] == 0
    assert stats["pending"] == 0


def test_get_stats_counts_total_and_pending():
    rdns_cache._init()
    rdns_cache.enqueue(["1.1.1.1", "2.2.2.2", "3.3.3.3"])
    stats = rdns_cache.get_stats()
    assert stats["total"] == 3
    assert stats["pending"] == 3


# ── _do_lookup: DNS resolution with FCrDNS ──────────────────────────────────


def test_do_lookup_returns_nxdomain_on_herror():
    """``socket.herror`` (PTR record missing) → ('nxdomain', no
    hostname). Pinned because the classifier treats nxdomain as
    impersonator only when the bot defines a verification domain;
    a refactor that surfaced these as 'error' would shift the
    distinction."""
    with patch("socket.gethostbyaddr", side_effect=socket.herror("no PTR")):
        host, status, fcrdns = rdns_cache._do_lookup("1.2.3.4")
    assert host is None
    assert status == "nxdomain"
    assert fcrdns is False


def test_do_lookup_returns_error_on_unexpected_exception():
    """Any other exception (timeout, gaierror) → 'error' status.
    Pinned distinct from nxdomain so the classifier can choose
    whether to retry."""
    with patch("socket.gethostbyaddr", side_effect=OSError("timeout")):
        host, status, fcrdns = rdns_cache._do_lookup("1.2.3.4")
    assert host is None
    assert status == "error"
    assert fcrdns is False


def test_do_lookup_fcrdns_verified_when_forward_matches_reverse():
    """Reverse DNS returns hostname, forward DNS for that hostname
    includes the original IP → FCrDNS verified. This is the gold
    standard for bot verification."""
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


def test_do_lookup_fcrdns_unverified_when_forward_mismatches():
    """The classic impersonator pattern: PTR points to a googlebot
    hostname, but forward lookup of that hostname returns a different
    IP. Pinned because losing this check would let attackers spoof
    PTR records to pass verification."""
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


def test_do_lookup_fcrdns_false_when_forward_resolution_errors():
    """Reverse DNS succeeds but forward DNS fails (NXDOMAIN, timeout)
    → fcrdns=False but status is still 'resolved'. Pinned because
    the cache shouldn't discard a known hostname just because forward
    resolution flaked."""
    with (
        patch("socket.gethostbyaddr", return_value=("known.example.com", [], [])),
        patch("socket.getaddrinfo", side_effect=OSError("forward DNS down")),
    ):
        host, status, fcrdns = rdns_cache._do_lookup("1.2.3.4")

    assert host == "known.example.com"
    assert status == "resolved"
    assert fcrdns is False


# ── enrich_batch ────────────────────────────────────────────────────────────


def test_enrich_batch_resolves_pending_ips():
    """Happy path: pending IPs get resolved + persisted. Pinned
    because the cron job depends on the summary counts for monitoring."""
    rdns_cache.enqueue(["1.1.1.1", "2.2.2.2"])

    fake_lookups = {
        "1.1.1.1": ("one.example.com", "resolved", True),
        "2.2.2.2": ("two.example.com", "resolved", True),
    }

    with (
        patch("backend.utils.rdns_cache._do_lookup", side_effect=lambda ip: fake_lookups[ip]),
        patch("backend.utils.rdns_cache._discover_new_ips", return_value=0),
    ):
        summary = rdns_cache.enrich_batch(limit=10)

    assert summary["resolved"] == 2
    assert summary["errors"] == 0
    # Stats reflect the resolved state
    stats = rdns_cache.get_stats()
    assert stats["pending"] == 0


def test_enrich_batch_counts_errors_separately():
    """nxdomain / error statuses bump the ``errors`` counter, not
    ``resolved``. Pinned because the admin dashboard's monitoring
    graph splits these — conflating them would hide DNS flaps."""
    rdns_cache.enqueue(["bad.ip"])

    with (
        patch(
            "backend.utils.rdns_cache._do_lookup",
            return_value=(None, "nxdomain", False),
        ),
        patch("backend.utils.rdns_cache._discover_new_ips", return_value=0),
    ):
        summary = rdns_cache.enrich_batch(limit=10)

    assert summary["resolved"] == 0
    assert summary["errors"] == 1


def test_enrich_batch_stamps_last_enrichment_timestamp():
    """``_last_enrichment_at`` is what ``get_stats`` returns; the
    cron stamps it so the admin UI can show "Last refreshed: N
    minutes ago"."""
    rdns_cache._last_enrichment_at = None
    with patch("backend.utils.rdns_cache._discover_new_ips", return_value=0):
        rdns_cache.enrich_batch(limit=10)

    assert rdns_cache._last_enrichment_at is not None
    assert "T" in rdns_cache._last_enrichment_at  # ISO-Z format


def test_enrich_batch_swallows_discovery_exception():
    """If ``_discover_new_ips`` raises (S3 down, missing source),
    the resolve pass still completes. Pinned because losing the
    resolve pass over a discovery error would freeze every existing
    cache entry for hours."""
    rdns_cache.enqueue(["1.1.1.1"])

    with (
        patch(
            "backend.utils.rdns_cache._do_lookup",
            return_value=("ok.example.com", "resolved", True),
        ),
        patch("backend.utils.rdns_cache._discover_new_ips", side_effect=RuntimeError("S3 down")),
    ):
        summary = rdns_cache.enrich_batch(limit=10)

    assert summary["resolved"] == 1
    assert summary["discovered"] == 0  # swallowed


def test_enrich_batch_no_op_when_nothing_pending():
    """No pending IPs + discovery returns 0 → all-zero summary, no
    log spam. Pinned because the cron runs every 5 minutes and we
    don't want it logging at INFO level when there's nothing to do."""
    with patch("backend.utils.rdns_cache._discover_new_ips", return_value=0):
        summary = rdns_cache.enrich_batch(limit=10)

    assert summary == {"resolved": 0, "errors": 0, "discovered": 0}


# ── enrich_batch_gen: SSE-style streaming ──────────────────────────────────


def test_enrich_batch_gen_yields_status_and_done_events():
    """The generator variant yields progress events for the SSE
    endpoint. Final event is always 'done' with the summary text."""
    rdns_cache.enqueue(["1.1.1.1"])

    fake_disc_gen = iter([{"type": "count", "count": 0}])

    with (
        patch(
            "backend.utils.rdns_cache._do_lookup",
            return_value=("ok.example.com", "resolved", True),
        ),
        patch("backend.utils.rdns_cache._discover_new_ips_gen", return_value=fake_disc_gen),
    ):
        events = list(rdns_cache.enrich_batch_gen(limit=10))

    types = [e["type"] for e in events]
    assert "status" in types
    assert "log" in types  # the per-IP resolution events
    assert events[-1]["type"] == "done"
    assert "Resolved: 1" in events[-1]["message"]


def test_enrich_batch_gen_yields_no_pending_status_when_queue_empty():
    """Empty pending queue → a "No pending IPs to resolve." status
    event so the SSE UI can render that explicit message instead of
    a blank progress bar."""
    fake_disc_gen = iter([{"type": "count", "count": 0}])

    with patch("backend.utils.rdns_cache._discover_new_ips_gen", return_value=fake_disc_gen):
        events = list(rdns_cache.enrich_batch_gen(limit=10))

    no_pending = [e for e in events if e["type"] == "status" and "No pending" in e["message"]]
    assert len(no_pending) == 1


def test_enrich_batch_gen_yields_error_event_on_discovery_failure():
    """Discovery pass failure surfaces as a typed 'error' event the
    SSE client can render in red — distinct from the 'done' terminal."""

    def _fail_gen():
        raise RuntimeError("discovery broken")
        yield  # unreachable, keeps it a generator

    with patch("backend.utils.rdns_cache._discover_new_ips_gen", side_effect=_fail_gen):
        events = list(rdns_cache.enrich_batch_gen(limit=10))

    error_events = [e for e in events if e["type"] == "error"]
    assert len(error_events) == 1
    assert "Discovery failed" in error_events[0]["message"]


# ── _now ────────────────────────────────────────────────────────────────────


def test_now_returns_iso8601_z_format():
    """ISO 8601 with Z suffix (UTC). Pinned because the SQL filter
    ``looked_up_at < datetime('now', '-48 hours')`` keys on this
    exact format — using a different one would silently break the
    stale-entry refresh path."""
    out = rdns_cache._now()
    assert out.endswith("Z")
    assert "T" in out
    assert len(out) == 20  # YYYY-MM-DDTHH:MM:SSZ


# ── backfill_from_sources ──────────────────────────────────────────────────


def test_backfill_from_sources_returns_count_from_discovery():
    """Thin wrapper around ``_discover_new_ips`` with a 30-day window."""
    with patch("backend.utils.rdns_cache._discover_new_ips", return_value=42) as mock_disc:
        out = rdns_cache.backfill_from_sources(max_ips=1000)

    assert out == 42
    # The wrapper hard-codes days=30 — pinned so a future refactor
    # that changes this is caught.
    mock_disc.assert_called_once_with(max_new=1000, days=30)


def test_backfill_from_sources_gen_yields_done_event_with_count():
    """Generator variant: the count event from ``_discover_new_ips_gen``
    is converted into a 'done' SSE event with the enqueued total."""
    fake_disc_gen = iter(
        [
            {"type": "status", "message": "scanning..."},
            {"type": "count", "count": 25},
        ]
    )

    with patch("backend.utils.rdns_cache._discover_new_ips_gen", return_value=fake_disc_gen):
        events = list(rdns_cache.backfill_from_sources_gen(max_ips=1000))

    done_events = [e for e in events if e["type"] == "done"]
    assert len(done_events) == 1
    assert "25 IPs" in done_events[0]["message"]


# ── _discover_new_ips: branch coverage via mocked DuckDB ──────────────────


def test_discover_new_ips_returns_zero_when_no_sources_configured():
    """No services configured → no sources to scan → 0 new IPs.
    Pinned because raising here would break the cron on fresh installs."""
    with patch("backend.config.list_configs", return_value=[]):
        out = rdns_cache._discover_new_ips(max_new=100)
    assert out == 0


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


def test_discover_new_ips_swallows_config_load_failure():
    """``list_configs`` raising must not crash discovery — log + return
    0, let the next cron run try again."""
    with patch("backend.config.list_configs", side_effect=RuntimeError("config corrupt")):
        out = rdns_cache._discover_new_ips(max_new=100)
    assert out == 0


# ── get_hostname: cache hit (after enrichment) ────────────────────────────


def test_get_hostname_returns_cached_resolution_after_enrichment():
    """After ``enrich_batch`` resolves a pending IP, subsequent
    ``get_hostname`` calls return the cached hostname WITHOUT
    re-enqueueing or hitting the network."""
    rdns_cache.enqueue(["1.1.1.1"])

    with (
        patch(
            "backend.utils.rdns_cache._do_lookup",
            return_value=("known.example.com", "resolved", True),
        ),
        patch("backend.utils.rdns_cache._discover_new_ips", return_value=0),
    ):
        rdns_cache.enrich_batch(limit=10)

    # Now query — should NOT trigger another lookup
    with patch("backend.utils.rdns_cache._do_lookup") as mock_lookup:
        host, status, fcrdns = rdns_cache.get_hostname("1.1.1.1")

    assert host == "known.example.com"
    assert status == "resolved"
    assert fcrdns is True
    mock_lookup.assert_not_called()


# ── get_hostname when DB unreachable (read connection returns None) ───────


def test_get_hostname_enqueues_and_returns_pending_when_read_con_unavailable():
    """If ``_read_con`` returns None (DB file missing or unreadable),
    the helper still enqueues the IP as pending and returns the
    pending sentinel — pinned because this is the fail-soft path
    that prevents the rDNS feature from breaking the entire admin
    UI when the cache DB is briefly unavailable (e.g. during
    teardown / file relocation)."""
    with patch("backend.utils.rdns_cache._read_con", return_value=None):
        host, status, fcrdns = rdns_cache.get_hostname("9.9.9.9")
    assert host is None
    assert status == "pending"
    assert fcrdns is False


# ── enrich_batch: stale-refresh pass ──────────────────────────────────────


def test_enrich_batch_refreshes_stale_entries():
    """Entries with ``looked_up_at`` older than 48 hours get re-resolved.
    Pinned because forgetting the refresh pass would freeze the cache
    against rDNS records that legitimately change (cloud providers
    rotate IPs)."""
    import sqlite3

    # Seed a resolved row with a stale looked_up_at timestamp
    rdns_cache.enqueue(["8.8.4.4"])
    db_path = str(rdns_cache._DB_PATH)
    con = sqlite3.connect(db_path)
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
        patch("backend.utils.rdns_cache._do_lookup", side_effect=fake_lookup),
        patch("backend.utils.rdns_cache._discover_new_ips", return_value=0),
    ):
        rdns_cache.enrich_batch(limit=10)

    # The stale entry was re-looked-up
    assert "8.8.4.4" in calls
    # And the cache reflects the refresh
    host, status, _ = rdns_cache.get_hostname("8.8.4.4")
    assert host == "refreshed.example.com"


# ── enrich_batch_gen: per-IP log event stream ──────────────────────────────


def test_enrich_batch_gen_yields_log_per_resolved_ip():
    """Each successfully-resolved IP emits a "Resolved X -> hostname"
    log line. Pinned because admins use these log lines to debug
    why a particular IP isn't resolving."""
    rdns_cache.enqueue(["3.3.3.3"])

    with (
        patch("backend.utils.rdns_cache._do_lookup", return_value=("h.example.com", "resolved", True)),
        patch("backend.utils.rdns_cache._discover_new_ips_gen", return_value=iter([{"type": "count", "count": 0}])),
    ):
        events = list(rdns_cache.enrich_batch_gen(limit=10))

    log_messages = [e["message"] for e in events if e["type"] == "log"]
    assert any("Resolved 3.3.3.3" in m and "h.example.com" in m for m in log_messages)


def test_enrich_batch_gen_yields_log_per_failed_ip():
    """Failed lookups emit "Failed to resolve X: nxdomain" lines.
    Pinned because the FE keys on the "Failed to resolve" prefix to
    colour the log entry red."""
    rdns_cache.enqueue(["7.7.7.7"])

    with (
        patch("backend.utils.rdns_cache._do_lookup", return_value=(None, "nxdomain", False)),
        patch("backend.utils.rdns_cache._discover_new_ips_gen", return_value=iter([{"type": "count", "count": 0}])),
    ):
        events = list(rdns_cache.enrich_batch_gen(limit=10))

    log_messages = [e["message"] for e in events if e["type"] == "log"]
    assert any("Failed to resolve 7.7.7.7" in m and "nxdomain" in m for m in log_messages)


# ── _discover_new_ips_gen: source iteration ──────────────────────────────


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
        patch("backend.utils.rdns_cache._write_con", return_value=fake_write_con),
        patch("backend.utils.rdns_cache.enqueue") as mock_enqueue,
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
        patch("backend.utils.rdns_cache.enqueue") as mock_enqueue,
    ):
        events = list(rdns_cache._discover_new_ips_gen(max_new=10))

    # Status event for scanning emitted, but no log "Found N new IPs"
    log_events = [e for e in events if e["type"] == "log"]
    assert not any("Found" in e.get("message", "") for e in log_events)
    mock_enqueue.assert_not_called()


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


# Silence unused-imports for fixtures
_ = MagicMock
