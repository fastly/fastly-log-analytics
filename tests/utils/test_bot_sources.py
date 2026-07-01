import json
from pathlib import Path
from unittest.mock import MagicMock, patch

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


@pytest.fixture(autouse=True)
def _isolated_cache_dir(tmp_path, monkeypatch):
    """Point ``_CACHE_DIR`` at a tmp_path so cached source files don't
    leak across tests and we don't write into the real data/cache/."""
    monkeypatch.setattr(bot_sources, "_CACHE_DIR", tmp_path)
    # Also clear the in-process matcher cache between tests.
    with bot_sources._matcher_lock:
        bot_sources._matcher_cache.clear()
    # Reset the content-hash version cache so it doesn't leak across tests.
    bot_sources._version_cache["mtime"] = None
    bot_sources._version_cache["version"] = None
    yield


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


def test_fetch_external_cidrs_json_array():
    source_config = {"url": "https://example.com/ips.json", "selector": None}

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(["1.1.1.1", "2.2.2.2/24"]).encode()
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        res = fetch_external_cidrs(source_config)
        assert len(res) == 2
        assert "1.1.1.1" in res


def test_fetch_external_cidrs_plaintext():
    source_config = {"url": "https://example.com/ips.txt", "selector": None}

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.read.return_value = b"# Comments\n1.1.1.1\n2.2.2.2/24\n\n"
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        res = fetch_external_cidrs(source_config)
        assert len(res) == 2
        assert "1.1.1.1" in res


def test_fetch_external_cidrs_google_format():
    source_config = {"url": "https://example.com/ips.json", "selector": '$.prefixes[*]["ipv4Prefix","ipv6Prefix"]'}

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        google_mock = {"prefixes": [{"ipv4Prefix": "1.1.1.1/32"}, {"ipv6Prefix": "2001:db8::/32"}]}
        mock_resp.read.return_value = json.dumps(google_mock).encode()
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        res = fetch_external_cidrs(source_config)
        assert len(res) == 2
        assert "1.1.1.1/32" in res
        assert "2001:db8::/32" in res


# ── load_source ──────────────────────────────────────────────────────────────


def test_load_source_returns_empty_when_cache_missing():
    assert load_source("well-known-bots") == []


def test_load_source_returns_cached_entries():
    entries = [{"id": "bot-1", "pattern": {"accepted": ["Bot/1"]}}]
    _seed_cache("well-known-bots", entries)
    assert load_source("well-known-bots") == entries


def test_load_source_returns_empty_for_corrupt_cache():
    """Truncated JSON / wrong shape must not raise — just degrade."""
    bot_sources._cache_path("well-known-bots").write_text("{ not valid")
    assert load_source("well-known-bots") == []


# ── get_all_sources_meta ─────────────────────────────────────────────────────


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


def test_get_all_sources_meta_returns_none_fields_when_cache_missing():
    meta = get_all_sources_meta()
    wkb = next(m for m in meta if m["id"] == "well-known-bots")
    assert wkb["entry_count"] is None
    assert wkb["last_updated"] is None


# ── get_bot_by_id ────────────────────────────────────────────────────────────


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


def test_get_bot_by_id_returns_none_for_unknown():
    _seed_cache("well-known-bots", [{"id": "googlebot"}])
    assert get_bot_by_id("never-heard-of") is None


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

    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(fake_data).encode()
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp):
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

    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(fake_data).encode()
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp):
        bot_sources.fetch_and_cache_source("well-known-bots")

    entries = bot_sources.load_source("well-known-bots")
    cidrs = entries[0]["verification"]["cidrs"]
    assert "157.55.39.0/24" in cidrs
    assert "207.46.13.0/24" in cidrs


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

    mock_main_resp = MagicMock()
    mock_main_resp.read.return_value = json.dumps(fake_well_known).encode()
    mock_main_resp.__enter__.return_value = mock_main_resp

    with (
        patch("urllib.request.urlopen", return_value=mock_main_resp),
        patch("backend.utils.bot_sources.fetch_external_cidrs", return_value=["8.8.8.0/24"]),
    ):
        bot_sources.fetch_and_cache_source("well-known-bots")

    entries = bot_sources.load_source("well-known-bots")
    assert "8.8.8.0/24" in entries[0]["verification"]["cidrs"]


def test_fetch_and_cache_source_raises_on_unknown_source_id():
    """Unknown source id → ValueError (not a silent no-op).
    Pinned because the refresh CLI uses this exit code to signal
    a configuration error."""
    with pytest.raises(ValueError, match="Unknown bot source"):
        bot_sources.fetch_and_cache_source("does-not-exist")


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
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps([{"id": "new"}]).encode()
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp):
        bot_sources.fetch_and_cache_source("well-known-bots")

    # The cache was cleared as a side-effect of the fetch
    assert bot_sources._matcher_cache == {}


# ── refresh_all_sources ─────────────────────────────────────────────────────


def test_refresh_all_sources_returns_metadata_for_each_enabled_source():
    """Walks ``BOT_SOURCES`` (filtering out disabled ones) and calls
    fetch_and_cache_source for each. Returns the list of metadata
    dicts the admin UI renders."""
    with patch(
        "backend.utils.bot_sources.fetch_and_cache_source",
        return_value={"id": "well-known-bots", "name": "Well-Known", "entry_count": 50},
    ) as mock_fetch:
        out = bot_sources.refresh_all_sources()

    assert mock_fetch.call_count == 1  # one enabled source
    assert len(out) == 1
    assert out[0]["entry_count"] == 50


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

    assert len(out) == 1  # only the good one succeeded
    assert out[0]["id"] == "good"


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


def test_get_bot_regex_pattern_returns_none_for_empty_sources():
    """No cached sources → no literals → None. The caller treats None
    as "skip this prefilter" rather than running an empty regex
    (which DuckDB rejects)."""
    assert bot_sources.get_bot_regex_pattern() is None


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


def test_enrich_bot_metadata_returns_early_on_empty_dataframe():
    """Empty DataFrame → no work, no exception. Pinned because the
    enrichment path is wired into the dashboard's empty-day render
    and crashing here would break the whole panel."""
    import pandas as pd

    df = pd.DataFrame()
    # Must not raise (and must not add columns to an empty df)
    bot_sources.enrich_bot_metadata(df)
    assert df.empty


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
        "backend.utils.rdns_cache.get_hostname",
        return_value=("crawl-1-2-3-4.googlebot.com", "ok", True),
    ):
        bot_sources.enrich_bot_metadata(df)

    assert "_bot_name" in df.columns
    assert "Googlebot" in df["_bot_name"][0]


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


def test_pattern_set_version_empty_when_no_cache():
    """No source files cached → empty string (rollup writer skips)."""
    assert bot_sources.get_pattern_set_version() == ""


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
