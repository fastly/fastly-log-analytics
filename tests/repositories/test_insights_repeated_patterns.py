"""Tests for the ``repeated_patterns`` ("Scripted Traffic Patterns") insight.

Detects client IPs sending requests on a highly regular cadence (scrapers,
pollers, cron jobs, beacons). The detection logic lives in a single live
DuckDB SQL template (Sheppard-corrected CV + modal-dominance gates), so the
behavioural assertions go through ``get_insights`` end-to-end; the severity /
masking / score logic is a pure row-processor and gets unit-tested directly.

Fixtures use WHOLE-SECOND timestamps (no ms) — verified-real log granularity is
1 second, so ms fixtures would be physically impossible (see
local-docs/repeated_pattern_detection_FINAL_plan.md D1).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from backend.repositories._base import _safe_table
from backend.repositories._sql import insights as SQL
from backend.repositories.insights import _insights_cache, get_insights
from backend.repositories.insights import definitions as defs
from backend.repositories.insights.registry import registry

# A 10-column row matching the REPEATED_PATTERNS SELECT projection:
# [ip, n_gaps, n_events, avg_interval, stddev_interval, cv_corr, modal_frac,
#  distinct_ua, span_s, mode_gap]
_PERFECT_BEACON_ROW = ("1.1.1.1", 34, 35, 60.0, 0.0, 0.0, 1.0, 1, 2040, 60)


def _create_table(con, table: str) -> None:
    con.execute(f'CREATE TABLE IF NOT EXISTS {table} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "ua" VARCHAR)')


def _seed_repeated_patterns(con, table: str) -> None:
    """Insert one row-set covering every scenario from the plan's test matrix.

    All events fall inside the last hour (the window scan) except a single
    anchor row 70 min back, which only exists so ``available_history`` clears
    the ``check_baseline`` gate (baseline_hours=1) without being scanned by the
    window-only detection SQL.
    """
    now = datetime.now(UTC).replace(microsecond=0)

    def ins(offset_s: int, ip: str, ua: str) -> None:
        ts = (now - timedelta(seconds=offset_s)).isoformat()
        con.execute(f'INSERT INTO {table} ("timestamp", "ip", "ua") VALUES (?, ?, ?)', [ts, ip, ua])

    # Anchor: outside the window (70 min < window_start at -60 min) so it lifts
    # available_history past baseline_hours without polluting detection.
    ins(70 * 60, "9.9.9.9", "anchor")

    # 1.1.1.1 — steady dense beacon: 35 events exactly 60 s apart, fixed UA.
    # cv≈0, modal=1, n_events≥30 → DETECTED, critical.
    for i in range(35):
        ins(60 + i * 60, "1.1.1.1", "Mozilla/5.0 SteadyClient")

    # 1.1.1.3 — UA-rotating beacon: 30 events 30 s apart, a DIFFERENT non-bot UA
    # each request → high distinct_ua but still DETECTED (proves no UA gate, D4).
    for i in range(30):
        ins(60 + i * 30, "1.1.1.3", f"Mozilla/5.0 Browser-{i}")

    # 2.2.2.2 — human/bursty: same-second burst + irregular long gaps → high CV,
    # low modal → IGNORED.
    bursty_gaps = [3, 47, 5, 2, 88, 14, 6, 120, 9, 4, 60, 33, 7, 200, 11, 5, 75, 19, 2]
    cum = 60
    for _ in range(3):  # parallel asset burst collapses to one active second
        ins(cum, "2.2.2.2", "Mozilla/5.0 Human")
    for gp in bursty_gaps:
        cum += gp
        ins(cum, "2.2.2.2", "Mozilla/5.0 Human")

    # 3.3.3.3 — small sample: 8 events 60 s apart → n_gaps=7 < 12 → IGNORED.
    for i in range(8):
        ins(60 + i * 60, "3.3.3.3", "Mozilla/5.0 Small")

    # 4.4.4.4 — Googlebot on a perfect 30 s cadence → SUPPRESSED by the allowlist.
    for i in range(30):
        ins(60 + i * 30, "4.4.4.4", "Googlebot/2.1 (+http://www.google.com/bot.html)")


def _anchor(con, table: str) -> None:
    """A single row 70 min back — outside the 1 h window but inside the temp
    table, so available_history clears check_baseline without being scanned by
    the window-only detection SQL."""
    now = datetime.now(UTC).replace(microsecond=0)
    ts = (now - timedelta(minutes=70)).isoformat()
    con.execute(f'INSERT INTO {table} ("timestamp", "ip", "ua") VALUES (?, ?, ?)', [ts, "9.9.9.9", "anchor"])


def _seed_gaps(
    con, table: str, ip: str, ua: str, first_offset_s: int, gaps: list[int], *, dups_per_second: int = 1
) -> None:
    """Insert one IP's events following a cumulative list of whole-second
    inter-arrival gaps. ``dups_per_second`` inserts N rows in the SAME second
    (same-second bursts) so n_events can exceed the distinct-second count —
    used to drive the rps gate independently of cadence regularity."""
    now = datetime.now(UTC).replace(microsecond=0)
    offsets = [first_offset_s]
    for g in gaps:
        offsets.append(offsets[-1] + g)
    for off in offsets:
        ts = (now - timedelta(seconds=off)).isoformat()
        for _ in range(dups_per_second):
            con.execute(f'INSERT INTO {table} ("timestamp", "ip", "ua") VALUES (?, ?, ?)', [ts, ip, ua])


def _repeated_card(con, src, *, mask_ips: bool = False) -> dict:
    _insights_cache.clear()
    res = get_insights(con, src, window_hours=1, baseline_hours=1, mask_ips=mask_ips)
    card = next((i for i in res["insights"] if i["id"] == "repeated_patterns"), None)
    assert card is not None, "repeated_patterns card missing from insights payload"
    # The card must have actually run, not short-circuited as a 'needs history'
    # placeholder or a SQL error.
    assert card.get("severity") != "error", f"repeated_patterns errored: {card.get('summary')}"
    return card


# ── End-to-end detection matrix ─────────────────────────────────────────────


def test_detection_matrix(in_memory_duckdb, test_service_source):
    """One seeded table exercises every scenario: steady + UA-rotating beacons
    DETECTED; human/bursty, small-sample, and allowlisted Googlebot IGNORED."""
    table = _safe_table(test_service_source["name"])
    _create_table(in_memory_duckdb, table)
    _seed_repeated_patterns(in_memory_duckdb, table)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    items = {item["label"]: item for item in card["items"]}

    # DETECTED
    assert "1.1.1.1" in items, "steady beacon should be detected"
    assert "1.1.1.3" in items, "UA-rotating beacon should be detected (no UA gate)"

    # IGNORED
    assert "2.2.2.2" not in items, "bursty human traffic must not be flagged (high CV)"
    assert "3.3.3.3" not in items, "small sample (n_gaps < 12) must not be flagged"
    assert "4.4.4.4" not in items, "Googlebot UA must be suppressed by the allowlist"
    assert "9.9.9.9" not in items, "single-event anchor must not be flagged"


def test_steady_beacon_is_critical_and_regular(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_table(in_memory_duckdb, table)
    _seed_repeated_patterns(in_memory_duckdb, table)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    item = next(i for i in card["items"] if i["label"] == "1.1.1.1")

    assert item["severity"] == "critical"  # score 100 and n_events ≥ 30
    assert item["current_val"] == 60.0  # ~every 60 s
    assert item["meta"]["n_events"] == 35
    assert item["meta"]["cv"] < 0.05  # Sheppard-corrected CV ≈ 0
    assert item["meta"]["modal_frac"] >= 0.95
    assert item["meta"]["score"] == 100
    # Card-level severity escalates to critical when any item is critical.
    assert card["severity"] == "critical"


def test_ua_rotating_beacon_detected_with_high_distinct_ua(in_memory_duckdb, test_service_source):
    """Proves D4: a beacon that rotates its UA on every request (high
    distinct_ua) is still flagged — UA diversity is informational, not a gate."""
    table = _safe_table(test_service_source["name"])
    _create_table(in_memory_duckdb, table)
    _seed_repeated_patterns(in_memory_duckdb, table)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    item = next(i for i in card["items"] if i["label"] == "1.1.1.3")

    assert item["meta"]["distinct_ua"] == 30  # a distinct UA per request
    assert item["meta"]["n_events"] == 30
    assert item["severity"] == "critical"


# ── Gate-isolating fixtures (each kills a specific mutation) ─────────────────


def test_jittered_beacon_detected_and_sheppard_correction_applied(in_memory_duckdb, test_service_source):
    """A mildly-jittered slow-path beacon (mode 6 s, a few ±1 s gaps) is DETECTED,
    and the Sheppard-corrected CV is strictly below the naive σ/mean. Removing
    the ``GREATEST(var_gap - 1/12, 0)`` term makes cv == naive → this fails."""
    table = _safe_table(test_service_source["name"])
    _create_table(in_memory_duckdb, table)
    _anchor(in_memory_duckdb, table)
    gaps = [6, 6, 5, 6, 7, 6, 6, 5, 6, 7, 6, 6, 6, 6, 6]  # mean 6, var>0, modal 1.0
    _seed_gaps(in_memory_duckdb, table, "1.1.1.2", "Mozilla/5.0 Jitter", 60, gaps)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    item = next((i for i in card["items"] if i["label"] == "1.1.1.2"), None)
    assert item is not None, "jittered slow-path beacon should be detected"
    m = item["meta"]
    naive_cv = m["stddev_s"] / m["mean_interval_s"]  # uncorrected σ/mean
    assert m["cv"] < naive_cv, f"Sheppard correction not applied: cv={m['cv']} naive={naive_cv}"
    assert 0 < m["cv"] < 0.3  # corrected CV clears the slow-path gate


def test_fast_path_subsecond_cadence_detected(in_memory_duckdb, test_service_source):
    """A 2 s-cadence poller (mean < 5 s, where the Sheppard CV is unreliable) is
    admitted by the modal-only FAST path. The slow path would reject it
    (mean < 5) — deleting the fast-path arm makes this undetected."""
    table = _safe_table(test_service_source["name"])
    _create_table(in_memory_duckdb, table)
    _anchor(in_memory_duckdb, table)
    _seed_gaps(in_memory_duckdb, table, "1.1.1.5", "Mozilla/5.0 Fast", 60, [2] * 14)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    item = next((i for i in card["items"] if i["label"] == "1.1.1.5"), None)
    assert item is not None, "fast-path (2 s cadence) poller should be detected"
    assert 1 <= item["meta"]["mean_interval_s"] < 5
    assert item["meta"]["modal_frac"] >= 0.85


def test_fast_path_below_modal_threshold_ignored(in_memory_duckdb, test_service_source):
    """A fast-cadence IP whose gaps alternate 2/4 s has modal_frac ≈ 0.5 < 0.85,
    so the fast path must NOT admit it (proves the 0.85 threshold gates)."""
    table = _safe_table(test_service_source["name"])
    _create_table(in_memory_duckdb, table)
    _anchor(in_memory_duckdb, table)
    _seed_gaps(in_memory_duckdb, table, "1.1.1.6", "Mozilla/5.0 IrregularFast", 60, [2, 4] * 7)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    assert "1.1.1.6" not in {i["label"] for i in card["items"]}, "fast-path modal < 0.85 must not be flagged"


def test_rps_flood_ignored_despite_perfect_cadence(in_memory_duckdb, test_service_source):
    """14 distinct active seconds, 1 s apart, but 3 requests per second →
    n_events/span ≈ 3.2 ≥ 2. The cadence is perfectly modal (would pass the fast
    path), so only the rps gate excludes it — deleting that gate flags it."""
    table = _safe_table(test_service_source["name"])
    _create_table(in_memory_duckdb, table)
    _anchor(in_memory_duckdb, table)
    _seed_gaps(in_memory_duckdb, table, "1.1.1.7", "Mozilla/5.0 Flood", 60, [1] * 13, dups_per_second=3)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    assert "1.1.1.7" not in {i["label"] for i in card["items"]}, "high-rps flood must be excluded by the rps gate"


def test_mask_ips_masks_label_and_filter(in_memory_duckdb, test_service_source):
    """M3: with mask_ips the label AND meta.filters.ip (which seeds
    investigate_url) are masked; without it the raw IP is present."""
    table = _safe_table(test_service_source["name"])
    _create_table(in_memory_duckdb, table)
    _seed_repeated_patterns(in_memory_duckdb, table)

    unmasked = _repeated_card(in_memory_duckdb, test_service_source, mask_ips=False)
    raw_labels = {i["label"] for i in unmasked["items"]}
    assert "1.1.1.1" in raw_labels

    masked = _repeated_card(in_memory_duckdb, test_service_source, mask_ips=True)
    masked_item = next(i for i in masked["items"] if i["label"] == "1.1.1.xxx")
    assert masked_item["meta"]["filters"]["ip"] == "1.1.1.xxx"
    assert "1.1.1.1" not in {i["label"] for i in masked["items"]}


# ── Row-processor unit tests (pure function) ────────────────────────────────


def test_processor_critical_requires_dense_and_regular():
    p = defs.repeated_patterns_processor
    # score 100 + n_events 30 → critical
    crit = p(("1.1.1.1", 29, 30, 30.0, 0.0, 0.0, 1.0, 30, 870, 30), None, {})
    assert crit["severity"] == "critical"
    # same regularity but sparse (n_events 20 < 30) → warning, not critical
    warn = p(("1.1.1.1", 19, 20, 60.0, 0.0, 0.0, 1.0, 1, 1140, 60), None, {})
    assert warn["severity"] == "warning"


def test_processor_low_score_is_info():
    p = defs.repeated_patterns_processor
    # cv≥1 zeroes the CV term; modal 0.5 → score ≈ 19 → info
    out = p(("5.5.5.5", 40, 50, 3.0, 2.0, 1.0, 0.5, 5, 150, 3), None, {})
    assert out["meta"]["score"] < 70
    assert out["severity"] == "info"


def test_processor_score_boundary_at_90():
    p = defs.repeated_patterns_processor
    # cv 0.1, modal 1.0 → 0.625*0.9 + 0.375 = 0.9375 → 94 ≥ 90
    above = p(("1.1.1.1", 40, 40, 60.0, 1.0, 0.1, 1.0, 1, 2400, 60), None, {})
    # cv 0.2, modal 1.0 → 0.5 + 0.375 = 0.875 → 88 < 90
    below = p(("1.1.1.1", 40, 40, 60.0, 1.0, 0.2, 1.0, 1, 2400, 60), None, {})
    assert above["meta"]["score"] == 94 and above["severity"] == "critical"
    assert below["meta"]["score"] == 88 and below["severity"] == "warning"


def test_processor_masks_ip_when_mask_ips():
    p = defs.repeated_patterns_processor
    masked = p(("203.0.113.42", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, 60), None, {"mask_ips": True})
    assert masked["label"] == "203.0.113.xxx"
    assert masked["meta"]["filters"]["ip"] == "203.0.113.xxx"


def test_processor_keeps_ip_for_admin():
    p = defs.repeated_patterns_processor
    raw = p(("203.0.113.42", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, 60), None, {})
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


def test_processor_computes_rps_and_handles_none_mode():
    p = defs.repeated_patterns_processor
    out = p(("1.1.1.1", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, None), None, {})
    # rps = n_events / span_s
    assert out["meta"]["rps"] == round(35 / 2040, 4)
    assert out["meta"]["mode_gap_s"] is None  # None mode tolerated


def test_severity_logic_clean_when_no_items():
    assert defs.repeated_patterns_severity([]) == "clean"
    assert defs.repeated_patterns_severity([{"severity": "warning"}]) == "warning"
    assert defs.repeated_patterns_severity([{"severity": "info"}, {"severity": "critical"}]) == "critical"


# ── SQL-hygiene guards ──────────────────────────────────────────────────────


def test_bot_ua_regex_contains_no_question_mark():
    """A literal '?' in the inlined regex would inflate the engine's
    sql.count('?') heuristic (it binds window_start to every '?'), breaking the
    parameter count. The curated token list must stay ?-free."""
    assert "?" not in SQL.REPEATED_BOT_UA_REGEX


def test_template_has_exactly_one_bind_after_hydration():
    """After substituting ua_col + the static bot regex, the template must keep
    exactly ONE '?' (the window start)."""
    hydrated = SQL.REPEATED_PATTERNS.format(
        table_name="t",
        ua_col='"ua"',
        bot_ua_regex=SQL.REPEATED_BOT_UA_REGEX,
    )
    assert hydrated.count("?") == 1


def test_mode_is_aliased_not_bare():
    """MODE() must be aliased (bare ``mode`` is a reserved word in DuckDB)."""
    assert "MODE(gap) AS mode_gap" in SQL.REPEATED_PATTERNS


# ── Registration + availability ─────────────────────────────────────────────


def test_repeated_patterns_registered():
    d = registry.get("repeated_patterns")
    assert d is not None
    assert d.title == "Scripted Traffic Patterns"
    assert d.required_fields == ["ip", "timestamp"]
    assert d.row_processor is not None
    assert d.severity_logic is not None


def test_repeated_patterns_in_availability_metadata():
    from backend.core.field_registry import INSIGHT_DEFINITIONS

    entry = next((d for d in INSIGHT_DEFINITIONS if d["id"] == "repeated_patterns"), None)
    assert entry is not None, "repeated_patterns missing from INSIGHT_DEFINITIONS"
    assert entry["required_fields"] == ["ip"]
    # Empty: the card hard-requires only core/always-on fields (ip, timestamp);
    # ua (Group A) is a soft bot-suppression enhancement, not a gate. A non-empty
    # required_groups would gray the card out in the settings/provision previews.
    assert entry["required_groups"] == []
