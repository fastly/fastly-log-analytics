"""``_parse_rum_line`` beacon-shape variants (celery-mode RUM ledger port).

One raw ``rum/raw/*.gz`` line arrives in one of two shapes and the parser has
to pick the right one:

* a **Faro payload** — the whole beacon body in ``rum_body``, parsed through
  ``extract_metrics_from_faro_payload`` into N metrics/exceptions per line;
* a **flat beacon** — ``rum_*`` scalar fields, with the values possibly only
  present in the beacon *URL's* query string (the pixel-GET shape) rather
  than as top-level JSON keys.

Getting the shape detection wrong is silent data loss: a Faro line misread as
flat yields one ``metric_name='unknown'`` row instead of every vital and
exception it carried, and a pixel-GET line whose values live only in the
query string yields a ``metric_value=0.0`` row. So these assert the produced
row contents, never merely that parsing returned.

Companion to tests/core/test_rum_ledger.py, which covers the ledger/DuckLake
side of the same path.
"""

import gzip
import json

from backend.core.ingest import _parse_rum_beacon_file, _parse_rum_line

SERVICE_ID = "svc-parse-variants"


# ── Faro payload shape (rum_body) ──────────────────────────────────────────


def _faro_payload() -> dict:
    return {
        "meta": {
            "browser": {"name": "Firefox", "mobile": True},
            "os": {"name": "Android"},
        },
        "page": {"url": "https://shop.example.com/checkout/cart?step=2"},
        "measurements": [
            {
                "type": "web-vitals",
                "context": {"rating": "needs-improvement"},
                "values": {"lcp": 2600.5, "delta": 12.0},
            }
        ],
        "exceptions": [
            {
                "value": "TypeError: undefined is not a function",
                "stacktrace": {"frames": [{"filename": "bundle.js", "lineno": 918, "colno": 27}]},
            }
        ],
    }


def test_parse_rum_line_extracts_vitals_and_exceptions_from_faro_body():
    """A Faro ``rum_body`` must fan out into one row per extracted metric —
    the web-vital into vitals, the exception into errors with its stack
    frame's file/line/col — not collapse into a single 'unknown' row."""
    log_data = {
        "timestamp": "2026-08-27T10:00:00Z",
        "rum_body": json.dumps(_faro_payload()),
        "rum_cid": "cid-abc",
        "fastly_req_id": "req-123",
        "geo_city": "Portland",
        "geo_country_code": "US",
        "server_pop": "PDX",
        "tls_version": "TLSv1.3",
        "time_to_first_byte": "88.5",
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, SERVICE_ID)

    # 'delta' is excluded by the extractor; only the real vital survives.
    assert [r["metric_name"] for r in vitals_rows] == ["lcp"]
    vital = vitals_rows[0]
    assert vital["metric_value"] == 2600.5
    assert vital["metric_rating"] == "needs-improvement"
    assert vital["pathname"] == "/checkout/cart"
    assert vital["browser"] == "Firefox"
    assert vital["os"] == "Android"
    assert vital["device"] == "Mobile"  # meta.browser.mobile
    assert vital["cid"] == "cid-abc"
    assert vital["req_id"] == "req-123"
    # Geo/TLS/TTFB come off the log line, not the Faro payload.
    assert vital["city"] == "Portland"
    assert vital["country"] == "US"
    assert vital["pop"] == "PDX"
    assert vital["tls"] == "TLSv1.3"
    assert vital["ttfb"] == 88.5

    assert len(errors_rows) == 1
    err = errors_rows[0]
    assert err["error_message"] == "TypeError: undefined is not a function"
    assert err["error_file"] == "bundle.js"
    assert err["error_line"] == 918
    assert err["error_col"] == 27
    assert err["pathname"] == "/checkout/cart"
    assert err["device"] == "Mobile"


def test_parse_rum_line_unwraps_double_encoded_faro_body():
    """Some beacon transports deliver ``rum_body`` double-JSON-encoded (a
    JSON string whose content is itself JSON). The parser unwraps one extra
    layer — without it the payload reads as a str, extraction is skipped,
    and the line silently degrades to a single 'unknown' vitals row."""
    log_data = {
        "timestamp": "2026-08-27T10:00:00Z",
        "rum_body": json.dumps(json.dumps(_faro_payload())),
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, SERVICE_ID)

    assert [r["metric_name"] for r in vitals_rows] == ["lcp"]
    assert len(errors_rows) == 1
    assert errors_rows[0]["error_file"] == "bundle.js"


def test_parse_rum_line_falls_back_to_flat_fields_when_faro_body_is_garbage():
    """An unparseable ``rum_body`` must not lose the line: the flat ``rum_*``
    fields on the same record are still authoritative."""
    log_data = {
        "timestamp": "2026-08-27T10:00:00Z",
        "rum_body": "{not json at all",
        "rum_metric_name": "CLS",
        "rum_metric_value": "0.08",
        "rum_metric_rating": "good",
        "rum_pathname": "/pricing",
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, SERVICE_ID)

    assert errors_rows == []
    assert len(vitals_rows) == 1
    assert vitals_rows[0]["metric_name"] == "CLS"
    assert vitals_rows[0]["metric_value"] == 0.08
    assert vitals_rows[0]["metric_rating"] == "good"
    assert vitals_rows[0]["pathname"] == "/pricing"


def test_parse_rum_line_faro_payload_with_no_metrics_yields_pageview():
    """An empty-but-valid Faro payload still represents a real page view —
    the extractor's fallback row must reach the vitals table rather than the
    line being dropped."""
    log_data = {
        "timestamp": "2026-08-27T10:00:00Z",
        "rum_body": json.dumps({"meta": {}, "page": {"url": "https://example.com/blog/post-1"}}),
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, SERVICE_ID)

    assert errors_rows == []
    assert [r["metric_name"] for r in vitals_rows] == ["pageview"]
    assert vitals_rows[0]["pathname"] == "/blog/post-1"
    assert vitals_rows[0]["metric_value"] == 1.0
    # No mobile hint in meta -> Desktop, and the Chrome/macOS defaults apply.
    assert vitals_rows[0]["device"] == "Desktop"
    assert vitals_rows[0]["browser"] == "Chrome"
    assert vitals_rows[0]["os"] == "macOS"


# ── flat beacon shape: values only in the beacon URL's query string ────────


def test_parse_rum_line_recovers_metric_from_beacon_url_query_string():
    """The pixel-GET beacon carries its values ONLY in the request URL's
    query string. If they aren't lifted out, every such line lands as
    metric_name='unknown', metric_value=0.0 — a silently empty vitals
    table."""
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "url": (
            "/rum-beacon?rum_metric_name=INP&rum_metric_value=145.25"
            "&rum_metric_rating=good&cid=cid-from-query&rum_pathname=/search"
        ),
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, SERVICE_ID)

    assert errors_rows == []
    assert len(vitals_rows) == 1
    row = vitals_rows[0]
    assert row["metric_name"] == "INP"
    assert row["metric_value"] == 145.25
    assert row["metric_rating"] == "good"
    assert row["cid"] == "cid-from-query"
    assert row["pathname"] == "/search"


def test_parse_rum_line_prefers_explicit_fields_over_query_string():
    """Query-string recovery is a fallback, never an override: an explicit
    top-level ``rum_*`` field wins over the same key in the URL."""
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "rum_metric_name": "LCP",
        "rum_metric_value": "1200",
        "rum_metric_rating": "good",
        "rum_cid": "cid-explicit",
        "rum_pathname": "/explicit",
        "url": (
            "/rum-beacon?rum_metric_name=INP&rum_metric_value=999"
            "&rum_metric_rating=poor&cid=cid-query&rum_pathname=/from-query"
        ),
    }

    vitals_rows, _ = _parse_rum_line(log_data, SERVICE_ID)

    row = vitals_rows[0]
    assert row["metric_name"] == "LCP"
    assert row["metric_value"] == 1200
    assert row["metric_rating"] == "good"
    assert row["cid"] == "cid-explicit"
    assert row["pathname"] == "/explicit"


def test_parse_rum_line_uses_rum_raw_query_when_url_absent():
    """``rum_raw_query`` is the VCL-side alias for the beacon URL — it must
    be consulted with the same query-string recovery as ``url``."""
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "rum_raw_query": "/b?rum_metric_name=TTFB&rum_metric_value=60.5",
    }

    vitals_rows, _ = _parse_rum_line(log_data, SERVICE_ID)

    assert vitals_rows[0]["metric_name"] == "TTFB"
    assert vitals_rows[0]["metric_value"] == 60.5


def test_parse_rum_line_keeps_non_numeric_metric_value_from_breaking_the_row():
    """A garbage ``rum_metric_value`` must degrade to 0.0 (a writable
    float64) rather than raise and lose the whole line — but the metric name
    it was reported under is preserved so the bad beacon is still visible."""
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "rum_metric_name": "LCP",
        "rum_metric_value": "not-a-number",
        "rum_pathname": "/broken",
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, SERVICE_ID)

    assert errors_rows == []
    assert vitals_rows[0]["metric_name"] == "LCP"
    assert vitals_rows[0]["metric_value"] == 0.0


def test_parse_rum_line_falls_back_to_referer_path_for_pathname():
    """With no pathname anywhere in the beacon, the referring page's path is
    the only attribution available — dropping to '/' would pile every
    unattributed beacon onto the root path in the pathname breakdowns."""
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "rum_metric_name": "FCP",
        "rum_metric_value": "700",
        "referer": "https://shop.example.com/collections/shoes?page=3",
    }

    vitals_rows, _ = _parse_rum_line(log_data, SERVICE_ID)

    assert vitals_rows[0]["pathname"] == "/collections/shoes"


def test_parse_rum_line_defaults_pathname_to_root_and_collapses_double_slash():
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "rum_metric_name": "FCP",
        "rum_metric_value": "700",
    }
    vitals_rows, _ = _parse_rum_line(log_data, SERVICE_ID)
    assert vitals_rows[0]["pathname"] == "/"

    log_data["rum_pathname"] = "//collections//shoes"
    vitals_rows, _ = _parse_rum_line(log_data, SERVICE_ID)
    assert vitals_rows[0]["pathname"] == "/collections/shoes"


def test_parse_rum_line_routes_flat_error_beacon_to_errors_only():
    """A flat ``rum_error_*`` beacon is an error, not a vital: it must
    produce exactly one errors row and no vitals row (a duplicate vitals row
    would double-count the beacon in every metric panel)."""
    log_data = {
        "timestamp": "2026-08-27T10:02:00Z",
        "rum_error_message": "ReferenceError: gtag is not defined",
        "rum_error_file": "analytics.js",
        "rum_error_line": "17",
        "rum_error_col": "5",
        "rum_pathname": "/account",
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, SERVICE_ID)

    assert vitals_rows == []
    assert len(errors_rows) == 1
    err = errors_rows[0]
    assert err["error_message"] == "ReferenceError: gtag is not defined"
    assert err["error_file"] == "analytics.js"
    assert err["error_line"] == 17
    assert err["error_col"] == 5
    assert err["pathname"] == "/account"


def test_parse_rum_line_returns_none_for_another_services_beacon():
    """Multi-tenant bucket routing: a line tagged for a different service is
    neither ours nor corrupt — the caller must be able to tell the
    difference, so the parser returns None rather than empty lists."""
    assert _parse_rum_line({"rum_service_id": "some-other-service", "rum_metric_name": "LCP"}, SERVICE_ID) is None
    # Our own service_id (either key spelling) parses normally.
    assert _parse_rum_line({"service_id": SERVICE_ID, "rum_metric_name": "LCP"}, SERVICE_ID) is not None


def test_parse_rum_line_falls_back_to_now_for_unparseable_timestamp():
    """A malformed ``timestamp`` must not fail the line — a NULL/absent
    timestamp is unwritable to the lake table (non-null column), so the
    parser substitutes now and keeps the beacon."""
    log_data = {"timestamp": "not-a-timestamp", "rum_metric_name": "LCP", "rum_metric_value": "1"}
    vitals_rows, _ = _parse_rum_line(log_data, SERVICE_ID)
    ts = vitals_rows[0]["timestamp"]
    assert ts is not None
    assert ts.tzinfo is not None


def test_parse_rum_beacon_file_ignores_blank_lines_without_quarantining_them():
    """Beacon files routinely end with a trailing newline. Counting that as
    corruption would quarantine (and alarm on) every single file."""
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        path = tmp.name
    good = json.dumps({"timestamp": "2026-08-27T10:04:00Z", "rum_metric_name": "LCP", "rum_metric_value": "1"})
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write("\n" + good + "\n   \n\n")

    vitals_rows, errors_rows, corrupt_lines = _parse_rum_beacon_file(path, SERVICE_ID)

    assert len(vitals_rows) == 1
    assert errors_rows == []
    assert corrupt_lines == []


def test_parse_rum_beacon_file_quarantines_valid_json_that_is_not_a_beacon_object():
    """A line can be valid JSON yet not a beacon object (a bare array or
    scalar from a truncated/garbled write). The parser raises on it, and the
    file-level reader must capture that as a quarantined line with the
    reason attached — not crash the whole file, which would strand every
    good beacon in it."""
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        path = tmp.name
    good = json.dumps({"timestamp": "2026-08-27T10:04:00Z", "rum_metric_name": "CLS", "rum_metric_value": "0.2"})
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write("[1, 2, 3]\n" + good + "\n")

    vitals_rows, errors_rows, corrupt_lines = _parse_rum_beacon_file(path, SERVICE_ID)

    # The good beacon still lands.
    assert [r["metric_name"] for r in vitals_rows] == ["CLS"]
    assert errors_rows == []
    assert len(corrupt_lines) == 1
    line, reason = corrupt_lines[0]
    assert line == "[1, 2, 3]"
    assert reason.startswith("parse_error: ")
    assert len(reason) <= 200


def test_parse_rum_line_normalizes_naive_timestamp_to_utc():
    """A naive ISO timestamp is assumed UTC (that is what the beacon emits);
    leaving it naive would break the timestamp('us', tz='UTC') arrow column."""
    log_data = {"timestamp": "2026-08-27T10:03:00", "rum_metric_name": "LCP", "rum_metric_value": "1"}
    vitals_rows, _ = _parse_rum_line(log_data, SERVICE_ID)
    ts = vitals_rows[0]["timestamp"]
    assert ts.tzinfo is not None
    assert ts.utcoffset().total_seconds() == 0
    assert (ts.year, ts.month, ts.day, ts.hour, ts.minute) == (2026, 8, 27, 10, 3)
