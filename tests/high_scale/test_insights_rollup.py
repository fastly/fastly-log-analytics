"""Hourly Insights rollup: schema, exactness rules, coverage, backfill, and real-ClickHouse equivalence."""

from __future__ import annotations

import json
import os
import random
import re
import subprocess
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from backend.config import ClickHouseConfig
from backend.core.clickhouse import ClickHouseClient
from backend.high_scale._common import visible_fact_clauses
from backend.high_scale.insights import rollup_backfill
from backend.high_scale.insights.rollup import (
    ROLLUP_DETECTIONS,
    ROLLUP_INSERT_COLUMNS,
    ROLLUP_SHAPES,
    covered_span,
    hour_totals,
    rollup_base_sql,
    rollup_problems,
    rollup_select_sql,
)
from backend.high_scale.insights.sql_queries import DETECTIONS, Detection, _base_sql
from tests.high_scale.conftest import clickhouse_single_node_ddl, free_loopback_port, wait_for_clickhouse_ready

SCHEMA = Path("backend/high_scale/sql/request_schema.sql")


def _squash(sql: str) -> str:
    return re.sub(r"\s*\)", ")", re.sub(r"\(\s*", "(", re.sub(r"\s+", " ", sql))).strip()


def test_schema_creates_rollup_table_and_view_idempotently_from_the_shared_select() -> None:
    text = SCHEMA.read_text()
    table = re.search(r"CREATE TABLE IF NOT EXISTS request_insight_hourly\s*\((.*?)\)\s*ENGINE", text, re.S)
    assert table, "rollup table must be created with IF NOT EXISTS"
    columns = [line.strip().split()[0] for line in table.group(1).strip().splitlines()]
    assert ", ".join(columns) == ROLLUP_INSERT_COLUMNS
    view = re.search(
        r"CREATE MATERIALIZED VIEW IF NOT EXISTS request_insight_hourly_mv\s+TO request_insight_hourly\s+AS (.*?);",
        text,
        re.S,
    )
    assert view, "rollup view must be created with IF NOT EXISTS and write TO the table"
    assert _squash(view.group(1)) == _squash(rollup_select_sql())
    # Concurrent pod start-ups run this file at once: nothing may drop, rewrite or populate existing data.
    assert "POPULATE" not in text and not re.search(r"(DROP|ALTER) TABLE\s+request_insight_hourly", text)
    assert text.index("CREATE TABLE IF NOT EXISTS request_insight_hourly") < text.index("request_insight_hourly_mv")


@pytest.mark.parametrize("insight_id", sorted(ROLLUP_DETECTIONS))
def test_converted_detections_read_baseline_hours_from_the_rollup(insight_id: str) -> None:
    shape = ROLLUP_DETECTIONS[insight_id]
    assert rollup_problems(DETECTIONS[insight_id], shape) == []
    sql = rollup_base_sql(DETECTIONS[insight_id], insight_id, shape)
    assert sql.startswith(f"/* insight:{insight_id} */ WITH base AS (")
    assert sql.count("FROM request_facts") == 1 and sql.count("FROM request_insight_hourly") == 1
    assert f"shape = '{shape}'" in sql
    # Both branches apply the same publication fence, so a rollup row is visible exactly when its batch is.
    fence = " AND ".join(visible_fact_clauses("request"))
    assert sql.count(fence) == 2
    assert "event_timestamp < {rollup_start:DateTime64(3)} OR event_timestamp >= {rollup_end:DateTime64(3)}" in sql
    assert "bucket_start >= {rollup_start:DateTime64(3)} AND bucket_start < {rollup_end:DateTime64(3)}" in sql
    rollup_branch = sql.split("UNION ALL", 1)[1]
    assert "custom_fields" not in rollup_branch and "event_timestamp" not in rollup_branch
    assert not re.search(r"\bcount(If)?\(", sql)


def test_every_other_detection_is_rejected_by_every_shape() -> None:
    for insight_id, detection in DETECTIONS.items():
        if insight_id in ROLLUP_DETECTIONS:
            continue
        for shape in ROLLUP_SHAPES:
            assert rollup_problems(detection, shape), (insight_id, shape)
    with pytest.raises(ValueError, match="latency_regression cannot use"):
        rollup_base_sql(DETECTIONS["latency_regression"], "latency_regression", "url_status")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT url, uniqExact(url) AS u FROM base GROUP BY url",
        "SELECT url, sumIf(is_b, 1) AS b FROM base GROUP BY url",
        "SELECT url, avgIf(status, is_b) AS a FROM base GROUP BY url",
        "SELECT url, count(DISTINCT status) AS c FROM base GROUP BY url",
        "SELECT url, max(status) AS m FROM base GROUP BY url",
    ],
)
def test_rollup_rejects_anything_but_request_counts(sql: str) -> None:
    assert rollup_problems(Detection(("url", "status"), sql), "url_status")


def test_rollup_rejects_fields_outside_the_shape() -> None:
    problems = rollup_problems(Detection(("url", "cache"), "SELECT url, count() FROM base GROUP BY url"), "url_status")
    assert problems == ["fields ['cache'] are not in shape url_status"]


def _hour(base: datetime, offset: int) -> int:
    return int((base + timedelta(hours=offset)).timestamp())


def test_covered_span_takes_the_longest_matching_run_of_whole_baseline_hours() -> None:
    day = datetime(2026, 11, 5, tzinfo=UTC)
    baseline_start, window_start = day.replace(hour=10, minute=17), day.replace(hour=16, minute=23)
    raw = {_hour(day, h): (10 + h, 1000 + h) for h in range(10, 17)}
    rollup = {hour: totals for hour, totals in raw.items() if hour != _hour(day, 12)}
    # 10:00 and 16:00 are partial hours, so the rollup can only stand in for 11:00-16:00.
    assert covered_span(baseline_start, window_start, raw, raw) == (day.replace(hour=11), day.replace(hour=16))
    assert covered_span(baseline_start, window_start, raw, rollup) == (day.replace(hour=13), day.replace(hour=16))
    same_count_other_batch = {**raw, _hour(day, 14): (24, 1)}
    assert covered_span(baseline_start, window_start, raw, same_count_other_batch) == (
        day.replace(hour=11),
        day.replace(hour=14),
    )
    # Hours without facts match an empty rollup; a run with no rows at all is not worth a rollup read.
    sparse = {_hour(day, 13): (5, 7)}
    assert covered_span(baseline_start, window_start, sparse, sparse) == (day.replace(hour=11), day.replace(hour=16))
    assert covered_span(baseline_start, window_start, {}, {}) is None
    assert covered_span(baseline_start, baseline_start + timedelta(minutes=30), raw, raw) is None


def test_hour_totals_accepts_quoted_64_bit_integers() -> None:
    assert hour_totals([("1700000000", "3", "18446744073709551615")]) == {1700000000: (3, 18446744073709551615)}


# ── Backfill CLI ──

_HOUR = datetime(2026, 11, 5, 9, tzinfo=UTC)
_A, _B, _C, _D = (str(uuid.UUID(int=i)) for i in range(1, 5))


class _BackfillClient:
    def __init__(self, raw: dict[datetime, dict[str, int]], rolled: dict[datetime, dict[str, dict[str, int]]]) -> None:
        self.raw, self.rolled = raw, rolled
        self.writes: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        params = params or {}
        assert params["service_id"] == "svc-1" and "svc-1" not in sql
        assert params["hour_end"] - params["hour_start"] == timedelta(hours=1)
        if sql.startswith(("ALTER", "INSERT")):
            self.writes.append((sql, params))
            return []
        hour = params["hour_start"]
        if "FROM request_facts" in sql:
            return [{"batch_id": b, "row_count": str(n)} for b, n in self.raw.get(hour, {}).items()]
        assert "FROM request_insight_hourly" in sql
        return [
            {"batch_id": b, "shape": shape, "row_count": n}
            for b, shapes in self.rolled.get(hour, {}).items()
            for shape, n in shapes.items()
        ]


def _full(count: int) -> dict[str, int]:
    return dict.fromkeys(ROLLUP_SHAPES, count)


def _backfill_client() -> _BackfillClient:
    older = _HOUR - timedelta(hours=1)
    return _BackfillClient(
        raw={_HOUR: {_A: 5, _B: 7, _C: 2}, older: {_A: 3}},
        rolled={
            _HOUR: {_A: _full(5), _B: {**_full(7), "geo": 6}, _D: _full(4)},
            older: {_A: _full(3)},
        },
    )


def _run_backfill(client: _BackfillClient, **kwargs: Any) -> tuple[list[rollup_backfill.HourPlan], list[str]]:
    lines: list[str] = []
    kwargs = {"days": 1, "now": _HOUR + timedelta(hours=2, minutes=40), **kwargs}
    plans = rollup_backfill.run(client, "svc-1", emit=lines.append, **kwargs)
    return plans, lines


def test_backfill_dry_run_classifies_batches_and_writes_nothing() -> None:
    client = _backfill_client()
    plans, lines = _run_backfill(client, apply=False)
    plan = plans[0]
    assert plan.hour == _HOUR and len(plans) == 22
    assert (plan.ok, plan.mismatched, plan.missing, plan.orphan) == ([_A], [_B], [_C], [_D])
    assert plans[1].ok == [_A] and not plans[1].to_insert and not plans[1].to_delete
    assert client.writes == []
    assert lines[0] == "2026-11-05T09:00Z rows=14 ok=1 missing=1 mismatched=1 orphan=1"
    assert lines[-1] == "would rewrite 1 hour(s) (dry run; add --apply)"


def test_backfill_apply_replaces_only_batches_that_disagree_in_bounded_statements() -> None:
    client = _backfill_client()
    _run_backfill(client, apply=True, batch_size=1)
    statements = [sql for sql, _ in client.writes]
    assert [s.split()[0] for s in statements] == ["ALTER", "ALTER", "INSERT", "INSERT"]
    assert f"batch_id IN ('{_B}')" in statements[0] and f"batch_id IN ('{_D}')" in statements[1]
    assert all("SETTINGS mutations_sync=2" in s and "bucket_start >= {hour_start" in s for s in statements[:2])
    assert f"batch_id IN ('{_B}')" in statements[2] and f"batch_id IN ('{_C}')" in statements[3]
    for sql in statements[2:]:
        assert sql.startswith(f"INSERT INTO request_insight_hourly ({ROLLUP_INSERT_COLUMNS}) SELECT ")
        assert "event_timestamp >= {hour_start:DateTime64(3)} AND event_timestamp < {hour_end:DateTime64(3)}" in sql
    assert all(params["hour_start"] == _HOUR for _, params in client.writes)


def test_backfill_is_resumable_and_bounded() -> None:
    done = _BackfillClient(raw={_HOUR: {_A: 5}}, rolled={_HOUR: {_A: _full(5)}})
    _run_backfill(done, apply=True)
    assert done.writes == []
    client = _backfill_client()
    _, lines = _run_backfill(client, apply=True, max_hours=0)
    assert client.writes == [] and lines[0].startswith("stopping after 0 rewritten hour(s)")
    for bad in ({"days": 31}, {"batch_size": 0}, {"batch_size": 501}, {"skip_recent_hours": 0}):
        with pytest.raises(ValueError):
            _run_backfill(_backfill_client(), apply=False, **bad)
    with pytest.raises(ValueError, match="service id"):
        rollup_backfill.run(_backfill_client(), "svc'; DROP", apply=False)
    with pytest.raises(ValueError):
        rollup_backfill.apply_hour(client, "svc-1", rollup_backfill.HourPlan(hour=_HOUR, missing=["x') OR 1=1 --"]), 10)


# ── Real ClickHouse equivalence (explicit gate) ──

_CH_GATE = pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
IMAGE = os.getenv("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13")
DB = "fla_rollup_it"
SERVICE = "svc-rollup"
OTHER_SERVICE = "svc-other"
# Fixed analyst clamp: a 6 h window after a 48 h baseline, neither edge on an hour boundary.
END = (datetime.now(UTC) - timedelta(hours=3)).replace(minute=37, second=0, microsecond=0)
WINDOW_START = END - timedelta(hours=6)
BASELINE_START = END - timedelta(hours=54)
LEGACY = str(uuid.uuid5(uuid.NAMESPACE_URL, "legacy"))
PENDING = str(uuid.uuid5(uuid.NAMESPACE_URL, "pending"))
UNPUBLISHED = str(uuid.uuid5(uuid.NAMESPACE_URL, "unpublished"))
OTHER = str(uuid.uuid5(uuid.NAMESPACE_URL, "other"))


def _pick(rng: random.Random, weighted: dict[Any, float]) -> Any:
    return rng.choices(list(weighted), weights=list(weighted.values()))[0]


def _request(rng: random.Random, when: datetime, batch: str, service: str = SERVICE) -> dict[str, Any]:
    w = when >= WINDOW_START
    urls = {"/": 30, "/api": 20, "/img/logo.png": 20, "/cart": 15, "/admin": 3, "/static/app.js": 12}
    if w:
        urls |= {"/wp-login.php": 3, "/.env": 2}
    url = _pick(rng, urls)
    places = {
        ("US", "Ashburn", "VA"): 30,
        ("US", "San Jose", "CA"): 25,
        ("DE", "Berlin", "BE"): 15,
        ("NG", "Lagos", "LA"): 20 if w else 2,
        ("GB", "London", "ENG"): 15 if w else 20,
        ("FR", "", ""): 5 if w else 8,
    }
    if w:
        places |= {("KP", "Pyongyang", "PY"): 3, ("IR", "Tehran", "TH"): 2, ("US", "Atlantis", "XX"): 5}
    country, city, region = _pick(rng, places)
    if url == "/api" and w:
        status = _pick(rng, {"500": 0.4, "200": 0.6})
    elif city == "Berlin" and w:
        status = _pick(rng, {"503": 0.3, "200": 0.7})
    else:
        status = _pick(rng, {"404": 0.05, "500": 0.01, "200": 0.94})
    if url == "/img/logo.png":
        cache = _pick(rng, {"HIT": 0.2, "MISS": 0.8} if w else {"HIT": 0.9, "MISS": 0.1})
    elif url == "/cart":
        cache = _pick(rng, {"PASS": 0.8, "MISS": 0.2} if w else {"PASS": 0.1, "MISS": 0.9})
    else:
        cache = _pick(
            rng,
            {"HIT": 0.35, "HIT-STALE": 0.05, "MISS": 0.5, "PASS": 0.1} if w else {"HIT": 0.7, "MISS": 0.2, "PASS": 0.1},
        )
    ost = ""
    if cache in ("MISS", "PASS"):
        ost = _pick(rng, {"503": 0.08, "200": 0.92} if w else {"500": 0.01, "200": 0.99})
    asn = _pick(
        rng,
        {"7922": 0.25, "3320": 0.1, "16509": 0.3, "14061": 0.15, "99999": 0.1, "": 0.1}
        if w
        else {"7922": 0.5, "3320": 0.2, "16509": 0.05, "14061": 0.1, "": 0.15},
    )
    if asn == "14061":
        p_type = _pick(rng, {"hosting": 0.6, "residential": 0.4} if w else {"hosting": 0.05, "residential": 0.95})
    else:
        p_type = _pick(
            rng,
            {"": 0.55, "hosting": 0.3, "vpn": 0.1, "residential": 0.05}
            if w
            else {"": 0.9, "hosting": 0.05, "vpn": 0.03, "residential": 0.02},
        )
    c_type, c_speed = _pick(
        rng,
        {("cable", "broadband"): 0.4, ("wifi", "broadband"): 0.2, ("mobile", "3g"): 0.4}
        if w
        else {("cable", "broadband"): 0.7, ("wifi", "broadband"): 0.25, ("mobile", "3g"): 0.05},
    )
    browsers = {f"Mozilla/5.0 Browser{i}": 1.0 for i in range(5)}
    fields = {
        "status": status,
        "cache": cache,
        "transport": _pick(rng, {"quic": 0.2, "tcp": 0.75, "": 0.05} if w else {"quic": 0.6, "tcp": 0.35, "": 0.05}),
        "ost": ost,
        "p_type": p_type,
        "method": _pick(
            rng,
            {"GET": 0.6, "POST": 0.3, "PUT": 0.05, "HEAD": 0.05} if w else {"GET": 0.9, "POST": 0.05, "HEAD": 0.05},
        ),
        "c_type": c_type,
        "c_speed": c_speed,
        "city": city,
        "region": region,
        "asn": asn,
        "ua": _pick(rng, {"python-requests/2.31": 4.0, **browsers} if w else {**browsers, "": 0.2}),
        "referer": _pick(
            rng,
            {"": 0.4, "https://spam.example": 0.35, "https://google.com": 0.2, "https://news.example": 0.05}
            if w
            else {"": 0.6, "https://google.com": 0.3, "https://news.example": 0.1},
        ),
        "waf_sig": _pick(
            rng,
            {"": 0.7, "SQLI, XSS": 0.15, "BOT-ANALYSIS, TRAVERSAL": 0.1, "XSS": 0.05}
            if w
            else {"": 0.97, "BOT-ANALYSIS": 0.02, "SQLI": 0.01},
        ),
        "elapsed": str(rng.randint(1000, 90000)),
    }
    return {
        "service_id": service,
        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "event_timestamp": when.isoformat(),
        "ingest_timestamp": when.isoformat(),
        "source_object_key": "raw/fixture.gz",
        "source_object_version": "v1",
        "line_ordinal": 0,
        "transform_version": "t1",
        "batch_id": batch,
        "publication_state": "visible",
        "country": country,
        "client_ip": f"203.0.113.{rng.randint(1, 250)}",
        "url": url,
        "custom_fields": fields,
        "cmcd": {},
    }


def _batches() -> dict[str, list[dict[str, Any]]]:
    """Rows grouped by batch: one visible batch per hour plus legacy, pending, unpublished and other-service rows."""
    rng = random.Random(20261105)
    batches: dict[str, list[dict[str, Any]]] = {}
    first_hour = BASELINE_START.replace(minute=0)
    hour = first_hour - timedelta(hours=1)
    while hour <= END:
        rate = 400 if hour >= WINDOW_START - timedelta(hours=1) else 60
        batch = str(uuid.uuid5(uuid.NAMESPACE_URL, f"hour-{hour.isoformat()}"))
        for _ in range(rate):
            when = hour + timedelta(seconds=rng.randrange(3600))
            if when <= END + timedelta(minutes=30):
                batches.setdefault(batch, []).append(_request(rng, when, batch))
        if hour.hour % 7 == 0:
            for extra, count in ((PENDING, 40), (UNPUBLISHED, 30), (OTHER, 50)):
                service = OTHER_SERVICE if extra == OTHER else SERVICE
                batches.setdefault(extra, []).extend(
                    _request(rng, hour + timedelta(seconds=rng.randrange(3600)), extra, service) for _ in range(count)
                )
        hour += timedelta(hours=1)
    # Rows written before the rollup existed: a contiguous block of baseline hours with no rollup rows.
    for offset in range(20, 23):
        legacy_hour = first_hour + timedelta(hours=offset)
        batches.setdefault(LEGACY, []).extend(
            _request(rng, legacy_hour + timedelta(seconds=rng.randrange(3600)), LEGACY) for _ in range(80)
        )
    return batches


class _Server:
    def __init__(self, port: int) -> None:
        self.base = f"http://127.0.0.1:{port}/"
        self.port = port

    def sql(self, sql: str, body: str | None = None) -> str:
        params = {"database": DB, "date_time_input_format": "best_effort"}
        if body is not None:
            params["query"] = sql
        response = httpx.post(
            self.base, params=params, content=body if body is not None else sql, auth=("admin", "adminpw"), timeout=120
        )
        if response.status_code != 200:
            raise RuntimeError(f"{sql[:80]} -> {response.text[:400]}")
        return response.text

    def insert(self, table: str, rows: list[dict[str, Any]]) -> None:
        self.sql(f"INSERT INTO {table} FORMAT JSONEachRow", "\n".join(json.dumps(r) for r in rows))

    def publish(self, batch: str, state: str, version: int) -> None:
        service = OTHER_SERVICE if batch == OTHER else SERVICE
        self.insert(
            "high_scale_batch_publications",
            [
                {
                    "batch_id": batch,
                    "service_id": service,
                    "domain": "request",
                    "generation": "1",
                    "batch_digest": "fixture",
                    "expected_rows": 1,
                    "visible_rows": 1,
                    "quorum_acked": 1,
                    "publication_state": state,
                    "manifest_version": version,
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            ],
        )

    def client(self) -> ClickHouseClient:
        client = ClickHouseClient(
            ClickHouseConfig(
                host="127.0.0.1", port=self.port, database=DB, user="admin", password="adminpw", secure=False
            )
        )
        client._enable_cf_rewrite_in_tests = True  # type: ignore[attr-defined]
        return client


@pytest.fixture(scope="module")
def server() -> Iterator[_Server]:
    port = free_loopback_port()
    name = f"fla-insights-rollup-it-{uuid.uuid4().hex[:8]}"
    subprocess.run(
        [
            "docker", "run", "-d", "--rm", "--name", name, "-p", f"127.0.0.1:{port}:8123",
            "-e", "CLICKHOUSE_USER=admin", "-e", "CLICKHOUSE_PASSWORD=adminpw",
            "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1", IMAGE,
        ],
        check=True,
        capture_output=True,
    )  # fmt: skip
    srv = _Server(port)
    try:
        wait_for_clickhouse_ready(srv.base)
        httpx.post(srv.base, content=f"CREATE DATABASE {DB}", auth=("admin", "adminpw")).raise_for_status()
        statements = clickhouse_single_node_ddl()
        view = [
            s for s in statements if s.startswith("CREATE MATERIALIZED VIEW IF NOT EXISTS request_insight_hourly_mv")
        ]
        assert len(view) == 1
        for statement in statements:
            if statement not in view:
                srv.sql(statement)
        batches = _batches()
        srv.insert("request_facts", batches.pop(LEGACY))
        for statement in statements:
            srv.sql(statement)  # a second start-up, now with the view: every statement is idempotent
        for batch, rows in batches.items():
            for start in range(0, len(rows), 500):
                srv.insert("request_facts", rows[start : start + 500])
        for version, batch in enumerate([LEGACY, *(b for b in batches if b not in (PENDING, UNPUBLISHED))]):
            srv.publish(batch, "visible", version + 1)
        srv.publish(PENDING, "pending", 1)
        yield srv
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True)


def _params(**extra: Any) -> dict[str, Any]:
    from backend.repositories.insights.shaping import AUTH_REGEX, BOT_UA_REGEX, COMPRESSIBLE_REGEX, PROBE_REGEX

    return {
        "service_id": SERVICE,
        "baseline_start": BASELINE_START,
        "window_start": WINDOW_START,
        "window_end": END,
        "window_hours": 6.0,
        "baseline_hours": 48.0,
        "probe_regex": PROBE_REGEX,
        "compress_regex": COMPRESSIBLE_REGEX,
        "auth_regex": AUTH_REGEX,
        "bot_ua_regex": BOT_UA_REGEX,
        **extra,
    }


def _rows(client: ClickHouseClient, sql: str, params: dict[str, Any]) -> list[str]:
    return sorted(json.dumps(row, sort_keys=True) for row in client.execute(sql, params))


def _assert_equivalent(client: ClickHouseClient, span: tuple[datetime, datetime]) -> int:
    fired = 0
    params = _params(rollup_start=span[0], rollup_end=span[1])
    for insight_id, shape in sorted(ROLLUP_DETECTIONS.items()):
        detection = DETECTIONS[insight_id]
        raw = _rows(client, _base_sql(detection, insight_id), params)
        rolled = _rows(client, rollup_base_sql(detection, insight_id, shape), params)
        assert rolled == raw, insight_id
        fired += bool(raw)
    return fired


def _plan_changes(client: ClickHouseClient) -> list[rollup_backfill.HourPlan]:
    plans = rollup_backfill.run(client, SERVICE, apply=False, days=3, emit=lambda _: None)
    return [plan for plan in plans if plan.to_insert or plan.to_delete]


@_CH_GATE
def test_rollup_matches_raw_facts_for_every_converted_detection(server: _Server) -> None:
    client = server.client()
    # Before the backfill the legacy hours have no rollup rows: the evaluator must keep reading them raw.
    stale = _plan_changes(client)
    assert {plan.hour for plan in stale} == {
        BASELINE_START.replace(minute=0) + timedelta(hours=h) for h in range(20, 23)
    }
    assert all(plan.missing == [LEGACY] and not plan.mismatched and not plan.orphan for plan in stale)
    spans = _evaluated_spans(client)
    assert set(spans) == set(ROLLUP_DETECTIONS)
    legacy_end = BASELINE_START.replace(minute=0) + timedelta(hours=23)
    assert set(spans.values()) == {(legacy_end, WINDOW_START.replace(minute=0))}
    assert _assert_equivalent(client, next(iter(spans.values()))) >= 16

    rollup_backfill.run(client, SERVICE, apply=True, days=3, batch_size=2, emit=lambda _: None)
    assert _plan_changes(client) == []
    full = (BASELINE_START.replace(minute=0) + timedelta(hours=1), WINDOW_START.replace(minute=0))
    assert set(_evaluated_spans(client).values()) == {full}
    assert _assert_equivalent(client, full) >= 16

    # Publishing the pending batch changes raw and rollup results together.
    server.publish(PENDING, "visible", 10_000)
    client._pub_fence_cache.clear()  # type: ignore[attr-defined]
    assert _assert_equivalent(client, full) >= 16

    # A failed-batch cleanup removes the batch from the rollup as well.
    victim = str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"hour-{(BASELINE_START.replace(minute=0) + timedelta(hours=30)).isoformat()}")
    )
    client.delete_batch_rows("request_facts", victim)
    remaining = server.sql(f"SELECT count() FROM request_insight_hourly WHERE batch_id = '{victim}'").strip()
    assert remaining == "0"
    assert _assert_equivalent(client, full) >= 16


def _evaluated_spans(client: ClickHouseClient) -> dict[str, tuple[datetime, datetime]]:
    from backend.high_scale.insights import evaluator

    seen: dict[str, tuple[datetime, datetime]] = {}

    class Spy:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            marker = re.match(r"/\* insight:(\w+) \*/", sql)
            if marker and "FROM request_insight_hourly" in sql and params and "rollup_start" in params:
                seen[marker.group(1)] = (params["rollup_start"], params["rollup_end"])
            return client.execute(sql, params)

    service = SimpleNamespace(service_id=SERVICE, client=Spy())
    payload, partial = evaluator._evaluate(
        service,  # type: ignore[arg-type]
        6.0,
        48.0,
        None,
        END.isoformat(),
        False,
        "",
        {},
    )
    assert not partial and payload["insights"]
    return seen


@_CH_GATE
def test_evaluator_cards_are_identical_with_and_without_the_rollup(
    server: _Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    from backend.high_scale.insights import evaluator

    client = server.client()

    def cards() -> list[dict[str, Any]]:
        payload, partial = evaluator._evaluate(
            SimpleNamespace(service_id=SERVICE, client=client),  # type: ignore[arg-type]
            6.0,
            48.0,
            None,
            END.isoformat(),
            False,
            "",
            {},
        )
        assert not partial
        return [
            {**card, "items": sorted(json.dumps(item, sort_keys=True) for item in card.get("items") or [])}
            for card in payload["insights"]
        ]

    with_rollup = cards()
    monkeypatch.setattr(evaluator, "ROLLUP_DETECTIONS", {})
    assert cards() == with_rollup
    assert sum(card["severity"] != "clean" for card in with_rollup if card["id"] in ROLLUP_DETECTIONS) >= 12
