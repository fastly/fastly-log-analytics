"""Unit tests for FOS client, CDN helpers, DMA/ASN enrichment, usage reconciliation, and HLL benchmarks."""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import backend.core.fos_client as fos_mod
import backend.core.metadata as metadata_db
from backend.core.fos_client import (
    _delete_objects_robust,
    _get_dma_map,
    _get_fos_client,
    _safe_cdn_url,
    _safe_table_name,
    backfill_fastly_edge_writes,
    clear_fos_client,
    enrich_asn_labels,
    format_asn_label,
    fos_reachable,
    get_asn_names,
    get_raw_tree_node,
    is_configured,
    log_usage_calls,
    purge_usage_log,
    reconcile_fastly_stats,
    update_cron_duration,
)
from backend.core.sql_validator import escape_sql_literal
from backend.utils.hll import HyperLogLog


# ── Helpers & Safety Tests ──
def test_safe_table_name_and_is_configured(fos_source):
    assert _safe_table_name("my-service.prod/v1") == "logs_my_service_prod_v1"
    assert _safe_table_name("123_svc") == "logs_123_svc"

    assert is_configured(fos_source) is True
    assert is_configured({**fos_source, "bucket": ""}) is False
    assert is_configured({**fos_source, "access_key_id": ""}) is False
    assert is_configured({"access_level": "read_only", "cdn_url": "", "cdn_secret": "", "bucket": "b"}) is False


# ── CDN URL Validation Tests ──
def test_cdn_url_validation():
    for url, expected in [
        ("https://my-service.global.ssl.fastly.net", True),
        ("https://example.fastly.net:8443", True),
        ("https://bucket.fastlystorage.app", True),
        ("http://my-service.global.ssl.fastly.net", False),
        ("file:///etc/passwd", False),
        ("ftp://evil.com", False),
        ("https://", False),
        ("https://evil.example.com", False),
        ("", False),
        (None, False),
    ]:
        out = _safe_cdn_url(url)
        if expected:
            assert out == url
        else:
            assert out is None


# ── Usage Logging Helper Tests ──
def test_usage_logging_helper(fos_source, monkeypatch):
    logged = []
    monkeypatch.setattr("backend.config.is_usage_logging_enabled", lambda *_a: True)
    monkeypatch.setattr("backend.core.metadata.log_usage_calls", lambda sid, calls, **kw: logged.append((sid, calls)))
    log_usage_calls(fos_source, [{"method": "GET"}])
    assert len(logged) == 1
    log_usage_calls({"name": ""}, [{"method": "GET"}])
    assert len(logged) == 1


# ── FOS Client & Robust Delete Tests ──
def test_get_fos_client_pooling_eviction_and_instrumentation(fos_source, monkeypatch):
    clear_fos_client()
    fake_raw_client = MagicMock()
    fake_raw_client.get_object.return_value = {"Body": io.BytesIO(b"hello")}
    fake_raw_client.head_object.return_value = {"ContentLength": 5}
    fake_raw_client.put_object.return_value = {}
    fake_raw_client.delete_object.return_value = {}
    fake_raw_client.delete_objects.return_value = {"Deleted": [{"Key": "k1"}]}

    monkeypatch.setattr("boto3.client", lambda *a, **kw: fake_raw_client)
    monkeypatch.setattr("backend.utils.telemetry.start_proxy_server", lambda: None)
    monkeypatch.setattr("backend.utils.telemetry.proxy_endpoint", lambda: "http://127.0.0.1:19999")
    monkeypatch.setattr("backend.utils.telemetry.install_boto3_proxy_hook", lambda *_a: None)

    c1 = _get_fos_client(fos_source)
    c2 = _get_fos_client(fos_source)
    assert c1._obj is c2._obj

    c1.get_object(Bucket="b", Key="k1")
    c1.head_object(Bucket="b", Key="k1")
    c1.put_object(Bucket="b", Key="k1", Body=b"abc")
    c1.delete_object(Bucket="b", Key="k1")
    c1.delete_objects(Bucket="b", Delete={"Objects": [{"Key": "k1"}, {"Key": "k2"}]})

    assert fake_raw_client.get_object.called
    assert fake_raw_client.delete_objects.called

    clear_fos_client(fos_source)
    assert len(fos_mod._fos_client_cache) == 0


def test_get_fos_client_paginator_and_errors(fos_source, monkeypatch):
    clear_fos_client()
    monkeypatch.setattr(fos_mod, "_get_fos_client", _get_fos_client)
    fake_raw = MagicMock()
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "raw/a.gz", "Size": 10}, {"Key": "raw/sub/b.gz", "Size": 20}]},
        {"Contents": [{"Key": "raw/c.gz", "Size": 30}]},
    ]
    fake_raw.get_paginator.return_value = fake_paginator
    monkeypatch.setattr("boto3.client", lambda *a, **kw: fake_raw)
    monkeypatch.setattr("backend.utils.telemetry.start_proxy_server", lambda: None)
    monkeypatch.setattr("backend.utils.telemetry.proxy_endpoint", lambda: "http://127.0.0.1:19999")
    monkeypatch.setattr("backend.utils.telemetry.install_boto3_proxy_hook", lambda *_a: None)

    client = _get_fos_client(fos_source)
    paginator = client.get_paginator("list_objects_v2", caller_hint="my_lister")
    pages = list(paginator.paginate(Bucket="b", Prefix="raw/"))
    assert len(pages) == 2

    tree = get_raw_tree_node({**fos_source, "prefix": ""}, prefix_filter="", root="raw")
    assert len(tree["children"]) == 3

    fake_raw.list_objects_v2.return_value = {"Contents": []}
    assert fos_reachable(fos_source)["reachable"] is True

    fake_paginator.paginate.side_effect = RuntimeError("paginate failed")
    with pytest.raises(RuntimeError, match="paginate failed"):
        list(client.get_paginator("list_objects_v2").paginate(Bucket="b"))

    fake_raw.get_object.side_effect = RuntimeError("s3 down")
    with pytest.raises(RuntimeError, match="s3 down"):
        client.get_object(Bucket="b", Key="k")


def test_delete_objects_robust_batching_and_fallback():
    fos = MagicMock()
    fos.delete_objects.return_value = {}
    assert _delete_objects_robust(fos, "b", []) == 0
    assert _delete_objects_robust(fos, "b", ["k1", "k2"]) == 2

    def _fail_batch(**kw):
        raise RuntimeError("Batch delete unsupported")

    fos_fallback = MagicMock()
    fos_fallback.delete_objects.side_effect = _fail_batch
    deleted_single = []

    def _del_one(Bucket, Key):
        if Key == "bad":
            raise RuntimeError("cannot delete bad")
        deleted_single.append(Key)

    fos_fallback.delete_object.side_effect = _del_one
    assert _delete_objects_robust(fos_fallback, "b", ["ok1", "bad", "ok2"]) == 2
    assert deleted_single == ["ok1", "ok2"]


# ── DMA Map & ASN Enrichment Tests ──
def test_get_dma_map_variants_and_cache(tmp_path, monkeypatch):
    fos_mod._dma_map_cache = None

    monkeypatch.setattr("os.path.exists", lambda _p: False)
    assert _get_dma_map() == {}
    assert _get_dma_map() == {}

    fos_mod._dma_map_cache = None
    geojson = {
        "features": [
            {"properties": {"dma_code": 501, "dma_name": "New York"}},
            {"properties": {"dma_code": "803", "name": "Los Angeles"}},
            {"properties": {"dma_code": 602, "dma1": "Chicago"}},
            {"properties": {}},
        ]
    }
    plain_file = tmp_path / "dma.json"
    plain_file.write_text(json.dumps(geojson))
    monkeypatch.setattr("os.path.exists", lambda _p: True)
    orig_open = open
    monkeypatch.setattr("builtins.open", lambda _p, *a, **kw: orig_open(plain_file, *a, **kw))
    assert _get_dma_map() == {"501": "New York", "803": "Los Angeles", "602": "Chicago"}

    fos_mod._dma_map_cache = None
    monkeypatch.setattr("builtins.open", MagicMock(side_effect=OSError("disk error")))
    assert _get_dma_map() == {}


def test_asn_helpers_enrich_lookup_and_format():
    values = [{"value": "7922"}, {"value": "GoogleBot"}, {"value": "15169"}, {"value": "country-code"}]
    with patch("backend.core.fos_client.get_asn_names", return_value={7922: "Comcast Cable", 15169: "Google"}):
        out = enrich_asn_labels(values, "svc-1")
    assert out is values
    assert values[0]["label"] == "Comcast Cable (7922)"
    assert "label" not in values[1]
    assert values[2]["label"] == "Google (15169)"
    assert "label" not in values[3]

    assert get_asn_names("svc", []) == {}
    assert get_asn_names("", [7922]) == {}
    with (
        patch("backend.core.metadata.lookup_asn_names", return_value={7922: "Comcast Cable", 15169: "Google"}),
        patch("backend.core.metadata.upsert_asn_names") as mock_upsert,
    ):
        assert get_asn_names("svc", [7922, 15169]) == {7922: "Comcast Cable", 15169: "Google"}
    mock_upsert.assert_not_called()

    assert format_asn_label(7922, "Comcast Cable") == "Comcast Cable (7922)"
    assert format_asn_label(7922, "") == "AS7922"
    assert format_asn_label(7922, "AS7922") == "AS7922"


# ── Usage-log & Reconciliation Helpers ──
def test_reconcile_fastly_stats_guards_and_fetch():
    with patch("backend.config.is_usage_logging_enabled", return_value=False):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}) == 0

    with patch("backend.config.is_usage_logging_enabled", return_value=True):
        assert reconcile_fastly_stats({"name": "svc"}) == 0

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.get_fastly_api_key", return_value=""),
    ):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}) == 0

    payload = {
        "data": [
            {
                "start_time": 1760000000,
                "object_storage_class_a_operations_count": 7,
                "object_storage_class_b_operations_count": 3,
            },
            {
                "start_time": 1760003600,
                "object_storage": {"class_a_operations_count": 2, "class_b_operations_count": 1},
            },
            {"start_time": None},
        ]
    }
    fetched: list = []

    def _fake_fastly(method, path, body=None, *, token, **_kw):
        fetched.append((method, path, token))
        return payload

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("backend.core.metadata.get_latest_reconciliation_ts", return_value=None),
        patch("backend.core.fastly.client.fastly", side_effect=_fake_fastly),
        patch("backend.core.metadata.reconcile_fastly_stats", return_value=2) as mock_recon,
    ):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}, hours_back=2) == 2

    assert len(fetched) == 1 and fetched[0][0] == "GET" and fetched[0][2] == "tok"
    assert fetched[0][1].startswith("/stats/aggregate?by=hour&from=")
    assert [(h["class_a"], h["class_b"]) for h in mock_recon.call_args.args[1]] == [(7, 3), (2, 1)]

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("backend.core.metadata.get_latest_reconciliation_ts", return_value=None),
        patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("HTTP 500")),
    ):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}) == 0


def test_usage_and_cron_forwarding_helpers():
    with (
        patch("backend.config.load_usage_logging_config", return_value={"retention_days": 7}),
        patch("backend.core.metadata.purge_usage_log") as mock_purge,
    ):
        purge_usage_log({})
        mock_purge.assert_not_called()
        purge_usage_log({"name": "svc"})
        mock_purge.assert_called_once_with("svc", 7)

    with patch("backend.core.metadata.update_cron_duration") as mock_upd:
        update_cron_duration({}, run_id=1, duration_s=0.5)
        mock_upd.assert_not_called()
        update_cron_duration({"name": "svc"}, run_id=42, duration_s=1.25, log_output="ok")
        mock_upd.assert_called_once_with("svc", 42, 1.25, log_output="ok")

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        log_usage_calls({"name": "svc"}, [{"method": "GET", "path": "/foo"}])
        mock_log.assert_not_called()

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.log_usage_calls") as mock_log_on,
    ):
        log_usage_calls({"name": "svc"}, [{"method": "GET"}], process_context="dashboard")
        mock_log_on.assert_called_once_with("svc", [{"method": "GET"}], process_context="dashboard")

    with patch("backend.config.is_usage_logging_enabled", return_value=False):
        assert backfill_fastly_edge_writes({"name": "svc"}) == 0
    with patch("backend.config.is_usage_logging_enabled", return_value=True):
        assert backfill_fastly_edge_writes({}) == 0

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.list_unbackfilled_fastly_edge_files", return_value=[]),
        patch("backend.core.metadata.log_synthetic_usage") as mock_synth_empty,
    ):
        assert backfill_fastly_edge_writes({"name": "svc"}) == 0
        mock_synth_empty.assert_not_called()

    files = [
        ("raw/2026-06-15T00:00:00Z/file.gz", "2026-06-15T00:00:00", 100, 5000),
        ("__seeding_attempted__", "2026-06-15T00:00:00", 0, 0),
        ("no-timestamp.gz", "2026-06-15T00:00:00", 50, 2500),
    ]
    captured: dict = {}
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.list_unbackfilled_fastly_edge_files", return_value=files),
        patch(
            "backend.core.metadata.log_synthetic_usage",
            side_effect=lambda sid, calls: (captured.update({"sid": sid, "calls": calls}), len(calls))[1],
        ),
    ):
        assert backfill_fastly_edge_writes({"name": "svc"}) == 2

    assert captured["sid"] == "svc" and len(captured["calls"]) == 2
    assert captured["calls"][0]["path"] == "raw/2026-06-15T00:00:00Z/file.gz"
    assert captured["calls"][0]["method"] == "PUT_OBJECT"
    assert captured["calls"][0]["_timestamp_override"] == "2026-06-15T00:00:00Z"
    assert captured["calls"][1]["_timestamp_override"] == "2026-06-15T00:00:00"


def _seed_reconciliation_row(service_id: str, ts_iso: str) -> None:
    import backend.core.metadata.usage_log as _usage_log_db

    con = _usage_log_db.get_con(service_id)
    con.execute(
        """
        INSERT INTO usage_log
        (timestamp, service_id, operation_class, operation_type, url, status,
         duration_ms, function_name, process_context, count)
        VALUES (?, ?, 'A', 'RECONCILE_A', 'fastly://stats/aggregate/test', 'OK',
                0.0, 'fastly.reconciliation', 'fastly:reconciliation', 1)
        """,
        (ts_iso, service_id),
    )
    con.commit()


@patch("backend.config.is_usage_logging_enabled", return_value=True)
@patch("backend.config.get_fastly_api_key", return_value="fake-key")
def test_reconcile_gate_recent_vs_old_vs_none(_key, _enabled):
    service_id = "recon-svc-1"
    metadata_db.insert_ingested_files(
        service_id, [("s3://bkt/raw/2026-05-22/12/2026-05-22T12:00:00.000-x.log.gz", 1, 1)]
    )
    src = {"name": service_id, "logging_service_id": "logsvc"}

    with patch("urllib.request.urlopen", side_effect=RuntimeError("called!")) as urlopen:
        fos_mod.reconcile_fastly_stats(src)
        assert urlopen.called

    old = (datetime.now(UTC) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _seed_reconciliation_row(service_id, old)
    with patch("urllib.request.urlopen", side_effect=RuntimeError("called!")) as urlopen:
        fos_mod.reconcile_fastly_stats(src)
        assert urlopen.called

    recent = (datetime.now(UTC) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _seed_reconciliation_row(service_id, recent)
    with patch("urllib.request.urlopen") as urlopen:
        assert fos_mod.reconcile_fastly_stats(src) == 0
        assert not urlopen.called


def test_microsecond_precision_for_elapsed_ottfb_round_trip_via_parquet(tmp_path):
    elapsed_us = [50_000, 1_234_567, 63_000_000, 9_007_199_254_740_993]
    ottfb_us = [128, 4_287, 12_345_678, 1_000_001]
    table = pa.table({"elapsed": pa.array(elapsed_us, type=pa.uint64()), "ottfb": pa.array(ottfb_us, type=pa.uint64())})
    path = str(tmp_path / "timing.parquet")
    pq.write_table(table, path, compression="zstd")
    loaded = pq.read_table(path)
    rows = sorted(zip(loaded.column("elapsed").to_pylist(), loaded.column("ottfb").to_pylist()))
    assert rows == sorted(zip(elapsed_us, ottfb_us))


# ── Merged from tests/perf/test_perf_optimizations.py ──


def _fake_src(service_id="svc-perf"):
    return {
        "name": service_id,
        "service_id": service_id,
        "bucket": "b",
        "endpoint": "e",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
    }


def _minimal_cfg(service_id="svc-perf"):
    return {
        "service_id": service_id,
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True}},
    }


def test_scheduler_job_misfire_grace_times():
    from backend.cron.scheduler import Scheduler

    sid = "svc-prewarm"
    cfg = _minimal_cfg(service_id=sid)
    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.fos_client.get_source_for_service", return_value=_fake_src(sid)),
        patch("backend.core.fos_client.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()
    job = s._sched.get_job(f"insights_prewarmer_{sid}")
    assert job is not None and job.misfire_grace_time > 240


_PREWARM_PREFIXES = (
    "/dashboard",
    "/network",
    "/origin",
    "/performance",
    "/security",
    "/charts",
    "/insights",
    "/fastly-value",
    "/usage",
    "/streaming",
    "/control-room",
    "/sessions/stream",
)


def _needs_prewarm(pathname: str) -> bool:
    return any(pathname.startswith(p) for p in _PREWARM_PREFIXES)


@pytest.mark.parametrize(
    ("route", "expected"),
    [
        ("/usage", True),
        ("/usage/", True),
        ("/streaming", True),
        ("/control-room", True),
        ("/sessions/stream", True),
        ("/sessions/stream/abc123", True),
        ("/sessions", False),
        ("/query", False),
        *[
            (r, True)
            for r in ("/dashboard", "/network", "/origin", "/performance", "/security", "/insights", "/fastly-value")
        ],
    ],
)
def test_prewarm_routes(route, expected):
    assert _needs_prewarm(route) is expected


@pytest.mark.benchmark
def test_bench_hll_add_1k_items(benchmark):
    items = [f"203.0.113.{i % 256}" for i in range(1000)]

    def _go():
        s = HyperLogLog()
        for item in items:
            s.add(item)
        return s

    assert benchmark(_go).count() > 0


@pytest.mark.benchmark
def test_bench_hll_count_after_100k_adds(benchmark):
    sketch = HyperLogLog()
    for i in range(100_000):
        sketch.add(f"203.0.113.{i % 256}.{i // 256}")
    assert benchmark(sketch.count) > 0


@pytest.mark.benchmark
def test_bench_hll_merge_24_hours(benchmark):
    sketches = []
    for hour in range(24):
        s = HyperLogLog()
        for i in range(5_000):
            s.add(f"hour{hour}:198.51.100.{i % 256}.{i // 256}")
        sketches.append(s)

    def _go():
        m = HyperLogLog()
        for s in sketches:
            m.merge(s)
        return m

    assert benchmark(_go).count() > 0


@pytest.mark.benchmark
def test_bench_hll_serialize_roundtrip(benchmark):
    sketch = HyperLogLog()
    for i in range(10_000):
        sketch.add(f"192.0.2.{i % 256}.{i // 256}")
    blob = sketch.to_bytes()
    assert benchmark(lambda: HyperLogLog.from_bytes(blob).to_bytes()) == blob


@pytest.mark.benchmark
def test_bench_safe_table_name(benchmark):
    assert benchmark(_safe_table_name, "svc-test-account-name-with-dashes-and-some-numbers-42").startswith("logs_")


@pytest.mark.benchmark
def test_bench_escape_sql_literal_typical(benchmark):
    key = "raw/2026-05-15/12/2026-05-15T12-34-56.svc-name.gz"
    assert benchmark(escape_sql_literal, key) == key


@pytest.mark.benchmark
def test_bench_escape_sql_literal_with_quotes(benchmark):
    assert "''" in benchmark(escape_sql_literal, "raw/2026-05-15/O'Brien's-bucket/file.gz")
