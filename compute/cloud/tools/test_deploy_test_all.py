from __future__ import annotations

import json
import subprocess
from pathlib import Path

import deploy_test_all
import httpx
import pytest
from deploy_test_all import (
    build_e2e_freshness_report,
    compute_lag_breakdown,
    download_parquet_files,
    evaluate_cron_health,
    finalize_deploy_report,
    generate_recommendations,
    inv_build_sha,
    inv_services,
    list_parquet_files,
    load_prior_deploys,
    load_session_cookie,
    main,
    parse_env_file,
    parse_minute_prefix_epoch,
    preflight_monitored_service,
    print_summary_banner,
    render_markdown_report,
    step_1_deploy,
    step_2_reconcile_vcl,
    step_4_deploy_check,
    step_5_confirm_crons,
    step_6_browser_verify,
    summarize_deploy_entry,
    summarize_emitted_log_files,
    summarize_numeric_series,
    validate_dashboard_presets,
    walk_raw_tree,
)


def test_parse_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# comment\nexport FASTLY_API_TOKEN='secret-token-123'\nOTHER_VAR=\"hello=world\"\nPLAIN=value\n"
    )
    parsed = parse_env_file(env_file)
    assert parsed == {
        "FASTLY_API_TOKEN": "secret-token-123",
        "OTHER_VAR": "hello=world",
        "PLAIN": "value",
    }


def test_parse_minute_prefix_epoch() -> None:
    prefix = "request/year=2026/month=10/day=31/hour=18/minute=45/file.log.gz"
    epoch = parse_minute_prefix_epoch(prefix)
    assert epoch == 1793472300
    assert parse_minute_prefix_epoch("request/year=2026/month=10/") is None


def test_compute_lag_breakdown() -> None:
    lag = compute_lag_breakdown(
        t_first_send=1000.0,
        t_send_done=1180.0,
        t_first_raw_fos=1011.4,
        max_minute_epoch=1140,
        t_ingest_done=1332.5,
        t_dashboard_verified=1335.0,
    )
    assert lag == {
        "raw_fos_landing_lag_s": 11.4,
        "settle_ready_epoch_s": 1320.0,
        "post_settle_ingest_lag_s": 12.5,
        "send_to_parquet_lag_s": 152.5,
        "send_to_dashboard_lag_s": 155.0,
    }


def test_evaluate_cron_health_passes_and_detects_budget_violations() -> None:
    required = {
        "request_ingest",
        "raw_delete",
        "retention_sweep",
        "storage_reconcile",
        "log_compact",
        "log_flush",
    }
    good_rows = [
        {
            "cron_kind": stage,
            "status": "ok",
            "error_count": 0,
            "backend_requests_used": 12,
            "cache_lookups": 8,
            "vcpu_ms": 145.0,
            "converted_raw_files": 4 if stage == "request_ingest" else 0,
            "deleted_raw_files": 4 if stage == "raw_delete" else 0,
            "parquet_files_written": 1 if stage == "request_ingest" else 0,
            "rows_written": 1200 if stage == "request_ingest" else 0,
        }
        for stage in required
    ]
    res = evaluate_cron_health(good_rows, required)
    assert res["ok"] is True
    assert res["missing_stages"] == []
    assert res["max_backend_requests_used"] == 12
    assert res["max_cache_lookups"] == 8
    assert res["max_vcpu_ms"] == 145.0
    assert res["total_raw_files_converted"] == 4
    assert res["total_raw_files_deleted"] == 4
    assert res["total_parquet_files_written"] == 1
    assert res["total_rows_written"] == 1200
    assert res["per_stage_distributions"]["request_ingest"]["converted_files"]["sum"] == 4

    bad_rows = [
        *good_rows[:-1],
        {
            "cron_kind": "request_ingest",
            "status": "ok",
            "error_count": 0,
            "backend_requests_used": 30,
            "cache_lookups": 60,
            "vcpu_ms": 825.0,
        },
    ]
    bad = evaluate_cron_health(bad_rows, required)
    assert bad["ok"] is False
    assert any("backend_requests_used" in e for e in bad["errors"])
    assert any("cache_lookups" in e for e in bad["errors"])
    assert any("vcpu_ms" in e for e in bad["errors"])


def test_evaluate_cron_health_budgets_compaction_worker_vcpu_per_pass() -> None:
    required = {"request_compact"}
    base = {
        "cron_kind": "request_compact",
        "status": "ok",
        "error_count": 0,
        "backend_requests_used": 8,
        "cache_lookups": 2,
        "vcpu_ms": 82.0,
    }
    # Eight worker executions of ~120 ms each: fine per execution even though the sum is ~960 ms.
    spread = evaluate_cron_health(
        [{**base, "details_json": json.dumps({"passes": 8, "worker_vcpu_ms": 960})}], required
    )
    assert spread["ok"] is True, spread["errors"]
    assert spread["max_vcpu_ms"] == 120.0
    # One worker execution of 960 ms busts the budget.
    single = evaluate_cron_health(
        [{**base, "details_json": json.dumps({"passes": 1, "worker_vcpu_ms": 960})}], required
    )
    assert single["ok"] is False
    assert any("worker vcpu_ms" in e for e in single["errors"])


def test_validate_dashboard_presets_rejects_10m() -> None:
    assert validate_dashboard_presets(["1h", "6h", "24h"]) == ("1h", "6h", "24h")
    assert validate_dashboard_presets(["1h", "7d", "30d"], allow_extended=True) == (
        "1h",
        "7d",
        "30d",
    )
    with pytest.raises(ValueError, match="10m is never a preset"):
        validate_dashboard_presets(["10m", "1h"])


def test_summarize_numeric_series_and_emitted_log_files() -> None:
    dist = summarize_numeric_series([10, 20, 30, 40, 100])
    assert dist["count"] == 5
    assert dist["min"] == 10.0
    assert dist["p50"] == 30.0
    assert dist["p95"] == 88.0
    assert dist["max"] == 100.0

    emitted = summarize_emitted_log_files(
        raw_file_entries=[
            {
                "path": "request/year=2026/month=11/day=03/hour=15/minute=10/2026-11-03T15:10:00.000-abc-IAD.log.gz",
                "size": 4096,
            },
            {
                "path": "request/year=2026/month=11/day=03/hour=15/minute=10/2026-11-03T15:10:10.000-def-IAD.log.gz",
                "size": 8192,
            },
        ],
        parquet_file_entries=[
            {"key": "svc/window-1.parquet", "bytes": 16384, "rows": 1000},
            {"key": "svc/window-2.parquet", "bytes": 32768, "rows": 2000},
        ],
        cron_rows=[],
        duration_s=20.0,
    )
    assert emitted["raw_files_emitted_total"] == 2
    assert emitted["raw_bytes_emitted_total"] == 12288
    assert emitted["raw_file_size_bytes"]["p50"] == 6144.0
    assert emitted["parquet_files_written_total"] == 2
    assert emitted["parquet_bytes_written_total"] == 49152
    assert emitted["parquet_rows_written_total"] == 3000
    assert emitted["by_pop"]["IAD"]["files"] == 2


def test_missing_cron_cpu_is_not_a_zero_cost_execution() -> None:
    report = evaluate_cron_health(
        [
            {"cron_kind": "request_ingest", "vcpu_ms": None},
            {"cron_kind": "request_ingest", "vcpu_ms": 120},
        ],
        {"request_ingest"},
    )
    assert report["ok"] is False
    assert report["overall_metrics"]["vcpu_ms"]["count"] == 1
    assert report["overall_metrics"]["vcpu_ms"]["p50"] == 120
    assert any("unmeasured vcpu_ms" in error for error in report["errors"])


def test_numeric_series_excludes_nonfinite_measurements() -> None:
    report = summarize_numeric_series([None, True, float("nan"), float("inf"), 0, 8])
    assert report["count"] == 2
    assert report["p50"] == 4


@pytest.mark.parametrize(
    "services",
    [
        [],
        [{"service_id": "historical"}],
        [{"service_id": "requested"}, {"service_id": "historical"}],
        None,
    ],
)
def test_preflight_rejects_missing_or_mixed_service_identity(
    services: list[dict[str, str]] | None,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/api/admin/services"
        return httpx.Response(200, json={"services": services})

    with (
        httpx.Client(base_url="https://example.test", transport=httpx.MockTransport(handler)) as client,
        pytest.raises(RuntimeError, match="refusing to deploy or measure another identity"),
    ):
        preflight_monitored_service(client, "requested")


def test_preflight_accepts_only_the_requested_configured_service() -> None:
    with httpx.Client(
        base_url="https://example.test",
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"services": [{"service_id": "requested"}]})),
    ) as client:
        preflight_monitored_service(client, "requested")


def test_missing_service_fails_before_deployment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state_dir = tmp_path / ".flp-state" / "test-install"
    state_dir.mkdir(parents=True)
    (state_dir / "inventory.json").write_text(json.dumps({"services": {"front": {"domain": "example.test"}}}))
    monkeypatch.setattr("deploy_test_all.Path.home", lambda: tmp_path)
    monkeypatch.setattr("deploy_test_all.parse_env_file", lambda _: {})
    monkeypatch.setenv("FASTLY_API_TOKEN", "test-token")
    monkeypatch.setattr("deploy_test_all.ensure_browser_auth", lambda *_: "test-session")
    monkeypatch.setattr(
        "deploy_test_all.app_client",
        lambda *_: httpx.Client(
            base_url="https://example.test",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json={"services": [{"service_id": "historical"}]})
            ),
        ),
    )

    def forbidden_deploy(*_args: object) -> None:
        raise AssertionError("Deployment must not start before identity validation")

    monkeypatch.setattr("deploy_test_all.step_1_deploy", forbidden_deploy)
    monkeypatch.setattr(
        "deploy_test_all.sys.argv",
        [
            "deploy_test_all.py",
            "--name",
            "test-install",
            "--service-id",
            "requested",
            "--work-dir",
            str(tmp_path / "campaign"),
        ],
    )
    with pytest.raises(RuntimeError, match="refusing to deploy"):
        main()


@pytest.mark.parametrize("visible_at", [1800.0, None])
def test_dashboard_freshness_uses_observed_browser_timestamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, visible_at: float | None
) -> None:
    def run_browser(command: list[str], **_kwargs: object) -> None:
        assert command[command.index("--ranges") + 1] == "24h,1h,6h"
        Path(command[command.index("--out") + 1]).write_text(
            json.dumps(
                {
                    "ok": True,
                    "dashboard": {"visibleAtEpochS": visible_at, "coldLoadMs": 500},
                }
            )
        )

    monkeypatch.setattr("deploy_test_all.subprocess.run", run_browser)
    traffic = {
        "t_first_send": 900.0,
        "t_send_done": 1000.0,
        "t_first_raw_fos": None,
        "max_minute_epoch": 960,
        "t_ingest_done": 1400.0,
    }
    step_6_browser_verify(tmp_path, "https://example.test", tmp_path, "svc1", 1, tmp_path, traffic)
    assert traffic["lag"]["send_to_dashboard_lag_s"] == (800.0 if visible_at is not None else None)


@pytest.mark.parametrize("case", ["missing", "hard_limit"])
def test_admin_success_preserves_missing_and_over_budget_cron_evidence(
    monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    monkeypatch.setattr("deploy_test_all.time.sleep", lambda _: None)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/files":
            return httpx.Response(200, json={"files": []})
        if request.url.path == "/api/admin/quarantine":
            return httpx.Response(200, json={"items": []})
        if request.url.path in {
            "/api/logs/cron_runs/live",
            "/api/logs/platform_errors/live",
        }:
            rows = (
                [
                    {"cron_kind": stage, "vcpu_ms": 200}
                    for stage in (
                        "retention_sweep",
                        "storage_reconcile",
                        "log_compact",
                        "log_flush",
                    )
                ]
                if case == "hard_limit" and request.url.path == "/api/logs/cron_runs/live"
                else []
            )
            return httpx.Response(200, json={"rows": rows})
        if request.url.path == "/api/sync-status":
            return httpx.Response(
                200,
                json={
                    "cron_runs": [
                        {
                            "id": "over-budget",
                            "started_at": "2099-01-01",
                            "converted_raw_files": 1,
                            "backend_requests_used": 40,
                            "vcpu_ms": 500,
                        }
                    ]
                    if case == "hard_limit"
                    else []
                },
            )
        if request.url.path == "/api/admin/logs/flush":
            return httpx.Response(200, json={"status": "ok", "outbox_depth": 0, "rows": 0})
        if request.url.path in {
            "/api/admin/logs/sweep",
            "/api/admin/usage/reconcile-storage",
            "/api/cron-runs/trigger",
        }:
            return httpx.Response(200, json={})
        raise AssertionError(f"Unexpected request: {request.url}")

    with (
        httpx.Client(base_url="https://example.test", transport=httpx.MockTransport(handler)) as client,
        pytest.raises(
            RuntimeError,
            match="Missing required cron stages"
            if case == "missing"
            else "Hard Fastly Compute resource limit exceeded",
        ),
    ):
        step_5_confirm_crons(client, "svc1", 1700000000, had_traffic=False)


def test_load_session_cookie(tmp_path: Path) -> None:
    state_file = tmp_path / "browser-state.json"
    state_file.write_text(
        json.dumps(
            {
                "cookies": [
                    {"name": "other", "value": "x"},
                    {"name": "flp_session", "value": "sess-abc"},
                ]
            }
        )
    )
    assert load_session_cookie(state_file) == "sess-abc"
    with pytest.raises(RuntimeError):
        load_session_cookie(tmp_path / "missing.json")


def test_walk_raw_tree_and_list_parquet_files() -> None:
    tree_map = {
        "request/": [
            {
                "kind": "directory",
                "path": "request/year=2026/month=10/day=31/hour=18/minute=30/",
            }
        ],
        "request/year=2026/month=10/day=31/hour=18/minute=30/": [
            {
                "kind": "file",
                "path": "request/year=2026/month=10/day=31/hour=18/minute=30/a.log.gz",
                "size": 512,
            }
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/admin/raw-tree":
            prefix = request.url.params.get("prefix", "request/")
            return httpx.Response(200, json={"nodes": tree_map.get(prefix, [])})
        if request.url.path == "/api/files":
            cursor = request.url.params.get("cursor")
            if not cursor:
                return httpx.Response(
                    200,
                    json={
                        "files": [{"key": "k1.parquet", "rows": 10}],
                        "next_cursor": "page2",
                        "complete": False,
                    },
                )
            return httpx.Response(
                200,
                json={
                    "files": [{"key": "k2.parquet", "rows": 20}],
                    "next_cursor": None,
                    "complete": True,
                },
            )
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, base_url="https://example.test") as client:
        raw_files, max_min = walk_raw_tree(client, "svc1", "request/")
        assert len(raw_files) == 1
        assert max_min == 1793471400

        pq_files = list_parquet_files(client, "2026-10-31T18:00:00Z", "2026-10-31T19:00:00Z")
        assert [f["key"] for f in pq_files] == ["k1.parquet", "k2.parquet"]


def test_step_2_reconcile_vcl_patches_when_log_period_differs() -> None:
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == "/api/admin/services/svc1":
            return httpx.Response(
                200,
                json={
                    "service_id": "svc1",
                    "log_period": 60,
                    "cmcd": {"enabled": False},
                    "rum": {"enabled": False, "log_period": 60},
                },
            )
        if request.method == "PATCH" and request.url.path == "/api/admin/services/svc1":
            body = json.loads(request.content.decode())
            assert body["log_period"] == 10
            assert body["rum"]["log_period"] == 10
            return httpx.Response(
                200,
                json={"job": {"id": "job-1", "state": "running"}},
            )
        if request.method == "POST" and request.url.path == "/api/admin/jobs/job-1/step":
            return httpx.Response(
                200,
                json={
                    "job": {
                        "id": "job-1",
                        "state": "succeeded",
                        "cloned_version": 42,
                        "activated_version": 42,
                    }
                },
            )
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, base_url="https://example.test") as client:
        res = step_2_reconcile_vcl(client, "svc1", 10)
        assert res["trigger_mode"] == "patch_settings"
        assert res["previous_log_period"] == 60
        assert res["log_period"] == 10
        assert res["activated_version"] == 42
        assert calls == [
            ("GET", "/api/admin/services/svc1"),
            ("PATCH", "/api/admin/services/svc1"),
            ("POST", "/api/admin/jobs/job-1/step"),
        ]


def _make_sample_report(
    run_ts: str,
    sha: str,
    cold_ms: int = 23000,
    warm_ms: int = 1200,
    dl_ms: int = 22100,
    files_fetched: int = 226,
    manifest_reqs: int = 2,
) -> dict:
    return {
        "ok": True,
        "deploy_timestamp": run_ts,
        "install_name": "flp-g2-live-20261029",
        "base_url": "https://flp-g2-live-20261029.global.ssl.fastly.net",
        "service_id": "xa20dYfOsL4MhjIo66mVhh",
        "target_url": "https://xa20d-g2-test.global.ssl.fastly.net",
        "total_duration_s": 310.0,
        "step_1_deploy": {"head_sha": sha, "skipped": False, "duration_s": 40.0},
        "step_2_reconcile_vcl": {
            "service_id": "xa20dYfOsL4MhjIo66mVhh",
            "trigger_mode": "reconcile",
            "duration_s": 12.0,
        },
        "step_3_confirm_active": {
            "live_build_sha": sha,
            "compute": {"active_version": 48},
            "worker": {"active_version": 48},
            "front": {"active_version": 48, "tick_interval_ms": 1000},
            "monitored_vcl": {
                "active_version": 42,
                "s3_log_period_s": 10,
                "status": " healthy",
            },
        },
        "step_4_traffic_and_ingest": {
            "stages": [{"stage": "10rps", "target_rps": 10, "sent": 600, "actual_rps": 10.0}],
            "schema": {"valid": True},
            "verify_overall": {
                "sent": 600,
                "matched": 600,
                "missing_count": 0,
                "duplicate_count": 0,
                "strict_mismatch_count": 0,
                "unknown_id_count": 0,
            },
            "verify_per_stage": {
                "10rps": {
                    "sent": 600,
                    "matched": 600,
                    "missing_count": 0,
                    "duplicate_count": 0,
                    "strict_mismatch_count": 0,
                    "unknown_id_count": 0,
                }
            },
            "lag": {
                "raw_fos_landing_lag_s": 11.2,
                "post_settle_ingest_lag_s": 8.4,
                "send_to_parquet_lag_s": 128.4,
                "send_to_dashboard_lag_s": 135.0,
            },
        },
        "step_5_confirm_crons": {
            "cron_health": {
                "ok": True,
                "stages_seen": [
                    "log_compact",
                    "log_flush",
                    "raw_delete",
                    "request_ingest",
                    "retention_sweep",
                    "storage_reconcile",
                ],
                "max_backend_requests_used": 14,
                "max_cache_lookups": 10,
                "max_vcpu_ms": 18.2,
            },
            "natural_clock_stages": ["raw_delete", "request_ingest"],
            "outbox_depth": 0,
            "platform_errors_count": 0,
            "quarantined_files_count": 0,
        },
        "step_6_browser_verify": {
            "ok": True,
            "dashboard": {
                "cold": {
                    "loadMs": cold_ms,
                    "parquetDownloadMs": dl_ms,
                    "workerPostDownloadMs": 320,
                    "parquetFilesFetched": files_fetched,
                    "manifestRequests": manifest_reqs,
                    "preParquetBootstrapMs": 600,
                    "totalRequests": 100500,
                },
                "warm": {"loadMs": warm_ms, "totalRequests": 100500},
            },
            "pages": {
                "/dashboard (cold)": {"loadMs": cold_ms},
                "/dashboard (warm)": {"loadMs": warm_ms},
                "/logs": {"loadMs": 420},
            },
        },
    }


def test_recommendations_and_5_prior_deploys_rotation(tmp_path: Path) -> None:
    deploys_root = tmp_path / "reports" / "deploys"
    timestamps = [f"2026-10-31-10-0{i}-00" for i in range(7)]  # 7 runs -> should retain current + 5 prior = 6 folders

    for idx, ts in enumerate(timestamps):
        priors = load_prior_deploys(deploys_root, ts, limit=5)
        rep = _make_sample_report(
            run_ts=ts,
            sha=f"abcdef{idx}0000000",
            cold_ms=24000 - idx * 3000,
            warm_ms=1100,
            dl_ms=22000 - idx * 3000,
            files_fetched=226,
            manifest_reqs=2 if idx < 5 else 1,
        )
        rep["prior_deploys"] = priors
        rep["recommendations"] = generate_recommendations(rep, priors)
        finalize_deploy_report(
            deploys_root=deploys_root,
            run_ts=ts,
            report=rep,
            log_text=f"Run log for {ts}\n",
            keep_prior=5,
        )

    # Verify current symlink and root index.html
    assert (deploys_root / "current").is_symlink()
    assert (deploys_root / "current").resolve() == (deploys_root / timestamps[-1]).resolve()
    root_html = (deploys_root / "index.html").read_text()
    assert '<base href="current/">' in root_html
    assert "FLP DEPLOYMENT &amp; E2E FRESHNESS REPORT" in root_html

    # Verify only 6 timestamped directories remain (current + 5 prior)
    remaining = sorted(d.name for d in deploys_root.iterdir() if d.is_dir() and not d.is_symlink())
    assert remaining == timestamps[1:]
    assert len(remaining) == 6

    # Verify the latest report has 5 prior deploys recorded
    latest_report = json.loads((deploys_root / "current" / "relics" / "report.json").read_text())
    assert len(latest_report["prior_deploys"]) == 5
    assert latest_report["prior_deploys"][0]["run_id"] == timestamps[-2]
    assert (deploys_root / "current" / "relics" / "e2e_freshness_report.json").is_file()
    assert (deploys_root / "current" / "relics" / "report.md").is_file()
    assert (deploys_root / "current" / "logs" / "deploy.log").is_file()


def test_report_consumers_preserve_browser_telemetry(
    capsys: pytest.CaptureFixture[str],
) -> None:
    report = _make_sample_report("2026-11-03-12-00-00", "abcdef0")
    browser = report["step_6_browser_verify"]
    browser["distributions"] = {
        "cleanDashboardLoadMs": {"p50": 625, "p95": 950, "count": 2},
        "filteredDashboardLoadMs": {"p50": 300, "p95": 350, "count": 2},
        "clsScore": {"p50": 0.01, "p95": 0.02, "max": 0.02},
    }
    browser["uiQuality"] = {"maxClsScore": 0.02}
    browser["apiTelemetry"] = {
        "totalCalls": 7,
        "rangeReadCount": 2,
        "rangeProbeCount": 1,
        "durationMs": {"p50": 40, "p95": 80},
    }
    browser["pages"]["/logs"]["apiSummary"] = {"totalCalls": 3}
    report["step_4_traffic_and_ingest"]["emitted_files"] = summarize_emitted_log_files(
        raw_file_entries=[{"key": "raw.log.gz", "size": 1024}],
        parquet_file_entries=[{"key": "window.parquet", "bytes": 2048, "rows": 4}],
        duration_s=30,
    )
    report["step_5_confirm_crons"]["cron_health"] = evaluate_cron_health(
        [{"cron_kind": "request_ingest", "vcpu_ms": 10}], {"request_ingest"}
    )
    summary = summarize_deploy_entry(report, "current")
    assert summary["clean_dashboard_p50_ms"] == 625
    assert summary["filtered_dashboard_p95_ms"] == 350
    assert summary["cls_max"] == 0.02
    freshness = build_e2e_freshness_report(report)[0]
    assert freshness["benchmark_distributions"] == browser["distributions"]
    assert freshness["ui_health"] == browser["uiQuality"]
    assert freshness["api_waterfall"] == browser["apiTelemetry"]
    markdown = render_markdown_report(report)
    assert "`p50=625ms`, `p95=950ms`" in markdown
    assert "| Strict mismatches | Unknown ids |" in markdown
    assert "CLS `p50=0.01`, `p95=0.02`, `max=0.02`" in markdown
    assert "`7` total `/api/*` calls" in markdown
    assert "`2.0` files/min" in markdown
    assert "`512.0` B/row" in markdown
    assert "| `request_ingest` | 1 |" in markdown
    assert "| `/logs` | `420ms` | `0` | `—` | `—` | `0` (`0ms`) | `3` |" in markdown
    print_summary_banner(report)
    output = capsys.readouterr().out
    assert "clean_presets(p50=625ms, p95=950ms)" in output
    assert "cls_max=0.02 | api_calls=7 (p50=40ms, p95=80ms)" in output


def fake_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stage: dict,
    returncode: int = 0,
    write_campaign: bool = True,
) -> list[list[str]]:
    calls: list[list[str]] = []

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(command))
        out = Path(str(kwargs["cwd"])) / "lt-report"
        out.mkdir(parents=True, exist_ok=True)
        lines = stage.get("expected_log_lines")
        record = {
            "label": "p10-s1",
            "run_id": "dc-1-p10-s1",
            "target_rps": 100.0,
            "started_at": "2026-11-05T10:00:00+00:00",
            "ended_at": "2026-11-05T10:06:00+00:00",
            "result": {
                "delivered": lines,
                "expected_log_lines": lines,
                "achieved_rps": 99.5,
                "first_sent_at": "2026-11-05T10:00:05Z",
                "last_response_at": "2026-11-05T10:03:05Z",
                "latency_ms": {"p50": 12.0, "p95": 45.5},
            },
            "ids": {
                "matched": lines,
                "missing_delivered": 0,
                "duplicates": 0,
                "unknown": 0,
            },
            "strict": {"mismatch_count": 0},
            "ingest": {
                "lag_s": 42.0,
                "reached_at": "2026-11-05T10:04:20+00:00",
                "settled_at": "2026-11-05T10:04:30+00:00",
                "send_to_parquet_s": 75.0,
            },
            "schema": {"valid": True, "problems": []},
            **stage,
        }
        campaign = {"campaign": "dc-1", "stages": [record]}
        if write_campaign:
            (out / "campaign.json").write_text(json.dumps(campaign))
        return subprocess.CompletedProcess(command, returncode, stdout=f"Report: {out / 'campaign.md'}\n", stderr="")

    monkeypatch.setattr("deploy_test_all.subprocess.run", run)
    return calls


def test_step_4_runs_shared_deploy_check(tmp_path, monkeypatch):
    calls = fake_subprocess(monkeypatch, stage={"status": "pass", "expected_log_lines": 18000})
    r = step_4_deploy_check(tmp_path, config=Path("fastly-lt.toml"), lt_repo=Path("/lt"))
    assert (
        calls[0][:5] == ["uv", "run", "--project", "/lt", "lt"]
        and "deploy-check" in calls[0]
        and r["expect_min_requests"] == 18000
    )


def test_deploy_test_all_no_longer_imports_loadtest():
    assert "loadtest" not in Path(deploy_test_all.__file__).read_text()


def test_step_4_maps_the_stage_onto_report_keys(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_subprocess(monkeypatch, stage={"status": "pass", "expected_log_lines": 18000})
    r = step_4_deploy_check(tmp_path, config=Path("fastly-lt.toml"), lt_repo=Path("/lt"))
    assert r["t_send_done"] - r["t_first_send"] == 180.0
    assert r["t_ingest_done"] - r["t_send_done"] == 75.0
    assert r["verify_overall"]["matched"] == 18000
    assert r["verify_overall"]["strict_mismatch_count"] == 0
    assert r["verify_overall"]["unknown_id_count"] == 0
    assert r["schema"] == {"valid": True, "problems": []}
    assert r["verify_per_stage"]["p10-s1"]["missing_count"] == 0
    assert r["stages"][0]["stage"] == "p10-s1"
    assert r["lag"]["send_to_parquet_lag_s"] == 75.0


def test_lags_end_at_the_reaching_poll_and_survive_the_browser_recompute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_subprocess(
        monkeypatch,
        stage={
            "status": "pass",
            "expected_log_lines": 10,
            "ingest": {
                "lag_s": 200.0,
                "reached_at": "2026-11-05T10:06:30+00:00",
                "settled_at": "2026-11-05T10:06:40+00:00",
                "send_to_parquet_s": 205.0,
            },
        },
    )
    traffic = step_4_deploy_check(tmp_path, config=Path("fastly-lt.toml"), lt_repo=Path("/lt"))
    assert traffic["t_ingest_done"] - traffic["t_send_done"] == 205.0
    assert traffic["lag"]["post_settle_ingest_lag_s"] == 30.0

    def run_browser(command: list[str], **_kwargs: object) -> None:
        Path(command[command.index("--out") + 1]).write_text(
            json.dumps({"ok": True, "dashboard": {"visibleAtEpochS": None}})
        )

    monkeypatch.setattr("deploy_test_all.subprocess.run", run_browser)
    step_6_browser_verify(tmp_path, "https://example.test", tmp_path, "svc1", 1, tmp_path, traffic)
    assert traffic["lag"]["send_to_parquet_lag_s"] == 205.0
    assert traffic["lag"]["post_settle_ingest_lag_s"] == 30.0


def test_step_4_raises_on_an_invalid_stage_schema(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_subprocess(
        monkeypatch,
        stage={
            "status": "pass",
            "expected_log_lines": 10,
            "schema": {"valid": False, "problems": ["missing columns: pop"]},
        },
    )
    with pytest.raises(RuntimeError, match="missing columns: pop"):
        step_4_deploy_check(tmp_path, config=Path("fastly-lt.toml"), lt_repo=Path("/lt"))


def test_step_4_raises_its_error_when_campaign_json_is_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_subprocess(
        monkeypatch,
        stage={"status": "pass", "expected_log_lines": 10},
        write_campaign=False,
    )
    with pytest.raises(RuntimeError, match="deploy-check"):
        step_4_deploy_check(tmp_path, config=Path("fastly-lt.toml"), lt_repo=Path("/lt"))


@pytest.mark.parametrize(("status", "returncode"), [("fail", 1), ("unverified", 2), ("pass", 1)])
def test_step_4_raises_unless_deploy_check_passed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: str, returncode: int
) -> None:
    fake_subprocess(
        monkeypatch,
        stage={"status": status, "reason": "settle timeout", "expected_log_lines": 10},
        returncode=returncode,
    )
    with pytest.raises(RuntimeError, match="deploy-check"):
        step_4_deploy_check(tmp_path, config=Path("fastly-lt.toml"), lt_repo=Path("/lt"))


def test_inv_services_and_build_sha() -> None:
    nested = {
        "build_sha": "12706a2e027846218c0690e7312b1af78c53cfc7",
        "services": {
            "compute": {"id": "c1", "version": 49},
            "worker": {"id": "w1", "version": 49},
            "front": {"id": "f1", "version": 50, "domain": "front.example.test"},
        },
    }
    assert inv_build_sha(nested) == "12706a2e027846218c0690e7312b1af78c53cfc7"
    assert inv_services(nested)["front"]["domain"] == "front.example.test"

    flat = {
        "release": {"build_sha": "abc1234"},
        "front": {"id": "f2", "version": 10, "domain": "flat.example.test"},
    }
    assert inv_build_sha(flat) == "abc1234"
    assert inv_services(flat)["front"]["domain"] == "flat.example.test"


def test_step_2_reconcile_vcl_nested_service_and_generation() -> None:
    calls: list[tuple[str, str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode()) if request.content else {}
        calls.append((request.method, request.url.path, body))
        if request.method == "GET" and request.url.path == "/api/admin/services/svc1":
            return httpx.Response(
                200,
                json={
                    "service": {
                        "service_id": "svc1",
                        "log_period": 60,
                        "cmcd": {"enabled": False},
                        "rum": {"enabled": False, "log_period": 60},
                        "status": "active",
                    },
                    "generation": "gen-1",
                    "job": None,
                },
            )
        if request.method == "PATCH" and request.url.path == "/api/admin/services/svc1":
            assert body["generation"] == "gen-1"
            assert body["log_period"] == 10
            return httpx.Response(
                200,
                json={
                    "service": {"service_id": "svc1", "log_period": 10},
                    "generation": "gen-2",
                },
            )
        if request.method == "POST" and request.url.path == "/api/admin/services/svc1/reconcile":
            assert body["generation"] == "gen-2"
            assert body["activate"] is True
            return httpx.Response(
                200,
                json={"job": {"id": "job-2", "status": "pending", "draft_version": 43}},
            )
        if request.method == "POST" and request.url.path == "/api/admin/jobs/job-2/step":
            return httpx.Response(
                200,
                json={
                    "job": {
                        "id": "job-2",
                        "status": "complete",
                        "draft_version": 43,
                    }
                },
            )
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, base_url="https://example.test") as client:
        res = step_2_reconcile_vcl(client, "svc1", 10)
        assert res["trigger_mode"] == "patch_settings"
        assert res["activated_version"] == 43
        assert [c[:2] for c in calls] == [
            ("GET", "/api/admin/services/svc1"),
            ("PATCH", "/api/admin/services/svc1"),
            ("POST", "/api/admin/services/svc1/reconcile"),
            ("POST", "/api/admin/jobs/job-2/step"),
        ]


def test_step_2_reconcile_vcl_retries_transient_409_step_conflict() -> None:
    step_attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal step_attempts
        if request.method == "GET" and request.url.path == "/api/admin/services/svc1":
            return httpx.Response(
                200,
                json={
                    "service": {
                        "service_id": "svc1",
                        "log_period": 10,
                        "cmcd": {"enabled": False},
                        "rum": {"enabled": False, "log_period": 10},
                        "status": "active",
                    },
                    "generation": "gen-1",
                    "job": None,
                },
            )
        if request.method == "POST" and request.url.path == "/api/admin/services/svc1/reconcile":
            return httpx.Response(
                200,
                json={"job": {"id": "job-3", "status": "pending", "draft_version": 44}},
            )
        if request.method == "POST" and request.url.path == "/api/admin/jobs/job-3/step":
            step_attempts += 1
            if step_attempts == 1:
                return httpx.Response(409, json={"error": "job_leased"})
            return httpx.Response(
                200,
                json={
                    "job": {
                        "id": "job-3",
                        "status": "complete",
                        "draft_version": 44,
                    }
                },
            )
        if request.method == "GET" and request.url.path == "/api/admin/jobs/job-3":
            return httpx.Response(
                200,
                json={"job": {"id": "job-3", "status": "pending", "draft_version": 44}},
            )
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, base_url="https://example.test") as client:
        res = step_2_reconcile_vcl(client, "svc1", 10)
        assert res["trigger_mode"] == "reconcile"
        assert res["activated_version"] == 44
        assert res["steps_taken"] == 1
        assert step_attempts == 2


def _git(root: Path, *args: str) -> str:
    import subprocess

    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def _deploy_fixture(tmp_path: Path, deployed_offset: int) -> tuple[Path, Path]:
    root = tmp_path / "repo"
    (root / "app").mkdir(parents=True)
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "t@example.com")
    _git(root, "config", "user.name", "t")
    (root / "app/main.rs").write_text("fn main() {}\n")
    _git(root, "add", "app")
    _git(root, "commit", "-qm", "runtime")
    (root / "README.md").write_text("docs\n")
    _git(root, "add", "README.md")
    _git(root, "commit", "-qm", "docs only")
    if deployed_offset == 0:
        (root / "app/main.rs").write_text("fn main() { run() }\n")
        _git(root, "add", "app")
        _git(root, "commit", "-qm", "runtime change")
    deployed = _git(root, "rev-parse", "HEAD~1")
    state = tmp_path / "state"
    state.mkdir()
    (state / "inventory.json").write_text(json.dumps({"build_sha": deployed}))
    return root, state


def _record_make(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    import subprocess

    real_run = subprocess.run
    ran: list[list[str]] = []

    def fake_run(cmd: list[str], *args: object, **kwargs: object) -> object:
        if cmd[0] != "make":
            return real_run(cmd, *args, **kwargs)
        ran.append(cmd)
        return None

    monkeypatch.setattr(subprocess, "run", fake_run)
    return ran


def test_step_1_skips_redeploy_when_app_tree_is_unchanged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root, state = _deploy_fixture(tmp_path, deployed_offset=1)
    ran = _record_make(monkeypatch)
    result = step_1_deploy(root, "inst", state, {}, skip_deploy=False)
    assert ran == []
    assert result["skipped"] is True
    assert "unchanged" in result["skip_reason"]


def test_step_1_redeploys_when_app_tree_changed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root, state = _deploy_fixture(tmp_path, deployed_offset=0)
    ran = _record_make(monkeypatch)
    result = step_1_deploy(root, "inst", state, {}, skip_deploy=False)
    assert [c[-2:] for c in ran if c[0] == "make"] == [
        [str(root / "app"), "build"],
        ["deploy", "NAME=inst"],
    ]
    assert result["skipped"] is False


def test_download_parquet_files_fetches_concurrently(tmp_path: Path) -> None:
    import threading

    barrier = threading.Barrier(4, timeout=5)

    def handler(request: httpx.Request) -> httpx.Response:
        barrier.wait()
        return httpx.Response(200, content=request.url.params["key"].encode())

    keys = [f"svc/k{i}" for i in range(4)]
    with httpx.Client(base_url="https://app.test", transport=httpx.MockTransport(handler)) as client:
        paths = download_parquet_files(client, keys, tmp_path)
    assert [p.read_bytes() for p in paths] == [k.encode() for k in keys]
