from datetime import UTC, datetime, timedelta

import pytest

import scripts.load_test.generate_synthetic_raw_logs as raw_logs
import scripts.load_test.generate_synthetic_rum_logs as rum_logs
from scripts.load_test.generate_synthetic_raw_logs import build_parser as build_raw_parser
from scripts.load_test.generate_synthetic_rum_logs import build_parser as build_rum_parser
from scripts.load_test.release_schedule import release_schedule


def test_release_schedule_50k_rps_has_one_timestamp_per_period() -> None:
    started_at = datetime(2026, 9, 15, tzinfo=UTC)

    timestamps = release_schedule(
        target_rps=50_000,
        period_seconds=10,
        duration_seconds=30,
        started_at=started_at,
        release_interval_seconds=10,
    )

    assert timestamps == tuple(started_at + timedelta(seconds=i * 10) for i in range(3))


def test_release_schedule_rejects_non_positive_interval() -> None:
    with pytest.raises(ValueError, match="release interval"):
        release_schedule(
            target_rps=50_000,
            period_seconds=10,
            duration_seconds=30,
            started_at=datetime(2026, 9, 15, tzinfo=UTC),
            release_interval_seconds=0,
        )


@pytest.mark.parametrize("build_parser", [build_raw_parser, build_rum_parser])
def test_generators_parse_prepared_release_options(build_parser) -> None:
    args = build_parser().parse_args(
        [
            "--bucket",
            "example",
            "--service-id",
            "TestLogSvcABC123",
            "--target-rps",
            "50000",
            "--prepare-dir",
            "prepared",
            "--release-only",
            "--release-interval-seconds",
            "10",
        ]
    )

    assert args.prepare_dir.name == "prepared"
    assert args.release_only is True
    assert args.release_interval_seconds == 10


@pytest.mark.parametrize("build_parser", [build_raw_parser, build_rum_parser])
def test_generators_default_to_direct_mode(build_parser) -> None:
    args = build_parser().parse_args(
        [
            "--bucket",
            "example",
            "--service-id",
            "TestLogSvcABC123",
            "--target-rps",
            "1",
        ]
    )

    assert args.prepare_dir is None
    assert args.release_only is False


@pytest.mark.parametrize("module", [raw_logs, rum_logs])
def test_prepared_dry_run_writes_payloads_without_network(module, tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            module.__name__,
            "--bucket",
            "example",
            "--service-id",
            "TestLogSvcABC123",
            "--target-rps",
            "1",
            "--log-period-seconds",
            "1",
            "--duration-seconds",
            "1",
            "--shards",
            "1",
            "--gen-workers",
            "1",
            "--dry-run",
            "--prepare-dir",
            str(tmp_path),
            "--release-interval-seconds",
            "1",
        ],
    )

    assert module.main() == 0
    assert (tmp_path / "manifest.json").exists()
    assert list((tmp_path / "period-000000").glob("*.json.gz"))
    assert "scheduled=" in capsys.readouterr().out


@pytest.mark.parametrize("module", [raw_logs, rum_logs])
def test_release_only_requires_prepared_directory(module, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            module.__name__,
            "--bucket",
            "example",
            "--service-id",
            "TestLogSvcABC123",
            "--target-rps",
            "1",
            "--release-only",
        ],
    )

    with pytest.raises(SystemExit):
        module.main()


def test_prepared_release_schedule_is_anchored_after_generation(tmp_path, monkeypatch) -> None:
    captured: dict[str, datetime] = {}
    original_write_manifest = raw_logs._write_manifest

    def record_manifest_written(prepare_dir, periods, args):
        original_write_manifest(prepare_dir, periods, args)
        captured["prepared_at"] = datetime.now(UTC)

    def capture_schedule(**kwargs):
        captured["release_started_at"] = kwargs["started_at"]
        return release_schedule(**kwargs)

    monkeypatch.setattr(raw_logs, "_write_manifest", record_manifest_written)
    monkeypatch.setattr(raw_logs, "release_schedule", capture_schedule)
    monkeypatch.setattr(raw_logs, "_release_prepared", lambda **kwargs: [])
    monkeypatch.setattr(
        "sys.argv",
        [
            "generate_synthetic_raw_logs",
            "--bucket",
            "example",
            "--service-id",
            "TestLogSvcABC123",
            "--target-rps",
            "1",
            "--log-period-seconds",
            "1",
            "--duration-seconds",
            "2",
            "--shards",
            "1",
            "--gen-workers",
            "1",
            "--dry-run",
            "--prepare-dir",
            str(tmp_path),
            "--release-interval-seconds",
            "1",
        ],
    )

    assert raw_logs.main() == 0
    assert captured["release_started_at"] >= captured["prepared_at"]


@pytest.mark.parametrize("module", [raw_logs, rum_logs])
def test_prepared_release_uploads_shards_concurrently(module, tmp_path) -> None:
    period_dir = tmp_path / "period-000000"
    period_dir.mkdir()
    shards = []
    for index in range(3):
        filename = f"shard-{index:06d}.json.gz"
        (period_dir / filename).write_bytes(f"payload-{index}".encode())
        shards.append({"filename": filename, "key": f"key-{index}", "lines": index + 1})
    manifest = {"periods": [{"shards": shards}]}
    uploaded = []

    class Client:
        def put_object(self, **kwargs):
            uploaded.append((kwargs["Key"], kwargs["Body"]))

    reports = module._release_prepared(
        prepare_dir=tmp_path,
        manifest=manifest,
        fos_client=Client(),
        bucket="bucket",
        dry_run=False,
        schedule=(datetime.now(UTC),),
    )

    assert sorted(key for key, _ in uploaded) == ["key-0", "key-1", "key-2"]
    assert reports[0].files == 3
    assert reports[0].lines == 6


@pytest.mark.parametrize("module", [raw_logs, rum_logs])
def test_prepared_release_rejects_schedule_violation(module, tmp_path, monkeypatch) -> None:
    import time

    period_dir = tmp_path / "period-000000"
    period_dir.mkdir()
    filename = "shard-000000.json.gz"
    (period_dir / filename).write_bytes(b"payload")
    manifest = {"periods": [{"shards": [{"filename": filename, "key": "k", "lines": 1}]}], "period_seconds": 0.1}

    class SlowClient:
        def put_object(self, **kwargs):
            time.sleep(0.5)

    now = datetime.now(UTC)
    with pytest.raises(RuntimeError, match="schedule violated"):
        module._release_prepared(
            prepare_dir=tmp_path,
            manifest=manifest,
            fos_client=SlowClient(),
            bucket="bucket",
            dry_run=False,
            schedule=(now, now + timedelta(seconds=0.1)),
        )
