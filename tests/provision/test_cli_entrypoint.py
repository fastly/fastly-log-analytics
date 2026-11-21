from __future__ import annotations

import pytest

from backend.core.metadata import pg_pool
from backend.provision import cli
from tests._helpers.pg_pool_fakes import install_fake_pg_pool


@pytest.mark.parametrize("handler_fails", [False, True])
def test_cli_main_closes_the_metadata_pool_a_command_opened(
    monkeypatch: pytest.MonkeyPatch, handler_fails: bool
) -> None:
    opened = install_fake_pg_pool(monkeypatch)

    def handler(_args) -> None:
        pg_pool.get_pg_pool()
        if handler_fails:
            raise RuntimeError("pg down")

    monkeypatch.setattr(cli, "handle_list_fields", handler)
    monkeypatch.setattr("sys.argv", ["cli", "list-fields"])

    if handler_fails:
        with pytest.raises(RuntimeError, match="pg down"):
            cli.main()
    else:
        with pytest.raises(SystemExit) as exited:
            cli.main()
        assert exited.value.code == 0
    assert len(opened) == 1 and opened[0].closed
    assert pg_pool._pool is None


@pytest.mark.no_postgres
def test_cli_set_snapshot_interval_updates_config():
    from typer.testing import CliRunner

    from backend import config as svcconfig

    svcconfig.save_config(
        "svc_cli_snap",
        {
            "service_id": "svc_cli_snap",
            "name": "CLI Snap Service",
            "provisioning": {"cron_sync": {"enabled": True, "snapshot_interval_minutes": 5}},
        },
    )
    runner = CliRunner()
    res = runner.invoke(
        cli.app, ["set-snapshot-interval", "--service-id", "svc_cli_snap", "--snapshot-interval-minutes", "60"]
    )
    assert res.exit_code == 0, res.output
    updated = svcconfig.load_config("svc_cli_snap")
    assert updated is not None
    assert updated["provisioning"]["cron_sync"]["snapshot_interval_minutes"] == 60


@pytest.mark.no_postgres
def test_cli_set_snapshot_interval_rejects_invalid_and_missing_service():
    from typer.testing import CliRunner

    runner = CliRunner()
    bad = runner.invoke(cli.app, ["set-snapshot-interval", "--service-id", "svc1", "--snapshot-interval-minutes", "10"])
    assert bad.exit_code == 2

    missing = runner.invoke(
        cli.app, ["set-snapshot-interval", "--service-id", "nonexistent_svc", "--snapshot-interval-minutes", "30"]
    )
    assert missing.exit_code == 1


@pytest.mark.no_postgres
def test_cli_provision_rejects_invalid_snapshot_interval():
    from typer.testing import CliRunner

    runner = CliRunner()
    bad = runner.invoke(cli.app, ["provision", "--snapshot-interval-minutes", "10"])
    assert bad.exit_code == 2


@pytest.mark.no_postgres
@pytest.mark.parametrize(
    ("sid", "initial_cfg"),
    [
        ("svc_null_prov", {"service_id": "svc_null_prov", "provisioning": None}),
        ("svc_str_prov", {"service_id": "svc_str_prov", "provisioning": "bad"}),
        ("svc_null_cs", {"service_id": "svc_null_cs", "provisioning": {"cron_sync": None}}),
        ("svc_str_cs", {"service_id": "svc_str_cs", "provisioning": {"cron_sync": "bad"}}),
    ],
)
def test_cli_set_snapshot_interval_recovers_null_or_non_dict_blocks(sid: str, initial_cfg: dict) -> None:
    from typer.testing import CliRunner

    from backend import config as svcconfig

    svcconfig.save_config(sid, initial_cfg)
    runner = CliRunner()
    res = runner.invoke(cli.app, ["set-snapshot-interval", "--service-id", sid, "--snapshot-interval-minutes", "30"])
    assert res.exit_code == 0, res.output
    reloaded = svcconfig.load_config(sid)
    assert reloaded is not None
    assert reloaded["provisioning"]["cron_sync"]["snapshot_interval_minutes"] == 30


@pytest.mark.no_postgres
@pytest.mark.parametrize("bad_val", [True, False, 15.9, float("inf"), float("nan")])
def test_validate_snapshot_interval_minutes_rejects_bool_and_float(bad_val) -> None:
    import typer

    with pytest.raises(typer.BadParameter):
        cli._validate_snapshot_interval_minutes(bad_val)
