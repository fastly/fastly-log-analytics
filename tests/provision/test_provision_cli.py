"""Typer command-surface tests for ``backend.provision.cli``.

Pairs with ``tests/test_provision_cli_handlers.py``: that file drives
``handle_*`` / ``wizard`` directly via ``SimpleNamespace`` to pin
business logic; THIS file drives the typer ``app`` via ``CliRunner``
to pin the OUTER dispatch surface — flag parsing, the
``--edge-only`` / ``--no-edge-only`` toggle, repeatable list options
(``--enable-group``), defaults, range/unknown-flag errors, ``--help``.

Audit finding: test_provision_cli_handlers.py:18 cites
``test_provision_cli.py`` but the file didn't exist. The docstring
also names an "outer dispatcher in ``backend/provision_cli.py``"; no
such module exists today — only a stale
``__pycache__/provision_cli.cpython-313.pyc`` from the Phase 10.5
rename to ``backend.provision.cli``. The typer ``app`` is the closest
exposed entry point and is what this file exercises.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from backend.provision import cli
from backend.provision.cli import app
from tests._helpers.config_fakes import mutate_via_save

# ── Merged from tests/test_provision_cli.py ──
runner = CliRunner()

SUBCOMMANDS = (
    "provision",
    "teardown",
    "invite-analyst",
    "update-logs",
    "update-cdn",
    "list-groups",
    "list-fields",
)


def test_top_level_help_lists_all_subcommands():
    """``--help`` enumerates every subcommand — losing one usually
    means it was un-registered."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for sub in SUBCOMMANDS:
        assert sub in result.output, f"subcommand {sub!r} missing from --help"


def test_no_args_prints_help_and_exits_nonzero():
    """``no_args_is_help=True`` → bare invocation prints help with
    non-zero exit, so shell-script misuse surfaces."""
    result = runner.invoke(app, [])
    assert result.exit_code != 0
    assert "provision" in result.output


def test_each_subcommand_has_working_help():
    """Each subcommand's ``--help`` panel renders — guards against
    typos in option/docstring definitions."""
    for sub in SUBCOMMANDS:
        result = runner.invoke(app, [sub, "--help"])
        assert result.exit_code == 0, f"--help for {sub!r} failed: {result.output}"


@patch("backend.provision.orchestrator.provision", return_value=[])
def test_provision_parses_edge_only_true_and_invokes_wizard(mock_provision):
    """``--edge-only`` arrives at wizard as ``True`` — typer's
    Optional[bool] toggle is the trickiest part of the surface."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        result = runner.invoke(app, ["provision", "--yes", "--token", "t", "--service-id", "s", "--edge-only"])
    assert result.exit_code == 0, result.output
    args = mock_wizard.call_args.args[0]
    assert args.edge_only is True
    assert args.token == "t"
    assert args.service_id == "s"
    assert args.yes is True


@patch("backend.provision.orchestrator.provision", return_value=[])
def test_provision_parses_no_edge_only_as_false(mock_provision):
    """``--no-edge-only`` half of the toggle arrives as ``False``."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        result = runner.invoke(app, ["provision", "--yes", "--service-id", "s", "--no-edge-only"])
    assert result.exit_code == 0, result.output
    assert mock_wizard.call_args.args[0].edge_only is False


@patch("backend.provision.orchestrator.provision", return_value=[])
def test_provision_edge_only_default_is_none(mock_provision):
    """When NEITHER flag is supplied, wizard sees ``None`` so its own
    default-resolution (yes→True, interactive→prompt) can run."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        runner.invoke(app, ["provision", "--yes", "--service-id", "s"])
    assert mock_wizard.call_args.args[0].edge_only is None


@patch("backend.provision.orchestrator.provision", return_value=[])
def test_provision_collects_repeated_enable_group_into_list(mock_provision):
    """Repeated ``--enable-group`` accumulates — collapsing would
    silently drop all but the last admin-supplied group."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        result = runner.invoke(
            app,
            [
                "provision",
                "--yes",
                "--service-id",
                "s",
                "--enable-group",
                "A",
                "--enable-group",
                "B",
                "--disable-field",
                "client.geo.region",
            ],
        )
    assert result.exit_code == 0, result.output
    args = mock_wizard.call_args.args[0]
    assert args.enable_group == ["A", "B"]
    assert args.disable_field == ["client.geo.region"]


@patch("backend.provision.orchestrator.provision", return_value=[])
def test_provision_applies_documented_default_values(mock_provision):
    """Defaults match the docker-compose-relied-upon contract:
    commit_interval_mins=5, log_retention_days=30, disable_*=False."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        runner.invoke(app, ["provision", "--yes", "--service-id", "s"])
    args = mock_wizard.call_args.args[0]
    assert args.commit_interval_mins == 5
    assert args.log_retention_days == 30
    assert args.disable_cron_sync is False
    assert args.disable_cron_compact is False
    assert args.disable_delete_after is False


def test_provision_rejects_out_of_range_sample_rate():
    """``--sample-rate`` is min=1, max=100 at the typer layer so the
    error points at the right flag (not the wizard's defence clamp)."""
    result = runner.invoke(app, ["provision", "--yes", "--service-id", "s", "--sample-rate", "500"])
    assert result.exit_code != 0


def test_teardown_propagates_no_remove_flags_to_handler():
    """``--no-remove-*`` flags arrive True — dropping any one silently
    widens the teardown blast radius."""
    with patch("backend.provision.cli.handle_teardown") as mock_handler:
        result = runner.invoke(
            app,
            [
                "teardown",
                "--yes",
                "--bucket",
                "b",
                "--token",
                "t",
                "--no-remove-logging",
                "--no-remove-cdn",
                "--no-remove-bucket",
            ],
        )
    assert result.exit_code == 0, result.output
    args = mock_handler.call_args.args[0]
    assert args.no_remove_logging is True
    assert args.no_remove_cdn is True
    assert args.no_remove_bucket is True
    assert args.bucket == "b"
    assert args.token == "t"


def test_teardown_remove_data_default_is_false():
    """``--remove-data`` is opt-in — default True would auto-delete
    the local DuckDB/cache on every teardown."""
    with patch("backend.provision.cli.handle_teardown") as mock_handler:
        runner.invoke(app, ["teardown", "--yes", "--bucket", "b", "--token", "t"])
    args = mock_handler.call_args.args[0]
    assert args.remove_data is False
    assert args.no_remove_logging is False
    assert args.no_remove_cdn is False
    assert args.no_remove_bucket is False


def test_update_logs_parses_dry_run_and_list_flags():
    """``--dry-run`` + repeated group flags arrive intact — the
    preset-swap merge-guard handler test fires these exact flags."""
    with patch("backend.provision.cli.handle_update_logs") as mock_handler:
        result = runner.invoke(
            app,
            [
                "update-logs",
                "--service-id",
                "svc",
                "--dry-run",
                "--preset",
                "standard",
                "--enable-group",
                "L",
                "--disable-group",
                "M",
            ],
        )
    assert result.exit_code == 0, result.output
    args = mock_handler.call_args.args[0]
    assert args.dry_run is True
    assert args.preset == "standard"
    assert args.enable_group == ["L"]
    assert args.disable_group == ["M"]
    assert args.service_id == "svc"


def test_invite_analyst_parses_minimal_flags():
    """Exposes only ``--yes`` and ``--service-id`` — adding flags
    without threading them through the handler has bitten this before."""
    with patch("backend.provision.cli.handle_invite_analyst") as mock_handler:
        result = runner.invoke(app, ["invite-analyst", "--yes", "--service-id", "svc-x"])
    assert result.exit_code == 0, result.output
    args = mock_handler.call_args.args[0]
    assert args.yes is True
    assert args.service_id == "svc-x"


def test_update_cdn_parses_service_and_token():
    """Simplest dispatcher — the two flags must still reach the handler."""
    with patch("backend.provision.cli.handle_update_cdn") as mock_handler:
        result = runner.invoke(app, ["update-cdn", "--service-id", "svc", "--token", "tok"])
    assert result.exit_code == 0, result.output
    args = mock_handler.call_args.args[0]
    assert args.service_id == "svc"
    assert args.token == "tok"


def test_list_groups_runs_without_service_id():
    """No ``--service-id`` succeeds — list-groups is sometimes the
    FIRST command an admin runs (no services yet)."""
    with patch("backend.provision.cli.handle_list_groups") as mock_handler:
        result = runner.invoke(app, ["list-groups"])
    assert result.exit_code == 0, result.output
    assert mock_handler.call_args.args[0].service_id is None


def test_list_fields_takes_no_args_and_runs():
    """``list-fields`` is argument-free — guards against adding a
    required option that breaks the catalog diagnostic."""
    with patch("backend.provision.cli.handle_list_fields") as mock_handler:
        result = runner.invoke(app, ["list-fields"])
    assert result.exit_code == 0, result.output
    assert mock_handler.call_count == 1


def test_unknown_subcommand_exits_nonzero():
    """Typo in subcommand → non-zero exit (shell scripts key on it)."""
    assert runner.invoke(app, ["nonexistent-subcommand"]).exit_code != 0


def test_unknown_flag_on_provision_exits_nonzero():
    """Unknown flag on a real subcommand exits non-zero (typer
    default) — silently ignoring would mask renamed-flag migrations."""
    result = runner.invoke(app, ["provision", "--yes", "--service-id", "s", "--this-flag-does-not-exist"])
    assert result.exit_code != 0


# ── Merged from tests/test_provision_cli_handlers.py ──
def _args(**overrides):
    """argparse.Namespace stand-in with the union of all CLI flags
    pre-populated to safe defaults — every handler reads ``getattr(args, X)``
    so missing attrs would AttributeError. The wizard takes the most
    flags, so default everything it inspects to None."""
    defaults = {
        "yes": True,  # non-interactive
        "token": None,
        "service_id": None,
        "endpoint_name": None,
        "region": None,
        "bucket": None,
        "prefix": None,
        "sample_rate": None,
        "edge_only": None,
        "period": None,
        "cdn_name": None,
        "cdn_prefix": None,
        "shield": None,
        "delete_after": True,
        "disable_delete_after": False,
        "disable_cron_sync": False,
        "commit_interval_mins": 5,
        "disable_cron_compact": False,
        "log_retention_days": 30,
        "preset": None,
        "enable_group": None,
        "disable_group": None,
        "enable_field": None,
        "disable_field": None,
        "dry_run": False,
        "remove_data": False,
        "no_remove_logging": False,
        "no_remove_cdn": False,
        "no_remove_bucket": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_handle_list_groups_prints_table_with_enabled_column(capsys):
    """Diagnostic command; pinned to ensure the column order (Group /
    Enabled / Bytes / Fields) stays stable since users grep the output."""
    cli.handle_list_groups(_args())
    out = capsys.readouterr().out
    assert "Group" in out and "Enabled" in out and "Bytes" in out
    # "(core)" row always present (group=None)
    assert "(core)" in out


def test_handle_list_groups_reads_existing_service_config(capsys):
    """When ``--service-id`` is provided, the "Enabled" column reflects
    that service's actual config — pinned because admins use the flag
    to audit a deployed service's setup."""
    fake_cfg = {"log_fields": {"groups": ["A"]}}
    with patch("backend.config.load_config", return_value=fake_cfg):
        cli.handle_list_groups(_args(service_id="svc-123"))
    out = capsys.readouterr().out
    # Just verify it didn't crash and rendered output
    assert "(core)" in out


def test_handle_list_fields_prints_field_catalog(capsys):
    """Pinned to ensure the field table renders — admins use this to
    discover field IDs for ``--enable-field`` flags."""
    cli.handle_list_fields(_args())
    out = capsys.readouterr().out
    assert "Field" in out and "Group" in out and "Type" in out
    # At least one core field always present
    assert "client.ip" in out or "timestamp" in out or "method" in out


def test_handle_teardown_fails_when_no_service_and_no_bucket(capsys):
    """Without a service config OR a ``--bucket`` flag, the handler
    can't know what to delete — must exit non-zero, not silently no-op.
    Pinned because a silent no-op would mislead admins into thinking
    the teardown succeeded."""
    with patch("backend.config.list_service_ids", return_value=[]):
        with pytest.raises(SystemExit) as exc:
            cli.handle_teardown(_args())
    assert exc.value.code == 1


def test_handle_teardown_falls_back_to_bucket_flag_when_no_config(capsys):
    """If no service config exists but ``--bucket`` is provided, the
    handler proceeds with a minimal state. Pinned because this is the
    recovery path for "deleted the config but the bucket still exists"
    cleanup scenarios."""
    with (
        patch("backend.config.list_service_ids", return_value=[]),
        patch("backend.provision.cli.perform_teardown", return_value=iter([])),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        # token from env so the prompt path is skipped under --yes
        cli.handle_teardown(_args(bucket="orphan-bucket", token="t"))


def test_handle_teardown_loads_state_from_service_config(monkeypatch):
    monkeypatch.delenv("FASTLY_API_KEY", raising=False)
    """When the service config exists, teardown state pulls the bucket,
    region, keys, endpoint name etc. from there. Pinned because losing
    this would force admins to re-supply every flag."""
    fake_cfg = {
        "fos_bucket": "my-bucket",
        "fos_region": "us-east-1",
        "fastly_api_key": "tok",
        "provisioning": {
            "fos_key_id": "k1",
            "endpoint_name": "MyEndpoint",
            "cdn_service_id": "cdn1",
            "cdn_url": "https://x.example",
        },
    }
    perform_calls = []

    def _record_teardown(state, token, opts=None):
        perform_calls.append((state, token, opts))
        return iter([])

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.provision.cli.perform_teardown", side_effect=_record_teardown),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        cli.handle_teardown(_args(service_id="svc-1"))

    assert len(perform_calls) == 1
    state, token, opts = perform_calls[0]
    assert state["fos_bucket_name"] == "my-bucket"
    assert state["endpoint_name"] == "MyEndpoint"
    assert state["cdn_service_id"] == "cdn1"
    assert token == "tok"
    # All remove_* flags default to True
    assert opts == {"remove_logging": True, "remove_cdn": True, "remove_bucket": True, "remove_scoring": True}


def test_handle_teardown_carries_scoring_block_into_state():
    """The teardown state must carry cfg['scoring'] so perform_teardown can tear
    down the Compute scorer + its stores. Pinned because the root cause of the
    orphaned-scoring bug was the CLI state builder omitting the scoring block."""
    fake_cfg = {
        "fos_bucket": "my-bucket",
        "fastly_api_key": "tok",
        "provisioning": {"endpoint_name": "E", "cdn_service_id": "cdn1"},
        "scoring": {
            "enabled": True,
            "scoring_service_id": "SCORESVC",
            "scoring_keys_store_id": "KEYS",
            "scoring_config_store_id": "CFG",
            "scoring_matrix_store_id": "MTX",
        },
    }
    perform_calls = []

    def _record(state, token, opts=None):
        perform_calls.append(state)
        return iter([])

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.provision.cli.perform_teardown", side_effect=_record),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        cli.handle_teardown(_args(service_id="svc-1", token="tok"))

    assert len(perform_calls) == 1
    assert perform_calls[0]["scoring"]["enabled"] is True
    assert perform_calls[0]["scoring"]["scoring_service_id"] == "SCORESVC"


def test_handle_teardown_respects_no_remove_flags():
    """The ``--no-remove-logging`` / ``--no-remove-cdn`` /
    ``--no-remove-bucket`` flags must propagate through to the
    orchestrator's opts dict. Pinned because partial teardowns are the
    only safe way to remove an analyst service without breaking the
    admin one."""
    perform_calls = []

    def _record(state, token, opts=None):
        perform_calls.append(opts)
        return iter([])

    with (
        patch("backend.config.list_service_ids", return_value=[]),
        patch("backend.provision.cli.perform_teardown", side_effect=_record),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        cli.handle_teardown(
            _args(
                bucket="b",
                token="t",
                no_remove_logging=True,
                no_remove_cdn=True,
                no_remove_bucket=False,
            )
        )
    assert perform_calls == [
        {"remove_logging": False, "remove_cdn": False, "remove_bucket": True, "remove_scoring": True}
    ]


def test_handle_teardown_exits_1_on_orchestrator_exception(capsys):
    """Any exception raised by perform_teardown surfaces as exit code 1
    plus a "Teardown failed:" message — pinned because the exit code is
    what shell scripts key on."""
    with (
        patch("backend.config.list_service_ids", return_value=[]),
        patch("backend.provision.cli.perform_teardown", side_effect=RuntimeError("network down")),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_teardown(_args(bucket="b", token="t"))
    assert exc.value.code == 1
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "Teardown failed" in combined or "network down" in combined


def test_handle_invite_analyst_fails_without_service_id(capsys):
    """No --service-id and no configured services → exit 1. Pinned
    because the handler is sometimes invoked from a CI step where
    no services have been provisioned yet."""
    with patch("backend.config.list_service_ids", return_value=[]):
        with pytest.raises(SystemExit) as exc:
            cli.handle_invite_analyst(_args())
    assert exc.value.code == 1


def test_handle_invite_analyst_auto_picks_single_service():
    """When exactly one service is configured, the handler picks it
    automatically rather than prompting. Pinned because most installs
    have a single service and the prompt would be friction."""
    fake_cfg = {"access_level": "read_write"}
    with (
        patch("backend.config.list_service_ids", return_value=["only-svc"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch(
            "backend.provision.cli.generate_analyst_invite",
            return_value={"service_id": "only-svc", "secret_key": "sk"},
        ),
    ):
        cli.handle_invite_analyst(_args())


def test_handle_invite_analyst_rejects_readonly_service(capsys):
    """An analyst can only be invited from a ``read_write`` service —
    inviting from another analyst's read-only replica must be refused.
    Pinned because the read-only invite would generate an unusable
    credential that fails on first read."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value={"access_level": "read_only"}),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_invite_analyst(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_invite_analyst_prints_invite_and_warns_about_secret(capsys):
    """Output must include the invite payload AND the "save the secret
    now" warning. Pinned because the secret can never be regenerated;
    losing the warning would lead to a lockout."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value={"access_level": "read_write"}),
        patch(
            "backend.provision.cli.generate_analyst_invite",
            return_value={"service_id": "svc", "secret_key": "abc123"},
        ),
    ):
        cli.handle_invite_analyst(_args(service_id="svc"))
    out = capsys.readouterr().out
    assert "abc123" in out
    assert "secret" in out.lower()


def test_handle_invite_analyst_exits_1_on_generation_failure(capsys):
    """If ``generate_analyst_invite`` raises (e.g., DB write failed),
    surface as exit 1 with the message. Pinned because silent failures
    here have led to admins thinking the invite went through."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value={"access_level": "read_write"}),
        patch("backend.provision.cli.generate_analyst_invite", side_effect=RuntimeError("db locked")),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_invite_analyst(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_logs_fails_when_no_services_configured(capsys):
    with patch("backend.config.list_service_ids", return_value=[]):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_logs(_args())
    assert exc.value.code == 1


def test_handle_update_logs_fails_when_config_missing(capsys):
    """``list_service_ids`` returns an id but ``load_config`` returns
    None — happens when the metadata file is mid-delete. Pinned
    because a None-returning load shouldn't crash the CLI."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value=None),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_logs(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_logs_dry_run_prints_format_and_returns(capsys):
    """``--dry-run`` prints the generated log format and returns
    without calling the Fastly API. Pinned because admins use this
    to preview before pushing."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value={"log_fields": {"groups": ["A"]}}),
        patch("backend.core.field_registry.generate_log_format", return_value="EXPECTED_LOG_FORMAT"),
        patch("backend.provision.cli.update_logging_endpoint") as mock_update,
    ):
        cli.handle_update_logs(_args(service_id="svc", dry_run=True))
    assert "EXPECTED_LOG_FORMAT" in capsys.readouterr().out
    # Crucially, the Fastly push must NOT have been called
    mock_update.assert_not_called()


def test_handle_update_logs_pushes_to_fastly_and_persists_config():
    """Non-dry-run: write the merged config + call
    ``update_logging_endpoint`` with the right shape. Pinned because
    the ``update_format=True`` flag is what tells the API to actually
    push the format (vs just bumping the version)."""
    write_calls = []
    update_calls = []

    def _record_update(cfg, token):
        update_calls.append((cfg, token))
        return iter([{"type": "done", "version": 42}])

    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={
                "fastly_api_key": "tok",
                "log_fields": {"groups": ["A"]},
                "provisioning": {"endpoint_name": "ep", "sample_rate": 50, "edge_only": True, "log_period": 60},
            },
        ),
        patch("backend.provision.cli.write_service_config", side_effect=lambda c: write_calls.append(c)),
        patch("backend.provision.cli.update_logging_endpoint", side_effect=_record_update),
        patch("backend.core.field_registry.format_hash", return_value="hash123"),
    ):
        cli.handle_update_logs(_args(service_id="svc"))

    assert len(write_calls) == 1
    persisted = write_calls[0]
    assert persisted["log_fields"]["format_hash"] == "hash123"
    assert "format_updated_at" in persisted["log_fields"]

    assert len(update_calls) == 1
    cfg, token = update_calls[0]
    assert cfg["update_format"] is True
    assert cfg["logging_service_id"] == "svc"
    assert cfg["sample_rate"] == 50
    assert token == "tok"


def test_handle_update_logs_preserves_scoring_custom_fields_on_preset_swap():
    """REGRESSION (sibling of 2026-06-02 state_sync incident): running
    ``handle_update_logs`` with a --preset (or --enable-group/--disable-group)
    argument used to clobber the entire cfg.log_fields, silently dropping
    the scoring custom_fields that ``enable_scoring`` had injected.
    The new merge guard preserves existing custom_fields and re-injects
    ``_SCORING_CUSTOM_FIELDS`` when scoring is enabled."""
    write_calls = []

    def _record_update(cfg, token):
        return iter([{"type": "done", "version": 1}])

    pre_scoring_field = {"name": "edge_score", "duckdb_type": "INTEGER", "enabled": True}

    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={
                "fastly_api_key": "tok",
                "scoring": {"enabled": True},
                "log_fields": {
                    "groups": ["A"],
                    "custom_fields": [pre_scoring_field],
                },
                "provisioning": {"endpoint_name": "ep"},
            },
        ),
        patch("backend.provision.cli.write_service_config", side_effect=lambda c: write_calls.append(c)),
        patch("backend.provision.cli.update_logging_endpoint", side_effect=_record_update),
        patch("backend.core.field_registry.format_hash", return_value="h"),
    ):
        # --preset triggers the rebuild path (the bug-bait branch). Without
        # the merge guard, the persisted cfg's custom_fields would be empty.
        cli.handle_update_logs(_args(service_id="svc", preset="standard"))

    from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES

    assert len(write_calls) == 1
    persisted_names = {cf["name"] for cf in write_calls[0]["log_fields"]["custom_fields"]}
    for name in _SCORING_FIELD_NAMES:
        assert name in persisted_names, f"scoring field {name!r} dropped on preset swap"


def test_handle_update_logs_exits_1_on_orchestrator_exception():
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={"fastly_api_key": "tok", "log_fields": {"groups": ["A"]}, "provisioning": {}},
        ),
        patch("backend.provision.cli.write_service_config"),
        patch("backend.provision.cli.update_logging_endpoint", side_effect=RuntimeError("api down")),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_logs(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_cdn_fails_when_no_services(capsys):
    with patch("backend.config.list_service_ids", return_value=[]):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_cdn(_args())
    assert exc.value.code == 1


def test_handle_update_cdn_fails_when_config_missing(capsys):
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value=None),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_cdn(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_cdn_uses_cdn_service_id_from_provisioning():
    """The CDN service ID lives under ``cfg['provisioning']['cdn_service_id']``
    — pinned because reading it from the wrong slot would re-deploy
    against the customer's CDN service (data loss)."""
    redeploy_calls = []
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={
                "fastly_api_key": "tok",
                "provisioning": {"cdn_service_id": "cdn-target", "rate_limiting": True},
            },
        ),
        patch("backend.provision.cli.account_has_rate_limiting", return_value=None),
        patch(
            "backend.provision.cli.redeploy_cdn_vcl",
            side_effect=lambda sid, token, rate_limiting=True: redeploy_calls.append((sid, token, rate_limiting)) or 5,
        ),
    ):
        cli.handle_update_cdn(_args(service_id="svc"))
    # Detection inconclusive (None) → fall back to the persisted flag (True).
    assert redeploy_calls == [("cdn-target", "tok", True)]


def test_handle_update_cdn_falls_back_to_find_by_name():
    """When ``cdn_service_id`` is missing from the config (config-
    corruption recovery), fall back to looking up by the configured
    CDN service name. Pinned because the fallback prevents admins
    from having to manually re-link a partial provision."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={
                "fastly_api_key": "tok",
                "cdn_service_name": "MyCDNService",
                "provisioning": {},
            },
        ),
        patch("backend.provision.cli.find_service_by_name", return_value={"id": "found-cdn-id"}),
        patch("backend.provision.cli.account_has_rate_limiting", return_value=None),
        patch("backend.provision.cli.redeploy_cdn_vcl", return_value=7) as mock_redeploy,
    ):
        cli.handle_update_cdn(_args(service_id="svc"))
    # The fallback ID must reach redeploy_cdn_vcl
    args_used, kwargs_used = mock_redeploy.call_args
    assert args_used[0] == "found-cdn-id"


def test_handle_update_cdn_fails_when_cdn_lookup_returns_none():
    """find_service_by_name → None → exit 1 (rather than passing
    None as the service ID, which would 404 the API)."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={"fastly_api_key": "tok", "cdn_service_name": "X", "provisioning": {}},
        ),
        patch("backend.provision.cli.find_service_by_name", return_value=None),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_cdn(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_cdn_exits_1_on_redeploy_exception():
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={"fastly_api_key": "tok", "provisioning": {"cdn_service_id": "cdn"}},
        ),
        patch("backend.provision.cli.account_has_rate_limiting", return_value=None),
        patch("backend.provision.cli.redeploy_cdn_vcl", side_effect=RuntimeError("vcl rejected")),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_cdn(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_cdn_reprobes_persists_and_applies_new_entitlement():
    """A customer who LOST edge rate limiting (or never had it) is detected on
    redeploy: the refreshed flag is persisted via save_config AND passed to
    redeploy_cdn_vcl so the CDN deploys without ratecounters."""
    saved = []
    redeploy_calls = []
    cfg = {
        "service_id": "svc",
        "fastly_api_key": "tok",
        # persisted flag says True, but the account no longer has the feature.
        "provisioning": {"cdn_service_id": "cdn", "rate_limiting": True},
    }
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(lambda sid, c: saved.append((sid, c)))),
        patch("backend.provision.cli.account_has_rate_limiting", return_value=False),
        patch(
            "backend.provision.cli.redeploy_cdn_vcl",
            side_effect=lambda sid, token, rate_limiting=True: redeploy_calls.append(rate_limiting) or 9,
        ),
    ):
        cli.handle_update_cdn(_args(service_id="svc"))

    # Refreshed flag persisted under provisioning.rate_limiting...
    assert saved and saved[0][0] == "svc"
    assert saved[0][1]["provisioning"]["rate_limiting"] is False
    # ...and the deploy uses the freshly-detected value, not the stale True.
    assert redeploy_calls == [False]


def test_handle_update_cdn_does_not_persist_when_flag_unchanged():
    """Detection matches the persisted flag → no redundant save_config write."""
    saved = []
    cfg = {
        "service_id": "svc",
        "fastly_api_key": "tok",
        "provisioning": {"cdn_service_id": "cdn", "rate_limiting": True},
    }
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(lambda sid, c: saved.append((sid, c)))),
        patch("backend.provision.cli.account_has_rate_limiting", return_value=True),
        patch("backend.provision.cli.redeploy_cdn_vcl", return_value=9),
    ):
        cli.handle_update_cdn(_args(service_id="svc"))

    assert saved == []


def test_wizard_yes_mode_builds_complete_config_with_defaults():
    """The ``--yes`` path skips every prompt and returns a fully-formed
    config. Pinned because the unattended-install flow (and the
    `/api/provision/start` API route) drive the same code path."""
    fake_svc = {"id": "svc-1", "name": "Customer Service"}
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value=fake_svc),
    ):
        cfg = cli.wizard(_args(token="tok", service_id="svc-1"))

    # Spot-check the canonical keys the orchestrator expects
    assert cfg["admin_token"] == "tok"
    assert cfg["logging_service_id"] == "svc-1"
    assert cfg["service_name"] == "Customer Service"
    assert cfg["fos_region"] == "us-east-1"
    assert cfg["fos_bucket_name"] == "fos-svc-1-logs"
    assert cfg["sample_rate"] == 100
    assert cfg["edge_only"] is True
    assert cfg["cdn_url"].startswith("https://svc-1.")
    assert cfg["delete_after"] is True
    assert "cdn_secret" in cfg and len(cfg["cdn_secret"]) >= 16
    assert cfg["log_fields"]["preset"] == "standard"


def test_wizard_exits_1_when_log_format_validation_fails(capsys):
    """If the pre-flight log-format check produces errors, the wizard
    aborts BEFORE asking for a token. Pinned because a bad log format
    means the deploy would fail anyway — better to fail fast than
    after the admin has typed in a token."""
    with patch("backend.provision.cli.validate_log_format", return_value=["LOG_FORMAT_TOO_LONG"]):
        with pytest.raises(SystemExit) as exc:
            cli.wizard(_args(token="tok", service_id="svc-1"))
    assert exc.value.code == 1
    assert "LOG_FORMAT_TOO_LONG" in capsys.readouterr().out


def test_wizard_sanitises_service_id_for_bucket_name():
    """Bucket name derives from service_id with non-alphanumeric chars
    replaced. Pinned because S3-compatible bucket names reject ``_``,
    ``.``, uppercase, etc., and a leaked unsafe id would fail at
    ``CreateBucket`` time."""
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
    ):
        cfg = cli.wizard(_args(token="t", service_id="My_Service.ID_123"))
    assert cfg["fos_bucket_name"] == "fos-my-service-id-123-logs"
    # No invalid bucket-name chars leak through
    assert "_" not in cfg["fos_bucket_name"]
    assert "." not in cfg["fos_bucket_name"]


def test_wizard_clamps_sample_rate_to_1_to_100():
    """sample_rate is clamped client-side to [1, 100] regardless of
    what the args say — pinned because Fastly rejects out-of-range
    values and clamping in the wizard prevents a confusing 422 from
    the deploy step."""
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
    ):
        cfg_high = cli.wizard(_args(token="t", service_id="s", sample_rate=500))
        cfg_low = cli.wizard(_args(token="t", service_id="s", sample_rate=-5))
    assert cfg_high["sample_rate"] == 100
    assert cfg_low["sample_rate"] == 1


def test_wizard_disable_delete_after_overrides_yes_default():
    """The wizard's ``--yes`` default for ``delete_after`` is True,
    but ``--disable-delete-after`` must flip it off. Pinned because
    customers retaining raw logs for compliance need this explicit
    opt-out."""
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
    ):
        cfg = cli.wizard(_args(token="t", service_id="s", disable_delete_after=True))
    assert cfg["delete_after"] is False


def test_wizard_disable_cron_flags_propagate_to_config():
    """Cron sync + cron compact default to enabled; the two
    ``--disable-*`` flags flip them off. Pinned because the cron
    scheduler reads these flags exactly once at startup."""
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
    ):
        cfg = cli.wizard(
            _args(token="t", service_id="s", disable_cron_sync=True, disable_cron_compact=True),
        )
    assert cfg["enable_cron_sync"] is False
    assert cfg["enable_cron_compact"] is False


def test_wizard_passes_log_fields_args_through_to_builder():
    """``--enable-group`` / ``--disable-group`` etc. must reach the
    log_fields config so the resulting VCL matches the admin's
    intent. Pinned because losing the wiring would silently fall
    back to the standard preset."""
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
    ):
        cfg = cli.wizard(
            _args(
                token="t",
                service_id="s",
                enable_group=["L"],
                disable_field=["client.geo.region"],
            ),
        )
    # The L group was added on top of the standard preset
    assert "L" in cfg["log_fields"]["groups"]
    # The disable_field override surfaces as False
    assert cfg["log_fields"]["field_overrides"].get("client.geo.region") is False


def test_wizard_confirmation_no_exits_silently(monkeypatch):
    """When the user answers No to the "Proceed?" prompt in
    interactive mode, exit with code 0 (success). Pinned because
    losing this would exit 1, making it look like a failure when
    the user just cancelled."""
    monkeypatch.setenv("FASTLY_API_KEY", "")
    args = _args(yes=False, token="t", service_id="s")

    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
        # All interactive prompts → defaults
        patch(
            "backend.provision.cli.ask", side_effect=lambda *a, **k: a[1] if len(a) > 1 else (k.get("default") or "")
        ),
        patch("backend.provision.cli.ask_int", side_effect=lambda *a, **k: 100),
        # ask_yes returns True for "edge_only", "delete_after"; False for "Proceed?"
        patch("backend.provision.cli.ask_yes", side_effect=[True, True, False]),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.wizard(args)

    assert exc.value.code == 0


def test_handle_teardown_no_user_confirmation_exits_silently():
    """When the user answers No to the "This cannot be undone" prompt
    in interactive mode, exit with code 0. Pinned because losing
    this would treat the user's cancellation as a teardown failure."""
    fake_cfg = {"fos_bucket": "my-bucket", "fastly_api_key": "tok", "provisioning": {}}

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.provision.cli.ask_yes", return_value=False),
        patch("backend.provision.cli.perform_teardown") as mock_teardown,
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_teardown(_args(yes=False, service_id="svc-1"))

    assert exc.value.code == 0
    mock_teardown.assert_not_called()


def test_handle_teardown_prompts_for_token_when_not_in_env_or_config():
    """When no --token, no FASTLY_API_KEY env var, and no
    stored fastly_api_key — prompt for it interactively. Pinned
    because losing this would exit 1 immediately even when the
    user could supply the token at runtime."""
    fake_cfg = {"fos_bucket": "b", "provisioning": {}}  # no fastly_api_key

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("os.getenv", return_value=None),
        patch("backend.provision.cli.ask", return_value="prompted-token"),
        patch("backend.provision.cli.perform_teardown", return_value=iter([])),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        # Should NOT exit (the prompt provides the token)
        cli.handle_teardown(_args(service_id="svc-1"))


def test_handle_teardown_exits_1_when_prompt_returns_no_token():
    """If the prompt returns empty, exit 1. Pinned because pressing
    Enter at the prompt without typing should fail-fast."""
    fake_cfg = {"fos_bucket": "b", "provisioning": {}}

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("os.getenv", return_value=None),
        patch("backend.provision.cli.ask", return_value=""),  # empty prompt response
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_teardown(_args(service_id="svc-1"))
    assert exc.value.code == 1


def test_handle_invite_analyst_multi_service_prompts_for_id():
    """When multiple services exist and no --service-id was supplied,
    print a menu and prompt for the ID. Pinned because losing this
    would auto-pick the first service silently (wrong service in a
    multi-tenant install)."""
    cfgs_by_id = {
        "svc-a": {"name": "Svc A", "access_level": "read_write"},
        "svc-b": {"name": "Svc B", "access_level": "read_write"},
    }

    with (
        patch("backend.config.list_service_ids", return_value=["svc-a", "svc-b"]),
        patch("backend.config.load_config", side_effect=lambda sid: cfgs_by_id.get(sid)),
        patch("backend.provision.cli.ask", return_value="svc-b"),
        patch(
            "backend.provision.cli.generate_analyst_invite",
            return_value={"service_id": "svc-b", "secret_key": "sk"},
        ),
    ):
        # Should NOT raise — the prompt picks svc-b
        cli.handle_invite_analyst(_args(service_id=None))


def test_wizard_default_token_from_env(monkeypatch):
    """When no ``--token`` is provided, the wizard reads ``FASTLY_API_KEY``
    from the environment. Pinned because docker-compose deployments
    set the env var rather than passing flags."""
    monkeypatch.setenv("FASTLY_API_KEY", "env-tok")
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}) as mock_fastly,
    ):
        cli.wizard(_args(service_id="s"))
    # fastly was called with the env-derived token
    _, kwargs = mock_fastly.call_args
    assert kwargs.get("token") == "env-tok"


def test_handle_enable_scoring_dispatches_to_orchestrator():
    """enable-scoring resolves service + token from config and calls
    session_scoring_orchestrator.enable_scoring. Pinned so the CLI redeploy
    path stays wired to the same flow the admin UI button runs."""
    fake_cfg = {"fastly_api_key": "tok", "scoring": {"enabled": True}}
    calls = []

    def _record(sid, token, **kw):
        calls.append((sid, token))
        return {"logging_service_active_version": 471}

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.provision.session_scoring_orchestrator.enable_scoring", side_effect=_record),
    ):
        cli.handle_enable_scoring(_args(service_id="svc-1"))

    assert calls == [("svc-1", "tok")]


def test_handle_enable_scoring_no_services_exits():
    """No configured service → clean sys.exit, not a traceback."""
    with (
        patch("backend.config.list_service_ids", return_value=[]),
        pytest.raises(SystemExit),
    ):
        cli.handle_enable_scoring(_args(service_id=None))


def test_handle_disable_scoring_dispatches_to_orchestrator():
    """disable-scoring (with --yes, non-interactive) resolves service + token
    and calls session_scoring_orchestrator.disable_scoring."""
    fake_cfg = {"fastly_api_key": "tok", "scoring": {"enabled": True}}
    calls = []

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch(
            "backend.provision.session_scoring_orchestrator.disable_scoring",
            side_effect=lambda sid, token, **kw: calls.append((sid, token)),
        ),
    ):
        cli.handle_disable_scoring(_args(service_id="svc-1", yes=True))

    assert calls == [("svc-1", "tok")]


def test_handle_disable_scoring_aborts_without_confirmation():
    """Without --yes the handler prompts; a 'no' answer aborts via sys.exit
    BEFORE touching the orchestrator."""
    fake_cfg = {"fastly_api_key": "tok", "scoring": {"enabled": True}}
    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.provision.cli.ask_yes", return_value=False),
        patch("backend.provision.session_scoring_orchestrator.disable_scoring") as mock_disable,
        pytest.raises(SystemExit),
    ):
        cli.handle_disable_scoring(_args(service_id="svc-1", yes=False))
    mock_disable.assert_not_called()
