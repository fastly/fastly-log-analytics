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

from unittest.mock import patch

from typer.testing import CliRunner

from backend.provision.cli import app

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


# ── --help ──────────────────────────────────────────────────────────────────


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


# ── cmd_provision ───────────────────────────────────────────────────────────


def test_provision_parses_edge_only_true_and_invokes_wizard():
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


def test_provision_parses_no_edge_only_as_false():
    """``--no-edge-only`` half of the toggle arrives as ``False``."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        result = runner.invoke(app, ["provision", "--yes", "--service-id", "s", "--no-edge-only"])
    assert result.exit_code == 0, result.output
    assert mock_wizard.call_args.args[0].edge_only is False


def test_provision_edge_only_default_is_none():
    """When NEITHER flag is supplied, wizard sees ``None`` so its own
    default-resolution (yes→True, interactive→prompt) can run."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        runner.invoke(app, ["provision", "--yes", "--service-id", "s"])
    assert mock_wizard.call_args.args[0].edge_only is None


def test_provision_collects_repeated_enable_group_into_list():
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


def test_provision_applies_documented_default_values():
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


# ── cmd_teardown ────────────────────────────────────────────────────────────


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


# ── update-logs / invite-analyst / update-cdn ───────────────────────────────


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


# ── list-groups / list-fields / unknown surfaces ───────────────────────────


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
