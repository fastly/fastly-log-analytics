from __future__ import annotations

import argparse

import pytest

from scripts.lib.cli import add_common_args


def _parser(**kwargs: object) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="t")
    add_common_args(parser, **kwargs)  # type: ignore[arg-type]
    return parser


def test_adds_nothing_unless_asked() -> None:
    with pytest.raises(SystemExit):
        _parser().parse_args(["--timeout", "1"])
    with pytest.raises(SystemExit):
        _parser().parse_args(["--json"])


def test_timeout_default_type_and_help() -> None:
    parser = _parser(timeout_default=5.0, timeout_label="HTTP request")
    assert parser.parse_args([]).timeout == 5.0
    assert parser.parse_args(["--timeout", "2.5"]).timeout == 2.5
    assert "HTTP request timeout in seconds (default: 5.0)" in parser.format_help()


def test_json_flag() -> None:
    parser = _parser(json_help="Output raw JSON results")
    assert parser.parse_args([]).json is False
    assert parser.parse_args(["--json"]).json is True
    assert "Output raw JSON results" in parser.format_help()


@pytest.mark.parametrize(
    ("module", "default"),
    [("scripts.check_environment_health", 5.0), ("scripts.dev.audit_environments", 10.0)],
)
def test_adopting_scripts_keep_their_timeout_defaults(
    module: str, default: float, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import importlib

    mod = importlib.import_module(module)
    monkeypatch.setattr("sys.argv", ["prog", "--help"])
    with pytest.raises(SystemExit):
        mod.main()
    out = capsys.readouterr().out
    assert f"timeout in seconds (default: {default})" in out
    assert "--json" in out
