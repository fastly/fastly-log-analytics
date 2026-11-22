from pathlib import Path

from pytest import MonkeyPatch
from scan import credential_values, find_exposures


def test_scanner_reports_file_but_not_credential_value(tmp_path: Path) -> None:
    environment = tmp_path / ".env"
    environment.write_text("FASTLY_API_TOKEN=synthetic-test-credential-never-real\n")
    source = tmp_path / "source.rs"
    source.write_text('let value = "synthetic-test-credential-never-real";')
    matches = find_exposures([source], credential_values(environment, tmp_path / "state"))
    assert matches == [source]


def test_nested_local_state_secrets_are_scanned(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    (state / "install.json").write_text(
        '{"services":{"name":"public"},"credentials":{"session_key":"synthetic-session-material-32-bytes"}}'
    )
    source = tmp_path / "source.rs"
    source.write_text("synthetic-session-material-32-bytes")
    assert find_exposures([source], credential_values(tmp_path / ".env", state)) == [source]


def test_missing_environment_and_state_are_not_errors(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr("scan.os.environ", {})
    assert credential_values(tmp_path / ".env", tmp_path / "state") == set()
