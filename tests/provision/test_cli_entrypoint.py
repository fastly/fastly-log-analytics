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
