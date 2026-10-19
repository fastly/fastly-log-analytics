"""Shared SQL identifier validator + single-owner guard for the bare-identifier regex."""

from __future__ import annotations

import pathlib

import pytest

from backend.core import sql_ident

BACKEND = pathlib.Path(__file__).resolve().parents[2] / "backend"


@pytest.mark.parametrize("name", ["lake", "_x", "a1", "Logs_Foo", "client_vitals", "_"])
def test_accepts_bare_identifiers(name: str) -> None:
    assert sql_ident.is_safe_ident(name) is True
    assert sql_ident.require_safe_ident(name, "bad ident") == name


@pytest.mark.parametrize(
    "name",
    ["", "1abc", "a-b", 'a"b', "a b", "a;DROP", "a\n", "x.y", "é", "lake'--"],
)
def test_rejects_everything_else(name: str) -> None:
    assert sql_ident.is_safe_ident(name) is False
    with pytest.raises(ValueError, match="bad ident"):
        sql_ident.require_safe_ident(name, "bad ident")


def test_rejects_non_strings() -> None:
    assert sql_ident.is_safe_ident(None) is False  # type: ignore[arg-type]
    assert sql_ident.is_safe_ident(5) is False  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        sql_ident.require_safe_ident(None, "bad ident")  # type: ignore[arg-type]


def test_error_message_embeds_label_and_repr() -> None:
    with pytest.raises(ValueError) as exc:
        sql_ident.require_safe_ident("a-b", "invalid ducklake alias")
    assert str(exc.value) == "invalid ducklake alias: 'a-b'"


def test_bare_identifier_regex_has_one_owner() -> None:
    """No module besides sql_ident may declare the bare-identifier regex literal."""
    needles = ("[A-Za-z_][A-Za-z0-9_]*",)
    offenders = []
    for path in BACKEND.rglob("*.py"):
        if path.name == "sql_ident.py":
            continue
        text = path.read_text()
        # pg_connection's unanchored token matcher and clickhouse param-name
        # matcher are lexers, not identifier validators.
        if path.name in {"pg_connection.py", "clickhouse_client.py"}:
            continue
        if any(n in text for n in needles):
            offenders.append(str(path.relative_to(BACKEND)))
    assert offenders == [], f"use backend.core.sql_ident instead of a private identifier regex: {offenders}"
