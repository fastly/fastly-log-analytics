"""SQL-literal builders route single quotes through the shared ``escape_sql_literal``."""

from __future__ import annotations

import pytest

from backend.core.rollups._common import quote_path_list
from backend.utils.sql_validator import escape_sql_literal


def test_quote_path_list_doubles_embedded_quotes() -> None:
    assert quote_path_list(["/a/o'brien.parquet", "/b.parquet"]) == "'/a/o''brien.parquet', '/b.parquet'"


def test_escape_sql_literal_rejects_non_str() -> None:
    with pytest.raises(TypeError):
        escape_sql_literal(5)  # type: ignore[arg-type]


def test_no_hand_rolled_quote_doubling_left_in_backend() -> None:
    import pathlib

    needle = 'replace("\'", "\'\'")'
    root = pathlib.Path(__file__).resolve().parents[2] / "backend"
    offenders = [
        str(p.relative_to(root))
        for p in root.rglob("*.py")
        if needle in p.read_text(encoding="utf-8") and p.name != "sql_validator.py"
    ]
    assert offenders == []
