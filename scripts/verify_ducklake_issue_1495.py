#!/usr/bin/env python3
"""Run DuckLake's superseded-inlined-table regression against a bundled extension."""

from __future__ import annotations

import os
import tempfile

import duckdb


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> None:
    if duckdb.__version__ != "1.5.4":
        raise RuntimeError(f"expected DuckDB 1.5.4, found {duckdb.__version__}")

    extension_path = os.environ["DUCKLAKE_EXTENSION_PATH"]
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "issue_1495.db")
        data_path = os.path.join(temp_dir, "data", "issue_1495")
        con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        try:
            con.execute(f"LOAD {_literal(extension_path)}")
            con.execute("LOAD parquet")
            catalog = _literal(f"ducklake:{db_path}")
            options = (
                f"DATA_PATH {_literal(data_path)}, DATA_INLINING_ROW_LIMIT 100, METADATA_CATALOG 'issue_1495_meta'"
            )
            con.execute(f"ATTACH {catalog} AS writer ({options})")
            con.execute(f"ATTACH {catalog} AS maintainer ({options})")
            con.execute("CREATE TABLE writer.events (id INTEGER, part INTEGER)")
            con.execute("ALTER TABLE writer.events SET PARTITIONED BY (part)")
            assert con.execute("SELECT count(*) FROM issue_1495_meta.ducklake_inlined_data_tables").fetchone() == (2,)
            assert con.execute("SELECT count(*) FROM maintainer.events").fetchone() == (0,)

            con.execute("INSERT INTO writer.events SELECT i, i % 2 FROM range(10) t(i)")
            con.execute("CALL ducklake_flush_inlined_data('writer')")
            con.execute("DELETE FROM maintainer.events WHERE id < 4")
            assert con.execute("SELECT count(*), sum(id) FROM maintainer.events").fetchone() == (6, 39)
            con.execute("INSERT INTO maintainer.events VALUES (10, 0)")
            assert con.execute("SELECT rows_flushed FROM ducklake_flush_inlined_data('maintainer')").fetchone() == (1,)
            assert con.execute("SELECT count(*), sum(id) FROM writer.events").fetchone() == (7, 49)

            con.execute("CREATE TABLE writer.t (id INTEGER)")
            con.execute("INSERT INTO writer.t VALUES (1), (2)")
            con.execute("SET VARIABLE insert_snapshot = (SELECT max(snapshot_id) FROM writer.snapshots())")
            assert con.execute(
                "SELECT count(*) FROM maintainer.t AT (VERSION => getvariable('insert_snapshot'))"
            ).fetchone() == (2,)
            con.execute("ALTER TABLE writer.t ADD COLUMN v INTEGER")
            con.execute("CALL ducklake_flush_inlined_data('writer', table_name => 't')")
            assert con.execute(
                "SELECT count(*), sum(id) FROM maintainer.t AT (VERSION => getvariable('insert_snapshot'))"
            ).fetchone() == (2, 3)

            con.execute("CREATE TABLE writer.p (id INTEGER)")
            con.execute("INSERT INTO writer.p VALUES (1), (2)")
            con.execute("SET VARIABLE pinned_snapshot = (SELECT max(snapshot_id) FROM writer.snapshots())")
            con.execute(
                f"ATTACH {catalog} AS pinned (DATA_PATH {_literal(data_path)}, "
                "METADATA_CATALOG 'issue_1495_meta', SNAPSHOT_VERSION getvariable('pinned_snapshot'))"
            )
            assert con.execute("SELECT count(*) FROM pinned.p").fetchone() == (2,)
            con.execute("ALTER TABLE writer.p ADD COLUMN v INTEGER")
            con.execute("CALL ducklake_flush_inlined_data('writer', table_name => 'p')")
            assert con.execute("SELECT count(*), sum(id) FROM pinned.p").fetchone() == (2, 3)
        finally:
            con.close()
    print("DuckLake issue #1495 regression passed")


if __name__ == "__main__":
    main()
