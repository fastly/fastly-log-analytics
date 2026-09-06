import os

import duckdb

from backend.core.local_compaction import _compact_single_partition


def test_compact_without_ip_column(tmp_path):
    # Create fake parquet files with timestamp and some data but no ip column
    con = duckdb.connect(":memory:")

    file1_name = "file1.parquet"
    file1 = os.path.join(tmp_path, file1_name)
    con.execute("CREATE TABLE t1 (timestamp TIMESTAMP, value INTEGER)")
    con.execute("INSERT INTO t1 VALUES ('2026-08-19 12:00:00', 42)")
    con.execute(f"COPY t1 TO '{file1}' (FORMAT PARQUET)")
    con.execute("DROP TABLE t1")

    file2_name = "file2.parquet"
    file2 = os.path.join(tmp_path, file2_name)
    con.execute("CREATE TABLE t2 (timestamp TIMESTAMP, value INTEGER)")
    con.execute("INSERT INTO t2 VALUES ('2026-08-19 12:05:00', 100)")
    con.execute(f"COPY t2 TO '{file2}' (FORMAT PARQUET)")
    con.close()

    # We will test _compact_single_partition directly on these files.
    # We must ensure that it successfully merges them into a single file and deletes the originals
    # without raising a Binder Error: Referenced column "ip" not found.
    parquets = [file1_name, file2_name]
    out_dir = str(tmp_path)

    # Run compaction!
    res = _compact_single_partition(out_dir, parquets)

    assert res is not None
    assert res["files_removed"] == 2
    assert len(res["removed_basenames"]) == 2

    # Verify the merged file exists and contains the sorted data
    merged_files = [f for f in os.listdir(out_dir) if f.endswith(".parquet") and f not in parquets]
    assert len(merged_files) == 1

    merged_path = os.path.join(out_dir, merged_files[0])
    con = duckdb.connect(":memory:")
    rows = con.execute(f"SELECT * FROM read_parquet('{merged_path}') ORDER BY timestamp").fetchall()
    assert len(rows) == 2
    assert rows[0][1] == 42
    assert rows[1][1] == 100
    con.close()
