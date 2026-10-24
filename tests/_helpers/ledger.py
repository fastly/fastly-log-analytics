"""Shared test helpers for the ``ingest_ledger`` table and DuckLake batch convert tests."""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb


def clear_ledger(service_id: str, *, quarantine: bool = False):
    """Wipe ``ingest_ledger`` (and optionally ``quarantined_files``) rows for
    ``service_id`` and return ``(con, cur)``."""
    from backend.core.metadata.base import get_con

    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (service_id,))
    if quarantine:
        cur.execute("DELETE FROM quarantined_files WHERE service_id=?", (service_id,))
    con.commit()
    return con, cur


def seed_discovered(
    con,
    cur,
    service_id: str,
    object_keys: list[str] | str,
    *,
    status: str = "discovered",
) -> None:
    """Insert ``object_keys`` into ``ingest_ledger`` for ``service_id``."""
    now = time.time()
    keys = [object_keys] if isinstance(object_keys, str) else object_keys
    for object_key in keys:
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT (service_id, object_key) DO UPDATE SET "
            "status=EXCLUDED.status, attempts=0, last_error=NULL, committed_at=NULL",
            (service_id, object_key, status, now),
        )
    con.commit()


def ledger_row(con, service_id: str, object_key: str) -> dict[str, Any]:
    """Fetch a single ``ingest_ledger`` row as a dict."""
    row = con.execute(
        "SELECT status, claimed_by, claimed_at, committed_at, attempts, last_error, raw_deleted_at "
        "FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row is not None, f"no ledger row for {object_key}"
    return dict(row)


def download_stub(s3_to_local: dict[str, str], requested: list[list[str]] | None = None):
    """Build a ``_download_chunk_to_local`` test stub backed by ``s3_to_local``."""

    def _download(fos_client, s3_paths, tmpdir):
        if requested is not None:
            requested.append(list(s3_paths))
        got = {p: s3_to_local[p] for p in s3_paths if p in s3_to_local}
        return got, {v: k for k, v in got.items()}

    return _download


def attacher(catalog: str, data_path: str, read_only_override: bool = False):
    """Build a file-backed ``ducklake:`` attach callback for tests."""

    def _attach(con_arg, src_arg=None, read_only: bool = False):
        con_arg.execute("INSTALL ducklake; LOAD ducklake;")
        ro = ", READ_ONLY" if (read_only or read_only_override) else ""
        for attempt in range(40):
            try:
                con_arg.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data_path}'{ro})")
                break
            except duckdb.Error as exc:
                if "already exists" in str(exc).lower():
                    break
                if attempt == 39:
                    raise
                time.sleep(0.05)
        return True

    return _attach


def lake_reader(attach_fn, *, read_only: bool = False):
    """Open a fresh DuckDB connection attached to the test DuckLake catalog."""
    con = duckdb.connect()
    attach_fn(con, None, read_only=read_only)
    return con


class DuckLakeLedgerEnv:
    """Reusable context manager for DuckLake + ingest ledger convert tests.

    Patches ``load_config``, ``SERVICES_DATA_DIR``, ``get_source_for_service``,
    ``_get_fos_client``, ``_download_chunk_to_local``, ``_ducklake_attach``,
    ``_configure_fos``, and ``ducklake_table_name``.

    Yields ``self``, which is also directly callable as an ``attach(con, src, read_only=...)``
    function for backwards compatibility with ``with _batch_env(...) as attach:``.
    """

    def __init__(
        self,
        service_id: str,
        tmp_path,
        download=None,
        *,
        s3_to_local: dict[str, str] | None = None,
        fos=None,
        attach=None,
        cfg: dict[str, Any] | None = None,
        bucket: str = "test-bucket",
        prefix: str = "",
        read_only_override: bool = False,
        table_name: str | None = "logs",
    ) -> None:
        self.service_id = service_id
        self.tmp_path = tmp_path
        self.bucket = bucket
        self.prefix = prefix
        self.src = {"service_id": service_id, "name": service_id, "bucket": bucket, "prefix": prefix}
        self.catalog = str(tmp_path / "cat.ducklake")
        self.data_path = str(tmp_path / "lakedata")
        self.real_attach = attacher(self.catalog, self.data_path, read_only_override)
        self.attach = attach if attach is not None else self.real_attach
        self.fos = fos if fos is not None else MagicMock()
        self.cfg = cfg if cfg is not None else {"service_id": service_id}
        if download is not None:
            self.download = download
        else:
            self.download = download_stub(s3_to_local or {})
        self.table_name = table_name
        self._stack = []

    def __call__(self, con_arg, src_arg=None, read_only: bool = False):
        return self.real_attach(con_arg, src_arg, read_only=read_only)

    def reader(self, *, read_only: bool = False):
        return lake_reader(self.real_attach, read_only=read_only)

    def __enter__(self):
        patches = [
            patch("backend.config.load_config", return_value=self.cfg),
            patch("backend.core.duckdb.get_source_for_service", return_value=self.src),
            patch("backend.core.ingest._get_fos_client", return_value=self.fos),
            patch("backend.core.ingest._download_chunk_to_local", side_effect=self.download),
            patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=self.attach),
            patch("backend.core.ingest._configure_fos"),
            patch("backend.core.duckdb._configure_fos"),
            patch("backend.core.duckdb.get_memory_connection", side_effect=lambda _src: duckdb.connect()),
            patch("backend.config.HOT_S3_ENDPOINT", False),
        ]
        if self.table_name is not None:
            patches.append(patch("backend.core.iceberg._ducklake.ducklake_table_name", return_value=self.table_name))
        else:
            patches.append(
                patch(
                    "backend.core.iceberg._ducklake.ducklake_table_name",
                    side_effect=lambda src, table_name="logs": table_name,
                )
            )
        self._stack = patches
        for p in self._stack:
            p.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for p in reversed(self._stack):
            p.stop()
        self._stack = []
        return False
