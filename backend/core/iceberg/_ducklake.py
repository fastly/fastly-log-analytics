"""DuckLake catalog attach + registration helpers.

The v3 storage layer replaces the pyiceberg write path with a DuckLake
catalog attached as ``lake`` on each DuckDB connection. This module owns
the attach contract:

- **Durability**: for cloud-backed sources the default ``DATA_PATH`` is
  ``s3://{bucket}/{prefix}/ducklake/`` so committed parquet lands in FOS.
  The raw ``.gz`` files are deleted after ingest, so a local-disk default
  would leave the ONLY copy of the data on the VM's ephemeral disk.
  Local-only sources (and an explicit ``DUCKLAKE_DATA_PATH`` override)
  keep a local data path.
- **Tenant isolation**: when ``DUCKLAKE_CATALOG`` points every service at
  one shared catalog (e.g. a Postgres DSN), a bare ``lake.logs`` would mix
  tenants. :func:`ducklake_table_name` derives a per-service table name
  that ALL readers and writers must use (view builder, buffer commit,
  ingest). Per-service file catalogs use it too — harmless there.
- **Size cap**: DuckLake's ``target_file_size`` is pinned to the same cap
  local compaction uses (``LOCAL_COMPACT_MAX_PARTITION_MB``, 256 MB
  default) so ``ducklake_merge_adjacent_files`` / rewrites never collapse
  parquet into fewer-larger files past the cap. MANY small files are GOOD
  for DuckDB scan parallelism — never raise this to "merge harder".

Trap #4 applies: every interpolated value in the ATTACH /
``ducklake_add_data_files`` SQL is escaped via ``escape_sql_literal`` or
validated as a bare identifier.
"""

from __future__ import annotations

import logging
import os
import re

from backend import config
from backend.utils.sql_validator import escape_sql_literal

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Same knob the local tiered compaction honors (backend/core/local_compaction.py
# _MAX_PARTITION_BYTES). Keeping the two caps on one env var means DuckLake
# merges and local compaction can never disagree about the ceiling.
_DUCKLAKE_TARGET_FILE_SIZE_MB = int(os.environ.get("LOCAL_COMPACT_MAX_PARTITION_MB", "256") or "256")


def _safe_ident(name: str) -> str:
    """Sanitize an arbitrary string into a bare SQL identifier fragment."""
    clean = re.sub(r"[^A-Za-z0-9_]", "_", name or "").strip("_").lower()
    return clean or "default"


def ducklake_table_name(src: dict, table_name: str = "logs") -> str:
    """Per-service DuckLake table name for ``src``.

    Under a shared catalog (``DUCKLAKE_CATALOG`` set) every service attaches
    the SAME ``lake`` — a bare ``lake.logs`` mixes tenants. This helper is
    the single naming authority used by the view builder, the buffer
    commit, and ingest; anything that reads or writes ``lake.*`` must go
    through it. Non-``logs`` tables (RUM ``client_vitals`` /
    ``client_errors``) get a per-service suffix too.
    """
    service_id = src.get("service_id") or src.get("name") or "default"
    base = f"logs_{_safe_ident(str(service_id))}"
    if table_name != "logs":
        base = f"{base}__{_safe_ident(table_name)}"
    return base


def _is_local_only(source: dict) -> bool:
    """Mirror of ``_core._is_local_only_source`` (kept inline to avoid a
    module-load cycle: _core imports this module during package init)."""
    if source.get("fos_local_warehouse") is True:
        return True
    endpoint = source.get("fos_endpoint") or source.get("endpoint") or ""
    return endpoint in ("http://localhost:0", "http://127.0.0.1:0")


def _default_data_path(source: dict) -> str:
    """Default DuckLake DATA_PATH for ``source``.

    Cloud-backed sources (same condition the old FOS iceberg write used)
    default to durable object storage: the pipeline deletes raw ``.gz``
    files after ingest, so a local default would leave the sole copy of
    the data on the VM's ephemeral disk. Local-only sources keep local
    parquet under SERVICES_DATA_DIR.
    """
    bucket = source.get("bucket")
    if bucket and not _is_local_only(source):
        prefix = (source.get("prefix") or "").strip("/")
        base = f"{prefix}/ducklake" if prefix else "ducklake"
        return f"s3://{bucket}/{base}/"
    service_id = source.get("service_id") or source.get("name", "default")
    return str(config.SERVICES_DATA_DIR / str(service_id) / "parquet")


def _ducklake_attach(con, source: dict, read_only: bool = False) -> bool:
    """Attach the DuckLake catalog for ``source`` as ``lake`` on ``con``.

    Returns True when the catalog is attached (including "already
    attached"), False on failure. On a read-write attach the size-cap
    option is (re)asserted — see module docstring.
    """
    # DUCKLAKE_CATALOG only — deliberately does NOT fall back to
    # METADATA_DSN. The two are separate concerns (commit-path catalog vs.
    # cron/ingest bookkeeping) and ADR-15 §2 states the code does not assume
    # they coincide. A fallback made that false in the one configuration where
    # it was reachable: INGEST_MODE=sync with METADATA_DSN set (the documented
    # halfway point of the SQLite→Postgres metadata migration), where it would
    # silently plant DuckLake's catalog tables inside the metadata database
    # AND abandon the per-service .ducklake file that held the real table
    # state — a silent catalog swap, which fails empty rather than loud. In
    # celery mode it was already unreachable: validate_ingest_mode() requires
    # a Postgres DUCKLAKE_CATALOG, so the left operand is never falsy there.
    dsn = config.DUCKLAKE_CATALOG
    service_id = source.get("service_id") or source.get("name", "default")

    if not dsn:
        dsn = str(config.SERVICES_DATA_DIR / f"{service_id}.ducklake")
    elif dsn.startswith(("postgres://", "postgresql://")):
        dsn = f"postgres:{dsn}"

    data_path = config.DUCKLAKE_DATA_PATH or _default_data_path(source)

    try:
        con.execute("INSTALL ducklake; LOAD ducklake;")
    except Exception as e:
        logger.warning("[ducklake] %s: failed to INSTALL/LOAD ducklake extension: %s", service_id, e)
        return False

    if read_only and (dsn.startswith("postgres:") or not os.path.exists(dsn)):
        # A read-only attach of a not-yet-initialized catalog fails ("does
        # not exist - and creating a new DuckLake is explicitly disabled") —
        # for a local file we can cheaply detect that via os.path.exists;
        # for a Postgres DSN we can't, so always pre-create idempotently.
        # Create with a transient read-write attach so fresh services get a
        # queryable (empty) lake immediately.
        try:
            con.execute(
                f"ATTACH 'ducklake:{escape_sql_literal(dsn)}' AS __lake_init "
                f"(DATA_PATH '{escape_sql_literal(data_path)}', OVERRIDE_DATA_PATH TRUE);"
            )
            con.execute("DETACH __lake_init")
        except Exception as e:
            if "already attached" not in str(e) and "already exists" not in str(e):
                logger.info("[ducklake] %s: could not pre-create catalog for read-only attach: %s", service_id, e)

    ro = ", READ_ONLY" if read_only else ""
    attach_sql = (
        f"ATTACH 'ducklake:{escape_sql_literal(dsn)}' AS lake "
        f"(DATA_PATH '{escape_sql_literal(data_path)}'{ro}, OVERRIDE_DATA_PATH TRUE);"
    )
    try:
        con.execute(attach_sql)
    except Exception as e:
        msg = str(e)
        if "already exists" in msg or "already attached" in msg:
            return True
        logger.warning("[ducklake] %s: failed to attach ducklake catalog: %s", service_id, e)
        return False
    if not read_only:
        _apply_target_file_size(con)
    return True


def _apply_target_file_size(con, alias: str = "lake") -> None:
    """Pin DuckLake's target file size to the compaction cap (idempotent).

    ``ducklake_merge_adjacent_files`` and table rewrites honor this option,
    so setting it at attach time is what keeps DuckLake-side compaction
    size-capped like the local tiered compaction. Checked before set so a
    steady-state attach doesn't write the catalog every time.
    """
    if not _IDENT_RE.match(alias):
        raise ValueError(f"invalid ducklake alias: {alias!r}")
    target = f"{_DUCKLAKE_TARGET_FILE_SIZE_MB}MiB"
    target_bytes = str(_DUCKLAKE_TARGET_FILE_SIZE_MB * 1024 * 1024)
    try:
        row = con.execute(f"SELECT value FROM {alias}.options() WHERE option_name = 'target_file_size'").fetchone()
        if row and str(row[0]) == target_bytes:
            return
        con.execute(f"CALL {alias}.set_option('target_file_size', '{target}')")
    except Exception as e:
        logger.warning("[ducklake] failed to apply target_file_size=%s: %s", target, e)


def ducklake_current_snapshot_id(con, alias: str = "lake") -> int | None:
    """Latest snapshot id of the attached DuckLake catalog, or None.

    Used as the fast-path staleness token for the per-service ``logs``
    view: a commit (from any process sharing the catalog) bumps the
    snapshot id, which invalidates the cached view SQL. Catalog-wide by
    design — under a shared catalog this is conservative (another
    tenant's commit forces a rebuild) but never stale.
    """
    if not _IDENT_RE.match(alias):
        raise ValueError(f"invalid ducklake alias: {alias!r}")
    try:
        row = con.execute(
            f"SELECT snapshot_id FROM ducklake_snapshots('{alias}') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        return int(row[0]) if row else None
    except Exception as e:
        logger.info("[ducklake] snapshot-id probe failed (lake not attached?): %s", e)
        return None


def _ducklake_add_data_files(con, s3_paths: list[str], alias: str = "lake", table: str = "logs") -> None:
    """Register existing parquet files into ``alias.table``.

    NOT idempotent at the DuckLake level — re-adding a path duplicates its
    rows (verified against ducklake d318a545). Callers must skip paths
    already present in ``ducklake_list_files`` (see
    ``_ducklake_migration.adopt_iceberg_to_ducklake``).
    """
    if not s3_paths:
        return
    if not _IDENT_RE.match(alias):
        raise ValueError(f"invalid ducklake alias: {alias!r}")
    if not _IDENT_RE.match(table):
        raise ValueError(f"invalid ducklake table name: {table!r}")
    paths_str = ", ".join(f"'{escape_sql_literal(p)}'" for p in s3_paths)
    con.execute(f"CALL ducklake_add_data_files('{alias}', '{table}', [{paths_str}]);")
