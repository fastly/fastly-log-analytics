"""One-time adoption of legacy pyiceberg-era parquet into DuckLake.

``adopt_iceberg_to_ducklake`` registers the parquet the old pyiceberg
pipeline left behind into the per-service DuckLake table, so pre-v3
history stays queryable through the ``lake``-backed logs view.

**FOS is the system of truth.** The legacy table's data files live at
``s3://{bucket}/{prefix}/iceberg/...`` for a cloud-backed source (and
under ``file://{cache}/iceberg/...`` for a local-only warehouse); the
``cache/{bucket}/data/`` hive tree is only a *mirror* that ``sync_data``
downloads on demand and that cache expiry prunes. Adopting the mirror
alone loses every partition older than the local cache window — the
common shape being ``data_retention_days: 365`` against the default
90-day ``cache_retention_days``, where the rows are not destroyed but
are stranded in an Iceberg table nothing reads any more. So the adopter
enumerates the legacy table's live data files from its own manifests and
registers those.

**The two sources are alternatives, not a union.** ``cache/.../data/`` is
a byte-for-byte mirror of the very FOS objects the manifests name (see
``sync_data``'s ``_cloud_uri_to_local_path`` mapping), and local
compaction rewrites subsets of that mirror into ``compacted_*.parquet``
/ ``daily/`` / ``weekly/`` files holding the same rows again. Adopting
manifests *and* the local tree would therefore double-count every row
inside the cache window — on default settings (cache 90 d ⊇ retention
30 d) that is the whole table. The legacy table wins whenever it yields
at least one live data file; the local tree is the fallback for a
service whose legacy table is genuinely absent (never provisioned, or
already torn down), which is also the shape of a fresh v3 service.

Idempotent: ``ducklake_add_data_files`` DUPLICATES rows when a path is
re-added (verified against ducklake d318a545, for ``s3://`` paths as
well as local ones), so files already present in ``ducklake_list_files``
are skipped. Note that DuckLake echoes an object-storage URI back
*verbatim* — ``os.path.abspath`` must never be applied to one, or the
comparison silently never matches and every re-run duplicates the whole
table. See :func:`_normalize_data_path`.

Validation compares the lake row-count delta against the adopted files'
own row counts and raises on mismatch.
"""

from __future__ import annotations

import glob
import json
import logging
import os
import re
import threading
import time

from backend.utils.sql_validator import escape_sql_literal

logger = logging.getLogger(__name__)

# RFC 3986 scheme. Anchored so a Windows drive letter or a bare relative
# path never reads as a URI.
_URI_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")

# Adopt in bounded batches so one bad file fails a small batch, not the run.
_ADOPT_BATCH_SIZE = 200

# cron_runs.task used for the boot-time adoption. Doubles as the durable
# "already done" guard — a terminal 'success' row means this service has
# been adopted and must never be adopted again (re-adoption duplicates
# rows). Reads as a normal cron row in /api/cron-runs and the Cron UI.
ADOPTION_TASK = "ducklake_adopt"

# Opt-out for operators who want to drive the adoption by hand via
# POST /api/admin/ducklake/migrate instead of on boot.
_SKIP_ENV = "FLA_SKIP_LEGACY_ADOPTION"


def _legacy_data_dirs(src: dict, cache_dir: str) -> list[str]:
    """Candidate directories holding the old pyiceberg-era parquet.

    ``{cache}/data`` is the hive mirror ``sync_data`` downloads into.
    ``{cache}/iceberg/data`` is kept for hand-migrated local warehouses;
    pyiceberg's own local layout is ``{cache}/iceberg/<ns>/<table>/data``,
    which the manifest enumeration in :func:`_legacy_iceberg_data_files`
    covers properly (and, unlike a glob, without picking up files the
    table has since dropped).
    """
    dirs = [os.path.join(cache_dir, "data")]
    dirs.append(os.path.join(cache_dir, "iceberg", "data"))
    return [d for d in dirs if os.path.isdir(d)]


def _normalize_data_path(path: str) -> str:
    """Canonical form for comparing data-file paths across the two systems.

    DuckLake stores whatever string it was handed: a local path comes
    back as the absolute path it was registered with, and an
    object-storage URI comes back verbatim (``s3://bucket/key``).
    ``os.path.abspath`` on the latter mangles it into
    ``/cwd/s3:/bucket/key`` — which is why the dedupe must branch on the
    scheme rather than normalising everything as a filesystem path.

    ``file://`` URIs (pyiceberg's local-warehouse form) are reduced to a
    plain absolute path so they compare equal to the same file found by
    the local glob, and so DuckDB gets a path shape it reads natively.
    """
    if not path:
        return path
    if path.startswith("file://"):
        return os.path.abspath(path[len("file://") :])
    if _URI_SCHEME_RE.match(path):
        # Any other scheme (s3://, gs://, …): leave untouched.
        return path
    return os.path.abspath(path)


def _local_legacy_files(src: dict, cache_dir: str) -> list[str]:
    """Absolute paths of every parquet under the legacy local data dirs."""
    files: list[str] = []
    for d in _legacy_data_dirs(src, cache_dir):
        files.extend(glob.glob(os.path.join(d, "**", "*.parquet"), recursive=True))
    return sorted({os.path.abspath(p) for p in files if os.path.isfile(p)})


def _legacy_metadata_exists(src: dict, identifier: tuple) -> bool:
    """Whether legacy Iceberg metadata is present for ``identifier``.

    Deliberately raises on a probe failure instead of returning False:
    "we could not look" must never be reported as "there is nothing
    there", because the caller turns False into a silent no-op and that
    is precisely the quiet half-migration this module exists to prevent.
    """
    from backend.core.iceberg import _core as _core_mod

    namespace, table_name = identifier
    if _core_mod._is_local_only_source(src):
        warehouse = _core_mod._warehouse_uri(src)
        root = warehouse[len("file://") :] if warehouse.startswith("file://") else warehouse
        return os.path.isdir(os.path.join(root, namespace, table_name, "metadata"))

    from backend.core.duckdb import _get_fos_client

    s3 = _get_fos_client(src)
    bucket = src["bucket"]
    for prefix in _core_mod._metadata_search_prefixes(src, namespace, table_name):
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if resp.get("KeyCount") or resp.get("Contents"):
            return True
    return False


def _load_legacy_table(src: dict, identifier: tuple):
    """Load the legacy Iceberg table, or return None when it never existed.

    The catalog is frozen for WRITES under v3, not unreadable — pyiceberg
    still resolves it fine. A fresh v3 pod has no local
    ``iceberg_catalog.db`` at all, so the FOS-registration fallback
    (same one ``sync_data`` uses) is load-bearing, not a nicety.

    Raises ``RuntimeError`` when metadata demonstrably exists but cannot
    be read: adopting a partial set is worse than refusing.
    """
    from backend.core.iceberg import _core as _core_mod

    catalog = _core_mod._get_catalog(src)
    try:
        _core_mod._refresh_local_catalog_metadata(catalog, src, identifier)
    except Exception as e:  # pragma: no cover - helper already swallows internally
        logger.info("[ducklake] legacy pointer refresh failed for %s (continuing): %s", identifier, e)

    load_err: Exception | None = None
    try:
        return _core_mod._load_table_cached(src, identifier, catalog)
    except Exception as e:
        load_err = e

    try:
        table = _core_mod._try_register_from_fos(catalog, src, identifier)
    except Exception as e:
        raise RuntimeError(f"legacy Iceberg table {identifier} could not be registered from FOS: {e}") from e
    if table is not None:
        return table

    # _try_register_from_fos swallows its own I/O failures, so "None" is
    # ambiguous between "absent" and "could not look". Re-probe explicitly.
    try:
        exists = _legacy_metadata_exists(src, identifier)
    except Exception as e:
        raise RuntimeError(
            f"could not determine whether a legacy Iceberg table exists for {identifier}: {e}. "
            "Refusing to adopt a partial set — fix FOS access and re-run."
        ) from e

    if exists:
        raise RuntimeError(
            f"legacy Iceberg metadata exists for {identifier} but the table could not be loaded: {load_err}. "
            "Refusing to adopt a partial set — a half-migrated table is worse than a refused one."
        )
    return None


def _legacy_iceberg_data_files(src: dict, table_name: str = "logs") -> list[str]:
    """Live data-file URIs at the legacy table's current snapshot.

    Walks the snapshot's manifests the way the pre-v3
    ``manifest._get_cached_or_scan_metadata`` did, skipping ``DELETED``
    entries. Returns ``[]`` for a service that never had a legacy table
    (fresh v3) or whose table is empty. Raises ``RuntimeError`` when the
    table exists but cannot be read — enumeration is all-or-nothing so a
    mid-walk failure can never yield a partial adoption.
    """
    from backend.core.iceberg import _core as _core_mod

    identifier = _core_mod._table_identifier(src, table_name=table_name)
    table = _load_legacy_table(src, identifier)
    if table is None:
        return []

    try:
        snapshot = table.current_snapshot()
    except Exception as e:
        raise RuntimeError(f"legacy Iceberg table {identifier} has an unreadable snapshot: {e}") from e
    if snapshot is None:
        return []

    io = table.io
    paths: list[str] = []
    deleted = 0
    try:
        for manifest in snapshot.manifests(io):
            if not (manifest.has_added_files or manifest.has_existing_files):
                continue
            for entry in manifest.fetch_manifest_entry(io):
                if entry.status.name == "DELETED" or not entry.data_file:
                    deleted += 1
                    continue
                file_path = entry.data_file.file_path
                if file_path:
                    paths.append(file_path)
    except Exception as e:
        raise RuntimeError(
            f"legacy Iceberg table {identifier} manifests could not be read: {e}. Refusing to adopt a partial set."
        ) from e

    logger.info(
        "[ducklake] %s: legacy Iceberg table lists %d live data file(s) (%d deleted entries skipped)",
        src.get("name"),
        len(paths),
        deleted,
    )
    return paths


def adopt_iceberg_to_ducklake(service_id: str) -> dict:
    """Register the legacy pyiceberg-era parquet into the service's DuckLake table.

    Returns a summary dict with ``adopted_files``, ``skipped_files`` and
    ``rows_adopted`` (the long-standing contract), plus ``source``
    (``iceberg_table`` / ``local_dirs`` / ``none``) and
    ``candidate_files``. Raises ``ValueError`` on an unknown service or a
    row-count validation mismatch, and ``RuntimeError`` when a legacy
    table exists but is unreadable.
    """
    from backend.core.duckdb import _cache_dir, get_connection, get_source_for_service
    from backend.core.iceberg._ducklake import _ducklake_add_data_files, _ducklake_attach, ducklake_table_name

    src = get_source_for_service(service_id)
    if src is None:
        raise ValueError(f"unknown service: {service_id}")

    cache_dir = _cache_dir(src)

    # FOS (or the local warehouse) first — see the module docstring on why
    # this is a preference, not a union.
    data_files = [_normalize_data_path(p) for p in _legacy_iceberg_data_files(src)]
    origin = "iceberg_table"
    if not data_files:
        data_files = _local_legacy_files(src, cache_dir)
        origin = "local_dirs" if data_files else "none"
    else:
        local_only = _local_legacy_files(src, cache_dir)
        if local_only:
            logger.info(
                "[ducklake] %s: ignoring %d local cached parquet file(s) — they mirror the "
                "legacy Iceberg table's own data files, which are being adopted instead",
                service_id,
                len(local_only),
            )

    data_files = sorted(set(data_files))
    if not data_files:
        logger.info("[ducklake] %s: no legacy parquet files to adopt", service_id)
        return {
            "adopted_files": 0,
            "skipped_files": 0,
            "rows_adopted": 0,
            "source": origin,
            "candidate_files": 0,
        }

    con = get_connection(src)
    try:
        # Re-attach read-write (get_connection attaches read-only for the pool).
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        if not _ducklake_attach(con, src, read_only=False):
            raise RuntimeError(f"failed to attach DuckLake read-write for {service_id}")

        table = ducklake_table_name(src)
        table_ident = 'lake."{}"'.format(table.replace('"', '""'))

        # Create the per-service table from the FIRST parquet's schema
        # (LIMIT 0 — schema only, no rows).
        first_lit = escape_sql_literal(data_files[0])
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_ident} AS SELECT * FROM read_parquet('{first_lit}') LIMIT 0")

        # Skip files DuckLake already tracks — re-adding duplicates rows.
        already = {
            _normalize_data_path(r[0])
            for r in con.execute(
                f"SELECT data_file FROM ducklake_list_files('lake', '{escape_sql_literal(table)}')"
            ).fetchall()
        }
        to_adopt = [p for p in data_files if p not in already]
        skipped = len(data_files) - len(to_adopt)
        if not to_adopt:
            logger.info("[ducklake] %s: all %d legacy files already adopted", service_id, len(data_files))
            return {
                "adopted_files": 0,
                "skipped_files": skipped,
                "rows_adopted": 0,
                "source": origin,
                "candidate_files": len(data_files),
            }

        pre_row = con.execute(f"SELECT count(*) FROM {table_ident}").fetchone()
        pre_count = int(pre_row[0]) if pre_row else 0

        adopted = 0
        expected_rows = 0
        for i in range(0, len(to_adopt), _ADOPT_BATCH_SIZE):
            batch = to_adopt[i : i + _ADOPT_BATCH_SIZE]
            paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in batch)
            batch_rows_res = con.execute(
                f"SELECT count(*) FROM read_parquet([{paths_sql}], union_by_name=true)"
            ).fetchone()
            batch_rows = int(batch_rows_res[0]) if batch_rows_res else 0
            _ducklake_add_data_files(con, batch, alias="lake", table=table)
            adopted += len(batch)
            expected_rows += batch_rows

        post_row = con.execute(f"SELECT count(*) FROM {table_ident}").fetchone()
        post_count = int(post_row[0]) if post_row else 0

        delta = post_count - pre_count
        if delta != expected_rows:
            raise ValueError(
                f"Migration validation failed for {service_id}: lake count delta {delta} "
                f"!= adopted files' row count {expected_rows} (pre={pre_count}, post={post_count})"
            )

        logger.info(
            "[ducklake] %s: adopted %d legacy parquet files from %s (%d rows, %d already present)",
            service_id,
            adopted,
            origin,
            expected_rows,
            skipped,
        )
        return {
            "adopted_files": adopted,
            "skipped_files": skipped,
            "rows_adopted": expected_rows,
            "source": origin,
            "candidate_files": len(data_files),
        }
    finally:
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        _ducklake_attach(con, src, read_only=True)
        con.close()


# ── Boot-time adoption ────────────────────────────────────────────────────────


def legacy_adoption_completed(service_id: str) -> bool:
    """True when a successful adoption is already recorded for this service.

    The guard is a terminal ``cron_runs`` row rather than a file or an
    in-memory flag: metadata may be SQLite (one file per service) or a
    shared Postgres (many pods, one row), and this is the one durable
    store that is correct in both. It is also the surface an operator
    already looks at.
    """
    from backend.core.metadata.base import get_con

    con = get_con(service_id)
    row = con.execute(
        "SELECT 1 FROM cron_runs WHERE service_id = ? AND task = ? AND status = 'success' LIMIT 1",
        (service_id, ADOPTION_TASK),
    ).fetchone()
    return row is not None


def run_legacy_adoption_once(service_id: str, *, force: bool = False) -> dict | None:
    """Adopt this service's legacy parquet exactly once, ever.

    Returns the adoption summary when it ran, or ``None`` when it was
    skipped (already done, opted out, or another pod holds the lease).
    Never raises: a failure is recorded as an ``error`` ``cron_runs`` row
    and logged, because a migration that cannot run must not take the
    process down with it.

    ``force=True`` bypasses the "already done" guard and the opt-out env
    var — the admin endpoint's explicit "run it now" button. It is still
    safe: the adoption itself skips every file DuckLake already tracks.
    """
    from backend.core.metadata.cron_log import log_cron_run, start_cron_run

    if not force and os.environ.get(_SKIP_ENV) == "1":
        logger.info("[ducklake] %s: legacy adoption skipped (%s=1)", service_id, _SKIP_ENV)
        return None

    if not force:
        try:
            if legacy_adoption_completed(service_id):
                return None
        except Exception as e:
            logger.warning("[ducklake] %s: could not read the adoption guard, skipping: %s", service_id, e)
            return None

    try:
        # Doubles as the multi-pod mutex: start_cron_run acquires the
        # job_runs lease atomically and raises when someone else holds it.
        run_id = start_cron_run(service_id, ADOPTION_TASK)
    except RuntimeError:
        logger.info("[ducklake] %s: legacy adoption already running elsewhere, skipping", service_id)
        return None
    except Exception as e:
        logger.warning("[ducklake] %s: could not start the adoption cron run: %s", service_id, e)
        return None

    t0 = time.monotonic()
    try:
        result = adopt_iceberg_to_ducklake(service_id)
    except Exception as e:
        logger.exception("[ducklake] %s: legacy adoption FAILED", service_id)
        try:
            log_cron_run(
                service_id,
                ADOPTION_TASK,
                time.monotonic() - t0,
                "error",
                error_message=str(e)[:2000],
                summary="Legacy pyiceberg → DuckLake adoption failed; pre-v3 history is not yet queryable.",
                run_id=run_id,
            )
        except Exception:
            logger.exception("[ducklake] %s: could not record the adoption failure", service_id)
        return None

    try:
        log_cron_run(
            service_id,
            ADOPTION_TASK,
            time.monotonic() - t0,
            "success",
            rows_ingested=int(result.get("rows_adopted", 0)),
            parquet_files_created=int(result.get("adopted_files", 0)),
            summary=json.dumps(result),
            run_id=run_id,
        )
    except Exception:
        # The adoption itself succeeded; losing the bookkeeping only means
        # the next boot re-runs it, which is idempotent by construction.
        logger.exception("[ducklake] %s: adoption succeeded but its cron_runs row could not be written", service_id)
    return result


def _adoption_sweep(service_ids: list[str]) -> None:
    for sid in service_ids:
        try:
            run_legacy_adoption_once(sid)
        except Exception:  # pragma: no cover - run_legacy_adoption_once already guards
            logger.exception("[ducklake] %s: legacy adoption sweep entry failed", sid)


def start_legacy_adoption_sweep(service_ids: list[str]) -> threading.Thread | None:
    """Kick off boot-time adoption for ``service_ids`` in the background.

    Adoption of a large table registers thousands of files and can take
    minutes, so it never runs on the lifespan path. Services are swept
    sequentially — the work is DuckDB/FOS-bound and fanning it out on the
    4-core VM buys nothing. Progress and outcome are observable as
    ``ducklake_adopt`` rows in ``cron_runs``.
    """
    if not service_ids:
        return None
    if os.environ.get(_SKIP_ENV) == "1":
        logger.info("[ducklake] legacy adoption sweep skipped (%s=1)", _SKIP_ENV)
        return None
    thread = threading.Thread(
        target=_adoption_sweep,
        args=(list(service_ids),),
        daemon=True,
        name="ducklake-legacy-adoption",
    )
    thread.start()
    return thread
