"""Explicit operator-approved ClickHouse RUM repair from terminal archive manifests."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import time
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from typing import Any

from backend import config
from backend.high_scale.archive import ClickHousePublication, PostgresBatchManifestStore
from backend.high_scale.postgres_control import PostgresControlPlane

_TERMINAL_ARCHIVE_STATES = frozenset({"manifest_committed", "deletion_eligible", "source_deleted"})
_TERMINAL_LEDGER_STATUSES = frozenset({"acknowledged", "source_deleted"})
_RUM_DOMAINS = frozenset({"rum_vitals", "rum_errors"})


def recover(manifest_prefix: str, *, apply: bool, approve_digest: str | None) -> dict[str, Any]:
    started = time.perf_counter()
    import psycopg
    from psycopg.rows import dict_row

    import backend.core.clickhouse as clickhouse_client
    from backend.high_scale.archive import _DOMAIN_COLUMNS, _DOMAIN_TABLES
    from backend.high_scale.ingest_controller import _default_archive_factory
    from backend.high_scale.postgres_control import _archive_from_row
    from backend.high_scale.recovery import RumRecoveryBatchAdapter, execute_rum_recovery, plan_rum_recovery

    if not re.fullmatch(r"[0-9a-f]{8,64}", manifest_prefix):
        raise ValueError("manifest prefix must be 8-64 lowercase hexadecimal characters")
    if apply and not re.fullmatch(r"[0-9a-f]{64}", approve_digest or ""):
        raise ValueError("apply requires reviewed plan digest")
    settings = config.load_clickhouse_config()
    if settings is None:
        raise ValueError("configured ClickHouse required")
    query_timeout_s = min(settings.query_timeout_s, 5)
    client = clickhouse_client.ClickHouseClient(replace(settings, query_timeout_s=query_timeout_s))
    limits = {
        "max_rows_to_read": "100000",
        "read_overflow_mode": "throw",
        "max_result_rows": "1001",
        "result_overflow_mode": "throw",
    }
    try:
        with psycopg.connect(os.environ["METADATA_DSN"], row_factory=dict_row) as connection:
            connection.execute("SET LOCAL statement_timeout='15s'")
            connection.execute("SET LOCAL lock_timeout='2s'")
            if not apply:
                connection.execute("SET TRANSACTION READ ONLY")
            records = connection.execute(
                "SELECT * FROM high_scale_archive_manifests WHERE manifest_id LIKE %s LIMIT 2",
                (manifest_prefix + "%",),
            ).fetchall()
            if len(records) != 1:
                raise ValueError("manifest prefix must identify exactly one archive")
            item = _archive_from_row(records[0])
            manifest = item.manifest
            if manifest.domain not in _RUM_DOMAINS or item.state not in _TERMINAL_ARCHIVE_STATES:
                raise ValueError("manifest is not a terminal committed RUM archive")
            if manifest.artifact.row_count > 1000 or manifest.artifact.size_bytes > 32 * 1024 * 1024:
                raise ValueError("manifest exceeds bounded repair limits")
            if apply:
                lock_bytes = hashlib.sha256(("rum-repair:" + manifest.service_id).encode()).digest()[:8]
                lock = int.from_bytes(lock_bytes, "big", signed=True)
                held = connection.execute("SELECT pg_try_advisory_xact_lock(%s) AS held", (lock,)).fetchone()
                if held is None or not held["held"]:
                    raise ValueError("another scoped RUM repair is active")
            suffix = " FOR SHARE" if apply else ""
            owner = connection.execute(
                "SELECT owner_epoch,current_owner FROM high_scale_ownership WHERE service_id=%s" + suffix,
                (manifest.service_id,),
            ).fetchone()
            if owner is None or owner["current_owner"] != "high_scale" or owner["owner_epoch"] != item.owner_epoch:
                raise ValueError("archive owner epoch is not current")
            source_keys = [source.key for source in manifest.sources]
            ledger = connection.execute(
                "SELECT object_key,status,archive_manifest_id,owner_epoch FROM high_scale_source_objects "
                "WHERE service_id=%s AND object_key=ANY(%s)" + suffix,
                (manifest.service_id, source_keys),
            ).fetchall()
            has_non_terminal_row = any(
                row["status"] not in _TERMINAL_LEDGER_STATUSES
                or row["archive_manifest_id"] != manifest.manifest_id
                or row["owner_epoch"] != item.owner_epoch
                for row in ledger
            )
            if len(ledger) != len(manifest.sources) or has_non_terminal_row:
                raise ValueError("source ledger is not terminal for this exact archive/owner")
            archive = _default_archive_factory(manifest.service_id)
            if archive is None:
                raise ValueError("verified archive reader required")

            def read_facts(domain: str, keys: list[str]) -> list[dict[str, Any]]:
                placeholders = ",".join(f"{{key{i}:String}}" for i in range(len(keys)))
                query_params = {"service_id": manifest.service_id, **{f"key{i}": key for i, key in enumerate(keys)}}
                rows = client.execute(
                    f"SELECT {','.join(_DOMAIN_COLUMNS[domain])} FROM {_DOMAIN_TABLES[domain]} "
                    f"WHERE service_id={{service_id:String}} AND source_object_key IN ({placeholders}) LIMIT 1001",
                    query_params,
                    settings=limits,
                )
                if len(rows) > 1000:
                    raise ValueError("existing fact result exceeds bound")
                receipts: dict[str, bool] = {}
                for row in rows:
                    batch_id = str(row["batch_id"])
                    if batch_id not in receipts:
                        receipt = client.execute(
                            "SELECT publication_state,expected_rows,visible_rows FROM high_scale_batch_publications FINAL "
                            "WHERE service_id={service_id:String} AND domain={domain:String} "
                            "AND generation={generation:String} AND batch_id={batch_id:String} LIMIT 2",
                            {
                                "service_id": manifest.service_id,
                                "domain": domain,
                                "generation": str(item.owner_epoch),
                                "batch_id": batch_id,
                            },
                            settings=limits,
                        )
                        receipts[batch_id] = (
                            len(receipt) == 1
                            and receipt[0]["publication_state"] == "visible"
                            and receipt[0]["expected_rows"] == receipt[0]["visible_rows"]
                        )
                    row["repair_visible"] = receipts[batch_id] and row["publication_state"] == "visible"
                return rows

            def make_plan():
                return plan_rum_recovery(manifest, archive, owner_epoch=item.owner_epoch, read_facts=read_facts)

            # Constructing these adapters performs no writes. Dry-run never calls publish.
            publication = ClickHousePublication(
                PostgresBatchManifestStore(PostgresControlPlane()),
                RumRecoveryBatchAdapter(client),
            )
            report = execute_rum_recovery(make_plan, publication, apply=apply, approve_digest=approve_digest)
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            return {**report, "read_only": not apply, "duration_ms": duration_ms}
    finally:
        client.close()


def recover_rum_main(argv: list[str] | None = None) -> int:
    rum_parser = argparse.ArgumentParser(description=__doc__)
    rum_parser.add_argument("--manifest-prefix", required=True)
    rum_parser.add_argument("--apply", action="store_true")
    rum_parser.add_argument("--approve-plan")
    args = rum_parser.parse_args(argv)
    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            report = recover(args.manifest_prefix, apply=args.apply, approve_digest=args.approve_plan)
    except Exception as exc:
        print(json.dumps({"status": "blocked", "error_type": type(exc).__name__}))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


main = recover_rum_main

__all__ = [
    "ClickHousePublication",
    "PostgresBatchManifestStore",
    "PostgresControlPlane",
    "config",
    "main",
    "recover",
    "recover_rum_main",
]


if __name__ == "__main__":
    raise SystemExit(main())
